-- Durable job scheduler. Re-runnable: drops and recreates.

-- Children before parents, tables before the types they use. No CASCADE:
-- a wrong order should fail loudly rather than silently drop dependents.
DROP TABLE IF EXISTS execution_attempt;
DROP TABLE IF EXISTS execution;
DROP TABLE IF EXISTS schedule;
DROP TYPE  IF EXISTS attempt_outcome;
DROP TYPE  IF EXISTS execution_status;

CREATE TYPE execution_status AS ENUM (
    'QUEUED', 'RUNNING', 'SUCCESS', 'FAILED'
);

CREATE TYPE attempt_outcome AS ENUM (
    'SUCCESS', 'FAILED', 'LEASE_EXPIRED', 'FENCED'
);

-- The recurring rule. Produces executions; never runs itself.
CREATE TABLE schedule (
    id                BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_type          TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    interval_minutes  INTEGER     NOT NULL CHECK (interval_minutes > 0),
    enabled           BOOLEAN     NOT NULL DEFAULT TRUE,

    -- Due-ness is next_run_at <= now, so a skipped tick is
    -- caught up rather than lost.
    next_run_at       TIMESTAMPTZ NOT NULL,

    max_attempts      INTEGER     NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),

    -- Audit only; nothing reads it. Kept because it cannot be backfilled
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX schedule_due_idx ON schedule (next_run_at) WHERE enabled;


-- One occurrence of a schedule, for one specific minute.
CREATE TABLE execution (
    id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- RESTRICT, not CASCADE: execution history must never be destroyed as a
    -- side effect of removing a schedule. Schedules are disabled, not deleted.
    schedule_id    BIGINT      NOT NULL REFERENCES schedule (id) ON DELETE RESTRICT,

    -- The intended fire time, truncated to the minute -- never the time the
    -- dispatcher noticed. A late tick therefore produces the same key as an
    -- on-time one.
    scheduled_for  TIMESTAMPTZ NOT NULL,

    status         execution_status NOT NULL DEFAULT 'QUEUED',

    claimed_by       TEXT,
    claim_token      UUID,          -- fencing token, rotated on every claim
    lease_expires_at TIMESTAMPTZ,   -- ownership deadline, not a work deadline

    -- Incremented at claim time, not at completion. A worker killed mid-job
    -- reports nothing, so counting on completion would leave this at 0
    -- forever: the job would be requeued endlessly, crashing one worker after
    -- another. Counting on claim bounds that to max_attempts.
    attempt          INTEGER     NOT NULL DEFAULT 0 CHECK (attempt >= 0),

    -- Snapshotted from schedule.max_attempts at creation: editing a schedule
    -- must not change the budget of work already in flight.
    max_attempts     INTEGER     NOT NULL CHECK (max_attempts >= 1),

    -- Backoff gate. A QUEUED row is invisible until now >= run_after, which
    -- keeps the delay in the database instead of in a process's timer.
    run_after        TIMESTAMPTZ NOT NULL,

    result           JSONB,
    last_error       TEXT,
    finished_at      TIMESTAMPTZ,

    -- Audit only; created_at - scheduled_for is dispatcher lag.
    -- how late the tick that created this row actually fired,
    -- which is worth having available and cannot be backfilled later.
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A duplicate tick recomputes the same key and collides here. With
    -- ON CONFLICT DO NOTHING this yields exactly one execution per schedule
    -- per fire time, across any number of dispatcher processes.
    CONSTRAINT execution_identity UNIQUE (schedule_id, scheduled_for),

    -- status and the lease columns must not drift apart: a QUEUED row that
    -- kept its claim_token would silently disable fencing.
    CONSTRAINT execution_state_invariants CHECK (
        CASE status
            WHEN 'QUEUED' THEN
                claimed_by IS NULL AND claim_token IS NULL
                AND lease_expires_at IS NULL AND finished_at IS NULL
            WHEN 'RUNNING' THEN
                claimed_by IS NOT NULL AND claim_token IS NOT NULL
                AND lease_expires_at IS NOT NULL AND finished_at IS NULL
            WHEN 'SUCCESS'   THEN finished_at IS NOT NULL
            WHEN 'FAILED'    THEN finished_at IS NOT NULL
            -- A CHECK passes when its expression is NULL, so ELSE false is what forces
            -- a newly added status to be handled here instead of silently allowed.
            ELSE false
        END
    )
);

CREATE INDEX execution_claimable_idx
    ON execution (run_after, scheduled_for) WHERE status = 'QUEUED';

CREATE INDEX execution_lease_idx
    ON execution (lease_expires_at) WHERE status = 'RUNNING';



-- Per-attempt history, kept separate from the execution's current state.
-- LEASE_EXPIRED and FENCED can only be recorded here: by the time either is
-- observed, the execution row has already moved on.
CREATE TABLE execution_attempt (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id  BIGINT      NOT NULL REFERENCES execution (id) ON DELETE RESTRICT,

    attempt       INTEGER     NOT NULL CHECK (attempt >= 1),
    worker_id     TEXT        NOT NULL,
    claim_token   UUID        NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    outcome       attempt_outcome,
    detail        TEXT,

    CONSTRAINT execution_attempt_identity UNIQUE (execution_id, attempt)
);

CREATE INDEX execution_attempt_execution_idx ON execution_attempt (execution_id);
