# Durable Job Scheduler

A crash-safe, exactly-once-dispatch job scheduler built on nothing but
Postgres — no Airflow, no Temporal, no SQS, no in-memory timers.

## The problem

Every backend eventually needs recurring jobs: refresh a cache, sync a
third-party connector, archive old rows, renew a subscription, send a
scheduled report. Two paths are common, and both have sharp edges:

- **Reach for a workflow platform** (Airflow, Temporal, Step Functions).
  Powerful, but it's new infrastructure to run, learn, and pay for — often
  overkill for "run this every 5 minutes."
- **Hand-roll it** with `setInterval`/`cron` + an in-memory job list. Fast to
  write, and fine until a deploy restarts the process mid-job, two instances
  double-run the same job, or a worker dies and the job silently vanishes.
  These failures are rare enough to pass code review and common enough to
  page someone at 2am.

This project is the missing middle: durable scheduling with real crash and
concurrency guarantees, using only a database every backend already has.

## Real-world use cases

Anything that needs to run on a schedule and *must not* be lost, duplicated,
or silently stuck:

- **Cache/materialized-view refresh** — recompute an expensive aggregate
  every N minutes without two workers recomputing it at once.
- **Third-party connector sync** — pull from Stripe/Salesforce/an API on a
  timer; a crash mid-sync must resume, not skip or double-import.
- **Data retention / archival** — nightly "delete or archive rows older than
  X," where running it twice must not double-delete.
- **Subscription billing / renewals** — a job that charges a customer must
  never fire twice for the same billing period; this design's idempotency
  key (`schedule_id, scheduled_for`) is built for exactly that.
- **Scheduled report/email generation** — daily digest jobs where a crashed
  worker's report must still go out, exactly once, not zero or two times.
- **Webhook/notification retry** — failed sends get backoff and a bounded
  retry budget instead of an ad-hoc retry loop per caller.

## How this fits into a real system

It's designed to be dropped into an existing service, not stood up as new
infrastructure:

- **No new moving parts.** If the product already runs Postgres — most do —
  there's no broker, no message queue, and no separate scheduler service to
  operate. `schema.sql` is three tables.
- **The ticker is a cron job or a tiny sidecar container**, calling
  `create_due_executions` + `sweep` once a minute. It holds no state itself;
  killing and restarting it loses nothing.
- **Workers scale horizontally like any stateless pod.** Add more to drain
  the queue faster; `FOR UPDATE SKIP LOCKED` means they never contend on the
  same row. A crashed worker's job is picked up by whichever worker is next.
- **Handlers are just functions** (`scheduler/handlers.py`), so wiring in
  real job logic — an actual cache refresh, an actual API call — is a matter
  of registering a function, not touching the scheduling machinery.
- **Scope, honestly stated:** this covers durable dispatch, concurrent
  claiming, and crash recovery (at-least-once execution, fenced so a revived
  worker can't corrupt a newer attempt's result). It does not implement
  async/long-running external jobs (a reconciler polling an external run
  id) — a natural next extension, deliberately left out to keep the core
  guarantees easy to verify.

## How to run

Only Postgres runs in Docker; the Python processes run on the host.

**1. Start the database and install dependencies.**

```bash
docker compose up -d --wait          # Postgres 17 on :5433
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
```

**2. (Optional) Run the tests.** They apply `schema.sql` themselves, so this
needs no further setup.

```bash
.venv/Scripts/python -m pytest -v    # 26 tests, ~1s
```

**3. Apply the schema and seed some schedules.**

```bash
type schema.sql | docker exec -i scheduler-pg psql -U scheduler -d scheduler
python add_schedules.py
```

**4. Start the processes, each in its own terminal.** They share nothing but
the database, so any of them can be started, killed, or restarted at any time.

```bash
python run_ticker.py 5         # creates due executions + reclaims expired leases, every 5s
python run_worker.py alpha     # claims one execution at a time and runs it
python run_worker.py beta      # add as many as you like
```

**5. Watch the state change.**

```bash
docker exec -it scheduler-pg psql -U scheduler -d scheduler -c \
  "SELECT id, status, attempt, claimed_by, last_error FROM execution ORDER BY id;"
```

Killing a worker mid-handler is worth trying: it records no outcome, its lease
expires, and the next sweep requeues the execution for someone else. The
`always-fails` schedule is there to show retry with backoff, then terminal
failure once the attempt budget runs out.

---

## Reading order

The system is easiest to follow in the order it does its work.

1. `schema.sql` — the tables and constraints. Most of the design is here.
2. `add_schedules.py` — where work comes from: a schedule is just a row.
3. `run_ticker.py`, `run_worker.py` — the two processes. Both are short loops.
4. `scheduler/dispatcher.py` — turns a due schedule into an execution.
5. `scheduler/worker.py` — claims an execution, runs it, records the outcome.
   The core.
6. `scheduler/recovery.py` — what happens when a worker never comes back.
7. `scheduler/ticker.py` — composes dispatch and recovery into one periodic pass.

`scheduler/db.py`, `scheduler/policy.py` and `scheduler/handlers.py` are small
enough to read whenever. The one thing worth knowing early is that `db.py` sets
`autocommit=True`, which is why every write is wrapped in an explicit
`with conn.transaction():`.

---

## What this project does

A **schedule** is a recurring job: "run `refresh-cache` every 5 minutes." An
**execution** is one occurrence of it: "the `refresh-cache` that was due at
10:35." Schedules are the intent; executions are the work.

Three moving parts, each a plain function against the database:

- The **dispatcher** finds schedules whose `next_run_at` has passed and inserts
  an execution for each, then advances the schedule. A unique constraint on
  `(schedule_id, scheduled_for)` means running it twice for the same minute
  cannot produce two executions.
- A **worker** claims one `QUEUED` execution with a single atomic `UPDATE`,
  commits, then runs the handler. The claim sets a **lease** — a durable expiry
  timestamp — and a **fencing token**. Many workers can poll the same table
  without waiting on each other.
- The **recovery sweep** finds executions whose lease expired and requeues
  them, so a worker that dies mid-job does not strand its work.

Nothing depends on a process staying alive. There are no in-memory timers and
no in-memory queue: kill everything, restart it, and the database still knows
what is due, what is running, and what has expired. Postgres is the only
durable state, and every safety property is enforced there — unique
constraints, row locks, conditional updates — never by anything a process
remembers.

## State machine

`QUEUED → RUNNING → SUCCESS | FAILED`, with `RUNNING → QUEUED` for retries.

| From | To | When | Enforced by |
|---|---|---|---|
| — | QUEUED | schedule is due | `UNIQUE (schedule_id, scheduled_for)` |
| QUEUED | RUNNING | worker claims it | `FOR UPDATE SKIP LOCKED`, `run_after <= now` |
| RUNNING | SUCCESS | handler returned | `AND claim_token = ?` |
| RUNNING | QUEUED | handler raised, or lease expired; attempts left | `AND claim_token = ?`, or the sweep |
| RUNNING | FAILED | attempts exhausted, or unknown `job_type` | `AND claim_token = ?`, or the sweep |

`SUCCESS` and `FAILED` are terminal. A `CHECK` constraint enforces the shape of
each state — a `QUEUED` row cannot hold a lease.

## At-least-once, not exactly-once

Two workers can never both hold a *valid claim* on one execution, and a
duplicate tick can never create a second execution.

But a handler can still run twice. An expired lease cannot distinguish a dead
worker from a slow one, so **invocation is at-least-once. Exactly-once is not
achievable here** and is not claimed.

Fencing keeps the *record* correct: every completing update carries
`AND claim_token = ?`, so a worker whose lease was reclaimed matches zero rows
and discards its result.

Side effects are the handler's job, not the scheduler's: be idempotent, or key
the effect on `(schedule_id, scheduled_for)` — deterministic, so every retry
computes the same key and the downstream can deduplicate. Where it cannot, the
window is narrowed, not closed.

## Crash points considered

| Crash | Outcome |
|---|---|
| Between creating an execution and advancing the schedule | One transaction — both roll back; next tick redoes it |
| Ticker fires twice for one minute | `advance()` already moved `next_run_at`; the unique constraint is the backstop |
| Ticker skips a minute | Still due; fires late, keeping its *intended* `scheduled_for` |
| Two dispatchers, or two sweeps, run together | `FOR UPDATE SKIP LOCKED` |
| Two workers claim together | One atomic `UPDATE`; the loser gets another row |
| Worker dies before, during, or after the handler | Lease expires, sweep requeues. The attempt was consumed at claim time, so a poison job cannot loop forever |
| Worker was only *paused*, wakes after reclaim, writes its result | Fenced by `claim_token`; result discarded, `FENCED` recorded |
| Sweep crashes mid-pass | Rolls back; next tick redoes it |

`execution_attempt` keeps per-attempt history separate from current state.
`LEASE_EXPIRED` and `FENCED` can only be recorded there — by the time either is
observed, the execution row has moved on.
