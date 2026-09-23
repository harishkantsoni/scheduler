"""Claim an execution, run its handler, record the outcome.

The claim is committed before the handler runs, so ownership is a durable
fact rather than a database lock. Locks die with the connection and never
expire, so a hung worker would strand its row forever; a lease expires, and
the sweep reclaims it.

Two workers never hold a valid claim on the same execution at once.

A handler can still run twice. When a lease expires, nothing can tell a dead
worker from a slow one, so invocation is at-least-once. Fencing keeps the
recorded result correct; idempotent handlers keep the side effects correct.
"""

from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scheduler import handlers
from scheduler.policy import DEFAULT_LEASE_SECONDS, backoff_delay


def claim(
    conn: psycopg.Connection,
    worker_id: str,
    now: datetime,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> dict[str, Any] | None:
    """Take ownership of one runnable execution, or return None.

    SKIP LOCKED is what makes N pollers both safe and parallel: plain FOR
    UPDATE would be safe but would queue every worker on the same row, since
    ORDER BY ... LIMIT 1 makes them all choose it.

    The SELECT and UPDATE are one statement, so there is no window between
    choosing a row and taking it.
    """
    with conn.transaction():
        row = conn.cursor(row_factory=dict_row).execute(
            """
            UPDATE execution AS e
            SET status           = 'RUNNING',
                claimed_by       = %(worker)s,
                claim_token      = gen_random_uuid(),
                lease_expires_at = %(now)s + make_interval(secs => %(lease)s),
                attempt          = e.attempt + 1
            FROM schedule AS s
            WHERE s.id = e.schedule_id
              AND e.id = (
                    SELECT id FROM execution
                    WHERE status = 'QUEUED'
                      AND run_after <= %(now)s
                    ORDER BY scheduled_for, id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
              )
            RETURNING e.id, e.attempt, e.max_attempts, e.claim_token,
                      s.job_type, s.payload
            """,
            {"worker": worker_id, "now": now, "lease": lease_seconds},
        ).fetchone()

        if row is None:
            return None

        # UNIQUE (execution_id, attempt) also stops a stale worker fabricating
        # a second history row for an attempt it no longer owns.
        conn.execute(
            """
            INSERT INTO execution_attempt
                (execution_id, attempt, worker_id, claim_token, started_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (row["id"], row["attempt"], worker_id, row["claim_token"], now),
        )

    return row


def _close_attempt(
    conn: psycopg.Connection,
    execution_id: int,
    attempt: int,
    now: datetime,
    outcome: str,
    detail: str | None,
) -> None:
    conn.execute(
        """
        UPDATE execution_attempt
        SET finished_at = %s, outcome = %s, detail = %s
        WHERE execution_id = %s AND attempt = %s
        """,
        (now, outcome, detail, execution_id, attempt),
    )


def complete_success(
    conn: psycopg.Connection,
    execution_id: int,
    attempt: int,
    claim_token: str,
    now: datetime,
    result: Any,
) -> bool:
    """Record SUCCESS. False means we were fenced out.

    `AND claim_token = %s` is the fence: if the sweep reclaimed this execution
    and handed it to another worker, the token has changed, this matches zero
    rows, and we discard our result rather than overwrite a newer attempt's.
    """
    with conn.transaction():
        updated = conn.execute(
            """
            UPDATE execution
            SET status           = 'SUCCESS',
                result           = %s,
                finished_at      = %s,
                lease_expires_at = NULL
            WHERE id = %s AND claim_token = %s
            """,
            (Jsonb(result), now, execution_id, claim_token),
        ).rowcount

        if updated == 0:
            _close_attempt(conn, execution_id, attempt, now, "FENCED",
                           "lost ownership before the result could be recorded")
            return False

        _close_attempt(conn, execution_id, attempt, now, "SUCCESS", None)
        return True


def complete_failure(
    conn: psycopg.Connection,
    execution_id: int,
    attempt: int,
    max_attempts: int,
    claim_token: str,
    now: datetime,
    error: str,
    force_terminal: bool = False,
) -> bool:
    """Requeue with backoff, or fail terminally. False means fenced out.

    force_terminal skips the retry budget for failures retrying cannot fix.

    Fenced like complete_success: a stale worker must not be able to requeue
    or terminally fail an execution a newer attempt now owns.
    """
    exhausted = force_terminal or attempt >= max_attempts

    with conn.transaction():
        if exhausted:
            updated = conn.execute(
                """
                UPDATE execution
                SET status           = 'FAILED',
                    last_error       = %s,
                    finished_at      = %s,
                    lease_expires_at = NULL
                WHERE id = %s AND claim_token = %s
                """,
                (error, now, execution_id, claim_token),
            ).rowcount
        else:
            updated = conn.execute(
                """
                UPDATE execution
                SET status           = 'QUEUED',
                    last_error       = %s,
                    run_after        = %s,
                    claimed_by       = NULL,
                    claim_token      = NULL,
                    lease_expires_at = NULL
                WHERE id = %s AND claim_token = %s
                """,
                (error, now + backoff_delay(attempt), execution_id, claim_token),
            ).rowcount

        if updated == 0:
            _close_attempt(conn, execution_id, attempt, now, "FENCED",
                           "lost ownership before the failure could be recorded")
            return False

        _close_attempt(conn, execution_id, attempt, now, "FAILED", error)
        return True


def run_one(
    conn: psycopg.Connection,
    worker_id: str,
    now: datetime,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    registry: dict[str, handlers.Handler] | None = None,
) -> str | None:
    """Claim and run at most one execution.

    Returns 'SUCCESS', 'FAILED', 'FENCED', or None if nothing was claimable.
    """
    claimed = claim(conn, worker_id, now, lease_seconds)
    if claimed is None:
        return None

    # No lock is held while the handler runs; only the lease.
    try:
        handler = handlers.resolve(claimed["job_type"], registry)
    except handlers.UnknownJobType as exc:
        # A missing handler is a configuration error, not a transient one.
        # Spending the retry budget on it only delays the alert.
        ok = complete_failure(
            conn, claimed["id"], claimed["attempt"], claimed["max_attempts"],
            claimed["claim_token"], now, f"{type(exc).__name__}: {exc}",
            force_terminal=True,
        )
        return "FAILED" if ok else "FENCED"

    try:
        result = handler(claimed["payload"])
    except Exception as exc:  # a handler failure is data, not a crash
        ok = complete_failure(
            conn, claimed["id"], claimed["attempt"], claimed["max_attempts"],
            claimed["claim_token"], now, f"{type(exc).__name__}: {exc}",
        )
        return "FAILED" if ok else "FENCED"

    ok = complete_success(
        conn, claimed["id"], claimed["attempt"], claimed["claim_token"], now, result
    )
    return "SUCCESS" if ok else "FENCED"
