"""Reclaim executions whose lease expired with no outcome recorded.

Run from the ticker. A worker that dies leaves its row RUNNING forever: the
lease is written into the row, so unlike a lock it does not vanish with the
connection. Nothing else would ever notice, so this sweep must.

When a lease expires there is no way to tell a dead worker from a slow one.
We reclaim anyway, because a job stranded forever is worse. That is why
handlers must be idempotent.
"""

from datetime import datetime

import psycopg

from scheduler.policy import backoff_delay


def sweep(conn: psycopg.Connection, now: datetime) -> list[int]:
    """Reclaim expired leases. Returns the ids reclaimed."""
    reclaimed: list[int] = []

    with conn.transaction():
        rows = conn.execute(
            """
            SELECT id, attempt, max_attempts
            FROM execution
            WHERE status = 'RUNNING'
              AND lease_expires_at < %s
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            """,
            (now,),
        ).fetchall()

        for execution_id, attempt, max_attempts in rows:
            # LEASE_EXPIRED can only be recorded here: once the execution goes
            # back to QUEUED its own row shows no trace of this attempt.
            conn.execute(
                """
                UPDATE execution_attempt
                SET finished_at = %s,
                    outcome     = 'LEASE_EXPIRED',
                    detail      = 'lease expired with no outcome'
                WHERE execution_id = %s AND attempt = %s AND outcome IS NULL
                """,
                (now, execution_id, attempt),
            )

            if attempt >= max_attempts:
                conn.execute(
                    """
                    UPDATE execution
                    SET status           = 'FAILED',
                        last_error       = 'lease expired; attempts exhausted',
                        finished_at      = %s,
                        lease_expires_at = NULL
                    WHERE id = %s
                    """,
                    (now, execution_id),
                )
            else:
                # Clearing claim_token fences the old worker: its completing
                # update will no longer match.
                conn.execute(
                    """
                    UPDATE execution
                    SET status           = 'QUEUED',
                        run_after        = %s,
                        claimed_by       = NULL,
                        claim_token      = NULL,
                        lease_expires_at = NULL
                    WHERE id = %s
                    """,
                    (now + backoff_delay(attempt), execution_id),
                )

            reclaimed.append(execution_id)

    return reclaimed
