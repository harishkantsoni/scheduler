"""Turn overdue schedules into QUEUED executions.

Called once per tick. The ticker is unreliable: it may fire twice for the same
minute, or skip a minute entirely. Both are handled here.
"""

from datetime import datetime, timedelta

import psycopg


def floor_to_minute(ts: datetime) -> datetime:
    """Truncate to the minute.

    scheduled_for is half of an execution's identity, so it must not carry
    sub-minute noise from however a schedule was seeded.
    """
    return ts.replace(second=0, microsecond=0)


def advance(next_run_at: datetime, interval_minutes: int, now: datetime) -> datetime:
    """The next fire time strictly after `now`.

    Missed intervals are skipped, not backfilled: an hour-long outage on a
    5-minute schedule produces one catch-up execution rather than twelve.
    That is a policy choice; backfilling is equally defensible.

    Strictly after matters. Were next_run_at left equal to now, a second tick
    at that same instant would find the schedule due again and fire the NEXT
    slot -- real extra work, under a key the unique constraint cannot catch.
    """
    step = timedelta(minutes=interval_minutes)
    nxt = next_run_at + step
    while nxt <= now:
        nxt += step
    return nxt


def create_due_executions(conn: psycopg.Connection, now: datetime) -> list[int]:
    """Create one QUEUED execution per overdue schedule, and advance each
    schedule's next_run_at. Returns the ids of the executions created.

    Both happen in a single transaction, so a crash can never advance a
    schedule without creating its execution.

    `now` is a parameter rather than the wall clock so tests can fire two
    ticks at the same instant, or jump forward, without waiting.
    """
    created_execution_ids: list[int] = []

    with conn.transaction():
        # FOR UPDATE stops concurrent dispatchers clobbering each other's
        # next_run_at. The unique constraint below is the actual duplicate
        # protection.
        due = conn.execute(
            """
            SELECT id, interval_minutes, next_run_at, max_attempts
            FROM schedule
            WHERE enabled AND next_run_at <= %s
            ORDER BY id
            FOR UPDATE
            """,
            (now,),
        ).fetchall()

        for schedule_id, interval_minutes, next_run_at, max_attempts in due:
            scheduled_for = floor_to_minute(next_run_at)

            row = conn.execute(
                """
                INSERT INTO execution
                    (schedule_id, scheduled_for, max_attempts, run_after)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (schedule_id, scheduled_for) DO NOTHING
                RETURNING id
                """,
                (schedule_id, scheduled_for, max_attempts, scheduled_for),
            ).fetchone()

            # None means the row already existed: a duplicate tick.
            if row is not None:
                created_execution_ids.append(row[0])

            conn.execute(
                "UPDATE schedule SET next_run_at = %s WHERE id = %s",
                (advance(next_run_at, interval_minutes, now), schedule_id),
            )

    return created_execution_ids
