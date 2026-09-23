"""Part 1 -- dispatch.

Proves the three things the task demands of the dispatcher:

  * a due schedule produces exactly one QUEUED execution
  * a duplicate tick creates no second execution -- and the guarantee lives
    in the DATABASE, not in this process's memory
  * a skipped tick is caught up, still named for its intended fire time
"""

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from scheduler import db, dispatcher


# ---------------------------------------------------------------------------
# Pure functions -- no database, no clock.
# ---------------------------------------------------------------------------

def test_floor_to_minute_discards_seconds_and_microseconds():
    ts = datetime(2026, 8, 3, 10, 0, 47, 831294, tzinfo=timezone.utc)
    assert dispatcher.floor_to_minute(ts) == datetime(
        2026, 8, 3, 10, 0, 0, 0, tzinfo=timezone.utc
    )


def test_advance_lands_strictly_after_now():
    """Never leave next_run_at == now: a duplicate tick at that same instant
    would find the schedule due again and fire the NEXT slot -- a different
    key, so the unique constraint could not catch it."""
    t = datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)
    assert dispatcher.advance(t, 5, now=t) == t + timedelta(minutes=5)
    # now sits exactly on a boundary -> step past it, do not stop on it
    assert dispatcher.advance(t, 5, now=t + timedelta(minutes=5)) == t + timedelta(
        minutes=10
    )


# ---------------------------------------------------------------------------
# Dispatch against a real database.
# ---------------------------------------------------------------------------

def test_due_schedule_produces_one_queued_execution(conn, make_schedule, t0):
    schedule_id = make_schedule(interval_minutes=5, next_run_at=t0)

    created = dispatcher.create_due_executions(conn, t0)

    assert len(created) == 1
    row = conn.execute(
        "SELECT schedule_id, scheduled_for, status FROM execution"
    ).fetchone()
    assert row == (schedule_id, t0, "QUEUED")


def test_schedule_not_yet_due_produces_nothing(conn, make_schedule, t0):
    make_schedule(next_run_at=t0 + timedelta(minutes=5))

    assert dispatcher.create_due_executions(conn, t0) == []


def test_disabled_schedule_is_never_dispatched(conn, make_schedule, t0):
    make_schedule(next_run_at=t0, enabled=False)

    assert dispatcher.create_due_executions(conn, t0) == []


# --- REQUIRED DEMO: a duplicate tick creates no duplicate execution --------

def test_duplicate_tick_creates_no_second_execution(conn, make_schedule, t0):
    """The ticker fires twice for the same minute."""
    make_schedule(interval_minutes=5, next_run_at=t0)

    first = dispatcher.create_due_executions(conn, t0)
    second = dispatcher.create_due_executions(conn, t0)  # the SAME instant, again

    assert len(first) == 1
    assert second == []
    assert conn.execute("SELECT count(*) FROM execution").fetchone()[0] == 1


def test_duplicate_identity_is_rejected_by_the_database(conn, make_schedule, t0):
    """The task says: "Solve this in the database, not in application memory."

    So prove it AT the database. Two independent connections -- which share
    no memory, exactly like two dispatcher processes -- try to create the
    same (schedule_id, scheduled_for). The second is refused by the unique
    constraint, with no application-level check anywhere in the path.
    """
    schedule_id = make_schedule(next_run_at=t0)

    insert = (
        "INSERT INTO execution (schedule_id, scheduled_for, max_attempts, run_after)"
        " VALUES (%s, %s, 3, %s)"
    )
    args = (schedule_id, t0, t0)

    with db.connect() as a, db.connect() as b:
        a.execute(insert, args)

        with pytest.raises(psycopg.errors.UniqueViolation):
            b.execute(insert, args)

    # ...and the form the dispatcher actually uses turns that refusal into a
    # silent no-op rather than an error.
    with db.connect() as c:
        c.execute(insert + " ON CONFLICT (schedule_id, scheduled_for) DO NOTHING", args)

    assert conn.execute("SELECT count(*) FROM execution").fetchone()[0] == 1


# --- REQUIRED BEHAVIOUR: a skipped tick must not lose the job -------------

def test_skipped_tick_is_caught_up_and_keeps_its_intended_time(conn, make_schedule, t0):
    """No tick fires at 10:00. The first tick back is at 10:03."""
    schedule_id = make_schedule(interval_minutes=5, next_run_at=t0)

    created = dispatcher.create_due_executions(conn, t0 + timedelta(minutes=3))

    assert len(created) == 1
    scheduled_for = conn.execute(
        "SELECT scheduled_for FROM execution WHERE schedule_id = %s", (schedule_id,)
    ).fetchone()[0]

    # Named 10:00 -- the time it was MEANT to run -- not 10:03, the time we
    # noticed. That is what makes a late fire produce the same key as an
    # on-time one.
    assert scheduled_for == t0


def test_long_outage_produces_one_catch_up_not_a_backlog(conn, make_schedule, t0):
    """Down from 10:00 to 11:00 on a 5-minute schedule.

    Policy choice: catch up ONCE rather than replaying all twelve missed
    slots, so a recovering system is not immediately hit by the backlog it
    accumulated while it was down.
    """
    make_schedule(interval_minutes=5, next_run_at=t0)

    back_up = t0 + timedelta(hours=1)
    assert len(dispatcher.create_due_executions(conn, back_up)) == 1

    # The schedule is now parked in the future, so the next tick is quiet.
    assert dispatcher.create_due_executions(conn, back_up) == []
    assert conn.execute("SELECT count(*) FROM execution").fetchone()[0] == 1

    next_run_at = conn.execute("SELECT next_run_at FROM schedule").fetchone()[0]
    assert next_run_at > back_up


def test_several_schedules_are_dispatched_independently(conn, make_schedule, t0):
    make_schedule(job_type="refresh-cache", next_run_at=t0)
    make_schedule(job_type="archive-rows", next_run_at=t0)
    make_schedule(job_type="not-yet", next_run_at=t0 + timedelta(minutes=30))

    created = dispatcher.create_due_executions(conn, t0)

    assert len(created) == 2
