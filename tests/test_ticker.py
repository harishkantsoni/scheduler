"""The ticker: dispatch and recovery in one periodic pass."""

from datetime import timedelta

from scheduler import ticker, worker


def test_tick_dispatches_due_schedules(conn, make_schedule, t0):
    make_schedule(job_type="noop", interval_minutes=60, next_run_at=t0)

    result = ticker.tick(conn, t0)

    assert len(result["dispatched"]) == 1
    assert result["reclaimed"] == []


def test_tick_reclaims_expired_leases(conn, make_schedule, t0):
    make_schedule(job_type="noop", interval_minutes=60, next_run_at=t0)
    execution_id = ticker.tick(conn, t0)["dispatched"][0]

    # A worker claims it and is never heard from again.
    worker.claim(conn, "ravi", t0, lease_seconds=300)

    result = ticker.tick(conn, t0 + timedelta(seconds=301))

    assert result["dispatched"] == []          # schedule is not due again yet
    assert result["reclaimed"] == [execution_id]
    assert conn.execute(
        "SELECT status FROM execution WHERE id = %s", (execution_id,)
    ).fetchone()[0] == "QUEUED"


def test_repeated_tick_at_the_same_instant_is_a_no_op(conn, make_schedule, t0):
    make_schedule(job_type="noop", interval_minutes=60, next_run_at=t0)

    first = ticker.tick(conn, t0)
    second = ticker.tick(conn, t0)

    assert len(first["dispatched"]) == 1
    assert second["dispatched"] == []
    assert conn.execute("SELECT count(*) FROM execution").fetchone()[0] == 1
