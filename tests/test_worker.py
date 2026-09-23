"""Parts 2 and 3 -- claim, execute, retry, recover, fence.

Contains three of the four demos the task requires:
  * 3+ concurrent workers draining a queue with no double-execution
  * a flaky handler succeeding on attempt 3
  * a killed worker's job being recovered and retried
"""

import threading
from datetime import timedelta

from scheduler import db, dispatcher, recovery, worker


def _status(conn, execution_id: int) -> str:
    return conn.execute(
        "SELECT status FROM execution WHERE id = %s", (execution_id,)
    ).fetchone()[0]


def _one_execution(conn, make_schedule, t0, job_type="noop", **kw):
    """Create a schedule, tick once, return the single execution's id."""
    make_schedule(job_type=job_type, next_run_at=t0, **kw)
    created = dispatcher.create_due_executions(conn, t0)
    assert len(created) == 1
    return created[0]


# ---------------------------------------------------------------------------
# Claiming
# ---------------------------------------------------------------------------

def test_claim_returns_none_when_nothing_is_queued(conn, t0):
    assert worker.claim(conn, "w1", t0) is None


def test_claim_takes_a_lease_and_consumes_an_attempt(conn, make_schedule, t0):
    execution_id = _one_execution(conn, make_schedule, t0)

    claimed = worker.claim(conn, "ravi", t0, lease_seconds=300)

    assert claimed["id"] == execution_id
    assert claimed["attempt"] == 1          # incremented at CLAIM, not completion
    assert claimed["claim_token"] is not None

    row = conn.execute(
        "SELECT status, claimed_by, lease_expires_at FROM execution WHERE id = %s",
        (execution_id,),
    ).fetchone()
    assert row[0] == "RUNNING"
    assert row[1] == "ravi"
    assert row[2] == t0 + timedelta(seconds=300)


def test_a_running_execution_cannot_be_claimed_again(conn, make_schedule, t0):
    _one_execution(conn, make_schedule, t0)

    assert worker.claim(conn, "ravi", t0) is not None
    assert worker.claim(conn, "priya", t0) is None      # no longer QUEUED


# ---------------------------------------------------------------------------
# Running
# ---------------------------------------------------------------------------

def test_successful_run_records_result(conn, make_schedule, t0):
    execution_id = _one_execution(conn, make_schedule, t0, job_type="refresh-cache",
                                  payload={"cache": "users"})

    assert worker.run_one(conn, "ravi", t0) == "SUCCESS"

    row = conn.execute(
        "SELECT status, result, finished_at FROM execution WHERE id = %s",
        (execution_id,),
    ).fetchone()
    assert row[0] == "SUCCESS"
    assert row[1] == {"refreshed": "users"}
    assert row[2] == t0


def test_failed_run_is_requeued_with_backoff(conn, make_schedule, t0):
    execution_id = _one_execution(conn, make_schedule, t0, job_type="always-fails")

    assert worker.run_one(conn, "ravi", t0) == "FAILED"

    row = conn.execute(
        "SELECT status, attempt, run_after, last_error, claimed_by FROM execution WHERE id = %s",
        (execution_id,),
    ).fetchone()
    assert row[0] == "QUEUED"                       # back in the queue
    assert row[1] == 1                              # the attempt was consumed
    assert row[2] == t0 + timedelta(seconds=60)     # 1st backoff
    assert "always fails" in row[3]
    assert row[4] is None                           # lease cleared


def test_backoff_gate_hides_the_row_until_run_after(conn, make_schedule, t0):
    _one_execution(conn, make_schedule, t0, job_type="always-fails")
    worker.run_one(conn, "ravi", t0)                # fails -> run_after = t0 + 60s

    assert worker.claim(conn, "ravi", t0 + timedelta(seconds=30)) is None
    assert worker.claim(conn, "ravi", t0 + timedelta(seconds=61)) is not None


def test_attempts_are_exhausted_into_terminal_failed(conn, make_schedule, t0):
    execution_id = _one_execution(conn, make_schedule, t0, job_type="always-fails")

    now = t0
    for _ in range(3):                              # max_attempts defaults to 3
        assert worker.run_one(conn, "ravi", now) == "FAILED"
        now += timedelta(minutes=10)                # step past each backoff

    row = conn.execute(
        "SELECT status, attempt, finished_at FROM execution WHERE id = %s",
        (execution_id,),
    ).fetchone()
    assert row[0] == "FAILED"                       # terminal
    assert row[1] == 3
    assert row[2] is not None

    # nothing left to claim
    assert worker.claim(conn, "ravi", now) is None


# --- REQUIRED DEMO: a flaky handler succeeding on attempt 3 ----------------

def test_flaky_handler_succeeds_on_third_attempt(conn, make_schedule, t0):
    calls = {"n": 0}

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError(f"attempt {calls['n']} failed")
        return {"succeeded_on": calls["n"]}

    registry = {"flaky": flaky}
    execution_id = _one_execution(conn, make_schedule, t0, job_type="flaky")

    # attempt 1 fails  -> backoff 60s
    assert worker.run_one(conn, "w1", t0, registry=registry) == "FAILED"
    # attempt 2 fails  -> backoff 120s
    assert worker.run_one(conn, "w1", t0 + timedelta(seconds=61), registry=registry) == "FAILED"
    # attempt 3 succeeds
    assert worker.run_one(conn, "w1", t0 + timedelta(seconds=200), registry=registry) == "SUCCESS"

    row = conn.execute(
        "SELECT status, result, attempt FROM execution WHERE id = %s", (execution_id,)
    ).fetchone()
    assert row == ("SUCCESS", {"succeeded_on": 3}, 3)

    # the per-attempt history tells the whole story, which the execution cannot
    outcomes = conn.execute(
        "SELECT attempt, outcome FROM execution_attempt WHERE execution_id = %s ORDER BY attempt",
        (execution_id,),
    ).fetchall()
    assert outcomes == [(1, "FAILED"), (2, "FAILED"), (3, "SUCCESS")]


# --- REQUIRED DEMO: a killed worker's job is recovered and retried ---------

def test_killed_worker_is_recovered_by_the_sweep(conn, make_schedule, t0):
    execution_id = _one_execution(conn, make_schedule, t0)

    # Ravi claims it... and is never heard from again.
    claimed = worker.claim(conn, "ravi", t0, lease_seconds=300)
    assert _status(conn, execution_id) == "RUNNING"

    # Before the lease expires, the sweep leaves it alone.
    assert recovery.sweep(conn, t0 + timedelta(seconds=299)) == []
    assert _status(conn, execution_id) == "RUNNING"

    # After it expires, the sweep reclaims it.
    after = t0 + timedelta(seconds=301)
    assert recovery.sweep(conn, after) == [execution_id]

    row = conn.execute(
        "SELECT status, attempt, claimed_by, claim_token FROM execution WHERE id = %s",
        (execution_id,),
    ).fetchone()
    assert row[0] == "QUEUED"
    assert row[1] == 1              # Ravi's attempt was consumed
    assert row[2] is None
    assert row[3] is None           # token cleared -> Ravi is now fenced out

    # The abandoned attempt is recorded in the history.
    assert conn.execute(
        "SELECT outcome FROM execution_attempt WHERE execution_id = %s AND attempt = 1",
        (execution_id,),
    ).fetchone()[0] == "LEASE_EXPIRED"

    # ...and somebody else finishes the job.
    later = after + timedelta(minutes=5)
    assert worker.run_one(conn, "priya", later) == "SUCCESS"
    assert _status(conn, execution_id) == "SUCCESS"


# --- The elevator scenario: a stale worker must not commit ----------------

def test_stale_worker_cannot_commit_over_a_newer_attempt(conn, make_schedule, t0):
    """Ravi was not dead -- only paused. He wakes up after his lease expired
    and after Priya has taken over, and tries to record SUCCESS.

    Fencing keeps the RECORD honest. It cannot stop the handler having run
    twice: that is why invocation is at-least-once, not exactly-once.
    """
    execution_id = _one_execution(conn, make_schedule, t0)

    ravi = worker.claim(conn, "ravi", t0, lease_seconds=300)
    stale_token = ravi["claim_token"]

    # Ravi's lease expires; the sweep hands the job on.
    after = t0 + timedelta(seconds=301)
    recovery.sweep(conn, after)
    priya = worker.claim(conn, "priya", after + timedelta(minutes=5))
    assert priya["claim_token"] != stale_token

    # Ravi wakes up and tries to write his result with the OLD token.
    accepted = worker.complete_success(
        conn, execution_id, attempt=1, claim_token=stale_token,
        now=after + timedelta(minutes=6), result={"from": "ravi"},
    )

    assert accepted is False                       # fenced out
    assert _status(conn, execution_id) == "RUNNING"  # still Priya's
    assert conn.execute(
        "SELECT claimed_by FROM execution WHERE id = %s", (execution_id,)
    ).fetchone()[0] == "priya"

    # Priya's result is the one that lands.
    assert worker.complete_success(
        conn, execution_id, attempt=2, claim_token=priya["claim_token"],
        now=after + timedelta(minutes=7), result={"from": "priya"},
    ) is True
    assert conn.execute(
        "SELECT result FROM execution WHERE id = %s", (execution_id,)
    ).fetchone()[0] == {"from": "priya"}


# --- REQUIRED DEMO: N concurrent workers, no double-execution --------------

def test_concurrent_workers_drain_the_queue_without_double_execution(
    conn, make_schedule, t0
):
    """Four workers, twelve jobs, real threads and real connections.

    Proves both halves of the task's requirement: safe (each job runs exactly
    once) AND not serialized (every worker gets work).
    """
    job_count = 12
    worker_count = 4

    for n in range(job_count):
        make_schedule(job_type="counted", next_run_at=t0, payload={"n": n})
    assert len(dispatcher.create_due_executions(conn, t0)) == job_count

    executed: list[int] = []
    who: list[str] = []
    guard = threading.Lock()

    def counted(payload):
        with guard:
            executed.append(payload["n"])
        return {"n": payload["n"]}

    registry = {"counted": counted}

    def drain(worker_id: str):
        with db.connect() as c:
            while True:
                outcome = worker.run_one(c, worker_id, t0, registry=registry)
                if outcome is None:
                    return
                with guard:
                    who.append(worker_id)

    threads = [
        threading.Thread(target=drain, args=(f"worker-{i}",))
        for i in range(worker_count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # every job ran EXACTLY once
    assert sorted(executed) == list(range(job_count))

    # every execution succeeded
    statuses = conn.execute(
        "SELECT status, count(*) FROM execution GROUP BY status"
    ).fetchall()
    assert statuses == [("SUCCESS", job_count)]

    # and the work was actually shared -- not serialized behind one worker
    assert len(set(who)) > 1


def test_only_one_worker_claims_a_single_execution_under_contention(
    conn, make_schedule, t0
):
    """Eight threads, one claimable execution, all racing the same statement.

    The drain test above shows no job ran twice across many rows; this forces
    the specific race the task cares about -- N workers going for the SAME row
    at the same instant -- and asserts exactly one wins.
    """
    _one_execution(conn, make_schedule, t0)

    contenders = 8
    gate = threading.Barrier(contenders)
    winners: list[str] = []
    guard = threading.Lock()

    def try_claim(worker_id: str):
        with db.connect() as c:
            gate.wait()                       # release all threads together
            if worker.claim(c, worker_id, t0) is not None:
                with guard:
                    winners.append(worker_id)

    threads = [
        threading.Thread(target=try_claim, args=(f"worker-{i}",))
        for i in range(contenders)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(winners) == 1
    assert conn.execute(
        "SELECT claimed_by, attempt FROM execution"
    ).fetchone() == (winners[0], 1)


def test_unknown_job_type_fails_terminally_without_burning_retries(
    conn, make_schedule, t0
):
    """A missing handler is a configuration error, not a transient one, so it
    fails on the first attempt rather than after the full retry budget."""
    execution_id = _one_execution(conn, make_schedule, t0, job_type="no-such-handler")

    assert worker.run_one(conn, "w1", t0) == "FAILED"

    row = conn.execute(
        "SELECT status, attempt, last_error FROM execution WHERE id = %s",
        (execution_id,),
    ).fetchone()
    assert row[0] == "FAILED"          # terminal already
    assert row[1] == 1                 # on the first attempt
    assert "UnknownJobType" in row[2]
