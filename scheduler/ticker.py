"""One pass of the periodic work: dispatch, then recover.

Called on an interval by cron, a timer, or a supervising process. How often
is a deployment decision; below a minute buys nothing, since fire times are
truncated to the minute. That caller is assumed unreliable -- it may fire
twice for the same minute, or skip one -- and both cases are handled
downstream.

Dispatch and recovery each open their own transaction rather than sharing
one: they are independent, so a failure in the sweep must not roll back
dispatch work that already succeeded.
"""

from datetime import datetime, timezone

import psycopg

from scheduler import dispatcher, recovery


def tick(conn: psycopg.Connection, now: datetime | None = None) -> dict[str, list[int]]:
    """One tick. Returns the execution ids created and the ids reclaimed."""
    now = now or datetime.now(timezone.utc)
    return {
        "dispatched": dispatcher.create_due_executions(conn, now),
        "reclaimed": recovery.sweep(conn, now),
    }
