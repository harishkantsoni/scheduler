"""Run one worker: claim executions and run their handlers, forever.

    python run_worker.py [worker_id]

Start several in separate terminals. They all claim from the same QUEUED
executions, and two never hold the same one at once.

Killing one mid-handler is worth trying: it records no outcome, its lease
expires, and the next sweep requeues the execution for someone else.
"""

import os
import sys
import time
from datetime import datetime, timezone

from scheduler import db, worker

POLL_SECONDS = 1.0


def main() -> None:
    worker_id = sys.argv[1] if len(sys.argv) > 1 else f"worker-{os.getpid()}"
    print(f"{worker_id} starting", flush=True)

    with db.connect() as conn:
        try:
            while True:
                outcome = worker.run_one(conn, worker_id, datetime.now(timezone.utc))
                if outcome is None:
                    # Nothing claimable. Sleep only when idle, so a backlog
                    # drains at full speed.
                    time.sleep(POLL_SECONDS)
                else:
                    print(f"{worker_id} {outcome}", flush=True)
        except KeyboardInterrupt:
            print(f"{worker_id} stopped", flush=True)


if __name__ == "__main__":
    main()
