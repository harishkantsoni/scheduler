"""Run the ticker: dispatch due schedules and reclaim expired leases, forever.

    python run_ticker.py [interval_seconds]

In production this would be a long-running process, or the body of a cron
entry firing once a minute. Running more than one is safe.
"""

import sys
import time

from scheduler import db, ticker


def main() -> None:
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    print(f"ticker starting, interval={interval}s", flush=True)

    with db.connect() as conn:
        try:
            while True:
                result = ticker.tick(conn)
                if result["dispatched"] or result["reclaimed"]:
                    print(
                        f"dispatched {result['dispatched']}, "
                        f"reclaimed {result['reclaimed']}",
                        flush=True,
                    )
                time.sleep(interval)
        except KeyboardInterrupt:
            print("ticker stopped", flush=True)


if __name__ == "__main__":
    main()
