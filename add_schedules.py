"""Add the demo schedules.

    python add_schedules.py

Schedule management is an explicit non-goal of the exercise, so this is just
a seeding convenience -- the scheduler itself only ever reads the table.

Does nothing if any schedules already exist, so it is safe to re-run.
"""

from psycopg.types.json import Jsonb

from scheduler import db

# (job_type, payload, interval_minutes)
SCHEDULES = [
    ("refresh-cache", {"cache": "users"}, 1),
    ("archive-rows", {"table": "events"}, 1),
    ("always-fails", {}, 2),
]


def main() -> None:
    with db.connect() as conn:
        existing = conn.execute("SELECT count(*) FROM schedule").fetchone()[0]
        if existing:
            print(f"{existing} schedule(s) already present, nothing added")
            return

        for job_type, payload, interval_minutes in SCHEDULES:
            conn.execute(
                """
                INSERT INTO schedule (job_type, payload, interval_minutes, next_run_at)
                VALUES (%s, %s, %s, now())
                """,
                (job_type, Jsonb(payload), interval_minutes),
            )

        print(f"added {len(SCHEDULES)} schedules, all due now")


if __name__ == "__main__":
    main()
