"""Shared pytest fixtures — the test harness.

We are the ticker. These fixtures give each test a clean database and a
made-up clock, so a test can fire two ticks at the same instant or jump an
hour forward without waiting for anything.

Tests run against the REAL Postgres from docker-compose. Nothing is mocked:
the properties under test (unique constraints, row locks, transaction
visibility) only exist in a real database, so a fake one would prove nothing.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest
from psycopg.types.json import Jsonb

from scheduler import db

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"

# The fixed instant every test builds its timeline from. Any value works;
# what matters is that it is OURS, not the wall clock.
T0 = datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session", autouse=True)
def _schema():
    """Create the tables once, before any test runs."""
    with db.connect() as conn:
        conn.execute(SCHEMA_PATH.read_text())


@pytest.fixture
def conn():
    """A connection whose tables are empty.

    Truncating before each test keeps tests independent — no test can pass
    or fail because of rows another test left behind. RESTART IDENTITY also
    resets the id sequences, so ids are predictable.
    """
    with db.connect() as c:
        c.execute("TRUNCATE execution, schedule RESTART IDENTITY CASCADE")
        yield c


@pytest.fixture
def t0():
    """The base instant. Tests express everything as t0 + timedelta(...)."""
    return T0


@pytest.fixture
def make_schedule(conn):
    """Insert a schedule and return its id."""

    def _make(
        job_type: str = "noop",
        interval_minutes: int = 5,
        next_run_at: datetime = T0,
        enabled: bool = True,
        payload: dict | None = None,
    ) -> int:
        row = conn.execute(
            """
            INSERT INTO schedule (job_type, payload, interval_minutes, enabled, next_run_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (job_type, Jsonb(payload or {}), interval_minutes, enabled, next_run_at),
        ).fetchone()
        return row[0]

    return _make
