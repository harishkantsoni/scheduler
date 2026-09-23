"""Database connection."""

import os

import psycopg

# 5433, not 5432: docker-compose publishes there to avoid clashing with a
# Postgres installed natively on the host.
DEFAULT_DSN = "postgresql://scheduler:scheduler@localhost:5433/scheduler"

DSN = os.environ.get("SCHEDULER_DSN", DEFAULT_DSN)


def connect(dsn: str | None = None) -> psycopg.Connection:
    """Open a connection. Each process opens its own; nothing is shared.

    autocommit=True, so transactions are only ever opened explicitly, with
    `with conn.transaction():`. A row lock lasts exactly as long as that
    block, which keeps lock lifetimes visible instead of implicit.
    """
    return psycopg.connect(dsn or DSN, autocommit=True)
