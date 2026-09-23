"""Timing policy: how long a claim is valid, and how long to wait after a
failure.

Both the worker and the recovery sweep need these, and neither owns the
other, so they live here rather than in either.
"""

from datetime import timedelta

DEFAULT_LEASE_SECONDS = 300
DEFAULT_BACKOFF_BASE_SECONDS = 60


def backoff_delay(
    attempt: int, base_seconds: int = DEFAULT_BACKOFF_BASE_SECONDS
) -> timedelta:
    """1 min, 2 min, 4 min, ...

    Production would add jitter so a fleet failing on one downstream outage
    does not retry in lockstep.
    """
    return timedelta(seconds=base_seconds * (2 ** (attempt - 1)))
