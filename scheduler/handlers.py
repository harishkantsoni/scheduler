"""Handler registry: job_type -> function.

Job logic is out of scope, so these are stubs.

A handler takes the schedule's payload and returns a JSON-serialisable
result. Raising means this attempt failed.

Invocation is at-least-once: a handler may run more than once for the same
execution, because an expired lease cannot tell a dead worker from a slow
one. A handler must therefore be safe to run twice.
"""

from typing import Any, Callable

Handler = Callable[[dict[str, Any]], Any]


class UnknownJobType(LookupError):
    pass


def noop(payload: dict[str, Any]) -> Any:
    return {"ok": True}


def refresh_cache(payload: dict[str, Any]) -> Any:
    return {"refreshed": payload.get("cache", "unknown")}


def archive_rows(payload: dict[str, Any]) -> Any:
    return {"archived": 0, "table": payload.get("table", "unknown")}


def always_fails(payload: dict[str, Any]) -> Any:
    raise RuntimeError("this handler always fails")


REGISTRY: dict[str, Handler] = {
    "noop": noop,
    "refresh-cache": refresh_cache,
    "archive-rows": archive_rows,
    "always-fails": always_fails,
}


def resolve(job_type: str, registry: dict[str, Handler] | None = None) -> Handler:
    """Look up a handler.

    Pass `registry` to use a different set -- tests do this rather than
    mutate the module-level REGISTRY.
    """
    reg = REGISTRY if registry is None else registry
    try:
        return reg[job_type]
    except KeyError:
        raise UnknownJobType(f"no handler registered for job_type {job_type!r}") from None
