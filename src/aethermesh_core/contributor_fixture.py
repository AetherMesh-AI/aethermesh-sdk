"""Pure simulated contributor projection; caller events are not live activity proof."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from .artifact_codec import CheckCancel, _cancel, _integer
from .capability_fixture import (
    _decode,
    _digest,
    _fields,
    _identifier,
    _items,
    _namespace,
)

_TRANSITIONS = {
    "selected": {"queued", "executing", "failed", "cancelled"},
    "queued": {"executing", "failed", "cancelled"},
    "executing": {"executing", "completed", "failed", "cancelled"},
    "completed": set(),
    "failed": set(),
    "cancelled": set(),
}


@dataclass(frozen=True)
class FixtureContributors:
    request_id: str
    names: tuple[str, ...]
    simulated: Literal[True] = True
    live_activity_verified: Literal[False] = False


def _routes(
    data: dict[str, Any], namespace: dict[str, str]
) -> dict[str, dict[str, Any]]:
    result = {}
    for row in _items(data["routes"], 32):
        _fields(row, {"request_id", "attempt_id", "artifact", "lineage"})
        attempt = _identifier(row["attempt_id"])
        _digest(row["artifact"])
        lineage = _identifier(row["lineage"])
        if (
            row["request_id"] != data["request_id"]
            or attempt in result
            or lineage not in namespace
        ):
            raise ValueError("conflicting fixture route")
        result[attempt] = row
    return result


def project_contributor_fixture(
    raw: bytes, *, check_cancel: CheckCancel | None = None
) -> FixtureContributors:
    """Project bounded simulated routed events, never advertise live contributors.

    Selected/queued routes alone produce no active name. Only a fresh executing
    event for the exact request, attempt and artifact contributes a family name.
    Inputs are synthetic and confer no execution or routing authority.
    """
    _cancel(check_cancel)
    data = _fields(
        _decode(raw),
        {
            "version",
            "request_id",
            "now",
            "max_event_age",
            "namespace",
            "routes",
            "events",
        },
    )
    _integer(data["version"], 1, 1)
    request = _digest(data["request_id"])
    _integer(data["now"], 1, 4102444800)
    _integer(data["max_event_age"], 1, 3600)
    namespace = _namespace(data["namespace"])
    routes = _routes(data, namespace)
    states = dict.fromkeys(routes, (0, "selected", 0))
    for event in _items(data["events"], 64):
        _cancel(check_cancel)
        _fields(
            event, {"request_id", "attempt_id", "artifact", "sequence", "state", "at"}
        )
        attempt = _identifier(event["attempt_id"])
        _digest(event["artifact"])
        state = _identifier(event["state"])
        _integer(event["sequence"], 1, 64)
        _integer(event["at"], 1, data["now"])
        if (
            attempt not in routes
            or event["request_id"] != request
            or event["artifact"] != routes[attempt]["artifact"]
        ):
            raise ValueError("unbound fixture event")
        sequence, previous, at = states[attempt]
        if (
            event["sequence"] != sequence + 1
            or event["at"] < at
            or state not in _TRANSITIONS[previous]
        ):
            raise ValueError("invalid fixture event order or transition")
        states[attempt] = (event["sequence"], state, event["at"])
    names = {
        namespace[routes[attempt]["lineage"]]
        for attempt, (_, state, at) in states.items()
        if state == "executing" and data["now"] - at <= data["max_event_age"]
    }
    _cancel(check_cancel)
    return FixtureContributors(request, tuple(sorted(names)))
