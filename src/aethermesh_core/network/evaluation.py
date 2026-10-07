"""Opt-in, bounded integer router evaluation with content-addressed requests.

This fixed two-feature calculation cannot load code, train models, or choose
what a peer contributes. Operators must supply an explicit lifetime budget.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from typing import Any

from .errors import ProtocolError, RemoteError

ROUTER_EVALUATION_OPERATION = "router.evaluate.v1"
_MAX_ROWS = 32
_MAX_TOTAL_ROWS = 4096


@dataclass(frozen=True)
class EvaluationBudget:
    """Immutable per-request and cumulative row limits, with global ceilings."""

    max_rows: int = 8
    max_total_rows: int = 256

    def __post_init__(self) -> None:
        for value, ceiling in (
            (self.max_rows, _MAX_ROWS),
            (self.max_total_rows, _MAX_TOTAL_ROWS),
        ):
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError("evaluation budget must use bounded positive integers")


def _sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _validate_content(content: dict[str, Any], max_rows: int) -> None:
    if not _sha256(content["context"]):
        raise RemoteError("invalid_request", "invalid evaluation context")
    weights = content["weights"]
    if (
        not isinstance(weights, list)
        or len(weights) != 3
        or any(
            type(value) is not int or not -1000 <= value <= 1000 for value in weights
        )
    ):
        raise RemoteError("invalid_request", "invalid evaluation weights")
    rows = content["rows"]
    if not isinstance(rows, list) or not rows:
        raise RemoteError("invalid_request", "invalid evaluation rows")
    if len(rows) > max_rows:
        raise RemoteError("resource_limit", "evaluation request row limit reached")
    for row in rows:
        if (
            not isinstance(row, list)
            or len(row) != 2
            or any(
                type(value) is not int or not -1000 <= value <= 1000 for value in row
            )
        ):
            raise RemoteError("invalid_request", "invalid evaluation row")


def _task_id(content: dict[str, Any]) -> str:
    canonical = json.dumps(
        content, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def _validate_request(payload: object, max_rows: int) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {
        "task_id",
        "context",
        "weights",
        "rows",
    }:
        raise RemoteError("invalid_request", "invalid evaluation request fields")
    if not _sha256(payload["task_id"]):
        raise RemoteError("invalid_request", "invalid evaluation task identifier")
    content = {key: payload[key] for key in ("context", "weights", "rows")}
    _validate_content(content, max_rows)
    if payload["task_id"] != _task_id(content):
        raise RemoteError(
            "invalid_request", "evaluation task identifier does not match"
        )
    return content


def evaluation_task(
    context: str,
    weights: tuple[int, int, int],
    rows: tuple[tuple[int, int], ...],
) -> dict[str, Any]:
    """Build a valid content-addressed request; no peer budget is reserved.

    ``context`` is a lowercase SHA-256 digest binding the caller's task context.
    Invalid inputs raise ``ValueError``. A peer can set stricter row limits.
    """
    if not isinstance(weights, tuple) or len(weights) != 3:
        raise ValueError("weights must be a three-integer tuple")
    if not isinstance(rows, tuple) or not 1 <= len(rows) <= _MAX_ROWS:
        raise ValueError("rows must be a nonempty tuple of at most 32 pairs")
    if any(not isinstance(row, tuple) or len(row) != 2 for row in rows):
        raise ValueError("rows must contain two-integer tuples")
    content = {
        "context": context,
        "weights": list(weights),
        "rows": [list(row) for row in rows],
    }
    try:
        _validate_content(content, _MAX_ROWS)
    except RemoteError as exc:
        raise ValueError("invalid evaluation task content") from exc
    return {"task_id": _task_id(content), **content}


def validate_evaluation_result(result: dict[str, Any], request: dict[str, Any]) -> None:
    """Reject malformed or miscorrelated peer results, without trusting predictions.

    This checks the response contract only. Independent evaluation is necessary
    before treating returned predictions as evidence for a learning decision.
    """
    try:
        content = _validate_request(request, _MAX_ROWS)
    except RemoteError as exc:
        raise ProtocolError(
            "The peer returned a result for an invalid evaluation request."
        ) from exc
    predictions = result.get("predictions")
    if (
        set(result) != {"task_id", "predictions"}
        or result["task_id"] != request["task_id"]
        or not isinstance(predictions, list)
        or len(predictions) != len(content["rows"])
        or any(type(value) is not int or value not in (0, 1) for value in predictions)
    ):
        raise ProtocolError("The peer returned an invalid evaluation result.")


class RouterEvaluator:
    """A fixed integer evaluator with an explicit, shared lifetime row budget.

    Construction performs no evaluation. Passing this object to ``PeerService``
    opts into experimental ``router.evaluate.v1`` for upgraded protocol-v2 clients.
    Legacy v1 sessions never advertise or run this operation. Reconnecting clients
    or restarting a service does not refill the evaluator's cumulative budget.
    """

    def __init__(self, budget: EvaluationBudget) -> None:
        if not isinstance(budget, EvaluationBudget):
            raise TypeError("budget must be an EvaluationBudget")
        self._budget = budget
        self._rows_used = 0
        self._lock = threading.Lock()

    @property
    def budget(self) -> EvaluationBudget:
        """The immutable limits explicitly chosen by the operator."""
        return self._budget

    @property
    def rows_used(self) -> int:
        """Cumulative rows reserved for valid requests, including repeated tasks."""
        with self._lock:
            return self._rows_used

    @property
    def rows_remaining(self) -> int:
        """Remaining lifetime row allowance; it is never reset by a session."""
        return self._budget.max_total_rows - self.rows_used

    def evaluate(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Validate, reserve bounded work, and return the exact v1 result shape.

        Invalid requests consume no budget. Valid repeated requests each consume
        their row count; the content address does not promise replay caching.
        """
        content = _validate_request(payload, self._budget.max_rows)
        rows = content["rows"]
        with self._lock:
            if self._rows_used + len(rows) > self._budget.max_total_rows:
                raise RemoteError(
                    "resource_limit", "evaluation total row limit reached"
                )
            self._rows_used += len(rows)
        w0, w1, bias = content["weights"]
        predictions = [int(w0 * x0 + w1 * x1 + bias >= 0) for x0, x1 in rows]
        return {"task_id": payload["task_id"], "predictions": predictions}
