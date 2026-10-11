"""Fixed, bounded synthetic-regression trainer/evaluator; no model/code loading.

This trusted local helper receives training rows OR evaluation features, never
held-out labels. Process separation is not an OS sandbox or independent identity.
"""

from __future__ import annotations

import json
import resource
import sys
from fractions import Fraction
from typing import Any


def integer(value: Any, limit: int = 1000000) -> int:
    if type(value) is not int or not -limit <= value <= limit:
        raise ValueError("invalid bounded integer")
    return value


def rational(pair: Any) -> Fraction:
    if not isinstance(pair, list) or len(pair) != 2:
        raise ValueError("invalid rational")
    numerator, denominator = (integer(value) for value in pair)
    if denominator <= 0:
        raise ValueError("invalid denominator")
    result = Fraction(numerator, denominator)
    if pair != [result.numerator, result.denominator]:
        raise ValueError("noncanonical rational")
    return result


def pair(value: Fraction) -> list[int]:
    result = [value.numerator, value.denominator]
    rational(result)
    return result


def features(row: list[int]) -> list[int]:
    x, y = row
    return [1, x, y, x * x, x * y, y * y]


def solve(gram: list[list[int]], rhs: list[int]) -> list[list[int]]:
    """Exact least squares through a fixed six-by-six normal system."""
    matrix = [
        [Fraction(value) for value in row] + [Fraction(target)]
        for row, target in zip(gram, rhs, strict=True)
    ]
    for column in range(6):
        pivot = next((row for row in range(column, 6) if matrix[row][column]), None)
        if pivot is None:
            raise ValueError("rank-deficient training data")
        matrix[column], matrix[pivot] = matrix[pivot], matrix[column]
        scale = matrix[column][column]
        matrix[column] = [value / scale for value in matrix[column]]
        for row in range(6):
            if row != column:
                scale = matrix[row][column]
                matrix[row] = [
                    left - scale * right
                    for left, right in zip(matrix[row], matrix[column], strict=True)
                ]
    return [pair(row[-1]) for row in matrix]


def train(payload: dict[str, Any]) -> dict[str, Any]:
    if set(payload) != {"version", "operation", "rows", "checkpoint"}:
        raise ValueError("invalid training fields")
    rows, checkpoint = payload["rows"], payload["checkpoint"]
    if not isinstance(rows, list) or not 6 <= len(rows) <= 16:
        raise ValueError("training row bound")
    for row in rows:
        if not isinstance(row, list) or len(row) != 3:
            raise ValueError("invalid training row")
        integer(row[0], 6)
        integer(row[1], 6)
        integer(row[2], 1000)
    if not isinstance(checkpoint, dict) or set(checkpoint) != {
        "processed",
        "gram",
        "rhs",
    }:
        raise ValueError("invalid checkpoint fields")
    processed = integer(checkpoint["processed"], 16)
    if not 0 <= processed < len(rows):
        raise ValueError("invalid checkpoint progress")
    gram, rhs = checkpoint["gram"], checkpoint["rhs"]
    if (
        not isinstance(gram, list)
        or len(gram) != 6
        or not isinstance(rhs, list)
        or len(rhs) != 6
    ):
        raise ValueError("invalid checkpoint dimensions")
    for row in gram:
        if not isinstance(row, list) or len(row) != 6:
            raise ValueError("invalid checkpoint matrix")
        for value in row:
            integer(value)
    for value in rhs:
        integer(value)
    end = min(processed + 4, len(rows))
    for x, y, target in rows[processed:end]:
        basis = features([x, y])
        for i in range(6):
            rhs[i] += basis[i] * target
            for j in range(6):
                gram[i][j] += basis[i] * basis[j]
    return {
        "checkpoint": {"processed": end, "gram": gram, "rhs": rhs},
        "model": solve(gram, rhs) if end == len(rows) else None,
    }


def evaluate(payload: dict[str, Any]) -> dict[str, Any]:
    if set(payload) != {"version", "operation", "task_id", "model", "rows"}:
        raise ValueError("invalid evaluation fields")
    model, rows, task_id = payload["model"], payload["rows"], payload["task_id"]
    if (
        not isinstance(task_id, str)
        or len(task_id) != 64
        or any(char not in "0123456789abcdef" for char in task_id)
    ):
        raise ValueError("invalid task ID")
    if not isinstance(model, list) or len(model) != 6:
        raise ValueError("invalid model")
    a, b, c, d, e, f = [rational(value) for value in model]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 16:
        raise ValueError("evaluation row bound")
    predictions = []
    for row in rows:
        if not isinstance(row, list) or len(row) != 2:
            raise ValueError("invalid feature row")
        x, y = (integer(value, 6) for value in row)
        # A separately implemented polynomial evaluation, not coordinator code.
        predictions.append(pair(a + x * (b + d * x + e * y) + y * (c + f * y)))
    return {"task_id": task_id, "predictions": predictions}


def main() -> int:
    resource.setrlimit(resource.RLIMIT_CPU, (2, 2))
    raw = sys.stdin.buffer.read(16385)
    if len(raw) > 16384:
        raise ValueError("input limit")
    payload = json.loads(raw)
    if (
        not isinstance(payload, dict)
        or type(payload.get("version")) is not int
        or payload["version"] != 1
    ):
        raise ValueError("unsupported worker version")
    operation = payload.get("operation")
    if operation == "train":
        result = train(payload)
    elif operation == "evaluate":
        result = evaluate(payload)
    else:
        raise ValueError("unsupported operation")
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"))
    if len(encoded) > 16384:
        raise ValueError("output limit")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
