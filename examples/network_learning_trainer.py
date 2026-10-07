"""Bounded toy CPU trainer; stdin contains training examples only.

This separate process imports no coordinator code and has no evaluation split
in its input. It is a data-flow boundary, not an operating-system sandbox.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from typing import Any


def train(payload: dict[str, Any]) -> dict[str, Any]:
    """Fit three integer perceptron weights in at most sixteen passes."""
    if set(payload) != {"rows"}:
        raise ValueError("only training rows are accepted")
    rows = payload["rows"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 32:
        raise ValueError("training requires 1 to 32 examples")
    for row in rows:
        if (
            not isinstance(row, list)
            or len(row) != 3
            or any(type(value) is not int for value in row)
            or any(not -10 <= feature <= 10 for feature in row[:2])
            or row[2] not in (0, 1)
        ):
            raise ValueError("invalid bounded training example")
    weights = [0, 0, -1]
    updates = 0
    epochs = 0
    for epoch in range(16):
        epochs = epoch + 1
        mistakes = 0
        for x0, x1, label in rows:
            prediction = int(weights[0] * x0 + weights[1] * x1 + weights[2] >= 0)
            error = label - prediction
            if error:
                weights = [
                    weights[0] + error * x0,
                    weights[1] + error * x1,
                    weights[2] + error,
                ]
                mistakes += 1
                updates += 1
        if not mistakes:
            break
    if any(not -1000 <= weight <= 1000 for weight in weights):
        raise ValueError("training exceeded the fixed router weight limits")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "weights": weights,
        "training_data_digest": hashlib.sha256(canonical.encode()).hexdigest(),
        "training_rows": len(rows),
        "training_correct": sum(
            int(weights[0] * x0 + weights[1] * x1 + weights[2] >= 0) == label
            for x0, x1, label in rows
        ),
        "epochs": epochs,
        "updates": updates,
        "pid": os.getpid(),
    }


def main() -> int:
    raw = sys.stdin.buffer.read(4097)
    if len(raw) > 4096:
        raise ValueError("training input exceeds 4096 bytes")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise TypeError("training input must be an object")
    print(json.dumps(train(payload), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
