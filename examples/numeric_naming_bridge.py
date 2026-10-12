"""Evaluate existing fixed numeric artifacts and make non-authoritative proposals.

No training, journal mutation, model loading, routing or production promotion.
Only the trusted adjacent numeric evaluator is run; never substitute received code.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import runpy
from dataclasses import asdict
from fractions import Fraction
from pathlib import Path
from typing import Any

from aethermesh_core.capability_fixture import evaluate_naming_fixture, fixture_digest

HERE = Path(__file__).resolve().parent
DEMO = runpy.run_path(str(HERE / "numeric_expert_demo.py"))
PROTOTYPE = "synthetic-quadratic-expert"
# Existing documented fitted coefficients; no solver or trainer is invoked.
FITTED = [[3, 1], [2, 1], [-1, 1], [1, 1], [1, 1], [2, 1]]
REGRESSING = [[4, 1], [2, 1], [-1, 1], [1, 1], [1, 1], [2, 1]]
TAXONOMY = {
    "revision": 1,
    "rules": [
        {
            "label": "numeric",
            "metric": "heldout_sse",
            "minimum": 0,
            "maximum": 250,
            "words": ["Numeric"],
        }
    ],
}


def sources() -> dict[str, str]:
    return {
        name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
        for name in (
            "numeric_expert_worker.py",
            "numeric_expert_demo.py",
            "numeric_naming_bridge.py",
        )
    }


def context() -> dict[str, Any]:
    source_ids = sources()
    if source_ids != LOADED_SOURCES:
        raise ValueError("numeric source changed since loading")
    dataset = fixture_digest(
        {"heldout": DEMO["HELDOUT"], "regression": DEMO["REGRESSION"]}
    )
    baseline = fixture_digest(DEMO["BASELINE"])
    return {
        "prototype": PROTOTYPE,
        "sources": source_ids,
        "baseline": baseline,
        "dataset": dataset,
        "schema": fixture_digest(
            {
                "version": 1,
                "model": "six canonical rational coefficients",
                "input": "two integer features",
                "output": "rational prediction",
            }
        ),
        "evaluation": fixture_digest(
            {
                "version": 1,
                "sources": source_ids,
                "dataset": dataset,
                "baseline": baseline,
                "metrics": "exact heldout SSE and per-case regression count",
            }
        ),
        "taxonomy": fixture_digest(TAXONOMY),
    }


def task(model: Any, binding: dict[str, Any]) -> dict[str, Any]:
    DEMO["model_values"](model)
    rows = [row[:2] for row in DEMO["HELDOUT"] + DEMO["REGRESSION"]]
    identity = fixture_digest(
        {"binding": binding, "artifact": fixture_digest(model), "rows": rows}
    )
    return {
        "version": 1,
        "operation": "evaluate",
        "task_id": identity,
        "model": model,
        "rows": rows,
    }


def verify_output(model: Any, binding: dict[str, Any], result: Any) -> dict[str, Any]:
    if binding != context():
        raise ValueError("numeric evaluation context mismatch")
    request = task(model, binding)
    predicted = DEMO["predictions"](model, request["rows"])
    expected = [[value.numerator, value.denominator] for value in predicted]
    if (
        not isinstance(result, dict)
        or set(result) != {"task_id", "predictions"}
        or result["task_id"] != request["task_id"]
        or DEMO["encode"](result["predictions"]) != DEMO["encode"](expected)
    ):
        raise ValueError(
            "unverified numeric output or exact evaluation binding mismatch"
        )
    return result


async def evaluate(model: Any, binding: dict[str, Any]) -> dict[str, Any]:
    if binding != context():
        raise ValueError("numeric evaluator source changed")
    result = await DEMO["run_worker"](task(model, binding))
    if sources() != binding["sources"]:
        raise ValueError("numeric evaluator source changed")
    return verify_output(model, binding, result)


def errors(result: dict[str, Any]) -> list[Fraction]:
    labels = [row[2] for row in DEMO["HELDOUT"] + DEMO["REGRESSION"]]
    return [
        (Fraction(*pair) - label) ** 2
        for pair, label in zip(result["predictions"], labels, strict=True)
    ]


def metrics(result: dict[str, Any], baseline_result: dict[str, Any]) -> dict[str, Any]:
    before, after = errors(baseline_result), errors(result)
    sse = sum(after[:4], start=Fraction(0))
    # This bounded bridge uses fixed integral artifacts. Never round evidence to
    # fit the fixture's numeric format; broader rational metrics need a contract.
    if sse.denominator != 1:
        raise ValueError("bridge requires integral exact SSE")
    regressed = sum(a > b for a, b in zip(after[4:], before[4:], strict=True))
    return {
        "heldout_sse": {"value": sse.numerator, "uncertainty": 0},
        "regressed_cases": {"value": regressed, "uncertainty": 0},
    }


def proposal(
    model: Any, binding: dict[str, Any], baseline_result: Any, result: Any
) -> dict[str, Any]:
    # Verify actual outputs before deriving report pins; never pin a self-claimed score.
    baseline_result = verify_output(DEMO["BASELINE"], binding, baseline_result)
    result = verify_output(model, binding, result)
    baseline = {
        "artifact": binding["baseline"],
        "parent": None,
        "lineage": PROTOTYPE,
        "schema": binding["schema"],
        "trainer": "historical-numeric-example",
        "intended": "numeric",
    }
    candidate = {
        **baseline,
        "artifact": fixture_digest(model),
        "parent": binding["baseline"],
    }
    shared = {
        key: binding[key]
        for key in ("baseline", "schema", "evaluation", "dataset", "taxonomy")
    }
    shared.update(lineage=PROTOTYPE, evaluator="local-verified-numeric-evaluation")
    reports = {
        "baseline": {
            **shared,
            "artifact": baseline["artifact"],
            "metrics": metrics(baseline_result, baseline_result),
        },
        "candidate": {
            **shared,
            "artifact": candidate["artifact"],
            "metrics": metrics(result, baseline_result),
        },
    }
    policy = {
        key: binding[key] for key in ("baseline", "evaluation", "dataset", "taxonomy")
    }
    policy.update(
        baseline_version=0,
        evaluators=["local-verified-numeric-evaluation"],
        metrics=[
            {
                "name": "heldout_sse",
                "direction": "lower",
                "gain": 1,
                "regression": 0,
                "uncertainty": 0,
                "minimum": 0,
                "maximum": 10**12,
            },
            {
                "name": "regressed_cases",
                "direction": "lower",
                "gain": 0,
                "regression": 0,
                "uncertainty": 0,
                "minimum": 0,
                "maximum": 0,
            },
        ],
    )
    payload = {
        "version": 1,
        "baseline": baseline,
        "candidate": candidate,
        "reports": reports,
        "taxonomy": TAXONOMY,
        "policy": policy,
        "namespace": [{"lineage": PROTOTYPE, "name": "Numeric"}],
        "hints": {
            "training_run_count": 0,
            "self_description": "Existing numeric prototype; evaluation only.",
        },
    }
    pins = {
        "trusted_report_digests": tuple(
            dict.fromkeys(
                fixture_digest(reports[key]) for key in ("baseline", "candidate")
            )
        ),
        "expected_policy_digest": fixture_digest(policy),
    }
    decision = evaluate_naming_fixture(DEMO["encode"](payload), **pins)
    return {
        "decision": asdict(decision),
        "fixture": payload,
        "pins": pins,
        "baseline_output": baseline_result,
        "candidate_output": result,
    }


async def run_bridge() -> dict[str, Any]:
    binding = context()
    baseline_result = await evaluate(copy.deepcopy(DEMO["BASELINE"]), binding)
    proposals = {}
    for name, model in (
        ("existing_fitted", FITTED),
        ("regression_control", REGRESSING),
    ):
        result = await evaluate(copy.deepcopy(model), binding)
        proposals[name] = proposal(model, binding, baseline_result, result)
    return {
        "prototype": PROTOTYPE,
        "binding": binding,
        "proposals": proposals,
        "actual_numeric_evaluation": True,
        "training_performed": False,
        "evidence_authenticated": False,
        "independent_operators_verified": False,
        "public_family_assigned": False,
        "production_promotion": False,
    }


LOADED_SOURCES = sources()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run_bridge()), sort_keys=True, indent=2))
