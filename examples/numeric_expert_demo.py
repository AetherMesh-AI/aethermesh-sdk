"""Recoverable synthetic numeric-expert experiment, outside the SDK runtime.

The local journal and helper processes are not replicated coordination or
independent operators. No peer listener, credential, paid provider, downloaded
code, model loader, or production routing capability is involved.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import sys
from fractions import Fraction
from pathlib import Path
from typing import Any

from aethermesh_core.learning_state import (
    LearningBinding,
    LearningJournal,
    LearningSnapshot,
)

WORKER = Path(__file__).with_name("numeric_expert_worker.py").resolve()
BASELINE = [[3, 1], [2, 1], [-1, 1], [0, 1], [0, 1], [0, 1]]
POLICY = {
    "version": 1,
    "minimum_heldout_sse_gain": 1,
    "maximum_regressed_cases": 0,
    "maximum_candidates": 1,
    "training_batch": 4,
}


def target(x: int, y: int) -> int:
    return 3 + 2 * x - y + x * x + x * y + 2 * y * y


TRAINING = [[x, y, target(x, y)] for x in (-3, -1, 1, 3) for y in (-3, -1, 1, 3)]
HELDOUT = [[x, y, target(x, y)] for x, y in ((2, 1), (1, 2), (-2, 1), (1, -2))]
REGRESSION = [[x, y, target(x, y)] for x, y in ((0, 0), (0, 2), (2, 0), (-2, -2))]


def encode(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("ascii")


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


def decode(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw)
    if not isinstance(value, dict) or encode(value) != raw:
        raise ValueError("state must be canonical JSON object")
    return value


def model_values(model: Any) -> list[Fraction]:
    if not isinstance(model, list) or len(model) != 6:
        raise ValueError("six coefficients required")
    values = []
    for pair in model:
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or any(type(v) is not int or abs(v) > 1000000 for v in pair)
            or pair[1] <= 0
        ):
            raise ValueError("invalid coefficient")
        value = Fraction(*pair)
        if pair != [value.numerator, value.denominator]:
            raise ValueError("noncanonical coefficient")
        values.append(value)
    return values


def predictions(model: Any, rows: list[list[int]]) -> list[Fraction]:
    coefficients = model_values(model)
    return [
        sum(
            (
                coefficient * feature
                for coefficient, feature in zip(
                    coefficients, (1, x, y, x**2, x * y, y**2), strict=True
                )
            ),
            start=Fraction(0),
        )
        for x, y in rows
    ]


def checkpoint(processed: Any) -> dict[str, Any]:
    if (
        type(processed) is not int
        or not 0 <= processed <= len(TRAINING)
        or processed % 4
    ):
        raise ValueError("invalid training progress")
    # Recompute from the frozen training prefix; do not trust trainer counters.
    basis = [(1, x, y, x**2, x * y, y**2) for x, y, _ in TRAINING[:processed]]
    return {
        "processed": processed,
        "gram": [
            [sum(row[i] * row[j] for row in basis) for j in range(6)] for i in range(6)
        ],
        "rhs": [
            sum(
                row[i] * example[2]
                for row, example in zip(basis, TRAINING[:processed], strict=True)
            )
            for i in range(6)
        ],
    }


async def read_bounded(stream: asyncio.StreamReader, limit: int) -> bytes:
    result = bytearray()
    while chunk := await stream.read(min(4096, limit + 1 - len(result))):
        result.extend(chunk)
        if len(result) > limit:
            raise ValueError("worker output limit")
    return bytes(result)


async def run_worker(payload: dict[str, Any], helper: Path = WORKER) -> dict[str, Any]:
    """Run only an explicitly trusted local script, with bounded pipes/deadline.

    The helper argument exists for isolated fault-injection tests. Never supply
    a downloaded script. A child process and CPU limit are not an OS sandbox.
    """
    raw = encode(payload)
    if len(raw) > 16384:
        raise ValueError("worker input limit")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(helper),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=16384,
    )
    readers: list[asyncio.Task[Any]] = []
    try:
        if process.stdin is None or process.stdout is None or process.stderr is None:
            raise RuntimeError("worker pipes unavailable")
        async with asyncio.timeout(5):
            readers = [
                asyncio.create_task(read_bounded(process.stdout, 16384)),
                asyncio.create_task(read_bounded(process.stderr, 4096)),
                asyncio.create_task(process.wait()),
            ]
            process.stdin.write(raw)
            await process.stdin.drain()
            process.stdin.close()
            out, _, code = await asyncio.gather(*readers)
            if code != 0:
                raise ValueError("worker failed")
            value = json.loads(out)
            if not isinstance(value, dict):
                raise TypeError("worker response must be an object")
            return value
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        await process.wait()


class NumericExperiment:
    """One candidate with a frozen evaluator and durable local transitions."""

    def __init__(self, directory: Path, job_id: str, *, create: bool = False) -> None:
        self.worker_sha256 = hashlib.sha256(WORKER.read_bytes()).hexdigest()
        source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.binding = LearningBinding(
            "synthetic-quadratic-expert",
            job_id,
            digest(
                {
                    "policy": POLICY,
                    "training": TRAINING,
                    "trainer": self.worker_sha256,
                    "baseline": BASELINE,
                }
            ),
            digest({"heldout": HELDOUT, "regression": REGRESSION}),
            digest({"coordinator": source_sha256, "worker": self.worker_sha256}),
        )
        if create:
            self.store = LearningJournal.create(
                directory,
                self.binding,
                state=encode(
                    {"phase": "training", "candidate": None, "evidence": None}
                ),
                checkpoint=encode(checkpoint(0)),
            )
        else:
            self.store = LearningJournal(directory, self.binding)
        try:
            current = self.store.snapshot()
            self.verify_snapshot(current)
            self.term = self.store.claim(expected_revision=current.revision).term
        except BaseException:
            self.store.close()
            raise

    def close(self) -> None:
        self.store.close()

    def task(self, model: Any) -> dict[str, Any]:
        model_values(model)
        rows = [row[:2] for row in HELDOUT + REGRESSION]
        task_id = digest(
            {
                "binding": self.binding.canonical,
                "candidate": model,
                "rows": rows,
                "round": 1,
            }
        )
        return {
            "version": 1,
            "operation": "evaluate",
            "task_id": task_id,
            "model": model,
            "rows": rows,
        }

    def assess(self, model: Any, evidence: Any) -> dict[str, Any]:
        task = self.task(model)
        expected = [
            [value.numerator, value.denominator]
            for value in predictions(model, task["rows"])
        ]
        if (
            not isinstance(evidence, dict)
            or set(evidence) != {"task_id", "predictions"}
            or evidence["task_id"] != task["task_id"]
            or encode(evidence["predictions"]) != encode(expected)
        ):
            raise ValueError("unverified or misbound evaluation result")
        baseline = predictions(BASELINE, task["rows"])
        candidate = [Fraction(*pair) for pair in expected]
        labels = [row[2] for row in HELDOUT + REGRESSION]
        errors_before = [
            (value - label) ** 2 for value, label in zip(baseline, labels, strict=True)
        ]
        errors_after = [
            (value - label) ** 2 for value, label in zip(candidate, labels, strict=True)
        ]
        gain = sum(errors_before[:4]) - sum(errors_after[:4])
        regressions = sum(
            after > before
            for before, after in zip(errors_before[4:], errors_after[4:], strict=True)
        )
        return {
            "promote": gain >= POLICY["minimum_heldout_sse_gain"] and regressions == 0,
            "heldout_sse_before": str(sum(errors_before[:4])),
            "heldout_sse_after": str(sum(errors_after[:4])),
            "regressed_cases": regressions,
        }

    def verify_snapshot(self, snapshot: LearningSnapshot) -> dict[str, Any]:
        state, saved = decode(snapshot.state), decode(snapshot.checkpoint)
        if set(state) != {"phase", "candidate", "evidence"} or encode(saved) != encode(
            checkpoint(saved.get("processed"))
        ):
            raise ValueError("invalid recovery state")
        phase, candidate, evidence = (
            state[key] for key in ("phase", "candidate", "evidence")
        )
        if phase == "training":
            if (
                saved["processed"] == len(TRAINING)
                or candidate is not None
                or evidence is not None
            ):
                raise ValueError("invalid training state")
        elif phase in {"evaluating", "promoted", "rejected", "rolled_back"}:
            if saved["processed"] != len(TRAINING):
                raise ValueError("incomplete candidate")
            model_values(candidate)
            if phase == "evaluating":
                if evidence is not None:
                    raise ValueError("premature evidence")
            else:
                assessment = self.assess(candidate, evidence)
                if assessment["promote"] != (phase in {"promoted", "rolled_back"}):
                    raise ValueError("decision violates frozen policy")
        elif phase == "cancelled":
            if evidence is not None:
                raise ValueError("cancelled state has evidence")
            if (candidate is not None) != (saved["processed"] == len(TRAINING)):
                raise ValueError("incomplete cancelled candidate")
            if candidate is not None:
                model_values(candidate)
        else:
            raise ValueError("unsupported state")
        return state

    def commit(
        self,
        previous: LearningSnapshot,
        state: dict[str, Any],
        saved: dict[str, Any],
        evidence: tuple[str, ...] = (),
    ) -> LearningSnapshot:
        def validate(
            current: LearningSnapshot, raw_state: bytes, raw_checkpoint: bytes
        ) -> None:
            decision = state["phase"] in {"promoted", "rejected"}
            expected_evidence = (digest(state["evidence"]),) if decision else ()
            if evidence != expected_evidence:
                raise ValueError("evidence identifier does not bind the decision")
            old = self.verify_snapshot(current)
            new = self.verify_snapshot(
                LearningSnapshot(
                    current.revision + 1, current.term, raw_state, raw_checkpoint
                )
            )
            progress = decode(current.checkpoint)["processed"]
            allowed = {
                "training": {"training", "evaluating", "cancelled"},
                "evaluating": {"promoted", "rejected", "cancelled"},
                "promoted": {"rolled_back"},
            }
            if new["phase"] not in allowed.get(old["phase"], set()):
                raise ValueError("invalid state transition")
            if new["phase"] == "cancelled":
                if (
                    saved != decode(current.checkpoint)
                    or new["candidate"] != old["candidate"]
                ):
                    raise ValueError("cancellation changed work")
            elif old["phase"] == "training":
                if saved["processed"] != progress + 4:
                    raise ValueError("invalid checkpoint advancement")
            elif (
                saved != decode(current.checkpoint)
                or new["candidate"] != old["candidate"]
            ):
                raise ValueError("decision changed candidate")

        return self.store.commit(
            term=self.term,
            expected_revision=previous.revision,
            state=encode(state),
            checkpoint=encode(saved),
            evidence=evidence,
            validate=validate,
        )

    async def step(self) -> dict[str, Any]:
        previous = self.store.snapshot()
        state = self.verify_snapshot(previous)
        saved = decode(previous.checkpoint)
        if hashlib.sha256(WORKER.read_bytes()).hexdigest() != self.worker_sha256:
            raise ValueError("worker source changed")
        if state["phase"] == "training":
            result = await run_worker(
                {
                    "version": 1,
                    "operation": "train",
                    "rows": TRAINING,
                    "checkpoint": saved,
                }
            )
            if set(result) != {"checkpoint", "model"} or encode(
                result["checkpoint"]
            ) != encode(checkpoint(saved["processed"] + 4)):
                raise ValueError("unverified training checkpoint")
            candidate = result["model"]
            done = result["checkpoint"]["processed"] == len(TRAINING)
            if done:
                model_values(candidate)
            elif candidate is not None:
                raise ValueError("premature candidate")
            self.commit(
                previous,
                {
                    "phase": "evaluating" if done else "training",
                    "candidate": candidate,
                    "evidence": None,
                },
                result["checkpoint"],
            )
        elif state["phase"] == "evaluating":
            result = await run_worker(self.task(state["candidate"]))
            decision = self.assess(state["candidate"], result)
            self.commit(
                previous,
                {
                    **state,
                    "phase": "promoted" if decision["promote"] else "rejected",
                    "evidence": result,
                },
                saved,
                (digest(result),),
            )
        else:
            raise ValueError("job is terminal; no automatic new candidate")
        return self.report()

    def cancel(self) -> dict[str, Any]:
        previous = self.store.snapshot()
        state = self.verify_snapshot(previous)
        self.commit(
            previous,
            {**state, "phase": "cancelled", "evidence": None},
            decode(previous.checkpoint),
        )
        return self.report()

    def rollback(self) -> dict[str, Any]:
        previous = self.store.snapshot()
        state = self.verify_snapshot(previous)
        self.commit(
            previous, {**state, "phase": "rolled_back"}, decode(previous.checkpoint)
        )
        return self.report()

    def report(self) -> dict[str, Any]:
        snapshot = self.store.snapshot()
        state = self.verify_snapshot(snapshot)
        return {
            "job_id": self.binding.job_id,
            "phase": state["phase"],
            "revision": snapshot.revision,
            "term": snapshot.term,
            "processed": decode(snapshot.checkpoint)["processed"],
            "active_model": state["candidate"]
            if state["phase"] == "promoted"
            else copy.deepcopy(BASELINE),
            "candidate": state["candidate"],
            "assessment": self.assess(state["candidate"], state["evidence"])
            if state["evidence"] is not None
            else None,
            "evidence_authenticated": False,
            "replicated_coordination": False,
        }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--steps", type=int, choices=range(6), default=5)
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument("--cancel", action="store_true")
    args = parser.parse_args()
    if args.rollback and args.cancel:
        parser.error("choose rollback or cancel")
    experiment = NumericExperiment(
        args.directory.resolve(), args.job_id, create=args.create
    )
    try:
        if args.rollback:
            result = experiment.rollback()
        elif args.cancel:
            result = experiment.cancel()
        else:
            for _ in range(args.steps):
                if experiment.report()["phase"] in {
                    "promoted",
                    "rejected",
                    "cancelled",
                    "rolled_back",
                }:
                    break
                await experiment.step()
            result = experiment.report()
        print(encode(result).decode())
    finally:
        experiment.close()


if __name__ == "__main__":
    asyncio.run(main())
