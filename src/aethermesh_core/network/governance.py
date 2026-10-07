"""Local, opt-in governance for fixed-size integer routers, not public consensus.

Only directly collected TLS observations count. No model loaders, remote code,
peer-supplied identities/scores, or automatic policy changes are supported.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any, cast

from .client import PeerClient
from .config import validate_fingerprint, validate_project_id
from .evaluation import evaluation_task

Weights = tuple[int, int, int]
Case = tuple[tuple[int, int], int]


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    ).hexdigest()


def _integer(value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("integer outside policy bounds")


def _weights(value: Weights) -> None:
    if not isinstance(value, tuple) or len(value) != 3:
        raise ValueError("a router requires an immutable three-integer weight tuple")
    for weight in value:
        _integer(weight, -1000, 1000)


def _predict(weights: Weights, row: tuple[int, int]) -> int:
    return int(weights[0] * row[0] + weights[1] * row[1] + weights[2] >= 0)


@dataclass(frozen=True)
class GovernancePolicy:
    """Human-configured authority; extra certificates never grant extra votes.

    Independence is an operator assertion, not a discovered fact. The producer
    operator cannot vote. Candidates cannot change this policy.
    """

    project: str
    authorities: tuple[tuple[str, str], ...]
    producer_operator: str
    quorum: int = 2
    microbatch_rows: int = 4
    minimum_gain: int = 1
    max_rounds: int = 16

    def __post_init__(self) -> None:
        validate_project_id(self.project)
        validate_project_id(self.producer_operator)
        _integer(self.quorum, 2, 32)
        _integer(self.microbatch_rows, 1, 32)
        _integer(self.minimum_gain, 1, 64)
        _integer(self.max_rounds, 1, 64)
        if (
            not isinstance(self.authorities, tuple)
            or not 2 <= len(self.authorities) <= 64
        ):
            raise ValueError(
                "authority configuration must be an immutable bounded tuple"
            )
        pins = set()
        operators = set()
        for binding in self.authorities:
            if not isinstance(binding, tuple) or len(binding) != 2:
                raise ValueError(
                    "authority entries must be immutable pin/operator pairs"
                )
            pin, operator = binding
            validate_fingerprint(pin)
            validate_project_id(operator)
            if pin in pins:
                raise ValueError("duplicate authority certificate")
            pins.add(pin)
            if operator != self.producer_operator:
                operators.add(operator)
        if len(operators) < self.quorum:
            raise ValueError(
                "quorum requires distinct configured non-producer operators"
            )

    @property
    def digest(self) -> str:
        """Commit to the full promotion policy, including trust configuration."""
        return _digest(
            {
                "version": "router-governance-v1",
                "project": self.project,
                "authorities": sorted(self.authorities),
                "producer_operator": self.producer_operator,
                "quorum": self.quorum,
                "microbatch_rows": self.microbatch_rows,
                "minimum_gain": self.minimum_gain,
                "max_rounds": self.max_rounds,
            }
        )


@dataclass(frozen=True)
class EvaluationSuite:
    """Coordinator-owned held-out and regression answers, not sent to workers.

    Labels require a trusted independent source. The digest establishes identity,
    not quality or secrecy. Fixed test reuse and feedback can cause overfitting.
    """

    heldout: tuple[Case, ...]
    regression: tuple[Case, ...]

    def __post_init__(self) -> None:
        seen: set[tuple[int, int]] = set()
        for cases in (self.heldout, self.regression):
            if not isinstance(cases, tuple) or not 1 <= len(cases) <= 64:
                raise ValueError("each split requires 1 to 64 immutable cases")
            for case in cases:
                if not isinstance(case, tuple) or len(case) != 2:
                    raise ValueError("cases must be immutable row/label pairs")
                row, label = case
                if not isinstance(row, tuple) or len(row) != 2:
                    raise ValueError("each row requires two immutable features")
                for feature in row:
                    _integer(feature, -1000, 1000)
                _integer(label, 0, 1)
                if row in seen:
                    raise ValueError(
                        "evaluation rows must be unique and splits disjoint"
                    )
                seen.add(row)

    @property
    def digest(self) -> str:
        return _digest(
            {"version": 1, "heldout": self.heldout, "regression": self.regression}
        )


@dataclass(frozen=True)
class TrainingProvenance:
    """Locally asserted source/data hashes, not signatures or publisher authority.

    The caller controls the trainer and checks its output. Nothing here permits
    fetching, installing or executing a candidate's source.
    """

    source_digest: str
    data_digest: str

    def __post_init__(self) -> None:
        validate_fingerprint(self.source_digest)
        validate_fingerprint(self.data_digest)


@dataclass(frozen=True)
class Candidate:
    """Quarantined artifact bound to a baseline, policy and evaluation suite."""

    candidate_id: str
    weights: Weights
    base_id: str
    epoch: int
    round_number: int
    provenance: TrainingProvenance


class GovernedRouter:
    """Bounded in-memory controller; promotion is local, never a wire operation.

    Construction/proposal perform no I/O. ``collect`` makes one bounded request.
    asyncio cancellation stops that request; ``cancel`` abandons its round.
    Each controller has a fresh run identity: prior-run task results cannot be
    reused. Restart loses active state/history. Receipts are audit evidence, not
    signed portable authority or recovery state; no restore API exists. Use one
    event loop. This controller is not thread-safe.
    """

    def __init__(
        self, policy: GovernancePolicy, suite: EvaluationSuite, initial_weights: Weights
    ) -> None:
        _weights(initial_weights)
        self._run_id = secrets.token_hex(32)
        self._policy = policy
        self._suite = suite
        self._active_weights = initial_weights
        self._active_id = _digest(
            {"project": policy.project, "initial": initial_weights}
        )
        self._epoch = 0
        self._round_number = 0
        self._pending: Candidate | None = None
        self._votes: dict[int, dict[str, str]] = {}
        self._previous: list[tuple[str, Weights]] = []
        self._receipts: list[dict[str, Any]] = []

    @property
    def run_id(self) -> str:
        """Fresh controller-run nonce, preventing reuse of prior-run tasks."""
        return self._run_id

    @property
    def active_id(self) -> str:
        return self._active_id

    @property
    def active_weights(self) -> Weights:
        return self._active_weights

    @property
    def pending(self) -> Candidate | None:
        return self._pending

    @property
    def receipts(self) -> tuple[dict[str, Any], ...]:
        """Defensive copies of audit evidence, without raw evaluation answers."""
        return tuple(json.loads(json.dumps(record)) for record in self._receipts)

    def route(self, row: tuple[int, int]) -> int:
        """Use the active router; quarantined candidates never affect routing."""
        if not isinstance(row, tuple) or len(row) != 2:
            raise ValueError("a routing row requires two immutable features")
        for feature in row:
            _integer(feature, -1000, 1000)
        return _predict(self.active_weights, row)

    def propose(self, weights: Weights, provenance: TrainingProvenance) -> Candidate:
        """Admit a local trainer artifact, leaving active routing unchanged."""
        _weights(weights)
        if not isinstance(provenance, TrainingProvenance):
            raise TypeError("validated local training provenance is required")
        if self._pending is not None or self._round_number >= self._policy.max_rounds:
            raise ValueError("pending round or exhausted round budget")
        self._round_number += 1
        candidate_id = _digest(
            {
                "project": self._policy.project,
                "policy": self._policy.digest,
                "suite": self._suite.digest,
                "base": self.active_id,
                "epoch": self._epoch,
                "round": self._round_number,
                "producer": self._policy.producer_operator,
                "run_id": self.run_id,
                "weights": weights,
                "source": provenance.source_digest,
                "data": provenance.data_digest,
            }
        )
        self._pending = Candidate(
            candidate_id,
            weights,
            self.active_id,
            self._epoch,
            self._round_number,
            provenance,
        )
        self._votes = {}
        return self._pending

    def _candidate(self) -> Candidate:
        if self._pending is None:
            raise ValueError("no candidate is pending")
        return self._pending

    @property
    def tasks(self) -> tuple[dict[str, Any], ...]:
        """Fresh wire payloads containing features, never expected labels."""
        candidate = self._candidate()
        tasks = []
        for split, cases in (
            ("heldout", self._suite.heldout),
            ("regression", self._suite.regression),
        ):
            for start in range(0, len(cases), self._policy.microbatch_rows):
                context = _digest(
                    {
                        "candidate": candidate.candidate_id,
                        "policy": self._policy.digest,
                        "suite": self._suite.digest,
                        "split": split,
                        "start": start,
                        "evaluator": "router.evaluate.v1",
                    }
                )
                rows = tuple(
                    row
                    for row, _ in cases[start : start + self._policy.microbatch_rows]
                )
                tasks.append(evaluation_task(context, candidate.weights, rows))
        return tuple(tasks)

    async def collect(self, client: PeerClient, task_index: int) -> None:
        """Use live TLS authority, then verify output before recording a vote.

        This cheap pilot recomputes each prediction. It detects dishonest outputs
        but deliberately makes no scalable large-model verification claim.
        """
        candidate = self._candidate()
        tasks = self.tasks
        _integer(task_index, 0, len(tasks) - 1)
        if client.project_id != self._policy.project:
            raise ValueError("peer session belongs to another project")
        info = client.peer_info
        if info is None:
            raise ValueError("an authenticated connected peer is required")
        operator = dict(self._policy.authorities).get(info.fingerprint)
        if operator is None or operator == self._policy.producer_operator:
            raise ValueError("peer has no independent evaluator authority")
        if operator in self._votes.get(task_index, {}):
            raise ValueError("operator already supplied the microtask")
        task = tasks[task_index]
        result = await client.request("router.evaluate.v1", task, timeout=5.0)
        if (
            self._pending is not candidate
            or candidate.base_id != self.active_id
            or candidate.epoch != self._epoch
        ):
            raise ValueError("stale evaluation round")
        if client.peer_info is not info:
            raise ValueError("authenticated peer session changed")
        expected = [
            _predict(candidate.weights, (row[0], row[1])) for row in task["rows"]
        ]
        predictions = result.get("predictions")
        if (
            set(result) != {"task_id", "predictions"}
            or result["task_id"] != task["task_id"]
            or not isinstance(predictions, list)
            or any(type(item) is not int for item in predictions)
            or predictions != expected
        ):
            raise ValueError("invalid, replayed or dishonest evaluation result")
        votes = self._votes.setdefault(task_index, {})
        if operator in votes:
            raise ValueError("concurrent duplicate operator result")
        votes[operator] = info.fingerprint

    def _record(self, decision: str, reason: str, **details: Any) -> dict[str, Any]:
        candidate = self._candidate()
        record = {
            "version": 1,
            "project": self._policy.project,
            "run_id": self.run_id,
            "candidate_id": candidate.candidate_id,
            "candidate_weights": list(candidate.weights),
            "active_id": self.active_id,
            "active_weights": list(self.active_weights),
            "task_ids": [task["task_id"] for task in self.tasks],
            "previous_receipt": self._receipts[-1]["receipt_digest"]
            if self._receipts
            else None,
            "base_id": candidate.base_id,
            "round": candidate.round_number,
            "epoch": candidate.epoch,
            "policy_digest": self._policy.digest,
            "suite_digest": self._suite.digest,
            "producer_operator": self._policy.producer_operator,
            "source_digest": candidate.provenance.source_digest,
            "training_data_digest": candidate.provenance.data_digest,
            "decision": decision,
            "reason": reason,
            "evaluators": {
                str(index): dict(sorted(votes.items()))
                for index, votes in sorted(self._votes.items())
            },
            **details,
        }
        record["receipt_digest"] = _digest(record)
        self._receipts.append(record)
        self._pending = None
        self._votes = {}
        return cast(dict[str, Any], json.loads(json.dumps(record)))

    def finish(self) -> dict[str, Any]:
        """Apply fixed gates and atomically promote, or retain the baseline."""
        candidate = self._candidate()
        if candidate.base_id != self.active_id or candidate.epoch != self._epoch:
            return self._record("rejected", "stale_baseline")
        if any(
            len(self._votes.get(index, {})) < self._policy.quorum
            for index in range(len(self.tasks))
        ):
            return self._record("rejected", "insufficient_independent_evidence")
        baseline = sum(
            _predict(self.active_weights, row) == label
            for row, label in self._suite.heldout
        )
        score = sum(
            _predict(candidate.weights, row) == label
            for row, label in self._suite.heldout
        )
        regressions = sum(
            _predict(self.active_weights, row) == label
            and _predict(candidate.weights, row) != label
            for row, label in self._suite.regression
        )
        details = {
            "baseline_correct": baseline,
            "candidate_correct": score,
            "heldout_cases": len(self._suite.heldout),
            "regressions": regressions,
        }
        if regressions or score - baseline < self._policy.minimum_gain:
            return self._record("rejected", "quality_gate", **details)
        self._previous.append((self.active_id, self.active_weights))
        self._active_id, self._active_weights = (
            candidate.candidate_id,
            candidate.weights,
        )
        self._epoch += 1
        return self._record("promoted", "policy_passed", **details)

    def cancel(self) -> dict[str, Any]:
        """Abandon a round; completed peer work cannot be undone."""
        return self._record("cancelled", "operator_cancelled")

    def rollback(self, expected_active_id: str) -> dict[str, Any]:
        """Explicit local action, guarded against a stale active revision."""
        if (
            self._pending is not None
            or expected_active_id != self.active_id
            or not self._previous
        ):
            raise ValueError("rollback requires current revision and no pending round")
        previous_id, previous_weights = self._previous.pop()
        record: dict[str, Any] = {
            "version": 1,
            "project": self._policy.project,
            "run_id": self.run_id,
            "decision": "rolled_back",
            "policy_digest": self._policy.digest,
            "suite_digest": self._suite.digest,
            "previous_receipt": self._receipts[-1]["receipt_digest"],
            "restored_weights": list(previous_weights),
            "from": self.active_id,
            "to": previous_id,
            "epoch": self._epoch + 1,
        }
        record["receipt_digest"] = _digest(record)
        self._active_id, self._active_weights = previous_id, previous_weights
        self._epoch += 1
        self._receipts.append(record)
        return cast(dict[str, Any], json.loads(json.dumps(record)))
