"""Adversarial local governance checks; real TLS coverage lives in integration tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
import unittest
from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from typing import Any
from unittest.mock import patch

from aethermesh_core.network.errors import RemoteError
from aethermesh_core.network.governance import (
    EvaluationSuite,
    GovernancePolicy,
    GovernedRouter,
    TrainingProvenance,
)
from aethermesh_core.network.profile import HardwareProfile, NodeProfile, PeerInfo

PIN_A = "a" * 64
PIN_B = "b" * 64
PIN_A_ROTATED = "c" * 64
PIN_PRODUCER = "d" * 64
PIN_UNKNOWN = "e" * 64
BASE = (0, 0, -1)
BETTER = (1, 0, 0)
PROVENANCE = TrainingProvenance("1" * 64, "2" * 64)


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    ).hexdigest()


def policy(**changes: Any) -> GovernancePolicy:
    fields: dict[str, Any] = {
        "project": "governed-test",
        "authorities": (
            (PIN_A, "operator-a"),
            (PIN_B, "operator-b"),
            (PIN_A_ROTATED, "operator-a"),
            (PIN_PRODUCER, "producer"),
        ),
        "producer_operator": "producer",
        "microbatch_rows": 2,
    }
    return GovernancePolicy(**(fields | changes))


def suite() -> EvaluationSuite:
    return EvaluationSuite(
        heldout=(((1, 0), 1), ((2, 0), 1), ((-1, 0), 0)),
        regression=(((-2, 0), 0), ((-3, 0), 0), ((3, 0), 1)),
    )


def peer_info(pin: str = PIN_A, node: str = "same-self-reported-node") -> PeerInfo:
    return PeerInfo(pin, 2, NodeProfile(node, "shared-name", HardwareProfile()))


def correct_result(task: dict[str, Any]) -> dict[str, Any]:
    w0, w1, bias = task["weights"]
    return {
        "task_id": task["task_id"],
        "predictions": [int(w0 * x0 + w1 * x1 + bias >= 0) for x0, x1 in task["rows"]],
    }


class FakePeer:
    """Controlled request boundary, never an authentication or transport substitute."""

    def __init__(
        self,
        pin: str = PIN_A,
        *,
        reply: Callable[[dict[str, Any]], dict[str, Any]] = correct_result,
        release: asyncio.Event | None = None,
    ) -> None:
        self.project_id = "governed-test"
        self.peer_info: PeerInfo | None = peer_info(pin)
        self.reply = reply
        self.release = release
        self.entered = asyncio.Event()
        self.requests: list[tuple[str, dict[str, Any], float]] = []

    async def request(
        self, operation: str, payload: dict[str, Any], *, timeout: float
    ) -> dict[str, Any]:
        self.requests.append((operation, json.loads(json.dumps(payload)), timeout))
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        return self.reply(payload)


async def collect_all(router: GovernedRouter) -> tuple[FakePeer, FakePeer]:
    peers = FakePeer(PIN_A), FakePeer(PIN_B)
    for index in range(len(router.tasks)):
        for peer in peers:
            await router.collect(peer, index)
    return peers


class PolicyValidationTests(unittest.TestCase):
    def test_exact_policy_digest_and_authority_order_independence(self) -> None:
        configured = policy()
        expected = digest(
            {
                "version": "router-governance-v1",
                "project": configured.project,
                "authorities": sorted(configured.authorities),
                "producer_operator": configured.producer_operator,
                "quorum": 2,
                "microbatch_rows": 2,
                "minimum_gain": 1,
                "max_rounds": 16,
            }
        )
        self.assertEqual(configured.digest, expected)
        self.assertEqual(
            configured.digest,
            replace(
                configured, authorities=tuple(reversed(configured.authorities))
            ).digest,
        )
        for changes in (
            {"project": "other-project"},
            {"producer_operator": "other-producer"},
            {"microbatch_rows": 1},
            {"minimum_gain": 2},
            {"max_rounds": 15},
            {"authorities": ((PIN_A, "operator-a"), (PIN_B, "new-operator"))},
            {
                "quorum": 3,
                "authorities": configured.authorities + ((PIN_UNKNOWN, "operator-c"),),
            },
        ):
            with self.subTest(changes=changes):
                self.assertNotEqual(
                    configured.digest, replace(configured, **changes).digest
                )

    def test_quorum_counts_configured_operators_not_certificates(self) -> None:
        for authorities in (
            ((PIN_A, "operator-a"), (PIN_B, "operator-a")),
            ((PIN_A, "operator-a"), (PIN_B, "producer")),
            ((PIN_A, "producer"), (PIN_B, "producer")),
        ):
            with (
                self.subTest(authorities=authorities),
                self.assertRaisesRegex(ValueError, "distinct configured non-producer"),
            ):
                policy(authorities=authorities)
        self.assertEqual(policy().quorum, 2)

    def test_authority_structure_is_immutable_bounded_and_unique(self) -> None:
        for authorities in (
            [],
            (),
            ((PIN_A, "operator-a"),),
            tuple((f"{index:064x}", f"operator-{index}") for index in range(65)),
            ([PIN_A, "operator-a"], (PIN_B, "operator-b")),
            ((PIN_A,), (PIN_B, "operator-b")),
            ((PIN_A, "operator-a", "extra"), (PIN_B, "operator-b")),
            ((PIN_A, "operator-a"), (PIN_A, "operator-b")),
            (("invalid-pin", "operator-a"), (PIN_B, "operator-b")),
            ((PIN_A, "invalid operator"), (PIN_B, "operator-b")),
        ):
            with self.subTest(authorities=authorities), self.assertRaises(ValueError):
                policy(authorities=authorities)
        maximum = policy(
            authorities=tuple(
                (f"{index:064x}", f"operator-{index}") for index in range(64)
            ),
            quorum=32,
        )
        self.assertEqual(len(maximum.authorities), 64)
        self.assertEqual(maximum.quorum, 32)

    def test_strict_integer_policy_ranges(self) -> None:
        for field, lower, upper in (
            ("quorum", 2, 32),
            ("microbatch_rows", 1, 32),
            ("minimum_gain", 1, 64),
            ("max_rounds", 1, 64),
        ):
            for value in (True, False, 2.0, "2", None, lower - 1, upper + 1):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    policy(**{field: value})
            if field != "quorum":
                self.assertEqual(getattr(policy(**{field: lower}), field), lower)
                self.assertEqual(getattr(policy(**{field: upper}), field), upper)
        for field in ("project", "producer_operator"):
            for value in ("", "not a project", True):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    policy(**{field: value})

    def test_policy_suite_and_provenance_are_frozen(self) -> None:
        for instance, field, value in (
            (policy(), "quorum", 1),
            (suite(), "heldout", ()),
            (PROVENANCE, "source_digest", PIN_A),
        ):
            with self.subTest(field=field), self.assertRaises(FrozenInstanceError):
                setattr(instance, field, value)


class SuiteAndProvenanceTests(unittest.TestCase):
    def test_suite_digest_binds_features_labels_and_split(self) -> None:
        evaluation = suite()
        self.assertEqual(
            evaluation.digest,
            digest(
                {
                    "version": 1,
                    "heldout": evaluation.heldout,
                    "regression": evaluation.regression,
                }
            ),
        )
        for changed in (
            replace(evaluation, heldout=(((1, 0), 0),) + evaluation.heldout[1:]),
            replace(evaluation, heldout=(((4, 0), 1),) + evaluation.heldout[1:]),
            EvaluationSuite(evaluation.regression, evaluation.heldout),
        ):
            self.assertNotEqual(evaluation.digest, changed.digest)

    def test_split_size_and_immutable_structure(self) -> None:
        for bad_split in (
            [],
            (),
            ((1, 0), 1),
            tuple(((index, 0), 1) for index in range(65)),
            ([[1, 0], 1],),
            (((1, 0),),),
            (((1, 0), 1, "extra"),),
            (([1, 0], 1),),
            (((1,), 1),),
            (((1, 0, 0), 1),),
        ):
            for field in ("heldout", "regression"):
                fields: dict[str, Any] = {
                    "heldout": (((-999, 0), 0),),
                    "regression": (((999, 0), 1),),
                }
                fields[field] = bad_split
                with (
                    self.subTest(field=field, split=bad_split),
                    self.assertRaises(ValueError),
                ):
                    EvaluationSuite(**fields)
        maximum = EvaluationSuite(
            tuple(((index, 0), 1) for index in range(64)),
            tuple(((index, 1), 0) for index in range(64)),
        )
        self.assertEqual(len(maximum.heldout), 64)
        self.assertEqual(len(maximum.regression), 64)

    def test_features_and_labels_reject_nonintegers_and_out_of_range(self) -> None:
        for value in (True, False, 1.0, float("nan"), "1", None, -1001, 1001):
            for position in (0, 1):
                row = [0, 0]
                row[position] = value
                with (
                    self.subTest(value=value, position=position),
                    self.assertRaises(ValueError),
                ):
                    EvaluationSuite(((tuple(row), 1),), (((3, 3), 1),))
        for value in (True, False, 0.0, float("nan"), "1", None, -1, 2):
            with self.subTest(label=value), self.assertRaises(ValueError):
                EvaluationSuite((((1, 0), value),), (((3, 3), 1),))
        accepted = EvaluationSuite((((-1000, 1000), 0),), (((1000, -1000), 1),))
        self.assertEqual(accepted.heldout[0][0], (-1000, 1000))

    def test_duplicate_features_rejected_within_and_between_splits(self) -> None:
        for heldout, regression in (
            ((((1, 0), 1), ((1, 0), 1)), (((2, 0), 0),)),
            ((((1, 0), 1), ((1, 0), 0)), (((2, 0), 0),)),
            ((((1, 0), 1),), (((2, 0), 0), ((2, 0), 1))),
            ((((1, 0), 1),), (((1, 0), 0),)),
        ):
            with (
                self.subTest(heldout=heldout, regression=regression),
                self.assertRaisesRegex(ValueError, "unique and splits disjoint"),
            ):
                EvaluationSuite(heldout, regression)

    def test_provenance_requires_two_strict_sha256_digests(self) -> None:
        self.assertEqual(PROVENANCE.source_digest, "1" * 64)
        self.assertEqual(PROVENANCE.data_digest, "2" * 64)
        for field in ("source_digest", "data_digest"):
            for value in ("", "a" * 63, "a" * 65, "G" * 64, "A" * 64, None, 1, True):
                fields = {"source_digest": "1" * 64, "data_digest": "2" * 64}
                fields[field] = value
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    TrainingProvenance(**fields)


class ProposalAndTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = policy()
        self.suite = suite()
        self.router = GovernedRouter(self.policy, self.suite, BASE)

    def test_initial_baseline_is_deterministic_and_boundary_routing_exact(self) -> None:
        self.assertEqual(
            self.router.active_id,
            digest({"project": self.policy.project, "initial": BASE}),
        )
        self.assertEqual(self.router.active_weights, BASE)
        self.assertIsNone(self.router.pending)
        self.assertEqual(self.router.receipts, ())
        self.assertEqual(self.router.route((1000, -1000)), 0)
        threshold = GovernedRouter(self.policy, self.suite, (1, -1, 0))
        self.assertEqual(threshold.route((0, 0)), 1)
        self.assertEqual(threshold.route((-1, 0)), 0)
        self.assertEqual(threshold.route((1000, -1000)), 1)

    def test_router_weights_and_routing_rows_are_strict_bounded_tuples(self) -> None:
        for bad in (
            [1, 0, 0],
            (),
            (1, 0),
            (1, 0, 0, 0),
            (True, 0, 0),
            (1.0, 0, 0),
            (float("nan"), 0, 0),
            (-1001, 0, 0),
            (0, 1001, 0),
            (0, 0, "0"),
        ):
            with self.subTest(weights=bad), self.assertRaises(ValueError):
                GovernedRouter(self.policy, self.suite, bad)
            with self.subTest(proposal=bad), self.assertRaises(ValueError):
                self.router.propose(bad, PROVENANCE)
            self.assertIsNone(self.router.pending)
        for bad in (
            [1, 0],
            (),
            (1,),
            (1, 0, 0),
            (True, 0),
            (0, 1.0),
            (0, float("nan")),
            (-1001, 0),
            (0, 1001),
        ):
            with self.subTest(row=bad), self.assertRaises(ValueError):
                self.router.route(bad)
        bounded = self.router.propose((-1000, 1000, -1000), PROVENANCE)
        self.assertEqual(bounded.weights, (-1000, 1000, -1000))
        self.assertEqual(bounded.round_number, 1)

    def test_provenance_is_required_before_spending_round_budget(self) -> None:
        for bad in (
            None,
            {},
            {"source_digest": "1" * 64, "data_digest": "2" * 64},
            "a" * 64,
        ):
            with self.subTest(provenance=bad), self.assertRaises(TypeError):
                self.router.propose(BETTER, bad)
            self.assertIsNone(self.router.pending)
        self.assertEqual(self.router.propose(BETTER, PROVENANCE).round_number, 1)

    def test_candidate_hash_binds_every_local_governance_input(self) -> None:
        baseline_id = self.router.active_id
        candidate = self.router.propose(BETTER, PROVENANCE)
        self.assertEqual(
            candidate.candidate_id,
            digest(
                {
                    "project": self.policy.project,
                    "run_id": self.router.run_id,
                    "policy": self.policy.digest,
                    "suite": self.suite.digest,
                    "base": baseline_id,
                    "epoch": 0,
                    "round": 1,
                    "producer": self.policy.producer_operator,
                    "weights": BETTER,
                    "source": PROVENANCE.source_digest,
                    "data": PROVENANCE.data_digest,
                }
            ),
        )
        self.assertIs(self.router.pending, candidate)
        self.assertEqual(candidate.base_id, baseline_id)
        self.assertEqual(candidate.epoch, 0)
        self.assertEqual(candidate.round_number, 1)
        self.assertEqual(candidate.provenance, PROVENANCE)
        self.assertEqual(self.router.active_id, baseline_id)
        self.assertEqual(self.router.route((1, 0)), 0)
        with self.assertRaises(FrozenInstanceError):
            candidate.weights = BASE
        with self.assertRaisesRegex(ValueError, "pending round"):
            self.router.propose((0, 1, 0), PROVENANCE)
        self.assertIs(self.router.pending, candidate)

    @patch(
        "aethermesh_core.network.governance.secrets.token_hex", return_value="0" * 64
    )
    def test_task_ids_change_for_candidate_policy_suite_base_project_and_provenance(
        self,
        token_hex: Any,
    ) -> None:
        self.router = GovernedRouter(self.policy, self.suite, BASE)
        token_hex.assert_called_once_with(32)
        self.router.propose(BETTER, PROVENANCE)
        reference = self.router.tasks[0]["task_id"]
        identical = GovernedRouter(self.policy, self.suite, BASE)
        identical.propose(BETTER, PROVENANCE)
        self.assertEqual(identical.run_id, self.router.run_id)
        self.assertEqual(identical.tasks[0]["task_id"], reference)
        variants = (
            (self.policy, self.suite, BASE, (2, 0, 0), PROVENANCE),
            (replace(self.policy, max_rounds=15), self.suite, BASE, BETTER, PROVENANCE),
            (
                self.policy,
                replace(self.suite, heldout=(((1, 0), 0),) + self.suite.heldout[1:]),
                BASE,
                BETTER,
                PROVENANCE,
            ),
            (self.policy, self.suite, (0, 0, -2), BETTER, PROVENANCE),
            (
                replace(self.policy, project="other-project"),
                self.suite,
                BASE,
                BETTER,
                PROVENANCE,
            ),
            (
                self.policy,
                self.suite,
                BASE,
                BETTER,
                replace(PROVENANCE, source_digest="3" * 64),
            ),
            (
                self.policy,
                self.suite,
                BASE,
                BETTER,
                replace(PROVENANCE, data_digest="4" * 64),
            ),
        )
        for changed_policy, changed_suite, base, weights, provenance in variants:
            with self.subTest(
                policy=changed_policy,
                suite=changed_suite,
                base=base,
                weights=weights,
                provenance=provenance,
            ):
                router = GovernedRouter(changed_policy, changed_suite, base)
                router.propose(weights, provenance)
                self.assertEqual(router.run_id, self.router.run_id)
                self.assertNotEqual(router.tasks[0]["task_id"], reference)
        self.router.cancel()
        self.router.propose(BETTER, PROVENANCE)
        self.assertNotEqual(self.router.tasks[0]["task_id"], reference)

    def test_microtasks_are_exact_content_addresses_without_labels_or_provenance(
        self,
    ) -> None:
        candidate = self.router.propose(BETTER, PROVENANCE)
        tasks = self.router.tasks
        self.assertEqual([len(task["rows"]) for task in tasks], [2, 1, 2, 1])
        self.assertEqual(len({task["task_id"] for task in tasks}), 4)
        self.assertEqual(
            [row for task in tasks for row in task["rows"]],
            [[1, 0], [2, 0], [-1, 0], [-2, 0], [-3, 0], [3, 0]],
        )
        for task, split, start in zip(
            tasks,
            ("heldout", "heldout", "regression", "regression"),
            (0, 2, 0, 2),
            strict=True,
        ):
            self.assertEqual(set(task), {"task_id", "context", "weights", "rows"})
            self.assertEqual(task["weights"], [1, 0, 0])
            self.assertEqual(
                task["context"],
                digest(
                    {
                        "candidate": candidate.candidate_id,
                        "policy": self.policy.digest,
                        "suite": self.suite.digest,
                        "split": split,
                        "start": start,
                        "evaluator": "router.evaluate.v1",
                    }
                ),
            )
            self.assertEqual(
                task["task_id"],
                digest({key: task[key] for key in ("context", "weights", "rows")}),
            )
            self.assertNotIn(PROVENANCE.source_digest, json.dumps(task))
            self.assertNotIn(PROVENANCE.data_digest, json.dumps(task))
        tasks[0]["weights"][0] = 999
        tasks[0]["rows"][0][0] = 999
        tasks[0]["labels"] = [1, 1]
        fresh = self.router.tasks[0]
        self.assertEqual(fresh["weights"], [1, 0, 0])
        self.assertEqual(fresh["rows"], [[1, 0], [2, 0]])
        self.assertNotIn("labels", fresh)
        self.assertEqual(self.router.pending, candidate)

    def test_no_pending_work_has_no_tasks_decision_or_cancel_receipt(self) -> None:
        for action in (
            lambda: self.router.tasks,
            self.router.finish,
            self.router.cancel,
        ):
            with self.assertRaisesRegex(ValueError, "no candidate"):
                action()
        self.assertEqual(self.router.receipts, ())

    def test_round_budget_is_not_refilled_by_cancellation_or_rejection(self) -> None:
        router = GovernedRouter(policy(max_rounds=2), self.suite, BASE)
        first = router.propose(BETTER, PROVENANCE)
        self.assertEqual(first.round_number, 1)
        router.cancel()
        second = router.propose(BETTER, PROVENANCE)
        self.assertEqual(second.round_number, 2)
        self.assertNotEqual(first.candidate_id, second.candidate_id)
        receipt = router.finish()
        self.assertEqual(receipt["reason"], "insufficient_independent_evidence")
        with self.assertRaisesRegex(ValueError, "exhausted round budget"):
            router.propose(BETTER, PROVENANCE)
        self.assertIsNone(router.pending)
        self.assertEqual(router.active_weights, BASE)
        self.assertEqual(len(router.receipts), 2)


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.router = GovernedRouter(policy(), suite(), BASE)
        self.candidate = self.router.propose(BETTER, PROVENANCE)

    async def test_valid_collection_uses_one_bounded_request_and_cannot_promote(
        self,
    ) -> None:
        peer = FakePeer()
        task = self.router.tasks[0]
        await self.router.collect(peer, 0)
        self.assertEqual(peer.requests, [("router.evaluate.v1", task, 5.0)])
        self.assertEqual(self.router.active_weights, BASE)
        self.assertIs(self.router.pending, self.candidate)
        self.assertEqual(self.router.receipts, ())
        receipt = self.router.finish()
        self.assertEqual(receipt["decision"], "rejected")
        self.assertEqual(receipt["reason"], "insufficient_independent_evidence")
        self.assertEqual(receipt["evaluators"], {"0": {"operator-a": PIN_A}})
        self.assertEqual(receipt["candidate_weights"], list(BETTER))
        self.assertEqual(receipt["active_weights"], list(BASE))
        self.assertEqual(receipt["active_id"], self.candidate.base_id)
        self.assertIsNone(receipt["previous_receipt"])

    async def test_unauthenticated_unauthorized_and_producer_cannot_vote(self) -> None:
        for info in (None, peer_info(PIN_UNKNOWN), peer_info(PIN_PRODUCER)):
            peer = FakePeer()
            peer.peer_info = info
            with self.subTest(info=info), self.assertRaises(ValueError):
                await self.router.collect(peer, 0)
            self.assertEqual(peer.requests, [])
        receipt = self.router.finish()
        self.assertEqual(receipt["evaluators"], {})
        self.assertEqual(receipt["reason"], "insufficient_independent_evidence")

    async def test_cross_project_and_invalid_task_indices_fail_before_request(
        self,
    ) -> None:
        peer = FakePeer()
        peer.project_id = "other-project"
        with self.assertRaisesRegex(ValueError, "another project"):
            await self.router.collect(peer, 0)
        peer.project_id = "governed-test"
        for index in (-1, len(self.router.tasks), True, False, 0.0, "0", None):
            with self.subTest(index=index), self.assertRaises(ValueError):
                await self.router.collect(peer, index)
        self.assertEqual(peer.requests, [])
        self.router.cancel()
        with self.assertRaisesRegex(ValueError, "no candidate"):
            await self.router.collect(peer, 0)
        self.assertEqual(peer.requests, [])

    async def test_multiple_certificates_nodes_and_sessions_for_one_operator_count_once(
        self,
    ) -> None:
        first = FakePeer(PIN_A)
        await self.router.collect(first, 0)
        alternate = FakePeer(PIN_A_ROTATED)
        alternate.peer_info = peer_info(PIN_A_ROTATED, "different-node")
        new_session = FakePeer(PIN_A)
        new_session.peer_info = peer_info(PIN_A, "yet-another-node")
        for duplicate in (first, alternate, new_session):
            before = len(duplicate.requests)
            with (
                self.subTest(peer=duplicate.peer_info),
                self.assertRaisesRegex(ValueError, "already supplied"),
            ):
                await self.router.collect(duplicate, 0)
            self.assertEqual(len(duplicate.requests), before)
        receipt = self.router.finish()
        self.assertEqual(receipt["evaluators"], {"0": {"operator-a": PIN_A}})
        self.assertEqual(receipt["decision"], "rejected")

    async def test_same_node_claim_does_not_merge_independent_configured_operators(
        self,
    ) -> None:
        peers = await collect_all(self.router)
        self.assertEqual(peers[0].peer_info.profile, peers[1].peer_info.profile)
        receipt = self.router.finish()
        self.assertEqual(receipt["decision"], "promoted")
        self.assertEqual(
            receipt["evaluators"],
            {
                str(index): {"operator-a": PIN_A, "operator-b": PIN_B}
                for index in range(4)
            },
        )

    async def test_invalid_results_never_record_evidence_and_allow_honest_retry(
        self,
    ) -> None:
        task = self.router.tasks[0]
        invalid = (
            {},
            {"task_id": task["task_id"]},
            {"predictions": [1, 1]},
            {"task_id": task["task_id"], "predictions": [1, 1], "score": 100},
            {"task_id": "0" * 64, "predictions": [1, 1]},
            {"task_id": task["task_id"], "predictions": None},
            {"task_id": task["task_id"], "predictions": (1, 1)},
            {"task_id": task["task_id"], "predictions": [True, True]},
            {"task_id": task["task_id"], "predictions": [1.0, 1.0]},
            {"task_id": task["task_id"], "predictions": [float("nan"), 1]},
            {"task_id": task["task_id"], "predictions": ["1", 1]},
            {"task_id": task["task_id"], "predictions": []},
            {"task_id": task["task_id"], "predictions": [1]},
            {"task_id": task["task_id"], "predictions": [1, 1, 1]},
            {"task_id": task["task_id"], "predictions": [0, 1]},
            {"task_id": task["task_id"], "predictions": [2, 1]},
        )
        for result in invalid:
            with (
                self.subTest(result=result),
                self.assertRaisesRegex(ValueError, "invalid, replayed or dishonest"),
            ):
                await self.router.collect(
                    FakePeer(reply=lambda _task, result=result: result), 0
                )
        await self.router.collect(FakePeer(), 0)
        self.assertEqual(
            self.router.finish()["evaluators"], {"0": {"operator-a": PIN_A}}
        )

    async def test_previous_task_and_cancelled_round_results_cannot_be_replayed(
        self,
    ) -> None:
        previous = correct_result(self.router.tasks[0])
        with self.assertRaisesRegex(ValueError, "replayed"):
            await self.router.collect(FakePeer(reply=lambda _task: previous), 1)
        self.router.cancel()
        candidate = self.router.propose(BETTER, PROVENANCE)
        self.assertNotEqual(candidate.candidate_id, self.candidate.candidate_id)
        with self.assertRaisesRegex(ValueError, "replayed"):
            await self.router.collect(FakePeer(reply=lambda _task: previous), 0)
        self.assertEqual(self.router.finish()["evaluators"], {})

    async def test_fresh_controller_run_identity_prevents_cross_restart_replay(
        self,
    ) -> None:
        restarted = GovernedRouter(policy(), suite(), BASE)
        previous_task = self.router.tasks[0]
        previous_result = correct_result(previous_task)
        restarted_candidate = restarted.propose(BETTER, PROVENANCE)
        self.assertEqual(restarted.active_id, self.router.active_id)
        self.assertRegex(self.router.run_id, r"^[0-9a-f]{64}$")
        self.assertRegex(restarted.run_id, r"^[0-9a-f]{64}$")
        self.assertNotEqual(restarted.run_id, self.router.run_id)
        self.assertNotEqual(
            restarted_candidate.candidate_id, self.candidate.candidate_id
        )
        self.assertNotEqual(restarted.tasks[0]["task_id"], previous_task["task_id"])
        self.assertEqual(restarted.tasks[0]["rows"], previous_task["rows"])
        self.assertEqual(restarted.tasks[0]["weights"], previous_task["weights"])
        with self.assertRaises(AttributeError):
            restarted.run_id = self.router.run_id
        with self.assertRaisesRegex(ValueError, "replayed"):
            await restarted.collect(FakePeer(reply=lambda _task: previous_result), 0)
        receipt = restarted.finish()
        self.assertEqual(receipt["evaluators"], {})
        self.assertEqual(receipt["reason"], "insufficient_independent_evidence")
        self.assertEqual(receipt["run_id"], restarted.run_id)
        self.assertEqual(restarted.active_weights, BASE)

    async def test_concurrent_duplicates_only_one_vote_can_win(self) -> None:
        release = asyncio.Event()
        first = FakePeer(PIN_A, release=release)
        second = FakePeer(PIN_A_ROTATED, release=release)
        requests = [
            asyncio.create_task(self.router.collect(peer, 0))
            for peer in (first, second)
        ]
        await asyncio.gather(first.entered.wait(), second.entered.wait())
        release.set()
        results = await asyncio.gather(*requests, return_exceptions=True)
        self.assertEqual(sum(result is None for result in results), 1)
        failures = [result for result in results if isinstance(result, ValueError)]
        self.assertEqual(len(failures), 1)
        self.assertEqual(str(failures[0]), "concurrent duplicate operator result")
        receipt = self.router.finish()
        self.assertEqual(set(receipt["evaluators"]["0"]), {"operator-a"})
        self.assertIn(receipt["evaluators"]["0"]["operator-a"], (PIN_A, PIN_A_ROTATED))
        self.assertEqual(receipt["decision"], "rejected")

    async def test_changed_peer_or_disconnect_during_collection_cannot_vote(
        self,
    ) -> None:
        for changed in (None, peer_info(PIN_B), peer_info(PIN_A, "changed-node")):
            release = asyncio.Event()
            peer = FakePeer(release=release)
            request = asyncio.create_task(self.router.collect(peer, 0))
            await peer.entered.wait()
            peer.peer_info = changed
            release.set()
            with (
                self.subTest(changed=changed),
                self.assertRaisesRegex(ValueError, "peer session changed"),
            ):
                await request
        self.assertEqual(self.router.finish()["evaluators"], {})

    async def test_reconnected_equal_peer_info_is_a_new_session(self) -> None:
        release = asyncio.Event()
        peer = FakePeer(release=release)
        original = peer.peer_info
        request = asyncio.create_task(self.router.collect(peer, 0))
        await peer.entered.wait()
        peer.peer_info = peer_info()
        self.assertEqual(peer.peer_info, original)
        self.assertIsNot(peer.peer_info, original)
        release.set()
        with self.assertRaisesRegex(ValueError, "peer session changed"):
            await request
        self.assertEqual(self.router.finish()["evaluators"], {})

    async def test_cancel_or_replacement_round_invalidates_inflight_result(
        self,
    ) -> None:
        for replace_round in (False, True):
            router = GovernedRouter(policy(), suite(), BASE)
            original = router.propose(BETTER, PROVENANCE)
            release = asyncio.Event()
            peer = FakePeer(release=release)
            request = asyncio.create_task(router.collect(peer, 0))
            await peer.entered.wait()
            cancelled = router.cancel()
            replacement = router.propose(BETTER, PROVENANCE) if replace_round else None
            release.set()
            with (
                self.subTest(replace_round=replace_round),
                self.assertRaisesRegex(ValueError, "stale evaluation round"),
            ):
                await request
            self.assertEqual(cancelled["candidate_id"], original.candidate_id)
            self.assertEqual(cancelled["evaluators"], {})
            self.assertIs(router.pending, replacement)
            self.assertEqual(router.active_weights, BASE)
            if replacement is not None:
                self.assertEqual(router.finish()["evaluators"], {})

    async def test_baseline_and_epoch_changes_during_collection_are_rejected(
        self,
    ) -> None:
        for field, value in (("_active_id", "f" * 64), ("_epoch", 1)):
            router = GovernedRouter(policy(), suite(), BASE)
            router.propose(BETTER, PROVENANCE)
            release = asyncio.Event()
            peer = FakePeer(release=release)
            request = asyncio.create_task(router.collect(peer, 0))
            await peer.entered.wait()
            setattr(router, field, value)
            release.set()
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "stale evaluation round"),
            ):
                await request
            receipt = router.finish()
            self.assertEqual(receipt["reason"], "stale_baseline")
            self.assertEqual(receipt["evaluators"], {})
            self.assertEqual(router.active_weights, BASE)

    async def test_async_cancellation_and_peer_failure_do_not_create_votes(
        self,
    ) -> None:
        release = asyncio.Event()
        peer = FakePeer(release=release)
        request = asyncio.create_task(self.router.collect(peer, 0))
        await peer.entered.wait()
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertIs(self.router.pending, self.candidate)

        def failed(_task: dict[str, Any]) -> dict[str, Any]:
            raise RemoteError("resource_limit", "operator budget exhausted")

        with self.assertRaises(RemoteError) as error:
            await self.router.collect(FakePeer(reply=failed), 0)
        self.assertEqual(error.exception.code, "resource_limit")
        receipt = self.router.cancel()
        self.assertEqual(receipt["decision"], "cancelled")
        self.assertEqual(receipt["reason"], "operator_cancelled")
        self.assertEqual(receipt["evaluators"], {})
        self.assertIsNone(self.router.pending)
        self.assertEqual(self.router.active_weights, BASE)


class PromotionAndRollbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_promotion_changes_routing_only_after_complete_independent_evidence(
        self,
    ) -> None:
        configured, evaluation = policy(), suite()
        router = GovernedRouter(configured, evaluation, BASE)
        base_id = router.active_id
        candidate = router.propose(BETTER, PROVENANCE)
        task_ids = [task["task_id"] for task in router.tasks]
        peers = await collect_all(router)
        self.assertEqual([len(peer.requests) for peer in peers], [4, 4])
        self.assertEqual(router.route((1, 0)), 0)
        self.assertEqual(router.active_id, base_id)
        receipt = router.finish()
        self.assertEqual(receipt["decision"], "promoted")
        self.assertEqual(receipt["reason"], "policy_passed")
        self.assertEqual(receipt["baseline_correct"], 1)
        self.assertEqual(receipt["candidate_correct"], 3)
        self.assertEqual(receipt["heldout_cases"], 3)
        self.assertEqual(receipt["regressions"], 0)
        self.assertEqual(receipt["candidate_id"], candidate.candidate_id)
        self.assertEqual(receipt["candidate_weights"], list(BETTER))
        self.assertEqual(receipt["active_weights"], list(BETTER))
        self.assertEqual(receipt["active_id"], candidate.candidate_id)
        self.assertEqual(receipt["task_ids"], task_ids)
        self.assertIsNone(receipt["previous_receipt"])
        self.assertEqual(receipt["base_id"], base_id)
        self.assertEqual(receipt["policy_digest"], configured.digest)
        self.assertEqual(receipt["suite_digest"], evaluation.digest)
        self.assertEqual(receipt["source_digest"], PROVENANCE.source_digest)
        self.assertEqual(receipt["training_data_digest"], PROVENANCE.data_digest)
        self.assertEqual(receipt["producer_operator"], "producer")
        self.assertEqual(receipt["epoch"], 0)
        self.assertEqual(receipt["run_id"], router.run_id)
        self.assertEqual(receipt["round"], 1)
        self.assertEqual(
            receipt["receipt_digest"],
            digest(
                {
                    key: value
                    for key, value in receipt.items()
                    if key != "receipt_digest"
                }
            ),
        )
        self.assertEqual(router.active_id, candidate.candidate_id)
        self.assertEqual(router.active_weights, BETTER)
        self.assertEqual(router.route((1, 0)), 1)
        self.assertIsNone(router.pending)
        self.assertEqual(router.receipts, (receipt,))
        with self.assertRaisesRegex(ValueError, "no candidate"):
            router.finish()
        serialized = json.dumps(receipt)
        for private in ('heldout"', 'regression"', "predictions", "labels", "rows"):
            self.assertNotIn(private, serialized)

    async def test_receipts_are_defensive_deep_copies(self) -> None:
        router = GovernedRouter(policy(), suite(), BASE)
        router.propose(BETTER, PROVENANCE)
        await collect_all(router)
        receipt = router.finish()
        expected = json.loads(json.dumps(receipt))
        receipt["decision"] = "forged"
        receipt["evaluators"]["0"]["operator-a"] = PIN_UNKNOWN
        receipt["candidate_weights"][0] = 999
        receipt["active_weights"][0] = 999
        receipt["task_ids"].clear()
        copied = router.receipts[0]
        copied["evaluators"].clear()
        copied["source_digest"] = PIN_UNKNOWN
        self.assertEqual(router.receipts, (expected,))
        self.assertEqual(router.active_weights, BETTER)

    async def test_missing_task_or_independent_vote_rejects_entire_round(self) -> None:
        for missing in ("task", "operator", "all"):
            router = GovernedRouter(policy(), suite(), BASE)
            original_id = router.active_id
            router.propose(BETTER, PROVENANCE)
            indices = (
                range(len(router.tasks) - (missing == "task"))
                if missing != "all"
                else ()
            )
            for index in indices:
                await router.collect(FakePeer(PIN_A), index)
                if missing != "operator":
                    await router.collect(FakePeer(PIN_B), index)
            receipt = router.finish()
            with self.subTest(missing=missing):
                self.assertEqual(receipt["decision"], "rejected")
                self.assertEqual(receipt["reason"], "insufficient_independent_evidence")
                self.assertEqual(router.active_id, original_id)
                self.assertEqual(router.active_weights, BASE)
                self.assertIsNone(router.pending)

    async def test_no_gain_and_below_minimum_gain_cannot_promote(self) -> None:
        for weights, minimum, expected_score in (
            (BASE, 1, 1),
            (BETTER, 3, 3),
            ((0, 0, 0), 1, 2),
        ):
            router = GovernedRouter(policy(minimum_gain=minimum), suite(), BASE)
            original_id = router.active_id
            router.propose(weights, PROVENANCE)
            await collect_all(router)
            receipt = router.finish()
            with self.subTest(weights=weights, minimum=minimum):
                self.assertEqual(receipt["decision"], "rejected")
                self.assertEqual(receipt["reason"], "quality_gate")
                self.assertEqual(receipt["candidate_correct"], expected_score)
                self.assertEqual(router.active_id, original_id)
                self.assertEqual(router.active_weights, BASE)

    async def test_per_example_regression_rejected_despite_equal_total_regression_accuracy(
        self,
    ) -> None:
        evaluation = EvaluationSuite(
            heldout=(((3, 0), 1), ((4, 0), 1)),
            regression=(((1, 0), 0), ((2, 0), 1)),
        )
        router = GovernedRouter(policy(), evaluation, BASE)
        original_id = router.active_id
        router.propose(BETTER, PROVENANCE)
        await collect_all(router)
        receipt = router.finish()
        self.assertEqual(receipt["baseline_correct"], 0)
        self.assertEqual(receipt["candidate_correct"], 2)
        self.assertEqual(receipt["regressions"], 1)
        self.assertEqual(receipt["decision"], "rejected")
        self.assertEqual(receipt["reason"], "quality_gate")
        self.assertEqual(router.active_id, original_id)
        self.assertEqual(router.route((1, 0)), 0)

    async def test_cancelled_fully_evaluated_candidate_never_changes_routing(
        self,
    ) -> None:
        router = GovernedRouter(policy(), suite(), BASE)
        original_id = router.active_id
        router.propose(BETTER, PROVENANCE)
        await collect_all(router)
        receipt = router.cancel()
        self.assertEqual(receipt["decision"], "cancelled")
        self.assertEqual(len(receipt["evaluators"]), 4)
        self.assertEqual(router.active_id, original_id)
        self.assertEqual(router.route((1, 0)), 0)
        with self.assertRaisesRegex(ValueError, "no candidate"):
            router.finish()
        next_candidate = router.propose(BETTER, PROVENANCE)
        self.assertEqual(next_candidate.round_number, 2)
        self.assertEqual(router.finish()["evaluators"], {})

    async def test_guarded_rollback_restores_route_and_advances_epoch_without_refilling_budget(
        self,
    ) -> None:
        router = GovernedRouter(policy(max_rounds=2), suite(), BASE)
        original_id = router.active_id
        with self.assertRaisesRegex(ValueError, "rollback requires"):
            router.rollback(original_id)
        candidate = router.propose(BETTER, PROVENANCE)
        with self.assertRaisesRegex(ValueError, "rollback requires"):
            router.rollback(original_id)
        await collect_all(router)
        router.finish()
        with self.assertRaisesRegex(ValueError, "rollback requires"):
            router.rollback(original_id)
        self.assertEqual(router.route((1, 0)), 1)
        rollback = router.rollback(candidate.candidate_id)
        self.assertEqual(rollback["version"], 1)
        self.assertEqual(rollback["project"], "governed-test")
        self.assertEqual(rollback["decision"], "rolled_back")
        self.assertEqual(rollback["from"], candidate.candidate_id)
        self.assertEqual(rollback["to"], original_id)
        self.assertEqual(rollback["epoch"], 2)
        self.assertEqual(rollback["run_id"], router.run_id)
        self.assertEqual(rollback["restored_weights"], list(BASE))
        self.assertEqual(rollback["policy_digest"], policy(max_rounds=2).digest)
        self.assertEqual(rollback["suite_digest"], suite().digest)
        self.assertEqual(
            rollback["previous_receipt"], router.receipts[0]["receipt_digest"]
        )
        self.assertEqual(
            rollback["receipt_digest"],
            digest(
                {
                    key: value
                    for key, value in rollback.items()
                    if key != "receipt_digest"
                }
            ),
        )
        self.assertEqual(router.active_id, original_id)
        self.assertEqual(router.active_weights, BASE)
        self.assertEqual(router.route((1, 0)), 0)
        self.assertEqual(len(router.receipts), 2)
        rollback["to"] = "forged"
        rollback["restored_weights"][0] = 999
        self.assertEqual(router.receipts[1]["to"], original_id)
        self.assertEqual(router.receipts[1]["restored_weights"], list(BASE))
        with self.assertRaisesRegex(ValueError, "rollback requires"):
            router.rollback(original_id)
        next_candidate = router.propose(BETTER, PROVENANCE)
        self.assertEqual(next_candidate.epoch, 2)
        self.assertEqual(next_candidate.round_number, 2)
        self.assertEqual(next_candidate.base_id, original_id)
        self.assertNotEqual(next_candidate.candidate_id, candidate.candidate_id)
        cancellation = router.cancel()
        self.assertEqual(
            cancellation["previous_receipt"], router.receipts[1]["receipt_digest"]
        )
        with self.assertRaisesRegex(ValueError, "exhausted round budget"):
            router.propose(BETTER, PROVENANCE)

    async def test_exact_minimum_gain_is_sufficient_without_regression(self) -> None:
        router = GovernedRouter(policy(minimum_gain=2), suite(), BASE)
        router.propose(BETTER, PROVENANCE)
        await collect_all(router)
        receipt = router.finish()
        self.assertEqual(receipt["candidate_correct"] - receipt["baseline_correct"], 2)
        self.assertEqual(receipt["regressions"], 0)
        self.assertEqual(receipt["decision"], "promoted")

    async def test_larger_quorum_needs_every_configured_independent_operator(
        self,
    ) -> None:
        configured = policy(
            quorum=3, authorities=policy().authorities + ((PIN_UNKNOWN, "operator-c"),)
        )
        for complete in (False, True):
            router = GovernedRouter(configured, suite(), BASE)
            router.propose(BETTER, PROVENANCE)
            await collect_all(router)
            if complete:
                for index in range(len(router.tasks)):
                    await router.collect(FakePeer(PIN_UNKNOWN), index)
            receipt = router.finish()
            with self.subTest(complete=complete):
                self.assertEqual(
                    receipt["decision"], "promoted" if complete else "rejected"
                )
                self.assertEqual(
                    receipt["reason"],
                    "policy_passed"
                    if complete
                    else "insufficient_independent_evidence",
                )
                self.assertEqual(len(receipt["evaluators"]["0"]), 3 if complete else 2)

    async def test_decision_receipt_chain_binds_cancel_reject_and_promote(self) -> None:
        router = GovernedRouter(policy(), suite(), BASE)
        router.propose(BETTER, PROVENANCE)
        cancelled = router.cancel()
        router.propose(BETTER, PROVENANCE)
        rejected = router.finish()
        router.propose(BETTER, PROVENANCE)
        await collect_all(router)
        promoted = router.finish()
        self.assertEqual(
            [record["decision"] for record in router.receipts],
            ["cancelled", "rejected", "promoted"],
        )
        self.assertIsNone(cancelled["previous_receipt"])
        self.assertEqual(rejected["previous_receipt"], cancelled["receipt_digest"])
        self.assertEqual(promoted["previous_receipt"], rejected["receipt_digest"])
        self.assertEqual([record["round"] for record in router.receipts], [1, 2, 3])
        for record in router.receipts:
            self.assertEqual(record["run_id"], router.run_id)
            self.assertEqual(
                record["receipt_digest"],
                digest(
                    {
                        key: value
                        for key, value in record.items()
                        if key != "receipt_digest"
                    }
                ),
            )
