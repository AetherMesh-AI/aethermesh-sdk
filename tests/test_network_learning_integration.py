"""Real-TLS negative tests and repeatable multi-process learning evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from aethermesh_core.network import (
    ConnectionClosed,
    EvaluationBudget,
    PeerClient,
    PeerEndpoint,
    PeerService,
    ProtocolError,
    RemoteError,
    RouterEvaluator,
    certificate_fingerprint,
    evaluation_task,
)
from aethermesh_core.network.governance import (
    EvaluationSuite,
    GovernancePolicy,
    GovernedRouter,
    TrainingProvenance,
)
from examples import network_learning as learning_example
from tests.network_test_support import TestIdentity as PeerIdentity
from tests.network_test_support import TestPKI

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "learning-integration"
PROVENANCE = TrainingProvenance("a" * 64, "b" * 64)
SUITE = EvaluationSuite(
    heldout=(((-4, -1), 0), ((4, 1), 1)),
    regression=(((-1, 0), 0), ((1, 0), 1)),
)


class AdversarialService(PeerService):
    """A genuine TLS peer with deliberately dishonest or delayed responses."""

    mode = "honest"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.saved: dict[str, Any] | None = None

    async def _operate(
        self, operation: str, payload: dict[str, Any], *, protocol_version: int = 1
    ) -> dict[str, Any]:
        if operation == "router.evaluate.v1":
            self.entered.set()
            if self.mode == "delay":
                await self.release.wait()
            if self.mode == "replay":
                assert self.saved is not None
                return self.saved
            result = await super()._operate(
                operation, payload, protocol_version=protocol_version
            )
            if self.mode == "forge":
                result["predictions"] = [1 - value for value in result["predictions"]]
            self.saved = result
            return result
        return await super()._operate(
            operation, payload, protocol_version=protocol_version
        )


class LearningExampleCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_primary_exception_survives_all_child_cleanup_failures(self) -> None:
        primary = ValueError("original pilot failure")
        children = [cast(asyncio.subprocess.Process, object()) for _ in range(2)]
        cleanup = AsyncMock(
            side_effect=[RuntimeError("first child"), RuntimeError("second child")]
        )
        with (
            patch.object(learning_example, "stop_process", cleanup),
            self.assertRaises(ValueError) as caught,
        ):
            try:
                raise primary
            finally:
                await learning_example.stop_processes(children, sys.exception())
        self.assertIs(caught.exception, primary)
        self.assertEqual(cleanup.await_count, 2)
        self.assertEqual(
            primary.__notes__,
            [
                "Child cleanup failed: RuntimeError: first child",
                "Child cleanup failed: RuntimeError: second child",
            ],
        )

    async def test_cleanup_failure_is_raised_when_no_primary_failed(self) -> None:
        first = RuntimeError("first child")
        children = [cast(asyncio.subprocess.Process, object()) for _ in range(2)]
        cleanup = AsyncMock(side_effect=[first, RuntimeError("second child")])
        with (
            patch.object(learning_example, "stop_process", cleanup),
            self.assertRaises(RuntimeError) as caught,
        ):
            await learning_example.stop_processes(children, None)
        self.assertIs(caught.exception, first)
        self.assertEqual(cleanup.await_count, 2)
        self.assertEqual(
            first.__notes__,
            ["Additional child cleanup failure: RuntimeError: second child"],
        )

    async def test_successful_cleanup_preserves_results_and_original_cancellation(
        self,
    ) -> None:
        child = cast(asyncio.subprocess.Process, object())
        with patch.object(
            learning_example, "stop_process", AsyncMock(return_value={"rows_used": 48})
        ):
            self.assertEqual(
                await learning_example.stop_processes([child], None),
                [{"rows_used": 48}],
            )
            cancellation = asyncio.CancelledError("operator cancelled")
            with self.assertRaises(asyncio.CancelledError) as caught:
                try:
                    raise cancellation
                finally:
                    await learning_example.stop_processes([child], sys.exception())
            self.assertIs(caught.exception, cancellation)

    async def test_child_error_tail_is_bounded_and_contains_root_cause(self) -> None:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('x' * 5000 + '\\nROOT CAUSE'); sys.exit(1)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        with self.assertRaises(RuntimeError) as caught:
            await learning_example.stop_process(process)
        self.assertTrue(str(caught.exception).endswith("ROOT CAUSE"))
        self.assertLess(len(str(caught.exception)), 4200)
        self.assertEqual(process.returncode, 1)


class NetworkLearningIntegrationTests(unittest.IsolatedAsyncioTestCase):
    directory: tempfile.TemporaryDirectory[str]
    pki: TestPKI
    client_pin: str
    server_pin: str
    alternate_pin: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory(prefix="learning-tls-test-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.pki = TestPKI.create(Path(cls.directory.name))
        cls.client_pin = certificate_fingerprint(cls.pki.client.certificate)
        cls.server_pin = certificate_fingerprint(cls.pki.server.certificate)
        cls.alternate_pin = certificate_fingerprint(cls.pki.unauthorized.certificate)

    async def peer(
        self,
        identity: PeerIdentity | None = None,
        *,
        budget: EvaluationBudget | None = None,
    ) -> tuple[AdversarialService, RouterEvaluator, PeerClient, PeerEndpoint]:
        identity = identity or self.pki.server
        evaluator = RouterEvaluator(
            budget or EvaluationBudget(max_rows=2, max_total_rows=32)
        )
        service = AdversarialService(
            identity.identity(),
            project_id=PROJECT,
            allowed_peers=frozenset({self.client_pin}),
            router_evaluator=evaluator,
        )
        self.addAsyncCleanup(service.close)
        await service.start()
        client = PeerClient(self.pki.client.identity(), project_id=PROJECT)
        self.addAsyncCleanup(client.close)
        endpoint = PeerEndpoint(
            *service.address, "localhost", certificate_fingerprint(identity.certificate)
        )
        await client.connect(endpoint)
        info = client.peer_info
        assert info is not None
        self.assertEqual(info.fingerprint, endpoint.fingerprint)
        self.assertIn("router.evaluate.v1", client.capabilities)
        return service, evaluator, client, endpoint

    def controller(self, *, duplicate_operator: bool = False) -> GovernedRouter:
        authorities = (
            (
                (self.server_pin, "operator-a"),
                (self.alternate_pin, "operator-a"),
                ("c" * 64, "operator-b"),
            )
            if duplicate_operator
            else ((self.server_pin, "operator-a"), (self.alternate_pin, "operator-b"))
        )
        return GovernedRouter(
            GovernancePolicy(PROJECT, authorities, "trainer", microbatch_rows=2),
            SUITE,
            (0, 0, -1),
        )

    async def test_budget_limits_apply_over_tls_and_reconnect_does_not_refill(
        self,
    ) -> None:
        _, evaluator, client, endpoint = await self.peer(
            budget=EvaluationBudget(max_rows=2, max_total_rows=4)
        )
        oversized = evaluation_task("a" * 64, (2, 2, 0), ((1, 1), (2, 2), (3, 3)))
        with self.assertRaises(RemoteError) as caught:
            await client.request("router.evaluate.v1", oversized)
        self.assertEqual(caught.exception.code, "resource_limit")
        self.assertEqual(evaluator.rows_used, 0)
        task = evaluation_task("b" * 64, (2, 2, 0), ((1, 1), (-1, -1)))
        expected = {"task_id": task["task_id"], "predictions": [1, 0]}
        self.assertEqual(await client.request("router.evaluate.v1", task), expected)
        self.assertEqual(await client.request("router.evaluate.v1", task), expected)
        self.assertEqual(evaluator.rows_used, 4)
        await client.close()
        await client.connect(endpoint)
        with self.assertRaises(RemoteError) as caught:
            await client.request("router.evaluate.v1", task)
        self.assertEqual(caught.exception.code, "resource_limit")
        self.assertEqual(evaluator.rows_remaining, 0)
        self.assertTrue(client.connected)

    async def test_dishonest_predictions_cannot_promote_over_authenticated_tls(
        self,
    ) -> None:
        service, evaluator, client, _ = await self.peer()
        service.mode = "forge"
        controller = self.controller()
        initial = controller.active_id
        controller.propose((2, 2, 0), PROVENANCE)
        with self.assertRaisesRegex(ValueError, "dishonest"):
            await controller.collect(client, 0)
        receipt = controller.finish()
        self.assertEqual(receipt["decision"], "rejected")
        self.assertEqual(receipt["reason"], "insufficient_independent_evidence")
        self.assertEqual(receipt["evaluators"], {})
        self.assertEqual(controller.active_id, initial)
        self.assertEqual(evaluator.rows_used, 2)
        self.assertTrue(client.connected)

    async def test_old_task_response_cannot_be_replayed_into_new_round(self) -> None:
        service, _, client, _ = await self.peer()
        controller = self.controller()
        controller.propose((2, 2, 0), PROVENANCE)
        old_id = controller.tasks[0]["task_id"]
        await controller.collect(client, 0)
        controller.cancel()
        controller.propose((2, 2, 0), PROVENANCE)
        self.assertNotEqual(controller.tasks[0]["task_id"], old_id)
        service.mode = "replay"
        with self.assertRaises(ProtocolError):
            await controller.collect(client, 0)
        receipt = controller.finish()
        self.assertEqual(receipt["evaluators"], {})
        self.assertEqual(receipt["decision"], "rejected")
        self.assertEqual(controller.route((1, 1)), 0)

    async def test_reconnect_and_two_certificates_do_not_multiply_operator_votes(
        self,
    ) -> None:
        _, first_evaluator, first, endpoint = await self.peer()
        _, second_evaluator, second, _ = await self.peer(self.pki.unauthorized)
        controller = self.controller(duplicate_operator=True)
        controller.propose((2, 2, 0), PROVENANCE)
        await controller.collect(first, 0)
        await first.close()
        await first.connect(endpoint)
        with self.assertRaisesRegex(ValueError, "operator already"):
            await controller.collect(first, 0)
        with self.assertRaisesRegex(ValueError, "operator already"):
            await controller.collect(second, 0)
        self.assertEqual(first_evaluator.rows_used, 2)
        self.assertEqual(second_evaluator.rows_used, 0)
        receipt = controller.finish()
        self.assertEqual(receipt["evaluators"], {"0": {"operator-a": self.server_pin}})
        self.assertEqual(receipt["reason"], "insufficient_independent_evidence")
        self.assertEqual(controller.route((1, 1)), 0)

    async def test_cancelled_round_rejects_late_result(self) -> None:
        service, _, client, _ = await self.peer()
        service.mode = "delay"
        controller = self.controller()
        controller.propose((2, 2, 0), PROVENANCE)
        pending = asyncio.create_task(controller.collect(client, 0))
        await asyncio.wait_for(service.entered.wait(), timeout=2)
        cancelled = controller.cancel()
        service.release.set()
        with self.assertRaisesRegex(ValueError, "stale evaluation round"):
            await pending
        self.assertEqual(cancelled["decision"], "cancelled")
        self.assertEqual(controller.receipts, (cancelled,))
        self.assertEqual(controller.route((1, 1)), 0)

    async def test_request_cancellation_and_disconnect_leave_no_evidence(self) -> None:
        service, evaluator, client, _ = await self.peer()
        service.mode = "delay"
        controller = self.controller()
        controller.propose((2, 2, 0), PROVENANCE)
        pending = asyncio.create_task(controller.collect(client, 0))
        await asyncio.wait_for(service.entered.wait(), timeout=2)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertEqual(controller.cancel()["evaluators"], {})
        self.assertEqual(evaluator.rows_used, 0)
        self.assertTrue(client.connected)
        service.entered.clear()
        controller.propose((2, 2, 0), PROVENANCE)
        pending = asyncio.create_task(controller.collect(client, 0))
        await asyncio.wait_for(service.entered.wait(), timeout=2)
        await service.close()
        with self.assertRaises(ConnectionClosed):
            await pending
        receipt = controller.finish()
        self.assertEqual(receipt["evaluators"], {})
        self.assertEqual(receipt["decision"], "rejected")
        self.assertEqual(controller.route((1, 1)), 0)

    async def run_example(self, directory: Path, index: int) -> dict[str, Any]:
        output = directory / f"result-{index}.json"
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(ROOT / "src")
        environment["TMPDIR"] = str(directory)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(directory / "network_learning.py"),
            "--ephemeral-test-pki",
            "--output",
            str(output),
            cwd=directory,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.communicate()
            raise
        self.assertEqual(process.returncode, 0, stderr.decode())
        summary = json.loads(stdout)
        self.assertEqual(summary["artifact"], str(output))
        result: dict[str, Any] = json.loads(output.read_text())
        self.assertEqual(result["coordinator_pid"], process.pid)
        self.assertEqual(
            len(
                {
                    process.pid,
                    result["trainer"]["pid"],
                    *(peer["pid"] for peer in result["evaluators"]),
                }
            ),
            4,
        )
        self.assertEqual(result["trainer"]["weights"], [2, 2, 0])
        self.assertEqual(result["trainer"]["training_rows"], 6)
        self.assertEqual(result["trainer"]["training_correct"], 6)
        self.assertGreater(result["trainer"]["updates"], 0)
        self.assertEqual(
            result["routing"],
            {"probe": [1, 1], "before": 0, "after_promotion": 1, "after_rollback": 0},
        )
        self.assertEqual(
            [item["decision"] for item in result["observations"]],
            ["rejected", "rejected", "promoted", "rejected"],
        )
        gained_but_unsafe = result["observations"][1]
        self.assertGreater(
            gained_but_unsafe["candidate_correct"],
            gained_but_unsafe["baseline_correct"],
        )
        self.assertEqual(gained_but_unsafe["regressions"], 2)
        promoted = result["observations"][2]
        self.assertEqual(promoted["baseline_correct"], 4)
        self.assertEqual(promoted["candidate_correct"], 8)
        self.assertEqual(promoted["regressions"], 0)
        self.assertEqual(promoted["quarantined_route"], 0)
        self.assertEqual(promoted["active_route"], 1)
        self.assertEqual(result["active_id"], result["initial_id"])
        self.assertNotEqual(result["promoted_id"], result["initial_id"])
        self.assertEqual([peer["rows_used"] for peer in result["evaluators"]], [48, 48])
        self.assertEqual(len({peer["fingerprint"] for peer in result["evaluators"]}), 2)
        self.assertLess(result["limits"]["max_task_payload_bytes"], 1024)
        self.assertTrue(result["ephemeral_credentials_deleted"])
        self.assertEqual(list(directory.glob("aethermesh-learning-pki-*")), [])
        self.assertEqual(len(result["receipts"]), 5)
        for receipt in result["receipts"]:
            stored = receipt["receipt_digest"]
            content = {
                key: value for key, value in receipt.items() if key != "receipt_digest"
            }
            self.assertEqual(
                stored,
                hashlib.sha256(
                    json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            )
            if receipt["decision"] != "rolled_back":
                self.assertEqual(len(receipt["evaluators"]), 6)
                for votes in receipt["evaluators"].values():
                    self.assertEqual(set(votes), {"evaluator-a", "evaluator-b"})
        return result

    async def test_copied_example_is_semantically_repeatable_and_cleans_credentials(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="learning-example-") as directory_name:
            directory = Path(directory_name)
            for name in (
                "network_learning.py",
                "network_learning_trainer.py",
                "network_learning_worker.py",
            ):
                shutil.copyfile(ROOT / "examples" / name, directory / name)
            first = await self.run_example(directory, 1)
            second = await self.run_example(directory, 2)
        # Fresh PKI and PIDs intentionally vary. Compare semantics, not the
        # certificate-bound candidate IDs or receipts' complete byte strings.
        for key in (
            "observations",
            "routing",
            "limits",
            "heldout_cases",
            "regression_cases",
        ):
            self.assertEqual(first[key], second[key])
        self.assertEqual(
            first["trainer"]["training_data_digest"],
            second["trainer"]["training_data_digest"],
        )
        self.assertEqual(
            first["trainer"]["source_digest"], second["trainer"]["source_digest"]
        )
        self.assertNotEqual(
            first["evaluators"][0]["fingerprint"],
            second["evaluators"][0]["fingerprint"],
        )
        self.assertNotEqual(first["promoted_id"], second["promoted_id"])


if __name__ == "__main__":
    unittest.main()
