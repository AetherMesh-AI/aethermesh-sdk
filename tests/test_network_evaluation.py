"""Strict, opt-in tiny router evaluation and cumulative resource limits."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from aethermesh_core.network import (
    ROUTER_EVALUATION_OPERATION,
    EvaluationBudget,
    PeerClient,
    PeerEndpoint,
    PeerService,
    ProtocolError,
    RemoteError,
    RouterEvaluator,
    TLSIdentity,
    certificate_fingerprint,
    evaluation_task,
)
from aethermesh_core.network.evaluation import validate_evaluation_result
from aethermesh_core.network.protocol import close_writer, read_frame, write_frame
from tests.network_test_support import TestPKI

CONTEXT = hashlib.sha256(b"public synthetic router task").hexdigest()


def task(rows: tuple[tuple[int, int], ...] = ((1, 2),)) -> dict[str, Any]:
    return evaluation_task(CONTEXT, (2, -1, 0), rows)


def with_digest(content: dict[str, Any]) -> dict[str, Any]:
    """Independent reference for the wire content-addressing contract."""
    digest = hashlib.sha256(
        json.dumps(
            content, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    ).hexdigest()
    return {"task_id": digest, **content}


class EvaluationBudgetTests(unittest.TestCase):
    def test_client_project_namespace_is_read_only_before_connection(self) -> None:
        identity = TLSIdentity(Path("certificate"), Path("key"), Path("ca"))
        client = PeerClient(identity, project_id="evaluation-test")
        self.assertEqual(client.project_id, "evaluation-test")
        with self.assertRaises(AttributeError):
            client.project_id = "another-project"
        with self.assertRaises(ProtocolError):
            client._welcome(
                {
                    "type": "welcome",
                    "version": 1,
                    "project": "evaluation-test",
                    "capabilities": ["status", ROUTER_EVALUATION_OPERATION],
                }
            )

    def test_defaults_ceilings_immutability_and_explicit_evaluator_budget(self) -> None:
        budget = EvaluationBudget()
        self.assertEqual((budget.max_rows, budget.max_total_rows), (8, 256))
        self.assertEqual(EvaluationBudget(32, 4096).max_total_rows, 4096)
        self.assertEqual(EvaluationBudget(1, 1).max_rows, 1)
        with self.assertRaises(FrozenInstanceError):
            budget.max_rows = 9
        with self.assertRaises(TypeError):
            RouterEvaluator()
        for value in (None, {}, 1):
            with self.subTest(value=value), self.assertRaises(TypeError):
                RouterEvaluator(value)
        evaluator = RouterEvaluator(budget)
        self.assertIs(evaluator.budget, budget)
        self.assertEqual(evaluator.rows_used, 0)
        self.assertEqual(evaluator.rows_remaining, 256)
        for field in ("budget", "rows_used", "rows_remaining"):
            with self.subTest(field=field), self.assertRaises(AttributeError):
                setattr(evaluator, field, 0)

    def test_invalid_budget_types_and_ranges(self) -> None:
        for field, ceiling in (("max_rows", 32), ("max_total_rows", 4096)):
            for value in (True, False, None, "1", 1.0, -1, 0, ceiling + 1):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    EvaluationBudget(**{field: value})


class EvaluationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.evaluator = RouterEvaluator(EvaluationBudget())

    def assert_rejected(self, payload: Any, code: str = "invalid_request") -> None:
        used = self.evaluator.rows_used
        with self.assertRaises(RemoteError) as raised:
            self.evaluator.evaluate(payload)
        self.assertEqual(raised.exception.code, code)
        self.assertEqual(self.evaluator.rows_used, used)

    def test_request_helper_canonical_digest_and_prediction_boundaries(self) -> None:
        payload = task(((1, 2), (-1, 2), (2, 1)))
        self.assertEqual(
            payload,
            with_digest(
                {
                    "context": CONTEXT,
                    "weights": [2, -1, 0],
                    "rows": [[1, 2], [-1, 2], [2, 1]],
                }
            ),
        )
        self.assertEqual(
            self.evaluator.evaluate(payload),
            {"task_id": payload["task_id"], "predictions": [1, 0, 1]},
        )
        extremes = evaluation_task(
            CONTEXT, (-1000, 1000, -1000), ((-1000, 1000), (1000, -1000))
        )
        self.assertEqual(self.evaluator.evaluate(extremes)["predictions"], [1, 0])
        self.assertEqual(self.evaluator.rows_used, 5)
        self.assertEqual(self.evaluator.rows_remaining, 251)

    def test_exact_request_fields_and_task_identifiers(self) -> None:
        payload = task()
        for value in (None, [], "request", {}, payload | {"extra": True}):
            with self.subTest(value=value):
                self.assert_rejected(value)
        for field in payload:
            missing = dict(payload)
            del missing[field]
            with self.subTest(missing=field):
                self.assert_rejected(missing)
        for identifier in (
            None,
            False,
            123,
            "",
            "0" * 63,
            "0" * 65,
            "A" * 64,
            "g" * 64,
            "a" * 64 + "\n",
            "a" * 64,
        ):
            with self.subTest(identifier=identifier):
                self.assert_rejected(payload | {"task_id": identifier})
        for change in (
            {"context": "a" * 64},
            {"weights": [3, -1, 0]},
            {"rows": [[1, 3]]},
        ):
            with self.subTest(change=change):
                self.assert_rejected(payload | change)

    def test_context_weights_rows_and_integer_values_are_strict(self) -> None:
        payload = task()
        invalid_values = {
            "context": (None, 2, "", "a" * 63, "A" * 64, "é" * 64),
            "weights": (None, {}, (1, 2, 3), [], [1, 2], [1, 2, 3, 4]),
            "rows": (None, {}, (), [], [[1]], [[1, 2, 3]], [None], [(1, 2)]),
        }
        for field, values in invalid_values.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    self.assert_rejected(payload | {field: value})
        for value in (True, False, 1.0, "1", None, -1001, 1001):
            for index in range(3):
                weights = [1, 2, 3]
                weights[index] = value
                with self.subTest(value=value, weight=index):
                    self.assert_rejected(payload | {"weights": weights})
            for index in range(2):
                row = [1, 2]
                row[index] = value
                with self.subTest(value=value, coordinate=index):
                    self.assert_rejected(payload | {"rows": [row]})

    def test_helper_rejects_invalid_inputs_without_reserving_budget(self) -> None:
        for weights in (None, [], (), (1, 2), (1, 2, 3, 4)):
            with self.subTest(weights=weights), self.assertRaises(ValueError):
                evaluation_task(CONTEXT, weights, ((1, 2),))
        for rows in (None, [], (), ((1, 2),) * 33, (None,), ([1, 2],), ((1,),)):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                evaluation_task(CONTEXT, (1, 2, 3), rows)
        for context, weights, rows in (
            ("bad", (1, 2, 3), ((1, 2),)),
            (CONTEXT, (True, 2, 3), ((1, 2),)),
            (CONTEXT, (1, 2, 3), ((1, 1001),)),
        ):
            with (
                self.subTest(context=context, weights=weights, rows=rows),
                self.assertRaises(ValueError),
            ):
                evaluation_task(context, weights, rows)
        self.assertEqual(self.evaluator.rows_used, 0)

    def test_per_request_limit_precedes_hashing_and_total_is_cumulative(self) -> None:
        oversized = task(((1, 2),) * 9)
        with patch("aethermesh_core.network.evaluation._task_id") as digest:
            self.assert_rejected(oversized, "resource_limit")
            digest.assert_not_called()
        self.evaluator = RouterEvaluator(EvaluationBudget(max_rows=2, max_total_rows=3))
        payload = task(((1, 2), (2, 1)))
        self.evaluator.evaluate(payload)
        self.assert_rejected(payload, "resource_limit")
        self.evaluator.evaluate(task())
        self.assertEqual(self.evaluator.rows_remaining, 0)
        self.assert_rejected(task(), "resource_limit")
        # Invalid requests still report their invalid schema after exhaustion.
        self.assert_rejected(task() | {"task_id": "bad"})

    def test_global_row_ceiling_and_replays_each_consume_budget(self) -> None:
        self.evaluator = RouterEvaluator(
            EvaluationBudget(max_rows=32, max_total_rows=64)
        )
        payload = task(((1000, -1000),) * 32)
        first = self.evaluator.evaluate(payload)
        self.assertEqual(first["predictions"], [1] * 32)
        self.assertEqual(self.evaluator.evaluate(payload), first)
        self.assertEqual(self.evaluator.rows_used, 64)
        self.assert_rejected(payload, "resource_limit")
        oversized = with_digest(
            {"context": CONTEXT, "weights": [1, 1, 1], "rows": [[1, 2]] * 33}
        )
        self.assert_rejected(oversized, "resource_limit")

    def test_concurrent_callers_share_one_atomic_total_allowance(self) -> None:
        self.evaluator = RouterEvaluator(EvaluationBudget(max_rows=2, max_total_rows=5))

        def attempt(_: int) -> bool:
            try:
                self.evaluator.evaluate(task(((1, 2), (2, 1))))
            except RemoteError as exc:
                self.assertEqual(exc.code, "resource_limit")
                return False
            return True

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(attempt, range(20)))
        self.assertEqual(sum(results), 2)
        self.assertEqual(self.evaluator.rows_used, 4)
        self.assertEqual(self.evaluator.rows_remaining, 1)

    def test_response_contract_matches_task_and_binary_prediction_count(self) -> None:
        payload = task()
        response = {"task_id": payload["task_id"], "predictions": [1]}
        validate_evaluation_result(response, payload)
        for change in (
            {"extra": 1},
            {"task_id": None},
            {"task_id": "a" * 64},
            {"predictions": None},
            {"predictions": (1,)},
            {"predictions": []},
            {"predictions": [0, 1]},
            {"predictions": [True]},
            {"predictions": [False]},
            {"predictions": [1.0]},
            {"predictions": ["1"]},
            {"predictions": [-1]},
            {"predictions": [2]},
        ):
            with self.subTest(change=change), self.assertRaises(ProtocolError):
                validate_evaluation_result(response | change, payload)
        for field in response:
            missing = dict(response)
            del missing[field]
            with self.subTest(missing=field), self.assertRaises(ProtocolError):
                validate_evaluation_result(missing, payload)
        with self.assertRaises(ProtocolError):
            validate_evaluation_result(response, {})
        # A structurally valid result still needs independent correctness checks.
        validate_evaluation_result(response | {"predictions": [0]}, payload)


class EvaluationServiceTests(unittest.IsolatedAsyncioTestCase):
    def service(self, **kwargs: Any) -> PeerService:
        return PeerService(
            TLSIdentity(Path("certificate"), Path("key"), Path("ca")),
            project_id="evaluation-test",
            allowed_peers=frozenset(),
            **kwargs,
        )

    async def test_evaluation_requires_object_opt_in_and_does_no_construction_work(
        self,
    ) -> None:
        evaluator = RouterEvaluator(EvaluationBudget())
        with patch.object(evaluator, "evaluate") as evaluate:
            service = self.service(router_evaluator=evaluator)
            self.assertEqual(
                service.capabilities, {"status", ROUTER_EVALUATION_OPERATION}
            )
            evaluate.assert_not_called()
        status = await service._operate("status", {}, protocol_version=2)
        self.assertEqual(
            status["capabilities"], [ROUTER_EVALUATION_OPERATION, "status"]
        )
        self.assertEqual(evaluator.rows_used, 0)
        self.assertEqual(
            (
                await service._operate(
                    ROUTER_EVALUATION_OPERATION, task(), protocol_version=2
                )
            )["predictions"],
            [1],
        )
        default = self.service()
        self.assertEqual(default.capabilities, {"status"})
        with self.assertRaises(RemoteError) as raised:
            await default._operate(ROUTER_EVALUATION_OPERATION, task())
        self.assertEqual(raised.exception.code, "unsupported_operation")
        with self.assertRaises(ValueError):
            self.service(capabilities=frozenset({ROUTER_EVALUATION_OPERATION}))
        for value in (True, EvaluationBudget(), {}):
            with self.subTest(value=value), self.assertRaises(TypeError):
                self.service(router_evaluator=value)


class EvaluationTLSTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory(prefix="aethermesh-evaluation-pki-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.pki = TestPKI.create(Path(cls.directory.name))

    async def asyncSetUp(self) -> None:
        self.evaluator = RouterEvaluator(EvaluationBudget(max_rows=2, max_total_rows=3))
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="evaluation-test",
            allowed_peers=frozenset(
                {certificate_fingerprint(self.pki.client.certificate)}
            ),
            capabilities=frozenset({"echo"}),
            router_evaluator=self.evaluator,
        )
        self.addAsyncCleanup(self.service.close)
        await self.service.start()
        self.endpoint = self.endpoint_for_service()

    def endpoint_for_service(self) -> PeerEndpoint:
        return PeerEndpoint(
            *self.service.address,
            "localhost",
            certificate_fingerprint(self.pki.server.certificate),
        )

    async def client(self) -> PeerClient:
        client = PeerClient(self.pki.client.identity(), project_id="evaluation-test")
        self.addAsyncCleanup(client.close)
        await client.connect(self.endpoint)
        return client

    async def test_real_tls_capability_evaluation_errors_reconnect_and_restart(
        self,
    ) -> None:
        client = await self.client()
        self.assertEqual(
            client.capabilities, ("echo", ROUTER_EVALUATION_OPERATION, "status")
        )
        status = await client.request("status")
        self.assertEqual(status["capabilities"], list(client.capabilities))
        self.assertEqual(
            await client.request("echo", {"text": "still available"}),
            {"text": "still available"},
        )
        payload = task(((1, 2), (0, 2)))
        self.assertEqual(
            await client.request(ROUTER_EVALUATION_OPERATION, payload),
            {"task_id": payload["task_id"], "predictions": [1, 0]},
        )
        with self.assertRaises(RemoteError) as invalid:
            await client.request(
                ROUTER_EVALUATION_OPERATION, payload | {"task_id": "bad"}
            )
        self.assertEqual(invalid.exception.code, "invalid_request")
        self.assertEqual(self.evaluator.rows_used, 2)
        await client.close()
        await client.connect(self.endpoint)
        self.assertEqual(
            (await client.request(ROUTER_EVALUATION_OPERATION, task()))["predictions"],
            [1],
        )
        with self.assertRaises(RemoteError) as limited:
            await client.request(ROUTER_EVALUATION_OPERATION, task())
        self.assertEqual(limited.exception.code, "resource_limit")
        self.assertEqual(
            str(limited.exception),
            "The peer has reached its evaluation resource limit.",
        )
        self.assertTrue(client.connected)
        await client.close()
        await self.service.close()
        await self.service.start()
        self.endpoint = self.endpoint_for_service()
        restarted = await self.client()
        with self.assertRaises(RemoteError) as still_limited:
            await restarted.request(ROUTER_EVALUATION_OPERATION, task())
        self.assertEqual(still_limited.exception.code, "resource_limit")
        self.assertEqual(self.evaluator.rows_used, 3)

    async def test_multiple_authenticated_sessions_share_budget(self) -> None:
        clients = await asyncio.gather(*(self.client() for _ in range(3)))
        results = await asyncio.gather(
            *(
                client.request(ROUTER_EVALUATION_OPERATION, task(((1, 2), (2, 1))))
                for client in clients
            ),
            return_exceptions=True,
        )
        self.assertEqual(sum(isinstance(result, dict) for result in results), 1)
        errors = [result for result in results if isinstance(result, RemoteError)]
        self.assertEqual([error.code for error in errors], ["resource_limit"] * 2)
        self.assertEqual(self.evaluator.rows_used, 2)

    async def test_legacy_v1_session_has_unchanged_capabilities_and_no_evaluation(
        self,
    ) -> None:
        reader, writer = await asyncio.open_connection(
            self.endpoint.host,
            self.endpoint.port,
            ssl=self.pki.client.identity().client_context(),
            server_hostname="localhost",
        )
        self.addAsyncCleanup(close_writer, writer)
        await write_frame(
            writer, {"type": "hello", "versions": [1], "project": "evaluation-test"}
        )
        welcome = await read_frame(reader)
        self.assertEqual(
            welcome,
            {
                "type": "welcome",
                "version": 1,
                "project": "evaluation-test",
                "capabilities": ["echo", "status"],
            },
        )
        await write_frame(
            writer, {"type": "request", "id": 1, "operation": "status", "payload": {}}
        )
        self.assertEqual(
            await read_frame(reader),
            {
                "type": "result",
                "id": 1,
                "result": {"protocol_version": 1, "capabilities": ["echo", "status"]},
            },
        )
        await write_frame(
            writer,
            {
                "type": "request",
                "id": 2,
                "operation": ROUTER_EVALUATION_OPERATION,
                "payload": task(),
            },
        )
        self.assertEqual((await read_frame(reader))["code"], "unsupported_operation")
        self.assertEqual(self.evaluator.rows_used, 0)
        await write_frame(
            writer,
            {
                "type": "request",
                "id": 3,
                "operation": "echo",
                "payload": {"text": "legacy still works"},
            },
        )
        self.assertEqual(
            (await read_frame(reader))["result"], {"text": "legacy still works"}
        )

    async def test_client_closes_session_for_invalid_evaluation_result(self) -> None:
        client = await self.client()
        payload = task()
        malformed = {"task_id": payload["task_id"], "predictions": [True]}
        with (
            patch.object(
                self.service, "_operate", new=AsyncMock(return_value=malformed)
            ),
            self.assertRaises(ProtocolError),
        ):
            await client.request(ROUTER_EVALUATION_OPERATION, payload)
        self.assertFalse(client.connected)

    async def test_request_helper_has_no_aliases_between_requests(self) -> None:
        client = await self.client()
        original = task()
        expected = copy.deepcopy(original)
        second = task()
        second["rows"][0][0] = -1000
        second["weights"][0] = -1000
        self.assertEqual(original, expected)
        result = await client.request(ROUTER_EVALUATION_OPERATION, original)
        self.assertEqual(result["predictions"], [1])
