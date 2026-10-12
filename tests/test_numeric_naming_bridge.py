"""Real fixed-model evaluation can inform proposals without training or authority."""

import asyncio
import copy
import unittest
from unittest.mock import patch

from aethermesh_core.capability_fixture import evaluate_naming_fixture, fixture_digest
from examples import numeric_naming_bridge as bridge


class NumericNamingBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_evaluation_accepted_and_rejected_without_training(self):
        actual_worker = bridge.DEMO["run_worker"]
        calls = []

        async def evaluate_only(payload):
            self.assertEqual(payload["operation"], "evaluate")
            calls.append(payload)
            return await actual_worker(payload)

        with patch.dict(bridge.DEMO, {"run_worker": evaluate_only}):
            result = await bridge.run_bridge()
        self.assertEqual(len(calls), 3)
        self.assertEqual(result["prototype"], "synthetic-quadratic-expert")
        self.assertTrue(result["actual_numeric_evaluation"])
        for key in (
            "training_performed",
            "evidence_authenticated",
            "independent_operators_verified",
            "public_family_assigned",
            "production_promotion",
        ):
            self.assertFalse(result[key])
        accepted = result["proposals"]["existing_fitted"]
        rejected = result["proposals"]["regression_control"]
        self.assertEqual(accepted["decision"]["status"], "eligible")
        self.assertEqual(accepted["decision"]["proposed_name"], "Numeric")
        self.assertEqual(accepted["decision"]["proposed_internal_version"], 1)
        self.assertEqual(rejected["decision"]["status"], "experimental")
        self.assertIsNone(rejected["decision"]["proposed_name"])
        self.assertEqual(rejected["decision"]["proposed_internal_version"], 0)
        self.assertIn("regression", rejected["decision"]["reasons"])
        for proposal, sse, regressed in ((accepted, 0, 0), (rejected, 4, 1)):
            reports = proposal["fixture"]["reports"]
            self.assertEqual(
                reports["baseline"]["metrics"]["heldout_sse"]["value"], 250
            )
            self.assertEqual(
                reports["candidate"]["metrics"]["heldout_sse"]["value"], sse
            )
            self.assertEqual(
                reports["candidate"]["metrics"]["regressed_cases"]["value"], regressed
            )
            for report in reports.values():
                for field in (
                    "baseline",
                    "schema",
                    "evaluation",
                    "dataset",
                    "taxonomy",
                ):
                    self.assertEqual(report[field], result["binding"][field])
            decision = evaluate_naming_fixture(
                bridge.DEMO["encode"](proposal["fixture"]), **proposal["pins"]
            )
            self.assertEqual(decision.status, proposal["decision"]["status"])
            self.assertTrue(decision.simulated)
            self.assertFalse(
                decision.publication_authorized or decision.promotion_committed
            )
            self.assertFalse(
                decision.live_capability_verified or decision.namespace_reserved
            )
        self.assertEqual(
            accepted["decision"]["candidate_artifact"], fixture_digest(bridge.FITTED)
        )
        self.assertEqual(
            rejected["decision"]["candidate_artifact"],
            fixture_digest(bridge.REGRESSING),
        )

    async def test_misbound_tampered_missing_extra_and_cross_artifact_outputs(self):
        binding = bridge.context()
        baseline = await bridge.evaluate(bridge.DEMO["BASELINE"], binding)
        result = await bridge.evaluate(bridge.FITTED, binding)
        for changed in (
            None,
            {},
            {**result, "extra": 1},
            {**result, "task_id": "a" * 64},
            baseline,
        ):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                bridge.proposal(bridge.FITTED, binding, baseline, changed)
        changed = copy.deepcopy(result)
        changed["predictions"][0][0] += 1
        with self.assertRaises(ValueError):
            bridge.proposal(bridge.FITTED, binding, baseline, changed)
        with self.assertRaises(ValueError):
            bridge.proposal(bridge.FITTED, binding, result, result)
        for field in ("baseline", "evaluation", "dataset", "schema", "taxonomy"):
            altered = {**binding, field: "a" * 64}
            with self.subTest(field=field), self.assertRaises(ValueError):
                bridge.proposal(bridge.FITTED, altered, baseline, result)
        no_gain = bridge.proposal(bridge.DEMO["BASELINE"], binding, baseline, baseline)
        self.assertIn("profile_binding", no_gain["decision"]["reasons"])

    async def test_source_drift_before_and_after_worker_is_rejected(self):
        binding = bridge.context()
        with (
            patch.object(bridge, "sources", return_value={}),
            self.assertRaisesRegex(ValueError, "source changed"),
        ):
            await bridge.evaluate(bridge.FITTED, binding)
        actual_worker = bridge.DEMO["run_worker"]

        async def drift(payload):
            result = await actual_worker(payload)
            bridge.LOADED_SOURCES = {}
            return result

        with (
            patch.object(bridge, "LOADED_SOURCES", bridge.LOADED_SOURCES),
            patch.dict(bridge.DEMO, {"run_worker": drift}),
            self.assertRaisesRegex(ValueError, "source changed"),
        ):
            await bridge.evaluate(bridge.FITTED, binding)
        with (
            patch.object(bridge, "sources", side_effect=[binding["sources"], {}]),
            self.assertRaisesRegex(ValueError, "source changed"),
        ):
            await bridge.evaluate(bridge.FITTED, binding)

    async def test_cancellation_never_returns_partial_proposals(self):
        started = asyncio.Event()

        async def wait(_payload):
            started.set()
            await asyncio.Event().wait()

        with patch.dict(bridge.DEMO, {"run_worker": wait}):
            running = asyncio.create_task(bridge.run_bridge())
            await started.wait()
            running.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await running

    def test_rational_metric_is_not_silently_rounded(self):
        result = {"predictions": [[1, 3]] * 8}
        with self.assertRaisesRegex(ValueError, "integral"):
            bridge.metrics(result, result)
