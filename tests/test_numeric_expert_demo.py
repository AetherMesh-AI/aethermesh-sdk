"""Synthetic expert training evidence, recovery, and adversarial decisions."""

from __future__ import annotations

import asyncio
import copy
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.learning_state import LearningConflict, LearningSnapshot
from examples import numeric_expert_demo as demo
from examples import numeric_expert_worker as worker

MODEL = [[3, 1], [2, 1], [-1, 1], [1, 1], [1, 1], [2, 1]]


class NumericExpertTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.directory = self.root / "job"
        self.experiment = demo.NumericExperiment(self.directory, "a" * 64, create=True)
        self.addCleanup(self.experiment.close)

    async def candidate(self) -> None:
        for _ in range(4):
            await self.experiment.step()
        self.assertEqual(self.experiment.report()["phase"], "evaluating")

    def evidence(self, model: Any = MODEL) -> dict[str, Any]:
        return worker.evaluate(self.experiment.task(model))

    async def test_real_training_recovery_promotion_and_explicit_rollback(self) -> None:
        training_features = {tuple(row[:2]) for row in demo.TRAINING}
        heldout_features = {tuple(row[:2]) for row in demo.HELDOUT}
        regression_features = {tuple(row[:2]) for row in demo.REGRESSION}
        self.assertFalse(training_features & heldout_features)
        self.assertFalse(training_features & regression_features)
        self.assertFalse(heldout_features & regression_features)
        await self.experiment.step()
        old = self.experiment.store.snapshot()
        self.experiment.close()
        replacement = demo.NumericExperiment(self.directory, "a" * 64)
        self.addCleanup(replacement.close)
        self.assertGreater(replacement.term, old.term)
        self.assertEqual(replacement.report()["processed"], 4)
        for _ in range(4):
            await replacement.step()
        report = replacement.report()
        self.assertEqual(report["candidate"], MODEL)
        self.assertEqual(report["active_model"], MODEL)
        self.assertEqual(
            report["assessment"],
            {
                "promote": True,
                "heldout_sse_before": "250",
                "heldout_sse_after": "0",
                "regressed_cases": 0,
            },
        )
        self.assertFalse(report["evidence_authenticated"])
        self.assertFalse(report["replicated_coordination"])
        rollback = replacement.rollback()
        self.assertEqual(rollback["phase"], "rolled_back")
        self.assertEqual(rollback["active_model"], demo.BASELINE)
        self.assertGreater(rollback["revision"], report["revision"])
        recovered = demo.NumericExperiment(self.directory, "a" * 64)
        self.addCleanup(recovered.close)
        self.assertEqual(recovered.report()["phase"], "rolled_back")
        with self.assertRaises(ValueError):
            await recovered.step()
        with self.assertRaises(ValueError):
            recovered.rollback()

    async def test_wrong_predictions_task_or_extra_claims_cannot_promote(self) -> None:
        await self.candidate()
        before = self.experiment.store.snapshot()
        honest = self.evidence()
        cases = [
            {**honest, "task_id": "b" * 64},
            {**honest, "predictions": [[0, 1]] * 8},
            {**honest, "predictions": [[True, 1]] * 8},
            {**honest, "score": 100},
            {**honest, "policy": {"allow": True}},
            {**honest, "predictions": honest["predictions"][:-1]},
            {**honest, "predictions": [[1000000000, 0]] * 8},
            [],
        ]
        for response in cases:
            with (
                self.subTest(response=response),
                patch.object(demo, "run_worker", return_value=response),
                self.assertRaises((ValueError, TypeError)),
            ):
                await self.experiment.step()
            self.assertEqual(self.experiment.store.snapshot(), before)
        self.assertEqual((await self.experiment.step())["phase"], "promoted")
        with self.assertRaises(ValueError):
            await self.experiment.step()
        current = self.experiment.store.snapshot()
        with self.assertRaises(LearningConflict):
            self.experiment.commit(
                current,
                demo.decode(current.state),
                demo.decode(current.checkpoint),
                (demo.digest(honest),),
            )

    async def test_replay_from_another_job_is_rejected(self) -> None:
        await self.candidate()
        evidence = self.evidence()
        other = demo.NumericExperiment(self.root / "other", "b" * 64, create=True)
        self.addCleanup(other.close)
        for _ in range(4):
            await other.step()
        before = other.store.snapshot()
        with (
            patch.object(demo, "run_worker", return_value=evidence),
            self.assertRaises(ValueError),
        ):
            await other.step()
        self.assertEqual(other.store.snapshot(), before)

    async def test_regression_and_no_gain_reject_bounded_candidates(self) -> None:
        original = demo.run_worker
        regressing = copy.deepcopy(MODEL)
        regressing[0] = [4, 1]
        for index, model in enumerate((regressing, demo.BASELINE)):
            experiment = demo.NumericExperiment(
                self.root / f"reject-{index}", f"{index:064x}", create=True
            )
            self.addCleanup(experiment.close)

            async def alter(
                payload: dict[str, Any], candidate: Any = model
            ) -> dict[str, Any]:
                response = await original(payload)
                if payload["operation"] == "train" and response["model"] is not None:
                    response["model"] = candidate
                return response

            with patch.object(demo, "run_worker", side_effect=alter):
                for _ in range(5):
                    await experiment.step()
            report = experiment.report()
            self.assertEqual(report["phase"], "rejected")
            self.assertEqual(report["active_model"], demo.BASELINE)
            self.assertEqual(
                report["assessment"]["regressed_cases"], 1 if index == 0 else 0
            )
            if index == 0:
                self.assertEqual(report["assessment"]["heldout_sse_after"], "4")
            else:
                self.assertEqual(report["assessment"]["heldout_sse_after"], "250")

    async def test_late_result_from_stale_coordinator_cannot_commit(self) -> None:
        await self.candidate()
        entered, release = asyncio.Event(), asyncio.Event()

        async def delayed(payload: dict[str, Any]) -> dict[str, Any]:
            entered.set()
            await release.wait()
            return worker.evaluate(payload)

        with patch.object(demo, "run_worker", side_effect=delayed):
            task = asyncio.create_task(self.experiment.step())
            await entered.wait()
            replacement = demo.NumericExperiment(self.directory, "a" * 64)
            self.addCleanup(replacement.close)
            before = replacement.store.snapshot()
            release.set()
            with self.assertRaises(LearningConflict):
                await task
        self.assertEqual(replacement.store.snapshot(), before)
        self.assertEqual((await replacement.step())["phase"], "promoted")

    async def test_cancel_is_durable_and_does_not_activate_candidate(self) -> None:
        await self.candidate()
        report = self.experiment.cancel()
        self.assertEqual(report["phase"], "cancelled")
        self.assertEqual(report["active_model"], demo.BASELINE)
        replacement = demo.NumericExperiment(self.directory, "a" * 64)
        self.addCleanup(replacement.close)
        self.assertEqual(replacement.report()["phase"], "cancelled")
        with self.assertRaises(ValueError):
            await replacement.step()
        with self.assertRaises(ValueError):
            replacement.rollback()

    async def test_forged_checkpoint_and_model_rejected_before_commit(self) -> None:
        before = self.experiment.store.snapshot()
        bad = demo.checkpoint(4)
        bad["gram"][0][0] = False
        for response in (
            {"checkpoint": bad, "model": None},
            {"checkpoint": demo.checkpoint(8), "model": None},
            {"checkpoint": demo.checkpoint(4), "model": MODEL},
            {"checkpoint": demo.checkpoint(4), "model": None, "policy": "replace"},
        ):
            with (
                self.subTest(response=response),
                patch.object(demo, "run_worker", return_value=response),
                self.assertRaises(ValueError),
            ):
                await self.experiment.step()
            self.assertEqual(self.experiment.store.snapshot(), before)
        for model in (
            [],
            [[True, 1]] * 6,
            [[1, 0]] * 6,
            [[2, 2]] * 6,
            [[1000001, 1]] * 6,
        ):
            with self.subTest(model=model), self.assertRaises(ValueError):
                demo.model_values(model)

    async def test_changed_policy_suite_evaluator_and_job_fail_on_resume(self) -> None:
        with (
            patch.dict(demo.POLICY, minimum_heldout_sse_gain=0),
            self.assertRaises(ValueError),
        ):
            demo.NumericExperiment(self.directory, "a" * 64)
        with patch.object(demo, "HELDOUT", [[6, 6, 0]]), self.assertRaises(ValueError):
            demo.NumericExperiment(self.directory, "a" * 64)
        with self.assertRaises(ValueError):
            demo.NumericExperiment(self.directory, "b" * 64)
        with (
            patch.object(demo, "WORKER", Path(__file__).resolve()),
            self.assertRaises(ValueError),
        ):
            await self.experiment.step()

    async def test_atomic_transition_checks_cannot_skip_training_or_gates(self) -> None:
        before = self.experiment.store.snapshot()
        variants = [
            (
                {"phase": "evaluating", "candidate": MODEL, "evidence": None},
                demo.checkpoint(16),
                (),
            ),
            (
                {"phase": "promoted", "candidate": MODEL, "evidence": self.evidence()},
                demo.checkpoint(16),
                (),
            ),
            (
                {"phase": "cancelled", "candidate": MODEL, "evidence": None},
                demo.checkpoint(16),
                (),
            ),
        ]
        for state, checkpoint, evidence in variants:
            with self.subTest(state=state), self.assertRaises(ValueError):
                self.experiment.commit(before, state, checkpoint, evidence)
            self.assertEqual(self.experiment.store.snapshot(), before)
        await self.candidate()
        before = self.experiment.store.snapshot()
        evidence = self.evidence()
        with self.assertRaises(ValueError):
            self.experiment.commit(
                before,
                {"phase": "promoted", "candidate": MODEL, "evidence": evidence},
                demo.checkpoint(16),
                ("f" * 64,),
            )

    async def test_recovery_rechecks_promoted_evidence(self) -> None:
        await self.candidate()
        snapshot = self.experiment.store.snapshot()
        bad_state = {
            "phase": "promoted",
            "candidate": MODEL,
            "evidence": {"task_id": "0" * 64, "predictions": [[0, 1]] * 8},
        }
        with self.assertRaises(ValueError):
            self.experiment.verify_snapshot(
                LearningSnapshot(
                    snapshot.revision,
                    snapshot.term,
                    demo.encode(bad_state),
                    snapshot.checkpoint,
                )
            )

    async def test_worker_bounds_errors_and_cancellation_reap_children(self) -> None:
        for index, body in enumerate(
            (
                "print('x'*20000)",
                "import sys;sys.stderr.write('x'*5000)",
                "print('[]')",
                "raise RuntimeError('fixture')",
            )
        ):
            helper = self.root / f"bad-{index}.py"
            helper.write_text(body)
            with self.subTest(body=body), self.assertRaises((ValueError, TypeError)):
                await demo.run_worker({}, helper)
        with self.assertRaises(ValueError):
            await demo.run_worker({"too_big": "x" * 16384})
        helper = self.root / "wait.py"
        pid_file = self.root / "pid"
        helper.write_text(
            f"import os,time\nfrom pathlib import Path\nPath({str(pid_file)!r}).write_text(str(os.getpid()))\ntime.sleep(30)\n"
        )
        task = asyncio.create_task(demo.run_worker({}, helper))
        async with asyncio.timeout(3):
            while not pid_file.exists():
                await asyncio.sleep(0.01)
        pid = int(pid_file.read_text())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    async def test_worker_deadline_reaps_timed_out_process(self) -> None:
        helper = self.root / "deadline.py"
        pid_file = self.root / "deadline-pid"
        helper.write_text(
            f"import os,time\nfrom pathlib import Path\nPath({str(pid_file)!r}).write_text(str(os.getpid()))\ntime.sleep(30)\n"
        )
        with self.assertRaises(TimeoutError):
            await demo.run_worker({}, helper)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pid_file.read_text()), 0)

    async def test_checkpoint_boolean_alias_and_report_mutation_are_rejected(
        self,
    ) -> None:
        snapshot = self.experiment.store.snapshot()
        altered = demo.decode(snapshot.checkpoint)
        altered["gram"][0][0] = False
        with self.assertRaises(ValueError):
            self.experiment.verify_snapshot(
                LearningSnapshot(
                    snapshot.revision,
                    snapshot.term,
                    snapshot.state,
                    demo.encode(altered),
                )
            )
        report = self.experiment.report()
        report["active_model"][0][0] = 999
        self.assertEqual(demo.BASELINE[0], [3, 1])
        self.assertEqual(self.experiment.report()["active_model"][0], [3, 1])

    async def test_separate_process_restart_and_crash_recovery(self) -> None:
        await self.experiment.step()
        self.experiment.close()
        code = """
import asyncio,os,sys
from pathlib import Path
from examples.numeric_expert_demo import NumericExperiment
experiment=NumericExperiment(Path(sys.argv[1]), "a"*64)
def crash(sql):
    if sql == "COMMIT": os._exit(73)
experiment.store._db.set_trace_callback(crash)
asyncio.run(experiment.step())
"""
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-c", code, str(self.directory)],
            capture_output=True,
            check=False,
            timeout=10,
        )
        self.assertEqual(result.returncode, 73, result.stderr.decode())
        replacement = demo.NumericExperiment(self.directory, "a" * 64)
        self.addCleanup(replacement.close)
        self.assertEqual(replacement.report()["processed"], 4)
        for _ in range(4):
            await replacement.step()
        self.assertEqual(replacement.report()["phase"], "promoted")


class NumericWorkerUnitTests(unittest.TestCase):
    def test_solver_learns_from_data_and_rejects_rank_deficiency(self) -> None:
        payload = {
            "version": 1,
            "operation": "train",
            "rows": copy.deepcopy(demo.TRAINING),
            "checkpoint": demo.checkpoint(0),
        }
        for _ in range(4):
            result = worker.train(payload)
            payload["checkpoint"] = result["checkpoint"]
        self.assertEqual(result["model"], MODEL)
        shifted = copy.deepcopy(demo.TRAINING)
        for row in shifted:
            row[2] += 7
        payload = {**payload, "rows": shifted, "checkpoint": demo.checkpoint(0)}
        for _ in range(4):
            result = worker.train(payload)
            payload["checkpoint"] = result["checkpoint"]
        self.assertEqual(result["model"][0], [10, 1])
        self.assertEqual(result["model"][1:], MODEL[1:])
        with self.assertRaises(ValueError):
            worker.solve([[0] * 6 for _ in range(6)], [0] * 6)

    def test_worker_rejects_bad_training_and_evaluation_contracts(self) -> None:
        good = {
            "version": 1,
            "operation": "train",
            "rows": demo.TRAINING,
            "checkpoint": demo.checkpoint(0),
        }
        for changes in (
            {"rows": []},
            {"rows": [[True, 0, 0]] * 6},
            {"rows": [[7, 0, 0]] * 6},
            {"checkpoint": {}},
            {"checkpoint": demo.checkpoint(16)},
            {"extra": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                worker.train(good | changes)
        for pair in ([1, 0], [2, 2], [True, 1], [1000001, 1], []):
            with self.subTest(pair=pair), self.assertRaises(ValueError):
                worker.rational(pair)
        good_eval = {
            "version": 1,
            "operation": "evaluate",
            "task_id": "a" * 64,
            "model": MODEL,
            "rows": [[0, 0]],
        }
        for changes in (
            {"model": []},
            {"rows": []},
            {"rows": [[7, 0]]},
            {"rows": [[1, 2, 3]]},
            {"task_id": "z" * 64},
            {"extra": True},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                worker.evaluate(good_eval | changes)


if __name__ == "__main__":
    unittest.main()
