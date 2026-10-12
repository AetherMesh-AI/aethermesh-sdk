"""Explicit retained repair candidates never authorize execution or charge release."""

import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import artifact_retained_repair as integration
from aethermesh_core.artifact_admission import (
    AdmissionConflict,
    AdmissionLimits,
    ArtifactAdmission,
)
from aethermesh_core.artifact_directory import initialize_artifact_directory
from aethermesh_core.artifact_owned_inventory import (
    OwnedInventorySelection,
    audit_owned_inventory,
)
from aethermesh_core.artifact_repair_plan import RepairPlanLimits, plan_artifact_repair
from aethermesh_core.artifact_retention import RetentionPlan, retention_terms
from aethermesh_core.artifact_retention_store import (
    RetentionStore,
    RetentionStoreLimits,
)
from tests.test_artifact_codec import package


class RetainedRepairTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.pkg = package()
        self.sources = tuple(self.make_source(index) for index in range(4))
        self.before = tuple(source.store.snapshot() for source in self.sources)
        self.limits = RepairPlanLimits(16, 8, 2, 2)

    def make_source(self, index):
        path, ledger_path = (
            self.root / f"objects-{index}",
            self.root / f"ledger-{index}",
        )
        policy, store_id, key = AdmissionLimits(8, 2), f"{index:064x}", "a" * 64
        ledger = ArtifactAdmission.create(ledger_path, store_id=store_id, limits=policy)
        try:
            options = {
                "ledger_directory": ledger_path,
                "store_id": store_id,
                "limits": policy,
            }
            initialize_artifact_directory(path, **options)
            ledger.reserve(
                key, self.pkg.manifest, index, expected_digest=self.pkg.manifest.digest
            )
            file = path / (key + ".fragment")
            file.touch(mode=0o600)
            file.write_bytes(self.pkg.fragments[index])
            store = RetentionStore(
                path, **options, retention_limits=RetentionStoreLimits(2, 8, 16384)
            )
            store.enable(expected_ledger_revision=ledger.snapshot().revision, at=1)
            selections = (OwnedInventorySelection(key, self.pkg.manifest),)
            report = audit_owned_inventory(
                path, selections, **options, max_read_bytes=4
            )
            terms = retention_terms(
                self.pkg.manifest,
                report,
                key,
                store_id=store_id,
                limits=policy,
                max_duration_seconds=50,
                max_records=4,
            )
            plan = RetentionPlan("b" * 64, terms).advance(
                "accept",
                operation_id="c" * 64,
                expected_revision=0,
                at=2,
                until=40,
                evidence=report,
            )
            committed = store.commit(
                plan,
                selections,
                expected_revision=0,
                expected_ledger_revision=report.ledger.revision,
                max_read_bytes=4,
            )
            return integration.RetainedRepairSource(
                f"source-{index}",
                f"fixture-domain-{index}",
                store,
                selections,
                committed.state.ledger.revision,
            )
        finally:
            ledger.close()

    def run_plan(self, **kwargs):
        return integration.plan_retained_artifact_repair(
            self.pkg.manifest,
            kwargs.pop("sources", self.sources),
            kwargs.pop("targets", (4, 5)),
            **(
                {
                    "expected_digest": self.pkg.manifest.digest,
                    "limits": self.limits,
                    "max_audit_read_bytes": 16,
                    "now": 3,
                }
                | kwargs
            ),
        )

    def assert_unchanged(self):
        self.assertEqual(
            tuple(source.store.snapshot() for source in self.sources), self.before
        )

    def test_fresh_candidate_preserves_complete_evidence_and_charges(self):
        result = self.run_plan()
        self.assertEqual(result.candidate.status, "candidate")
        self.assertEqual(len(result.candidate.inputs), 4)
        self.assertEqual(result.audit_read_bytes, 16)
        self.assertTrue(result.requires_revalidation)
        self.assertFalse(
            result.serving_authorized
            or result.deletion_authorized
            or result.retention_confirmed
            or result.candidate.execution_authorized
            or result.candidate.publisher_authorized
            or result.candidate.failure_domains_verified
        )
        for evidence in result.evidence:
            self.assertEqual(evidence.inventory.ledger, evidence.state.ledger)
            self.assertEqual(evidence.eligibility[0].reason, "eligible")
            self.assertEqual(evidence.state.ledger.charged_bytes, 4)
        self.assert_unchanged()

    def test_elapsed_and_explicit_expiry_exclude_without_reclamation(self):
        elapsed = self.run_plan(now=40)
        self.assertEqual(elapsed.candidate.status, "blocked")
        self.assertTrue(
            all(e.eligibility[0].reason == "expired" for e in elapsed.evidence)
        )
        self.assert_unchanged()
        source = self.sources[0]
        state = source.store.snapshot()
        plan = state.plans[0].advance(
            "expire", operation_id="d" * 64, expected_revision=1, at=40
        )
        committed = source.store.commit(
            plan,
            (),
            expected_revision=1,
            expected_ledger_revision=state.ledger.revision,
            max_read_bytes=0,
        )
        with self.assertRaises(AdmissionConflict):
            self.run_plan(now=40)
        sources = (
            replace(source, expected_ledger_revision=committed.state.ledger.revision),
        ) + self.sources[1:]
        result = self.run_plan(sources=sources, now=40)
        self.assertEqual(result.evidence[0].eligibility[0].reason, "expired")
        self.assertEqual(committed.state.ledger.charged_bytes, 4)
        self.assertEqual(source.store.snapshot(), committed.state)

    def test_corrupt_missing_and_stale_admission_are_ineligible(self):
        source = self.sources[0]
        file = source.store.path / ("a" * 64 + ".fragment")
        file.write_bytes(b"bad!")
        result = self.run_plan()
        self.assertEqual(result.evidence[0].eligibility[0].reason, "unverified")
        self.assertEqual(result.candidate.status, "blocked")
        file.unlink()
        self.assertEqual(self.run_plan().evidence[0].eligibility[0].reason, "missing")
        file.touch(mode=0o600)
        file.write_bytes(self.pkg.fragments[0])
        ledger = ArtifactAdmission(
            source.store.ledger_directory,
            store_id=source.store.store_id,
            limits=source.store.limits,
        )
        try:
            ledger.invalidate(
                "a" * 64, expected_revision=ledger.snapshot().records[0].revision
            )
            revision = ledger.snapshot().revision
        finally:
            ledger.close()
        with self.assertRaises(AdmissionConflict):
            self.run_plan()
        sources = (replace(source, expected_ledger_revision=revision),) + self.sources[
            1:
        ]
        result = self.run_plan(sources=sources)
        self.assertEqual(result.evidence[0].eligibility[0].reason, "stale_admission")
        self.assertEqual(result.candidate.status, "blocked")
        self.assertEqual(result.evidence[0].state.ledger.charged_bytes, 4)

    def test_budget_clock_alias_and_cancellation_fail_without_changes(self):
        def cancel():
            raise InterruptedError("cancel")

        for kwargs in (
            {"max_audit_read_bytes": 15},
            {"now": 1},
            {"sources": (self.sources[0],) * 2},
            {"sources": ()},
            {"check_cancel": cancel},
        ):
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaises((ValueError, InterruptedError)),
            ):
                self.run_plan(**kwargs)
        self.assert_unchanged()

    def test_target_already_present_still_blocks_when_retention_ineligible(self):
        result = self.run_plan(now=40, targets=(0,))
        self.assertEqual(result.candidate.reason, "target_already_observed")
        self.assertEqual(result.candidate.inputs, ())
        self.assert_unchanged()

    def test_stale_audit_and_change_during_planning_are_rejected(self):
        original = integration._audit_locked

        def stale(*args, **kwargs):
            report = original(*args, **kwargs)
            return replace(
                report,
                ledger=replace(report.ledger, revision=report.ledger.revision - 1),
            )

        with (
            patch.object(integration, "_audit_locked", stale),
            self.assertRaises(AdmissionConflict),
        ):
            self.run_plan()
        original_state = RetentionStore._state
        calls = {}

        def changed(store, ledger):
            state = original_state(store, ledger)
            key = store.store_id
            calls[key] = calls.get(key, 0) + 1
            if calls[key] > 1:
                return replace(
                    state,
                    ledger=replace(state.ledger, revision=state.ledger.revision + 1),
                )
            return state

        with (
            patch.object(RetentionStore, "_state", changed),
            self.assertRaises(AdmissionConflict),
        ):
            self.run_plan()
        self.assert_unchanged()

    def test_process_restart_and_changed_revision_rejection(self):
        result = subprocess.run(
            [sys.executable, "-c", CHILD, str(self.root), "plan"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "candidate")
        source = self.sources[0]
        ledger = ArtifactAdmission(
            source.store.ledger_directory,
            store_id=source.store.store_id,
            limits=source.store.limits,
        )
        try:
            ledger.invalidate(
                "a" * 64, expected_revision=ledger.snapshot().records[0].revision
            )
        finally:
            ledger.close()
        result = subprocess.run(
            [sys.executable, "-c", CHILD, str(self.root), "plan"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "rejected")

    def test_concurrent_cooperative_writer_is_rejected_under_all_store_locks(self):
        original = integration.plan_artifact_repair

        def compete(*args, **kwargs):
            result = subprocess.run(
                [sys.executable, "-c", CHILD, str(self.root), "writer"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "rejected")
            return original(*args, **kwargs)

        with patch.object(integration, "plan_artifact_repair", compete):
            self.assertEqual(self.run_plan().candidate.status, "candidate")
        self.assert_unchanged()

    def test_direct_admission_writer_is_fenced_by_sqlite(self):
        original = integration.plan_artifact_repair

        def compete(*args, **kwargs):
            result = subprocess.run(
                [sys.executable, "-c", CHILD, str(self.root), "direct"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "admission-opened\nsqlite-busy")
            return original(*args, **kwargs)

        with patch.object(integration, "plan_artifact_repair", compete):
            self.assertEqual(self.run_plan().candidate.status, "candidate")
        self.assert_unchanged()

    def test_unretained_and_quarantined_bytes_remain_fully_charged(self):
        from aethermesh_core.artifact_quarantine import (
            FailedStagingRequest,
            inspect_failed_staging,
            quarantine_failed_staging,
        )

        source = self.sources[0]
        store = source.store
        key = "e" * 64
        ledger = ArtifactAdmission(
            store.ledger_directory, store_id=store.store_id, limits=store.limits
        )
        try:
            ledger.reserve(
                key, self.pkg.manifest, 0, expected_digest=self.pkg.manifest.digest
            )
            final = store.path / (key + ".fragment")
            final.touch(mode=0o600)
            final.write_bytes(self.pkg.fragments[0])
            selections = source.selections + (
                OwnedInventorySelection(key, self.pkg.manifest),
            )
            sources = (
                replace(
                    source,
                    selections=selections,
                    expected_ledger_revision=ledger.snapshot().revision,
                ),
            ) + self.sources[1:]
            result = self.run_plan(sources=sources, max_audit_read_bytes=20)
            self.assertEqual(result.evidence[0].eligibility[1].reason, "no_intent")
            self.assertEqual(result.candidate.status, "candidate")
            final.unlink()
            stage = store.path / (key + ".staging")
            stage.touch(mode=0o600)
            stage.write_bytes(b"x")
            request = FailedStagingRequest(
                store.path,
                self.pkg.manifest,
                0,
                self.pkg.manifest.digest,
                key,
                ledger_directory=store.ledger_directory,
                store_id=store.store_id,
                limits=store.limits,
                max_read_bytes=4,
            )
            quarantine_failed_staging(request, inspect_failed_staging(request))
            sources = (
                replace(
                    sources[0], expected_ledger_revision=ledger.snapshot().revision
                ),
            ) + sources[1:]
            before = ledger.snapshot()
            result = self.run_plan(sources=sources, max_audit_read_bytes=20)
            self.assertEqual(result.evidence[0].eligibility[1].reason, "quarantined")
            self.assertEqual(result.evidence[0].inventory.ledger.charged_bytes, 8)
            self.assertEqual(result.audit_read_bytes, 20)
            self.assertEqual(ledger.snapshot(), before)
            self.assertEqual((store.path / (key + ".quarantine")).read_bytes(), b"x")
        finally:
            ledger.close()

    def test_pure_filter_is_only_restrictive_and_requires_valid_inventory_ids(self):
        result = self.run_plan()
        inventories = result.candidate.evidence
        for allowed in (("f" * 64,), ("a" * 64, "a" * 64), ["a" * 64]):
            with self.subTest(allowed=allowed), self.assertRaises(ValueError):
                plan_artifact_repair(
                    self.pkg.manifest,
                    (replace(inventories[0], eligible_request_ids=allowed),)
                    + inventories[1:],
                    (4, 5),
                    expected_digest=self.pkg.manifest.digest,
                    limits=self.limits,
                    now=3,
                )
        blocked = plan_artifact_repair(
            self.pkg.manifest,
            (replace(inventories[0], eligible_request_ids=()),) + inventories[1:],
            (4, 5),
            expected_digest=self.pkg.manifest.digest,
            limits=self.limits,
            now=3,
        )
        self.assertEqual(blocked.status, "blocked")
        self.assertEqual(blocked.evidence[0].report, inventories[0].report)
        self.assert_unchanged()


CHILD = """
import sys, sqlite3
from pathlib import Path
from aethermesh_core.artifact_retained_repair import RetainedRepairSource,plan_retained_artifact_repair
from aethermesh_core.artifact_admission import AdmissionLimits,ArtifactAdmission
from aethermesh_core.artifact_owned_inventory import OwnedInventorySelection
from aethermesh_core.artifact_repair_plan import RepairPlanLimits
from aethermesh_core.artifact_retention_store import RetentionStore,RetentionStoreLimits
from tests.test_artifact_codec import package
root=Path(sys.argv[1]); pkg=package()
sources=tuple(RetainedRepairSource(f"source-{i}",f"fixture-domain-{i}",RetentionStore(root/f"objects-{i}",root/f"ledger-{i}",f"{i:064x}",AdmissionLimits(8,2),RetentionStoreLimits(2,8,16384)),(OwnedInventorySelection("a"*64,pkg.manifest),),4) for i in range(4))
if sys.argv[2]=="direct":
 store=sources[0].store
 ledger=ArtifactAdmission(store.ledger_directory,store_id=store.store_id,limits=store.limits)
 try:
  print("admission-opened",flush=True)
  ledger._db.execute("PRAGMA busy_timeout=50")
  ledger.invalidate("a"*64,expected_revision=ledger.snapshot().records[0].revision)
  print("mutated")
 except sqlite3.OperationalError as error:
  if error.sqlite_errorcode != sqlite3.SQLITE_BUSY: raise
  print("sqlite-busy")
 finally: ledger.close()
 sys.exit(0)
try:
 if sys.argv[2]=="writer":
  sources[0].store.enable(expected_ledger_revision=4,at=3)
  print("opened")
 else:
  result=plan_retained_artifact_repair(pkg.manifest,sources,(4,5),expected_digest=pkg.manifest.digest,limits=RepairPlanLimits(16,8,2,2),max_audit_read_bytes=16,now=3)
  print(result.candidate.status)
except (ValueError,OSError): print("rejected")
"""
