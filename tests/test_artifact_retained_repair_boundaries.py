"""Local fault injection at the reviewed retained-planning boundary; no publication."""

import unittest
from dataclasses import replace
from unittest.mock import patch

from aethermesh_core import artifact_retained_repair as integration
from aethermesh_core.artifact_admission import AdmissionConflict, ArtifactAdmission
from aethermesh_core.artifact_owned_inventory import (
    OwnedInventorySelection,
    audit_owned_inventory,
)
from aethermesh_core.artifact_repair_plan import RepairPlanLimits
from aethermesh_core.artifact_retention import RetentionPlan, retention_terms
from aethermesh_core.artifact_retention_store import RetentionStore
from tests import test_artifact_retained_repair as fixture
from tests.test_artifact_codec import package


class RetentionBoundaryTests(unittest.TestCase):
    setUp = fixture.RetainedRepairTests.setUp
    make_source = fixture.RetainedRepairTests.make_source
    run_plan = fixture.RetainedRepairTests.run_plan
    assert_unchanged = fixture.RetainedRepairTests.assert_unchanged

    def test_cross_store_report_with_same_revision_and_charge_is_rejected(self):
        source = self.sources[1]
        report = audit_owned_inventory(
            source.store.path,
            source.selections,
            ledger_directory=source.store.ledger_directory,
            store_id=source.store.store_id,
            limits=source.store.limits,
            max_read_bytes=4,
        )
        self.assertEqual(report.ledger.revision, self.before[0].ledger.revision)
        self.assertEqual(
            report.ledger.charged_bytes, self.before[0].ledger.charged_bytes
        )
        with (
            patch.object(integration, "_audit_locked", return_value=report),
            self.assertRaises(AdmissionConflict),
        ):
            self.run_plan()
        self.assert_unchanged()

    def test_change_in_not_yet_locked_store_rejects_mixed_snapshot(self):
        original = RetentionStore._state
        changed = False

        def state_then_change_later_store(store, ledger):
            nonlocal changed
            state = original(store, ledger)
            if not changed:
                changed = True
                target = self.sources[-1].store
                writer = ArtifactAdmission(
                    target.ledger_directory,
                    store_id=target.store_id,
                    limits=target.limits,
                )
                try:
                    row = writer.snapshot().records[0]
                    writer.invalidate(row.request_id, expected_revision=row.revision)
                finally:
                    writer.close()
            return state

        with (
            patch.object(RetentionStore, "_state", state_then_change_later_store),
            patch.object(integration, "_audit_locked") as audit,
            self.assertRaises(AdmissionConflict),
        ):
            self.run_plan()
        audit.assert_not_called()
        after = tuple(source.store.snapshot() for source in self.sources)
        self.assertEqual(after[:-1], self.before[:-1])
        self.assertEqual(after[-1].ledger.revision, self.before[-1].ledger.revision + 1)
        self.assertEqual(
            after[-1].ledger.charged_bytes, self.before[-1].ledger.charged_bytes
        )
        sources = self.sources[:-1] + (
            replace(
                self.sources[-1], expected_ledger_revision=after[-1].ledger.revision
            ),
        )
        result = self.run_plan(sources=sources)
        self.assertEqual(result.candidate.status, "blocked")
        self.assertEqual(result.evidence[-1].eligibility[0].reason, "stale_admission")

    def test_transaction_change_after_observation_before_proposal_rolls_back(self):
        original = integration._audit_locked
        ledgers = []

        def audit_then_change_first(*args, **kwargs):
            report = original(*args, **kwargs)
            ledgers.append(kwargs["ledger"])
            if len(ledgers) == 4:
                # Fault injection inside the held transaction, after the first audit.
                ledgers[0]._db.execute("UPDATE catalog SET revision=revision+1")
            return report

        with (
            patch.object(integration, "_audit_locked", audit_then_change_first),
            patch.object(
                integration,
                "plan_artifact_repair",
                wraps=integration.plan_artifact_repair,
            ) as propose,
            self.assertRaisesRegex(AdmissionConflict, "changed during planning"),
        ):
            self.run_plan()
        propose.assert_called_once()
        self.assert_unchanged()
        self.assertEqual(self.run_plan().candidate.status, "candidate")

    def test_exact_clock_boundaries_and_read_only_clock_limit(self):
        for now, status, reason in (
            (2, "candidate", "eligible"),
            (39, "candidate", "eligible"),
            (40, "blocked", "expired"),
            (41, "blocked", "expired"),
            (100, "blocked", "expired"),
        ):
            with self.subTest(now=now):
                result = self.run_plan(now=now)
                self.assertEqual(result.candidate.status, status)
                self.assertTrue(
                    all(row.eligibility[0].reason == reason for row in result.evidence)
                )
                self.assertFalse(
                    result.serving_authorized or result.retention_confirmed
                )
                self.assert_unchanged()
        with self.assertRaises(AdmissionConflict):
            self.run_plan(now=1)
        # Documented limit: read-only observations do not persist clock advancement.
        self.assertEqual(self.run_plan(now=39).candidate.status, "candidate")
        self.assert_unchanged()

    def test_later_clock_in_one_store_fences_the_whole_call(self):
        source = self.sources[0]
        store = source.store
        report = audit_owned_inventory(
            store.path,
            source.selections,
            ledger_directory=store.ledger_directory,
            store_id=store.store_id,
            limits=store.limits,
            max_read_bytes=4,
        )
        renewed = (
            self.before[0]
            .plans[0]
            .advance(
                "renew",
                operation_id="d" * 64,
                expected_revision=1,
                at=20,
                until=60,
                evidence=report,
            )
        )
        state = store.commit(
            renewed,
            source.selections,
            expected_revision=1,
            expected_ledger_revision=report.ledger.revision,
            max_read_bytes=4,
        ).state
        sources = (
            replace(source, expected_ledger_revision=state.ledger.revision),
        ) + self.sources[1:]
        with self.assertRaises(AdmissionConflict):
            self.run_plan(sources=sources, now=19)
        self.assertEqual(
            self.run_plan(sources=sources, now=20).candidate.status, "candidate"
        )
        at_other_deadline = self.run_plan(sources=sources, now=40)
        self.assertEqual(at_other_deadline.candidate.status, "blocked")
        self.assertEqual(
            at_other_deadline.evidence[0].eligibility[0].reason, "eligible"
        )
        self.assertTrue(
            all(
                row.eligibility[0].reason == "expired"
                for row in at_other_deadline.evidence[1:]
            )
        )
        self.assertEqual(store.snapshot(), state)

    def test_all_stripe_and_audit_budgets_with_unequal_final_stripe(self):
        self.root = self.root / "two-stripes"
        self.root.mkdir(mode=0o700)
        self.pkg = package(bytes(range(21)), fragment_bytes=4)
        sources = []
        for index in range(4):
            source = self.make_source(index)
            store = source.store
            ledger = ArtifactAdmission(
                store.ledger_directory, store_id=store.store_id, limits=store.limits
            )
            try:
                key = "d" * 64
                ledger.reserve(
                    key,
                    self.pkg.manifest,
                    index + 6,
                    expected_digest=self.pkg.manifest.digest,
                )
                path = store.path / (key + ".fragment")
                path.touch(mode=0o600)
                path.write_bytes(self.pkg.fragments[index + 6])
                selections = source.selections + (
                    OwnedInventorySelection(key, self.pkg.manifest),
                )
                report = audit_owned_inventory(
                    store.path,
                    selections,
                    ledger_directory=store.ledger_directory,
                    store_id=store.store_id,
                    limits=store.limits,
                    max_read_bytes=8,
                )
                terms = retention_terms(
                    self.pkg.manifest,
                    report,
                    key,
                    store_id=store.store_id,
                    limits=store.limits,
                    max_duration_seconds=50,
                    max_records=4,
                )
                plan = RetentionPlan("e" * 64, terms).advance(
                    "accept",
                    operation_id="f" * 64,
                    expected_revision=0,
                    at=2,
                    until=40,
                    evidence=report,
                )
                state = store.commit(
                    plan,
                    selections,
                    expected_revision=0,
                    expected_ledger_revision=report.ledger.revision,
                    max_read_bytes=8,
                ).state
                sources.append(
                    replace(
                        source,
                        selections=selections,
                        expected_ledger_revision=state.ledger.revision,
                    )
                )
            finally:
                ledger.close()
        self.sources = tuple(sources)
        self.before = tuple(source.store.snapshot() for source in self.sources)
        limits = RepairPlanLimits(24, 2, 3, 1)
        result = self.run_plan(targets=(10,), limits=limits, max_audit_read_bytes=24)
        self.assertEqual(result.candidate.status, "candidate")
        self.assertEqual(result.audit_read_bytes, 24)
        self.assertEqual(result.candidate.required_read_bytes, 24)
        self.assertEqual(result.candidate.required_write_bytes, 2)
        self.assertEqual(
            [row.size for row in result.candidate.inputs], [4] * 4 + [2] * 4
        )
        self.assertEqual(
            [len(batch) for batch in result.candidate.input_batches], [3, 3, 2]
        )
        for short in (
            replace(limits, max_read_bytes=23),
            replace(limits, max_write_bytes=1),
        ):
            with self.subTest(short=short):
                blocked = self.run_plan(
                    targets=(10,), limits=short, max_audit_read_bytes=24
                )
                self.assertEqual(blocked.candidate.reason, "byte_budget")
                self.assertEqual(blocked.candidate.required_read_bytes, 24)
                self.assertEqual(blocked.candidate.inputs, ())
        with (
            patch.object(integration, "_audit_locked") as audit,
            self.assertRaisesRegex(ValueError, "aggregate"),
        ):
            self.run_plan(targets=(10,), limits=limits, max_audit_read_bytes=23)
        audit.assert_not_called()
        self.assert_unchanged()

    def test_cancellation_after_first_store_releases_partial_locks(self):
        original = RetentionStore._state
        opened = []

        def observe_open(store, ledger):
            state = original(store, ledger)
            opened.append(store.store_id)
            return state

        def cancel_after_first():
            if opened:
                raise InterruptedError("cancel after first locked snapshot")

        with (
            patch.object(RetentionStore, "_state", observe_open),
            patch.object(integration, "_audit_locked") as audit,
            self.assertRaises(InterruptedError),
        ):
            self.run_plan(check_cancel=cancel_after_first)
        self.assertEqual(len(opened), 1)
        audit.assert_not_called()
        self.assert_unchanged()
        self.assertEqual(self.run_plan().candidate.status, "candidate")

    def test_cancellation_inside_payload_audit_releases_all_locks(self):
        from aethermesh_core import artifact_inventory

        original = artifact_inventory.verify_fragment_file
        verifying = False

        def verify(*args, **kwargs):
            nonlocal verifying
            verifying = True
            return original(*args, **kwargs)

        def cancel_in_verify():
            if verifying:
                raise InterruptedError("cancel during fragment verification")

        with (
            patch.object(
                artifact_inventory, "verify_fragment_file", side_effect=verify
            ) as observed,
            self.assertRaises(InterruptedError),
        ):
            self.run_plan(check_cancel=cancel_in_verify)
        observed.assert_called_once()
        self.assert_unchanged()
        self.assertEqual(self.run_plan().candidate.status, "candidate")
