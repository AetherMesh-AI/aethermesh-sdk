"""Durable local intent requires original ownership and fresh bounded evidence."""

import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import artifact_retention_store as durable
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
from aethermesh_core.artifact_retention import RetentionPlan, retention_terms
from tests.test_artifact_codec import package


class RetentionStoreTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.path, self.ledger_path = self.root / "objects", self.root / "ledger"
        self.limits = AdmissionLimits(32, 4)
        self.options = {
            "ledger_directory": self.ledger_path,
            "store_id": "f" * 64,
            "limits": self.limits,
        }
        self.ledger = ArtifactAdmission.create(
            self.ledger_path, store_id="f" * 64, limits=self.limits
        )
        self.addCleanup(self.ledger.close)
        initialize_artifact_directory(self.path, **self.options)
        self.pkg = package()
        self.key = "a" * 64
        self.ledger.reserve(
            self.key, self.pkg.manifest, 0, expected_digest=self.pkg.manifest.digest
        )
        self.file = self.path / (self.key + ".fragment")
        self.file.touch(mode=0o600)
        self.file.write_bytes(self.pkg.fragments[0])
        self.selections = (OwnedInventorySelection(self.key, self.pkg.manifest),)
        self.store = durable.RetentionStore(
            self.path,
            **self.options,
            retention_limits=durable.RetentionStoreLimits(4, 16, 65536),
        )
        self.before = self.ledger.snapshot()

    def enable(self):
        return self.store.enable(
            expected_ledger_revision=self.ledger.snapshot().revision, at=1
        )

    def audit(self):
        return audit_owned_inventory(
            self.path, self.selections, **self.options, max_read_bytes=16
        )

    def accept(self, operation="c", intent="b"):
        report = self.audit()
        terms = retention_terms(
            self.pkg.manifest,
            report,
            self.key,
            store_id=self.options["store_id"],
            limits=self.limits,
            max_duration_seconds=50,
            max_records=8,
        )
        return RetentionPlan(intent * 64, terms).advance(
            "accept",
            operation_id=operation * 64,
            expected_revision=0,
            at=2,
            until=40,
            evidence=report,
        )

    def commit(self, plan, **kwargs):
        return self.store.commit(
            plan,
            self.selections,
            **(
                {
                    "expected_revision": len(plan.records) - 1,
                    "expected_ledger_revision": self.ledger.snapshot().revision,
                    "max_read_bytes": 16,
                }
                | kwargs
            ),
        )

    def expire(self, plan):
        expired = plan.advance(
            "expire",
            operation_id="e" * 64,
            expected_revision=len(plan.records),
            at=plan.records[-1].until,
        )
        return self.store.commit(
            expired,
            (),
            expected_revision=len(plan.records),
            expected_ledger_revision=self.ledger.snapshot().revision,
            max_read_bytes=0,
        )

    def assert_charges(self):
        snapshot = self.ledger.snapshot()
        self.assertEqual(snapshot.records, self.before.records)
        self.assertEqual(snapshot.charged_bytes, self.before.charged_bytes)

    def test_restart_renew_expire_and_exact_old_retry(self):
        enabled = self.enable()
        self.assertEqual(enabled.ledger.revision, self.before.revision + 1)
        plan = self.accept()
        revision = self.ledger.snapshot().revision
        accepted = self.commit(plan)
        self.assertTrue(accepted.durable_committed and accepted.verification_performed)
        self.assertFalse(
            accepted.retention_confirmed
            or accepted.publisher_authorized
            or accepted.serving_authorized
        )
        report = self.audit()
        renewed = plan.advance(
            "renew",
            operation_id="d" * 64,
            expected_revision=1,
            at=20,
            until=60,
            evidence=report,
        )
        self.commit(renewed)
        expired = self.expire(renewed)
        self.assertEqual(expired.state.active_count, 0)
        self.assertFalse(expired.verification_performed)
        self.assertEqual(replace(self.store).snapshot(), expired.state)
        replay = self.commit(plan, expected_ledger_revision=revision)
        self.assertTrue(replay.replayed)
        self.assertFalse(replay.verification_performed)
        self.assertEqual(replay.state, expired.state)
        self.assertEqual(replay.operation, accepted.operation)
        with self.assertRaises(AdmissionConflict):
            self.commit(plan, expected_ledger_revision=revision, max_read_bytes=15)
        self.assert_charges()

    def test_prepare_does_not_commit_and_changed_bytes_fail_closed(self):
        with self.assertRaises(ValueError):
            self.store.snapshot()
        self.enable()
        plan = self.accept()
        empty = self.store.snapshot()
        self.assertEqual(empty.plans, ())
        self.file.write_bytes(b"bad!")
        with self.assertRaises(ValueError):
            self.commit(plan)
        self.assertEqual(self.store.snapshot(), empty)
        self.file.unlink()
        with self.assertRaises(ValueError):
            self.commit(plan)
        self.assertEqual(self.store.snapshot(), empty)
        self.assert_charges()

    def test_expiry_requires_no_bytes_and_preserves_lifetime_claim(self):
        self.enable()
        plan = self.accept()
        self.commit(plan)
        self.file.unlink()
        receipt = self.expire(plan)
        self.assertEqual(receipt.state.clock_high_water, 40)
        self.assert_charges()
        self.file.touch(mode=0o600)
        self.file.write_bytes(self.pkg.fragments[0])
        second = self.accept(operation="f", intent="d")
        second = replace(second, records=(replace(second.records[0], at=41, until=60),))
        with self.assertRaises(AdmissionConflict):
            self.commit(second)

    def test_expected_revisions_and_cancellation_are_atomic(self):
        self.enable()
        plan = self.accept()
        state = self.store.snapshot()
        for kwargs in ({"expected_revision": 1}, {"expected_ledger_revision": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(AdmissionConflict):
                self.commit(plan, **kwargs)

        def cancel():
            raise InterruptedError("cancel")

        with self.assertRaises(InterruptedError):
            self.commit(plan, check_cancel=cancel)
        self.assertEqual(self.store.snapshot(), state)
        with (
            patch.object(
                durable.RetentionStore,
                "_finish",
                side_effect=InterruptedError("uncertain acknowledgment"),
            ),
            self.assertRaises(InterruptedError),
        ):
            self.commit(plan)
        replay = self.commit(plan, expected_ledger_revision=state.ledger.revision)
        self.assertTrue(replay.replayed)
        self.assertEqual(len(replay.state.operations), 1)
        self.assert_charges()

    def test_terminal_capacity_is_reserved(self):
        self.store = replace(
            self.store, retention_limits=durable.RetentionStoreLimits(1, 2, 65536)
        )
        self.enable()
        plan = self.accept()
        accepted = self.commit(plan)
        renewed = plan.advance(
            "renew",
            operation_id="d" * 64,
            expected_revision=1,
            at=20,
            until=60,
            evidence=self.audit(),
        )
        with self.assertRaises(ValueError):
            self.commit(renewed)
        self.assertEqual(self.store.snapshot(), accepted.state)
        self.expire(plan)
        self.assert_charges()

    def test_enable_is_explicit_atomic_and_policy_frozen(self):
        with self.assertRaises(AdmissionConflict):
            self.store.enable(expected_ledger_revision=0, at=1)
        self.assertEqual(self.ledger.snapshot(), self.before)
        enabled = self.enable()
        self.assertEqual(self.store.enable(expected_ledger_revision=0, at=100), enabled)
        with self.assertRaises(ValueError):
            replace(
                self.store, retention_limits=durable.RetentionStoreLimits(1, 2, 65536)
            ).enable(expected_ledger_revision=0, at=100)
        self.assert_charges()

    def test_real_process_crashes_and_identical_retry(self):
        self.enable()
        plan = self.accept()
        original = self.store.snapshot()
        request = self.root / "request.json"
        request.write_bytes(plan.encode())
        for mode, code in (("before", 73), ("after", 74), ("ack", 75)):
            with self.subTest(mode=mode):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        CHILD,
                        str(self.root),
                        mode,
                        str(original.ledger.revision),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, code, result.stderr)
                state = self.store.snapshot()
                if mode == "before":
                    self.assertEqual(state, original)
                else:
                    self.assertEqual(len(state.operations), 1)
                    replay = self.commit(
                        plan, expected_ledger_revision=original.ledger.revision
                    )
                    self.assertTrue(replay.replayed)
        self.assert_charges()

    def test_competing_processes_have_one_commit(self):
        self.enable()
        plan = self.accept()
        (self.root / "request.json").write_bytes(plan.encode())
        revision = self.ledger.snapshot().revision
        children = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    CHILD,
                    str(self.root),
                    "race",
                    str(revision),
                    op,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for op in ("c", "d")
        ]
        try:
            for child in children:
                self.assertEqual(child.stdout.readline().strip(), "ready")
            for child in children:
                child.stdin.write("go\n")
                child.stdin.flush()
            results = [child.communicate(timeout=10) for child in children]
            self.assertEqual(
                sorted(stdout.strip() for stdout, _ in results),
                ["committed", "rejected"],
                results,
            )
            self.assertTrue(all(child.returncode == 0 for child in children), results)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate()
        self.assertEqual(len(self.store.snapshot().operations), 1)
        self.assert_charges()

    def test_global_revision_reserves_terminal_slot_for_admission_writers(self):
        self.ledger._db.execute("UPDATE catalog SET revision=4092")
        self.ledger._db.commit()
        self.enable()
        plan = self.accept()
        self.commit(plan)
        row = self.ledger.snapshot().records[0]
        self.ledger.invalidate(self.key, expected_revision=row.revision)
        row = self.ledger.snapshot().records[0]
        with self.assertRaises(ValueError):
            self.ledger.invalidate(self.key, expected_revision=row.revision)
        expired = self.expire(plan)
        self.assertEqual(expired.state.ledger.revision, 4096)
        self.assertEqual(expired.state.ledger.charged_bytes, self.before.charged_bytes)

    def test_fresh_evidence_and_accounting_fences(self):
        self.enable()
        plan = self.accept()
        state = self.store.snapshot()
        false_record = replace(plan.records[0], evidence_digest="0" * 64)
        with self.assertRaises(AdmissionConflict):
            self.commit(replace(plan, records=(false_record,)))
        wrong_terms = replace(
            plan.terms,
            admission=replace(plan.terms.admission, state="verified_observation"),
        )
        with self.assertRaises(AdmissionConflict):
            self.commit(
                RetentionPlan(plan.intent_id, wrong_terms).advance(
                    "accept",
                    operation_id="c" * 64,
                    expected_revision=0,
                    at=2,
                    until=40,
                    evidence=self.audit(),
                )
            )
        self.assertEqual(self.store.snapshot(), state)

    def test_committed_accounting_change_rolls_back(self):
        self.enable()
        plan = self.accept()
        state = self.store.snapshot()
        original = durable.RetentionStore._write

        def write_with_bad_accounting(store, ledger, current, proposed, request):
            operation = original(store, ledger, current, proposed, request)
            row = ledger._snapshot().records[0]
            updated = replace(row, state="verified_observation")
            ledger._db.execute(
                "UPDATE fragments SET state=?, checksum=?",
                (updated.state, updated.checksum),
            )
            return operation

        with (
            patch.object(durable.RetentionStore, "_write", write_with_bad_accounting),
            self.assertRaises(AdmissionConflict),
        ):
            self.commit(plan)
        self.assertEqual(self.store.snapshot(), state)
        self.assert_charges()

    def test_unknown_prepared_admission_is_not_authority(self):
        self.enable()
        report = self.audit()
        invented = replace(report.ledger.records[0], request_id="f" * 64)
        fake = replace(
            report,
            ledger=replace(report.ledger, records=(invented,)),
            observations=(replace(report.observations[0], admission=invented),),
        )
        terms = retention_terms(
            self.pkg.manifest,
            fake,
            invented.request_id,
            store_id=self.options["store_id"],
            limits=self.limits,
            max_duration_seconds=50,
            max_records=8,
        )
        plan = RetentionPlan("b" * 64, terms).advance(
            "accept",
            operation_id="c" * 64,
            expected_revision=0,
            at=2,
            until=40,
            evidence=fake,
        )
        with self.assertRaises(AdmissionConflict):
            self.commit(plan)
        self.assertEqual(self.store.snapshot().plans, ())

    def test_missing_table_never_recreated(self):
        self.enable()
        self.commit(self.accept())
        self.ledger._db.execute("DROP TABLE retention_operations")
        self.ledger._db.commit()
        for action in (
            self.store.snapshot,
            lambda: self.store.enable(expected_ledger_revision=0, at=1),
        ):
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                action()
        self.assertEqual(
            self.ledger._db.execute("SELECT SUM(size) FROM fragments").fetchone()[0],
            self.before.charged_bytes,
        )
        self.assertEqual(self.file.read_bytes(), self.pkg.fragments[0])

    def test_v1_orphan_tables_require_explicit_enablement(self):
        self.ledger._db.execute("CREATE TABLE retention_orphan (value TEXT)")
        self.ledger._db.commit()
        with self.assertRaisesRegex(ValueError, "explicit version-2"):
            self.enable()
        self.assertEqual(
            self.ledger._db.execute("SELECT version FROM catalog").fetchone()[0], 1
        )

    def test_catalog_corruption_fails_closed_without_repair(self):
        self.enable()
        self.commit(self.accept())
        self.ledger._db.execute("UPDATE retention_catalog SET clock=clock+1")
        self.ledger._db.commit()
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.store.snapshot()
        self.assertEqual(self.file.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(
            self.ledger._db.execute("SELECT SUM(size) FROM fragments").fetchone()[0],
            self.before.charged_bytes,
        )

    def test_bounded_policies_and_request_validation(self):
        for args in (
            (0, 2, 4096),
            (33, 2, 4096),
            (1, 1, 4096),
            (1, 1025, 4096),
            (1, 2, 4095),
            (1, 2, 1048577),
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                durable.RetentionStoreLimits(*args)
        self.enable()
        plan = self.accept()
        for proposed, selections, kwargs in (
            (None, self.selections, {}),
            (RetentionPlan(plan.intent_id, plan.terms), self.selections, {}),
            (plan, list(self.selections), {}),
            (plan, (), {}),
            (plan, self.selections, {"max_read_bytes": 3}),
        ):
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.store.commit(
                    proposed,
                    selections,
                    **(
                        {
                            "expected_revision": 0,
                            "expected_ledger_revision": self.ledger.snapshot().revision,
                            "max_read_bytes": 16,
                        }
                        | kwargs
                    ),
                )
        self.assertEqual(self.store.snapshot().plans, ())

    def test_cancellation_after_audit_and_after_commit(self):
        self.enable()
        plan = self.accept()
        state = self.store.snapshot()

        def cancel():
            raise InterruptedError("cancel")

        original = durable._audit_locked

        def audit_then_cancel(*args, **kwargs):
            original(*args, **kwargs)
            cancel()

        with (
            patch.object(durable, "_audit_locked", audit_then_cancel),
            self.assertRaises(InterruptedError),
        ):
            self.commit(plan)
        self.assertEqual(self.store.snapshot(), state)
        calls = 0

        def after_commit():
            nonlocal calls
            calls += 1
            if self.ledger._db.execute(
                "SELECT COUNT(*) FROM retention_operations"
            ).fetchone()[0]:
                cancel()

        with self.assertRaises(InterruptedError):
            self.commit(plan, check_cancel=after_commit)
        self.assertGreater(calls, 1)
        self.assertTrue(
            self.commit(plan, expected_ledger_revision=state.ledger.revision).replayed
        )
        self.assert_charges()

    def test_clock_rollback_and_stale_preparation_fail(self):
        self.enable()
        plan = self.accept()
        self.commit(plan)
        expired = self.expire(plan)
        second = self.accept(operation="f", intent="d")
        with self.assertRaisesRegex(AdmissionConflict, "clock"):
            self.commit(second)
        self.assertEqual(self.store.snapshot(), expired.state)

    def test_metadata_budget_reserves_expiry(self):
        self.store = replace(
            self.store, retention_limits=durable.RetentionStoreLimits(4, 16, 4096)
        )
        self.enable()
        plan = self.accept()
        self.commit(plan)
        blocked = False
        for index in range(1, 7):
            renewed = plan.advance(
                "renew",
                operation_id=f"{index:064x}",
                expected_revision=len(plan.records),
                at=2 + index,
                until=40 + index,
                evidence=self.audit(),
            )
            try:
                self.commit(renewed)
            except ValueError as error:
                self.assertIn("reserve", str(error))
                blocked = True
                break
            plan = renewed
        self.assertTrue(blocked)
        self.expire(plan)
        self.assert_charges()

    def test_sqlite_full_rolls_back_enablement(self):
        original = durable.RetentionStore._open

        @contextmanager
        def no_more_pages(store):
            with original(store) as (ledger, directory):
                count = ledger._db.execute("PRAGMA page_count").fetchone()[0]
                ledger._db.execute(f"PRAGMA max_page_count={count}")
                yield ledger, directory

        with (
            patch.object(durable.RetentionStore, "_open", no_more_pages),
            self.assertRaises(sqlite3.OperationalError) as caught,
        ):
            self.enable()
        self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_FULL)
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertEqual(
            self.ledger._db.execute("SELECT version FROM catalog").fetchone()[0], 1
        )
        self.assertEqual(
            self.ledger._db.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'retention_%'"
            ).fetchall(),
            [],
        )
        self.enable()
        self.assert_charges()

    def test_lost_marker_and_copied_database_never_restore_ownership(self):
        self.enable()
        plan = self.accept()
        self.commit(plan)
        marker = self.path / "store.json"
        original = marker.read_bytes()
        marker.unlink()
        with self.assertRaises((ValueError, OSError)):
            self.expire(plan)
        # This is fixture restoration only; production has no marker recreation API.
        marker.touch(mode=0o600)
        marker.write_bytes(original)
        copied = self.root / "copied"
        shutil.copytree(self.ledger_path, copied)
        with self.assertRaises(ValueError):
            replace(self.store, ledger_directory=copied).snapshot()
        self.assertEqual(self.store.snapshot().active_count, 1)
        self.assert_charges()

    def test_enablement_process_crash_is_atomic(self):
        for mode, code in (("enable-before", 76), ("enable-after", 77)):
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    CHILD,
                    str(self.root),
                    mode,
                    str(self.before.revision),
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(result.returncode, code, result.stderr)
            if mode == "enable-before":
                self.assertEqual(self.ledger.snapshot(), self.before)
                self.assertEqual(
                    self.ledger._db.execute(
                        "SELECT name FROM sqlite_master WHERE name LIKE 'retention_%'"
                    ).fetchall(),
                    [],
                )
            else:
                self.assertEqual(self.store.snapshot(), self.enable())
        self.assert_charges()

    def test_distinct_admission_cannot_exceed_intent_limit(self):
        self.store = replace(
            self.store, retention_limits=durable.RetentionStoreLimits(1, 16, 65536)
        )
        self.enable()
        self.commit(self.accept())
        self.key = "d" * 64
        self.ledger.reserve(
            self.key, self.pkg.manifest, 0, expected_digest=self.pkg.manifest.digest
        )
        extra = self.path / (self.key + ".fragment")
        extra.touch(mode=0o600)
        extra.write_bytes(self.pkg.fragments[0])
        self.selections += (OwnedInventorySelection(self.key, self.pkg.manifest),)
        plan = self.accept(operation="f", intent="e")
        before = self.store.snapshot()
        with self.assertRaisesRegex(ValueError, "record bound"):
            self.commit(plan)
        self.assertEqual(self.store.snapshot(), before)

    def test_sqlite_full_during_commit_preserves_last_complete_state(self):
        self.enable()
        plan = self.accept()
        plan = RetentionPlan(
            plan.intent_id, replace(plan.terms, max_records=16)
        ).advance(
            "accept",
            operation_id="c" * 64,
            expected_revision=0,
            at=2,
            until=40,
            evidence=self.audit(),
        )
        self.commit(plan)
        original = durable.RetentionStore._open

        @contextmanager
        def no_more_pages(store):
            with original(store) as (ledger, directory):
                count = ledger._db.execute("PRAGMA page_count").fetchone()[0]
                ledger._db.execute(f"PRAGMA max_page_count={count}")
                yield ledger, directory

        failed = None
        for index in range(1, 15):
            renewed = plan.advance(
                "renew",
                operation_id=f"{index:064x}",
                expected_revision=len(plan.records),
                at=2 + index,
                until=40 + index,
                evidence=self.audit(),
            )
            before = self.store.snapshot()
            with patch.object(durable.RetentionStore, "_open", no_more_pages):
                try:
                    self.commit(renewed)
                except sqlite3.OperationalError as error:
                    self.assertEqual(error.sqlite_errorcode, sqlite3.SQLITE_FULL)
                    failed = renewed
                    break
            plan = renewed
        self.assertIsNotNone(failed)
        self.assertEqual(self.store.snapshot(), before)
        self.commit(failed)
        self.expire(failed)
        self.assert_charges()


CHILD = """
import os, sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from aethermesh_core.artifact_retention_store import RetentionStore, RetentionStoreLimits
from aethermesh_core.artifact_admission import AdmissionLimits
from aethermesh_core.artifact_owned_inventory import OwnedInventorySelection
from aethermesh_core.artifact_retention import RetentionPlan
from tests.test_artifact_codec import package
root = Path(sys.argv[1])
mode = sys.argv[2]
store = RetentionStore(root/"objects", root/"ledger", "f"*64, AdmissionLimits(32,4), RetentionStoreLimits(4,16,65536))
if mode.startswith("enable-"):
 from aethermesh_core import _artifact_retention_db as storage
 original = storage.seal
 if mode == "enable-before":
  def crash(ledger):
   original(ledger)
   os._exit(76)
  storage.seal = crash
 store.enable(expected_ledger_revision=int(sys.argv[3]),at=1)
 os._exit(77)
plan = RetentionPlan.decode((root/"request.json").read_bytes())
if mode == "race":
 intent = sys.argv[4]*64
 root_digest = RetentionPlan(intent,plan.terms).root_digest
 plan = replace(plan, intent_id=intent, records=(replace(plan.records[0],operation_id=sys.argv[4]*64,previous_digest=root_digest),))
 print("ready", flush=True)
 sys.stdin.readline()
if mode == "before":
 original = RetentionStore._write
 def crash(*args):
  original(*args)
  os._exit(73)
 RetentionStore._write = crash
if mode == "after":
 RetentionStore._finish = lambda *args: os._exit(74)
try:
 store.commit(plan,(OwnedInventorySelection("a"*64, package().manifest),),expected_revision=0,expected_ledger_revision=int(sys.argv[3]),max_read_bytes=16)
 if mode == "ack": os._exit(75)
 print("committed",flush=True)
except (ValueError, OSError):
 print("rejected",flush=True)
"""
