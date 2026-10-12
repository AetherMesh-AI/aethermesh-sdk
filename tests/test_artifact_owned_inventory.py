"""Owned audit fences local accounting; it never grants a retention promise."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import artifact_owned_inventory as owned
from aethermesh_core.artifact_admission import (
    AdmissionConflict,
    AdmissionLimits,
    ArtifactAdmission,
)
from aethermesh_core.artifact_directory import initialize_artifact_directory
from aethermesh_core.artifact_files import verify_fragment_file
from aethermesh_core.artifact_quarantine import (
    FailedStagingRequest,
    inspect_failed_staging,
    quarantine_failed_staging,
)
from tests.test_artifact_codec import package


class OwnedInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.path, self.ledger_path = self.root / "objects", self.root / "ledger"
        self.policy = AdmissionLimits(max_bytes=32, max_objects=4)
        self.options = {
            "ledger_directory": self.ledger_path,
            "store_id": "f" * 64,
            "limits": self.policy,
        }
        self.ledger = ArtifactAdmission.create(
            self.ledger_path, store_id="f" * 64, limits=self.policy
        )
        self.addCleanup(self.ledger.close)
        initialize_artifact_directory(self.path, **self.options)
        self.pkg = package()
        self.ids = tuple(c * 64 for c in "abcd")
        self.selections = tuple(
            owned.OwnedInventorySelection(key, self.pkg.manifest) for key in self.ids
        )
        for key in self.ids:
            self.ledger.reserve(
                key, self.pkg.manifest, 0, expected_digest=self.pkg.manifest.digest
            )
        self.good = self.path / (self.ids[0] + ".fragment")
        self.bad = self.path / (self.ids[2] + ".fragment")
        for path in (self.good, self.bad):
            path.touch(mode=0o600)
            path.write_bytes(self.pkg.fragments[0])
        verified = verify_fragment_file(
            self.pkg.manifest,
            0,
            self.bad,
            expected_digest=self.pkg.manifest.digest,
            max_read_bytes=4,
        )
        record = self.ledger.snapshot().records[2]
        self.ledger.record_verification(
            self.ids[2], observation=verified, expected_revision=record.revision
        )
        self.bad.write_bytes(b"bad!")
        stage = self.path / (self.ids[3] + ".staging")
        stage.touch(mode=0o600)
        stage.write_bytes(b"x")
        request = FailedStagingRequest(
            self.path,
            self.pkg.manifest,
            0,
            self.pkg.manifest.digest,
            self.ids[3],
            max_read_bytes=4,
            **self.options,
        )
        quarantine_failed_staging(request, inspect_failed_staging(request))
        self.before = self.ledger.snapshot()
        self.files = {path.name: path.read_bytes() for path in self.path.iterdir()}

    def audit(self, **kwargs):
        args = self.options | {"max_read_bytes": 16} | kwargs
        return owned.audit_owned_inventory(self.path, self.selections, **args)

    def assert_preserved(self):
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertEqual(
            {path.name: path.read_bytes() for path in self.path.iterdir()}, self.files
        )

    def test_complete_report_rehashes_finals_skips_quarantine_and_preserves_everything(
        self,
    ):
        with patch.object(
            owned.inventory, "verify_fragment_file", wraps=verify_fragment_file
        ) as verify:
            report = self.audit()
        self.assertEqual(
            [item.status for item in report.observations],
            ["verified", "missing", "unverified", "quarantined"],
        )
        self.assertEqual(report.ledger, self.before)
        self.assertEqual(report.read_budget_bytes, 16)
        self.assertEqual(report.observed_regular_bytes, 9)
        self.assertEqual(
            [call.args[2].suffix for call in verify.call_args_list], [".fragment"] * 3
        )
        self.assertEqual(report.observations[3].path.suffix, ".quarantine")
        for flag in (
            report.retention_confirmed,
            report.publisher_authorized,
            report.repair_authorized,
        ):
            self.assertFalse(flag)
        self.assert_preserved()

    def test_invalid_selection_and_aggregate_budget_fail_before_payload_reads(self):
        cases = (
            (),
            self.selections[:-1],
            self.selections + (self.selections[0],),
            list(self.selections),
            (None,),
            self.selections * 49,
            (owned.OwnedInventorySelection(self.ids[0], package(b"other").manifest),)
            + self.selections[1:],
        )
        for selections in cases:
            with (
                self.subTest(selections=selections),
                patch.object(owned.inventory, "_observe") as observe,
                self.assertRaises(ValueError),
            ):
                owned.audit_owned_inventory(
                    self.path, selections, max_read_bytes=16, **self.options
                )
            observe.assert_not_called()
        with (
            patch.object(owned.inventory, "_observe") as observe,
            self.assertRaisesRegex(ValueError, "budget"),
        ):
            self.audit(max_read_bytes=15)
        observe.assert_not_called()
        self.assert_preserved()

    def test_staging_orphan_and_external_link_block_the_whole_audit(self):
        unknown = self.path / "unknown"
        stage = self.path / (self.ids[1] + ".staging")
        outside = self.root / "outside"
        for extra in (unknown, stage, outside):
            with self.subTest(path=extra):
                if extra == outside:
                    extra.hardlink_to(self.good)
                else:
                    extra.touch(mode=0o600)
                with (
                    patch.object(owned.inventory, "_observe") as observe,
                    self.assertRaisesRegex(ValueError, "directory recovery"),
                ):
                    self.audit()
                observe.assert_not_called()
                extra.unlink()
                self.assert_preserved()

    def test_missing_marker_and_copied_ledger_fail_before_payload_reads(self):
        copy_path = self.root / "copy"
        copied = ArtifactAdmission.create(
            copy_path, store_id="f" * 64, limits=self.policy
        )
        self.addCleanup(copied.close)
        self.ledger._db.backup(copied._db)
        with patch.object(owned.inventory, "_observe") as observe:
            with self.assertRaisesRegex(ValueError, "marker"):
                self.audit(ledger_directory=copy_path)
            (self.path / "store.json").unlink()
            with self.assertRaises((ValueError, FileNotFoundError)):
                self.audit()
            observe.assert_not_called()
        self.assertFalse((self.path / "store.json").exists())
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertEqual(copied.snapshot(), self.before)

    def test_cancellation_exceptions_are_not_unverified_results(self):
        real = owned.inventory.verify_fragment_file
        for error in (RuntimeError("cancel"), ValueError("cancel"), OSError("cancel")):
            reached = []

            def verify(*args, reached=reached, **kwargs):
                reached.append(True)
                return real(*args, **kwargs)

            def cancel(reached=reached, error=error):
                if reached:
                    raise error

            with (
                patch.object(
                    owned.inventory, "verify_fragment_file", side_effect=verify
                ),
                self.assertRaises(type(error)),
            ):
                self.audit(check_cancel=cancel)
            self.assert_preserved()

    def test_changed_directory_accounting_rejects_whole_report(self):
        original = owned.inventory._observe

        def observe(*args):
            result = original(*args)
            extra = self.path / (self.ids[1] + ".fragment")
            extra.touch(mode=0o600)
            extra.write_bytes(self.pkg.fragments[0])
            return result

        with (
            patch.object(owned.inventory, "_observe", side_effect=observe),
            self.assertRaisesRegex(ValueError, "accounting changed"),
        ):
            self.audit()
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_binding_loss_and_ledger_updates_reject_after_observation(self):
        original = owned.inventory._observe

        def observe(*args):
            result = original(*args)
            marker = self.path / "store.json"
            marker.write_bytes(b"{}")
            return result

        with (
            patch.object(owned.inventory, "_observe", side_effect=observe),
            self.assertRaisesRegex(ValueError, "marker"),
        ):
            self.audit()
        (self.path / "store.json").write_bytes(self.files["store.json"])

        def update(*args):
            result = original(*args)
            record = self.ledger.snapshot().records[0]
            self.ledger.invalidate(record.request_id, expected_revision=record.revision)
            return result

        with (
            patch.object(owned.inventory, "_observe", side_effect=update),
            self.assertRaises(AdmissionConflict),
        ):
            self.audit()
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_process_lock_excludes_writers_and_cross_process_revision_fences(self):
        script = r"""
import sys
from pathlib import Path
from aethermesh_core.artifact_admission import ArtifactAdmission, AdmissionLimits
from aethermesh_core.artifact_directory import inspect_artifact_directory
root, mode = Path(sys.argv[1]), sys.argv[2]
options = dict(ledger_directory=root/'ledger', store_id='f'*64, limits=AdmissionLimits(max_bytes=32,max_objects=4))
if mode == 'lock':
    try: inspect_artifact_directory(root/'objects', **options)
    except BlockingIOError: sys.exit(73)
else:
    ledger = ArtifactAdmission(root/'ledger', store_id='f'*64, limits=options['limits'])
    record = ledger.snapshot().records[0]
    ledger.invalidate(record.request_id, expected_revision=record.revision)
    ledger.close()
"""
        original = owned.inventory._observe
        for mode in ("lock", "revision"):
            calls = []

            def observe(*args, mode=mode, calls=calls):
                result = original(*args)
                if not calls:
                    child = subprocess.run(
                        [sys.executable, "-c", script, str(self.root), mode],
                        capture_output=True,
                        text=True,
                        timeout=15,
                        check=False,
                    )
                    self.assertEqual(
                        child.returncode, 73 if mode == "lock" else 0, child.stderr
                    )
                    calls.append(True)
                return result

            with patch.object(owned.inventory, "_observe", side_effect=observe):
                if mode == "lock":
                    self.audit()
                    self.assert_preserved()
                else:
                    with self.assertRaises(AdmissionConflict):
                        self.audit()
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_empty_owned_store_has_no_payload_reads(self):
        ledger_path, path = self.root / "empty-ledger", self.root / "empty-objects"
        ArtifactAdmission.create(
            ledger_path, store_id="e" * 64, limits=self.policy
        ).close()
        options = {
            "ledger_directory": ledger_path,
            "store_id": "e" * 64,
            "limits": self.policy,
        }
        initialize_artifact_directory(path, **options)
        with patch.object(owned.inventory, "_observe") as observe:
            report = owned.audit_owned_inventory(path, (), max_read_bytes=0, **options)
        observe.assert_not_called()
        self.assertEqual(report.observations, ())
        self.assertEqual(report.observed_regular_bytes, 0)
