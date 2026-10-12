"""Restart reconciliation, aggregate bounds and conservative failure behavior."""

import dataclasses
import errno
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.artifact_admission import (
    AdmissionConflict,
    AdmissionLimits,
    ArtifactAdmission,
)
from aethermesh_core.artifact_files import verify_fragment_file
from aethermesh_core.artifact_inventory import InventorySelection, reconcile_inventory
from tests.test_artifact_codec import package


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.limits = AdmissionLimits(max_bytes=1024, max_objects=6)
        self.directory = self.root / "ledger"
        self.ledger = ArtifactAdmission.create(
            self.directory, store_id="a" * 64, limits=self.limits
        )
        self.addCleanup(self.ledger.close)
        self.pkg = package()
        self.items = []
        for index in range(3):
            request = f"{index:064x}"
            self.ledger.reserve(
                request,
                self.pkg.manifest,
                index,
                expected_digest=self.pkg.manifest.digest,
            )
            path = self.root / f"fragment-{index}"
            path.write_bytes(self.pkg.fragments[index])
            self.items.append(InventorySelection(request, self.pkg.manifest, path))
        self.before = self.ledger.snapshot()

    def audit(self, **kwargs):
        options = {"max_read_bytes": self.before.charged_bytes}
        return reconcile_inventory(self.ledger, tuple(self.items), **(options | kwargs))

    def test_reopen_rechecks_files_and_keeps_all_charges_and_metadata(self):
        for record, item in zip(self.before.records, self.items, strict=True):
            observation = verify_fragment_file(
                item.manifest,
                record.fragment_index,
                item.path,
                expected_digest=record.manifest_digest,
                max_read_bytes=record.size,
            )
            self.ledger.record_verification(
                record.request_id,
                expected_revision=record.revision,
                observation=observation,
            )
        saved = self.ledger.snapshot()
        self.items[1].path.unlink()
        self.items[2].path.write_bytes(b"bad!")
        with closing(
            ArtifactAdmission(self.directory, store_id="a" * 64, limits=self.limits)
        ) as reopened:
            report = reconcile_inventory(
                reopened,
                tuple(reversed(self.items)),
                max_read_bytes=saved.charged_bytes,
            )
            self.assertEqual(
                [entry.status for entry in report.observations],
                ["verified", "missing", "unverified"],
            )
            self.assertEqual(reopened.snapshot(), saved)
            self.assertEqual(report.ledger_revision, saved.revision)
            self.assertEqual(report.charged_bytes, saved.charged_bytes)
            self.assertEqual(report.read_budget_bytes, saved.charged_bytes)
            self.assertFalse(report.retention_confirmed)
            self.assertFalse(report.publisher_authorized)
        self.assertFalse(self.items[1].path.exists())
        self.assertEqual(self.items[2].path.read_bytes(), b"bad!")

    def test_complete_selection_and_all_bindings_preflight_before_file_io(self):
        other = package(b"other bytes")
        choices = (
            (),
            tuple(self.items[:-1]),
            tuple(self.items + [self.items[0]]),
            tuple(
                self.items + [dataclasses.replace(self.items[0], request_id="f" * 64)]
            ),
            tuple(
                self.items[:2]
                + [dataclasses.replace(self.items[2], manifest=other.manifest)]
            ),
            tuple(
                self.items[:2]
                + [dataclasses.replace(self.items[2], path=Path("relative"))]
            ),
            tuple([self.items[0]] * 193),
            list(self.items),
        )
        for selections in choices:
            with (
                self.subTest(selections=selections),
                patch(
                    "aethermesh_core.artifact_inventory.verify_fragment_file"
                ) as verify,
            ):
                with self.assertRaises(ValueError):
                    reconcile_inventory(self.ledger, selections, max_read_bytes=1024)
                verify.assert_not_called()
        for budget in (
            0,
            self.before.charged_bytes - 1,
            True,
            -1,
            64 * 1024 * 1024 + 1,
        ):
            with (
                self.subTest(budget=budget),
                patch(
                    "aethermesh_core.artifact_inventory.verify_fragment_file"
                ) as verify,
            ):
                with self.assertRaises(ValueError):
                    self.audit(max_read_bytes=budget)
                verify.assert_not_called()

    def test_empty_inventory_requires_no_reads(self):
        with (
            closing(
                ArtifactAdmission.create(
                    self.root / "empty", store_id="c" * 64, limits=self.limits
                )
            ) as ledger,
            patch("aethermesh_core.artifact_inventory.verify_fragment_file") as verify,
        ):
            report = reconcile_inventory(ledger, (), max_read_bytes=0)
            self.assertEqual(report.observations, ())
            self.assertEqual(report.read_budget_bytes, 0)
            verify.assert_not_called()

    def test_unsafe_and_unreadable_files_are_not_available_or_deleted(self):
        first = self.items[0].path
        first.unlink()
        first.symlink_to(self.items[1].path)
        last = self.items[2].path
        last.unlink()
        os.mkfifo(last)
        report = self.audit()
        self.assertEqual(
            [entry.status for entry in report.observations],
            ["unverified", "verified", "unverified"],
        )
        self.assertTrue(first.is_symlink())
        self.assertEqual(self.ledger.snapshot(), self.before)
        with patch(
            "aethermesh_core.artifact_files.os.read",
            side_effect=OSError(errno.EIO, "read"),
        ):
            report = self.audit()
        self.assertTrue(
            all(entry.status == "unverified" for entry in report.observations)
        )
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_cancellation_exceptions_are_never_reported_as_file_failures(self):
        baseline_calls = []
        self.audit(check_cancel=lambda: baseline_calls.append(True))
        for error_type in (
            InterruptedError,
            FileNotFoundError,
            ValueError,
            KeyboardInterrupt,
        ):
            for stop in (1, 3, len(baseline_calls)):
                calls = []

                def cancel(
                    selected_stop=stop, selected_error=error_type, selected_calls=calls
                ):
                    selected_calls.append(True)
                    if len(selected_calls) == selected_stop:
                        raise selected_error("cancelled")

                with self.subTest(error=error_type, stop=stop):
                    with self.assertRaises(error_type):
                        self.audit(check_cancel=cancel)
                    self.assertEqual(len(calls), stop)
                    self.assertEqual(self.ledger.snapshot(), self.before)

    def test_ledger_change_from_another_connection_rejects_entire_report(self):
        changed = []
        with closing(
            ArtifactAdmission(self.directory, store_id="a" * 64, limits=self.limits)
        ) as other:

            def change():
                if not changed:
                    record = self.before.records[0]
                    other.invalidate(
                        record.request_id, expected_revision=record.revision
                    )
                    changed.append(True)

            with patch(
                "aethermesh_core.artifact_inventory.verify_fragment_file",
                wraps=verify_fragment_file,
            ) as verify:
                calls = []

                def callback():
                    calls.append(True)
                    if len(calls) == 3:
                        change()

                with self.assertRaises(AdmissionConflict):
                    self.audit(check_cancel=callback)
                self.assertEqual(verify.call_count, 3)
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_process_update_during_file_read_rejects_report(self):
        original = os.read
        writes = []
        child = """
import sys
from pathlib import Path
from aethermesh_core.artifact_admission import ArtifactAdmission, AdmissionLimits
ledger = ArtifactAdmission(Path(sys.argv[1]), store_id="a"*64,
                           limits=AdmissionLimits(max_bytes=1024, max_objects=6))
try:
    ledger.invalidate("0"*64, expected_revision=1)
finally:
    ledger.close()
"""

        def read(fd, size):
            if not writes:
                writes.append(True)
                result = subprocess.run(
                    [sys.executable, "-c", child, str(self.directory)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
            return original(fd, size)

        with (
            patch("aethermesh_core.artifact_files.os.read", side_effect=read),
            self.assertRaises(AdmissionConflict),
        ):
            self.audit()
        self.assertEqual(writes, [True])
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_real_reads_stay_within_aggregate_budget(self):
        reads = []
        original = os.read

        def read(fd, size):
            value = original(fd, size)
            reads.append(len(value))
            return value

        with patch("aethermesh_core.artifact_files.os.read", side_effect=read):
            report = self.audit()
        self.assertEqual(sum(reads), report.read_budget_bytes)
        self.assertLessEqual(max(reads), 32768)
        self.assertTrue(
            all(entry.status == "verified" for entry in report.observations)
        )
        self.assertEqual(self.ledger.snapshot(), self.before)
