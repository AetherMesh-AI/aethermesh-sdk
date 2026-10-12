"""Admission-first ingestion, atomic publication and conservative crash outcomes."""

import errno
import os
import stat
import subprocess
import sys
import tempfile
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.artifact_admission import AdmissionLimits, ArtifactAdmission
from aethermesh_core.artifact_directory import (
    initialize_artifact_directory,
    inspect_artifact_directory,
)
from aethermesh_core.artifact_ingestion import ingest_fragment_file
from tests.test_artifact_codec import package


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.ledger_path = self.root / "ledger"
        self.path = self.root / "objects"
        self.limits = AdmissionLimits(max_bytes=16, max_objects=2)
        self.args = {
            "ledger_directory": self.ledger_path,
            "store_id": "a" * 64,
            "limits": self.limits,
        }
        self.ledger = ArtifactAdmission.create(
            self.ledger_path, store_id="a" * 64, limits=self.limits
        )
        self.addCleanup(self.ledger.close)
        initialize_artifact_directory(self.path, **self.args)
        self.pkg = package()
        self.source = self.root / "source"
        self.source.write_bytes(self.pkg.fragments[0])
        self.request = "b" * 64
        self.final = self.path / (self.request + ".fragment")
        self.stage = self.path / (self.request + ".staging")

    def ingest(self, **kwargs):
        options = self.args | {
            "expected_digest": self.pkg.manifest.digest,
            "request_id": self.request,
            "max_read_bytes": 8,
        }
        return ingest_fragment_file(
            self.path, self.source, self.pkg.manifest, 0, **(options | kwargs)
        )

    def charged(self):
        return self.ledger.snapshot().charged_bytes

    def inspect(self):
        return inspect_artifact_directory(self.path, **self.args)

    def test_success_and_verified_retry_without_source_do_not_double_charge(self):
        result = self.ingest()
        self.assertFalse(result.already_present)
        self.assertEqual(result.bytes_read, 8)
        self.assertTrue(result.integrity_verified)
        self.assertFalse(result.retention_confirmed)
        self.assertFalse(result.publisher_authorized)
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(self.final.stat().st_mode & 0o777, 0o600)
        self.assertFalse(self.stage.exists())
        self.assertFalse(self.inspect().blocked)
        snapshot = self.ledger.snapshot()
        self.source.unlink()
        again = self.ingest()
        self.assertTrue(again.already_present)
        self.assertEqual(again.bytes_read, 4)
        self.assertEqual(self.ledger.snapshot(), snapshot)
        self.final.write_bytes(b"bad!")
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.ingest()
        self.assertEqual(self.charged(), 4)
        self.assertEqual(self.final.read_bytes(), b"bad!")

    def test_preflight_and_orphans_fail_before_admission_or_payload_open(self):
        for options in (
            {"max_read_bytes": 7},
            {"max_read_bytes": True},
            {"expected_digest": "f" * 64},
        ):
            with (
                self.subTest(options=options),
                patch("aethermesh_core.artifact_ingestion._source") as opened,
            ):
                with self.assertRaises(ValueError):
                    self.ingest(**options)
                opened.assert_not_called()
                self.assertEqual(self.charged(), 0)
        self.final.touch(mode=0o600)
        self.final.write_bytes(self.pkg.fragments[0])
        with self.assertRaisesRegex(ValueError, "reconciliation"):
            self.ingest()
        self.assertEqual(self.charged(), 0)
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])

    def test_source_rejection_keeps_reservation_without_creating_stage(self):
        self.source.unlink()
        with self.assertRaises(FileNotFoundError):
            self.ingest()
        self.assertEqual(self.charged(), 4)
        self.assertFalse(self.stage.exists())
        os.mkfifo(self.source)
        with self.assertRaises(ValueError):
            self.ingest()
        self.source.unlink()
        self.source.symlink_to(self.root / "absent")
        with self.assertRaises(OSError):
            self.ingest()
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.charged(), 4)

    def test_short_io_and_actual_read_budget(self):
        original_read, original_write = os.read, os.write
        reads = []

        def read(fd, size):
            data = original_read(fd, min(2, size))
            reads.append(len(data))
            return data

        def write(fd, data):
            return original_write(fd, data[:1])

        with (
            patch("aethermesh_core.artifact_ingestion.os.read", side_effect=read),
            patch("aethermesh_core.artifact_ingestion.os.write", side_effect=write),
        ):
            result = self.ingest()
        self.assertEqual(sum(reads), result.bytes_read)
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])

    def test_bad_bytes_and_write_failure_leave_charged_staging(self):
        self.source.write_bytes(b"bad!")
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.ingest()
        self.assertTrue(self.stage.exists())
        self.assertFalse(self.final.exists())
        self.assertEqual(self.charged(), 4)
        self.assertTrue(self.inspect().blocked)
        with self.assertRaisesRegex(ValueError, "reconciliation"):
            self.ingest()

    def test_disk_full_preserves_partial_stage_and_charge(self):
        original = os.write
        writes = []

        def fail(fd, data):
            if writes:
                raise OSError(errno.ENOSPC, "disk full")
            writes.append(True)
            return original(fd, data[:1])

        with (
            patch("aethermesh_core.artifact_ingestion.os.write", side_effect=fail),
            self.assertRaises(OSError),
        ):
            self.ingest()
        self.assertEqual(self.stage.stat().st_size, 1)
        self.assertFalse(self.final.exists())
        self.assertEqual(self.charged(), 4)

    def test_cancellation_after_admission_and_during_write_preserves_state(self):
        calls = []

        def cancel():
            calls.append(True)
            if self.charged():
                raise InterruptedError("cancelled after admission")

        with self.assertRaises(InterruptedError):
            self.ingest(check_cancel=cancel)
        self.assertEqual(self.charged(), 4)
        self.assertFalse(self.stage.exists())

        def cancel_staging():
            if self.stage.exists():
                raise ValueError("caller cancellation")

        with self.assertRaisesRegex(ValueError, "caller cancellation"):
            self.ingest(check_cancel=cancel_staging)
        self.assertTrue(self.stage.exists())
        self.assertFalse(self.final.exists())
        self.assertEqual(self.charged(), 4)

    def test_source_mutation_and_stage_replacement_are_not_published(self):
        from aethermesh_core.artifact_ingestion import _copy

        def mutate(*args):
            _copy(*args)
            self.source.write_bytes(b"bad!")

        with (
            patch("aethermesh_core.artifact_ingestion._copy", side_effect=mutate),
            self.assertRaisesRegex(ValueError, "source changed"),
        ):
            self.ingest()
        self.assertFalse(self.final.exists())
        self.assertEqual(self.charged(), 4)

    def test_link_collision_preserves_both_files(self):
        original = os.link

        def race(*args, **kwargs):
            self.final.write_bytes(b"other")
            return original(*args, **kwargs)

        with (
            patch("aethermesh_core.artifact_ingestion.os.link", side_effect=race),
            patch("os.supports_dir_fd", os.supports_dir_fd | {os.link}),
            self.assertRaises(FileExistsError),
        ):
            self.ingest()
        self.assertEqual(self.final.read_bytes(), b"other")
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.charged(), 4)

    def test_post_link_sync_failure_preserves_both_names_and_charge(self):
        original = os.fsync

        def fail(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError(errno.EIO, "directory sync")
            original(fd)

        with (
            patch("aethermesh_core.artifact_ingestion.os.fsync", side_effect=fail),
            self.assertRaises(OSError),
        ):
            self.ingest()
        self.assertEqual(self.final.stat().st_ino, self.stage.stat().st_ino)
        self.assertEqual(self.charged(), 4)
        self.assertTrue(self.inspect().blocked)

    def test_ledger_change_before_publication_preserves_unpublished_staging(self):
        changed = []

        def change():
            if self.stage.exists() and not changed:
                record = self.ledger.snapshot().records[0]
                self.ledger.invalidate(
                    record.request_id, expected_revision=record.revision
                )
                changed.append(True)

        with self.assertRaisesRegex(ValueError, "admission changed"):
            self.ingest(check_cancel=change)
        self.assertFalse(self.final.exists())
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.charged(), 4)

    def test_process_exit_before_and_after_publication_preserves_recovery_evidence(
        self,
    ):
        child = """
import os, sys
from pathlib import Path
from unittest.mock import patch
from aethermesh_core.artifact_admission import AdmissionLimits
from aethermesh_core.artifact_ingestion import ingest_fragment_file
from aethermesh_core.artifact_codec import ArtifactLimits, package_artifact
pkg = package_artifact(bytes(range(16)), project="artifact", kind="dataset", data_class="synthetic", expires_at=100, limits=ArtifactLimits())
mode = sys.argv[4]
original = os.link

def link(*args, **kwargs):
    if mode == "after": original(*args, **kwargs)
    os._exit(76)

with patch("aethermesh_core.artifact_ingestion.os.link", side_effect=link):
    with patch("os.supports_dir_fd", os.supports_dir_fd | {os.link}):
        ingest_fragment_file(Path(sys.argv[2]), Path(sys.argv[3]), pkg.manifest, 0,
            expected_digest=pkg.manifest.digest, request_id="b"*64,
            ledger_directory=Path(sys.argv[1]), store_id="c"*64,
            limits=AdmissionLimits(max_bytes=16, max_objects=2), max_read_bytes=8)
"""
        for mode in ("before", "after"):
            ledger_path, path = (
                self.root / f"ledger-{mode}",
                self.root / f"objects-{mode}",
            )
            ArtifactAdmission.create(
                ledger_path, store_id="c" * 64, limits=self.limits
            ).close()
            args = self.args | {"ledger_directory": ledger_path, "store_id": "c" * 64}
            initialize_artifact_directory(path, **args)
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    child,
                    str(ledger_path),
                    str(path),
                    str(self.source),
                    mode,
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(result.returncode, 76, result.stderr)
            self.assertEqual(result.stderr, "")
            self.assertTrue((path / self.stage.name).exists())
            self.assertEqual((path / self.final.name).exists(), mode == "after")
            report = inspect_artifact_directory(path, **args)
            self.assertTrue(report.blocked)
            self.assertEqual(report.charged_bytes, 4)
            self.assertEqual(report.observed_regular_bytes, 4)

    def test_verified_staging_mutation_before_link_is_rejected(self):
        from aethermesh_core.artifact_ingestion import _publish

        def mutate(*args):
            self.stage.write_bytes(b"bad!")
            _publish(*args)

        with (
            patch("aethermesh_core.artifact_ingestion._publish", side_effect=mutate),
            self.assertRaisesRegex(ValueError, "staging bytes changed"),
        ):
            self.ingest()
        self.assertFalse(self.final.exists())
        self.assertEqual(self.stage.read_bytes(), b"bad!")
        self.assertEqual(self.charged(), 4)

    def test_prelink_path_replacement_preserves_unowned_name(self):
        from aethermesh_core.artifact_ingestion import _publish

        def replace(*args):
            self.stage.rename(self.path / "unowned-original")
            self.stage.write_bytes(b"other")
            _publish(*args)

        with (
            patch("aethermesh_core.artifact_ingestion._publish", side_effect=replace),
            self.assertRaises(ValueError),
        ):
            self.ingest()
        self.assertFalse(self.final.exists())
        self.assertEqual(self.stage.read_bytes(), b"other")
        self.assertTrue((self.path / "unowned-original").exists())
        self.assertEqual(self.charged(), 4)

    def test_sync_failure_after_alias_removal_can_retry_verified_final(self):
        original = os.fsync
        calls = []

        def fail(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                calls.append(True)
                if len(calls) == 2:
                    raise OSError(errno.EIO, "final directory sync")
            original(fd)

        with (
            patch("aethermesh_core.artifact_ingestion.os.fsync", side_effect=fail),
            self.assertRaises(OSError),
        ):
            self.ingest()
        self.assertFalse(self.stage.exists())
        self.assertTrue(self.final.exists())
        self.assertEqual(self.charged(), 4)
        self.assertTrue(self.ingest().already_present)

    def test_cancellation_is_not_rechecked_after_publication_commit(self):
        original = os.link
        committed = []

        def link(*args, **kwargs):
            original(*args, **kwargs)
            committed.append(True)

        def cancel():
            if committed:
                raise InterruptedError("too late to undo publication")

        with (
            patch("aethermesh_core.artifact_ingestion.os.link", side_effect=link),
            patch("os.supports_dir_fd", os.supports_dir_fd | {os.link}),
        ):
            result = self.ingest(check_cancel=cancel)
        self.assertTrue(result.integrity_verified)
        self.assertEqual(committed, [True])
        self.assertFalse(self.stage.exists())

    def test_record_quota_prevents_source_open(self):
        self.ledger.reserve(
            "c" * 64, self.pkg.manifest, 1, expected_digest=self.pkg.manifest.digest
        )
        self.ledger.reserve(
            "d" * 64, self.pkg.manifest, 2, expected_digest=self.pkg.manifest.digest
        )
        with (
            patch("aethermesh_core.artifact_ingestion._source") as opened,
            self.assertRaises(ValueError),
        ):
            self.ingest()
        opened.assert_not_called()
        self.assertEqual(self.charged(), 8)
        self.assertFalse(self.stage.exists())

    def test_unlink_failure_never_rolls_back_published_bytes_or_charge(self):
        with (
            patch(
                "aethermesh_core.artifact_ingestion.os.unlink",
                side_effect=OSError(errno.EIO, "unlink failed"),
            ),
            self.assertRaises(OSError),
        ):
            self.ingest()
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(self.final.stat().st_ino, self.stage.stat().st_ino)
        self.assertTrue(self.inspect().blocked)
        self.assertEqual(self.charged(), 4)

    def test_known_staging_descriptor_closes_on_interruption(self):
        captured = []

        def interrupt(_source, target, _size, _cancel):
            captured.append(target)
            raise KeyboardInterrupt("copy interrupted")

        with (
            patch("aethermesh_core.artifact_ingestion._copy", side_effect=interrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.ingest()
        self.assertEqual(len(captured), 1)
        with self.assertRaises(OSError):
            os.fstat(captured[0])
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.charged(), 4)

    def test_uncertain_stage_creation_preserves_file_and_capacity(self):
        original = os.open
        captured = []

        def interrupt(name, *args, **kwargs):
            fd = original(name, *args, **kwargs)
            if name == self.stage.name:
                captured.append(fd)
                raise KeyboardInterrupt("open result unknown")
            return fd

        try:
            with (
                patch(
                    "aethermesh_core.artifact_ingestion.os.open", side_effect=interrupt
                ),
                patch("os.supports_dir_fd", os.supports_dir_fd | {os.open}),
                self.assertRaises(KeyboardInterrupt),
            ):
                self.ingest()
            self.assertEqual(len(captured), 1)
            os.fstat(captured[0])
            self.assertTrue(self.stage.exists())
            self.assertTrue(self.inspect().blocked)
            self.assertEqual(self.charged(), 4)
        finally:
            for fd in captured:
                os.close(fd)

    def test_post_publication_close_error_keeps_final_for_verified_retry(self):
        from aethermesh_core.artifact_ingestion import _close_known

        captured = []

        def close(fd):
            _close_known(fd)
            if self.final.exists() and not captured:
                captured.append(fd)
                raise OSError("close result uncertain")

        with (
            patch("aethermesh_core.artifact_ingestion._close_known", side_effect=close),
            self.assertRaises(OSError),
        ):
            self.ingest()
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(self.charged(), 4)
        self.assertTrue(self.ingest().already_present)

    def test_ledger_change_during_preflight_never_reserves_requested_payload(self):
        calls = []

        def change():
            calls.append(True)
            if len(calls) == 2:
                self.ledger.reserve(
                    "c" * 64,
                    self.pkg.manifest,
                    1,
                    expected_digest=self.pkg.manifest.digest,
                )

        with self.assertRaisesRegex(ValueError, "before ingestion"):
            self.ingest(check_cancel=change)
        self.assertEqual(
            [row.request_id for row in self.ledger.snapshot().records], ["c" * 64]
        )
        self.assertFalse(self.stage.exists())

    def test_ledger_change_after_commit_reports_uncertainty_without_rollback(self):
        from aethermesh_core.artifact_ingestion import _publish

        def change(*args):
            _publish(*args)
            record = self.ledger.snapshot().records[0]
            self.ledger.invalidate(record.request_id, expected_revision=record.revision)

        with (
            patch("aethermesh_core.artifact_ingestion._publish", side_effect=change),
            self.assertRaisesRegex(ValueError, "during ingestion"),
        ):
            self.ingest()
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.charged(), 4)
        self.assertTrue(self.ingest().already_present)

    def test_multiblock_copy_uses_bounded_requests_and_python_allocations(self):
        pkg = package(bytes(range(256)) * 1025)
        size = len(pkg.fragments[0])
        policy = AdmissionLimits(max_bytes=size, max_objects=1)
        ledger_path, path = self.root / "large-ledger", self.root / "large-objects"
        ArtifactAdmission.create(ledger_path, store_id="f" * 64, limits=policy).close()
        initialize_artifact_directory(
            path, ledger_directory=ledger_path, store_id="f" * 64, limits=policy
        )
        source = self.root / "large-source"
        source.write_bytes(pkg.fragments[0])
        original = os.read
        sizes = []

        def read(fd, count):
            sizes.append(count)
            return original(fd, count)

        tracemalloc.start()
        try:
            with patch("aethermesh_core.artifact_ingestion.os.read", side_effect=read):
                result = ingest_fragment_file(
                    path,
                    source,
                    pkg.manifest,
                    0,
                    expected_digest=pkg.manifest.digest,
                    request_id="b" * 64,
                    ledger_directory=ledger_path,
                    store_id="f" * 64,
                    limits=policy,
                    max_read_bytes=2 * size,
                )
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertGreater(len(sizes), 2)
        self.assertLessEqual(max(sizes), 32768)
        self.assertEqual(sum(sizes), 2 * size)
        self.assertEqual(result.bytes_read, 2 * size)
        self.assertLess(peak, 1024 * 1024)

    def test_invalidation_after_reserve_cannot_become_a_new_baseline(self):
        original = ArtifactAdmission.reserve
        from aethermesh_core.artifact_ingestion import _source

        def invalidate(ledger, *args, **kwargs):
            record = original(ledger, *args, **kwargs)
            self.ledger.invalidate(record.request_id, expected_revision=record.revision)
            return record

        with (
            patch.object(ArtifactAdmission, "reserve", invalidate),
            patch(
                "aethermesh_core.artifact_ingestion._source", wraps=_source
            ) as opened,
        ):
            with self.assertRaisesRegex(ValueError, "during reservation"):
                self.ingest()
            opened.assert_not_called()
        self.assertFalse(self.stage.exists())
        self.assertFalse(self.final.exists())
        self.assertEqual(self.charged(), 4)
        self.assertEqual(self.ledger.snapshot().records[0].state, "reserved")

    def test_unrelated_admission_during_reserve_is_not_absorbed(self):
        original = ArtifactAdmission.reserve

        def add(ledger, *args, **kwargs):
            record = original(ledger, *args, **kwargs)
            original(
                self.ledger,
                "c" * 64,
                self.pkg.manifest,
                1,
                expected_digest=self.pkg.manifest.digest,
            )
            return record

        with (
            patch.object(ArtifactAdmission, "reserve", add),
            patch("aethermesh_core.artifact_ingestion._source") as opened,
        ):
            with self.assertRaisesRegex(ValueError, "during reservation"):
                self.ingest()
            opened.assert_not_called()
        self.assertEqual(self.charged(), 8)
        self.assertFalse(self.stage.exists())

    def test_intervening_update_before_own_reserve_rejects_shifted_revision(self):
        original = ArtifactAdmission.reserve

        def add_first(ledger, *args, **kwargs):
            original(
                self.ledger,
                "c" * 64,
                self.pkg.manifest,
                1,
                expected_digest=self.pkg.manifest.digest,
            )
            return original(ledger, *args, **kwargs)

        with (
            patch.object(ArtifactAdmission, "reserve", add_first),
            patch("aethermesh_core.artifact_ingestion._source") as opened,
        ):
            with self.assertRaisesRegex(ValueError, "during reservation"):
                self.ingest()
            opened.assert_not_called()
        self.assertEqual(self.charged(), 8)
        self.assertFalse(self.final.exists())

    def test_existing_reservation_revision_change_before_retry_is_rejected(self):
        self.ledger.reserve(
            self.request, self.pkg.manifest, 0, expected_digest=self.pkg.manifest.digest
        )
        original = ArtifactAdmission.reserve

        def invalidate_first(ledger, *args, **kwargs):
            record = self.ledger.snapshot().records[0]
            self.ledger.invalidate(record.request_id, expected_revision=record.revision)
            return original(ledger, *args, **kwargs)

        with (
            patch.object(ArtifactAdmission, "reserve", invalidate_first),
            patch("aethermesh_core.artifact_ingestion._source") as opened,
        ):
            with self.assertRaisesRegex(ValueError, "during reservation"):
                self.ingest()
            opened.assert_not_called()
        self.assertEqual(self.charged(), 4)
        self.assertFalse(self.stage.exists())

    def test_process_invalidation_between_reserve_and_snapshot_rejects_io(self):
        original = ArtifactAdmission.reserve
        child = """
import sys
from pathlib import Path
from aethermesh_core.artifact_admission import AdmissionLimits, ArtifactAdmission
ledger = ArtifactAdmission(Path(sys.argv[1]), store_id="a"*64,
                           limits=AdmissionLimits(max_bytes=16, max_objects=2))
try:
    ledger.invalidate(sys.argv[2], expected_revision=int(sys.argv[3]))
finally:
    ledger.close()
"""

        def interleave(ledger, *args, **kwargs):
            record = original(ledger, *args, **kwargs)
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    child,
                    str(self.ledger_path),
                    record.request_id,
                    str(record.revision),
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            return record

        with (
            patch.object(ArtifactAdmission, "reserve", interleave),
            patch("aethermesh_core.artifact_ingestion._source") as opened,
        ):
            with self.assertRaisesRegex(ValueError, "during reservation"):
                self.ingest()
            opened.assert_not_called()
        self.assertEqual(self.charged(), 4)
        self.assertFalse(self.stage.exists())
