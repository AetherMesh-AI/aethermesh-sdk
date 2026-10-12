"""One-directory ownership, bounded orphan accounting and preserved failure state."""

import errno
import os
import sqlite3
import stat
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
from aethermesh_core.artifact_directory import (
    initialize_artifact_directory,
    inspect_artifact_directory,
)
from tests.test_artifact_codec import package


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.ledger_path = self.root / "ledger"
        self.path = self.root / "objects"
        self.limits = AdmissionLimits(max_bytes=64, max_objects=8)
        self.args = {
            "ledger_directory": self.ledger_path,
            "store_id": "a" * 64,
            "limits": self.limits,
        }
        self.ledger = ArtifactAdmission.create(
            self.ledger_path, store_id="a" * 64, limits=self.limits
        )
        self.addCleanup(self.ledger.close)
        self.pkg = package()
        self.record = self.ledger.reserve(
            "b" * 64, self.pkg.manifest, 0, expected_digest=self.pkg.manifest.digest
        )
        initialize_artifact_directory(self.path, **self.args)
        self.before = self.ledger.snapshot()
        self.final_name = "b" * 64 + ".fragment"
        self.stage_name = "b" * 64 + ".staging"

    def inspect(self, **kwargs):
        return inspect_artifact_directory(self.path, **self.args, **kwargs)

    def file(self, name, data=b"1234"):
        path = self.path / name
        path.touch(mode=0o600)
        path.write_bytes(data)
        return path

    def test_exclusive_durable_binding_and_empty_reopen(self):
        report = self.inspect()
        self.assertFalse(report.blocked)
        self.assertEqual(report.charged_bytes, 4)
        self.assertEqual(report.observed_regular_bytes, 0)
        self.assertEqual(report.entries, ())
        self.assertEqual(self.before.revision, self.record.revision + 1)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.path / "store.json").stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            initialize_artifact_directory(self.path, **self.args)
        with self.assertRaises(AdmissionConflict):
            initialize_artifact_directory(self.root / "second", **self.args)
        info = self.path.stat()
        with self.assertRaises(AdmissionConflict):
            self.ledger._bind_directory(info.st_dev, info.st_ino)
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertEqual(list((self.root / "second").iterdir()), [])

    def test_regular_final_staging_and_duplicate_links_count_once(self):
        final = self.file(self.final_name)
        report = self.inspect()
        self.assertFalse(report.blocked)
        self.assertEqual(report.observed_regular_bytes, 4)
        self.assertEqual(report.entries[0].phase, "fragment")
        stage = self.path / self.stage_name
        stage.hardlink_to(final)
        report = self.inspect()
        self.assertTrue(report.blocked)
        self.assertEqual(report.observed_regular_bytes, 4)
        self.assertIn("unfinished_staging", [item.problem for item in report.entries])
        stage.unlink()
        self.file(self.stage_name)
        self.assertEqual(self.inspect().observed_regular_bytes, 8)
        self.assertTrue(self.inspect().blocked)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_orphan_unsafe_and_external_links_block_without_deleting(self):
        orphan = self.file("unrecognized", b"1234567")
        link = self.path / "symlink"
        link.symlink_to(orphan)
        fifo = self.path / "fifo"
        os.mkfifo(fifo, 0o600)
        report = self.inspect()
        self.assertTrue(report.blocked)
        self.assertEqual(report.observed_regular_bytes, 7)
        self.assertEqual(
            {item.problem for item in report.entries}, {"orphan_entry", "unsafe_entry"}
        )
        self.assertTrue(link.is_symlink())
        self.assertEqual(orphan.read_bytes(), b"1234567")
        for item in (link, fifo, orphan):
            item.unlink()
        final = self.file(self.final_name)
        external = self.root / "external"
        external.hardlink_to(final)
        self.assertTrue(self.inspect().blocked)
        external.unlink()
        final.write_bytes(b"oversized")
        self.assertTrue(self.inspect().blocked)
        final.write_bytes(b"1234")
        final.chmod(0o644)
        self.assertTrue(self.inspect().blocked)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_bounded_enumeration_never_reads_payloads(self):
        self.file(self.final_name)
        from aethermesh_core.artifact_directory import _read

        with patch("aethermesh_core.artifact_directory._read", wraps=_read) as read:
            self.inspect()
            self.assertEqual(
                [call.args[1] for call in read.call_args_list], ["store.json"]
            )
        for index in range(384):
            self.file(f"orphan-{index}", b"")
        with self.assertRaisesRegex(ValueError, "entry bound"):
            self.inspect()
        self.assertEqual(len(list(self.path.iterdir())), 386)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_marker_and_database_identity_fail_closed(self):
        marker = self.path / "store.json"
        original = marker.read_bytes()
        marker.write_bytes(b"wrong version or namespace")
        with self.assertRaises(ValueError):
            self.inspect()
        marker.write_bytes(original)
        copied_ledger = self.root / "copied"
        copied_ledger.mkdir(mode=0o700)
        copied = copied_ledger / "admission.sqlite3"
        copied.write_bytes((self.ledger_path / "admission.sqlite3").read_bytes())
        copied.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "marker"):
            inspect_artifact_directory(
                self.path, **(self.args | {"ledger_directory": copied_ledger})
            )
        marker.unlink()
        with self.assertRaises(ValueError):
            self.inspect()
        with self.assertRaises(FileExistsError):
            initialize_artifact_directory(self.path, **self.args)

    def test_binding_corruption_and_directory_replacement_fail_closed(self):
        renamed = self.root / "renamed"
        self.path.rename(renamed)
        self.path.mkdir(mode=0o700)
        (self.path / "store.json").write_bytes((renamed / "store.json").read_bytes())
        with self.assertRaisesRegex(ValueError, "does not own"):
            self.inspect()
        with closing(sqlite3.connect(self.ledger_path / "admission.sqlite3")) as db, db:
            db.execute("DELETE FROM owned_directory")
        with self.assertRaises(ValueError):
            self.inspect()

    def test_cancellation_and_changed_ledger_reject_entire_report(self):
        def cancel():
            raise InterruptedError("cancelled")

        with self.assertRaises(InterruptedError):
            self.inspect(check_cancel=cancel)
        calls = []

        def change():
            calls.append(True)
            if len(calls) == 2:
                self.ledger.invalidate(
                    self.record.request_id, expected_revision=self.record.revision
                )

        with self.assertRaises(AdmissionConflict):
            self.inspect(check_cancel=change)
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_process_lock_excludes_open_and_crash_releases_it(self):
        child = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_DIRECTORY)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
print("locked", flush=True)
sys.stdin.readline()
os._exit(73)
"""
        process = subprocess.Popen(
            [sys.executable, "-c", child, str(self.path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(process.stdout.readline().strip(), "locked")
            with self.assertRaises(BlockingIOError):
                self.inspect()
            _, errors = process.communicate("exit\n", timeout=10)
            self.assertEqual(errors, "")
            self.assertEqual(process.returncode, 73)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)
        self.assertFalse(self.inspect().blocked)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_initialization_fault_preserves_binding_and_incomplete_directory(self):
        for failure in ("write", "fsync"):
            ledger_path = self.root / f"ledger-{failure}"
            directory = self.root / f"objects-{failure}"
            ledger = ArtifactAdmission.create(
                ledger_path, store_id="c" * 64, limits=self.limits
            )
            try:
                args = self.args | {
                    "ledger_directory": ledger_path,
                    "store_id": "c" * 64,
                }
                with (
                    patch(
                        f"aethermesh_core.artifact_directory.os.{failure}",
                        side_effect=OSError(errno.ENOSPC, "disk full"),
                    ),
                    self.assertRaises(OSError),
                ):
                    initialize_artifact_directory(directory, **args)
                self.assertTrue(directory.is_dir())
                with self.assertRaises(FileExistsError):
                    initialize_artifact_directory(directory, **args)
                self.assertEqual(ledger.snapshot().charged_bytes, 0)
                if failure == "write":
                    with self.assertRaises(AdmissionConflict):
                        initialize_artifact_directory(self.root / "replacement", **args)
            finally:
                ledger.close()

    def test_open_failures_close_guards_without_releasing_binding(self):
        original = os.open
        for selected in ("objects", "store.json"):

            def fail(name, *args, selected_name=selected, **kwargs):
                if name == selected_name:
                    raise PermissionError("injected open failure")
                return original(name, *args, **kwargs)

            with (
                patch("aethermesh_core.artifact_directory.os.open", side_effect=fail),
                patch("os.supports_dir_fd", os.supports_dir_fd | {os.open}),
                self.assertRaises(PermissionError),
            ):
                self.inspect()
            self.assertEqual(self.ledger.snapshot(), self.before)

    def test_directory_replacement_during_report_is_rejected(self):
        calls = []

        def replace():
            calls.append(True)
            if len(calls) == 2:
                self.path.rename(self.root / "old")
                self.path.mkdir(mode=0o700)

        with self.assertRaisesRegex(ValueError, "replaced"):
            self.inspect(check_cancel=replace)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_marker_short_writes_and_close_failure_preserve_claim(self):
        ledger_path = self.root / "fault-ledger"
        directory = self.root / "fault-objects"
        with closing(
            ArtifactAdmission.create(ledger_path, store_id="e" * 64, limits=self.limits)
        ) as ledger:
            args = self.args | {"ledger_directory": ledger_path, "store_id": "e" * 64}
            original_write, original_close = os.write, os.close
            closed = []

            def short(fd, data):
                return original_write(fd, data[:3])

            def close(fd):
                marker = stat.S_ISREG(os.fstat(fd).st_mode)
                original_close(fd)
                if marker:
                    closed.append(fd)
                    raise OSError("close result uncertain")

            with (
                patch("aethermesh_core.artifact_directory.os.write", side_effect=short),
                patch("aethermesh_core.artifact_directory.os.close", side_effect=close),
                self.assertRaises(OSError),
            ):
                initialize_artifact_directory(directory, **args)
            self.assertEqual(len(closed), 1)
            with self.assertRaises(OSError):
                os.fstat(closed[0])
            self.assertEqual(ledger.snapshot().charged_bytes, 0)
            with self.assertRaises(AdmissionConflict):
                initialize_artifact_directory(self.root / "fault-replacement", **args)

    def test_two_process_initializers_cannot_claim_two_directories(self):
        ledger_path = self.root / "race-ledger"
        ArtifactAdmission.create(
            ledger_path, store_id="f" * 64, limits=self.limits
        ).close()
        child = """
import sys
from pathlib import Path
from aethermesh_core.artifact_admission import AdmissionConflict, AdmissionLimits
from aethermesh_core.artifact_directory import initialize_artifact_directory
print("ready", flush=True)
sys.stdin.readline()
try:
    initialize_artifact_directory(Path(sys.argv[2]), ledger_directory=Path(sys.argv[1]),
                                  store_id="f"*64, limits=AdmissionLimits(max_bytes=64, max_objects=8))
    print("claimed", flush=True)
except AdmissionConflict:
    print("rejected", flush=True)
"""
        paths = [self.root / f"race-{i}" for i in range(2)]
        children = [
            subprocess.Popen(
                [sys.executable, "-c", child, str(ledger_path), str(path)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for path in paths
        ]
        try:
            for process in children:
                self.assertEqual(process.stdout.readline().strip(), "ready")
            for process in children:
                process.stdin.write("go\n")
                process.stdin.flush()
            results = [process.communicate(timeout=10) for process in children]
            self.assertEqual(
                sorted(out.strip() for out, _ in results), ["claimed", "rejected"]
            )
            self.assertTrue(all(process.returncode == 0 for process in children))
            self.assertTrue(all(not error for _, error in results))
        finally:
            for process in children:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=10)
        self.assertEqual(sum((path / "store.json").exists() for path in paths), 1)
        with closing(
            ArtifactAdmission(ledger_path, store_id="f" * 64, limits=self.limits)
        ) as ledger:
            self.assertEqual(ledger.snapshot().revision, 1)
            self.assertEqual(ledger.snapshot().charged_bytes, 0)

    def test_marker_fsync_failure_keeps_binding_and_selected_bytes(self):
        ledger_path = self.root / "sync-ledger"
        path = self.root / "sync-objects"
        with closing(
            ArtifactAdmission.create(ledger_path, store_id="d" * 64, limits=self.limits)
        ) as ledger:
            original = os.fsync

            def fail_file(fd):
                if stat.S_ISREG(os.fstat(fd).st_mode):
                    raise OSError(errno.EIO, "file sync failed")
                original(fd)

            args = self.args | {"ledger_directory": ledger_path, "store_id": "d" * 64}
            with (
                patch(
                    "aethermesh_core.artifact_directory.os.fsync", side_effect=fail_file
                ),
                self.assertRaises(OSError),
            ):
                initialize_artifact_directory(path, **args)
            self.assertTrue((path / "store.json").exists())
            self.assertEqual(ledger.snapshot().revision, 1)
            self.assertEqual(ledger.snapshot().charged_bytes, 0)
            with self.assertRaises(AdmissionConflict):
                initialize_artifact_directory(self.root / "sync-replacement", **args)
