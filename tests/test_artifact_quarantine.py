"""Failed bytes remain owned and charged; uncertain ownership never gets repaired."""

import hashlib
import os
import subprocess
import sys
import tempfile
import tracemalloc
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import artifact_quarantine as quarantine
from aethermesh_core.artifact_admission import (
    AdmissionConflict,
    AdmissionLimits,
    ArtifactAdmission,
)
from aethermesh_core.artifact_directory import (
    initialize_artifact_directory,
    inspect_artifact_directory,
)
from aethermesh_core.artifact_ingestion import ingest_fragment_file
from aethermesh_core.artifact_recovery import recover_verified_fragment
from tests.test_artifact_codec import package


class QuarantineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.path, self.ledger_path = self.root / "objects", self.root / "ledger"
        self.policy = AdmissionLimits(max_bytes=16, max_objects=2)
        self.options = {
            "ledger_directory": self.ledger_path,
            "store_id": "a" * 64,
            "limits": self.policy,
        }
        self.ledger = ArtifactAdmission.create(
            self.ledger_path, store_id="a" * 64, limits=self.policy
        )
        self.addCleanup(self.ledger.close)
        initialize_artifact_directory(self.path, **self.options)
        self.pkg = package()
        self.request_id = "b" * 64
        self.ledger.reserve(
            self.request_id,
            self.pkg.manifest,
            0,
            expected_digest=self.pkg.manifest.digest,
        )
        self.before = self.ledger.snapshot()
        self.stage = self.path / (self.request_id + ".staging")
        self.destination = self.path / (self.request_id + ".quarantine")
        self.stage.touch(mode=0o600)
        self.stage.write_bytes(b"bad!")
        self.request = quarantine.FailedStagingRequest(
            self.path,
            self.pkg.manifest,
            0,
            self.pkg.manifest.digest,
            self.request_id,
            max_read_bytes=4,
            **self.options,
        )

    def inspect(self, **kwargs):
        return quarantine.inspect_failed_staging(self.request, **kwargs)

    def act(self, evidence=None, **kwargs):
        return quarantine.quarantine_failed_staging(
            self.request, self.inspect() if evidence is None else evidence, **kwargs
        )

    def assert_preserved(self, data=b"bad!"):
        self.assertEqual(self.stage.read_bytes(), data)
        self.assertFalse(self.destination.exists())
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_explicit_observation_quarantine_and_reinspection(self):
        evidence = self.inspect()
        self.assertEqual(evidence.classification, "corrupt")
        self.assertEqual(evidence.observed_sha256, hashlib.sha256(b"bad!").hexdigest())
        self.assertEqual(evidence.bytes_read, 4)
        self.assert_preserved()
        report = self.act(evidence)
        self.assertEqual(report.outcome, "quarantined")
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.destination.read_bytes(), b"bad!")
        self.assertEqual(self.destination.stat().st_ino, evidence.file_stamp[1])
        for flag in (
            report.charge_released,
            report.integrity_verified,
            report.publisher_authorized,
            report.retention_confirmed,
        ):
            self.assertFalse(flag)
        with self.assertRaisesRegex(ValueError, "stale"):
            self.act(evidence)
        self.assertEqual(self.act().outcome, "already_quarantined")
        accounting = inspect_artifact_directory(self.path, **self.options)
        self.assertFalse(accounting.blocked)
        self.assertEqual(accounting.observed_regular_bytes, 4)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_empty_partial_valid_and_oversized(self):
        for data in (b"", b"x"):
            self.stage.write_bytes(data)
            evidence = self.inspect()
            self.assertEqual(evidence.classification, "partial")
            self.assertEqual(evidence.bytes_read, len(data))
            self.assertEqual(evidence.observed_sha256, hashlib.sha256(data).hexdigest())
        self.stage.write_bytes(self.pkg.fragments[0])
        with self.assertRaisesRegex(ValueError, "verified recovery"):
            self.inspect()
        self.stage.write_bytes(b"oversize")
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.inspect()
        self.assert_preserved(b"oversize")

    def test_quarantine_does_not_allow_reusing_charge_but_other_admission_works(self):
        self.stage.write_bytes(b"")
        self.act()
        source = self.root / "source"
        source.write_bytes(self.pkg.fragments[0])
        args = dict(
            expected_digest=self.pkg.manifest.digest,
            request_id=self.request_id,
            **self.options,
        )
        with self.assertRaisesRegex(ValueError, "quarantined"):
            ingest_fragment_file(
                self.path, source, self.pkg.manifest, 0, max_read_bytes=8, **args
            )
        with self.assertRaisesRegex(ValueError, "quarantined"):
            recover_verified_fragment(
                self.path, self.pkg.manifest, 0, max_read_bytes=4, **args
            )
        self.assertEqual(self.ledger.snapshot(), self.before)
        args["request_id"] = "c" * 64
        result = ingest_fragment_file(
            self.path, source, self.pkg.manifest, 0, max_read_bytes=8, **args
        )
        self.assertEqual(result.path.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(self.ledger.snapshot().charged_bytes, 8)
        self.assertEqual(self.destination.read_bytes(), b"")

    def test_same_inode_interrupted_alias_is_explicitly_finished(self):
        self.destination.hardlink_to(self.stage)
        self.assertTrue(inspect_artifact_directory(self.path, **self.options).blocked)
        self.assertEqual(self.act().outcome, "removed_quarantine_alias")
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.destination.read_bytes(), b"bad!")
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_quarantined_and_final_aliases_block_even_at_exact_admitted_length(self):
        self.act()
        final = self.path / (self.request_id + ".fragment")
        final.hardlink_to(self.destination)
        report = inspect_artifact_directory(self.path, **self.options)
        self.assertTrue(report.blocked)
        self.assertEqual(report.observed_regular_bytes, 4)
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.inspect()
        self.assertEqual(final.read_bytes(), b"bad!")
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_different_inode_destination_and_unknown_links_are_preserved(self):
        self.stage.write_bytes(b"x")
        self.destination.touch(mode=0o600)
        self.destination.write_bytes(b"x")
        with self.assertRaises(ValueError):
            self.inspect()
        self.assertEqual(self.destination.read_bytes(), b"x")
        self.destination.unlink()
        outside = self.root / "outside"
        outside.hardlink_to(self.stage)
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.inspect()
        self.assert_preserved(b"x")
        outside.unlink()
        self.destination.hardlink_to(self.stage)
        final = self.path / (self.request_id + ".fragment")
        final.hardlink_to(self.stage)
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.inspect()
        self.assertTrue(final.exists())

    def test_missing_marker_and_copied_ledger_cannot_confer_ownership(self):
        copy_path = self.root / "copied-ledger"
        copied = ArtifactAdmission.create(
            copy_path, store_id="a" * 64, limits=self.policy
        )
        self.addCleanup(copied.close)
        self.ledger._db.backup(copied._db)
        copied_request = replace(self.request, ledger_directory=copy_path)
        with patch("aethermesh_core.artifact_quarantine._observe_bytes") as read:
            with self.assertRaisesRegex(ValueError, "marker"):
                quarantine.inspect_failed_staging(copied_request)
            marker = self.path / "store.json"
            marker.unlink()
            for selected in (self.request, copied_request):
                with self.assertRaises((ValueError, FileNotFoundError)):
                    quarantine.inspect_failed_staging(selected)
            read.assert_not_called()
        self.assertFalse(marker.exists())
        self.assertEqual(copied.snapshot(), self.before)
        self.assert_preserved()

    def test_missing_claim_budget_binding_and_missing_bytes(self):
        for selected in (
            replace(self.request, max_read_bytes=3),
            replace(self.request, expected_digest="f" * 64),
            replace(self.request, request_id="c" * 64),
        ):
            with self.assertRaises(ValueError):
                quarantine.inspect_failed_staging(selected)
        self.stage.unlink()
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.inspect()
        self.stage.touch(mode=0o600)
        self.stage.write_bytes(b"bad!")
        with self.ledger._db:
            self.ledger._db.execute("DROP TABLE owned_directory")
        with self.assertRaisesRegex(ValueError, "does not own"):
            self.inspect()
        self.assertIsNone(self.ledger._directory_binding())
        self.assert_preserved()

    def test_stale_file_and_snapshot_evidence_cannot_mutate(self):
        evidence = self.inspect()
        self.stage.write_bytes(b"nope")
        with self.assertRaisesRegex(ValueError, "stale"):
            self.act(evidence)
        self.assert_preserved(b"nope")
        evidence = self.inspect()
        self.ledger.invalidate(
            self.request_id, expected_revision=self.before.records[0].revision
        )
        with self.assertRaisesRegex(ValueError, "stale"):
            self.act(evidence)
        self.assertFalse(self.destination.exists())
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_observation_mutation_and_ledger_revision_are_fenced(self):
        real_read = quarantine.files._read_fragment

        def change_file(*args):
            result = real_read(*args)
            self.stage.write_bytes(b"nope")
            return result

        with (
            patch.object(quarantine.files, "_read_fragment", side_effect=change_file),
            self.assertRaisesRegex(ValueError, "changed during observation"),
        ):
            self.inspect()
        self.assert_preserved(b"nope")

        def change_ledger(*args):
            result = real_read(*args)
            self.ledger.invalidate(
                self.request_id, expected_revision=self.before.records[0].revision
            )
            return result

        with (
            patch.object(quarantine.files, "_read_fragment", side_effect=change_ledger),
            self.assertRaises(AdmissionConflict),
        ):
            self.inspect()
        self.assertFalse(self.destination.exists())

    def test_cancellation_and_precommit_change_preserve_bytes(self):
        evidence = self.inspect()

        def cancel():
            raise RuntimeError("cancel")

        with self.assertRaisesRegex(RuntimeError, "cancel"):
            self.act(evidence, check_cancel=cancel)
        self.assert_preserved()
        real_sync = os.fsync

        def change(fd):
            real_sync(fd)
            self.stage.write_bytes(b"nope")

        with (
            patch.object(quarantine.os, "fsync", side_effect=change),
            self.assertRaisesRegex(ValueError, "changed before quarantine"),
        ):
            self.act(evidence)
        self.assert_preserved(b"nope")

    def test_precommit_ledger_change_and_postcommit_uncertainty_keep_charge(self):
        evidence = self.inspect()
        real_sync = os.fsync

        def change(fd):
            real_sync(fd)
            self.ledger.invalidate(
                self.request_id,
                expected_revision=self.ledger.snapshot().records[0].revision,
            )

        with (
            patch.object(quarantine.os, "fsync", side_effect=change),
            self.assertRaises(AdmissionConflict),
        ):
            self.act(evidence)
        self.assertFalse(self.destination.exists())
        evidence = self.inspect()
        real_publish = quarantine.ingestion._publish

        def publish_then_change(*args):
            real_publish(*args)
            self.ledger.invalidate(
                self.request_id,
                expected_revision=self.ledger.snapshot().records[0].revision,
            )

        with (
            patch.object(
                quarantine.ingestion, "_publish", side_effect=publish_then_change
            ),
            self.assertRaises(AdmissionConflict),
        ):
            self.act(evidence)
        self.assertEqual(self.destination.read_bytes(), b"bad!")
        self.assertEqual(
            self.ledger.snapshot().charged_bytes, self.before.charged_bytes
        )

    def test_open_failure_never_closes_unknown_descriptor(self):
        real_open = os.open

        def fail(name, *args, **kwargs):
            if name == self.stage.name:
                raise OSError("payload open failed")
            return real_open(name, *args, **kwargs)

        with (
            patch.object(quarantine.os, "open", side_effect=fail),
            patch("os.supports_dir_fd", os.supports_dir_fd | {os.open}),
            self.assertRaisesRegex(OSError, "payload open failed"),
        ):
            self.inspect()
        self.assert_preserved()

    def test_post_accounting_permission_and_link_changes_are_rejected(self):
        real_account = quarantine.directories._account
        outside = self.root / "outside"
        for change in (
            lambda: self.stage.chmod(0o644),
            lambda: outside.hardlink_to(self.stage),
        ):

            def account(*args, change=change, **kwargs):
                result = real_account(*args, **kwargs)
                change()
                return result

            with (
                patch.object(quarantine.directories, "_account", side_effect=account),
                self.assertRaises(ValueError),
            ):
                self.inspect()
            self.assert_preserved()
            self.stage.chmod(0o600)
            outside.unlink(missing_ok=True)

    def test_cancel_during_read_or_after_sync_and_lost_binding_prevent_commit(self):
        evidence = self.inspect()
        real_read, real_sync = quarantine.files._read_fragment, os.fsync
        for phase in ("read", "sync", "marker"):
            reached = []

            def read(*args, reached=reached):
                reached.append(True)
                return real_read(*args)

            def sync(fd, reached=reached, phase=phase):
                real_sync(fd)
                reached.append(True)
                if phase == "marker":
                    (self.path / "store.json").unlink()

            def cancel(reached=reached, phase=phase):
                if reached and phase != "marker":
                    raise RuntimeError("cancel at boundary")

            with (
                patch.object(quarantine.files, "_read_fragment", side_effect=read)
                if phase == "read"
                else patch.object(quarantine.os, "fsync", side_effect=sync),
                self.assertRaises((RuntimeError, ValueError)),
            ):
                self.act(evidence, check_cancel=cancel)
            self.assert_preserved()

    def test_sync_and_close_faults_preserve_bytes_and_charges(self):
        evidence = self.inspect()
        real_sync = os.fsync
        for fail_at in (1, 2, 3):
            calls = []

            def sync(fd, calls=calls, fail_at=fail_at):
                calls.append(fd)
                if len(calls) == fail_at:
                    raise OSError("sync fault")
                real_sync(fd)

            with (
                patch.object(quarantine.os, "fsync", side_effect=sync),
                self.assertRaisesRegex(OSError, "sync fault"),
            ):
                self.act(evidence)
            self.assertTrue(self.stage.exists() or self.destination.exists())
            self.assertEqual(self.ledger.snapshot(), self.before)
            self.act()
            self.destination.rename(self.stage)
            evidence = self.inspect()
        real_close = quarantine.directories._close_known

        def close(fd):
            payload = fd is not None and os.fstat(fd).st_ino == evidence.file_stamp[1]
            real_close(fd)
            if payload:
                raise OSError("payload close fault")

        with (
            patch.object(quarantine.directories, "_close_known", side_effect=close),
            self.assertRaisesRegex(OSError, "payload close fault"),
        ):
            self.act(evidence)
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.destination.read_bytes(), b"bad!")
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_destination_race_never_overwrites_or_adopts_bytes(self):
        evidence = self.inspect()
        real_link = os.link

        def link(*args, **kwargs):
            self.destination.touch(mode=0o600)
            self.destination.write_bytes(b"x")
            return real_link(*args, **kwargs)

        with (
            patch.object(os, "link", side_effect=link),
            patch("os.supports_dir_fd", os.supports_dir_fd | {os.link}),
            self.assertRaises(FileExistsError),
        ):
            self.act(evidence)
        self.assertEqual(self.stage.read_bytes(), b"bad!")
        self.assertEqual(self.destination.read_bytes(), b"x")
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_multiblock_observation_has_bounded_reads_and_allocations(self):
        pkg = package(bytes(range(256)) * 1025)
        data = b"x" * len(pkg.fragments[0])
        policy = AdmissionLimits(max_bytes=len(data), max_objects=1)
        ledger_path, path = self.root / "large-ledger", self.root / "large-objects"
        ledger = ArtifactAdmission.create(ledger_path, store_id="f" * 64, limits=policy)
        self.addCleanup(ledger.close)
        initialize_artifact_directory(
            path, ledger_directory=ledger_path, store_id="f" * 64, limits=policy
        )
        ledger.reserve(
            self.request_id, pkg.manifest, 0, expected_digest=pkg.manifest.digest
        )
        stage = path / self.stage.name
        stage.touch(mode=0o600)
        stage.write_bytes(data)
        request = quarantine.FailedStagingRequest(
            path,
            pkg.manifest,
            0,
            pkg.manifest.digest,
            self.request_id,
            ledger_path,
            "f" * 64,
            policy,
            len(data),
        )
        real_read = os.read
        sizes = []

        def read(fd, count):
            sizes.append(count)
            return real_read(fd, count)

        tracemalloc.start()
        try:
            with patch.object(os, "read", side_effect=read):
                evidence = quarantine.inspect_failed_staging(request)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(evidence.bytes_read, len(data))
        self.assertEqual(evidence.observed_sha256, hashlib.sha256(data).hexdigest())
        self.assertLessEqual(max(sizes), 32768)
        self.assertLess(peak, 1024 * 1024)

    def test_real_process_crashes_preserve_aliases_and_charge(self):
        script = r"""
import os, sys
from pathlib import Path
from unittest.mock import patch
from aethermesh_core import artifact_quarantine as q
from aethermesh_core.artifact_admission import AdmissionLimits
from tests.test_artifact_codec import package
root, boundary = Path(sys.argv[1]), sys.argv[2]
boundary = boundary.removeprefix('alias_')
p = package()
r = q.FailedStagingRequest(root/'objects', p.manifest, 0, p.manifest.digest, 'b'*64, root/'ledger', 'a'*64, AdmissionLimits(max_bytes=16,max_objects=2), 4)
e = q.inspect_failed_staging(r)
real_link, real_unlink = os.link, os.unlink
def link(*a, **kw):
    if boundary == 'before_link': os._exit(79)
    real_link(*a, **kw)
    if boundary == 'after_link': os._exit(79)
def unlink(*a, **kw):
    if boundary == 'before_unlink': os._exit(79)
    real_unlink(*a, **kw)
    if boundary == 'after_unlink': os._exit(79)
with patch.object(os, 'link', link), patch.object(os, 'unlink', unlink), patch('os.supports_dir_fd', os.supports_dir_fd | {os.link}):
    q.quarantine_failed_staging(r, e)
"""
        for boundary in (
            "before_link",
            "after_link",
            "before_unlink",
            "after_unlink",
            "alias_before_unlink",
            "alias_after_unlink",
        ):
            with self.subTest(boundary=boundary):
                if boundary.startswith("alias_"):
                    self.destination.hardlink_to(self.stage)
                result = subprocess.run(
                    [sys.executable, "-c", script, str(self.root), boundary],
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=15,
                )
                self.assertEqual(result.returncode, 79, result.stderr)
                self.assertTrue(self.stage.exists() or self.destination.exists())
                self.assertEqual(self.ledger.snapshot(), self.before)
                self.act()
                self.assertEqual(self.destination.read_bytes(), b"bad!")
                self.assertFalse(self.stage.exists())
                self.assertEqual(self.ledger.snapshot(), self.before)
                self.destination.rename(self.stage)
