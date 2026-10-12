"""Explicit verified recovery preserves ambiguous evidence and every charge."""

import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.artifact_admission import AdmissionLimits, ArtifactAdmission
from aethermesh_core.artifact_directory import (
    initialize_artifact_directory,
    inspect_artifact_directory,
)
from aethermesh_core.artifact_ingestion import ingest_fragment_file
from aethermesh_core.artifact_recovery import recover_verified_fragment
from tests.test_artifact_codec import package


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.ledger_path, self.path = self.root / "ledger", self.root / "objects"
        self.policy = AdmissionLimits(max_bytes=16, max_objects=2)
        self.options = {
            "ledger_directory": self.ledger_path,
            "store_id": "a" * 64,
            "limits": self.policy,
        }
        self.pkg = package()
        self.source = self.root / "source"
        self.source.write_bytes(self.pkg.fragments[0])
        self.request = "b" * 64
        self.ledger = ArtifactAdmission.create(
            self.ledger_path, store_id="a" * 64, limits=self.policy
        )
        self.addCleanup(self.ledger.close)
        self.stage = self.path / (self.request + ".staging")
        self.final = self.path / (self.request + ".fragment")
        initialize_artifact_directory(self.path, **self.options)
        self.create_staging(self.path, self.options)
        self.before = self.ledger.snapshot()

    def create_staging(self, path, options):
        with (
            patch(
                "aethermesh_core.artifact_ingestion._publish",
                side_effect=OSError("before link"),
            ),
            self.assertRaises(OSError),
        ):
            ingest_fragment_file(
                path,
                self.source,
                self.pkg.manifest,
                0,
                expected_digest=self.pkg.manifest.digest,
                request_id=self.request,
                max_read_bytes=8,
                **options,
            )

    def recover(self, **kwargs):
        options = self.options | {
            "request_id": self.request,
            "expected_digest": self.pkg.manifest.digest,
            "max_read_bytes": 4,
        }
        return recover_verified_fragment(
            self.path, self.pkg.manifest, 0, **(options | kwargs)
        )

    def test_publish_verified_staging_and_repeat_without_ledger_mutation(self):
        report = self.recover()
        self.assertEqual(report.outcome, "published_staging")
        self.assertEqual(report.bytes_read, 4)
        self.assertTrue(report.integrity_verified)
        self.assertFalse(report.retention_confirmed)
        self.assertFalse(report.publisher_authorized)
        self.assertFalse(report.charge_released)
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertEqual(self.recover().outcome, "already_published")
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_remove_only_same_inode_verified_alias(self):
        self.final.hardlink_to(self.stage)
        self.assertTrue(inspect_artifact_directory(self.path, **self.options).blocked)
        before_inode = self.final.stat().st_ino
        report = self.recover()
        self.assertEqual(report.outcome, "removed_verified_alias")
        self.assertEqual(self.final.stat().st_ino, before_inode)
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertFalse(inspect_artifact_directory(self.path, **self.options).blocked)

    def test_partial_and_corrupt_staging_are_preserved(self):
        for data in (b"x", b"bad!"):
            self.stage.write_bytes(data)
            with self.assertRaises(ValueError):
                self.recover()
            self.assertEqual(self.stage.read_bytes(), data)
            self.assertFalse(self.final.exists())
            self.assertEqual(self.ledger.snapshot(), self.before)

    def test_different_inode_final_and_external_link_block_recovery(self):
        self.final.touch(mode=0o600)
        self.final.write_bytes(self.pkg.fragments[0])
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.recover()
        self.assertTrue(self.stage.exists())
        self.assertTrue(self.final.exists())
        self.final.unlink()
        outside = self.root / "outside"
        outside.hardlink_to(self.stage)
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.recover()
        self.assertTrue(outside.exists())
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_unrelated_staging_is_not_silently_ignored(self):
        self.ledger.reserve(
            "c" * 64, self.pkg.manifest, 1, expected_digest=self.pkg.manifest.digest
        )
        other = self.path / ("c" * 64 + ".staging")
        other.touch(mode=0o600)
        other.write_bytes(self.pkg.fragments[1])
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.recover()
        self.assertTrue(other.exists())
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.ledger.snapshot().charged_bytes, 8)

    def test_binding_budget_and_missing_bytes_fail_without_mutation(self):
        for args in (
            {"request_id": "c" * 64},
            {"expected_digest": "f" * 64},
            {"max_read_bytes": 3},
        ):
            with (
                self.subTest(args=args),
                patch("aethermesh_core.artifact_recovery._verify") as verify,
            ):
                with self.assertRaises(ValueError):
                    self.recover(**args)
                verify.assert_not_called()
        self.stage.unlink()
        with self.assertRaisesRegex(ValueError, "no admitted bytes"):
            self.recover()
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_cancellation_and_ledger_change_prevent_alias_removal(self):
        self.final.hardlink_to(self.stage)
        calls = []

        def cancel():
            calls.append(True)
            if len(calls) == 3:
                raise InterruptedError("cancelled")

        with self.assertRaises(InterruptedError):
            self.recover(check_cancel=cancel)
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.ledger.snapshot(), self.before)
        changed = []

        def invalidate():
            if not changed:
                record = self.before.records[0]
                self.ledger.invalidate(
                    record.request_id, expected_revision=record.revision
                )
                changed.append(True)

        from aethermesh_core.artifact_recovery import _verify

        def change(*args):
            result = _verify(*args)
            invalidate()
            return result

        with (
            patch("aethermesh_core.artifact_recovery._verify", side_effect=change),
            self.assertRaisesRegex(ValueError, "before recovery"),
        ):
            self.recover()
        self.assertTrue(self.stage.exists())
        self.assertEqual(self.ledger.snapshot().charged_bytes, 4)

    def test_changed_staging_after_verification_prevents_publication(self):
        from aethermesh_core.artifact_recovery import _verify

        def change(*args):
            result = _verify(*args)
            self.stage.write_bytes(b"bad!")
            return result

        with (
            patch("aethermesh_core.artifact_recovery._verify", side_effect=change),
            self.assertRaisesRegex(ValueError, "staging changed"),
        ):
            self.recover()
        self.assertFalse(self.final.exists())
        self.assertEqual(self.stage.read_bytes(), b"bad!")
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_post_commit_revision_change_preserves_final_and_charge(self):
        from aethermesh_core.artifact_recovery import _publish

        def change(*args):
            _publish(*args)
            record = self.before.records[0]
            self.ledger.invalidate(record.request_id, expected_revision=record.revision)

        with (
            patch("aethermesh_core.artifact_recovery._publish", side_effect=change),
            self.assertRaisesRegex(ValueError, "during recovery"),
        ):
            self.recover()
        self.assertFalse(self.stage.exists())
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(self.ledger.snapshot().charged_bytes, 4)
        self.assertEqual(self.recover().outcome, "already_published")

    def test_actual_process_exits_around_link_and_alias_removal(self):
        child = """
import os, sys
from pathlib import Path
from unittest.mock import patch
from aethermesh_core.artifact_admission import AdmissionLimits
from aethermesh_core.artifact_codec import ArtifactLimits, package_artifact
from aethermesh_core.artifact_recovery import recover_verified_fragment
pkg = package_artifact(bytes(range(16)), project="artifact", kind="dataset", data_class="synthetic", expires_at=100, limits=ArtifactLimits())
mode = sys.argv[3].removeprefix("alias_")
original_link, original_unlink = os.link, os.unlink

def link(*args, **kwargs):
    if mode == "before_link": os._exit(79)
    original_link(*args, **kwargs)
    if mode == "after_link": os._exit(79)

def unlink(*args, **kwargs):
    if mode == "before_unlink": os._exit(79)
    original_unlink(*args, **kwargs)
    if mode == "after_unlink": os._exit(79)

with patch("os.link", side_effect=link), patch("os.unlink", side_effect=unlink), patch("os.supports_dir_fd", os.supports_dir_fd | {os.link}):
    recover_verified_fragment(Path(sys.argv[2]), pkg.manifest, 0,
        expected_digest=pkg.manifest.digest, request_id="b"*64,
        ledger_directory=Path(sys.argv[1]), store_id="c"*64,
        limits=AdmissionLimits(max_bytes=16, max_objects=2), max_read_bytes=4)
"""
        for mode in (
            "before_link",
            "after_link",
            "before_unlink",
            "after_unlink",
            "alias_before_unlink",
            "alias_after_unlink",
        ):
            with self.subTest(mode=mode):
                ledger_path, path = (
                    self.root / f"ledger-{mode}",
                    self.root / f"objects-{mode}",
                )
                ArtifactAdmission.create(
                    ledger_path, store_id="c" * 64, limits=self.policy
                ).close()
                options = self.options | {
                    "ledger_directory": ledger_path,
                    "store_id": "c" * 64,
                }
                initialize_artifact_directory(path, **options)
                self.create_staging(path, options)
                if mode.startswith("alias_"):
                    (path / self.final.name).hardlink_to(path / self.stage.name)
                result = subprocess.run(
                    [sys.executable, "-c", child, str(ledger_path), str(path), mode],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 79, result.stderr)
                self.assertEqual(result.stderr, "")
                stage, final = path / self.stage.name, path / self.final.name
                self.assertEqual(
                    stage.exists(), mode.removeprefix("alias_") != "after_unlink"
                )
                self.assertEqual(final.exists(), mode != "before_link")
                with closing(
                    ArtifactAdmission(
                        ledger_path, store_id="c" * 64, limits=self.policy
                    )
                ) as ledger:
                    before = ledger.snapshot()
                recovered = recover_verified_fragment(
                    path,
                    self.pkg.manifest,
                    0,
                    expected_digest=self.pkg.manifest.digest,
                    request_id=self.request,
                    max_read_bytes=4,
                    **options,
                )
                self.assertFalse(recovered.charge_released)
                self.assertFalse(stage.exists())
                self.assertEqual(final.read_bytes(), self.pkg.fragments[0])
                with closing(
                    ArtifactAdmission(
                        ledger_path, store_id="c" * 64, limits=self.policy
                    )
                ) as ledger:
                    self.assertEqual(ledger.snapshot(), before)
                    self.assertEqual(before.charged_bytes, 4)

    def test_alias_recovery_reads_one_payload_pass(self):
        self.final.hardlink_to(self.stage)
        original = os.read
        reads = []

        def read(fd, count):
            data = original(fd, count)
            reads.append(len(data))
            return data

        with patch("os.read", side_effect=read):
            report = self.recover()
        self.assertEqual(sum(reads), 4)
        self.assertEqual(report.bytes_read, 4)
        self.assertEqual(self.ledger.snapshot(), self.before)

    def test_unlink_failure_preserves_verified_alias_and_all_charges(self):
        self.final.hardlink_to(self.stage)
        with (
            patch("os.unlink", side_effect=OSError("unlink failed")),
            self.assertRaises(OSError),
        ):
            self.recover()
        self.assertEqual(self.stage.stat().st_ino, self.final.stat().st_ino)
        self.assertEqual(self.ledger.snapshot(), self.before)
        self.assertEqual(self.recover().outcome, "removed_verified_alias")

    def test_replaced_alias_is_never_unlinked(self):
        self.final.hardlink_to(self.stage)
        from aethermesh_core.artifact_recovery import _finish_publication

        original = self.path / "original-alias"

        def replace(*args):
            self.stage.rename(original)
            self.stage.write_bytes(b"other")
            _finish_publication(*args)

        with (
            patch(
                "aethermesh_core.artifact_recovery._finish_publication",
                side_effect=replace,
            ),
            self.assertRaises(ValueError),
        ):
            self.recover()
        self.assertEqual(self.stage.read_bytes(), b"other")
        self.assertTrue(original.exists())
        self.assertEqual(self.final.read_bytes(), self.pkg.fragments[0])
        self.assertEqual(self.ledger.snapshot(), self.before)
