"""Durable metadata admission with conservative charges and real-process races."""

import dataclasses
import errno
import os
import sqlite3
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
from tests.test_artifact_codec import package

STORE = "a" * 64
REQUEST = "b" * 64

CHILD = """
import os, sys
from pathlib import Path
from unittest.mock import patch
from aethermesh_core.artifact_admission import ArtifactAdmission, AdmissionLimits
from aethermesh_core.artifact_codec import ArtifactLimits, package_artifact
pkg=package_artifact(bytes(range(16)),project="artifact",kind="dataset",data_class="synthetic",expires_at=100,limits=ArtifactLimits())
ledger=ArtifactAdmission(Path(sys.argv[1]),store_id="a"*64,limits=AdmissionLimits(max_bytes=4,max_objects=1))
mode=sys.argv[2]
if mode=="race":
 print("ready",flush=True)
 sys.stdin.readline()
elif mode=="before":
 ledger._db.set_trace_callback(lambda sql: os._exit(73) if sql=="COMMIT" else None)
try:
 ledger.reserve(sys.argv[3],pkg.manifest,0,expected_digest=pkg.manifest.digest)
 if mode=="after": os._exit(74)
 print("admitted",flush=True)
except ValueError:
 print("rejected",flush=True)
finally:
 ledger.close()
"""


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.directory = self.root / "ledger"
        self.limits = AdmissionLimits(max_bytes=8, max_objects=2)
        self.ledger = ArtifactAdmission.create(
            self.directory, store_id=STORE, limits=self.limits
        )
        self.addCleanup(self.ledger.close)
        self.pkg = package()

    def reserve(self, request=REQUEST, index=0):
        return self.ledger.reserve(
            request, self.pkg.manifest, index, expected_digest=self.pkg.manifest.digest
        )

    def test_idempotent_admission_reopen_and_frozen_policy(self):
        first = self.reserve()
        self.assertEqual(first, self.reserve())
        with closing(
            ArtifactAdmission(self.directory, store_id=STORE, limits=self.limits)
        ) as reopened:
            self.assertEqual(reopened.snapshot().records, (first,))
            self.assertEqual(reopened.snapshot().charged_bytes, 4)
        with self.assertRaises(AdmissionConflict):
            self.reserve(index=1)
        second = self.reserve("c" * 64, 1)
        self.assertGreater(second.revision, first.revision)
        with self.assertRaises(ValueError):
            self.reserve("d" * 64, 2)
        for args in (
            {"store_id": "c" * 64, "limits": self.limits},
            {"store_id": STORE, "limits": AdmissionLimits(max_bytes=9, max_objects=2)},
        ):
            with self.assertRaises(ValueError):
                ArtifactAdmission(self.directory, **args)
        self.assertEqual(self.ledger.snapshot().charged_bytes, 8)

    def test_byte_and_object_ceilings_apply_independently(self):
        policies = (
            AdmissionLimits(max_bytes=4, max_objects=2),
            AdmissionLimits(max_bytes=8, max_objects=1),
        )
        for number, limits in enumerate(policies):
            with closing(
                ArtifactAdmission.create(
                    self.root / f"budget-{number}", store_id=STORE, limits=limits
                )
            ) as ledger:
                ledger.reserve(
                    REQUEST,
                    self.pkg.manifest,
                    0,
                    expected_digest=self.pkg.manifest.digest,
                )
                with self.assertRaisesRegex(ValueError, "quota exhausted"):
                    ledger.reserve(
                        "c" * 64,
                        self.pkg.manifest,
                        1,
                        expected_digest=self.pkg.manifest.digest,
                    )
                self.assertEqual(ledger.snapshot().charged_bytes, 4)
                self.assertEqual(len(ledger.snapshot().records), 1)

    def test_observations_are_revision_fenced_and_never_release_capacity(self):
        first = self.reserve()
        path = self.root / "fragment"
        path.write_bytes(self.pkg.fragments[0])
        observation = verify_fragment_file(
            self.pkg.manifest,
            0,
            path,
            expected_digest=self.pkg.manifest.digest,
            max_read_bytes=4,
        )
        saved = self.ledger.record_verification(
            REQUEST, expected_revision=first.revision, observation=observation
        )
        self.assertEqual(saved.state, "verified_observation")
        path.unlink()
        self.assertEqual(self.ledger.snapshot().records[0], saved)
        invalidated = self.ledger.invalidate(REQUEST, expected_revision=saved.revision)
        self.assertEqual(invalidated.state, "reserved")
        self.assertEqual(self.reserve(), invalidated)
        self.assertEqual(self.ledger.snapshot().charged_bytes, 4)
        with self.assertRaises(AdmissionConflict):
            self.ledger.record_verification(
                REQUEST, expected_revision=first.revision, observation=observation
            )
        with self.assertRaises(ValueError):
            self.ledger.record_verification(
                REQUEST,
                expected_revision=invalidated.revision,
                observation=dataclasses.replace(observation, bytes_read=3),
            )
        self.assertEqual(self.ledger.snapshot().records[0], invalidated)

    def test_competing_processes_cannot_overadmit(self):
        path = self.root / "race"
        ArtifactAdmission.create(
            path, store_id=STORE, limits=AdmissionLimits(max_bytes=4, max_objects=1)
        ).close()
        children = [
            subprocess.Popen(
                [sys.executable, "-c", CHILD, str(path), "race", f"{i:064x}"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for i in (1, 2)
        ]
        try:
            for child in children:
                self.assertEqual(child.stdout.readline().strip(), "ready")
            for child in children:
                child.stdin.write("go\n")
                child.stdin.flush()
            outputs = [child.communicate(timeout=10) for child in children]
            self.assertEqual(
                sorted(out.strip() for out, _ in outputs), ["admitted", "rejected"]
            )
            self.assertTrue(all(not err for _, err in outputs))
            self.assertTrue(all(child.returncode == 0 for child in children))
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=10)
        with closing(
            ArtifactAdmission(
                path, store_id=STORE, limits=AdmissionLimits(max_bytes=4, max_objects=1)
            )
        ) as ledger:
            self.assertEqual(ledger.snapshot().charged_bytes, 4)
            self.assertEqual(len(ledger.snapshot().records), 1)

    def test_real_process_crashes_before_and_after_commit(self):
        for mode, code, count in (("before", 73, 0), ("after", 74, 1)):
            path = self.root / mode
            ArtifactAdmission.create(
                path, store_id=STORE, limits=AdmissionLimits(max_bytes=4, max_objects=1)
            ).close()
            result = subprocess.run(
                [sys.executable, "-c", CHILD, str(path), mode, REQUEST],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(result.returncode, code, result.stderr)
            with closing(
                ArtifactAdmission(
                    path,
                    store_id=STORE,
                    limits=AdmissionLimits(max_bytes=4, max_objects=1),
                )
            ) as ledger:
                self.assertEqual(len(ledger.snapshot().records), count)
                self.assertEqual(ledger.snapshot().charged_bytes, 4 * count)
                record = ledger.reserve(
                    REQUEST,
                    self.pkg.manifest,
                    0,
                    expected_digest=self.pkg.manifest.digest,
                )
                self.assertEqual(record.request_id, REQUEST)
                self.assertEqual(ledger.snapshot().charged_bytes, 4)

    def test_open_during_commit_tolerates_removed_optional_journal(self):
        first = self.reserve()
        journal = self.directory / "admission.sqlite3-journal"
        original_lstat = Path.lstat
        commits = []
        with closing(sqlite3.connect(self.directory / "admission.sqlite3")) as writer:
            writer.execute("UPDATE catalog SET revision=revision+1")
            self.assertTrue(journal.exists())

            def commit_before_journal_stat(path, *args, **kwargs):
                if path == journal:
                    self.assertEqual(commits, [])
                    writer.commit()
                    commits.append(True)
                    self.assertFalse(journal.exists())
                return original_lstat(path, *args, **kwargs)

            with (
                patch.object(Path, "lstat", commit_before_journal_stat),
                closing(
                    ArtifactAdmission(
                        self.directory, store_id=STORE, limits=self.limits
                    )
                ) as reopened,
            ):
                snapshot = reopened.snapshot()
                self.assertEqual(snapshot.records, (first,))
                self.assertEqual(snapshot.charged_bytes, 4)
                self.assertEqual(snapshot.revision, first.revision + 1)
        self.assertEqual(commits, [True])

    def test_optional_journal_preserves_unsafe_file_and_other_error_rejection(self):
        journal = self.directory / "admission.sqlite3-journal"
        original_lstat = Path.lstat
        for error in (errno.EACCES, errno.EIO, errno.ENOTDIR):
            with self.subTest(error=error):

                def fail_journal_stat(path, *args, selected_error=error, **kwargs):
                    if path == journal:
                        raise OSError(selected_error, "journal inspection failed")
                    return original_lstat(path, *args, **kwargs)

                with (
                    patch.object(Path, "lstat", fail_journal_stat),
                    self.assertRaises(OSError) as caught,
                ):
                    ArtifactAdmission(
                        self.directory, store_id=STORE, limits=self.limits
                    )
                self.assertEqual(caught.exception.errno, error)
        for kind in ("symlink", "directory", "public", "hardlink"):
            with self.subTest(kind=kind):
                if kind == "symlink":
                    journal.symlink_to(self.root / "absent")
                elif kind == "directory":
                    journal.mkdir(mode=0o700)
                elif kind == "hardlink":
                    journal.hardlink_to(self.directory / "admission.sqlite3")
                else:
                    journal.touch(mode=0o644)
                    journal.chmod(0o644)
                try:
                    with self.assertRaises(ValueError):
                        ArtifactAdmission(
                            self.directory, store_id=STORE, limits=self.limits
                        )
                finally:
                    if kind == "directory":
                        journal.rmdir()
                    else:
                        journal.unlink()

    def test_limits_and_corrupted_inventory_fail_closed(self):
        with self.assertRaises(ValueError):
            AdmissionLimits(max_bytes=True, max_objects=1)
        self.reserve()
        with closing(sqlite3.connect(self.directory / "admission.sqlite3")) as db, db:
            db.execute("UPDATE catalog SET revision=4096")
        with self.assertRaises(ValueError):
            self.ledger.invalidate(REQUEST, expected_revision=1)
        with closing(sqlite3.connect(self.directory / "admission.sqlite3")) as db, db:
            db.execute("UPDATE fragments SET size=9")
        with self.assertRaises(ValueError):
            self.ledger.snapshot()

    def test_valid_type_corruption_cannot_silently_reduce_charge(self):
        self.reserve()
        with closing(sqlite3.connect(self.directory / "admission.sqlite3")) as db, db:
            db.execute("UPDATE fragments SET size=3")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.ledger.snapshot()
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.reserve("c" * 64, 1)

    def test_initial_configuration_failure_closes_actual_connection(self):
        opened = []

        def fail(db):
            opened.append(db)
            raise OSError("configuration failed")

        with (
            patch("aethermesh_core._local_sqlite._configure", side_effect=fail),
            self.assertRaises(OSError),
        ):
            ArtifactAdmission.create(
                self.root / "incomplete", store_id=STORE, limits=self.limits
            )
        self.assertEqual(len(opened), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            opened[0].execute("SELECT 1")
        self.assertTrue((self.root / "incomplete").is_dir())

    def test_private_exclusive_creation(self):
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(
            (self.directory / "admission.sqlite3").stat().st_mode & 0o777, 0o600
        )
        with self.assertRaises(FileExistsError):
            ArtifactAdmission.create(self.directory, store_id=STORE, limits=self.limits)
        alias = self.root / "alias"
        alias.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(ValueError):
            ArtifactAdmission(alias, store_id=STORE, limits=self.limits)
        os.chmod(self.directory, 0o755)
        with self.assertRaises(ValueError):
            ArtifactAdmission(self.directory, store_id=STORE, limits=self.limits)
