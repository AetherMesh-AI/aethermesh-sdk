"""Local recovery/fencing tests; no distributed trust or training claims."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.learning_state import (
    LearningBinding,
    LearningConflict,
    LearningJournal,
    LearningSnapshot,
)

BINDING = LearningBinding("learning-test", "1" * 64, "2" * 64, "3" * 64, "4" * 64)
EVIDENCE = "5" * 64


def accept(previous: LearningSnapshot, state: bytes, checkpoint: bytes) -> None:
    """Test-only validator for opaque byte persistence, not a promotion policy."""
    assert isinstance(previous, LearningSnapshot)
    assert isinstance(state, bytes)
    assert isinstance(checkpoint, bytes)


class LearningStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve() / "job"
        self.store = LearningJournal.create(self.directory, BINDING, state=b"initial")
        self.addCleanup(self.store.close)

    def open(self) -> LearningJournal:
        store = LearningJournal(self.directory, BINDING)
        self.addCleanup(store.close)
        return store

    def commit(self, store: LearningJournal, **changes: object) -> LearningSnapshot:
        current = store.snapshot()
        args = {
            "term": current.term,
            "expected_revision": current.revision,
            "state": b"candidate",
            "checkpoint": b"checkpoint",
            "evidence": (EVIDENCE,),
            "validate": accept,
        }
        return store.commit(**(args | changes))

    def test_restart_preserves_complete_checkpoint_and_binding(self) -> None:
        initial = self.store.snapshot()
        self.assertEqual(initial, LearningSnapshot(0, 0, b"initial", b""))
        claimed = self.store.claim(expected_revision=0)
        self.assertEqual((claimed.revision, claimed.term), (1, 1))
        result = self.commit(self.store)
        self.assertEqual(result.state_sha256, hashlib.sha256(b"candidate").hexdigest())
        self.assertEqual(
            result.checkpoint_sha256, hashlib.sha256(b"checkpoint").hexdigest()
        )
        self.store.close()
        recovered = self.open().snapshot()
        self.assertEqual(recovered, result)
        with self.assertRaises(FrozenInstanceError):
            recovered.state = b"rewritten"
        self.assertEqual((self.directory.stat().st_mode & 0o777), 0o700)
        self.assertEqual(
            ((self.directory / "state.sqlite3").stat().st_mode & 0o777), 0o600
        )

    def test_takeover_fences_old_coordinator_even_with_latest_revision(self) -> None:
        old = self.store.claim(expected_revision=0)
        replacement = self.open()
        new = replacement.claim(expected_revision=old.revision)
        self.assertEqual(new.term, old.term + 1)
        with self.assertRaises(LearningConflict):
            self.commit(self.store, term=old.term, expected_revision=new.revision)
        with self.assertRaises(LearningConflict):
            self.store.claim(expected_revision=old.revision)
        self.assertEqual(self.commit(replacement).term, new.term)
        for term in (0, True, "2"):
            with self.subTest(term=term), self.assertRaises(LearningConflict):
                self.commit(replacement, term=term, evidence=())
        for revision in (True, -1, "3"):
            with self.subTest(revision=revision), self.assertRaises(LearningConflict):
                replacement.claim(expected_revision=revision)

    def test_duplicate_evidence_survives_restart_and_rollback_state(self) -> None:
        self.store.claim(expected_revision=0)
        first = self.commit(self.store)
        recovered = self.open()
        rollback = self.commit(recovered, state=b"initial", checkpoint=b"", evidence=())
        self.assertGreater(rollback.revision, first.revision)
        with self.assertRaises(LearningConflict):
            self.commit(recovered)
        self.assertEqual(recovered.snapshot(), rollback)
        with self.assertRaises(ValueError):
            self.commit(recovered, evidence=(EVIDENCE, EVIDENCE))
        # A fresh independent job is a distinct replay namespace.
        other = LearningJournal.create(
            self.directory.parent / "other",
            replace(BINDING, job_id="6" * 64),
            state=b"initial",
        )
        self.addCleanup(other.close)
        other.claim(expected_revision=0)
        self.commit(other)

    def test_rejection_and_cancellation_leave_no_partial_state_or_tombstone(
        self,
    ) -> None:
        original = self.store.claim(expected_revision=0)
        for error in (ValueError("bad evidence"), KeyboardInterrupt()):

            def reject(*args: object, rejection: BaseException = error) -> None:
                raise rejection

            with self.subTest(error=type(error)), self.assertRaises(type(error)):
                self.commit(self.store, validate=reject)
            self.assertEqual(self.store.snapshot(), original)
        self.commit(self.store)

    def test_process_crash_after_sql_writes_recovers_previous_state(self) -> None:
        original = self.store.claim(expected_revision=0)
        code = """
import os, sys
from pathlib import Path
from aethermesh_core.learning_state import LearningBinding, LearningJournal
binding = LearningBinding("learning-test", "1"*64, "2"*64, "3"*64, "4"*64)
store = LearningJournal(Path(sys.argv[1]), binding)
def crash(sql):
    if sql == "COMMIT":
        os._exit(71)
store._db.set_trace_callback(crash)
store.commit(term=1, expected_revision=1, state=b"candidate", checkpoint=b"x"*1048576,
             evidence=("5"*64,), validate=lambda *args: None)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.directory)],
            capture_output=True,
            check=False,
            timeout=15,
        )
        self.assertEqual(result.returncode, 71, result.stderr.decode())
        self.assertTrue((self.directory / "state.sqlite3-journal").exists())
        recovered = self.open()
        self.assertEqual(recovered.snapshot(), original)
        self.commit(recovered)

    def test_process_death_after_commit_preserves_result_and_replay_guard(self) -> None:
        self.store.claim(expected_revision=0)
        code = """
import os, sys
from pathlib import Path
from aethermesh_core.learning_state import LearningBinding, LearningJournal
binding = LearningBinding("learning-test", "1"*64, "2"*64, "3"*64, "4"*64)
store = LearningJournal(Path(sys.argv[1]), binding)
store.commit(term=1, expected_revision=1, state=b"candidate", checkpoint=b"checkpoint",
             evidence=("5"*64,), validate=lambda *args: None)
os._exit(72)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.directory)],
            capture_output=True,
            check=False,
            timeout=15,
        )
        self.assertEqual(result.returncode, 72, result.stderr.decode())
        recovered = self.open()
        self.assertEqual(
            recovered.snapshot(), LearningSnapshot(2, 1, b"candidate", b"checkpoint")
        )
        with self.assertRaises(LearningConflict):
            self.commit(recovered)

    def test_oversized_or_unexpected_local_storage_is_rejected(self) -> None:
        unexpected = self.directory / "unexpected"
        unexpected.touch(mode=0o600)
        with self.assertRaises(ValueError):
            self.open()
        unexpected.unlink()
        journal = self.directory / "state.sqlite3-journal"
        journal.touch(mode=0o600)
        with journal.open("wb") as stream:
            stream.truncate(4194305)
        with self.assertRaises(ValueError):
            self.open()
        journal.unlink()
        self.store.close()
        with (self.directory / "state.sqlite3").open("ab") as stream:
            stream.truncate(4194305)
        with self.assertRaises(ValueError):
            self.open()

    def test_nonstandard_database_page_size_is_rejected(self) -> None:
        other = self.directory.parent / "wrong-page-size"
        other.mkdir(mode=0o700)
        database = other / "state.sqlite3"
        database.touch(mode=0o600)
        with closing(sqlite3.connect(database)) as db, db:
            db.execute("PRAGMA page_size=2048")
            db.execute("CREATE TABLE unused (value INTEGER)")
        with self.assertRaises(ValueError):
            LearningJournal(other, BINDING)

    def test_compare_and_swap_prevents_lost_updates(self) -> None:
        original = self.store.claim(expected_revision=0)
        second = self.open()
        self.commit(self.store)
        with self.assertRaises(LearningConflict):
            self.commit(second, expected_revision=original.revision, evidence=())
        self.assertEqual(second.snapshot(), self.store.snapshot())

    def test_policy_suite_evaluator_job_project_and_version_mismatch(self) -> None:
        for field in (
            "project",
            "job_id",
            "policy_sha256",
            "suite_sha256",
            "evaluator_sha256",
        ):
            value = "other" if field == "project" else "a" * 64
            with self.subTest(field=field), self.assertRaises(ValueError):
                LearningJournal(self.directory, replace(BINDING, **{field: value}))
        self.store.close()
        with closing(sqlite3.connect(self.directory / "state.sqlite3")) as db, db:
            db.execute("UPDATE job SET version=2")
        with self.assertRaises(ValueError):
            self.open()

    def test_binding_rejects_bad_commitments(self) -> None:
        for field in (
            "project",
            "job_id",
            "policy_sha256",
            "suite_sha256",
            "evaluator_sha256",
        ):
            for bad in ("", "x" * 64, None, True):
                if field == "project" and bad == "x" * 64:
                    continue
                with self.subTest(field=field, bad=bad), self.assertRaises(ValueError):
                    replace(BINDING, **{field: bad})
        for constructor in (
            lambda: LearningJournal(self.directory, None),
            lambda: LearningJournal.create(
                self.directory.parent / "bad", None, state=b""
            ),
        ):
            with self.assertRaises(TypeError):
                constructor()

    def test_payload_and_evidence_bounds(self) -> None:
        self.store.claim(expected_revision=0)
        result = self.commit(self.store, state=b"s" * 65536, checkpoint=b"c" * 1048576)
        self.assertEqual(self.open().snapshot(), result)
        for field, bad in (
            ("state", b"s" * 65537),
            ("checkpoint", b"c" * 1048577),
            ("state", bytearray()),
            ("checkpoint", "text"),
            ("evidence", []),
            ("evidence", ("bad",)),
            ("evidence", tuple(f"{i:064x}" for i in range(33))),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.commit(self.store, **{field: bad})
        with self.assertRaises(ValueError):
            LearningJournal.create(
                self.directory.parent / "bad", BINDING, state=b"s" * 65537
            )

    def test_lifetime_limits_cannot_reset_on_reopen(self) -> None:
        self.store.claim(expected_revision=0)
        with closing(sqlite3.connect(self.directory / "state.sqlite3")) as db, db:
            db.executemany(
                "INSERT INTO evidence VALUES (?)", ((f"{i:064x}",) for i in range(1024))
            )
        with self.assertRaisesRegex(ValueError, "evidence limit"):
            self.commit(self.open())
        self.commit(self.store, evidence=())
        with closing(sqlite3.connect(self.directory / "state.sqlite3")) as db, db:
            db.execute("UPDATE job SET revision=4096")
        with self.assertRaisesRegex(ValueError, "revision limit"):
            self.open().claim(expected_revision=4096)

    def test_corrupt_snapshot_fails_closed(self) -> None:
        for statement in (
            "UPDATE job SET state=x'00'",
            "UPDATE job SET checkpoint=x'00'",
            "UPDATE job SET term=-1",
            "UPDATE job SET revision='bad'",
            "UPDATE job SET term='bad'",
        ):
            with self.subTest(statement=statement):
                with (
                    closing(sqlite3.connect(self.directory / "state.sqlite3")) as db,
                    db,
                ):
                    db.execute(statement)
                with self.assertRaises(ValueError):
                    self.open()
                with (
                    closing(sqlite3.connect(self.directory / "state.sqlite3")) as db,
                    db,
                ):
                    db.execute(
                        "UPDATE job SET state=?, checkpoint=?, revision=0, term=0",
                        (b"initial", b""),
                    )

    def test_paths_exclusive_physical_and_private(self) -> None:
        with self.assertRaises(FileExistsError):
            LearningJournal.create(self.directory, BINDING, state=b"overwrite")
        with self.assertRaises(ValueError):
            LearningJournal(Path("relative"), BINDING)
        with self.assertRaises(ValueError):
            LearningJournal.create(Path("relative"), BINDING, state=b"")
        alias = self.directory.parent / "alias"
        alias.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(ValueError):
            LearningJournal(alias, BINDING)
        self.directory.chmod(0o755)
        with self.assertRaises(ValueError):
            self.open()
        self.directory.chmod(0o700)
        database = self.directory / "state.sqlite3"
        database.chmod(0o644)
        with self.assertRaises(ValueError):
            self.open()
        database.chmod(0o600)
        os.link(database, self.directory.parent / "hardlink")
        with self.assertRaises(ValueError):
            self.open()
        (self.directory.parent / "hardlink").unlink()
        journal = self.directory / "state.sqlite3-journal"
        journal.symlink_to(self.directory.parent / "absent")
        with self.assertRaises(ValueError):
            self.open()
        journal.unlink()
        with (
            patch("aethermesh_core._local_sqlite.os.getuid", return_value=-1),
            self.assertRaises(ValueError),
        ):
            self.open()
        database.rename(self.directory / "saved")
        database.symlink_to(self.directory / "saved")
        with self.assertRaises(ValueError):
            self.open()

    def test_initialization_fsync_failure_is_reported(self) -> None:
        with (
            patch(
                "aethermesh_core._local_sqlite.os.fsync", side_effect=OSError("disk")
            ),
            self.assertRaises(OSError),
        ):
            LearningJournal.create(
                self.directory.parent / "incomplete", BINDING, state=b"initial"
            )


if __name__ == "__main__":
    unittest.main()
