"""Bounded local recovery storage for an externally governed learning job.

This is a trusted local service utility, not a peer protocol, model loader,
training engine, evidence authenticator, or distributed consensus service.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Callable
from dataclasses import asdict, dataclass
from itertools import islice
from pathlib import Path

from .network.config import validate_fingerprint, validate_project_id

_STATE_LIMIT = 65536
_CHECKPOINT_LIMIT = 1048576
_REVISION_LIMIT = 4096
_EVIDENCE_LIMIT = 1024


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _payload(state: bytes, checkpoint: bytes) -> None:
    for value, limit in ((state, _STATE_LIMIT), (checkpoint, _CHECKPOINT_LIMIT)):
        if type(value) is not bytes or len(value) > limit:
            raise ValueError("state/checkpoint must be bounded immutable bytes")


@dataclass(frozen=True)
class LearningBinding:
    """Operator-selected commitments; hashes convey no publisher authority.

    Use a fresh job_id digest per independent run. The owning service supplies
    and verifies the actual policy, evaluation suite, and evaluator separately.
    """

    project: str
    job_id: str
    policy_sha256: str
    suite_sha256: str
    evaluator_sha256: str

    def __post_init__(self) -> None:
        validate_project_id(self.project)
        for value in (
            self.job_id,
            self.policy_sha256,
            self.suite_sha256,
            self.evaluator_sha256,
        ):
            validate_fingerprint(value)

    @property
    def canonical(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class LearningSnapshot:
    """Complete immutable recovery bytes at a committed revision and term."""

    revision: int
    term: int
    state: bytes
    checkpoint: bytes

    @property
    def state_sha256(self) -> str:
        return _digest(self.state)

    @property
    def checkpoint_sha256(self) -> str:
        return _digest(self.checkpoint)


Validator = Callable[[LearningSnapshot, bytes, bytes], None]


class LearningConflict(ValueError):
    """Stale coordinator/revision or duplicate evidence; no write committed."""


def _private(path: Path, *, directory: bool) -> None:
    info = path.lstat()
    kind = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if (
        not kind
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
        or (not directory and (info.st_nlink != 1 or info.st_size > 4194304))
    ):
        raise ValueError("learning storage must be private, owned, and physical")


class LearningJournal:
    """One local job, atomic state/checkpoint replacement, and replay tombstones.

    Use only trusted, stable local parent directories. Never open peer-supplied
    SQLite files. Local filesystem permissions are the authority boundary;
    terms fence accidental competing coordinators, not hostile local users.
    Connections are synchronous, thread-confined, and must be closed explicitly.
    """

    def __init__(self, directory: Path, binding: LearningBinding) -> None:
        if not isinstance(binding, LearningBinding):
            raise TypeError("binding must be a LearningBinding")
        if directory != directory.resolve():
            raise ValueError("learning directory must be an absolute physical path")
        _private(directory, directory=True)
        if any(
            path.name not in {"state.sqlite3", "state.sqlite3-journal"}
            for path in islice(directory.iterdir(), 3)
        ):
            raise ValueError("unexpected learning storage entries")
        database = directory / "state.sqlite3"
        _private(database, directory=False)
        journal = directory / "state.sqlite3-journal"
        if journal.exists() or journal.is_symlink():
            _private(journal, directory=False)
        self.binding = binding
        self._db = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=5)
        try:
            self._db.execute("PRAGMA trusted_schema=OFF")
            self._db.execute("PRAGMA journal_mode=DELETE")
            self._db.execute("PRAGMA synchronous=EXTRA")
            if self._db.execute("PRAGMA page_size").fetchone()[0] != 4096:
                raise ValueError("unsupported learning page size")
            self._db.execute("PRAGMA max_page_count=1024")
            row = self._db.execute("SELECT version, binding FROM job").fetchall()
            if row != [(1, binding.canonical)]:
                raise ValueError("unsupported journal or learning binding mismatch")
            self.snapshot()
        except BaseException:
            self._db.close()
            raise

    @classmethod
    def create(
        cls,
        directory: Path,
        binding: LearningBinding,
        *,
        state: bytes,
        checkpoint: bytes = b"",
    ) -> LearningJournal:
        """Exclusively create an operator-selected job with no active coordinator.

        Failures can leave a private incomplete directory. Preserve/inspect it;
        never infer that an existing directory is safe to overwrite or resume.
        """
        if not isinstance(binding, LearningBinding):
            raise TypeError("binding must be a LearningBinding")
        _payload(state, checkpoint)
        if directory != directory.resolve():
            raise ValueError("learning directory must be an absolute physical path")
        directory.mkdir(mode=0o700)
        database = directory / "state.sqlite3"
        descriptor = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        db = sqlite3.connect(database)
        try:
            db.execute("PRAGMA page_size=4096")
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=EXTRA")
            db.execute("PRAGMA max_page_count=1024")
            with db:
                db.execute(
                    "CREATE TABLE job (version INTEGER, binding TEXT, revision INTEGER, "
                    "term INTEGER, state BLOB, checkpoint BLOB, state_hash TEXT, checkpoint_hash TEXT)"
                )
                db.execute("CREATE TABLE evidence (digest TEXT PRIMARY KEY)")
                db.execute(
                    "INSERT INTO job VALUES (1, ?, 0, 0, ?, ?, ?, ?)",
                    (
                        binding.canonical,
                        state,
                        checkpoint,
                        _digest(state),
                        _digest(checkpoint),
                    ),
                )
        finally:
            db.close()
        # SQLite syncs its rollback journal and database; persist the new directory
        # entry as well. This is local durability subject to the filesystem/hardware.
        for path in (directory, directory.parent):
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return cls(directory, binding)

    def close(self) -> None:
        self._db.close()

    def snapshot(self) -> LearningSnapshot:
        """Read committed recovery bytes; verify their stored content digests."""
        revision, term, state, checkpoint, state_hash, checkpoint_hash = (
            self._db.execute(
                "SELECT revision, term, state, checkpoint, state_hash, checkpoint_hash FROM job"
            ).fetchone()
        )
        _payload(state, checkpoint)
        if (
            type(revision) is not int
            or type(term) is not int
            or not 0 <= term <= revision <= _REVISION_LIMIT
            or _digest(state) != state_hash
            or _digest(checkpoint) != checkpoint_hash
        ):
            raise ValueError("invalid learning snapshot")
        return LearningSnapshot(revision, term, state, checkpoint)

    def _current(self, expected_revision: int) -> LearningSnapshot:
        current = self.snapshot()
        if type(expected_revision) is not int or current.revision != expected_revision:
            raise LearningConflict("learning revision changed")
        if current.revision == _REVISION_LIMIT:
            raise ValueError("learning revision limit reached")
        return current

    def claim(self, *, expected_revision: int) -> LearningSnapshot:
        """Explicitly replace the coordinator; all earlier terms become stale.

        There is no fixed peer, clock lease, automatic election, or remote claim.
        Repeated takeovers are bounded by the same lifetime revision limit.
        """
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            current = self._current(expected_revision)
            self._db.execute("UPDATE job SET revision=revision+1, term=term+1")
            result = LearningSnapshot(
                current.revision + 1,
                current.term + 1,
                current.state,
                current.checkpoint,
            )
        return result

    def commit(
        self,
        *,
        term: int,
        expected_revision: int,
        state: bytes,
        checkpoint: bytes,
        evidence: tuple[str, ...],
        validate: Validator,
    ) -> LearningSnapshot:
        """Atomically commit verified state, checkpoint, and evidence tombstones.

        The trusted owning service MUST validate the complete transition against
        its frozen policy and independently verify incoming evidence. `validate`
        runs synchronously inside the transaction and must raise on rejection.
        It must be bounded, side-effect-free, and must not reenter this store.
        The store does not authenticate evidence or enforce model promotion rules.
        Empty evidence permits checkpoint-only commits. Reusing a digest anywhere
        in this job is rejected, including after cancellation or model rollback.
        """
        _payload(state, checkpoint)
        if (
            not isinstance(evidence, tuple)
            or len(evidence) > 32
            or len(set(evidence)) != len(evidence)
        ):
            raise ValueError("invalid or repeated evidence identifiers")
        for value in evidence:
            validate_fingerprint(value)
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            current = self._current(expected_revision)
            if type(term) is not int or term == 0 or current.term != term:
                raise LearningConflict("learning coordinator changed")
            count = self._db.execute("SELECT count(*) FROM evidence").fetchone()[0]
            if count + len(evidence) > _EVIDENCE_LIMIT:
                raise ValueError("learning evidence limit reached")
            for digest in evidence:
                if self._db.execute(
                    "SELECT 1 FROM evidence WHERE digest=?", (digest,)
                ).fetchone():
                    raise LearningConflict("learning evidence already recorded")
            validate(current, state, checkpoint)
            self._db.executemany(
                "INSERT INTO evidence VALUES (?)", ((item,) for item in evidence)
            )
            self._db.execute(
                "UPDATE job SET revision=revision+1, state=?, checkpoint=?, state_hash=?, checkpoint_hash=?",
                (state, checkpoint, _digest(state), _digest(checkpoint)),
            )
            result = LearningSnapshot(current.revision + 1, term, state, checkpoint)
        return result
