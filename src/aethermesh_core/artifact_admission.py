"""Conservative durable local admission metadata, not a retained-object service.

All records remain charged. No expiry, release, reclamation, file deletion or
hosting is implemented. Verification is a recorded observation, not retention.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, astuple, dataclass, replace
from pathlib import Path

from ._local_sqlite import (
    create_private_database,
    open_private_database,
    sync_directory_creation,
)
from .artifact_codec import MAX_FRAGMENT_BYTES, ArtifactManifest, _integer, _trusted
from .artifact_files import FragmentFileVerification
from .network.config import validate_fingerprint, validate_project_id

_MAX_REVISION = 4096


@dataclass(frozen=True)
class AdmissionLimits:
    max_bytes: int
    max_objects: int

    def __post_init__(self) -> None:
        _integer(self.max_bytes, 1, 64 * 1024 * 1024)
        _integer(self.max_objects, 1, 192)

    @property
    def canonical(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class FragmentAdmission:
    request_id: str
    project: str
    manifest_digest: str
    fragment_index: int
    size: int
    sha256: str
    transfer_digest: str
    state: str
    revision: int

    def __post_init__(self) -> None:
        for digest in (
            self.request_id,
            self.manifest_digest,
            self.sha256,
            self.transfer_digest,
        ):
            validate_fingerprint(digest)
        validate_project_id(self.project)
        _integer(self.fragment_index, 0, 191)
        _integer(self.size, 1, MAX_FRAGMENT_BYTES)
        _integer(self.revision, 1, _MAX_REVISION)
        if self.state not in ("reserved", "verified_observation"):
            raise ValueError("unsupported admission state")

    @property
    def checksum(self) -> str:
        encoded = json.dumps(astuple(self), separators=(",", ":")).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()

    @property
    def binding(self) -> tuple[str, str, str, int, int, str, str]:
        return (
            self.request_id,
            self.project,
            self.manifest_digest,
            self.fragment_index,
            self.size,
            self.sha256,
            self.transfer_digest,
        )


@dataclass(frozen=True)
class AdmissionSnapshot:
    revision: int
    charged_bytes: int
    records: tuple[FragmentAdmission, ...]


class AdmissionConflict(ValueError):
    """Request reuse or stale record revision; no transition committed."""


class ArtifactAdmission:
    """One operator-selected ledger shared by local processes across projects.

    Connections are thread-confined and explicitly closed. All participants must
    use this SAME private database and frozen policy; separate ledgers do not
    coordinate. SQLite serializes admission with BEGIN IMMEDIATE. Metadata does
    not reserve filesystem blocks, stop external writers or prove retained data.
    """

    def __init__(
        self, directory: Path, *, store_id: str, limits: AdmissionLimits
    ) -> None:
        validate_fingerprint(store_id)
        if not isinstance(limits, AdmissionLimits):
            raise TypeError("explicit admission limits required")
        self._store_id = store_id
        self._limits = limits
        self._db = open_private_database(directory, "admission.sqlite3")
        try:
            self.snapshot()
        except BaseException:
            self._db.close()
            raise

    @classmethod
    def create(
        cls, directory: Path, *, store_id: str, limits: AdmissionLimits
    ) -> ArtifactAdmission:
        validate_fingerprint(store_id)
        if not isinstance(limits, AdmissionLimits):
            raise TypeError("explicit admission limits required")
        db = create_private_database(directory, "admission.sqlite3")
        try:
            with db:
                db.execute(
                    "CREATE TABLE catalog (version INTEGER, store_id TEXT, policy TEXT, revision INTEGER)"
                )
                db.execute(
                    "CREATE TABLE fragments (request_id TEXT PRIMARY KEY, project TEXT, manifest_digest TEXT, fragment_index INTEGER, size INTEGER, sha256 TEXT, transfer_digest TEXT, state TEXT, revision INTEGER, checksum TEXT)"
                )
                db.execute(
                    "INSERT INTO catalog VALUES (1, ?, ?, 0)",
                    (store_id, limits.canonical),
                )
        finally:
            db.close()
        sync_directory_creation(directory)
        return cls(directory, store_id=store_id, limits=limits)

    def close(self) -> None:
        self._db.close()

    def _snapshot(self) -> AdmissionSnapshot:
        rows = self._db.execute(
            "SELECT version, store_id, policy, revision FROM catalog"
        ).fetchmany(2)
        if len(rows) != 1 or rows[0][:3] != (1, self._store_id, self._limits.canonical):
            raise ValueError("admission version, identity or policy mismatch")
        revision = rows[0][3]
        _integer(revision, 0, _MAX_REVISION)
        values = self._db.execute(
            "SELECT * FROM fragments ORDER BY request_id"
        ).fetchmany(self._limits.max_objects + 1)
        if len(values) > self._limits.max_objects:
            raise ValueError("admission object bound exceeded")
        records = tuple(FragmentAdmission(*row[:-1]) for row in values)
        if any(
            record.checksum != row[-1]
            for record, row in zip(records, values, strict=True)
        ):
            raise ValueError("admission record checksum mismatch")
        charged = sum(record.size for record in records)
        if (
            charged > self._limits.max_bytes
            or len({record.request_id for record in records}) != len(records)
            or any(record.revision > revision for record in records)
        ):
            raise ValueError("inconsistent admission inventory")
        return AdmissionSnapshot(revision, charged, records)

    def snapshot(self) -> AdmissionSnapshot:
        with self._db:
            self._db.execute("BEGIN")
            return self._snapshot()

    @staticmethod
    def _advance(snapshot: AdmissionSnapshot) -> int:
        if snapshot.revision == _MAX_REVISION:
            raise ValueError("admission lifetime revision limit reached")
        return snapshot.revision + 1

    def reserve(
        self,
        request_id: str,
        manifest: ArtifactManifest,
        index: int,
        *,
        expected_digest: str,
    ) -> FragmentAdmission:
        """Idempotently charge one immutable logical object; no bytes are written.

        Reusing a request ID with another binding fails. Different request IDs
        charge separately even for equal content; deduplication is not assumed.
        Reserved or invalidated records are NEVER automatically freed.
        """
        _trusted(manifest, expected_digest)
        size = manifest.fragment_size(index)
        proposed = FragmentAdmission(
            request_id,
            manifest.project,
            expected_digest,
            index,
            size,
            *manifest.fragments[index],
            "reserved",
            1,
        )
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            snapshot = self._snapshot()
            existing = next(
                (
                    record
                    for record in snapshot.records
                    if record.request_id == request_id
                ),
                None,
            )
            if existing is not None:
                if existing.binding != proposed.binding:
                    raise AdmissionConflict("request ID binding changed")
                return existing
            if (
                len(snapshot.records) == self._limits.max_objects
                or snapshot.charged_bytes + size > self._limits.max_bytes
            ):
                raise ValueError("durable admission quota exhausted")
            record = replace(proposed, revision=self._advance(snapshot))
            self._db.execute(
                "INSERT INTO fragments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                astuple(record) + (record.checksum,),
            )
            self._db.execute("UPDATE catalog SET revision=?", (record.revision,))
            return record

    def _record(
        self, request_id: str, expected_revision: int
    ) -> tuple[AdmissionSnapshot, FragmentAdmission]:
        validate_fingerprint(request_id)
        _integer(expected_revision, 1, _MAX_REVISION)
        snapshot = self._snapshot()
        record = next(
            (value for value in snapshot.records if value.request_id == request_id),
            None,
        )
        if record is None or record.revision != expected_revision:
            raise AdmissionConflict("admission record revision changed")
        return snapshot, record

    def _update(
        self, snapshot: AdmissionSnapshot, record: FragmentAdmission, state: str
    ) -> FragmentAdmission:
        updated = replace(record, state=state, revision=self._advance(snapshot))
        self._db.execute(
            "UPDATE fragments SET state=?, revision=?, checksum=? WHERE request_id=?",
            (state, updated.revision, updated.checksum, record.request_id),
        )
        self._db.execute("UPDATE catalog SET revision=?", (updated.revision,))
        return updated

    def record_verification(
        self,
        request_id: str,
        *,
        expected_revision: int,
        observation: FragmentFileVerification,
    ) -> FragmentAdmission:
        """Record a trusted local caller's observation; no future retention claim.

        The caller must obtain fresh verification from its own selected file.
        This object is not signed evidence, and this ledger does not authenticate
        callers or lock/own any fragment path. Publisher authority stays separate.
        """
        if not isinstance(observation, FragmentFileVerification):
            raise TypeError("fragment verification observation required")
        _integer(observation.fragment_index, 0, 191)
        _integer(observation.bytes_read, 1, MAX_FRAGMENT_BYTES)
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            snapshot, record = self._record(request_id, expected_revision)
            if (
                observation.manifest_digest != record.manifest_digest
                or observation.fragment_index != record.fragment_index
                or observation.bytes_read != record.size
                or observation.transfer.project_id != record.project
                or observation.transfer.size != record.size
                or observation.transfer.sha256 != record.sha256
                or observation.transfer.digest != record.transfer_digest
                or observation.integrity_verified is not True
                or observation.publisher_authorized is not False
                or observation.retention_confirmed is not False
            ):
                raise ValueError("verification observation binding mismatch")
            return self._update(snapshot, record, "verified_observation")

    def invalidate(
        self, request_id: str, *, expected_revision: int
    ) -> FragmentAdmission:
        """Fence prior observations/metadata writers while keeping capacity charged."""
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            snapshot, record = self._record(request_id, expected_revision)
            return self._update(snapshot, record, "reserved")
