"""Explicit, evidence-pinned quarantine with intact ownership and no reclamation."""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import artifact_admission as admission
from . import artifact_codec as codec
from . import artifact_directory as directories
from . import artifact_files as files
from . import artifact_ingestion as ingestion
from .artifact_recovery import _admitted


@dataclass(frozen=True)
class FailedStagingRequest:
    """Explicit local selection; paths and roots must come from the operator."""

    path: Path
    manifest: codec.ArtifactManifest
    index: int
    expected_digest: str
    request_id: str
    ledger_directory: Path
    store_id: str
    limits: admission.AdmissionLimits
    max_read_bytes: int


@dataclass(frozen=True)
class FailedStagingEvidence:
    """Local observation to compare again, not a transferable authorization."""

    request_id: str
    ledger: admission.AdmissionSnapshot
    directory_identity: tuple[int, int]
    database_identity: tuple[int, int]
    file_stamp: tuple[int, int, int, int, int]
    names: tuple[str, ...]
    observed_sha256: str
    observed_transfer_digest: str | None
    classification: Literal["partial", "corrupt"]
    bytes_read: int
    publisher_authorized: Literal[False] = False
    retention_confirmed: Literal[False] = False


@dataclass(frozen=True)
class FragmentQuarantine:
    request_id: str
    path: Path
    outcome: Literal["quarantined", "removed_quarantine_alias", "already_quarantined"]
    bytes_read: int
    charge_released: Literal[False] = False
    integrity_verified: Literal[False] = False
    publisher_authorized: Literal[False] = False
    retention_confirmed: Literal[False] = False


def _fence(
    request: FailedStagingRequest,
    ledger: admission.ArtifactAdmission,
    directory: int,
    snapshot: admission.AdmissionSnapshot,
) -> None:
    directories._validate_binding(
        ledger, directory, request.ledger_directory, request.store_id, request.limits
    )
    if ledger.snapshot() != snapshot:
        raise admission.AdmissionConflict("admission changed during quarantine")


def _observe_bytes(
    fd: int,
    directory: int,
    name: str,
    record: admission.FragmentAdmission,
    check_cancel: codec.CheckCancel | None,
) -> tuple[os.stat_result, str, str | None]:
    before = os.fstat(fd)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or before.st_mode & 0o077
        or not 0 <= before.st_size <= record.size
    ):
        raise ValueError("failed staging must be private, regular and within admission")
    digest, transfer_digest = hashlib.sha256(b"").hexdigest(), None
    if before.st_size:
        transfer = files._read_fragment(
            fd, record.project, before.st_size, check_cancel
        )
        digest, transfer_digest = transfer.sha256, transfer.digest
    named = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if files._stamp(before) != files._stamp(os.fstat(fd)) or files._stamp(
        before
    ) != files._stamp(named):
        raise ValueError("failed staging changed during observation")
    if before.st_size == record.size and (digest, transfer_digest) == (
        record.sha256,
        record.transfer_digest,
    ):
        raise ValueError("complete valid staging requires verified recovery")
    return before, digest, transfer_digest


@contextmanager
def _observation(
    request: FailedStagingRequest,
    check_cancel: codec.CheckCancel | None,
) -> Iterator[tuple[FailedStagingEvidence, admission.ArtifactAdmission, int, int]]:
    codec._trusted(request.manifest, request.expected_digest)
    codec._integer(request.max_read_bytes, 1, codec.MAX_FRAGMENT_BYTES)
    if request.manifest.fragment_size(request.index) > request.max_read_bytes:
        raise ValueError("failed-staging read budget must cover admitted size")
    codec._cancel(check_cancel)
    with directories._owned_ledger(
        request.path, request.ledger_directory, request.store_id, request.limits
    ) as (
        ledger,
        directory,
    ):
        snapshot = ledger.snapshot()
        record = _admitted(
            snapshot,
            request.request_id,
            request.manifest,
            request.index,
            request.expected_digest,
        )
        entries, _, blocked = directories._account(
            directory,
            {row.request_id: row.size for row in snapshot.records},
            check_cancel,
            recovering=request.request_id,
        )
        names = tuple(
            entry.name for entry in entries if entry.request_id == request.request_id
        )
        if blocked or not names or request.request_id + ".fragment" in names:
            raise ValueError("ambiguous failed staging requires operator investigation")
        fd: int | None = None
        try:
            fd = os.open(
                names[0], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
            )
            before, digest, transfer_digest = _observe_bytes(
                fd, directory, names[0], record, check_cancel
            )
            for name in names:
                ingestion._same_file(fd, directory, name)
            if before.st_nlink != len(names):
                raise ValueError("failed staging has unaccounted links")
            directory_info = os.fstat(directory)
            database = (request.ledger_directory / "admission.sqlite3").stat()
            evidence = FailedStagingEvidence(
                request.request_id,
                snapshot,
                (directory_info.st_dev, directory_info.st_ino),
                (database.st_dev, database.st_ino),
                files._stamp(before),
                names,
                digest,
                transfer_digest,
                "partial" if before.st_size < record.size else "corrupt",
                before.st_size,
            )
            codec._cancel(check_cancel)
            _fence(request, ledger, directory, snapshot)
            yield evidence, ledger, directory, fd
        finally:
            directories._close_known(fd)


def inspect_failed_staging(
    request: FailedStagingRequest,
    *,
    check_cancel: codec.CheckCancel | None = None,
) -> FailedStagingEvidence:
    """Read bounded failed bytes only after positively validating local ownership."""
    with _observation(request, check_cancel) as (evidence, _, _, _):
        return evidence


def quarantine_failed_staging(
    request: FailedStagingRequest,
    evidence: FailedStagingEvidence,
    *,
    check_cancel: codec.CheckCancel | None = None,
) -> FragmentQuarantine:
    """Re-observe exact evidence, preserve failed bytes and retain the full charge.

    After an uncertain result, explicitly inspect again before retrying. Evidence
    can become stale through our own link/unlink steps. Never infer permission to
    restore ownership, reclaim capacity or delete quarantined bytes from age.
    """
    with _observation(request, check_cancel) as (observed, ledger, directory, fd):
        if observed != evidence:
            raise ValueError("failed-staging evidence is stale or mismatched")
        os.fsync(fd)
        codec._cancel(check_cancel)
        _fence(request, ledger, directory, observed.ledger)
        before = os.fstat(fd)
        if files._stamp(before) != observed.file_stamp:
            raise ValueError("failed staging changed before quarantine")
        stage, destination = (
            request.request_id + ".staging",
            request.request_id + ".quarantine",
        )
        for name in observed.names:
            ingestion._same_file(fd, directory, name)
        outcome: Literal[
            "quarantined", "removed_quarantine_alias", "already_quarantined"
        ]
        if stage not in observed.names:
            os.fsync(directory)
            outcome = "already_quarantined"
        elif destination in observed.names:
            ingestion._finish_publication(directory, fd, stage, destination)
            outcome = "removed_quarantine_alias"
        else:
            ingestion._publish(directory, fd, stage, destination, before)
            outcome = "quarantined"
        _fence(request, ledger, directory, observed.ledger)
        return FragmentQuarantine(
            request.request_id, request.path / destination, outcome, observed.bytes_read
        )
