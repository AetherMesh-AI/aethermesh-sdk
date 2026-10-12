"""Bounded local payload ingestion into a durably claimed private directory.

Capacity is charged before payload I/O and never released here. Failed staging
is preserved; only the verified staging alias of a successful publication is
removed. No network hosting, orphan adoption or publisher authority is implied.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .artifact_admission import (
    AdmissionConflict,
    AdmissionLimits,
    AdmissionSnapshot,
    FragmentAdmission,
)
from .artifact_codec import (
    MAX_FRAGMENT_BYTES,
    ArtifactManifest,
    CheckCancel,
    _cancel,
    _integer,
    _trusted,
)
from .artifact_directory import _account, _close_known, _owned_ledger
from .artifact_files import _read_fragment, _stamp
from .network.content import MAX_CONTENT_BYTES
from .network.content_files import selected_parent


@dataclass(frozen=True)
class FragmentIngestion:
    request_id: str
    manifest_digest: str
    fragment_index: int
    path: Path
    bytes_read: int
    already_present: bool
    integrity_verified: Literal[True] = True
    retention_confirmed: Literal[False] = False
    publisher_authorized: Literal[False] = False


@contextmanager
def _source(path: Path, size: int) -> Iterator[tuple[int, int, str, os.stat_result]]:
    with selected_parent(path) as (parent, name):
        fd: int | None = None
        try:
            fd = os.open(
                name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
            )
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                raise ValueError("ingestion source must be regular and exact length")
            yield fd, parent, name, before
        finally:
            _close_known(fd)


def _copy(
    source: int, target: int, size: int, check_cancel: CheckCancel | None
) -> None:
    total = 0
    while total < size:
        _cancel(check_cancel)
        block = os.read(source, min(MAX_CONTENT_BYTES, size - total))
        if not block:
            raise ValueError("ingestion source truncated")
        remaining = memoryview(block)
        while remaining:
            _cancel(check_cancel)
            count = os.write(target, remaining)
            if count <= 0:
                raise OSError("ingestion write failed")
            remaining = remaining[count:]
        total += len(block)


def _verify(
    fd: int,
    directory: int,
    name: str,
    manifest: ArtifactManifest,
    index: int,
    check_cancel: CheckCancel | None,
) -> os.stat_result:
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or before.st_size != manifest.fragment_size(
        index
    ):
        raise ValueError("ingestion file must be regular and exact length")
    os.lseek(fd, 0, os.SEEK_SET)
    transfer = _read_fragment(fd, manifest.project, before.st_size, check_cancel)
    named = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if _stamp(before) != _stamp(os.fstat(fd)) or _stamp(before) != _stamp(named):
        raise ValueError("ingestion file changed during verification")
    if (transfer.sha256, transfer.digest) != manifest.fragments[index]:
        raise ValueError("ingestion fragment integrity mismatch")
    return before


def _same_file(fd: int, directory: int, name: str) -> None:
    current = os.fstat(fd)
    named = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if (current.st_dev, current.st_ino, current.st_size) != (
        named.st_dev,
        named.st_ino,
        named.st_size,
    ) or not stat.S_ISREG(named.st_mode):
        raise ValueError("ingestion publication pathname changed")


def _publish(
    directory: int, fd: int, stage: str, final: str, verified: os.stat_result
) -> None:
    # Directories are trusted and stable. These checks detect observed pathname
    # changes; they are not an atomic sandbox against hostile same-user writers.
    if _stamp(os.fstat(fd)) != _stamp(verified):
        raise ValueError("verified staging bytes changed before publication")
    _same_file(fd, directory, stage)
    os.link(
        stage, final, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False
    )
    _finish_publication(directory, fd, stage, final)


def _finish_publication(directory: int, fd: int, stage: str, final: str) -> None:
    _same_file(fd, directory, final)
    os.fsync(directory)
    _same_file(fd, directory, stage)
    os.unlink(stage, dir_fd=directory)
    os.fsync(directory)


def _existing(
    directory: int,
    final: str,
    manifest: ArtifactManifest,
    index: int,
    check_cancel: CheckCancel | None,
) -> None:
    fd: int | None = None
    try:
        fd = os.open(
            final, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        _verify(fd, directory, final, manifest, index, check_cancel)
        _cancel(check_cancel)
        os.fsync(fd)
        os.fsync(directory)
    finally:
        _close_known(fd)


def _expected_reservation(
    initial: AdmissionSnapshot, record: FragmentAdmission
) -> AdmissionSnapshot:
    existing = next(
        (row for row in initial.records if row.request_id == record.request_id), None
    )
    if existing is not None:
        if record != existing:
            raise AdmissionConflict("admission changed during reservation")
        return initial
    if record.revision != initial.revision + 1:
        raise AdmissionConflict("admission changed during reservation")
    return AdmissionSnapshot(
        initial.revision + 1,
        initial.charged_bytes + record.size,
        tuple(sorted((*initial.records, record), key=lambda row: row.request_id)),
    )


def ingest_fragment_file(
    path: Path,
    source: Path,
    manifest: ArtifactManifest,
    index: int,
    *,
    expected_digest: str,
    request_id: str,
    ledger_directory: Path,
    store_id: str,
    limits: AdmissionLimits,
    max_read_bytes: int,
    check_cancel: CheckCancel | None = None,
) -> FragmentIngestion:
    """Reserve, copy, rehash and no-overwrite publish one explicit local fragment.

    The full two-pass logical read budget is required before any ledger mutation
    or payload open. One directory lock serializes cooperating ingesters. Existing
    orphan/staging/unsafe accounting blocks ingestion. A fresh reservation is
    committed before source or staging I/O; all failures keep it charged.

    Cancellation is honored through the final pre-link check. A successful link
    is the publication commit point; later sync/close errors may mean the final
    exists. Never infer absence or permission to reclaim from a raised exception.
    Retries only reverify a clean existing final; they never clean failed staging.
    """
    _trusted(manifest, expected_digest)
    size = manifest.fragment_size(index)
    _integer(max_read_bytes, 1, 2 * MAX_FRAGMENT_BYTES)
    if 2 * size > max_read_bytes:
        raise ValueError("ingestion two-pass read budget exceeded")
    if not source.is_absolute():
        raise ValueError("ingestion requires an explicit absolute source path")
    _cancel(check_cancel)
    with _owned_ledger(path, ledger_directory, store_id, limits) as (ledger, directory):
        initial = ledger.snapshot()
        entries, _, blocked = _account(
            directory,
            {row.request_id: row.size for row in initial.records},
            check_cancel,
        )
        if blocked:
            raise ValueError("owned directory requires explicit reconciliation")
        if any(
            entry.request_id == request_id and entry.phase == "quarantine"
            for entry in entries
        ):
            raise ValueError("quarantined request cannot reuse its admission")
        _cancel(check_cancel)
        if ledger.snapshot() != initial:
            raise AdmissionConflict("admission changed before ingestion")
        record = ledger.reserve(
            request_id, manifest, index, expected_digest=expected_digest
        )
        admitted = ledger.snapshot()
        if admitted != _expected_reservation(initial, record):
            raise AdmissionConflict("admission changed during reservation")
        _cancel(check_cancel)
        final, stage = request_id + ".fragment", request_id + ".staging"
        existing = any(entry.name == final for entry in entries)
        if existing:
            _existing(directory, final, manifest, index, check_cancel)
        else:
            with _source(source, size) as (input_fd, parent, name, before):
                fd: int | None = None
                try:
                    fd = os.open(
                        stage,
                        os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                        0o600,
                        dir_fd=directory,
                    )
                    _copy(input_fd, fd, size, check_cancel)
                    named = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if _stamp(before) != _stamp(os.fstat(input_fd)) or _stamp(
                        before
                    ) != _stamp(named):
                        raise ValueError("ingestion source changed during copy")
                    os.fsync(fd)
                    verified = _verify(
                        fd, directory, stage, manifest, index, check_cancel
                    )
                    _cancel(check_cancel)
                    if ledger.snapshot() != admitted:
                        raise AdmissionConflict("admission changed before publication")
                    _cancel(check_cancel)
                    _publish(directory, fd, stage, final, verified)
                finally:
                    _close_known(fd)
        if ledger.snapshot() != admitted:
            raise AdmissionConflict("admission changed during ingestion")
        return FragmentIngestion(
            record.request_id,
            expected_digest,
            index,
            path / final,
            size if existing else 2 * size,
            existing,
        )
