"""Private retained-directory layout and bounded, non-destructive accounting.

No payload ingestion, adoption, deletion, expiry, charge release or retention
proof. Every uncertain entry blocks future ingestion until explicit resolution.
"""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Literal

from .artifact_admission import AdmissionConflict, AdmissionLimits, ArtifactAdmission
from .artifact_codec import CheckCancel, _cancel
from .network.content_files import selected_parent
from .network.transfer_files import _private, _read

_MARKER = "store.json"
_MAX_ENTRIES = 385  # Marker plus at most two names per 192 reservations.


@dataclass(frozen=True)
class DirectoryEntry:
    name: str
    request_id: str | None
    phase: str
    regular_bytes: int | None
    problem: str | None


@dataclass(frozen=True)
class DirectoryAccounting:
    ledger_revision: int
    charged_bytes: int
    observed_regular_bytes: int
    entries: tuple[DirectoryEntry, ...]
    blocked: bool
    content_verified: Literal[False] = False
    retention_confirmed: Literal[False] = False
    publisher_authorized: Literal[False] = False


def _close_known(fd: int | None) -> None:
    if fd is not None:
        os.close(fd)


@contextmanager
def _directory(path: Path, *, create: bool) -> Iterator[int]:
    if not path.is_absolute():
        raise ValueError("owned directory requires an absolute path")
    with selected_parent(path) as (parent, name):
        if create:
            os.mkdir(name, 0o700, dir_fd=parent)
            os.fsync(parent)
        fd: int | None = None
        try:
            fd = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
            )
            _private(os.fstat(fd), directory=True)
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            before = os.fstat(fd)
            yield fd
            named = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if (before.st_dev, before.st_ino) != (named.st_dev, named.st_ino):
                raise ValueError("owned directory was replaced")
        finally:
            _close_known(fd)


def _binding(ledger_directory: Path, store_id: str, limits: AdmissionLimits) -> bytes:
    info = (ledger_directory / "admission.sqlite3").stat()
    return json.dumps(
        {
            "version": 1,
            "store_id": store_id,
            "policy": limits.canonical,
            "database_device": info.st_dev,
            "database_inode": info.st_ino,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _write_marker(directory: int, data: bytes) -> None:
    fd: int | None = None
    try:
        fd = os.open(
            _MARKER,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        remaining = memoryview(data)
        while remaining:
            count = os.write(fd, remaining)
            if count <= 0:
                raise OSError("owned-directory marker write failed")
            remaining = remaining[count:]
        os.fsync(fd)
    finally:
        _close_known(fd)


def initialize_artifact_directory(
    path: Path, *, ledger_directory: Path, store_id: str, limits: AdmissionLimits
) -> None:
    """Exclusively initialize a selected private directory for an existing ledger.

    The marker pins format, namespace, policy and the local database inode.
    Failures preserve the incomplete directory for explicit inspection; never
    adopt/overwrite it or automatically remove files. No payload is ingested.
    """
    with (
        closing(
            ArtifactAdmission(ledger_directory, store_id=store_id, limits=limits)
        ) as ledger,
        _directory(path, create=True) as directory,
    ):
        info = os.fstat(directory)
        ledger._bind_directory(info.st_dev, info.st_ino)
        _write_marker(directory, _binding(ledger_directory, store_id, limits))
        os.fsync(directory)


def _entry(name: str, info: os.stat_result, known: set[str]) -> DirectoryEntry:
    match = re.fullmatch(r"([0-9a-f]{64})\.(fragment|staging|quarantine)", name)
    request = match[1] if match else None
    phase = match[2] if match else "unknown"
    size = info.st_size if stat.S_ISREG(info.st_mode) else None
    problem = None
    if size is None or info.st_uid != os.getuid() or info.st_mode & 0o077:
        problem = "unsafe_entry"
    elif request not in known:
        problem = "orphan_entry"
    elif phase == "staging":
        problem = "unfinished_staging"
    return DirectoryEntry(name, request, phase, size, problem)


def _account(
    directory: int,
    sizes: dict[str, int],
    check_cancel: CheckCancel | None,
    *,
    recovering: str | None = None,
) -> tuple[tuple[DirectoryEntry, ...], int, bool]:
    with os.scandir(directory) as scan:
        names = sorted(entry.name for entry in islice(scan, _MAX_ENTRIES + 1))
    if len(names) > _MAX_ENTRIES:
        raise ValueError("owned directory entry bound exceeded")
    entries = []
    inodes: dict[tuple[int, int], tuple[os.stat_result, list[DirectoryEntry]]] = {}
    known = set(sizes)
    for name in names:
        _cancel(check_cancel)
        if name == _MARKER:
            continue
        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
        entry = _entry(name, info, known)
        entries.append(entry)
        key = (info.st_dev, info.st_ino)
        if key not in inodes:
            inodes[key] = (info, [])
        inodes[key][1].append(entry)
    total = 0
    charged_usage: dict[str, int] = {}
    blocked = any(
        entry.problem is not None
        and not (
            entry.request_id == recovering and entry.problem == "unfinished_staging"
        )
        for entry in entries
    )
    quarantined = {entry.request_id for entry in entries if entry.phase == "quarantine"}
    if any(
        entry.phase == "fragment" and entry.request_id in quarantined
        for entry in entries
    ):
        blocked = True
    for info, linked in inodes.values():
        if not stat.S_ISREG(info.st_mode):
            continue
        total += info.st_size
        requests = {entry.request_id for entry in linked}
        if info.st_nlink != len(linked) or len(requests) != 1:
            blocked = True
        for request in requests:
            if request in sizes:
                charged_usage[request] = charged_usage.get(request, 0) + info.st_size
        if any(
            entry.phase == "fragment"
            and info.st_size != sizes.get(entry.request_id or "")
            for entry in linked
        ):
            blocked = True
    if any(size > sizes[request] for request, size in charged_usage.items()):
        blocked = True
    return tuple(entries), total, blocked


def _validate_binding(
    ledger: ArtifactAdmission,
    directory: int,
    ledger_directory: Path,
    store_id: str,
    limits: AdmissionLimits,
) -> None:
    info = os.fstat(directory)
    if ledger._directory_binding() != (info.st_dev, info.st_ino):
        raise ValueError("ledger does not own this directory")
    if _read(directory, _MARKER) != _binding(ledger_directory, store_id, limits):
        raise ValueError("owned directory marker or ledger binding mismatch")


@contextmanager
def _owned_ledger(
    path: Path, ledger_directory: Path, store_id: str, limits: AdmissionLimits
) -> Iterator[tuple[ArtifactAdmission, int]]:
    with (
        closing(
            ArtifactAdmission(ledger_directory, store_id=store_id, limits=limits)
        ) as ledger,
        _directory(path, create=False) as directory,
    ):
        _validate_binding(ledger, directory, ledger_directory, store_id, limits)
        yield ledger, directory


def inspect_artifact_directory(
    path: Path,
    *,
    ledger_directory: Path,
    store_id: str,
    limits: AdmissionLimits,
    check_cancel: CheckCancel | None = None,
) -> DirectoryAccounting:
    """Inspect at most 385 entries under an exclusive nonblocking directory lock.

    Read only the bounded marker, never payloads. Regular-file logical lengths
    are counted once per observed inode, including orphan regular files. Other
    object types and unknown links block the report. This does not measure disk
    blocks or reconcile unknown entries into the admission ledger.

    All cooperating future writers must lock this same directory inode. Neither
    permissions nor advisory locks isolate hostile same-user code. Reports are
    observations, not ingestion grants; no charges or files are changed.
    """
    _cancel(check_cancel)
    with _owned_ledger(path, ledger_directory, store_id, limits) as (ledger, directory):
        snapshot = ledger.snapshot()
        entries, total, blocked = _account(
            directory,
            {record.request_id: record.size for record in snapshot.records},
            check_cancel,
        )
        _cancel(check_cancel)
        if ledger.snapshot() != snapshot:
            raise AdmissionConflict("admission changed during directory accounting")
        return DirectoryAccounting(
            snapshot.revision, snapshot.charged_bytes, total, entries, blocked
        )
