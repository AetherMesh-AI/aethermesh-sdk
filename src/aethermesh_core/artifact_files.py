"""Read-only, bounded-memory verification of one explicitly selected fragment.

No retained-store admission, hosting, encryption, or publisher authorization.
A result describes the bytes observed during this call, not a reusable file
capability or a promise that the path will still hold those bytes later.
"""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .artifact_codec import (
    MAX_FRAGMENT_BYTES,
    ArtifactManifest,
    CheckCancel,
    _cancel,
    _integer,
    _trusted,
)
from .network.content import MAX_CONTENT_BYTES
from .network.content_files import selected_parent
from .network.transfer import TransferManifest


@dataclass(frozen=True)
class FragmentFileVerification:
    manifest_digest: str
    fragment_index: int
    transfer: TransferManifest
    bytes_read: int
    integrity_verified: Literal[True] = True
    publisher_authorized: Literal[False] = False
    retention_confirmed: Literal[False] = False


def _stamp(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read_fragment(
    fd: int, project: str, size: int, check_cancel: CheckCancel | None
) -> TransferManifest:
    whole = hashlib.sha256()
    chunk = hashlib.sha256()
    chunks: list[str] = []
    total = in_chunk = 0
    while total < size:
        _cancel(check_cancel)
        block = os.read(fd, min(MAX_CONTENT_BYTES - in_chunk, size - total))
        if not block:
            raise ValueError("fragment file truncated during verification")
        whole.update(block)
        chunk.update(block)
        total += len(block)
        in_chunk += len(block)
        if in_chunk == MAX_CONTENT_BYTES or total == size:
            chunks.append(chunk.hexdigest())
            chunk = hashlib.sha256()
            in_chunk = 0
    return TransferManifest(project, whole.hexdigest(), size, tuple(chunks))


def verify_fragment_file(
    manifest: ArtifactManifest,
    index: int,
    path: Path,
    *,
    expected_digest: str,
    max_read_bytes: int,
    check_cancel: CheckCancel | None = None,
) -> FragmentFileVerification:
    """Verify one regular file against an explicitly trusted artifact root.

    Admit the logical read budget before opening the file, then check its type
    and exact length before reading. Read requests never exceed 32 KiB; short
    reads preserve transport chunk boundaries. Cancellation is checked before
    opening, before each read, and before returning the integrity-only result.

    Paths must traverse trusted physical directories on supported POSIX systems.
    Symlinks and nonregular files fail closed. Observable mutation/replacement is
    rejected, but this is not a sandbox against hostile same-user processes.
    Reverify when using or serving the file; a returned result grants no lease,
    durable retention, publisher authority, or future path integrity.
    """
    _trusted(manifest, expected_digest)
    _integer(max_read_bytes, 1, MAX_FRAGMENT_BYTES)
    size = manifest.fragment_size(index)
    if size > max_read_bytes:
        raise ValueError("fragment read budget exceeded")
    _cancel(check_cancel)
    with selected_parent(path) as (directory, name):
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                raise ValueError("fragment input must be a regular file of exact size")
            transfer = _read_fragment(fd, manifest.project, size, check_cancel)
            after = os.fstat(fd)
            named = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if _stamp(before) != _stamp(after) or _stamp(before) != _stamp(named):
                raise ValueError("fragment file changed during verification")
            expected_hash, expected_transfer = manifest.fragments[index]
            if transfer.sha256 != expected_hash or transfer.digest != expected_transfer:
                raise ValueError("fragment file integrity or transfer binding mismatch")
            _cancel(check_cancel)
            return FragmentFileVerification(expected_digest, index, transfer, size)
        finally:
            os.close(fd)
