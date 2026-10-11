"""Explicit private checkpoints for resumable opaque bytes, never executable input."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import islice
from pathlib import Path

from .client import PeerClient
from .content import MAX_CONTENT_BYTES
from .content_files import publish_new, selected_parent
from .transfer import MAX_TRANSFER_BYTES, TransferManifest


def _private(
    info: os.stat_result, *, directory: bool = False, max_links: int = 1
) -> None:
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("checkpoint must be private and owned by the current user")
    if not directory and (
        not 1 <= info.st_nlink <= max_links or info.st_size > MAX_CONTENT_BYTES
    ):
        raise ValueError("invalid checkpoint file")


def _read(directory: int, name: str) -> bytes | None:
    try:
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
    except FileNotFoundError:
        return None
    try:
        _private(os.fstat(fd))
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(MAX_CONTENT_BYTES + 1)
        if len(data) > MAX_CONTENT_BYTES:
            raise ValueError("checkpoint grew beyond its limit")
        return data
    finally:
        os.close(fd)


def _save(directory: int, name: str, data: bytes) -> None:
    publish_new(directory, name, data)
    os.fsync(directory)


def _remove_orphan(directory: int, name: str, published: set[str]) -> None:
    """Recover the crash window between link publication and temporary unlink."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        info = os.fstat(fd)
        _private(info, max_links=2)
        if info.st_nlink == 2:
            linked = False
            for candidate in published:
                try:
                    other = os.stat(candidate, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if (other.st_dev, other.st_ino) == (info.st_dev, info.st_ino):
                    linked = True
                    break
            if not linked:
                raise ValueError("checkpoint temporary file has an unrelated hard link")
        os.unlink(name, dir_fd=directory)
    finally:
        os.close(fd)


@contextmanager
def checkpoint(
    path: Path, manifest: TransferManifest, *, resume: bool
) -> Iterator[int]:
    """Lock an explicit 0700 directory; durable chunk links are the checkpoints.

    POSIX filesystems must support directory fsync and advisory directory locks.
    Trusted owned parents are required; this does not isolate hostile same-UID code.
    """
    with selected_parent(path) as (parent, name):
        if not resume:
            os.mkdir(name, 0o700, dir_fd=parent)
            os.fsync(parent)
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            _private(os.fstat(fd), directory=True)
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.fsync(fd)
            with os.scandir(fd) as entries:
                names = [entry.name for entry in islice(entries, 259)]
            if len(names) > 258:
                raise ValueError("too many checkpoint entries")
            allowed = {f"{index:03}.chunk" for index in range(len(manifest.chunks))}
            for item in names:
                if re.fullmatch(r"\.aethermesh-content-[0-9a-f]{32}", item):
                    _remove_orphan(fd, item, allowed | {"manifest.json"})
                elif item not in allowed | {"manifest.json"}:
                    raise ValueError("unexpected checkpoint entry")
            saved = _read(fd, "manifest.json")
            if saved is None:
                if any(item in allowed for item in names):
                    raise ValueError("chunks have no manifest")
                _save(fd, "manifest.json", manifest.encode())
            elif saved != manifest.encode():
                raise ValueError("checkpoint manifest mismatch")
            os.fsync(fd)
            yield fd
        finally:
            os.close(fd)


def verified_chunk(
    directory: int, manifest: TransferManifest, index: int
) -> bytes | None:
    """Rehash every stored chunk; discard corrupt private regular files safely."""
    name = f"{index:03}.chunk"
    data = _read(directory, name)
    ref = manifest.chunk(index)
    if data is not None and (
        len(data) != ref.size or hashlib.sha256(data).hexdigest() != ref.sha256
    ):
        os.unlink(name, dir_fd=directory)
        os.fsync(directory)
        return None
    return data


async def download_chunks(
    client: PeerClient, manifest: TransferManifest, directory: int, *, timeout: float
) -> int:
    """Fetch only missing verified chunks; no retry, reconnect or hidden worker."""
    fetched = 0
    for index in range(len(manifest.chunks)):
        if verified_chunk(directory, manifest, index) is None:
            data = await client.fetch_chunk(manifest, index, timeout=timeout)
            _save(directory, f"{index:03}.chunk", data)
            fetched += 1
    return fetched


def assemble(directory: int, manifest: TransferManifest) -> bytes:
    """Reverify the bounded complete file immediately before publication."""
    chunks = []
    for index in range(len(manifest.chunks)):
        data = verified_chunk(directory, manifest, index)
        if data is None:
            raise ValueError("checkpoint changed during assembly")
        chunks.append(data)
    data = b"".join(chunks)
    if (
        len(data) != manifest.size
        or hashlib.sha256(data).hexdigest() != manifest.sha256
    ):
        raise ValueError("whole transfer integrity mismatch")
    return data


def publish_transfer(directory: int, name: str, data: bytes) -> None:
    """Fsync completed output and its directory; link is the irreversible commit."""
    publish_new(directory, name, data, limit=MAX_TRANSFER_BYTES)
    os.fsync(directory)


async def resume_download(
    client: PeerClient,
    manifest: TransferManifest,
    path: Path,
    *,
    resume: bool,
    timeout: float,
) -> tuple[bytes, int]:
    """Own the staging lock through chunk verification and complete assembly."""
    state = checkpoint(path, manifest, resume=resume)
    with state as directory:
        fetched = await download_chunks(client, manifest, directory, timeout=timeout)
        return assemble(directory, manifest), fetched
