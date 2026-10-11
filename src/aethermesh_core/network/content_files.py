"""CLI-only bounded regular-file snapshots and atomic, no-overwrite output.

Directory descriptors anchor every path component. Unsupported platforms fail
closed; there is no check-then-follow fallback for symlinks. Callers must own and
trust the selected directories; this is not a sandbox against hostile local users.
"""

from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .content import MAX_CONTENT_BYTES


@contextmanager
def selected_parent(path: Path) -> Iterator[tuple[int, str]]:
    """Open existing physical directories, rejecting symlinks and '..'."""
    if (
        os.name != "posix"
        or not hasattr(os, "O_NOFOLLOW")
        or os.open not in os.supports_dir_fd
        or os.link not in os.supports_dir_fd
    ):
        raise ValueError("safe content file operations are unavailable")
    parts = path.parts[1:] if path.is_absolute() else path.parts
    if not parts or ".." in parts:
        raise ValueError("content requires a file path without parent traversal")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open(path.anchor if path.is_absolute() else ".", flags)
    try:
        for part in parts[:-1]:
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        yield directory, parts[-1]
    finally:
        os.close(directory)


def read_selected(path: Path) -> bytes:
    """Read one regular file, never following a link or blocking on a FIFO."""
    with selected_parent(path) as (directory, name):
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_CONTENT_BYTES:
                raise ValueError("content input must be a small regular file")
            data = stream.read(MAX_CONTENT_BYTES + 1)
            if len(data) > MAX_CONTENT_BYTES:
                raise ValueError("content input grew beyond the size limit")
            return data


def require_absent(directory: int, name: str) -> None:
    """Reject existing destinations before connecting; publication rechecks atomically."""
    try:
        os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise FileExistsError("content output already exists")


def publish_new(directory: int, name: str, data: bytes) -> None:
    """Publish complete verified bytes with owner-only mode and no overwrite.

    A same-directory hard link atomically installs the completed temporary file.
    No peer chooses a filename. A process crash can leave a temporary file; a
    successful link is the commit point and later cancellation cannot undo it.
    """
    if len(data) > MAX_CONTENT_BYTES:
        raise ValueError("content exceeds the output size limit")
    temporary = ".aethermesh-content-" + secrets.token_hex(16)
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory,
    )
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(
            temporary,
            name,
            src_dir_fd=directory,
            dst_dir_fd=directory,
            follow_symlinks=False,
        )
    finally:
        os.unlink(temporary, dir_fd=directory)
