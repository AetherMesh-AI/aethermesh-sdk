"""Bounded file-to-file recovery; no hosting, persistent quota or retention claim."""

from __future__ import annotations

import hashlib
import os
import secrets
import stat
import threading
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path

from .artifact_codec import (
    GENERATOR,
    MAX_ARTIFACT_BYTES,
    ArtifactManifest,
    ArtifactPublication,
    CheckCancel,
    _cancel,
    _combine,
    _integer,
    _inverse,
    _trusted,
)
from .artifact_files import _read_fragment, _stamp
from .network.content_files import require_absent, selected_parent
from .network.transfer import TransferManifest


@dataclass(frozen=True)
class StagingUsage:
    bytes: int
    jobs: int
    blocked: bool


class StagingQuota:
    """Caller-shared, nonblocking process-local admission for temporary outputs.

    Reuse one instance across concurrent calls in the same resource scope.
    Separate instances/processes do not coordinate. This neither reserves actual
    filesystem blocks nor accounts for retained outputs after publication.
    Uncertain temporary-file cleanup blocks all new admission; reconciliation
    and creating a replacement quota require explicit caller action.
    """

    def __init__(self, *, max_bytes: int, max_jobs: int) -> None:
        _integer(max_bytes, 1, 4 * MAX_ARTIFACT_BYTES)
        _integer(max_jobs, 1, 4)
        self._max_bytes = max_bytes
        self._max_jobs = max_jobs
        self._bytes = self._jobs = 0
        self._blocked = False
        self._lock = threading.Lock()

    def usage(self) -> StagingUsage:
        with self._lock:
            return StagingUsage(self._bytes, self._jobs, self._blocked)

    def _poison(self) -> None:
        with self._lock:
            self._blocked = True

    @contextmanager
    def reserve(self, size: int) -> Iterator[None]:
        _integer(size, 1, MAX_ARTIFACT_BYTES)
        with self._lock:
            if (
                self._blocked
                or self._bytes + size > self._max_bytes
                or self._jobs >= self._max_jobs
            ):
                raise ValueError("staging quota exhausted")
            self._bytes += size
            self._jobs += 1
        try:
            yield
        finally:
            with self._lock:
                self._bytes -= size
                self._jobs -= 1


@dataclass(frozen=True)
class _Source:
    index: int
    fd: int
    directory: int
    name: str
    before: os.stat_result


def _selection(
    manifest: ArtifactManifest, files: tuple[tuple[int, Path], ...], max_read_bytes: int
) -> dict[int, Path]:
    _integer(max_read_bytes, 1, 4 * MAX_ARTIFACT_BYTES)
    stripes = len(manifest.fragments) // 6
    if not isinstance(files, tuple) or len(files) != stripes * 4:
        raise ValueError("select exactly four files per stripe")
    selected: dict[int, Path] = {}
    for entry in files:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise ValueError("invalid fragment file selection")
        index, path = entry
        manifest.fragment_size(index)
        if index in selected or not isinstance(path, Path):
            raise ValueError("duplicate index or invalid path")
        selected[index] = path
    for start in range(0, len(manifest.fragments), 6):
        if sum(index in selected for index in range(start, start + 6)) != 4:
            raise ValueError("select exactly four files per stripe")
    # First pass verifies inputs, second decodes, third verifies staged original.
    cost = 2 * sum(manifest.fragment_size(i) for i in selected) + manifest.source_size
    if cost > max_read_bytes:
        raise ValueError("streaming read budget exceeded")
    return selected


def _open_stripe(
    stack: ExitStack, manifest: ArtifactManifest, selected: dict[int, Path], start: int
) -> tuple[_Source, ...]:
    sources = []
    for index in range(start, start + 6):
        if index not in selected:
            continue
        directory, name = stack.enter_context(selected_parent(selected[index]))
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        stack.callback(os.close, fd)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size != manifest.fragment_size(
            index
        ):
            raise ValueError("fragment input type or size mismatch")
        sources.append(_Source(index, fd, directory, name, before))
    return tuple(sources)


def _unchanged(source: _Source) -> None:
    after = os.fstat(source.fd)
    named = os.stat(source.name, dir_fd=source.directory, follow_symlinks=False)
    if _stamp(source.before) != _stamp(after) or _stamp(source.before) != _stamp(named):
        raise ValueError("fragment input changed")


def _read_block(fd: int, size: int, check: CheckCancel | None) -> bytes:
    block = bytearray()
    while len(block) < size:
        _cancel(check)
        part = os.read(fd, size - len(block))
        if not part:
            raise ValueError("fragment input truncated")
        block.extend(part)
    return bytes(block)


def _write_block(fd: int, data: bytes, offset: int, check: CheckCancel | None) -> None:
    written = 0
    while written < len(data):
        _cancel(check)
        count = os.pwrite(fd, data[written:], offset + written)
        if count == 0:
            raise OSError("staging write made no progress")
        written += count


def _decode_stripe(
    output: int,
    manifest: ArtifactManifest,
    sources: tuple[_Source, ...],
    start: int,
    check: CheckCancel | None,
) -> None:
    size = manifest.fragment_size(start)
    inverse = _inverse(tuple(GENERATOR[source.index % 6] for source in sources))
    whole = [hashlib.sha256() for _ in range(6)]
    chunks: list[list[str]] = [[] for _ in range(6)]
    stripe_offset = (start // 6) * 4 * manifest.fragment_bytes
    for offset in range(0, size, 32768):
        width = min(32768, size - offset)
        encoded = tuple(_read_block(source.fd, width, check) for source in sources)
        data = tuple(_combine(row, encoded, check) for row in inverse)
        codeword = data + tuple(_combine(row, data, check) for row in GENERATOR[4:])
        for index, block in enumerate(codeword):
            whole[index].update(block)
            chunks[index].append(hashlib.sha256(block).hexdigest())
            if index < 4:
                position = stripe_offset + index * size + offset
                take = max(0, min(len(block), manifest.source_size - position))
                if any(block[take:]):
                    raise ValueError("nonzero decoded padding")
                _write_block(output, block[:take], position, check)
    for index in range(6):
        transfer = TransferManifest(
            manifest.project, whole[index].hexdigest(), size, tuple(chunks[index])
        )
        if (transfer.sha256, transfer.digest) != manifest.fragments[start + index]:
            raise ValueError("decoded stripe integrity mismatch")


def _recover(
    output: int,
    manifest: ArtifactManifest,
    selected: dict[int, Path],
    check: CheckCancel | None,
) -> None:
    for start in range(0, len(manifest.fragments), 6):
        with ExitStack() as stack:
            sources = _open_stripe(stack, manifest, selected, start)
            for source in sources:
                transfer = _read_fragment(
                    source.fd, manifest.project, source.before.st_size, check
                )
                if (transfer.sha256, transfer.digest) != manifest.fragments[
                    source.index
                ]:
                    raise ValueError("input fragment integrity mismatch")
                _unchanged(source)
                os.lseek(source.fd, 0, os.SEEK_SET)
            _decode_stripe(output, manifest, sources, start, check)
            for source in sources:
                _unchanged(source)
    before = os.fstat(output)
    if before.st_size != manifest.source_size:
        raise ValueError("staged output size mismatch")
    os.lseek(output, 0, os.SEEK_SET)
    transfer = _read_fragment(output, manifest.project, manifest.source_size, check)
    if _stamp(before) != _stamp(os.fstat(output)):
        raise ValueError("staged original changed during verification")
    if transfer.sha256 != manifest.source_sha256:
        raise ValueError("staged original integrity mismatch")


def publish_artifact_files(
    manifest: ArtifactManifest,
    files: tuple[tuple[int, Path], ...],
    output: Path,
    *,
    expected_digest: str,
    max_read_bytes: int,
    staging: StagingQuota,
    check_cancel: CheckCancel | None = None,
) -> ArtifactPublication:
    """Stream four selected files per stripe into private, verified publication.

    Read admission counts two passes over selected input bytes plus one over the
    staged original. Reserve the whole original's logical staging size before
    I/O. Existing artifact ceilings remain; quota is shared only by callers using
    this same object in one process. Inputs are verified before decode and full
    codeword/original hashes are checked again before no-overwrite publication.

    No cleanup is promised after process/power loss. A crash can leave a private
    temporary file; an error after linking can leave the destination committed.
    Inspect those paths rather than retrying with overwrite. No retention or
    power-loss durability, publisher permission, or hostile-user sandbox claim.
    """
    _trusted(manifest, expected_digest)
    selected = _selection(manifest, files, max_read_bytes)
    if not isinstance(staging, StagingQuota):
        raise TypeError("explicit staging quota required")
    _cancel(check_cancel)
    with (
        staging.reserve(manifest.source_size),
        selected_parent(output) as (directory, name),
    ):
        require_absent(directory, name)
        temporary = ".aethermesh-stage-" + secrets.token_hex(16)
        fd = os.open(
            temporary,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        try:
            _recover(fd, manifest, selected, check_cancel)
            os.fsync(fd)
            _cancel(check_cancel)
            os.link(
                temporary,
                name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
        finally:
            try:
                try:
                    os.close(fd)
                finally:
                    os.unlink(temporary, dir_fd=directory)
            except BaseException:
                staging._poison()
                raise
    return ArtifactPublication(expected_digest, output)
