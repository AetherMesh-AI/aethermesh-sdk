"""Opt-in bounded 4+2 erasure packaging of opaque synthetic/public artifacts.

No encryption, publisher authorization, hosting, model loading, or deployment.
This deliberately small in-memory foundation cannot package whole large datasets.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from .network.config import validate_fingerprint, validate_project_id
from .network.content_files import publish_new, require_absent, selected_parent
from .network.transfer import TransferManifest, TransferStore

MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_FRAGMENT_BYTES = 5_000_000
MAX_FRAGMENTS = 192
CODEC = "rs4+2-gf256-11d-systematic-v1"
# Systematic Vandermonde: V[i,j]=(2**i)**j in GF(256), G=V*inverse(V[:4]).
GENERATOR = (
    (1, 0, 0, 0),
    (0, 1, 0, 0),
    (0, 0, 1, 0),
    (0, 0, 0, 1),
    (64, 120, 54, 15),
    (231, 210, 87, 99),
)
CheckCancel = Callable[[], None]


def _integer(value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("integer outside artifact bounds")


def _sha(data: bytes | bytearray) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _cancel(check: CheckCancel | None) -> None:
    if check is not None:
        check()


def _multiply(a: int, b: int) -> int:
    result = 0
    while b:
        if b & 1:
            result ^= a
        b >>= 1
        a <<= 1
        if a & 256:
            a ^= 0x11D
    return result


@lru_cache(maxsize=256)
def _table(coefficient: int) -> bytes:
    return bytes(_multiply(coefficient, value) for value in range(256))


def _inverse(rows: tuple[tuple[int, ...], ...]) -> tuple[tuple[int, ...], ...]:
    matrix = [list(row) + [int(i == j) for j in range(4)] for i, row in enumerate(rows)]
    for column in range(4):
        pivot = next((row for row in range(column, 4) if matrix[row][column]), None)
        if pivot is None:
            raise ValueError("singular fragment matrix")
        matrix[column], matrix[pivot] = matrix[pivot], matrix[column]
        scale = next(
            value
            for value in range(1, 256)
            if _multiply(matrix[column][column], value) == 1
        )
        matrix[column] = [_multiply(value, scale) for value in matrix[column]]
        for row in range(4):
            if row != column:
                scale = matrix[row][column]
                matrix[row] = [
                    left ^ _multiply(scale, right)
                    for left, right in zip(matrix[row], matrix[column], strict=True)
                ]
    return tuple(tuple(row[4:]) for row in matrix)


def _combine(
    coefficients: tuple[int, ...], parts: tuple[bytes, ...], check: CheckCancel | None
) -> bytes:
    output = bytearray()
    for offset in range(0, len(parts[0]), 32768):
        _cancel(check)
        width = min(32768, len(parts[0]) - offset)
        value = 0
        for coefficient, part in zip(coefficients, parts, strict=True):
            if coefficient:
                block = part[offset : offset + width].translate(_table(coefficient))
                value ^= int.from_bytes(block, "little")
        output.extend(value.to_bytes(width, "little"))
    return bytes(output)


@dataclass(frozen=True)
class ArtifactLimits:
    """Per-call ceilings; no implicit node-wide allocation or reservation."""

    fragment_bytes: int = MAX_FRAGMENT_BYTES
    max_source_bytes: int = MAX_ARTIFACT_BYTES
    max_encoded_bytes: int = 16 * 1024 * 1024
    max_fragments: int = MAX_FRAGMENTS

    def __post_init__(self) -> None:
        for value, maximum in (
            (self.fragment_bytes, MAX_FRAGMENT_BYTES),
            (self.max_source_bytes, MAX_ARTIFACT_BYTES),
            (self.max_encoded_bytes, 16 * 1024 * 1024),
            (self.max_fragments, MAX_FRAGMENTS),
        ):
            _integer(value, 1, maximum)


@dataclass(frozen=True)
class ArtifactManifest:
    """Canonical identity; ordered positions bind stripe/index, never locations."""

    project: str
    kind: str
    data_class: str
    expires_at: int
    source_size: int
    source_sha256: str
    fragment_bytes: int
    # Global index i means stripe i//6 and code row i%6. Equal hashes are legal.
    fragments: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        validate_project_id(self.project)
        validate_fingerprint(self.source_sha256)
        if self.kind not in ("dataset", "model") or self.data_class not in (
            "synthetic",
            "public",
        ):
            raise ValueError("unsupported artifact classification")
        _integer(self.expires_at, 1, 4102444800)
        _integer(self.source_size, 1, MAX_ARTIFACT_BYTES)
        _integer(self.fragment_bytes, 1, MAX_FRAGMENT_BYTES)
        count = 6 * (
            (self.source_size + 4 * self.fragment_bytes - 1)
            // (4 * self.fragment_bytes)
        )
        if (
            not isinstance(self.fragments, tuple)
            or len(self.fragments) != count
            or count > MAX_FRAGMENTS
        ):
            raise ValueError("invalid fragment count")
        for entry in self.fragments:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ValueError("invalid fragment references")
            for digest in entry:
                validate_fingerprint(digest)

    def fragment_size(self, index: int) -> int:
        _integer(index, 0, len(self.fragments) - 1)
        length = min(
            4 * self.fragment_bytes,
            self.source_size - (index // 6) * 4 * self.fragment_bytes,
        )
        return (length + 3) // 4

    def encode(self) -> bytes:
        return _json(
            {
                "version": 1,
                "codec": CODEC,
                "project": self.project,
                "kind": self.kind,
                "data_class": self.data_class,
                "expires_at": self.expires_at,
                "source_size": self.source_size,
                "source_sha256": self.source_sha256,
                "fragment_bytes": self.fragment_bytes,
                "fragments": self.fragments,
            }
        )

    @property
    def digest(self) -> str:
        return _sha(self.encode())

    @classmethod
    def decode(
        cls, raw: bytes, *, project: str, expected_digest: str
    ) -> ArtifactManifest:
        validate_fingerprint(expected_digest)
        if type(raw) is not bytes or len(raw) > 32768 or _sha(raw) != expected_digest:
            raise ValueError("artifact manifest size or digest mismatch")
        try:
            value = json.loads(raw)
            if not isinstance(value, dict) or set(value) != {
                "version",
                "codec",
                "project",
                "kind",
                "data_class",
                "expires_at",
                "source_size",
                "source_sha256",
                "fragment_bytes",
                "fragments",
            }:
                raise ValueError("artifact manifest fields")
            if (
                type(value["version"]) is not int
                or value["version"] != 1
                or value["codec"] != CODEC
                or value["project"] != project
            ):
                raise ValueError("artifact manifest context or version")
            entries = value["fragments"]
            if (
                not isinstance(entries, list)
                or len(entries) > MAX_FRAGMENTS
                or any(
                    not isinstance(entry, list) or len(entry) != 2 for entry in entries
                )
            ):
                raise ValueError("artifact manifest fragment bounds")
            manifest = cls(
                project,
                value["kind"],
                value["data_class"],
                value["expires_at"],
                value["source_size"],
                value["source_sha256"],
                value["fragment_bytes"],
                tuple((entry[0], entry[1]) for entry in entries),
            )
            if manifest.encode() != raw:
                raise ValueError("noncanonical artifact manifest")
            return manifest
        except (TypeError, KeyError, UnicodeError, RecursionError) as exc:
            raise ValueError("invalid artifact manifest") from exc

    def transfer_manifest(self, index: int, data: bytes) -> TransferManifest:
        """Verify a selected fragment and bind its unchanged transfer manifest."""
        if (
            type(data) is not bytes
            or len(data) != self.fragment_size(index)
            or _sha(data) != self.fragments[index][0]
        ):
            raise ValueError("fragment integrity mismatch")
        transfer = TransferStore(self.project, data, max_served_bytes=0).manifest
        if transfer.digest != self.fragments[index][1]:
            raise ValueError("fragment transfer binding mismatch")
        return transfer


@dataclass(frozen=True)
class ArtifactPackage:
    manifest: ArtifactManifest
    fragments: tuple[bytes, ...]


def package_artifact(
    data: bytes,
    *,
    project: str,
    kind: str,
    data_class: str,
    expires_at: int,
    limits: ArtifactLimits,
    check_cancel: CheckCancel | None = None,
) -> ArtifactPackage:
    """Package only caller-selected opaque bytes; no file walking or encryption."""
    if (
        not isinstance(limits, ArtifactLimits)
        or type(data) is not bytes
        or not 1 <= len(data) <= limits.max_source_bytes
    ):
        raise ValueError("artifact source limit")
    validate_project_id(project)
    if kind not in ("dataset", "model") or data_class not in ("synthetic", "public"):
        raise ValueError("unsupported artifact classification")
    _integer(expires_at, 1, 4102444800)
    stripes = (len(data) + 4 * limits.fragment_bytes - 1) // (4 * limits.fragment_bytes)
    if 6 * stripes > limits.max_fragments:
        raise ValueError("fragment count budget")
    sizes = tuple(
        (
            min(
                4 * limits.fragment_bytes,
                len(data) - stripe * 4 * limits.fragment_bytes,
            )
            + 3
        )
        // 4
        for stripe in range(stripes)
    )
    if sum(sizes) * 6 > limits.max_encoded_bytes:
        raise ValueError("encoded artifact budget")
    fragments: list[bytes] = []
    for stripe, size in enumerate(sizes):
        _cancel(check_cancel)
        start = stripe * 4 * limits.fragment_bytes
        parts = tuple(
            data[start + i * size : start + (i + 1) * size].ljust(size, b"\x00")
            for i in range(4)
        )
        fragments.extend(parts)
        fragments.extend(_combine(row, parts, check_cancel) for row in GENERATOR[4:])
    refs = tuple(
        (_sha(part), TransferStore(project, part, max_served_bytes=0).manifest.digest)
        for part in fragments
    )
    manifest = ArtifactManifest(
        project,
        kind,
        data_class,
        expires_at,
        len(data),
        _sha(data),
        limits.fragment_bytes,
        refs,
    )
    _cancel(check_cancel)
    return ArtifactPackage(manifest, tuple(fragments))


def _trusted(manifest: ArtifactManifest, expected_digest: str) -> None:
    validate_fingerprint(expected_digest)
    if manifest.digest != expected_digest:
        raise ValueError("caller-selected manifest digest mismatch")


def _verified(
    manifest: ArtifactManifest, fragments: tuple[tuple[int, bytes], ...]
) -> dict[int, bytes]:
    if not isinstance(fragments, tuple) or len(fragments) > len(manifest.fragments):
        raise ValueError("fragment collection bound")
    result: dict[int, bytes] = {}
    for entry in fragments:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise ValueError("invalid fragment entry")
        index, data = entry
        manifest.transfer_manifest(index, data)
        if index in result:
            raise ValueError("duplicate fragment index")
        result[index] = data
    return result


def reconstruct_artifact(
    manifest: ArtifactManifest,
    fragments: tuple[tuple[int, bytes], ...],
    *,
    expected_digest: str,
    check_cancel: CheckCancel | None = None,
) -> bytes:
    """Verify before decode, then verify the complete bounded artifact digest."""
    _trusted(manifest, expected_digest)
    available = _verified(manifest, fragments)
    output = bytearray()
    for start in range(0, len(manifest.fragments), 6):
        selected = tuple(
            index for index in range(start, start + 6) if index in available
        )[:4]
        if len(selected) != 4:
            raise ValueError("insufficient verified fragments in stripe")
        inverse = _inverse(tuple(GENERATOR[index % 6] for index in selected))
        parts = tuple(available[index] for index in selected)
        decoded_parts = tuple(_combine(row, parts, check_cancel) for row in inverse)
        for index, decoded in enumerate(decoded_parts):
            manifest.transfer_manifest(start + index, decoded)
            take = min(len(decoded), manifest.source_size - len(output))
            if any(decoded[take:]):
                raise ValueError("nonzero final padding")
            output.extend(decoded[:take])
        # Validate the complete codeword, including parity that was not supplied.
        for index in (4, 5):
            manifest.transfer_manifest(
                start + index, _combine(GENERATOR[index], decoded_parts, check_cancel)
            )
    if len(output) != manifest.source_size or _sha(output) != manifest.source_sha256:
        raise ValueError("whole artifact integrity mismatch")
    _cancel(check_cancel)
    return bytes(output)


@dataclass(frozen=True)
class ArtifactPublication:
    """Integrity-only result; publisher permission requires separate policy."""

    manifest_digest: str
    output: Path
    integrity_verified: Literal[True] = True
    publisher_authorized: Literal[False] = False


def publish_artifact(
    manifest: ArtifactManifest,
    fragments: tuple[tuple[int, bytes], ...],
    output: Path,
    *,
    expected_digest: str,
    check_cancel: CheckCancel | None = None,
) -> ArtifactPublication:
    """Atomic no-overwrite local publication, after complete integrity checking.

    Hash integrity grants no publisher authority. An error after the atomic link
    may leave a committed destination; inspect it rather than retry/overwrite.
    """
    with selected_parent(output) as (directory, name):
        require_absent(directory, name)
        data = reconstruct_artifact(
            manifest,
            fragments,
            expected_digest=expected_digest,
            check_cancel=check_cancel,
        )
        _cancel(check_cancel)
        publish_new(directory, name, data, limit=MAX_ARTIFACT_BYTES)
    return ArtifactPublication(manifest.digest, output)


@dataclass(frozen=True)
class RepairLimits:
    max_read_bytes: int = 16 * 1024 * 1024
    max_write_bytes: int = 10_000_000
    max_fragments: int = 2

    def __post_init__(self) -> None:
        for value, maximum in (
            (self.max_read_bytes, 16 * 1024 * 1024),
            (self.max_write_bytes, 16 * 1024 * 1024),
            (self.max_fragments, MAX_FRAGMENTS),
        ):
            _integer(value, 1, maximum)


def repair_artifact_fragments(
    manifest: ArtifactManifest,
    fragments: tuple[tuple[int, bytes], ...],
    targets: tuple[int, ...],
    *,
    expected_digest: str,
    limits: RepairLimits,
    now: int,
    deleted: bool = False,
    check_cancel: CheckCancel | None = None,
) -> tuple[tuple[int, bytes], ...]:
    """One bounded local repair calculation, not a network queue or repair lease."""
    _trusted(manifest, expected_digest)
    _integer(now, 1, 4102444800)
    if type(deleted) is not bool or deleted or now >= manifest.expires_at:
        raise ValueError("artifact expired or deleted")
    if (
        not isinstance(limits, RepairLimits)
        or not isinstance(targets, tuple)
        or not 1 <= len(targets) <= limits.max_fragments
    ):
        raise ValueError("repair target limit")
    sizes = [manifest.fragment_size(index) for index in targets]
    if len(set(targets)) != len(targets) or sum(sizes) > limits.max_write_bytes:
        raise ValueError("repair output budget or duplicate target")
    available = _verified(manifest, fragments)
    if (
        any(index in available for index in targets)
        or sum(map(len, available.values())) > limits.max_read_bytes
    ):
        raise ValueError("repair input budget or existing target")
    data = reconstruct_artifact(
        manifest, fragments, expected_digest=expected_digest, check_cancel=check_cancel
    )
    package = package_artifact(
        data,
        project=manifest.project,
        kind=manifest.kind,
        data_class=manifest.data_class,
        expires_at=manifest.expires_at,
        limits=ArtifactLimits(fragment_bytes=manifest.fragment_bytes),
        check_cancel=check_cancel,
    )
    if package.manifest != manifest:
        raise ValueError("inconsistent erasure manifest")
    return tuple((index, package.fragments[index]) for index in targets)
