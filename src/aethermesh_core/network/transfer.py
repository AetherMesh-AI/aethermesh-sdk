"""Bounded immutable transfer manifests; byte integrity grants no authority."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .config import validate_fingerprint, validate_project_id
from .content import MAX_CONTENT_BYTES, ContentRef, validate_content_result
from .errors import ProtocolError, RemoteError

MANIFEST_OPERATION = "content.manifest.v1"
CHUNK_OPERATION = "content.chunk.v1"
TRANSFER_OPERATIONS = frozenset({MANIFEST_OPERATION, CHUNK_OPERATION})
MAX_TRANSFER_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class TransferManifest:
    """Canonical version-1 manifest, bound to a caller-selected SHA-256 digest."""

    project_id: str
    sha256: str
    size: int
    chunks: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_project_id(self.project_id)
        validate_fingerprint(self.sha256)
        if type(self.size) is not int or not 0 <= self.size <= MAX_TRANSFER_BYTES:
            raise ValueError("invalid transfer size")
        if (
            not isinstance(self.chunks, tuple)
            or len(self.chunks)
            != (self.size + MAX_CONTENT_BYTES - 1) // MAX_CONTENT_BYTES
        ):
            raise ValueError("invalid chunk count")
        for digest in self.chunks:
            validate_fingerprint(digest)

    def encode(self) -> bytes:
        """UTF-8, sorted ASCII JSON keys, compact separators, no final newline."""
        return json.dumps(
            {
                "version": 1,
                "project": self.project_id,
                "sha256": self.sha256,
                "size": self.size,
                "chunk_size": MAX_CONTENT_BYTES,
                "chunks": list(self.chunks),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.encode()).hexdigest()

    def chunk(self, index: int) -> ContentRef:
        if type(index) is not int or not 0 <= index < len(self.chunks):
            raise ValueError("invalid chunk index")
        return ContentRef(
            self.project_id,
            self.chunks[index],
            min(MAX_CONTENT_BYTES, self.size - index * MAX_CONTENT_BYTES),
        )

    @classmethod
    def decode(cls, data: bytes, project: str, digest: str) -> TransferManifest:
        """Reject alternate encodings, duplicate keys, substitutions and oversize."""
        validate_project_id(project)
        validate_fingerprint(digest)
        if len(data) > MAX_CONTENT_BYTES or hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("invalid manifest digest or size")
        value = json.loads(data)
        if (
            not isinstance(value, dict)
            or set(value)
            != {"version", "project", "sha256", "size", "chunk_size", "chunks"}
            or type(value["version"]) is not int
            or value["version"] != 1
            or (
                type(value["chunk_size"]) is not int
                or value["chunk_size"] != MAX_CONTENT_BYTES
                or value["project"] != project
                or not isinstance(value["chunks"], list)
            )
        ):
            raise ValueError("invalid manifest schema")
        manifest = cls(project, value["sha256"], value["size"], tuple(value["chunks"]))
        if manifest.encode() != data:
            raise ValueError("noncanonical manifest")
        return manifest


def validate_transfer_result(
    operation: str, result: dict[str, Any], request: dict[str, Any]
) -> bytes:
    """Validate results even when using the raw request API."""
    try:
        if operation == MANIFEST_OPERATION:
            if set(request) != {"project", "sha256"}:
                raise ValueError("invalid manifest request")
            ref = ContentRef(request["project"], request["sha256"], result["size"])
        else:
            if set(request) != {"project", "manifest", "index", "sha256", "size"}:
                raise ValueError("invalid chunk request")
            validate_fingerprint(request["manifest"])
            if type(request["index"]) is not int or not 0 <= request["index"] < 256:
                raise ValueError("invalid chunk index")
            ref = ContentRef(request["project"], request["sha256"], request["size"])
        data = validate_content_result(result, ref.to_dict())
        if operation == MANIFEST_OPERATION:
            TransferManifest.decode(data, ref.project_id, ref.sha256)
        return data
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as exc:
        raise ProtocolError("The peer returned invalid transfer content.") from exc


class TransferStore:
    """One opt-in, at-most-8-MiB snapshot with one shared lifetime byte budget."""

    def __init__(self, project_id: str, data: bytes, *, max_served_bytes: int) -> None:
        validate_project_id(project_id)
        if not isinstance(data, bytes) or len(data) > MAX_TRANSFER_BYTES:
            raise ValueError("transfer requires at most 8 MiB of bytes")
        if (
            type(max_served_bytes) is not int
            or not 0 <= max_served_bytes <= 64 * 1024 * 1024
        ):
            raise ValueError("invalid transfer budget")
        self._manifest = TransferManifest(
            project_id,
            hashlib.sha256(data).hexdigest(),
            len(data),
            tuple(
                hashlib.sha256(data[i : i + MAX_CONTENT_BYTES]).hexdigest()
                for i in range(0, len(data), MAX_CONTENT_BYTES)
            ),
        )
        self._data = data
        self._remaining = max_served_bytes

    @property
    def manifest(self) -> TransferManifest:
        """Immutable snapshot metadata; no caller can replace it after construction."""
        return self._manifest

    def get(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            if operation not in TRANSFER_OPERATIONS:
                raise ValueError("invalid transfer operation")
            if operation == MANIFEST_OPERATION:
                if payload != {
                    "project": self.manifest.project_id,
                    "sha256": self.manifest.digest,
                }:
                    raise ValueError("invalid manifest reference")
                data = self.manifest.encode()
            else:
                ref = self.manifest.chunk(payload["index"])
                if payload != {
                    **ref.to_dict(),
                    "manifest": self.manifest.digest,
                    "index": payload["index"],
                }:
                    raise ValueError("invalid chunk reference")
                offset = payload["index"] * MAX_CONTENT_BYTES
                data = self._data[offset : offset + ref.size]
        except (ValueError, TypeError, KeyError) as exc:
            raise RemoteError("invalid_request", "invalid transfer reference") from exc
        if len(data) > self._remaining:
            raise RemoteError("resource_limit", "transfer serving budget exhausted")
        self._remaining -= len(data)
        return {
            "project": self.manifest.project_id,
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "data": base64.b64encode(data).decode("ascii"),
        }
