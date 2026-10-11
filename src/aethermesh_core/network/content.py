"""Opt-in small opaque content exchange; integrity is not publisher authority."""

from __future__ import annotations

import base64
import binascii
import hashlib
from dataclasses import dataclass
from typing import Any

from .config import validate_fingerprint, validate_project_id
from .errors import ProtocolError, RemoteError

CONTENT_OPERATION = "content.get.v1"
MAX_CONTENT_BYTES = 32768
MAX_STORE_BYTES = 1048576
MAX_STORE_ITEMS = 64


@dataclass(frozen=True)
class ContentRef:
    """Caller-selected project, SHA-256 and length, never a filesystem path.

    Obtain this reference through a separately trusted channel or evaluate its
    provenance in the application. Matching bytes alone authorize nothing.
    """

    project_id: str
    sha256: str
    size: int

    def __post_init__(self) -> None:
        validate_project_id(self.project_id)
        validate_fingerprint(self.sha256)
        if type(self.size) is not int or not 0 <= self.size <= MAX_CONTENT_BYTES:
            raise ValueError("content size must be between 0 and 32768 bytes")

    def to_dict(self) -> dict[str, Any]:
        return {"project": self.project_id, "sha256": self.sha256, "size": self.size}


def _reference(value: dict[str, Any]) -> ContentRef:
    if set(value) != {"project", "sha256", "size"}:
        raise ValueError("invalid content reference fields")
    return ContentRef(value["project"], value["sha256"], value["size"])


def validate_content_result(result: dict[str, Any], request: dict[str, Any]) -> bytes:
    """Reject corrupt, oversized, noncanonical or substituted peer content."""
    try:
        ref = _reference(request)
        if set(result) != {"project", "sha256", "size", "data"}:
            raise ValueError("invalid content result fields")
        if _reference({key: result[key] for key in request}) != ref:
            raise ValueError("content reference mismatch")
        encoded = result["data"]
        if not isinstance(encoded, str) or len(encoded) > 4 * ((ref.size + 2) // 3):
            raise ValueError("invalid content encoding size")
        data = base64.b64decode(encoded, validate=True)
        if (
            len(data) != ref.size
            or base64.b64encode(data).decode("ascii") != encoded
            or hashlib.sha256(data).hexdigest() != ref.sha256
        ):
            raise ValueError("content integrity mismatch")
        return data
    except (ValueError, TypeError, binascii.Error) as exc:
        raise ProtocolError("The peer returned invalid content.") from exc


class ContentStore:
    """Explicit immutable byte snapshot for one project; no disk or execution.

    Every peer admitted to the owning service can request these bytes by hash.
    Construction opts into sharing only this snapshot, not a directory. A
    lifetime byte budget is consumed before each response, even if cancelled;
    restarting a service with the same store does not reset the budget.
    """

    def __init__(
        self,
        project_id: str,
        blobs: tuple[bytes, ...],
        *,
        max_served_bytes: int = 8 * MAX_STORE_BYTES,
    ) -> None:
        validate_project_id(project_id)
        if not isinstance(blobs, tuple) or len(blobs) > MAX_STORE_ITEMS:
            raise ValueError("content requires a tuple of at most 64 byte strings")
        if (
            type(max_served_bytes) is not int
            or not 0 <= max_served_bytes <= 64 * MAX_STORE_BYTES
        ):
            raise ValueError("invalid content serving budget")
        total = 0
        self._blobs: dict[str, bytes] = {}
        for data in blobs:
            if not isinstance(data, bytes) or len(data) > MAX_CONTENT_BYTES:
                raise ValueError("each content blob must be at most 32768 bytes")
            total += len(data)
            if total > MAX_STORE_BYTES:
                raise ValueError("content snapshot exceeds 1 MiB")
            self._blobs[hashlib.sha256(data).hexdigest()] = data
        self._project_id = project_id
        self._remaining = max_served_bytes

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def references(self) -> tuple[ContentRef, ...]:
        """Local inventory; no peer listing operation or automatic discovery."""
        return tuple(
            ContentRef(self.project_id, digest, len(data))
            for digest, data in self._blobs.items()
        )

    def get(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Bounded synchronous lookup; called inside the service request budget."""
        try:
            ref = _reference(payload)
        except (ValueError, TypeError) as exc:
            raise RemoteError("invalid_request", "invalid content reference") from exc
        if ref.project_id != self.project_id:
            raise RemoteError("invalid_request", "content project mismatch")
        data = self._blobs.get(ref.sha256)
        if data is None or len(data) != ref.size:
            raise RemoteError("not_found", "content unavailable")
        if len(data) > self._remaining:
            raise RemoteError("resource_limit", "content serving budget exhausted")
        self._remaining -= len(data)
        return {**ref.to_dict(), "data": base64.b64encode(data).decode("ascii")}
