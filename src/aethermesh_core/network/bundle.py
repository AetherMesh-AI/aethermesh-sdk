"""Bounded bundle-v2 header claims; never invoke Git or decompress peer objects."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from .config import validate_fingerprint, validate_project_id
from .transfer import MAX_TRANSFER_BYTES

MAX_HEADER_BYTES = 16384


def validate_branch(ref: str) -> None:
    """Conservative full branch names only; no revision expressions or aliases."""
    if not isinstance(ref, str) or not re.fullmatch(
        r"refs/heads/[A-Za-z0-9_/-][A-Za-z0-9._/-]{0,119}", ref
    ):
        raise ValueError("a bounded full branch ref is required")
    if (
        ref.endswith(".")
        or ".." in ref
        or any(
            not part or part.startswith(".") or part.endswith(".lock")
            for part in ref.split("/")
        )
    ):
        raise ValueError("invalid branch ref")


def validate_oid(oid: str) -> None:
    if (
        not isinstance(oid, str)
        or not re.fullmatch(r"[0-9a-f]{40}", oid)
        or oid == "0" * 40
    ):
        raise ValueError("a full lowercase nonzero SHA-1 commit OID is required")


@dataclass(frozen=True)
class BundleInfo:
    """Header claims and selected-byte digest, not validated Git object contents."""

    project_id: str
    sha256: str
    size: int
    ref: str
    claimed_oid: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "project": self.project_id,
            "sha256": self.sha256,
            "size": self.size,
            "ref": self.ref,
            "claimed_oid": self.claimed_oid,
            "bundle_version": 2,
            "object_format": "sha1",
            "pack_validated": False,
            "publisher_authorized": False,
        }


def inspect_bundle(data: bytes, *, project_id: str, expected_sha256: str) -> BundleInfo:
    """Parse only a bounded v2 header; opaque PACK bytes remain unvalidated."""
    validate_project_id(project_id)
    validate_fingerprint(expected_sha256)
    if not isinstance(data, bytes) or len(data) > MAX_TRANSFER_BYTES:
        raise ValueError("bundle exceeds the 8-MiB bound")
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ValueError("bundle digest mismatch")
    end = data.find(b"\n\n", 0, MAX_HEADER_BYTES)
    if end < 0 or data[end + 2 : end + 6] != b"PACK":
        raise ValueError("invalid or oversized bundle header")
    lines = data[:end].split(b"\n")
    if len(lines) != 2 or lines[0] != b"# v2 git bundle":
        raise ValueError("only one-ref self-contained bundle v2 is supported")
    try:
        oid, ref = lines[1].decode("ascii").split(" ")
    except (ValueError, UnicodeError) as exc:
        raise ValueError("invalid bundle reference line") from exc
    validate_oid(oid)
    validate_branch(ref)
    return BundleInfo(project_id, expected_sha256, len(data), ref, oid)
