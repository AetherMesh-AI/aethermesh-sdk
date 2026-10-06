"""Small JSON helpers shared by local lifecycle commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any, TypeVar

_LocalError = TypeVar("_LocalError", bound=ValueError)


def load_json_mapping(
    path: Path, label: str, error_type: type[_LocalError]
) -> dict[str, Any]:
    """Read a required JSON object and raise the caller's local error type."""

    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise error_type(f"required {label} file is missing") from exc
    except OSError as exc:
        raise error_type(f"could not read {label} file: {exc}") from exc
    try:
        document = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise error_type(f"{label} JSON is malformed: {exc.msg}") from exc
    if isinstance(document, dict):
        return document
    raise error_type(f"{label} JSON must be an object")


def require_text_field(
    document: dict[str, Any], field_name: str, label: str, error_type: type[_LocalError]
) -> str:
    """Return a non-empty string field or raise the caller's local error type."""

    value = document.get(field_name)
    if isinstance(value, str) and value:
        return value
    raise error_type(f"{label} field {field_name!r} must be a string")


def append_json_line(
    path: Path, payload: Mapping[str, object], *, create_parent: bool = False
) -> None:
    """Append one deterministic JSONL entry to a local lifecycle log."""

    if create_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(dict(payload), sort_keys=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{serialized}\n")


def canonical_json_hash(document: dict[str, Any], *, prefix: str = "") -> str:
    """Hash a JSON object in stable key order for local receipts."""

    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return f"{prefix}{sha256(encoded).hexdigest()}"
