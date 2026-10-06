"""Validation and deterministic serialization for Phase 1 local job envelopes."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from aethermesh_core.scheduler import SUPPORTED_LOCAL_JOB_TYPES

JOB_ENVELOPE_SCHEMA_VERSION = 1
_REQUIRED_FIELDS = frozenset(
    {
        "job_id",
        "schema_version",
        "creator_node_id",
        "created_at",
        "job_type",
        "input_manifest",
        "expected_outputs",
        "validation_requirements",
        "lineage",
        "contribution",
    }
)
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_LOCAL_JOB_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,127}\Z")


class JobEnvelopeError(ValueError):
    """Raised when a Phase 1 local job envelope violates its schema."""


def canonical_job_envelope_json(document: object) -> str:
    """Return deterministic UTF-8 JSON text suitable for local fixtures and receipts."""
    validate_job_envelope(document)
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def validate_job_envelope(document: object) -> dict[str, Any]:
    """Validate one local-only Phase 1 envelope without reading or writing files."""
    envelope = _object(document, "job envelope", _REQUIRED_FIELDS)
    _job_id(envelope["job_id"])
    _integer(envelope, "schema_version", JOB_ENVELOPE_SCHEMA_VERSION, "job envelope")
    _text(envelope, "creator_node_id", "job envelope")
    _timestamp(envelope, "created_at")
    _job_type(envelope["job_type"])
    _input_manifest(envelope["input_manifest"])
    output_paths = _expected_outputs(envelope["expected_outputs"])
    _validation_requirements(envelope["validation_requirements"])
    _lineage(envelope["lineage"])
    _contribution(envelope["contribution"], envelope["creator_node_id"], output_paths)
    return envelope


def _object(
    value: object,
    context: str,
    required: frozenset[str],
    allowed: frozenset[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JobEnvelopeError(f"{context} must be an object")
    fields = set(value)
    missing = sorted(required - fields)
    unknown = sorted(fields - (allowed or required))
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unknown:
            details.append(f"unsupported: {', '.join(unknown)}")
        raise JobEnvelopeError(f"{context} fields invalid ({'; '.join(details)})")
    return value


def _text(document: dict[str, Any], field: str, context: str) -> str:
    value = document[field]
    if not isinstance(value, str) or not value.strip():
        raise JobEnvelopeError(f"{context}.{field} must be a non-empty string")
    return value


def _job_id(value: object) -> str:
    """Require a local stable ID or a deterministic content-addressed ID."""
    if not isinstance(value, str) or not (
        _LOCAL_JOB_ID.fullmatch(value) or _SHA256.fullmatch(value)
    ):
        raise JobEnvelopeError(
            "job envelope.job_id must be a local ID or sha256 content ID"
        )
    return value


def _job_type(value: object) -> str:
    job_type = _non_empty_text(value, "job envelope.job_type")
    if job_type not in SUPPORTED_LOCAL_JOB_TYPES:
        supported_types = ", ".join(SUPPORTED_LOCAL_JOB_TYPES)
        raise JobEnvelopeError(
            f"job envelope.job_type must be one of: {supported_types}"
        )
    return job_type


def _integer(document: dict[str, Any], field: str, expected: int, context: str) -> None:
    value = document[field]
    if not isinstance(value, int) or isinstance(value, bool) or value != expected:
        raise JobEnvelopeError(f"{context}.{field} must be integer {expected}")


def _timestamp(document: dict[str, Any], field: str) -> None:
    value = _text(document, field, "job envelope")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise JobEnvelopeError(f"job envelope.{field} must be a UTC timestamp") from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise JobEnvelopeError(f"job envelope.{field} must be a UTC timestamp")


def _input_manifest(value: object) -> None:
    manifest = _object(value, "input_manifest", frozenset({"files"}))
    files = manifest["files"]
    if not isinstance(files, list) or not files:
        raise JobEnvelopeError("input_manifest.files must be a non-empty list")
    for index, file_entry in enumerate(files):
        entry = _object(
            file_entry,
            f"input_manifest.files[{index}]",
            frozenset({"path", "sha256", "size_bytes"}),
            frozenset({"path", "sha256", "size_bytes", "metadata"}),
        )
        _relative_path(_text(entry, "path", f"input_manifest.files[{index}]"))
        digest = _text(entry, "sha256", f"input_manifest.files[{index}]")
        if not _SHA256.fullmatch(digest):
            raise JobEnvelopeError(
                f"input_manifest.files[{index}].sha256 must be a sha256 digest"
            )
        size = entry["size_bytes"]
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise JobEnvelopeError(
                f"input_manifest.files[{index}].size_bytes must be a non-negative integer"
            )
        if "metadata" in entry and not isinstance(entry["metadata"], dict):
            raise JobEnvelopeError(
                f"input_manifest.files[{index}].metadata must be an object"
            )


def _expected_outputs(value: object) -> set[str]:
    outputs = _object(value, "expected_outputs", frozenset({"artifacts"}))
    artifacts = outputs["artifacts"]
    if not isinstance(artifacts, list) or not artifacts:
        raise JobEnvelopeError("expected_outputs.artifacts must be a non-empty list")
    paths: set[str] = set()
    for index, artifact in enumerate(artifacts):
        entry = _object(
            artifact,
            f"expected_outputs.artifacts[{index}]",
            frozenset({"path", "media_type"}),
        )
        path = _text(entry, "path", f"expected_outputs.artifacts[{index}]")
        _relative_path(path)
        _text(entry, "media_type", f"expected_outputs.artifacts[{index}]")
        paths.add(path)
    return paths


def _validation_requirements(value: object) -> None:
    requirements = _object(value, "validation_requirements", frozenset({"checks"}))
    checks = requirements["checks"]
    if not isinstance(checks, list) or not checks:
        raise JobEnvelopeError(
            "validation_requirements.checks must be a non-empty list"
        )
    for index, check in enumerate(checks):
        entry = _object(
            check,
            f"validation_requirements.checks[{index}]",
            frozenset({"check_id", "receipt_path", "pass_criteria"}),
        )
        _text(entry, "check_id", f"validation_requirements.checks[{index}]")
        _relative_path(
            _text(entry, "receipt_path", f"validation_requirements.checks[{index}]")
        )
        if not isinstance(entry["pass_criteria"], dict) or not entry["pass_criteria"]:
            raise JobEnvelopeError(
                f"validation_requirements.checks[{index}].pass_criteria must be a non-empty object"
            )


def _lineage(value: object) -> None:
    lineage = _object(
        value,
        "lineage",
        frozenset({"parent_job_ids", "source_manifests", "prior_validation_receipts"}),
    )
    for field in ("parent_job_ids", "source_manifests", "prior_validation_receipts"):
        references = lineage[field]
        if not isinstance(references, list):
            raise JobEnvelopeError(f"lineage.{field} must be a list")
        for reference in references:
            _relative_path(reference) if field != "parent_job_ids" else _non_empty_text(
                reference, f"lineage.{field}"
            )


def _contribution(value: object, creator_node_id: str, output_paths: set[str]) -> None:
    contribution = _object(
        value,
        "contribution",
        frozenset({"creator_node_id", "executor_node_id", "produced_artifacts"}),
    )
    if _text(contribution, "creator_node_id", "contribution") != creator_node_id:
        raise JobEnvelopeError(
            "contribution.creator_node_id must match job envelope.creator_node_id"
        )
    executor = contribution["executor_node_id"]
    if executor is not None and (not isinstance(executor, str) or not executor.strip()):
        raise JobEnvelopeError(
            "contribution.executor_node_id must be a non-empty string or null"
        )
    artifacts = contribution["produced_artifacts"]
    if not isinstance(artifacts, list):
        raise JobEnvelopeError("contribution.produced_artifacts must be a list")
    for artifact in artifacts:
        _relative_path(artifact)
        if artifact not in output_paths:
            raise JobEnvelopeError(
                "contribution.produced_artifacts must be declared in expected_outputs"
            )


def _non_empty_text(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise JobEnvelopeError(f"{context} entries must be non-empty strings")
    return value


def _relative_path(value: object) -> str:
    path = _non_empty_text(value, "local path")
    if (
        path.startswith("/")
        or "\\" in path
        or re.match(r"[A-Za-z]:", path)
        or "://" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise JobEnvelopeError("local paths must be safe relative paths")
    return path
