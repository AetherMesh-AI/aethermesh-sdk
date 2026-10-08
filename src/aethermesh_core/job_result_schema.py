"""Validation for local, auditable Phase 1 result report records."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

JOB_RESULT_SCHEMA_VERSION = 10
MAX_INLINE_OUTPUT_PAYLOAD_BYTES = 4 * 1024
RESULT_STATUSES = frozenset(
    {
        "succeeded",
        "failed",
        "timed_out",
        "cancelled",
        "validation_failed",
        "partially_completed",
    }
)
VALIDATION_STATUSES = frozenset({"pending", "passed", "failed", "error", "not_run"})
_FINAL_VALIDATION_STATUSES = frozenset({"passed", "failed", "error"})
_FAILURE_FIELDS = frozenset(
    {"execution", "validation", "malformed_input", "missing_artifact"}
)
_LINEAGE_FIELDS = frozenset(
    {
        "input_manifest_ids",
        "output_manifest_ids",
        "parent_job_ids",
        "parent_task_ids",
        "artifact_ids",
    }
)
_REFERENCE_FIELDS = frozenset(
    {
        "manifest_hash",
        "manifest_ref",
        "artifact_refs",
        "validation_receipt_ids",
        "log_refs",
    }
)
_REQUIRED_REFERENCE_FIELDS = _REFERENCE_FIELDS - {"manifest_ref"}
_OUTPUT_PAYLOAD_FIELDS = frozenset({"inline_payload", "payload_ref", "payload_digest"})
_CONTRIBUTION_FIELDS = frozenset(
    {
        "creator_node_id",
        "executor_node_id",
        "validator_node_id",
        "upstream_lineage_sources",
        "local_operator_id",
    }
)
_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "result_id",
        "job_id",
        "task_id",
        "capability",
        "model_expert_id",
        "expert_version",
        "creator_node_id",
        "executor_node_id",
        "manifest_id",
        "output_payload",
        "references",
        "created_at",
        "status",
        "exit_code",
        "started_at",
        "finished_at",
        "reported_at",
        "duration_ms",
        "summary",
        "error_summary",
        "validation_status",
        "validation_receipt_id",
        "validator_node_id",
        "failure_reasons",
        "lineage",
        "contribution",
        "result_hash",
    }
)


class JobResultSchemaError(ValueError):
    """Raised when a local result report violates its stable schema."""


def validate_job_result_document(
    document: object, *, verify_result_hash: bool = True
) -> dict[str, Any]:
    """Validate one local-only result report without reading or writing files."""

    required_fields = (
        _REQUIRED_FIELDS if verify_result_hash else _REQUIRED_FIELDS - {"result_hash"}
    )
    result = _object(
        document,
        "job result",
        required_fields,
        allowed=_REQUIRED_FIELDS,
    )
    _exact_integer(
        result["schema_version"],
        "job result.schema_version",
        JOB_RESULT_SCHEMA_VERSION,
    )
    for field in (
        "result_id",
        "job_id",
        "task_id",
        "capability",
        "model_expert_id",
        "expert_version",
        "creator_node_id",
        "executor_node_id",
        "manifest_id",
        "validation_receipt_id",
        "validator_node_id",
    ):
        _identifier(result[field], f"job result.{field}")
    created_at = _timestamp(result["created_at"], "job result.created_at")
    started_at = _timestamp(result["started_at"], "job result.started_at")
    finished_at = _timestamp(result["finished_at"], "job result.finished_at")
    reported_at = _timestamp(result["reported_at"], "job result.reported_at")
    if created_at > started_at or finished_at < started_at or reported_at < finished_at:
        raise JobResultSchemaError(
            "job result timestamps must be chronologically ordered"
        )
    duration_ms = result["duration_ms"]
    if (
        not isinstance(duration_ms, int)
        or isinstance(duration_ms, bool)
        or duration_ms < 0
    ):
        raise JobResultSchemaError(
            "job result.duration_ms must be a non-negative integer"
        )
    if duration_ms != int((finished_at - started_at).total_seconds() * 1000):
        raise JobResultSchemaError(
            "job result.duration_ms must match started_at and finished_at"
        )
    if result["status"] not in RESULT_STATUSES:
        raise JobResultSchemaError("job result.status is unsupported")
    if not isinstance(result["exit_code"], int) or isinstance(
        result["exit_code"], bool
    ):
        raise JobResultSchemaError("job result.exit_code must be an integer")
    _summary(result["summary"], "job result.summary", nullable=False)
    _summary(result["error_summary"], "job result.error_summary", nullable=True)
    if result["status"] == "succeeded" and result["error_summary"] is not None:
        raise JobResultSchemaError(
            "job result.error_summary must be null when the job succeeded"
        )
    if result["status"] != "succeeded" and result["error_summary"] is None:
        raise JobResultSchemaError(
            "job result.error_summary is required when the job did not succeed"
        )
    if result["validation_status"] not in VALIDATION_STATUSES:
        raise JobResultSchemaError("job result.validation_status is unsupported")
    _output_payload(result["output_payload"], result["status"])
    _references(
        result["references"], result["manifest_id"], result["validation_receipt_id"]
    )
    _failure_reasons(result["failure_reasons"])
    _lineage(result["lineage"])
    _contribution(
        result["contribution"],
        result["creator_node_id"],
        result["executor_node_id"],
        result["validator_node_id"],
        result["lineage"],
    )
    validation_is_final = result["validation_status"] in _FINAL_VALIDATION_STATUSES
    result_hash = result.get("result_hash")
    if "result_hash" in result:
        if validation_is_final and result_hash is None:
            raise JobResultSchemaError(
                "job result.result_hash is required after final validation"
            )
        if not validation_is_final and result_hash is not None:
            raise JobResultSchemaError(
                "job result.result_hash must be null while validation is pending or not run"
            )
        if result_hash is not None:
            _content_hash(result_hash, "job result.result_hash")
    if verify_result_hash and validation_is_final:
        # Import lazily because result hashing first uses this validator to
        # establish the canonical payload shape.
        from aethermesh_core.result_hash import canonical_result_document_hash

        if result["result_hash"] != canonical_result_document_hash(result):
            raise JobResultSchemaError(
                "job result.result_hash does not match the canonical result payload"
            )
    return result


def _object(
    value: object,
    context: str,
    required: frozenset[str],
    *,
    allowed: frozenset[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JobResultSchemaError(f"{context} must be an object")
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - (allowed if allowed is not None else required))
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unknown:
            details.append(f"unsupported: {', '.join(unknown)}")
        raise JobResultSchemaError(f"{context} fields invalid ({'; '.join(details)})")
    return value


def _identifier(value: object, context: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(character.isspace() for character in value)
    ):
        raise JobResultSchemaError(f"{context} must be a non-empty identifier")
    return value


def _timestamp(value: object, context: str) -> datetime:
    if not isinstance(value, str):
        raise JobResultSchemaError(f"{context} must be a UTC timestamp")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise JobResultSchemaError(f"{context} must be a UTC timestamp") from exc


def _exact_integer(value: object, context: str, expected: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value != expected:
        raise JobResultSchemaError(f"{context} must be integer {expected}")


def _summary(value: object, context: str, *, nullable: bool) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str) or not value or len(value) > 512:
        suffix = " or null" if nullable else ""
        raise JobResultSchemaError(
            f"{context} must be a non-empty string up to 512 characters{suffix}"
        )


def _failure_reasons(value: object) -> None:
    reasons = _object(value, "job result.failure_reasons", _FAILURE_FIELDS)
    for field, reason in reasons.items():
        if reason is not None and (not isinstance(reason, str) or not reason):
            raise JobResultSchemaError(
                f"job result.failure_reasons.{field} must be a non-empty string or null"
            )


def _output_payload(value: object, status: object) -> None:
    payload = _object(value, "job result.output_payload", _OUTPUT_PAYLOAD_FIELDS)
    inline_payload = payload["inline_payload"]
    payload_ref = payload["payload_ref"]
    payload_digest = payload["payload_digest"]
    has_inline_payload = inline_payload is not None
    has_payload_ref = payload_ref is not None
    if not has_inline_payload and not has_payload_ref:
        if status == "succeeded":
            raise JobResultSchemaError(
                "successful job result must include an output payload"
            )
        if payload_digest is not None:
            raise JobResultSchemaError(
                "job result.output_payload.payload_digest requires a payload reference"
            )
        return
    if has_inline_payload and has_payload_ref:
        raise JobResultSchemaError(
            "job result.output_payload must contain exactly one of inline_payload or payload_ref"
        )
    if has_inline_payload:
        if payload_digest is not None:
            raise JobResultSchemaError(
                "job result.output_payload.payload_digest must be null for inline payloads"
            )
        try:
            encoded = json.dumps(
                inline_payload, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise JobResultSchemaError(
                "job result.output_payload.inline_payload must be JSON-compatible"
            ) from exc
        if len(encoded) > MAX_INLINE_OUTPUT_PAYLOAD_BYTES:
            raise JobResultSchemaError(
                "job result.output_payload.inline_payload exceeds the local inline size limit"
            )
        return
    _artifact_reference_list([payload_ref], "job result.output_payload.payload_ref")
    _content_addressed_id(payload_digest, "job result.output_payload.payload_digest")


def _references(value: object, manifest_id: object, receipt_id: object) -> None:
    references = _object(
        value,
        "job result.references",
        _REQUIRED_REFERENCE_FIELDS,
        allowed=_REFERENCE_FIELDS,
    )
    manifest_hash = references["manifest_hash"]
    manifest_ref = references.get("manifest_ref")
    if (manifest_hash is None) == (manifest_ref is None):
        raise JobResultSchemaError(
            "job result.references requires exactly one of manifest_hash or manifest_ref"
        )
    if manifest_hash is not None:
        if manifest_hash != manifest_id:
            raise JobResultSchemaError(
                "job result.references.manifest_hash must match manifest_id"
            )
        _content_addressed_id(manifest_hash, "job result.references.manifest_hash")
    else:
        _immutable_local_manifest_ref(manifest_ref)
        if manifest_ref != manifest_id:
            raise JobResultSchemaError(
                "job result.references.manifest_ref must match manifest_id"
            )
    for field in ("artifact_refs", "log_refs"):
        _artifact_reference_list(references[field], f"job result.references.{field}")
    _identifier_list(
        references["validation_receipt_ids"],
        "job result.references.validation_receipt_ids",
    )
    if receipt_id not in references["validation_receipt_ids"]:
        raise JobResultSchemaError(
            "job result.references.validation_receipt_ids must include validation_receipt_id"
        )


def _content_hash(value: object, context: str) -> None:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
    ):
        raise JobResultSchemaError(
            f"{context} must be an algorithm-prefixed lowercase SHA-256 digest"
        )


def _content_addressed_id(value: object, context: str) -> None:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
    ):
        raise JobResultSchemaError(f"{context} must be a SHA-256 content-addressed ID")


def _artifact_reference_list(value: object, context: str) -> None:
    if not isinstance(value, list):
        raise JobResultSchemaError(f"{context} must be a list")
    for reference in value:
        if not isinstance(reference, str) or not reference:
            raise JobResultSchemaError(f"{context} entries must be local references")
        if reference.startswith("sha256:"):
            _content_addressed_id(reference, f"{context} entries")
        elif (
            reference.startswith(("/", "~"))
            or _starts_with_uri_scheme(reference)
            or "\\" in reference
            or any(part == ".." for part in reference.split("/"))
        ):
            raise JobResultSchemaError(
                f"{context} entries must be relative local paths or content-addressed IDs"
            )


def _starts_with_uri_scheme(value: str) -> bool:
    scheme, separator, _remainder = value.partition(":")
    return bool(
        separator
        and scheme
        and scheme.isascii()
        and scheme[0].isalpha()
        and all(character.isalnum() or character in "+.-" for character in scheme)
    )


def _immutable_local_manifest_ref(value: object) -> None:
    """Accept only the atomic local work-manifest record as hash fallback."""

    if (
        not isinstance(value, str)
        or re.fullmatch(r"data/job-submissions/local-job-[0-9a-f]{32}\.json", value)
        is None
    ):
        raise JobResultSchemaError(
            "job result.references.manifest_ref must be an immutable local work manifest reference"
        )


def _identifier_list(value: object, context: str) -> None:
    if not isinstance(value, list):
        raise JobResultSchemaError(f"{context} must be a list")
    for item in value:
        _identifier(item, f"{context} entries")


def _lineage(value: object) -> None:
    lineage = _object(value, "job result.lineage", _LINEAGE_FIELDS)
    for field, references in lineage.items():
        _identifier_list(references, f"job result.lineage.{field}")


def _contribution(
    value: object,
    creator_node_id: object,
    executor_node_id: object,
    validator_node_id: object,
    lineage: object,
) -> None:
    contribution = _object(value, "job result.contribution", _CONTRIBUTION_FIELDS)
    for field, expected in (
        ("creator_node_id", creator_node_id),
        ("executor_node_id", executor_node_id),
        ("validator_node_id", validator_node_id),
    ):
        if contribution[field] != expected:
            raise JobResultSchemaError(
                f"job result.contribution.{field} must match the top-level record"
            )
        _identifier(contribution[field], f"job result.contribution.{field}")
    _identifier_list(
        contribution["upstream_lineage_sources"],
        "job result.contribution.upstream_lineage_sources",
    )
    if (
        not isinstance(lineage, dict)
        or contribution["upstream_lineage_sources"] != lineage["parent_job_ids"]
    ):
        raise JobResultSchemaError(
            "job result.contribution.upstream_lineage_sources must match lineage.parent_job_ids"
        )
    operator = contribution["local_operator_id"]
    if operator is not None:
        _identifier(operator, "job result.contribution.local_operator_id")
