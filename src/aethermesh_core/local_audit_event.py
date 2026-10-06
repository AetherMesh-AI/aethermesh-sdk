"""Canonical local JSONL audit events for prototype actions.

Every event records UTC ``timestamp``, a stable per-flow ``local_run_id``, and
the positive ``event_sequence`` offset within that run. Together with actor
and creator IDs plus event-specific manifest, receipt, lineage, and
contribution references, these are the minimum local-only reconstruction
fields. References are relative paths to avoid disclosing host locations.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath
from typing import Any

AUDIT_EVENT_SCHEMA_VERSION = 2
AUDIT_REDACTED_VALUE = "[REDACTED]"
_SENSITIVE_KEY_PARTS = frozenset(
    {
        "token",
        "secret",
        "password",
        "privatekey",
        "apikey",
        "seed",
        "credential",
        "authorization",
    }
)
_PRIVATE_CONTENT_KEYS = frozenset(
    {
        "env",
        "environment",
        "environmentmap",
        "environmentvariables",
        "envmap",
        "envvars",
        "payload",
        "rawpayload",
        "rawrequest",
        "rawrequestpayload",
        "requestbody",
        "requestpayload",
    }
)
_LOCAL_PATH_IN_TEXT = re.compile(
    r"(?<![A-Za-z0-9/])(?:file:///|~[/\\]|/(?!/)|[A-Za-z]:[/\\]|\\\\)[^\s\"']+"
)
_SECRET_IN_TEXT = re.compile(
    r"""(?ix)
    \b(?:bearer|basic)\s+[a-z0-9._~+/:=-]+
    |
    [\"']?(?:[a-z0-9]+[_-])?(?:api[_-]?key|private[_-]?key|password|secret|token|seed(?:[_ -]?phrase)?|credential|authorization)[\"']?\s*[:=]\s*(?:\"[^\"\r\n]*\"|'[^'\r\n]*'|[^,;\r\n]+)
    """
)
AUDIT_EVENT_TYPES = frozenset(
    {
        "node_initialized",
        "manifest_created",
        "work_submitted",
        "job_submitted",
        "validation_attempted",
        "validation_result",
        "validation_receipt_created",
        "lineage_linked",
        "contribution_record_updated",
        "job.execution.started",
        "job.execution.finished",
        "result.reported",
        "capability_advertised",
        "node.shutdown",
    }
)
_REQUIRED_FIELDS = frozenset(
    {
        "event_id",
        "timestamp",
        "event_type",
        "actor_node_id",
        "creator_node_id",
        "local_run_id",
        "event_sequence",
        "schema_version",
    }
)
_OPTIONAL_FIELDS = frozenset(
    {
        "manifest_id",
        "work_id",
        "validation_receipt_id",
        "lineage_parent_ids",
        "contribution_attribution_ids",
        "related_file_paths",
        "hashes",
        "signatures",
        "node_instance_id",
        "shutdown_reason",
        "exit_mode",
        "final_lifecycle_state",
        "validation_status",
        "manifest_ref",
        "validation_receipt_ref",
        "lineage_refs",
        "contribution_attribution_refs",
        "capability_advertisement_action",
        "node_id",
        "capability_id",
        "manifest_digest",
        "advertisement_payload_digest",
        "validation_receipt_refs",
        "contribution_attribution",
        "job_id",
        "local_node_id",
        "validation_expectation",
        "attribution_metadata_hash",
        "executing_node_id",
        "execution_id",
        "worker_node_id",
        "terminal_status",
        "finished_at",
        "duration_ms",
        "validator_node_id",
        "output_artifact_refs",
        "error_summary",
        "reporting_node_id",
        "result_status",
        "validation_result",
        "validator_name",
    }
)


class LocalAuditEventError(ValueError):
    """Raised when a local audit event does not match the canonical format."""


def sanitize_local_audit_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Return an audit-safe copy without secrets or host-specific absolute paths."""

    return {key: _sanitize_audit_value(value, key=key) for key, value in event.items()}


def validate_local_audit_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return one canonical local audit event without writing it."""

    document = dict(event)
    if any(not isinstance(field, str) for field in document):
        raise LocalAuditEventError("local audit event field names must be strings")
    unknown_fields = set(document) - _REQUIRED_FIELDS - _OPTIONAL_FIELDS
    if unknown_fields:
        raise LocalAuditEventError(
            f"local audit event has unsupported fields: {', '.join(sorted(unknown_fields))}"
        )
    missing_fields = _REQUIRED_FIELDS - set(document)
    if missing_fields:
        raise LocalAuditEventError(
            f"local audit event is missing required fields: {', '.join(sorted(missing_fields))}"
        )
    if (
        not isinstance(document["schema_version"], int)
        or isinstance(document["schema_version"], bool)
        or document["schema_version"] != AUDIT_EVENT_SCHEMA_VERSION
    ):
        raise LocalAuditEventError("local audit event.schema_version must be integer 2")
    for field in ("event_id", "actor_node_id", "local_run_id"):
        _require_text(document[field], f"local audit event.{field}")
    if (
        not isinstance(document["event_sequence"], int)
        or isinstance(document["event_sequence"], bool)
        or document["event_sequence"] < 1
    ):
        raise LocalAuditEventError(
            "local audit event.event_sequence must be a positive integer"
        )
    _require_timestamp(document["timestamp"])
    creator_node_id = document["creator_node_id"]
    if creator_node_id is not None:
        _require_text(creator_node_id, "local audit event.creator_node_id")
    if (
        not isinstance(document["event_type"], str)
        or document["event_type"] not in AUDIT_EVENT_TYPES
    ):
        raise LocalAuditEventError("local audit event.event_type is unsupported")
    for field in ("manifest_id", "work_id", "validation_receipt_id"):
        if field in document:
            _require_text(document[field], f"local audit event.{field}")
    for field in ("lineage_parent_ids", "contribution_attribution_ids"):
        if field in document:
            _require_text_list(document[field], f"local audit event.{field}")
    for field in ("lineage_refs", "contribution_attribution_refs"):
        if field in document:
            _require_local_paths(document[field], f"local audit event.{field}")
    for field in (
        "node_instance_id",
        "exit_mode",
        "final_lifecycle_state",
        "validation_status",
    ):
        if field in document:
            _require_text(document[field], f"local audit event.{field}")
    for field in ("shutdown_reason", "manifest_ref", "validation_receipt_ref"):
        if field in document and document[field] is not None:
            _require_text(document[field], f"local audit event.{field}")
    for field in ("manifest_ref", "validation_receipt_ref"):
        if field in document and document[field] is not None:
            _require_local_paths([document[field]], f"local audit event.{field}")
    if "related_file_paths" in document:
        _require_local_paths(
            document["related_file_paths"], "local audit event.related_file_paths"
        )
    for field in ("hashes", "signatures"):
        if field in document:
            _require_text_mapping(document[field], f"local audit event.{field}")
    if document["event_type"] == "node.shutdown":
        _validate_shutdown_event(document)
    if document["event_type"] == "capability_advertised":
        _validate_capability_advertisement_event(document)
    if document["event_type"] == "job_submitted":
        _validate_job_submission_event(document)
    if document["event_type"] == "job.execution.started":
        _validate_job_execution_started_event(document)
    if document["event_type"] == "job.execution.finished":
        _validate_job_execution_finished_event(document)
    if document["event_type"] == "result.reported":
        _validate_result_reported_event(document)
    if document["event_type"] == "validation_receipt_created":
        _validate_validation_receipt_created_event(document)
    if document["event_type"] == "contribution_record_updated":
        _validate_contribution_record_updated_event(document)
    return document


def append_local_audit_event(
    path: str | Path, event: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate and append one event; this API never updates prior JSONL records."""

    document = validate_local_audit_event(sanitize_local_audit_event(event))
    audit_path = Path(path)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return document


def _sanitize_audit_value(value: Any, *, key: object | None = None) -> Any:
    if isinstance(key, str) and _is_private_audit_key(key):
        return AUDIT_REDACTED_VALUE
    if isinstance(value, Mapping):
        return {
            child_key: _sanitize_audit_value(child_value, key=child_key)
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_audit_value(item) for item in value]
    if isinstance(value, str):
        return _redact_secret_text(_sanitize_local_path(value))
    return value


def _is_private_audit_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    if normalized in _SENSITIVE_KEY_PARTS or normalized in _PRIVATE_CONTENT_KEYS:
        return True
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _sanitize_local_path(value: str) -> str:
    path_variants = (Path(value), PureWindowsPath(value))
    if not value.startswith("~") and not any(
        path.is_absolute() for path in path_variants
    ):
        return _LOCAL_PATH_IN_TEXT.sub(
            lambda match: _local_path_label(match.group()), value
        )
    return _local_path_label(value)


def _redact_secret_text(value: str) -> str:
    return _SECRET_IN_TEXT.sub(AUDIT_REDACTED_VALUE, value)


def _local_path_label(value: str) -> str:
    path_variants = (Path(value), PureWindowsPath(value))
    name = next((path.name for path in reversed(path_variants) if path.name), "")
    return f"local-path/{name}" if name else "local-path"


def _require_text(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise LocalAuditEventError(f"{label} must be a non-empty string")


def _require_timestamp(value: object) -> None:
    if not isinstance(value, str):
        raise LocalAuditEventError(
            "local audit event.timestamp must be a UTC timestamp"
        )
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise LocalAuditEventError(
            "local audit event.timestamp must be a UTC timestamp"
        ) from exc


def _require_text_list(value: object, label: str) -> None:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise LocalAuditEventError(f"{label} must be a list of non-empty strings")


def _require_local_paths(value: object, label: str) -> None:
    if not isinstance(value, list):
        raise LocalAuditEventError(f"{label} must be a list of non-empty strings")
    _require_text_list(value, label)
    for item in value:
        path_variants = (Path(item), PureWindowsPath(item))
        has_parent_reference = any(".." in path.parts for path in path_variants)
        is_absolute = any(path.is_absolute() for path in path_variants)
        if (
            is_absolute
            or has_parent_reference
            or item.startswith("~")
            or "://" in item
            or "\\" in item
        ):
            raise LocalAuditEventError(f"{label} must contain safe relative paths")


def _require_text_mapping(value: object, label: str) -> None:
    if not isinstance(value, dict) or any(
        not isinstance(key, str)
        or not key.strip()
        or not isinstance(item, str)
        or not item.strip()
        for key, item in value.items()
    ):
        raise LocalAuditEventError(f"{label} must be an object of non-empty strings")


def _validate_shutdown_event(document: Mapping[str, Any]) -> None:
    """Require explicit local context for durable node shutdown audit entries."""

    required_fields = (
        "node_instance_id",
        "shutdown_reason",
        "exit_mode",
        "final_lifecycle_state",
        "validation_status",
        "manifest_ref",
        "validation_receipt_ref",
        "lineage_refs",
        "contribution_attribution_refs",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            f"node.shutdown event is missing required context: {', '.join(missing)}"
        )
    _require_text(document["creator_node_id"], "node.shutdown.creator_node_id")


def _validate_capability_advertisement_event(document: Mapping[str, Any]) -> None:
    """Require compact provenance for one local capability advertisement."""

    required_fields = (
        "capability_advertisement_action",
        "node_id",
        "capability_id",
        "manifest_ref",
        "manifest_digest",
        "advertisement_payload_digest",
        "validation_status",
        "validation_receipt_refs",
        "lineage_refs",
        "contribution_attribution",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            "capability_advertised event is missing required context: "
            f"{', '.join(missing)}"
        )
    if document["capability_advertisement_action"] not in {
        "created",
        "refreshed",
        "replaced",
    }:
        raise LocalAuditEventError(
            "capability_advertised action must be created, refreshed, or replaced"
        )
    for field in (
        "node_id",
        "capability_id",
        "manifest_digest",
        "advertisement_payload_digest",
        "validation_status",
    ):
        _require_text(document[field], f"capability_advertised.{field}")
    _require_text(document["creator_node_id"], "capability_advertised.creator_node_id")
    _require_local_paths(
        document["validation_receipt_refs"],
        "capability_advertised.validation_receipt_refs",
    )
    _require_text_mapping(
        document["contribution_attribution"],
        "capability_advertised.contribution_attribution",
    )


def _validate_contribution_record_updated_event(document: Mapping[str, Any]) -> None:
    """Limit ledger updates to the documented local evidence outcomes."""

    if "validation_status" not in document:
        raise LocalAuditEventError(
            "contribution_record_updated event is missing validation_status"
        )
    if document["validation_status"] not in {
        "recorded",
        "already_recorded",
        "rejected",
        "validation_failed",
    }:
        raise LocalAuditEventError(
            "contribution_record_updated.validation_status is unsupported"
        )


def _validate_job_submission_event(document: Mapping[str, Any]) -> None:
    """Require compact, non-payload evidence for one accepted local submission."""

    required_fields = (
        "job_id",
        "local_node_id",
        "manifest_ref",
        "hashes",
        "lineage_refs",
        "validation_expectation",
        "contribution_attribution",
        "attribution_metadata_hash",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            f"job_submitted event is missing required context: {', '.join(missing)}"
        )
    for field in (
        "job_id",
        "local_node_id",
        "validation_expectation",
        "attribution_metadata_hash",
    ):
        _require_text(document[field], f"job_submitted.{field}")
    if document["actor_node_id"] != document["local_node_id"]:
        raise LocalAuditEventError(
            "job_submitted.local_node_id must match actor_node_id"
        )
    _require_text(document["creator_node_id"], "job_submitted.creator_node_id")
    _require_text_mapping(
        document["contribution_attribution"], "job_submitted.contribution_attribution"
    )


def _validate_job_execution_started_event(document: Mapping[str, Any]) -> None:
    """Require compact provenance before a locally submitted job can run."""

    required_fields = (
        "job_id",
        "executing_node_id",
        "manifest_id",
        "manifest_ref",
        "hashes",
        "lineage_refs",
        "contribution_attribution",
        "attribution_metadata_hash",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            "job.execution.started event is missing required context: "
            f"{', '.join(missing)}"
        )
    for field in (
        "job_id",
        "executing_node_id",
        "manifest_id",
        "attribution_metadata_hash",
    ):
        _require_text(document[field], f"job.execution.started.{field}")
    if document["actor_node_id"] != document["executing_node_id"]:
        raise LocalAuditEventError(
            "job.execution.started.executing_node_id must match actor_node_id"
        )
    _require_text(document["creator_node_id"], "job.execution.started.creator_node_id")
    _require_text_mapping(
        document["contribution_attribution"],
        "job.execution.started.contribution_attribution",
    )


def _validate_job_execution_finished_event(document: Mapping[str, Any]) -> None:
    """Require terminal local execution evidence without rewriting start entries."""

    required_fields = (
        "job_id",
        "execution_id",
        "worker_node_id",
        "manifest_id",
        "manifest_ref",
        "lineage_refs",
        "terminal_status",
        "finished_at",
        "duration_ms",
        "validation_receipt_ref",
        "validator_node_id",
        "output_artifact_refs",
        "error_summary",
        "contribution_attribution",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            "job.execution.finished event is missing required context: "
            f"{', '.join(missing)}"
        )
    for field in ("job_id", "execution_id", "manifest_id", "terminal_status"):
        _require_text(document[field], f"job.execution.finished.{field}")
    if document["terminal_status"] not in {
        "completed",
        "failed",
        "cancelled",
        "validation-failed",
    }:
        raise LocalAuditEventError(
            "job.execution.finished.terminal_status is unsupported"
        )
    _require_timestamp(document["finished_at"])
    if (
        not isinstance(document["duration_ms"], int)
        or isinstance(document["duration_ms"], bool)
        or document["duration_ms"] < 0
    ):
        raise LocalAuditEventError(
            "job.execution.finished.duration_ms must be a non-negative integer"
        )
    for field in ("worker_node_id", "validator_node_id", "error_summary"):
        if document[field] is not None:
            _require_text(document[field], f"job.execution.finished.{field}")
    _require_local_paths(
        document["output_artifact_refs"],
        "job.execution.finished.output_artifact_refs",
    )
    _require_text(document["creator_node_id"], "job.execution.finished.creator_node_id")
    _require_text_mapping(
        document["contribution_attribution"],
        "job.execution.finished.contribution_attribution",
    )


def _validate_result_reported_event(document: Mapping[str, Any]) -> None:
    """Require compact references for one accepted local result report."""

    required_fields = (
        "reporting_node_id",
        "work_id",
        "manifest_id",
        "manifest_ref",
        "validation_receipt_id",
        "validation_receipt_ref",
        "lineage_refs",
        "contribution_attribution_ids",
        "contribution_attribution_refs",
        "result_status",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            "result.reported event is missing required context: " + ", ".join(missing)
        )
    for field in ("reporting_node_id", "work_id", "manifest_id", "result_status"):
        _require_text(document[field], f"result.reported.{field}")
    if document["actor_node_id"] != document["reporting_node_id"]:
        raise LocalAuditEventError(
            "result.reported.reporting_node_id must match actor_node_id"
        )
    _require_text(document["creator_node_id"], "result.reported.creator_node_id")
    _require_local_paths(document["lineage_refs"], "result.reported.lineage_refs")
    _require_local_paths(
        document["contribution_attribution_refs"],
        "result.reported.contribution_attribution_refs",
    )


def _validate_validation_receipt_created_event(document: Mapping[str, Any]) -> None:
    """Require durable receipt provenance for one local validation creation."""

    required_fields = (
        "work_id",
        "manifest_id",
        "manifest_ref",
        "validation_receipt_id",
        "validation_receipt_ref",
        "validation_result",
        "validator_node_id",
        "validator_name",
        "lineage_refs",
        "contribution_attribution_ids",
        "contribution_attribution",
    )
    missing = [field for field in required_fields if field not in document]
    if missing:
        raise LocalAuditEventError(
            "validation_receipt_created event is missing required context: "
            f"{', '.join(missing)}"
        )
    for field in (
        "work_id",
        "manifest_id",
        "validation_receipt_id",
        "validation_result",
        "validator_node_id",
        "validator_name",
    ):
        _require_text(document[field], f"validation_receipt_created.{field}")
    if document["validation_result"] not in {"accepted", "rejected"}:
        raise LocalAuditEventError(
            "validation_receipt_created.validation_result must be accepted or rejected"
        )
    if document["actor_node_id"] != document["validator_node_id"]:
        raise LocalAuditEventError(
            "validation_receipt_created.validator_node_id must match actor_node_id"
        )
    _require_text(
        document["creator_node_id"], "validation_receipt_created.creator_node_id"
    )
    _require_local_paths(
        document["lineage_refs"], "validation_receipt_created.lineage_refs"
    )
    _require_text_mapping(
        document["contribution_attribution"],
        "validation_receipt_created.contribution_attribution",
    )
