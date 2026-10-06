"""Schema validation and stable hashing for local validation receipts."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
from datetime import UTC, datetime
from importlib import metadata
from typing import Any

VALIDATION_RECEIPT_SCHEMA_VERSION = 10
VALIDATION_STATUSES = frozenset({"pass", "fail", "error", "skipped"})
RECEIPT_STATUSES = frozenset({"accepted", "rejected"})
VALIDATION_RECEIPT_ID_PREFIX = "local-validation-receipt-"
_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "receipt_id",
        "receipt_hash",
        "result_hash",
        "created_at",
        "validated_at",
        "creator_node_id",
        "contributor_node_id",
        "model_expert_id",
        "expert_version",
        "job_id",
        "work_id",
        "manifest_id",
        "manifest_reference",
        "status",
        "rejection_reason",
        "validation_status",
        "validation_method",
        "validator_id",
        "validator_software",
        "lineage",
        "contribution",
        "evidence",
    }
)
_LINEAGE_FIELDS = frozenset(
    {
        "contributor_node_id",
        "parent_work_ids",
        "source_manifest_refs",
        "input_hashes",
        "output_hashes",
        "prior_receipt_ids",
    }
)
_MANIFEST_REFERENCE_FIELDS = frozenset({"manifest_id", "manifest_ref"})
_CONTRIBUTION_FIELDS = frozenset(
    {
        "contributor_node_id",
        "submitter_id",
        "local_node_id",
        "claimed_role",
        "contribution_manifest_ref",
    }
)
_EVIDENCE_FIELDS = frozenset(
    {
        "test_command",
        "environment_summary",
        "exit_code",
        "log_path",
        "artifact_path",
        "reason",
        "next_local_action",
    }
)


class ValidationReceiptSchemaError(ValueError):
    """Raised when a local validation receipt violates its stable schema."""


def validation_receipt_id(work_id: str) -> str:
    """Return the stable local receipt identifier for one work item."""

    _identifier(work_id, "validation receipt.work_id")
    return f"{VALIDATION_RECEIPT_ID_PREFIX}{work_id}"


def canonical_validation_receipt_hash(document: object) -> str:
    """Hash stable receipt evidence, excluding local audit times and hash itself."""

    if not isinstance(document, dict):
        raise ValidationReceiptSchemaError("validation receipt must be an object")
    payload = {
        key: value
        for key, value in document.items()
        if key not in {"created_at", "validated_at", "receipt_hash"}
    }
    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValidationReceiptSchemaError(
            "validation receipt must contain JSON-compatible data"
        ) from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def validate_validation_receipt_document(document: object) -> dict[str, Any]:
    """Validate one JSON-only local receipt without filesystem access."""

    receipt = _object(document, "validation receipt", _REQUIRED_FIELDS)
    if (
        not isinstance(receipt["schema_version"], int)
        or isinstance(receipt["schema_version"], bool)
        or receipt["schema_version"] != VALIDATION_RECEIPT_SCHEMA_VERSION
    ):
        raise ValidationReceiptSchemaError(
            "validation receipt.schema_version must be integer 10"
        )
    for field in (
        "receipt_id",
        "creator_node_id",
        "contributor_node_id",
        "model_expert_id",
        "expert_version",
        "job_id",
        "work_id",
        "validator_id",
    ):
        _identifier(receipt[field], f"validation receipt.{field}")
    if receipt["receipt_id"] != validation_receipt_id(receipt["work_id"]):
        raise ValidationReceiptSchemaError(
            "validation receipt.receipt_id must match its work_id"
        )
    if receipt["job_id"] != receipt["work_id"]:
        raise ValidationReceiptSchemaError(
            "validation receipt.job_id must match its work_id"
        )
    _nullable_content_addressed_id(
        receipt["manifest_id"], "validation receipt.manifest_id"
    )
    _manifest_reference(receipt["manifest_reference"], receipt["manifest_id"])
    _sha256_content_id(receipt["result_hash"], "validation receipt.result_hash")
    _timestamp(receipt["created_at"], "validation receipt.created_at")
    _timestamp(receipt["validated_at"], "validation receipt.validated_at")
    status = receipt["status"]
    if not isinstance(status, str) or status not in RECEIPT_STATUSES:
        raise ValidationReceiptSchemaError(
            "validation receipt.status must be accepted or rejected"
        )
    if (
        not isinstance(receipt["validation_status"], str)
        or receipt["validation_status"] not in VALIDATION_STATUSES
    ):
        raise ValidationReceiptSchemaError(
            "validation receipt.validation_status is unsupported"
        )
    expected_status = (
        "accepted" if receipt["validation_status"] == "pass" else "rejected"
    )
    if status != expected_status:
        raise ValidationReceiptSchemaError(
            "validation receipt.status does not match validation_status"
        )
    rejection_reason = receipt["rejection_reason"]
    if status == "rejected":
        if not isinstance(rejection_reason, str) or not rejection_reason.strip():
            raise ValidationReceiptSchemaError(
                "rejected validation receipt.rejection_reason must be a non-empty string"
            )
    elif rejection_reason is not None:
        raise ValidationReceiptSchemaError(
            "accepted validation receipt.rejection_reason must be null"
        )
    _lineage(receipt["lineage"], receipt["contributor_node_id"])
    _contribution(receipt["contribution"], receipt["contributor_node_id"])
    validate_validator_software_metadata(
        receipt["validator_software"], receipt_schema_version=receipt["schema_version"]
    )
    _validation_method(receipt["validation_method"], receipt)
    _evidence(receipt["evidence"])
    if not isinstance(receipt["receipt_hash"], str) or receipt[
        "receipt_hash"
    ] != canonical_validation_receipt_hash(receipt):
        raise ValidationReceiptSchemaError(
            "validation receipt.receipt_hash does not match"
        )
    return receipt


def capture_validator_software_metadata(
    *, validator_name: str, receipt_schema_version: int
) -> dict[str, object]:
    """Capture minimal local software/runtime facts when a receipt is created."""

    try:
        validator_version = metadata.version("aethermesh")
    except metadata.PackageNotFoundError:
        from aethermesh_core import __version__

        validator_version = __version__
    document: dict[str, object] = {
        "validator_name": validator_name,
        "validator_version": validator_version or "unknown",
        "validator_build_identifier": os.environ.get("AETHERMESH_BUILD_ID", "unknown"),
        "runtime_name": platform.python_implementation() or "unknown",
        "runtime_version": platform.python_version() or "unknown",
        "platform": f"{platform.system() or 'unknown'}/{platform.machine() or 'unknown'}",
        "receipt_schema_version": receipt_schema_version,
    }
    try:
        validate_validator_software_metadata(
            document, receipt_schema_version=receipt_schema_version
        )
    except ValidationReceiptSchemaError:
        document["validator_build_identifier"] = "unknown"
        validate_validator_software_metadata(
            document, receipt_schema_version=receipt_schema_version
        )
    return document


def validate_validator_software_metadata(
    value: object, *, receipt_schema_version: int
) -> dict[str, object]:
    """Validate minimal, path-safe validator software/runtime metadata."""

    metadata_document = _object(
        value,
        "validation receipt.validator_software",
        frozenset(
            {
                "validator_name",
                "validator_version",
                "validator_build_identifier",
                "runtime_name",
                "runtime_version",
                "platform",
                "receipt_schema_version",
            }
        ),
    )
    for field in (
        "validator_name",
        "validator_version",
        "validator_build_identifier",
        "runtime_name",
        "runtime_version",
        "platform",
    ):
        if (
            not isinstance(metadata_document[field], str)
            or not metadata_document[field].strip()
        ):
            raise ValidationReceiptSchemaError(
                f"validation receipt.validator_software.{field} must be a non-empty string"
            )
    build_identifier = metadata_document["validator_build_identifier"]
    if "/" in build_identifier or "\\" in build_identifier:
        raise ValidationReceiptSchemaError(
            "validation receipt.validator_software.validator_build_identifier must not be a path"
        )
    metadata_schema_version = metadata_document["receipt_schema_version"]
    if (
        not isinstance(metadata_schema_version, int)
        or isinstance(metadata_schema_version, bool)
        or metadata_schema_version != receipt_schema_version
    ):
        raise ValidationReceiptSchemaError(
            "validation receipt.validator_software.receipt_schema_version must match receipt"
        )
    return dict(metadata_document)


def _object(value: object, label: str, required: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationReceiptSchemaError(f"{label} must be an object")
    fields = set(value)
    missing = sorted(required - fields)
    if missing:
        raise ValidationReceiptSchemaError(f"{label} missing: {', '.join(missing)}")
    unknown = sorted(fields - required)
    if unknown:
        raise ValidationReceiptSchemaError(
            f"{label} contains unsupported fields: {', '.join(unknown)}"
        )
    return value


def _identifier(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or any(character.isspace() for character in value)
    ):
        raise ValidationReceiptSchemaError(f"{label} must be a non-empty identifier")


def _timestamp(value: object, label: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z", value
    ):
        raise ValidationReceiptSchemaError(f"{label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationReceiptSchemaError(f"{label} must be a UTC timestamp") from exc
    if parsed.tzinfo != UTC:
        raise ValidationReceiptSchemaError(f"{label} must be a UTC timestamp")


def _string_list(value: object, label: str) -> None:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item and item.strip() == item for item in value
    ):
        raise ValidationReceiptSchemaError(f"{label} must be a string list")


def _content_addressed_id(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
    ):
        raise ValidationReceiptSchemaError(
            f"{label} must be a SHA-256 content-addressed ID"
        )


def _sha256_content_id(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
    ):
        raise ValidationReceiptSchemaError(
            f"{label} must be an algorithm-prefixed lowercase SHA-256 digest"
        )


def _nullable_content_addressed_id(value: object, label: str) -> None:
    if value is not None:
        _content_addressed_id(value, label)


def _local_reference(value: object, label: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValidationReceiptSchemaError(
            f"{label} must be a safe relative local path"
        )
    if (
        value.startswith(("/", "~"))
        or re.match(r"[A-Za-z][A-Za-z0-9+.-]*:", value) is not None
        or "\\" in value
        or any(part == ".." for part in value.split("/"))
    ):
        raise ValidationReceiptSchemaError(
            f"{label} must be a safe relative local path"
        )


def _nullable_string(value: object, label: str) -> None:
    if value is not None and (
        not isinstance(value, str) or not value or value.strip() != value
    ):
        raise ValidationReceiptSchemaError(
            f"{label} must be null or a non-empty string"
        )


def _nullable_identifier(value: object, label: str) -> None:
    if value is not None:
        _identifier(value, label)


def _validation_method(value: object, receipt: dict[str, Any]) -> None:
    method = _object(
        value,
        "validation receipt.validation_method",
        frozenset(
            {
                "kind",
                "description",
                "manifest_id",
                "creator_node_id",
                "contributor_node_id",
                "work_id",
                "lineage_parent_work_ids",
                "contribution_manifest_ref",
            }
        ),
    )
    if not isinstance(method["kind"], str) or not method["kind"].strip():
        raise ValidationReceiptSchemaError(
            "validation receipt.validation_method.kind must be a non-empty string"
        )
    if not isinstance(method["description"], str) or not method["description"].strip():
        raise ValidationReceiptSchemaError(
            "validation receipt.validation_method.description must be a non-empty string"
        )
    for field in ("manifest_id", "creator_node_id", "contributor_node_id", "work_id"):
        if method[field] != receipt[field]:
            raise ValidationReceiptSchemaError(
                f"validation receipt.validation_method.{field} must match receipt"
            )
    _string_list(
        method["lineage_parent_work_ids"],
        "validation receipt.validation_method.lineage_parent_work_ids",
    )
    if method["lineage_parent_work_ids"] != receipt["lineage"]["parent_work_ids"]:
        raise ValidationReceiptSchemaError(
            "validation receipt.validation_method.lineage_parent_work_ids must match receipt"
        )
    if (
        method["contribution_manifest_ref"]
        != receipt["contribution"]["contribution_manifest_ref"]
    ):
        raise ValidationReceiptSchemaError(
            "validation receipt.validation_method.contribution_manifest_ref must match receipt"
        )


def _manifest_reference(value: object, manifest_id: object) -> None:
    if value is None:
        if manifest_id is not None:
            raise ValidationReceiptSchemaError(
                "validation receipt.manifest_reference is required when manifest_id is present"
            )
        return
    reference = _object(
        value, "validation receipt.manifest_reference", _MANIFEST_REFERENCE_FIELDS
    )
    _content_addressed_id(
        reference["manifest_id"], "validation receipt.manifest_reference.manifest_id"
    )
    _local_reference(
        reference["manifest_ref"], "validation receipt.manifest_reference.manifest_ref"
    )
    if reference["manifest_id"] != manifest_id:
        raise ValidationReceiptSchemaError(
            "validation receipt.manifest_reference.manifest_id must match receipt"
        )


def _lineage(value: object, contributor_node_id: str) -> None:
    lineage = _object(value, "validation receipt.lineage", _LINEAGE_FIELDS)
    _identifier(
        lineage["contributor_node_id"],
        "validation receipt.lineage.contributor_node_id",
    )
    if lineage["contributor_node_id"] != contributor_node_id:
        raise ValidationReceiptSchemaError(
            "validation receipt.lineage.contributor_node_id must match receipt"
        )
    for field in ("parent_work_ids", "prior_receipt_ids"):
        _string_list(lineage[field], f"validation receipt.lineage.{field}")
        for identifier in lineage[field]:
            _identifier(identifier, f"validation receipt.lineage.{field} entries")
    _string_list(
        lineage["source_manifest_refs"],
        "validation receipt.lineage.source_manifest_refs",
    )
    for reference in lineage["source_manifest_refs"]:
        _local_reference(
            reference, "validation receipt.lineage.source_manifest_refs entries"
        )
    for field in ("input_hashes", "output_hashes"):
        _string_list(lineage[field], f"validation receipt.lineage.{field}")
        for content_hash in lineage[field]:
            _content_addressed_id(
                content_hash, f"validation receipt.lineage.{field} entries"
            )


def _contribution(value: object, contributor_node_id: str) -> None:
    contribution = _object(
        value, "validation receipt.contribution", _CONTRIBUTION_FIELDS
    )
    _identifier(
        contribution["contributor_node_id"],
        "validation receipt.contribution.contributor_node_id",
    )
    if contribution["contributor_node_id"] != contributor_node_id:
        raise ValidationReceiptSchemaError(
            "validation receipt.contribution.contributor_node_id must match receipt"
        )
    for field in ("submitter_id", "local_node_id", "claimed_role"):
        _nullable_identifier(
            contribution[field], f"validation receipt.contribution.{field}"
        )
    _local_reference(
        contribution["contribution_manifest_ref"],
        "validation receipt.contribution.contribution_manifest_ref",
        nullable=True,
    )


def _evidence(value: object) -> None:
    evidence = _object(value, "validation receipt.evidence", _EVIDENCE_FIELDS)
    for field in ("test_command", "environment_summary", "log_path", "artifact_path"):
        _nullable_string(evidence[field], f"validation receipt.evidence.{field}")
    for field in ("log_path", "artifact_path"):
        _local_reference(
            evidence[field], f"validation receipt.evidence.{field}", nullable=True
        )
    if not isinstance(evidence["reason"], str) or not evidence["reason"].strip():
        raise ValidationReceiptSchemaError(
            "validation receipt.evidence.reason must be a non-empty string"
        )
    if (
        not isinstance(evidence["next_local_action"], str)
        or not evidence["next_local_action"].strip()
    ):
        raise ValidationReceiptSchemaError(
            "validation receipt.evidence.next_local_action must be a non-empty string"
        )
    exit_code = evidence["exit_code"]
    if exit_code is not None and (
        not isinstance(exit_code, int) or isinstance(exit_code, bool)
    ):
        raise ValidationReceiptSchemaError(
            "validation receipt.evidence.exit_code must be null or an integer"
        )
