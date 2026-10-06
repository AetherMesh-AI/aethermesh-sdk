"""Validation for the local-only version 1 capability record contract."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

CAPABILITY_RECORD_SCHEMA_VERSION = 1
CAPABILITY_TYPES = frozenset({"model", "tool", "worker", "runtime"})
VALIDATION_STATUSES = frozenset({"unvalidated", "passed", "failed"})
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "capability_id",
        "capability_version",
        "node_id",
        "creator_node_id",
        "created_at",
        "updated_at",
        "metadata",
        "manifest_refs",
        "validation",
        "lineage",
        "contribution_attribution",
    }
)
_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9_.-]{2,127}\Z")
_SEMVER = re.compile(
    r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
    r"(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
_URI_SCHEME = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*:")


class CapabilityRecordError(ValueError):
    """Raised when a local capability record is incomplete or dishonest."""


def validate_capability_record(
    document: object, *, local_node_id: str, local_schema_root: str | Path = "."
) -> dict[str, Any]:
    """Validate and return one local-only capability record without writing it.

    A passed claim requires local receipt evidence; an unvalidated claim is
    accepted only as explicitly untrusted local metadata.
    """
    if not isinstance(document, dict):
        raise CapabilityRecordError("capability record must be an object")
    _reject_unknown_fields(document)
    _require_int(document, "schema_version", CAPABILITY_RECORD_SCHEMA_VERSION)
    _require_identifier(document, "capability_id")
    _require_capability_version(document)
    node_id = _require_identifier(document, "node_id")
    if node_id != _require_identifier_value(local_node_id, "local_node_id"):
        raise CapabilityRecordError("node_id must match the local node identity")
    _require_identifier(document, "creator_node_id")
    _require_timestamp(document, "created_at")
    _require_timestamp(document, "updated_at")
    _require_metadata(document, local_schema_root=local_schema_root)
    _require_ref_list(document, "manifest_refs", minimum=1)
    _require_lineage(document)
    _require_attribution(document)
    _require_validation(document)
    return document


def _reject_unknown_fields(document: dict[str, Any]) -> None:
    allowed_by_field = {
        "metadata": frozenset(
            {
                "name",
                "description",
                "type",
                "supported_input_formats",
                "supported_input_schemas",
                "supported_output_formats",
                "supported_output_schemas",
                "constraints",
                "local_execution_requirements",
            }
        ),
        "validation": frozenset(
            {
                "status",
                "receipt_ids",
                "receipt_evidence",
                "last_validated_at",
                "check_name",
                "failure_reason",
            }
        ),
        "lineage": frozenset(
            {"source_manifest_ref", "prior_capability_id", "local_build_artifact_ref"}
        ),
        "contribution_attribution": frozenset(
            {"creator_node_id", "maintainer_node_id", "work_receipt_ids"}
        ),
    }
    unknown = sorted(document.keys() - _TOP_LEVEL_FIELDS)
    if unknown:
        raise CapabilityRecordError(
            f"capability record contains unsupported fields: {', '.join(unknown)}"
        )
    for field, allowed in allowed_by_field.items():
        value = document.get(field)
        if isinstance(value, dict):
            unknown = sorted(value.keys() - allowed)
            if unknown:
                raise CapabilityRecordError(
                    f"{field} contains unsupported fields: {', '.join(unknown)}"
                )


def _require_metadata(
    document: dict[str, Any], *, local_schema_root: str | Path
) -> None:
    metadata = _require_object(document, "metadata")
    _require_string(metadata, "name")
    _require_string(metadata, "description")
    capability_type = _require_string(metadata, "type")
    if capability_type not in CAPABILITY_TYPES:
        raise CapabilityRecordError("metadata.type is not allowed")
    _require_string_list(metadata, "supported_input_formats", minimum=1)
    _require_schemas(
        metadata, "supported_input_schemas", local_schema_root=local_schema_root
    )
    _require_string_list(metadata, "supported_output_formats", minimum=1)
    _require_schemas(
        metadata, "supported_output_schemas", local_schema_root=local_schema_root
    )
    if not isinstance(metadata.get("constraints"), dict):
        raise CapabilityRecordError("metadata.constraints must be an object")
    _require_string_list(metadata, "local_execution_requirements", minimum=1)


def _require_validation(document: dict[str, Any]) -> None:
    validation = _require_object(document, "validation")
    status = _require_string(validation, "status")
    if status not in VALIDATION_STATUSES:
        raise CapabilityRecordError("validation.status is not allowed")
    _require_identifier_list(validation, "receipt_ids")
    if status == "passed":
        if not validation["receipt_ids"]:
            raise CapabilityRecordError("passed validation requires a receipt ID")
        _require_timestamp(validation, "last_validated_at")
        _require_string(validation, "check_name")
        if "failure_reason" in validation:
            raise CapabilityRecordError(
                "passed validation must not include failure_reason"
            )
    elif status == "failed":
        _require_timestamp(validation, "last_validated_at")
        _require_string(validation, "check_name")
        _require_string(validation, "failure_reason")
    else:
        if validation["receipt_ids"]:
            raise CapabilityRecordError(
                "unvalidated capability must not claim receipt IDs"
            )
        for field in ("last_validated_at", "check_name", "failure_reason"):
            if field in validation:
                raise CapabilityRecordError(
                    f"unvalidated capability must not include {field}"
                )
    _require_receipt_evidence(validation, document)


def _require_receipt_evidence(
    validation: dict[str, Any], document: dict[str, Any]
) -> None:
    evidence = validation.get("receipt_evidence")
    if not isinstance(evidence, list):
        raise CapabilityRecordError("validation.receipt_evidence must be a list")
    receipt_ids = validation["receipt_ids"]
    if len(evidence) != len(receipt_ids):
        raise CapabilityRecordError(
            "validation.receipt_evidence must match validation.receipt_ids"
        )
    expected = {
        "capability_name": document["metadata"]["name"],
        "capability_version": document["capability_version"],
        "creator_node_id": document["creator_node_id"],
        "manifest_ref": document["lineage"]["source_manifest_ref"],
    }
    supported_input_schemas = document["metadata"]["supported_input_schemas"]
    supported_output_schemas = document["metadata"]["supported_output_schemas"]
    for index, receipt in enumerate(evidence):
        if not isinstance(receipt, dict):
            raise CapabilityRecordError(
                f"validation.receipt_evidence[{index}] must be an object"
            )
        allowed_fields = {"receipt_id", "input_schema", "output_schema", *expected}
        if receipt.keys() != allowed_fields:
            raise CapabilityRecordError(
                f"validation.receipt_evidence[{index}] must contain exactly the "
                "documented receipt fields"
            )
        if receipt.get("receipt_id") != receipt_ids[index]:
            raise CapabilityRecordError(
                "validation.receipt_evidence receipt_id must match validation.receipt_ids"
            )
        if {key: receipt.get(key) for key in expected} != expected:
            raise CapabilityRecordError(
                "validation receipt must record matching capability name, version, "
                "creator node ID, and manifest reference"
            )
        if receipt.get("input_schema") not in supported_input_schemas:
            raise CapabilityRecordError(
                "validation receipt must record a supported input schema reference"
            )
        if receipt.get("output_schema") not in supported_output_schemas:
            raise CapabilityRecordError(
                "validation receipt must record a supported output schema reference"
            )


def _require_lineage(document: dict[str, Any]) -> None:
    lineage = _require_object(document, "lineage")
    _require_ref(lineage, "source_manifest_ref")
    _require_optional_identifier(lineage, "prior_capability_id")
    _require_optional_ref(lineage, "local_build_artifact_ref")


def _require_schemas(
    metadata: dict[str, Any], field: str, *, local_schema_root: str | Path
) -> None:
    schemas = metadata.get(field)
    if not isinstance(schemas, list) or not schemas:
        raise CapabilityRecordError(f"metadata.{field} must be a non-empty list")
    root = Path(local_schema_root).resolve()
    for index, schema in enumerate(schemas):
        context = f"metadata.{field}[{index}]"
        if not isinstance(schema, dict) or set(schema) != {
            "schema_ref",
            "schema_id",
            "schema_version",
            "schema_digest",
        }:
            raise CapabilityRecordError(
                f"{context} must contain exactly schema_ref, schema_id, schema_version, and schema_digest"
            )
        schema_ref = schema["schema_ref"]
        _require_safe_ref(schema_ref, f"{context}.schema_ref")
        schema_id = schema["schema_id"]
        if not isinstance(schema_id, str) or not schema_id.strip():
            raise CapabilityRecordError(
                f"{context}.schema_id must be a non-empty string"
            )
        schema_version = schema["schema_version"]
        if not isinstance(schema_version, str) or not _SEMVER.fullmatch(schema_version):
            raise CapabilityRecordError(
                f"{context}.schema_version must be a semantic version"
            )
        digest = schema["schema_digest"]
        if not isinstance(digest, str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", digest
        ):
            raise CapabilityRecordError(
                f"{context}.schema_digest must be a lowercase SHA-256 digest"
            )
        try:
            path = (root / schema_ref).resolve(strict=True)
            path.relative_to(root)
            contents = path.read_bytes()
        except (OSError, ValueError) as exc:
            raise CapabilityRecordError(
                f"{context}.schema_ref does not name a readable local schema"
            ) from exc
        if sha256(contents).hexdigest() != digest.removeprefix("sha256:"):
            raise CapabilityRecordError(
                f"{context}.schema_digest does not match local schema"
            )
        try:
            local_schema = json.loads(contents)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CapabilityRecordError(
                f"{context}.schema_ref must contain readable JSON"
            ) from exc
        if (
            not isinstance(local_schema, dict)
            or local_schema.get("$schema")
            != "https://json-schema.org/draft/2020-12/schema"
        ):
            raise CapabilityRecordError(
                f"{context}.schema_ref must contain a supported JSON Schema draft"
            )
        if local_schema.get("x-aethermesh-schema-version") != schema_version:
            raise CapabilityRecordError(
                f"{context}.schema_version must match the local schema version"
            )
        if local_schema.get("$id") != schema_id:
            raise CapabilityRecordError(
                f"{context}.schema_id must match the local schema ID"
            )


def _require_attribution(document: dict[str, Any]) -> None:
    attribution = _require_object(document, "contribution_attribution")
    if (
        _require_identifier(attribution, "creator_node_id")
        != document["creator_node_id"]
    ):
        raise CapabilityRecordError(
            "attribution creator_node_id must match creator_node_id"
        )
    _require_identifier(attribution, "maintainer_node_id")
    _require_identifier_list(attribution, "work_receipt_ids")


def _require_object(document: dict[str, Any], field: str) -> dict[str, Any]:
    value = document.get(field)
    if not isinstance(value, dict):
        raise CapabilityRecordError(f"{field} must be an object")
    return value


def _require_string(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value.strip():
        raise CapabilityRecordError(f"{field} must be a non-empty string")
    return value


def _require_int(document: dict[str, Any], field: str, expected: int) -> None:
    value = document.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value != expected:
        raise CapabilityRecordError(f"{field} must be integer {expected}")


def _require_capability_version(document: dict[str, Any]) -> str:
    value = _require_string(document, "capability_version")
    if not _SEMVER.fullmatch(value):
        raise CapabilityRecordError("capability_version must be a semantic version")
    return value


def _require_identifier(document: dict[str, Any], field: str) -> str:
    return _require_identifier_value(document.get(field), field)


def _require_identifier_value(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CapabilityRecordError(f"{field} must be a non-empty string")
    if not _IDENTIFIER.fullmatch(value):
        raise CapabilityRecordError(f"{field} must be a stable local identifier")
    return value


def _require_timestamp(document: dict[str, Any], field: str) -> None:
    value = _require_string(document, field)
    if not _TIMESTAMP.fullmatch(value):
        raise CapabilityRecordError(f"{field} must be an RFC 3339 UTC timestamp")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise CapabilityRecordError(
            f"{field} must be an RFC 3339 UTC timestamp"
        ) from exc


def _require_ref_list(
    document: dict[str, Any], field: str, *, minimum: int = 0
) -> None:
    values = document.get(field)
    if not isinstance(values, list) or len(values) < minimum:
        raise CapabilityRecordError(
            f"{field} must be a list with at least {minimum} item(s)"
        )
    for index, value in enumerate(values):
        _require_safe_ref(value, f"{field}[{index}]")


def _require_identifier_list(document: dict[str, Any], field: str) -> None:
    values = document.get(field)
    if not isinstance(values, list):
        raise CapabilityRecordError(f"{field} must be a list")
    for index, value in enumerate(values):
        if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
            raise CapabilityRecordError(
                f"{field}[{index}] must be a stable local identifier"
            )


def _require_string_list(document: dict[str, Any], field: str, *, minimum: int) -> None:
    values = document.get(field)
    if not isinstance(values, list) or len(values) < minimum:
        raise CapabilityRecordError(
            f"{field} must be a list with at least {minimum} item(s)"
        )
    for index, value in enumerate(values):
        if not isinstance(value, str) or not value.strip():
            raise CapabilityRecordError(f"{field}[{index}] must be a non-empty string")


def _require_ref(document: dict[str, Any], field: str) -> None:
    _require_safe_ref(document.get(field), field)


def _require_optional_identifier(document: dict[str, Any], field: str) -> None:
    if field in document and document[field] is not None:
        _require_identifier(document, field)


def _require_optional_ref(document: dict[str, Any], field: str) -> None:
    if field in document and document[field] is not None:
        _require_ref(document, field)


def _require_safe_ref(value: object, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith(("/", "~"))
        or "\\" in value
        or ".." in value.split("/")
        or _URI_SCHEME.match(value)
    ):
        raise CapabilityRecordError(f"{field} must be a safe local relative reference")
