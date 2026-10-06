"""Small, local-only version 1 through 7 model/expert manifest validation."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from referencing.exceptions import Unresolvable

MANIFEST_SCHEMA_VERSION = 7
RECEIPT_VERSION = "aethermesh-expert-validation-receipt/v0"
_HASH = re.compile(r"sha256:[0-9a-f]{64}\Z")
_PLACEHOLDER_HASH = re.compile(r"placeholder:sha256:[0-9a-f]{64}\Z")
_PLACEHOLDER_CREATOR_NODE_ID = re.compile(
    r"(?:node[-_.]?)?(?:placeholder|unknown|unset|none|null)(?:[-_.]?(?:node|id|node[-_.]?id))?\Z",
    re.IGNORECASE,
)
_SAFE_REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]*\Z")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
_STATUSES = {"unvalidated", "passed", "failed"}
_JSON_SCHEMA_TYPES = {
    "array",
    "boolean",
    "integer",
    "null",
    "number",
    "object",
    "string",
}
_JSON_SCHEMA_CONSTRAINTS_BY_TYPE = {
    "array": {
        "const",
        "contains",
        "enum",
        "maxContains",
        "maxItems",
        "minContains",
        "minItems",
        "uniqueItems",
    },
    "boolean": {"const", "enum"},
    "integer": {
        "const",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "maximum",
        "minimum",
        "multipleOf",
    },
    "null": {"const", "enum"},
    "number": {
        "const",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "maximum",
        "minimum",
        "multipleOf",
    },
    "object": {
        "const",
        "dependentRequired",
        "enum",
        "maxProperties",
        "minProperties",
        "propertyNames",
        "required",
    },
    "string": {"const", "enum", "maxLength", "minLength", "pattern"},
}
_V1_TOP_LEVEL = {
    "version",
    "model_id",
    "expert_id",
    "name",
    "creator_node_id",
    "created_at",
    "artifact_hash",
    "artifact",
    "supported_task_categories",
    "runtime_requirements",
    "lineage",
    "validation",
    "contribution_attribution",
}
_V2_TOP_LEVEL = _V1_TOP_LEVEL | {"manifest_id", "capabilities", "input_schema_ref"}
_V3_TOP_LEVEL = _V2_TOP_LEVEL | {"output_schema_ref"}
_V4_TOP_LEVEL = _V3_TOP_LEVEL | {
    "author",
    "owner",
    "attribution_notes",
}
_V5_TOP_LEVEL = _V4_TOP_LEVEL | {"training_lineage"}
_V6_TOP_LEVEL = _V5_TOP_LEVEL | {"validation_history"}
_V7_TOP_LEVEL = _V6_TOP_LEVEL | {"external_artifacts", "license"}


class ExpertManifestError(ValueError):
    """Raised when a local expert manifest is incomplete or malformed."""


def load_expert_manifest(path: str | Path) -> dict[str, Any]:
    """Load and structurally validate one hand-authored expert manifest."""
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise ExpertManifestError("could not read expert manifest") from exc
    except UnicodeDecodeError as exc:
        raise ExpertManifestError(
            "expert manifest JSON is malformed: invalid UTF-8"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ExpertManifestError(
            f"expert manifest JSON is malformed: {exc.msg}"
        ) from exc
    validate_expert_manifest(document)
    _input_schema_ref(document, Path(path).parent)
    _output_schema_ref(document, Path(path).parent)
    return cast(dict[str, Any], document)


def validate_expert_manifest(document: object) -> None:
    """Validate required manifest fields without claiming network trust or capability."""
    document = _top_level_object(document)
    if document["version"] >= 2:
        _string(document["manifest_id"], "manifest_id")
        _reference(document["input_schema_ref"], "input_schema_ref")
    if document["version"] >= 3:
        _reference(document["output_schema_ref"], "output_schema_ref")
    _identity(document)
    _creator_node_id(document["creator_node_id"])
    _string(document["created_at"], "created_at")
    if document["version"] >= 4:
        for field in ("author", "owner", "attribution_notes"):
            _text(document[field], field)
    if document["version"] >= 5:
        _training_lineage(document["training_lineage"])
    if document["version"] >= 6:
        _validation_history(document["validation_history"])
    if document["version"] >= 7:
        _license(document)
    _text(document["name"], "name")
    _timestamp(document["created_at"], "created_at")
    _artifact(document["artifact"])
    _artifact_hash(document)
    _strings(document["supported_task_categories"], "supported_task_categories", True)
    _strings(document["runtime_requirements"], "runtime_requirements", True)
    if document["version"] >= 2:
        _capabilities(document["capabilities"])
    _lineage(document["lineage"])
    _validation(document["validation"])
    _attribution(document["contribution_attribution"], document["creator_node_id"])
    _consistent_validation_attribution(document)


def expert_is_usable(path: str | Path) -> bool:
    """Require passed validation plus matching local artifact and receipt evidence."""
    manifest_path = Path(path)
    document = load_expert_manifest(manifest_path)
    validation = _object(document["validation"], _VALIDATION, "validation")
    if validation["status"] != "passed":
        return False
    artifact = _object(document["artifact"], {"reference", "sha256"}, "artifact")
    try:
        artifact_path = _local_reference_path(
            manifest_path.parent, str(artifact["reference"])
        )
        receipt_path = _local_reference_path(
            manifest_path.parent, str(validation["receipt_path"])
        )
        return (
            artifact_path.is_file()
            and _sha256(artifact_path) == artifact["sha256"]
            and _receipt_matches_manifest(receipt_path, document)
        )
    except OSError:
        return False


def validate_expert_output(path: str | Path, output: object) -> None:
    """Validate one local expert output against its declared version 3 contract."""
    manifest_path = Path(path)
    document = load_expert_manifest(manifest_path)
    if document["version"] < 3:
        raise ExpertManifestError(
            "expert manifest does not declare an output_schema_ref"
        )
    schema = _output_schema_ref(document, manifest_path.parent)
    try:
        Draft202012Validator(cast(Any, schema)).validate(output)
    except (ValidationError, Unresolvable) as exc:
        raise ExpertManifestError("output does not satisfy output_schema_ref") from exc


def _receipt_matches_manifest(path: Path, document: dict[str, Any]) -> bool:
    """Require receipt evidence to bind the manifest and validation result."""
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(receipt, dict):
        return False
    validation = cast(dict[str, Any], document["validation"])
    return receipt == {
        "receipt_version": RECEIPT_VERSION,
        "name": document["name"],
        **(
            {"manifest_id": document["manifest_id"]} if document["version"] >= 2 else {}
        ),
        **{field: document[field] for field in _identity_fields(document)},
        "creator_node_id": document["creator_node_id"],
        "created_at": document["created_at"],
        **(
            {
                "author": document["author"],
                "owner": document["owner"],
                "attribution_notes": document["attribution_notes"],
            }
            if document["version"] >= 4
            else {}
        ),
        "artifact_hash": document["artifact_hash"],
        **(
            {"input_schema_ref": document["input_schema_ref"]}
            if document["version"] >= 2
            else {}
        ),
        **(
            {"output_schema_ref": document["output_schema_ref"]}
            if document["version"] >= 3
            else {}
        ),
        **(
            {
                "lineage": document["lineage"],
                "contribution_attribution": document["contribution_attribution"],
            }
            if document["version"] >= 3
            else {}
        ),
        **(
            {"training_lineage": document["training_lineage"]}
            if document["version"] >= 5
            else {}
        ),
        **(
            {"validation_history": document["validation_history"]}
            if document["version"] >= 6
            else {}
        ),
        **(
            {
                "external_artifacts": document["external_artifacts"],
                "license": document["license"],
            }
            if document["version"] >= 7 and document["external_artifacts"]
            else {}
        ),
        "validated_at": validation["last_validated_at"],
        "validator_node_id": validation["validator_node_id"],
        "status": validation["status"],
    }


def _object(value: object, keys: set[str], context: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        listed = ", ".join(sorted(keys))
        raise ExpertManifestError(f"{context} must contain exactly: {listed}")
    return cast(dict[str, Any], value)


def _top_level_object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ExpertManifestError("expert manifest must be a JSON object")
    if "version" not in value:
        raise ExpertManifestError(
            "expert manifest is missing required field(s): version"
        )
    _manifest_version(value["version"])
    allowed = (
        _V7_TOP_LEVEL
        if value["version"] == 7
        else _V6_TOP_LEVEL
        if value["version"] == 6
        else _V5_TOP_LEVEL
        if value["version"] == 5
        else _V4_TOP_LEVEL
        if value["version"] == 4
        else _V3_TOP_LEVEL
        if value["version"] == 3
        else _V2_TOP_LEVEL
        if value["version"] == 2
        else _V1_TOP_LEVEL
    )
    required = allowed - {"model_id", "expert_id", "license"}
    fields = set(value)
    missing = required - fields
    if missing:
        raise ExpertManifestError(
            "expert manifest is missing required field(s): "
            + ", ".join(sorted(missing))
        )
    if not fields <= allowed:
        listed = ", ".join(sorted(allowed))
        raise ExpertManifestError(
            f"expert manifest must contain exactly these allowed fields: {listed}"
        )
    return cast(dict[str, Any], value)


def _identity(document: dict[str, Any]) -> None:
    fields = _identity_fields(document)
    if not any(
        isinstance(document[field], str) and document[field].strip() for field in fields
    ):
        raise ExpertManifestError(
            "expert manifest requires at least one non-empty model_id or expert_id"
        )
    for field in fields:
        _string(document[field], field)


def _identity_fields(document: dict[str, Any]) -> set[str]:
    return {field for field in ("model_id", "expert_id") if field in document}


def _manifest_version(value: object) -> None:
    if type(value) is not int or value not in {
        1,
        2,
        3,
        4,
        5,
        6,
        MANIFEST_SCHEMA_VERSION,
    }:
        raise ExpertManifestError("version must be 1, 2, 3, 4, 5, 6, or 7")


def _string(value: object, context: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(char.isspace() for char in value)
    ):
        raise ExpertManifestError(
            f"{context} must be a non-empty whitespace-free string"
        )


def _creator_node_id(value: object) -> None:
    """Require a concrete local creator identity without constraining its future format."""
    _string(value, "creator_node_id")
    if _PLACEHOLDER_CREATOR_NODE_ID.fullmatch(cast(str, value)):
        raise ExpertManifestError(
            "creator_node_id must name a concrete creator node, not a placeholder"
        )


def _text(value: object, context: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ExpertManifestError(f"{context} must be a non-empty string")


def _timestamp(value: object, context: str) -> None:
    _string(value, context)
    timestamp = cast(str, value)
    if not _TIMESTAMP.fullmatch(timestamp):
        raise ExpertManifestError(f"{context} must be a UTC timestamp ending in Z")
    try:
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ExpertManifestError(
            f"{context} must be a UTC timestamp ending in Z"
        ) from exc


def _reference(value: object, context: str) -> None:
    _string(value, context)
    reference = cast(str, value)
    if (
        not _SAFE_REFERENCE.fullmatch(reference)
        or "/../" in f"/{reference}"
        or reference.startswith("/")
    ):
        raise ExpertManifestError(f"{context} must be a safe local relative reference")


def _strings(value: object, context: str, required: bool = False) -> None:
    if not isinstance(value, list) or (required and not value):
        raise ExpertManifestError(
            f"{context} must be a non-empty list of non-empty strings"
        )
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ExpertManifestError(f"{context} must be a list of non-empty strings")


def _nullable_reference(value: object, context: str) -> None:
    if value is not None:
        _reference(value, context)


def _artifact(value: object) -> None:
    value = _object(value, {"reference", "sha256"}, "artifact")
    _reference(value["reference"], "artifact.reference")
    if value["sha256"] is not None and (
        not isinstance(value["sha256"], str) or not _HASH.fullmatch(value["sha256"])
    ):
        raise ExpertManifestError(
            "artifact.sha256 must be null or a lowercase sha256 content hash"
        )


def deterministic_non_model_artifact_placeholder(document: dict[str, Any]) -> str:
    """Return the reproducible identity for an early expert without an artifact."""
    artifact = cast(dict[str, Any], document["artifact"])
    inputs = {
        "artifact_reference": artifact["reference"],
        "creator_node_id": document["creator_node_id"],
        "expert_id": document.get("expert_id", ""),
        "model_id": document.get("model_id", ""),
        "version": document["version"],
    }
    encoded = json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "placeholder:sha256:" + hashlib.sha256(encoded).hexdigest()


def _artifact_hash(document: dict[str, Any]) -> None:
    artifact_hash = document["artifact_hash"]
    artifact = cast(dict[str, Any], document["artifact"])
    if isinstance(artifact_hash, str) and _HASH.fullmatch(artifact_hash):
        if artifact_hash != artifact["sha256"]:
            raise ExpertManifestError(
                "artifact_hash must match artifact.sha256 for a concrete artifact"
            )
        return
    expected = deterministic_non_model_artifact_placeholder(document)
    if (
        "model_id" in document
        or artifact["sha256"] is not None
        or artifact_hash != expected
        or not _PLACEHOLDER_HASH.fullmatch(str(artifact_hash))
    ):
        raise ExpertManifestError(
            "artifact_hash must be a concrete sha256 hash or a deterministic "
            "non-model expert placeholder"
        )


def _input_schema_ref(document: dict[str, Any], manifest_root: Path) -> None:
    """Require a readable local JSON Schema for each version 2 expert input."""
    if document["version"] == 1:
        return
    reference = document["input_schema_ref"]
    try:
        contents = _local_reference_path(manifest_root, cast(str, reference)).read_text(
            encoding="utf-8"
        )
    except (OSError, UnicodeDecodeError) as exc:
        raise ExpertManifestError(
            "input_schema_ref must name a readable local JSON Schema"
        ) from exc
    try:
        schema = json.loads(contents)
    except json.JSONDecodeError as exc:
        raise ExpertManifestError(
            "input_schema_ref must contain readable JSON"
        ) from exc
    if not isinstance(schema, dict) or schema.get("$schema") != (
        "https://json-schema.org/draft/2020-12/schema"
    ):
        raise ExpertManifestError(
            "input_schema_ref must point to a JSON Schema draft 2020-12 file"
        )
    if schema.get("type") != "object" or not isinstance(schema.get("properties"), dict):
        raise ExpertManifestError(
            "input_schema_ref schema must describe an object with properties"
        )
    required = schema.get("required")
    if (
        not isinstance(required, list)
        or not required
        or any(not isinstance(field, str) or not field for field in required)
        or len(required) != len(set(required))
    ):
        raise ExpertManifestError(
            "input_schema_ref schema must list unique required input fields"
        )
    properties = cast(dict[str, Any], schema["properties"])
    if any(field not in properties for field in required) or any(
        not isinstance(properties[field], dict)
        or not isinstance(properties[field].get("type"), str)
        or properties[field].get("type") not in _JSON_SCHEMA_TYPES
        for field in required
    ):
        raise ExpertManifestError(
            "input_schema_ref schema must declare accepted types for required input fields"
        )
    if schema.get("additionalProperties") is not False or not any(
        any(
            key
            in _JSON_SCHEMA_CONSTRAINTS_BY_TYPE[
                cast(str, cast(dict[str, Any], properties[field])["type"])
            ]
            for key in cast(dict[str, Any], properties[field])
        )
        for field in required
    ):
        raise ExpertManifestError(
            "input_schema_ref schema must declare input validation constraints"
        )
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ExpertManifestError(
            "input_schema_ref must contain a valid JSON Schema draft 2020-12 schema"
        ) from exc


def _output_schema_ref(
    document: dict[str, Any], manifest_root: Path
) -> dict[str, Any] | None:
    """Require a readable local JSON Schema for each version 3 expert output."""
    if document["version"] < 3:
        return None
    reference = document["output_schema_ref"]
    try:
        contents = _local_reference_path(manifest_root, cast(str, reference)).read_text(
            encoding="utf-8"
        )
    except (OSError, UnicodeDecodeError) as exc:
        raise ExpertManifestError(
            "output_schema_ref must name a readable local JSON Schema"
        ) from exc
    try:
        schema = json.loads(contents)
    except json.JSONDecodeError as exc:
        raise ExpertManifestError(
            "output_schema_ref must contain readable JSON"
        ) from exc
    if not isinstance(schema, dict) or schema.get("$schema") != (
        "https://json-schema.org/draft/2020-12/schema"
    ):
        raise ExpertManifestError(
            "output_schema_ref must point to a JSON Schema draft 2020-12 file"
        )
    schema_type = schema.get("type")
    if not isinstance(schema_type, str) or schema_type not in _JSON_SCHEMA_TYPES:
        raise ExpertManifestError(
            "output_schema_ref schema must declare a JSON output type"
        )
    _require_local_schema_refs(schema)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ExpertManifestError(
            "output_schema_ref must contain a valid JSON Schema draft 2020-12 schema"
        ) from exc
    return cast(dict[str, Any], schema)


def _require_local_schema_refs(value: object) -> None:
    """Reject schema references that could require non-local resolution."""
    if isinstance(value, dict):
        for key, nested in value.items():
            if (
                key in {"$ref", "$dynamicRef"}
                and isinstance(nested, str)
                and not nested.startswith("#")
            ):
                raise ExpertManifestError(
                    "output_schema_ref schema references must be local fragments"
                )
            _require_local_schema_refs(nested)
    elif isinstance(value, list):
        for nested in value:
            _require_local_schema_refs(nested)


def _lineage(value: object) -> None:
    value = _object(value, _LINEAGE, "lineage")
    _string(value["source_model"], "lineage.source_model")
    _nullable_reference(value["adapter_ref"], "lineage.adapter_ref")
    _nullable_reference(value["prompt_template_ref"], "lineage.prompt_template_ref")
    if not isinstance(value["local_changes"], str):
        raise ExpertManifestError("lineage.local_changes must be a string")
    _strings(value["parent_manifest_ids"], "lineage.parent_manifest_ids")
    _strings(value["derived_artifact_refs"], "lineage.derived_artifact_refs")
    for item in value["derived_artifact_refs"]:
        _reference(item, "lineage.derived_artifact_refs item")


_VALIDATION = {
    "test_command",
    "expected_inputs_ref",
    "receipt_path",
    "last_validated_at",
    "status",
    "validator_node_id",
}
_CAPABILITY = {
    "capability_id",
    "name",
    "description",
    "input_modality",
    "output_modality",
    "supported_task_type",
    "local_constraints",
    "known_limitations",
    "validation",
}
_CAPABILITY_VALIDATION = {
    "status",
    "local_test_name",
    "validation_receipt_id",
    "validated_at",
    "result_summary",
}
_LINEAGE = {
    "source_model",
    "adapter_ref",
    "prompt_template_ref",
    "local_changes",
    "parent_manifest_ids",
    "derived_artifact_refs",
}
_TRAINING_LINEAGE_ENTRY = {"kind", "reference", "sha256"}
_EXTERNAL_ARTIFACT = {"kind", "reference"}
_EXTERNAL_ARTIFACT_KINDS = {
    "adapter",
    "checkpoint",
    "dataset-derived-artifact",
    "expert-code",
    "model",
    "prompt-pack",
}


def _training_lineage(value: object) -> None:
    """Accept declared, local training inputs without inventing unknown provenance."""
    if not isinstance(value, list):
        raise ExpertManifestError("training_lineage must be a list")
    for index, entry in enumerate(value):
        context = f"training_lineage[{index}]"
        entry = _object(entry, _TRAINING_LINEAGE_ENTRY, context)
        _text(entry["kind"], f"{context}.kind")
        _reference(entry["reference"], f"{context}.reference")
        if not isinstance(entry["sha256"], str) or not _HASH.fullmatch(entry["sha256"]):
            raise ExpertManifestError(
                f"{context}.sha256 must be a lowercase sha256 content hash"
            )


def _validation_history(value: object) -> None:
    """Reserve an evidence-only append point without treating it as network trust."""
    if not isinstance(value, list):
        raise ExpertManifestError("validation_history must be a list")


def _license(document: dict[str, Any]) -> None:
    """Require a declared license for explicitly external artifact inputs only."""
    artifacts = document["external_artifacts"]
    if not isinstance(artifacts, list):
        raise ExpertManifestError("external_artifacts must be a list")
    for index, artifact in enumerate(artifacts):
        context = f"external_artifacts[{index}]"
        artifact = _object(artifact, _EXTERNAL_ARTIFACT, context)
        if artifact["kind"] not in _EXTERNAL_ARTIFACT_KINDS:
            choices = ", ".join(sorted(_EXTERNAL_ARTIFACT_KINDS))
            raise ExpertManifestError(f"{context}.kind must be one of: {choices}")
        _text(artifact["reference"], f"{context}.reference")
    has_license = "license" in document
    if artifacts and not has_license:
        raise ExpertManifestError(
            "license is required when external_artifacts are declared"
        )
    if not artifacts and has_license:
        raise ExpertManifestError(
            "license must be omitted when no external_artifacts are declared"
        )
    if has_license:
        _text(document["license"], "license")


def _capabilities(value: object) -> None:
    if not isinstance(value, list) or not value:
        raise ExpertManifestError("capabilities must be a non-empty list")
    seen_ids: set[str] = set()
    for index, capability in enumerate(value):
        context = f"capabilities[{index}]"
        capability = _object(capability, _CAPABILITY, context)
        capability_id = capability["capability_id"]
        _string(capability_id, f"{context}.capability_id")
        if capability_id in seen_ids:
            raise ExpertManifestError("capability_id values must be unique")
        seen_ids.add(cast(str, capability_id))
        for field in (
            "name",
            "description",
            "input_modality",
            "output_modality",
            "supported_task_type",
        ):
            _text(capability[field], f"{context}.{field}")
        _strings(capability["local_constraints"], f"{context}.local_constraints")
        _strings(capability["known_limitations"], f"{context}.known_limitations")
        _capability_validation(capability["validation"], context)


def _capability_validation(value: object, capability_context: str) -> None:
    context = f"{capability_context}.validation"
    validation = _object(value, _CAPABILITY_VALIDATION, context)
    status = validation["status"]
    if not isinstance(status, str) or status not in _STATUSES:
        raise ExpertManifestError(
            f"{context}.status must be unvalidated, passed, or failed"
        )
    evidence_fields = _CAPABILITY_VALIDATION - {"status"}
    if status == "unvalidated":
        if any(validation[field] is not None for field in evidence_fields):
            raise ExpertManifestError(
                f"{context} must not claim validation evidence when unvalidated"
            )
        return
    _text(validation["local_test_name"], f"{context}.local_test_name")
    _string(validation["validation_receipt_id"], f"{context}.validation_receipt_id")
    _text(validation["result_summary"], f"{context}.result_summary")
    _timestamp(validation["validated_at"], f"{context}.validated_at")


def _validation(value: object) -> None:
    value = _object(value, _VALIDATION, "validation")
    if not isinstance(value["status"], str) or value["status"] not in _STATUSES:
        raise ExpertManifestError(
            "validation.status must be unvalidated, passed, or failed"
        )
    for field in ("expected_inputs_ref", "receipt_path"):
        _nullable_reference(value[field], f"validation.{field}")
    for field in ("test_command", "validator_node_id"):
        if value[field] is not None:
            _text(value[field], f"validation.{field}")
    if value["last_validated_at"] is not None:
        _timestamp(value["last_validated_at"], "validation.last_validated_at")
    if value["status"] == "passed" and any(
        value[field] is None for field in _VALIDATION - {"status"}
    ):
        raise ExpertManifestError("passed validation requires complete local evidence")


def _attribution(value: object, creator_node_id: object) -> None:
    value = _object(
        value,
        {"creator_node_id", "modifier_node_ids", "validator_node_id", "receipt_refs"},
        "contribution_attribution",
    )
    if value["creator_node_id"] != creator_node_id:
        raise ExpertManifestError(
            "contribution_attribution.creator_node_id must match creator_node_id"
        )
    _strings(value["modifier_node_ids"], "contribution_attribution.modifier_node_ids")
    if value["validator_node_id"] is not None:
        _string(
            value["validator_node_id"], "contribution_attribution.validator_node_id"
        )
    _strings(value["receipt_refs"], "contribution_attribution.receipt_refs")
    for item in value["receipt_refs"]:
        _reference(item, "contribution_attribution.receipt_refs item")


def _consistent_validation_attribution(document: dict[str, Any]) -> None:
    validation = cast(dict[str, Any], document["validation"])
    attribution = cast(dict[str, Any], document["contribution_attribution"])
    if attribution["validator_node_id"] != validation["validator_node_id"]:
        raise ExpertManifestError(
            "contribution validator_node_id must match validation.validator_node_id"
        )
    receipt_path = validation["receipt_path"]
    if receipt_path is not None and receipt_path not in attribution["receipt_refs"]:
        raise ExpertManifestError(
            "validation.receipt_path must appear in contribution receipt_refs"
        )


def _local_reference_path(root: Path, reference: str) -> Path:
    resolved_root = root.resolve()
    path = (resolved_root / reference).resolve()
    if resolved_root != path and resolved_root not in path.parents:
        raise OSError("local reference escapes manifest directory")
    return path


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
