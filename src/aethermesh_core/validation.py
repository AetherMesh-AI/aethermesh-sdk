"""Local validation gate for reported AetherMesh job results."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from aethermesh_core.models import Job, JobResult
from aethermesh_core.runner import (
    build_basic_compute_output,
    build_hash_output,
    build_keyword_extract_output,
    build_schema_transform_output,
    build_text_chunk_output,
    build_text_embed_output,
    build_text_retrieve_output,
    build_text_stats_output,
)


@dataclass(frozen=True)
class ValidationResult:
    """Deterministic outcome from validating one reported job result."""

    job_id: str
    result_job_id: str
    valid: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the validation result into a JSON-compatible dictionary."""

        return asdict(self)


@dataclass(frozen=True)
class _SchemaNode:
    """One node in a declared, local output schema."""

    type_name: str
    accepted_types: tuple[type[object], ...]
    fields: dict[str, _SchemaNode] | None = None
    items: _SchemaNode | None = None


_STRING = _SchemaNode("str", (str,))
_INTEGER = _SchemaNode("int", (int,))
_NUMBER = _SchemaNode("number", (int, float))
_BOOLEAN = _SchemaNode("bool", (bool,))


def _object(**fields: _SchemaNode) -> _SchemaNode:
    return _SchemaNode("object", (dict,), fields=fields)


def _array(items: _SchemaNode) -> _SchemaNode:
    return _SchemaNode("array", (list,), items=items)


_OUTPUT_SCHEMAS_V1 = {
    "echo": _STRING,
    "hash": _object(algorithm=_STRING, digest=_STRING),
    "basic_compute": _object(operation=_STRING, result=_NUMBER),
    "text_stats": _object(
        character_count=_INTEGER,
        word_count=_INTEGER,
        line_count=_INTEGER,
        normalized_preview=_STRING,
    ),
    "keyword_extract": _object(
        keywords=_array(_object(term=_STRING, count=_INTEGER)),
        unique_terms=_INTEGER,
        total_terms=_INTEGER,
    ),
    "text_chunk": _object(
        chunks=_array(_object(index=_INTEGER, text=_STRING, character_count=_INTEGER)),
        chunk_count=_INTEGER,
        character_count=_INTEGER,
    ),
    "text_embed": _object(
        dimensions=_INTEGER,
        token_count=_INTEGER,
        unique_terms=_INTEGER,
        vector=_array(_INTEGER),
    ),
    "text_retrieve": _object(
        query_terms=_array(_STRING),
        matches=_array(
            _object(
                id=_STRING,
                score=_NUMBER,
                matched_term_count=_INTEGER,
                matched_terms=_array(_STRING),
            )
        ),
    ),
}


def validate_job_result(
    job: Job, result: JobResult, *, expected_node_id: str | None = None
) -> ValidationResult:
    """Validate a reported result against the assigned local job.

    Each supported work type has an explicit output schema version 1. The
    schema gate runs before semantic value validation, so malformed, partial,
    or unexpected output is rejected with a stable schema path. This is fully
    local and deterministic.
    """

    if job.job_type not in {
        "echo",
        "hash",
        "basic_compute",
        "schema_transform",
        "keyword_extract",
        "text_chunk",
        "text_embed",
        "text_retrieve",
        "text_stats",
    }:
        return _invalid(job, result, "unsupported_job_type")
    if result.status != "completed":
        return _invalid(job, result, "result_not_completed")
    if result.job_id != job.job_id:
        return _invalid(job, result, "job_id_mismatch")
    if expected_node_id is not None and result.node_id != expected_node_id:
        return _invalid(job, result, "result_node_id_mismatch")
    if result.contribution_units != 1:
        return _invalid(job, result, "unexpected_contribution_units")

    expected_output: object
    if job.job_type == "echo":
        if "message" not in job.payload or not isinstance(job.payload["message"], str):
            return _invalid(job, result, "missing_payload_message")
        expected_output = job.payload["message"]
    elif job.job_type in {"hash", "basic_compute", "schema_transform"}:
        builders = {
            "hash": build_hash_output,
            "basic_compute": build_basic_compute_output,
            "schema_transform": build_schema_transform_output,
        }
        try:
            expected_output = builders[job.job_type](job.payload)
        except ValueError:
            return _invalid(job, result, f"malformed_{job.job_type}_payload")
    elif job.job_type == "text_stats":
        if "text" not in job.payload or not isinstance(job.payload["text"], str):
            return _invalid(job, result, "missing_payload_text")
        expected_output = build_text_stats_output(job.payload["text"])
    elif job.job_type == "keyword_extract":
        try:
            expected_output = build_keyword_extract_output(job.payload)
        except ValueError:
            return _invalid(job, result, "malformed_keyword_extract_payload")
    elif job.job_type == "text_chunk":
        try:
            expected_output = build_text_chunk_output(job.payload)
        except ValueError:
            return _invalid(job, result, "malformed_text_chunk_payload")
    elif job.job_type == "text_embed":
        try:
            expected_output = build_text_embed_output(job.payload)
        except ValueError:
            return _invalid(job, result, "malformed_text_embed_payload")
    else:  # text_retrieve is the remaining supported job type.
        try:
            expected_output = build_text_retrieve_output(job.payload)
        except ValueError:
            return _invalid(job, result, "malformed_text_retrieve_payload")

    schema = _declared_output_schema(job)
    if schema_error := _validate_declared_output_schema(
        job.job_type, schema, result.output
    ):
        return _invalid(job, result, schema_error)
    if result.output != expected_output:
        return _invalid(job, result, "output_mismatch")
    return ValidationResult(job.job_id, result.job_id, True, "ok")


def _declared_output_schema(job: Job) -> _SchemaNode:
    """Return the version 1 output schema declared for a supported work item."""

    if job.job_type != "schema_transform":
        return _OUTPUT_SCHEMAS_V1[job.job_type]

    declared_fields = job.payload["schema"]["fields"]
    primitive_schemas = {
        "string": _STRING,
        "integer": _INTEGER,
        "boolean": _BOOLEAN,
    }
    return _object(
        **{
            name: primitive_schemas[field_type]
            for name, field_type in declared_fields.items()
        }
    )


def _validate_declared_output_schema(
    job_type: str, schema: _SchemaNode, actual: object, path: str = "output"
) -> str | None:
    """Validate an output against a work item's declared version 1 schema.

    Version 1 explicitly declares required fields and array item types and has
    no optional fields. Semantic equality is checked separately to distinguish
    a correctly-shaped but wrong output from a schema failure.
    """

    schema_path = f"output_schema.v1.{job_type}.{path}"
    if schema.fields is not None:
        if not isinstance(actual, dict):
            return f"{schema_path}: expected object"
        expected_fields = set(schema.fields)
        actual_fields = set(actual)
        missing = sorted(expected_fields - actual_fields)
        if missing:
            return f"{schema_path}: missing required field {missing[0]}"
        unknown = sorted(actual_fields - expected_fields)
        if unknown:
            return f"{schema_path}: unknown field {unknown[0]}"
        for field in sorted(expected_fields):
            if reason := _validate_declared_output_schema(
                job_type, schema.fields[field], actual[field], f"{path}.{field}"
            ):
                return reason
        return None
    if schema.items is not None:
        if not isinstance(actual, list):
            return f"{schema_path}: expected array"
        for index, value in enumerate(actual):
            if reason := _validate_declared_output_schema(
                job_type, schema.items, value, f"{path}[{index}]"
            ):
                return reason
        return None
    valid = isinstance(actual, schema.accepted_types)
    if schema.type_name in {"int", "number"} and isinstance(actual, bool):
        valid = False
    if not valid:
        return f"{schema_path}: expected {schema.type_name}"
    return None


def _invalid(job: Job, result: JobResult, reason: str) -> ValidationResult:
    return ValidationResult(
        job_id=job.job_id,
        result_job_id=result.job_id,
        valid=False,
        reason=reason,
    )
