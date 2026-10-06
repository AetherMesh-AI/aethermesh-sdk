"""Canonical SHA-256 hashing for accounted and durable local job results."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from aethermesh_core.job_result_schema import validate_job_result_document
from aethermesh_core.models import JobResult

RESULT_HASH_ALGORITHM = "sha256"
RESULT_HASH_FIELDS = ("job_id", "node_id", "status", "output", "error")


def result_hash(result: JobResult) -> str:
    """Return the canonical lowercase SHA-256 digest for a ``JobResult``.

    The hash intentionally covers only the stable result identity/content fields
    that describe what a node reported and what was accounted. Contribution
    units, validation details, timestamps, message ids, receipt ids, and paths
    are intentionally excluded so the same result content has the same proof
    across local artifacts.
    """

    return result_hash_from_fields(
        job_id=result.job_id,
        node_id=result.node_id,
        status=result.status,
        output=result.output,
        error=result.error,
    )


def result_hash_from_fields(
    *,
    job_id: str,
    node_id: str,
    status: str,
    output: Any,
    error: str | None,
) -> str:
    """Hash the canonical result fields used by result messages and audits."""

    canonical = {
        "job_id": job_id,
        "node_id": node_id,
        "status": status,
        "output": output,
        "error": error,
    }
    return hashlib.sha256(_canonical_json_bytes(canonical)).hexdigest()


def canonical_result_document_hash(document: object) -> str:
    """Hash a completed Phase 1 result record without runtime-only fields.

    This is separate from :func:`result_hash`, which preserves the legacy local
    flow receipt contract. It is the Phase 1 durable-result contract.
    """

    result = validate_job_result_document(document, verify_result_hash=False)
    if result["validation_status"] not in {"passed", "failed", "error"}:
        raise ValueError(
            "a result hash requires a passed, failed, or error validation receipt"
        )
    payload = {
        "schema_version": result["schema_version"],
        "result_id": result["result_id"],
        "job_id": result["job_id"],
        "task_id": result["task_id"],
        "capability": result["capability"],
        "model_expert_id": result["model_expert_id"],
        "expert_version": result["expert_version"],
        "creator_node_id": result["creator_node_id"],
        "executor_node_id": result["executor_node_id"],
        "manifest_ref": result["manifest_id"],
        "output_payload": _portable_output_payload(result["output_payload"]),
        "references": _portable_references(result["references"]),
        "result_content": {
            "status": result["status"],
            "exit_code": result["exit_code"],
            "summary": result["summary"],
            "error_summary": result["error_summary"],
            "failure_reasons": result["failure_reasons"],
        },
        "validation": {
            "status": result["validation_status"],
            "receipt_id": result["validation_receipt_id"],
            "validator_node_id": result["validator_node_id"],
        },
        "lineage": result["lineage"],
        "contribution": result["contribution"],
    }
    return "sha256:" + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def result_hash_manifest(document: object) -> dict[str, str]:
    """Return the explicit-algorithm manifest stored with a durable result."""

    return {
        "algorithm": RESULT_HASH_ALGORITHM,
        "result_hash": canonical_result_document_hash(document),
    }


def validate_validation_receipt_result_hash(
    receipt: object, expected_result_hash: str
) -> None:
    """Require a local validation receipt to identify its exact result hash."""

    if not isinstance(receipt, dict):
        raise ValueError("validation receipt must be an object")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    if receipt.get("result_hash") != expected_result_hash:
        raise ValueError("validation receipt result_hash does not match the result")


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    try:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("result hash fields must be JSON-compatible") from exc


def _portable_output_payload(output_payload: dict[str, Any]) -> dict[str, Any]:
    """Cover result content while excluding a machine-local storage path."""

    return {
        "inline_payload": output_payload["inline_payload"],
        "payload_digest": output_payload["payload_digest"],
    }


def _portable_references(references: dict[str, Any]) -> dict[str, Any]:
    """Exclude machine-local paths while retaining portable evidence identities."""

    return {
        "manifest_hash": references["manifest_hash"],
        "artifact_refs": [
            reference
            for reference in references["artifact_refs"]
            if reference.startswith("sha256:")
        ],
        "validation_receipt_ids": references["validation_receipt_ids"],
        "log_refs": [
            reference
            for reference in references["log_refs"]
            if reference.startswith("sha256:")
        ],
    }
