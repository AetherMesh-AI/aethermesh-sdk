"""Validation for the local-only version 5 contribution record contract."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

if os.name == "nt":  # pragma: no cover - justification: Windows-only import
    import msvcrt
else:  # pragma: no cover - justification: mutually exclusive platform import
    import fcntl

from aethermesh_core.job_result_schema import (
    JobResultSchemaError,
    validate_job_result_document,
)
from aethermesh_core.local_audit_event import append_local_audit_event
from aethermesh_core.local_json_helpers import canonical_json_hash
from aethermesh_core.validation_receipt_schema import (
    ValidationReceiptSchemaError,
    validate_validation_receipt_document,
    validation_receipt_id,
)

CONTRIBUTION_RECORD_SCHEMA_VERSION = 5
LOCAL_CONTRIBUTION_JOURNAL_ENTRY_VERSION = 1
VALIDATION_STATUSES = frozenset(
    {"unvalidated", "pending", "valid", "invalid", "superseded"}
)
AUTHOR_KINDS = frozenset({"human", "node"})
CREATION_MODES = frozenset({"manual", "automatic"})
PHASE_1_JOB_CAPABILITIES = {
    "echo": "work.echo",
    "hash": "work.hash",
    "basic_compute": "work.basic_compute",
    "schema_transform": "work.schema_transform",
    "keyword_extract": "work.keyword_extract",
    "text_chunk": "work.text_chunk",
    "text_embed": "work.text_embed",
    "text_stats": "work.text_stats",
}
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "record_id",
        "job_id",
        "validation_receipt_id",
        "result_hash_algorithm",
        "result_hash",
        "creator_node_id",
        "contributor_node_id",
        "created_at",
        "work_type",
        "job_type",
        "capability",
        "contribution_summary",
        "source",
        "manifest_links",
        "validation",
        "lineage",
        "attribution",
    }
)
_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9_.-]{2,127}\Z")
_LOCAL_JOB_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,127}\Z")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_URI_SCHEME = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*:")


class ContributionRecordError(ValueError):
    """Raised when a local contribution record violates its stable contract."""


def validate_contribution_record(document: object) -> dict[str, Any]:
    """Validate a local contribution record without implying network consensus."""
    if not isinstance(document, dict):
        raise ContributionRecordError("contribution record must be an object")
    _exact_fields(document, _TOP_LEVEL_FIELDS, "contribution record")
    _require_int(document, "schema_version", CONTRIBUTION_RECORD_SCHEMA_VERSION)
    for field in ("record_id", "creator_node_id", "contributor_node_id"):
        _require_identifier(document, field)
    _require_local_job_id(document, "job_id")
    if document["validation_receipt_id"] != validation_receipt_id(document["job_id"]):
        raise ContributionRecordError(
            "validation_receipt_id must match the local validation receipt for job_id"
        )
    if document["result_hash_algorithm"] != "sha256":
        raise ContributionRecordError("result_hash_algorithm must be sha256")
    _require_sha256(document, "result_hash")
    _require_timestamp(document, "created_at")
    _require_string(document, "work_type")
    _require_phase_1_job_capability(document)
    _require_string(document, "contribution_summary")
    _source(document["source"])
    _manifest_links(document["manifest_links"])
    _validation(document["validation"])
    _lineage(document["lineage"], document["contributor_node_id"])
    _attribution(document["attribution"])
    return document


def validate_local_contribution_record(
    document: object, local_root: Path
) -> dict[str, Any]:
    """Validate one contribution and its referenced receipt under a local root."""

    contribution = validate_contribution_record(document)
    receipt_ref = contribution["validation"]["validation_receipt_ref"]
    if receipt_ref is None:
        raise ContributionRecordError(
            "validation_receipt_ref is required for local evidence"
        )
    if contribution["manifest_links"]["validation_manifest_ref"] != receipt_ref:
        raise ContributionRecordError(
            "validation manifest reference must match validation_receipt_ref"
        )
    receipt_document = _read_local_json(local_root, receipt_ref, "validation receipt")
    try:
        receipt = validate_validation_receipt_document(receipt_document)
    except ValidationReceiptSchemaError as exc:
        raise ContributionRecordError("validation receipt file is invalid") from exc
    expected_values = {
        "receipt_id": contribution["validation_receipt_id"],
        "job_id": contribution["job_id"],
        "creator_node_id": contribution["creator_node_id"],
        "contributor_node_id": contribution["contributor_node_id"],
        "validator_id": contribution["validation"]["validator_node_id"],
    }
    for field, expected in expected_values.items():
        if receipt[field] != expected:
            raise ContributionRecordError(
                f"validation receipt {field} does not match contribution record"
            )
    if receipt["result_hash"] not in contribution["lineage"]["output_hashes"]:
        raise ContributionRecordError(
            "validation receipt result_hash is not preserved in contribution lineage"
        )
    if receipt["result_hash"] != contribution["result_hash"]:
        raise ContributionRecordError(
            "validation receipt result_hash does not match contribution record"
        )
    expected_status = "valid" if receipt["validation_status"] == "pass" else "invalid"
    if contribution["validation"]["status"] != expected_status:
        raise ContributionRecordError(
            "validation receipt status does not match contribution record"
        )
    if contribution["validation"]["failure_reason"] != receipt["rejection_reason"]:
        raise ContributionRecordError(
            "validation receipt rejection_reason does not match contribution record"
        )
    validated_at = contribution["validation"]["validated_at"]
    if validated_at is None:
        raise ContributionRecordError(
            "validated_at is required for local validation evidence"
        )
    contribution_time = datetime.fromisoformat(validated_at)
    receipt_time = datetime.fromisoformat(receipt["validated_at"])
    if contribution_time != receipt_time:
        raise ContributionRecordError(
            "validation receipt validated_at does not match contribution record"
        )
    work_manifest_ref = contribution["manifest_links"]["work_manifest_ref"]
    if work_manifest_ref is None:
        raise ContributionRecordError(
            "work_manifest_ref is required for local evidence"
        )
    work_manifest = _read_local_json(local_root, work_manifest_ref, "work manifest")
    if (
        not isinstance(work_manifest, dict)
        or work_manifest.get("job_id") != contribution["job_id"]
    ):
        raise ContributionRecordError(
            "work manifest job_id does not match contribution record"
        )
    if work_manifest.get("creator_node_id") != contribution["creator_node_id"]:
        raise ContributionRecordError(
            "work manifest creator_node_id does not match contribution record"
        )
    if work_manifest.get("job_type") != contribution["job_type"]:
        raise ContributionRecordError(
            "work manifest job_type does not match contribution record"
        )
    result_ref = contribution["source"]["local_source_path"]
    if result_ref is None:
        result_ref = contribution["source"]["artifact_ref"]
    result_document = _read_local_json(local_root, result_ref, "result payload")
    try:
        result = validate_job_result_document(result_document)
    except JobResultSchemaError as exc:
        raise ContributionRecordError(
            "result payload is invalid or its hash does not match its canonical payload"
        ) from exc
    result_evidence = {
        "result_hash": contribution["result_hash"],
        "job_id": contribution["job_id"],
        "creator_node_id": contribution["creator_node_id"],
        "executor_node_id": contribution["contributor_node_id"],
        "manifest_id": receipt["manifest_id"],
        "validation_receipt_id": contribution["validation_receipt_id"],
        "model_expert_id": receipt["model_expert_id"],
        "expert_version": receipt["expert_version"],
    }
    for field, expected in result_evidence.items():
        if result[field] != expected:
            raise ContributionRecordError(
                f"result payload {field} does not match contribution evidence"
            )
    return contribution


def record_validated_contribution(
    document: object,
    local_root: Path,
    journal_path: Path,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    audit_path: Path | None = None,
) -> dict[str, Any]:
    """Append receipt-backed attribution and one local audit event per attempt."""

    resolved_audit_path = audit_path or journal_path.with_name(
        "contribution-ledger-updates.jsonl"
    )
    rejected_evidence = False
    audit_manifest_id: str | None = None
    try:
        contribution = validate_local_contribution_record(document, local_root)
        receipt_ref = cast(str, contribution["validation"]["validation_receipt_ref"])
        receipt = validate_validation_receipt_document(
            _read_local_json(local_root, receipt_ref, "validation receipt")
        )
        audit_manifest_id = receipt["manifest_id"]
        if receipt["status"] != "accepted" or receipt["validation_status"] != "pass":
            rejected_evidence = True
            raise ContributionRecordError(
                "contribution recording requires an accepted passed validation receipt"
            )
        work_manifest_ref = cast(
            str, contribution["manifest_links"]["work_manifest_ref"]
        )
        work_manifest = cast(
            dict[str, Any],
            _read_local_json(local_root, work_manifest_ref, "work manifest"),
        )
        if (
            canonical_json_hash(work_manifest, prefix="sha256:")
            != receipt["manifest_id"]
        ):
            raise ContributionRecordError(
                "validation receipt manifest_id does not match work manifest"
            )

        with _contribution_journal_lock(journal_path):
            entries = _load_contribution_journal(journal_path)
            manifest_id = receipt["manifest_id"]
            for existing_entry in entries:
                if existing_entry["manifest_id"] == manifest_id:
                    _append_ledger_update_audit(
                        resolved_audit_path,
                        contribution,
                        manifest_id,
                        clock,
                        "already_recorded",
                    )
                    return existing_entry

            entry: dict[str, Any] = {
                "entry_version": LOCAL_CONTRIBUTION_JOURNAL_ENTRY_VERSION,
                "recorded_at": _utc_timestamp(clock()),
                "manifest_id": manifest_id,
                "creator_node_id": contribution["creator_node_id"],
                "contributor_node_id": contribution["contributor_node_id"],
                "validator_node_id": receipt["validator_id"],
                "validation_receipt_id": contribution["validation_receipt_id"],
                "lineage_parent_contribution_ids": contribution["lineage"][
                    "parent_contribution_ids"
                ],
                "contribution": contribution,
                "prior_entry_hash": entries[-1]["entry_hash"] if entries else None,
            }
            entry["entry_hash"] = _journal_entry_hash(entry)
            try:
                with journal_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(entry, sort_keys=True))
                    handle.write("\n")
            except OSError as exc:
                raise ContributionRecordError(
                    "contribution journal is unwritable"
                ) from exc
        _append_ledger_update_audit(
            resolved_audit_path, contribution, manifest_id, clock, "recorded"
        )
        return entry
    except ContributionRecordError:
        outcome = "rejected" if rejected_evidence else "validation_failed"
        _append_ledger_update_audit(
            resolved_audit_path, document, audit_manifest_id, clock, outcome
        )
        raise


def _append_ledger_update_audit(
    audit_path: Path,
    document: object,
    manifest_id: str | None,
    clock: Callable[[], datetime],
    outcome: str,
) -> None:
    """Append compact local evidence, never raw work payloads or credentials."""

    # Only copy attribution from a schema-valid record. This helper also handles
    # malformed input, whose arbitrary strings must not become durable audit data.
    try:
        contribution = validate_contribution_record(document)
    except ContributionRecordError:
        contribution = {}
    contributor = contribution.get("contributor_node_id")
    if not isinstance(contributor, str) or not contributor.strip():
        contributor = "local-ledger"
    creator = contribution.get("creator_node_id")
    if not isinstance(creator, str) or not creator.strip():
        creator = None
    update_identity = {
        field: value if isinstance(value, str) and value.strip() else None
        for field in ("record_id", "job_id", "validation_receipt_id")
        for value in (contribution.get(field),)
    }
    update_identity["outcome"] = outcome
    update_id = "ledger-update-" + canonical_json_hash(update_identity).removeprefix(
        "sha256:"
    )
    event: dict[str, Any] = {
        "schema_version": 2,
        "event_id": update_id,
        "timestamp": _utc_timestamp(clock()),
        "event_type": "contribution_record_updated",
        "actor_node_id": contributor,
        "creator_node_id": creator,
        "local_run_id": update_id,
        "event_sequence": 1,
        "validation_status": outcome,
    }
    if contribution:
        event.update(
            {
                "work_id": contribution["job_id"],
                "validation_receipt_id": contribution["validation_receipt_id"],
                "manifest_ref": contribution["manifest_links"]["work_manifest_ref"],
                "validation_receipt_ref": contribution["validation"][
                    "validation_receipt_ref"
                ],
                "lineage_parent_ids": contribution["lineage"][
                    "parent_contribution_ids"
                ],
                "contribution_attribution_ids": [contribution["record_id"]],
            }
        )
    if manifest_id is not None:
        event["manifest_id"] = manifest_id
    append_local_audit_event(audit_path, event)


def new_unvalidated_validation() -> dict[str, Any]:
    """Return the required initial validation state for a new contribution."""

    state = _validation_state_document("unvalidated", None, None, None, None)
    return {**state, "status_history": [state]}


def apply_local_validation_receipt(
    document: object, local_root: Path, validation_receipt_ref: str
) -> dict[str, Any]:
    """Append receipt-backed local validation without replacing contribution evidence."""

    contribution = validate_contribution_record(document)
    _optional_ref(
        {"validation_receipt_ref": validation_receipt_ref}, "validation_receipt_ref"
    )
    try:
        receipt = validate_validation_receipt_document(
            _read_local_json(local_root, validation_receipt_ref, "validation receipt")
        )
    except ValidationReceiptSchemaError as exc:
        raise ContributionRecordError("validation receipt file is invalid") from exc
    if receipt["receipt_id"] != contribution["validation_receipt_id"]:
        raise ContributionRecordError(
            "validation receipt does not match contribution record"
        )

    updated = deepcopy(contribution)
    state = _validation_state_document(
        "valid" if receipt["validation_status"] == "pass" else "invalid",
        receipt["validator_id"],
        validation_receipt_ref,
        _utc_timestamp(datetime.fromisoformat(receipt["validated_at"])),
        receipt["rejection_reason"],
    )
    updated["validation"] = {
        **state,
        "status_history": [*contribution["validation"]["status_history"], state],
    }
    updated["manifest_links"]["validation_manifest_ref"] = validation_receipt_ref
    return validate_local_contribution_record(updated, local_root)


@contextmanager
def _contribution_journal_lock(journal_path: Path) -> Iterator[None]:
    """Serialize journal validation, deduplication, and append across processes."""

    journal_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = journal_path.with_name(f".{journal_path.name}.lock")
    try:
        with lock_path.open("a+b") as lock_handle:
            if os.name == "nt":  # pragma: no cover - justification: Windows-only lock
                lock_handle.seek(0)
                if lock_handle.read(1) == b"":
                    lock_handle.write(b"\0")
                    lock_handle.flush()
                lock_handle.seek(0)
                # Non-Windows type stubs omit this Windows-only native API.
                cast(Any, msvcrt).locking(
                    lock_handle.fileno(),
                    cast(Any, msvcrt).LK_LOCK,
                    1,
                )
            else:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if (
                    os.name == "nt"
                ):  # pragma: no cover - justification: Windows-only unlock
                    lock_handle.seek(0)
                    # Non-Windows type stubs omit this Windows-only native API.
                    cast(Any, msvcrt).locking(
                        lock_handle.fileno(),
                        cast(Any, msvcrt).LK_UNLCK,
                        1,
                    )
                else:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    except OSError as exc:
        raise ContributionRecordError(
            "contribution journal lock is unavailable"
        ) from exc


def _load_contribution_journal(journal_path: Path) -> list[dict[str, Any]]:
    if not journal_path.exists():
        return []
    try:
        lines = journal_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ContributionRecordError("contribution journal is unreadable") from exc

    entries: list[dict[str, Any]] = []
    previous_hash: str | None = None
    for index, line in enumerate(lines, start=1):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContributionRecordError(
                f"contribution journal entry {index} is malformed"
            ) from exc
        if not isinstance(entry, dict) or entry.get("entry_version") != (
            LOCAL_CONTRIBUTION_JOURNAL_ENTRY_VERSION
        ):
            raise ContributionRecordError(
                f"contribution journal entry {index} is invalid"
            )
        if entry.get("prior_entry_hash") != previous_hash:
            raise ContributionRecordError(
                f"contribution journal entry {index} has a broken hash chain"
            )
        entry_hash = entry.get("entry_hash")
        if not isinstance(entry_hash, str) or entry_hash != _journal_entry_hash(entry):
            raise ContributionRecordError(
                f"contribution journal entry {index} has an invalid hash"
            )
        manifest_id = entry.get("manifest_id")
        if not isinstance(manifest_id, str) or not _SHA256.fullmatch(manifest_id):
            raise ContributionRecordError(
                f"contribution journal entry {index} has an invalid manifest_id"
            )
        entries.append(entry)
        previous_hash = entry_hash
    return entries


def _journal_entry_hash(entry: dict[str, Any]) -> str:
    return canonical_json_hash(
        {key: value for key, value in entry.items() if key != "entry_hash"},
        prefix="sha256:",
    )


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ContributionRecordError(
            "contribution record clock must be timezone-aware"
        )
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _exact_fields(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContributionRecordError(f"{label} must be an object")
    missing = sorted(fields - value.keys())
    if missing:
        raise ContributionRecordError(f"{label} missing: {', '.join(missing)}")
    unknown = sorted(value.keys() - fields)
    if unknown:
        raise ContributionRecordError(
            f"{label} contains unsupported fields: {', '.join(unknown)}"
        )
    return value


def _require_int(document: dict[str, Any], field: str, expected: int) -> None:
    if document.get(field) != expected or isinstance(document.get(field), bool):
        raise ContributionRecordError(f"{field} must be integer {expected}")


def _require_identifier(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ContributionRecordError(f"{field} must be a stable local identifier")
    return value


def _require_local_job_id(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not (
        _LOCAL_JOB_ID.fullmatch(value) or _SHA256.fullmatch(value)
    ):
        raise ContributionRecordError(
            f"{field} must be a local ID or sha256 content ID"
        )
    return value


def _require_string(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ContributionRecordError(f"{field} must be a non-empty string")
    return value


def _require_sha256(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ContributionRecordError(f"{field} must be a sha256 hash")
    return value


def _require_phase_1_job_capability(document: dict[str, Any]) -> None:
    job_type = _require_string(document, "job_type")
    expected_capability = PHASE_1_JOB_CAPABILITIES.get(job_type)
    if expected_capability is None:
        supported = ", ".join(sorted(PHASE_1_JOB_CAPABILITIES))
        raise ContributionRecordError(
            f"job_type must be one of the supported Phase 1 types: {supported}"
        )
    if document["work_type"] != job_type:
        raise ContributionRecordError("work_type must match job_type")
    capability = _require_string(document, "capability")
    if capability != expected_capability:
        raise ContributionRecordError(
            "capability must match the local manifest capability for job_type"
        )


def _require_timestamp(document: dict[str, Any], field: str) -> None:
    value = _require_string(document, field)
    if not _TIMESTAMP.fullmatch(value):
        raise ContributionRecordError(f"{field} must be an RFC 3339 UTC timestamp")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ContributionRecordError(
            f"{field} must be an RFC 3339 UTC timestamp"
        ) from exc


def _source(value: object) -> None:
    source = _exact_fields(
        value, frozenset({"local_source_path", "artifact_ref"}), "source"
    )
    _optional_ref(source, "local_source_path")
    _optional_ref(source, "artifact_ref")
    if source["local_source_path"] is None and source["artifact_ref"] is None:
        raise ContributionRecordError(
            "source requires local_source_path or artifact_ref"
        )


def _manifest_links(value: object) -> None:
    links = _exact_fields(
        value,
        frozenset(
            {
                "node_manifest_ref",
                "work_manifest_ref",
                "input_manifest_ref",
                "output_manifest_ref",
                "validation_manifest_ref",
            }
        ),
        "manifest_links",
    )
    for field in links:
        _optional_ref(links, field)


def _validation(value: object) -> None:
    validation = _exact_fields(
        value,
        frozenset(
            {
                "status",
                "validator_node_id",
                "validation_receipt_ref",
                "validated_at",
                "failure_reason",
                "status_history",
            }
        ),
        "validation",
    )
    state = _validation_state(
        {field: validation[field] for field in validation if field != "status_history"},
        "validation",
    )
    history = validation["status_history"]
    if not isinstance(history, list) or not history:
        raise ContributionRecordError(
            "validation.status_history must be a non-empty list"
        )
    states = [
        _validation_state(entry, "validation.status_history entry") for entry in history
    ]
    if states[0]["status"] != "unvalidated":
        raise ContributionRecordError(
            "validation.status_history must begin unvalidated"
        )
    if states[-1] != state:
        raise ContributionRecordError(
            "validation state must match its latest history entry"
        )


def _validation_state_document(
    status: str,
    validator_node_id: str | None,
    validation_receipt_ref: str | None,
    validated_at: str | None,
    failure_reason: str | None,
) -> dict[str, str | None]:
    return {
        "status": status,
        "validator_node_id": validator_node_id,
        "validation_receipt_ref": validation_receipt_ref,
        "validated_at": validated_at,
        "failure_reason": failure_reason,
    }


def _validation_state(value: object, label: str) -> dict[str, str | None]:
    state = _exact_fields(
        value,
        frozenset(
            {
                "status",
                "validator_node_id",
                "validation_receipt_ref",
                "validated_at",
                "failure_reason",
            }
        ),
        label,
    )
    status = _require_string(state, "status")
    if status not in VALIDATION_STATUSES:
        raise ContributionRecordError(f"{label}.status is not allowed")
    _optional_identifier(state, "validator_node_id")
    _optional_ref(state, "validation_receipt_ref")
    _optional_timestamp(state, "validated_at")
    _optional_string(state, "failure_reason")
    if status in {"invalid", "superseded"} and state["failure_reason"] is None:
        raise ContributionRecordError(
            "invalid or superseded validation requires failure_reason"
        )
    if status not in {"invalid", "superseded"} and state["failure_reason"] is not None:
        raise ContributionRecordError(
            "only invalid or superseded validation may include failure_reason"
        )
    if status != "unvalidated" and state["validation_receipt_ref"] is None:
        raise ContributionRecordError(
            "non-unvalidated validation requires a receipt reference"
        )
    if status == "unvalidated" and any(
        state[field] is not None
        for field in (
            "validator_node_id",
            "validation_receipt_ref",
            "validated_at",
            "failure_reason",
        )
    ):
        raise ContributionRecordError(
            "unvalidated validation cannot include validation evidence"
        )
    return cast(dict[str, str | None], state)


def _lineage(value: object, document_contributor_node_id: str) -> None:
    lineage = _exact_fields(
        value,
        frozenset(
            {
                "contributor_node_id",
                "parent_contribution_ids",
                "derived_artifact_ids",
                "input_hashes",
                "output_hashes",
                "deterministic_reproduction_notes",
            }
        ),
        "lineage",
    )
    for field in ("parent_contribution_ids", "derived_artifact_ids"):
        _identifier_list(lineage, field)
    _require_identifier(lineage, "contributor_node_id")
    for field in ("input_hashes", "output_hashes"):
        _sha256_list(lineage, field)
    _optional_string(lineage, "deterministic_reproduction_notes")
    if lineage["contributor_node_id"] != document_contributor_node_id:
        raise ContributionRecordError(
            "lineage.contributor_node_id must match contributor_node_id"
        )


def _attribution(value: object) -> None:
    attribution = _exact_fields(
        value,
        frozenset(
            {
                "author_id",
                "author_kind",
                "role",
                "declared_tool_or_runtime",
                "creation_mode",
            }
        ),
        "attribution",
    )
    _require_identifier(attribution, "author_id")
    if _require_string(attribution, "author_kind") not in AUTHOR_KINDS:
        raise ContributionRecordError("attribution.author_kind is not allowed")
    _require_string(attribution, "role")
    _optional_string(attribution, "declared_tool_or_runtime")
    if _require_string(attribution, "creation_mode") not in CREATION_MODES:
        raise ContributionRecordError("attribution.creation_mode is not allowed")


def _optional_identifier(document: dict[str, Any], field: str) -> None:
    if document.get(field) is not None:
        _require_identifier(document, field)


def _identifier_list(document: dict[str, Any], field: str) -> None:
    _matching_string_list(document, field, _IDENTIFIER, "a stable local identifier")


def _sha256_list(document: dict[str, Any], field: str) -> None:
    _matching_string_list(document, field, _SHA256, "a lowercase SHA-256 digest")


def _matching_string_list(
    document: dict[str, Any],
    field: str,
    pattern: re.Pattern[str],
    description: str,
) -> None:
    values = document.get(field)
    if not isinstance(values, list):
        raise ContributionRecordError(f"{field} must be a list")
    for index, value in enumerate(values):
        if not isinstance(value, str) or not pattern.fullmatch(value):
            raise ContributionRecordError(f"{field}[{index}] must be {description}")


def _optional_ref(document: dict[str, Any], field: str) -> None:
    value = document.get(field)
    if value is None:
        return
    if (
        not isinstance(value, str)
        or not value
        or value.startswith(("/", "~"))
        or "\\" in value
        or ".." in value.split("/")
        or _URI_SCHEME.match(value)
    ):
        raise ContributionRecordError(
            f"{field} must be a safe local relative reference"
        )


def _optional_timestamp(document: dict[str, Any], field: str) -> None:
    if document.get(field) is not None:
        _require_timestamp(document, field)


def _local_reference_path(local_root: Path, reference: str) -> Path:
    root = local_root.resolve()
    path = (root / reference).resolve()
    if root != path and root not in path.parents:
        raise ContributionRecordError("validation_receipt_ref escapes local root")
    return path


def _read_local_json(local_root: Path, reference: str, label: str) -> object:
    path = _local_reference_path(local_root, reference)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ContributionRecordError(f"{label} file does not exist") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ContributionRecordError(f"{label} file is unreadable") from exc


def _optional_string(document: dict[str, Any], field: str) -> None:
    if document.get(field) is not None:
        _require_string(document, field)
