"""Contribution ledger helpers for local job results."""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aethermesh_core.models import JobResult
from aethermesh_core.result_hash import result_hash as canonical_result_hash

_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")


@dataclass(frozen=True)
class ContributionRecord:
    """Audit record derived from one local job result."""

    node_id: str
    job_id: str
    status: str
    contribution_units: int
    message: str | None = None
    validation_valid: bool | None = None
    validation_reason: str | None = None
    job_type: str | None = None
    result_hash: str | None = None
    version_metadata_ref: str | None = None
    created_at: str | None = None
    manifest_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record into a JSON-compatible dictionary."""

        document = asdict(self)
        if self.version_metadata_ref is None:
            document.pop("version_metadata_ref")
        if self.manifest_ref is None:
            document.pop("manifest_ref")
        if self.created_at is None:
            document.pop("created_at")
        return document

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ContributionRecord:
        """Deserialize one JSON-compatible contribution record."""

        _require_record_field(payload, "node_id", str)
        _require_record_field(payload, "job_id", str)
        _require_record_field(payload, "status", str)
        _require_record_field(payload, "contribution_units", int)
        message = payload.get("message")
        if message is not None and not isinstance(message, str):
            raise LedgerPersistenceError(
                "ledger record field 'message' must be a string or null"
            )
        validation_valid = payload.get("validation_valid")
        if validation_valid is not None and not isinstance(validation_valid, bool):
            raise LedgerPersistenceError(
                "ledger record field 'validation_valid' must be a boolean or null"
            )
        validation_reason = payload.get("validation_reason")
        if validation_reason is not None and not isinstance(validation_reason, str):
            raise LedgerPersistenceError(
                "ledger record field 'validation_reason' must be a string or null"
            )
        job_type = payload.get("job_type")
        if job_type is not None and not isinstance(job_type, str):
            raise LedgerPersistenceError(
                "ledger record field 'job_type' must be a string or null"
            )
        result_hash = payload.get("result_hash")
        if result_hash is not None:
            _require_result_hash("result_hash", result_hash)
        version_ref = payload.get("version_metadata_ref")
        if version_ref is not None and not isinstance(version_ref, str):
            raise LedgerPersistenceError(
                "ledger record field 'version_metadata_ref' must be a string or null"
            )
        manifest_ref = payload.get("manifest_ref")
        _require_optional_manifest_ref(manifest_ref)
        created_at = payload.get("created_at")
        if created_at is not None:
            _require_utc_timestamp("created_at", created_at)
        return cls(
            node_id=payload["node_id"],
            job_id=payload["job_id"],
            status=payload["status"],
            contribution_units=payload["contribution_units"],
            message=message,
            validation_valid=validation_valid,
            validation_reason=validation_reason,
            job_type=job_type,
            result_hash=result_hash,
            version_metadata_ref=version_ref,
            created_at=created_at,
            manifest_ref=manifest_ref,
        )


@dataclass(frozen=True)
class ContributionSummary:
    """Aggregated contribution totals for one local node."""

    node_id: str
    completed_job_count: int
    failed_job_count: int
    total_result_count: int
    total_contribution_units: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize the summary into a JSON-compatible dictionary."""

        return asdict(self)


class ContributionLedger:
    """Small in-memory ledger for prototype contribution accounting."""

    def __init__(
        self,
        records: list[ContributionRecord] | None = None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._records = list(records) if records is not None else []
        self._clock = clock

    def record(
        self,
        result: JobResult,
        *,
        validation_valid: bool | None = None,
        validation_reason: str | None = None,
        job_type: str | None = None,
        version_metadata_ref: str | None = None,
        manifest_ref: str | None = None,
    ) -> ContributionRecord:
        """Record one result and return its local contribution record.

        Only completed results with positive integer contribution units add to
        totals. Failed, zero, and negative-unit results are preserved for local
        auditability but contribute zero units.

        ``manifest_ref`` is recorded only when the caller has known manifest
        provenance; it may be a stable local reference or content hash.
        """

        _require_optional_manifest_ref(manifest_ref)
        units = _accounted_units(result)
        message = (
            result.error if result.error is not None else _string_output(result.output)
        )
        record = ContributionRecord(
            node_id=result.node_id,
            job_id=result.job_id,
            status=result.status,
            contribution_units=units,
            message=message,
            validation_valid=validation_valid,
            validation_reason=validation_reason,
            job_type=job_type,
            result_hash=canonical_result_hash(result),
            version_metadata_ref=version_metadata_ref,
            created_at=_utc_timestamp(self._clock()),
            manifest_ref=manifest_ref,
        )
        self._records.append(record)
        return record

    def summary_for_node(self, node_id: str) -> ContributionSummary:
        """Return deterministic aggregate totals for one local node."""

        node_records = [record for record in self._records if record.node_id == node_id]
        return ContributionSummary(
            node_id=node_id,
            completed_job_count=sum(
                1 for record in node_records if record.status == "completed"
            ),
            failed_job_count=sum(
                1 for record in node_records if record.status == "failed"
            ),
            total_result_count=len(node_records),
            total_contribution_units=sum(
                record.contribution_units for record in node_records
            ),
        )

    def node_ids(self) -> list[str]:
        """Return node ids present in the ledger in deterministic order."""

        return sorted({record.node_id for record in self._records})

    def summary_document(self, ledger_path: str | Path) -> dict[str, Any]:
        """Return a deterministic JSON-compatible summary of all records."""

        node_summaries = [self.summary_for_node(node_id) for node_id in self.node_ids()]
        return {
            "ledger_path": str(ledger_path),
            "record_count": len(self._records),
            "completed_result_count": sum(
                summary.completed_job_count for summary in node_summaries
            ),
            "failed_result_count": sum(
                summary.failed_job_count for summary in node_summaries
            ),
            "total_contribution_units": sum(
                summary.total_contribution_units for summary in node_summaries
            ),
            "nodes": [
                {
                    "node_id": summary.node_id,
                    "record_count": summary.total_result_count,
                    "completed_result_count": summary.completed_job_count,
                    "failed_result_count": summary.failed_job_count,
                    "total_contribution_units": summary.total_contribution_units,
                }
                for summary in node_summaries
            ],
        }

    def to_document(self, extra_fields: dict[str, Any] | None = None) -> dict[str, Any]:
        """Serialize the ledger into the small local JSON file shape."""

        document = dict(extra_fields) if extra_fields is not None else {}
        document["version"] = 1
        document["records"] = [record.to_dict() for record in self._records]
        return document

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> ContributionLedger:
        """Deserialize a ledger from the local JSON file shape."""

        version = document.get("version")
        if version != 1 or isinstance(version, bool):
            raise LedgerPersistenceError("ledger JSON must contain version 1")
        records = document.get("records")
        if not isinstance(records, list):
            raise LedgerPersistenceError("ledger JSON field 'records' must be a list")
        return cls([_record_from_json_value(record) for record in records])


class LedgerPersistenceError(ValueError):
    """Raised when a local ledger JSON file cannot be safely loaded or saved."""


def load_ledger_document(path: str | Path) -> tuple[ContributionLedger, dict[str, Any]]:
    """Load a JSON-backed local ledger, treating a missing file as empty.

    The returned metadata contains unknown top-level fields so callers can write
    them back without introducing a migration layer.
    """

    ledger_path = Path(path)
    if not ledger_path.exists():
        return ContributionLedger(), {}

    try:
        with ledger_path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except json.JSONDecodeError as exc:
        raise LedgerPersistenceError(f"ledger JSON is malformed: {exc.msg}") from exc
    except OSError as exc:
        raise LedgerPersistenceError(f"could not read ledger file: {exc}") from exc

    if not isinstance(document, dict):
        raise LedgerPersistenceError("ledger JSON must be an object")
    ledger = ContributionLedger.from_document(document)
    extras = {
        key: value
        for key, value in document.items()
        if key not in {"version", "records"}
    }
    return ledger, extras


def load_existing_ledger_document(
    path: str | Path,
) -> tuple[ContributionLedger, dict[str, Any]]:
    """Load an existing JSON-backed local ledger without creating defaults."""

    ledger_path = Path(path)
    if not ledger_path.exists():
        raise LedgerPersistenceError(f"ledger file does not exist: {ledger_path}")
    return load_ledger_document(ledger_path)


def save_ledger_document(
    path: str | Path,
    ledger: ContributionLedger,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """Write a JSON-backed local ledger via temp-file then atomic replace."""

    ledger_path = Path(path)
    parent = ledger_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    document = ledger.to_document(extra_fields)
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=parent,
            prefix=f".{ledger_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_name = handle.name
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_name, ledger_path)
    except OSError as exc:
        if temp_name is not None:
            _remove_temp_file(temp_name)
        raise LedgerPersistenceError(f"could not write ledger file: {exc}") from exc


def _accounted_units(result: JobResult) -> int:
    if result.status != "completed":
        return 0
    if not isinstance(result.contribution_units, int) or isinstance(
        result.contribution_units, bool
    ):
        raise ValueError("contribution_units must be an integer")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    return max(result.contribution_units, 0)


def _string_output(output: Any) -> str | None:
    if output is None:
        return None
    if isinstance(output, str):
        return output
    return str(output)


def _require_record_field(
    payload: dict[str, Any], field_name: str, expected_type: type
) -> None:
    value = payload.get(field_name)
    if not isinstance(value, expected_type) or isinstance(value, bool):
        raise LedgerPersistenceError(
            f"ledger record field '{field_name}' must be {expected_type.__name__}"
        )


def _require_optional_manifest_ref(value: object) -> None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise LedgerPersistenceError(
            "ledger record field 'manifest_ref' must be a non-empty string or null"
        )


def _require_result_hash(field_name: str, value: object) -> None:
    if not isinstance(value, str) or len(value) != 64:
        raise LedgerPersistenceError(
            f"ledger record field '{field_name}' must be a lowercase SHA-256 hex digest"
        )
    if value.lower() != value or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise LedgerPersistenceError(
            f"ledger record field '{field_name}' must be a lowercase SHA-256 hex digest"
        )


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("ledger clock must return a timezone-aware datetime")
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _require_utc_timestamp(field_name: str, value: object) -> None:
    if not isinstance(value, str) or not _UTC_TIMESTAMP.fullmatch(value):
        raise LedgerPersistenceError(
            f"ledger record field '{field_name}' must be an RFC 3339 UTC timestamp"
        )
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise LedgerPersistenceError(
            f"ledger record field '{field_name}' must be an RFC 3339 UTC timestamp"
        ) from exc


def _record_from_json_value(value: Any) -> ContributionRecord:
    if not isinstance(value, dict):
        raise LedgerPersistenceError("ledger records must be objects")
    return ContributionRecord.from_dict(value)


def _remove_temp_file(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        return
