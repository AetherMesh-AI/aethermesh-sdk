"""Bounded retention tables in the original owned admission database; no I/O service."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from .artifact_admission import AdmissionSnapshot, ArtifactAdmission
from .artifact_codec import _integer
from .artifact_retention import RetentionPlan, _digest, _json
from .network.config import validate_fingerprint


@dataclass(frozen=True)
class RetentionStoreLimits:
    max_intents: int
    max_operations: int
    max_metadata_bytes: int

    def __post_init__(self) -> None:
        _integer(self.max_intents, 1, 32)
        _integer(self.max_operations, 2, 1024)
        _integer(self.max_metadata_bytes, 4096, 1024 * 1024)

    @property
    def canonical(self) -> str:
        return _json(asdict(self)).decode("ascii")


@dataclass(frozen=True)
class RetentionOperation:
    operation_id: str
    intent_id: str
    request_digest: str
    record_digest: str
    revision: int

    def __post_init__(self) -> None:
        for value in (
            self.operation_id,
            self.intent_id,
            self.request_digest,
            self.record_digest,
        ):
            validate_fingerprint(value)
        _integer(self.revision, 1, 4096)


@dataclass(frozen=True)
class RetentionStoreSnapshot:
    ledger: AdmissionSnapshot
    limits: RetentionStoreLimits
    enabled_revision: int
    enabled_at: int
    clock_high_water: int
    last_revision: int
    plans: tuple[RetentionPlan, ...]
    operations: tuple[RetentionOperation, ...]

    @property
    def active_count(self) -> int:
        # Historical nonterminal intent, not a claim about present wall-clock availability.
        return sum(plan.records[-1].action != "expire" for plan in self.plans)


def owner(ledger: ArtifactAdmission) -> str:
    binding = ledger._directory_binding()
    if binding is None:
        raise ValueError("retention requires original directory ownership")
    info = (ledger._directory_path / "admission.sqlite3").stat()
    return _json(
        {
            "store_id": ledger._store_id,
            "policy": ledger._limits.canonical,
            "directory": binding,
            "database": (info.st_dev, info.st_ino),
        }
    ).decode("ascii")


def _rows(
    ledger: ArtifactAdmission, limits: RetentionStoreLimits
) -> tuple[list[Any], list[Any]]:
    plans = ledger._db.execute(
        "SELECT intent_id, request_id, body FROM retention_plans ORDER BY intent_id"
    ).fetchmany(limits.max_intents + 1)
    operations = ledger._db.execute(
        "SELECT operation_id, intent_id, request_digest, record_digest, revision FROM retention_operations ORDER BY revision"
    ).fetchmany(limits.max_operations + 1)
    if len(plans) > limits.max_intents or len(operations) > limits.max_operations:
        raise ValueError("retention store record bound")
    if (
        any(type(row[2]) is not bytes or len(row[2]) > 65536 for row in plans)
        or sum(len(row[2]) for row in plans) > limits.max_metadata_bytes
    ):
        raise ValueError("retention store metadata bound")
    return plans, operations


def _checksum(header: tuple[Any, ...], plans: list[Any], operations: list[Any]) -> str:
    return _digest(
        {
            "header": header,
            "plans": [(a, b, raw.decode("ascii")) for a, b, raw in plans],
            "operations": operations,
        }
    )


def _limits(raw: Any) -> RetentionStoreLimits:
    if not isinstance(raw, str) or len(raw) > 256:
        raise ValueError("invalid retention policy")
    result = RetentionStoreLimits(**json.loads(raw))
    if result.canonical != raw:
        raise ValueError("noncanonical retention policy")
    return result


def seal(ledger: ArtifactAdmission) -> None:
    """Seal already bounded transaction changes; never commit the transaction."""
    header = ledger._db.execute(
        "SELECT version, owner, policy, enabled_revision, enabled_at, clock, last_revision FROM retention_catalog"
    ).fetchone()
    plans, operations = _rows(ledger, _limits(header[2]))
    ledger._db.execute(
        "UPDATE retention_catalog SET checksum=?",
        (_checksum(header, plans, operations),),
    )


def _plans(
    ledger: ArtifactAdmission, snapshot: AdmissionSnapshot, rows: list[Any]
) -> tuple[RetentionPlan, ...]:
    result = tuple(RetentionPlan.decode(row[2]) for row in rows)
    identity = json.loads(owner(ledger))
    records = {row.request_id: row for row in snapshot.records}
    seen = set()
    for plan, (intent_id, request_id, _) in zip(result, rows, strict=True):
        terms = plan.terms
        current = records.get(request_id)
        if (
            not plan.records
            or plan.intent_id != intent_id
            or terms.admission.request_id != request_id
            or request_id in seen
            or current is None
            or current.binding != terms.admission.binding
            or terms.store_id != ledger._store_id
            or terms.limits != ledger._limits
            or terms.directory_identity != tuple(identity["directory"])
            or terms.database_identity != tuple(identity["database"])
        ):
            raise ValueError("retention plan ownership or admission mismatch")
        if any(
            row.admission_revision > current.revision
            or row.charged_bytes > snapshot.charged_bytes
            for row in plan.records
        ):
            raise ValueError("retention evidence exceeds current admission state")
        seen.add(request_id)
    return result


def _operations(
    plans: tuple[RetentionPlan, ...], rows: list[Any], enabled: int, current: int
) -> tuple[RetentionOperation, ...]:
    operations = tuple(RetentionOperation(*row) for row in rows)
    expected = {
        row.operation_id: (plan.intent_id, row)
        for plan in plans
        for row in plan.records
    }
    count = sum(len(plan.records) for plan in plans)
    if (
        len(expected) != count
        or len(operations) != count
        or {item.operation_id for item in operations} != set(expected)
        or len({item.revision for item in operations}) != count
    ):
        raise ValueError("retention operation index mismatch")
    indexed = {item.operation_id: item for item in operations}
    for operation in operations:
        intent_id, record = expected[operation.operation_id]
        if (
            operation.intent_id != intent_id
            or operation.record_digest != record.digest
            or not max(enabled, record.ledger_revision) < operation.revision <= current
        ):
            raise ValueError("retention operation revision or digest mismatch")
    for plan in plans:
        revisions = [indexed[row.operation_id].revision for row in plan.records]
        if revisions != sorted(revisions):
            raise ValueError("retention history reordered")
    return operations


def read_state(
    ledger: ArtifactAdmission, snapshot: AdmissionSnapshot
) -> RetentionStoreSnapshot:
    tables = {
        row[0]
        for row in ledger._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'retention_%'"
        )
    }
    if tables != {"retention_catalog", "retention_plans", "retention_operations"}:
        raise ValueError("retention schema mismatch")
    rows = ledger._db.execute(
        "SELECT version, owner, policy, enabled_revision, enabled_at, clock, last_revision, checksum FROM retention_catalog"
    ).fetchmany(2)
    if len(rows) != 1 or rows[0][0] != 1 or rows[0][1] != owner(ledger):
        raise ValueError("retention catalog ownership/version mismatch")
    version, binding, policy, enabled, enabled_at, clock, last, checksum = rows[0]
    limits = _limits(policy)
    _integer(enabled, 1, snapshot.revision)
    _integer(last, enabled, snapshot.revision)
    _integer(enabled_at, 1, 4102444800)
    _integer(clock, enabled_at, 4102444800)
    plan_rows, operation_rows = _rows(ledger, limits)
    if (
        _checksum(
            (version, binding, policy, enabled, enabled_at, clock, last),
            plan_rows,
            operation_rows,
        )
        != checksum
    ):
        raise ValueError("retention catalog checksum mismatch")
    plans = _plans(ledger, snapshot, plan_rows)
    operations = _operations(plans, operation_rows, enabled, snapshot.revision)
    by_id = {row.operation_id: row.at for plan in plans for row in plan.records}
    times = [enabled_at] + [by_id[item.operation_id] for item in operations]
    if (
        times != sorted(times)
        or clock != times[-1]
        or last != (operations[-1].revision if operations else enabled)
    ):
        raise ValueError("retention clock or revision history mismatch")
    state = RetentionStoreSnapshot(
        snapshot, limits, enabled, enabled_at, clock, last, plans, operations
    )
    if (
        len(operations) + state.active_count > limits.max_operations
        or snapshot.revision + state.active_count > 4096
        or sum(len(row[2]) for row in plan_rows) + 1024 * state.active_count
        > limits.max_metadata_bytes
    ):
        raise ValueError("retention terminal capacity reserve exceeded")
    return state
