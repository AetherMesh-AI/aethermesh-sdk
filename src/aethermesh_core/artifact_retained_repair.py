"""Fresh bounded local planning from durable intent; no serving/deletion authority."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from typing import Literal

from .artifact_admission import AdmissionConflict
from .artifact_codec import ArtifactManifest, CheckCancel, _cancel, _integer, _trusted
from .artifact_owned_inventory import (
    OwnedInventoryReport,
    OwnedInventorySelection,
    _audit_locked,
)
from .artifact_repair_plan import (
    RepairCandidate,
    RepairInventory,
    RepairPlanLimits,
    plan_artifact_repair,
)
from .artifact_retention_store import RetentionStore, RetentionStoreSnapshot
from .network.config import validate_project_id


@dataclass(frozen=True)
class RetainedRepairSource:
    source_id: str
    failure_domain: str
    store: RetentionStore
    selections: tuple[OwnedInventorySelection, ...]
    expected_ledger_revision: int

    def __post_init__(self) -> None:
        validate_project_id(self.source_id)
        validate_project_id(self.failure_domain)
        _integer(self.expected_ledger_revision, 0, 4096)
        if not isinstance(self.store, RetentionStore) or (
            not isinstance(self.selections, tuple)
            or len(self.selections) > 192
            or any(
                not isinstance(row, OwnedInventorySelection) for row in self.selections
            )
        ):
            raise ValueError("bounded explicit retention source required")


@dataclass(frozen=True)
class RetentionEligibility:
    request_id: str
    intent_id: str | None
    until: int | None
    reason: Literal[
        "eligible",
        "no_intent",
        "expired",
        "missing",
        "unverified",
        "quarantined",
        "stale_admission",
    ]


@dataclass(frozen=True)
class RetainedRepairEvidence:
    source_id: str
    state: RetentionStoreSnapshot
    inventory: OwnedInventoryReport
    eligibility: tuple[RetentionEligibility, ...]


@dataclass(frozen=True)
class RetainedRepairCandidate:
    candidate: RepairCandidate
    evidence: tuple[RetainedRepairEvidence, ...]
    observed_at: int
    audit_read_bytes: int
    requires_revalidation: Literal[True] = True
    serving_authorized: Literal[False] = False
    deletion_authorized: Literal[False] = False
    retention_confirmed: Literal[False] = False


def _eligibility(
    state: RetentionStoreSnapshot, report: OwnedInventoryReport, now: int
) -> tuple[RetentionEligibility, ...]:
    if report.ledger != state.ledger or now < state.clock_high_water:
        raise AdmissionConflict("stale retention inventory or clock")
    plans = {plan.terms.admission.request_id: plan for plan in state.plans}
    result = []
    for observation in report.observations:
        key = observation.admission.request_id
        plan = plans.get(key)
        reason: Literal[
            "eligible",
            "no_intent",
            "expired",
            "missing",
            "unverified",
            "quarantined",
            "stale_admission",
        ] = "eligible"
        if observation.status != "verified":
            reason = observation.status
        elif plan is None:
            reason = "no_intent"
        elif plan.records[-1].admission_revision != observation.admission.revision:
            reason = "stale_admission"
        elif plan.records[-1].action == "expire" or now >= plan.records[-1].until:
            reason = "expired"
        result.append(
            RetentionEligibility(
                key,
                plan.intent_id if plan else None,
                plan.records[-1].until if plan else None,
                reason,
            )
        )
    return tuple(result)


def plan_retained_artifact_repair(
    manifest: ArtifactManifest,
    sources: tuple[RetainedRepairSource, ...],
    targets: tuple[int, ...],
    *,
    expected_digest: str,
    limits: RepairPlanLimits,
    max_audit_read_bytes: int,
    now: int,
    deleted: bool = False,
    check_cancel: CheckCancel | None = None,
) -> RetainedRepairCandidate:
    """Hold all original stores while auditing and planning; release before returning.

    Caller time and configured domain labels are assumptions. Eligibility is only
    a source-selection restriction. Results may become stale immediately on return.
    No record, charge, clock, payload or authority is changed by this function.
    """
    _trusted(manifest, expected_digest)
    _integer(now, 1, 4102444800)
    _integer(max_audit_read_bytes, 0, 64 * 1024 * 1024)
    if (
        not isinstance(sources, tuple)
        or not 1 <= len(sources) <= 8
        or any(not isinstance(row, RetainedRepairSource) for row in sources)
    ):
        raise ValueError("select one to eight explicit local retention stores")
    if (
        len({row.source_id for row in sources}) != len(sources)
        or len({row.store.path for row in sources}) != len(sources)
        or len({row.store.ledger_directory for row in sources}) != len(sources)
    ):
        raise ValueError("repeated local source or store")
    ordered = sorted(sources, key=lambda row: str(row.store.path))
    evidence = []
    inventories = []
    with ExitStack() as stack:
        opened = []
        for source in ordered:
            _cancel(check_cancel)
            ledger, directory = stack.enter_context(source.store._open())
            stack.enter_context(ledger._db)
            ledger._db.execute("BEGIN IMMEDIATE")
            state = source.store._state(ledger)
            if (
                state.ledger.revision != source.expected_ledger_revision
                or now < state.clock_high_water
            ):
                raise AdmissionConflict("stale retained source revision or clock")
            opened.append((source, ledger, directory, state))
        read_bytes = sum(state.ledger.charged_bytes for _, _, _, state in opened)
        if read_bytes > max_audit_read_bytes:
            raise ValueError("aggregate retained audit read budget exceeded")
        for source, ledger, directory, state in opened:
            report = _audit_locked(
                source.store.path,
                source.selections,
                ledger=ledger,
                directory=directory,
                ledger_directory=source.store.ledger_directory,
                store_id=source.store.store_id,
                limits=source.store.limits,
                max_read_bytes=state.ledger.charged_bytes,
                check_cancel=check_cancel,
                transactional=True,
            )
            eligible = _eligibility(state, report, now)
            evidence.append(
                RetainedRepairEvidence(source.source_id, state, report, eligible)
            )
            inventories.append(
                RepairInventory(
                    source.source_id,
                    source.failure_domain,
                    report,
                    tuple(
                        row.request_id for row in eligible if row.reason == "eligible"
                    ),
                )
            )
        candidate = plan_artifact_repair(
            manifest,
            tuple(inventories),
            targets,
            expected_digest=expected_digest,
            limits=limits,
            now=now,
            deleted=deleted,
            check_cancel=check_cancel,
        )
        for source, ledger, directory, state in opened:
            _cancel(check_cancel)
            source.store._fence(ledger, directory)
            if source.store._state(ledger) != state:
                raise AdmissionConflict("retained inventory changed during planning")
        return RetainedRepairCandidate(
            candidate,
            tuple(sorted(evidence, key=lambda item: item.source_id)),
            now,
            read_bytes,
        )
