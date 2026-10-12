"""Pure bounded repair candidates from local observations, never execution authority."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from . import artifact_codec as codec
from .artifact_admission import AdmissionSnapshot, FragmentAdmission
from .artifact_owned_inventory import OwnedFragmentObservation, OwnedInventoryReport
from .network.config import validate_project_id


@dataclass(frozen=True)
class RepairInventory:
    """Caller-configured domain; its independence is not attested by this object."""

    source_id: str
    failure_domain: str
    report: OwnedInventoryReport
    eligible_request_ids: tuple[str, ...] | None = None


@dataclass(frozen=True)
class RepairPlanLimits:
    max_read_bytes: int
    max_write_bytes: int
    max_inflight: int
    max_targets: int

    def __post_init__(self) -> None:
        codec._integer(self.max_read_bytes, 0, 16 * 1024 * 1024)
        codec._integer(self.max_write_bytes, 0, 16 * 1024 * 1024)
        codec._integer(self.max_inflight, 1, 8)
        codec._integer(self.max_targets, 1, codec.MAX_FRAGMENTS)


@dataclass(frozen=True)
class RepairInput:
    source_id: str
    failure_domain: str
    request_id: str
    fragment_index: int
    size: int


@dataclass(frozen=True)
class RepairCandidate:
    manifest_digest: str
    status: Literal["candidate", "blocked"]
    reason: str | None
    targets: tuple[int, ...]
    inputs: tuple[RepairInput, ...]
    input_batches: tuple[tuple[RepairInput, ...], ...]
    required_read_bytes: int
    required_write_bytes: int
    limits: RepairPlanLimits
    evidence: tuple[RepairInventory, ...]
    requires_revalidation: Literal[True] = True
    failure_domains_verified: Literal[False] = False
    retention_confirmed: Literal[False] = False
    publisher_authorized: Literal[False] = False
    execution_authorized: Literal[False] = False


def _validate_report(report: OwnedInventoryReport) -> None:
    if not isinstance(report.ledger, AdmissionSnapshot):
        raise TypeError("invalid repair inventory ledger snapshot")
    records = report.ledger.records
    if (
        not isinstance(records, tuple)
        or len(records) > 192
        or any(not isinstance(row, FragmentAdmission) for row in records)
        or not isinstance(report.observations, tuple)
        or len(report.observations) != len(records)
        or any(
            not isinstance(row, OwnedFragmentObservation) for row in report.observations
        )
        or len({row.request_id for row in records}) != len(records)
        or tuple(row.admission for row in report.observations) != records
        or any(
            row.status not in ("verified", "missing", "unverified", "quarantined")
            for row in report.observations
        )
    ):
        raise ValueError("incomplete or inconsistent repair inventory")
    codec._integer(report.ledger.revision, 0, 4096)
    codec._integer(report.ledger.charged_bytes, 0, 64 * 1024 * 1024)
    codec._integer(report.read_budget_bytes, 0, 64 * 1024 * 1024)
    codec._integer(report.observed_regular_bytes, 0, 64 * 1024 * 1024)
    if (
        sum(record.size for record in records) != report.ledger.charged_bytes
        or any(record.revision > report.ledger.revision for record in records)
        or report.read_budget_bytes != report.ledger.charged_bytes
        or report.retention_confirmed is not False
        or report.publisher_authorized is not False
        or report.repair_authorized is not False
    ):
        raise ValueError("inventory accounting or authority mismatch")


def _inventories(
    inventories: tuple[RepairInventory, ...], check_cancel: codec.CheckCancel | None
) -> tuple[RepairInventory, ...]:
    if not isinstance(inventories, tuple) or not 1 <= len(inventories) <= 32:
        raise ValueError("repair requires one to 32 bounded local inventories")
    sources: set[str] = set()
    directories: set[tuple[int, int]] = set()
    databases: set[tuple[int, int]] = set()
    for selected in inventories:
        codec._cancel(check_cancel)
        if not isinstance(selected, RepairInventory) or not isinstance(
            selected.report, OwnedInventoryReport
        ):
            raise TypeError("repair requires typed local audit observations")
        validate_project_id(selected.source_id)
        validate_project_id(selected.failure_domain)
        report = selected.report
        for identity in (report.directory_identity, report.database_identity):
            if not isinstance(identity, tuple) or len(identity) != 2:
                raise ValueError("invalid repair inventory identity")
            codec._integer(identity[0], 0, 2**63 - 1)
            codec._integer(identity[1], 1, 2**63 - 1)
        if (
            selected.source_id in sources
            or report.directory_identity in directories
            or report.database_identity in databases
        ):
            raise ValueError("repeated source or local ownership identity")
        sources.add(selected.source_id)
        directories.add(report.directory_identity)
        databases.add(report.database_identity)
        _validate_report(report)
        allowed = selected.eligible_request_ids
        if allowed is not None and (
            not isinstance(allowed, tuple)
            or len(allowed) > 192
            or len(set(allowed)) != len(allowed)
            or not set(allowed) <= {row.request_id for row in report.ledger.records}
        ):
            raise ValueError("invalid repair eligibility filter")
    return tuple(sorted(inventories, key=lambda item: item.source_id))


def _inputs(
    manifest: codec.ArtifactManifest,
    inventories: tuple[RepairInventory, ...],
    check_cancel: codec.CheckCancel | None,
) -> tuple[RepairInput, ...]:
    result = []
    root = manifest.digest
    for selected in inventories:
        for observation in selected.report.observations:
            codec._cancel(check_cancel)
            record = observation.admission
            if record.manifest_digest != root:
                continue
            if (
                record.project != manifest.project
                or record.size != manifest.fragment_size(record.fragment_index)
                or (record.sha256, record.transfer_digest)
                != manifest.fragments[record.fragment_index]
            ):
                raise ValueError("repair inventory artifact binding mismatch")
            if observation.status == "verified":
                result.append(
                    RepairInput(
                        selected.source_id,
                        selected.failure_domain,
                        record.request_id,
                        record.fragment_index,
                        record.size,
                    )
                )
                if len(result) > codec.MAX_FRAGMENTS:
                    raise ValueError("repair candidate source bound exceeded")
    return tuple(result)


def _stripe(
    start: int, inputs: tuple[RepairInput, ...], check_cancel: codec.CheckCancel | None
) -> tuple[RepairInput, ...]:
    # Six indices and at most 32 configured domains: bounded augmenting paths
    # avoid incorrectly rejecting a feasible assignment due to greedy ordering.
    choices = {
        index: tuple(item for item in inputs if item.fragment_index == index)
        for index in range(start, start + 6)
    }
    matched: dict[str, RepairInput] = {}

    def place(index: int, seen: set[str]) -> bool:
        for item in choices[index]:
            codec._cancel(check_cancel)
            if item.failure_domain in seen:
                continue
            seen.add(item.failure_domain)
            previous = matched.get(item.failure_domain)
            if previous is None or place(previous.fragment_index, seen):
                matched[item.failure_domain] = item
                return True
        return False

    for index in choices:
        place(index, set())
        if len(matched) == 4:
            return tuple(sorted(matched.values(), key=lambda item: item.fragment_index))
    return ()


def plan_artifact_repair(
    manifest: codec.ArtifactManifest,
    inventories: tuple[RepairInventory, ...],
    targets: tuple[int, ...],
    *,
    expected_digest: str,
    limits: RepairPlanLimits,
    now: int,
    deleted: bool = False,
    check_cancel: codec.CheckCancel | None = None,
) -> RepairCandidate:
    """Propose all-artifact source batches without any file/network/codec I/O.

    Local inode identities share one host namespace. Configured domain labels are
    assumptions, not physical independence or remote authentication. Reports can
    already be stale; no timestamp or copied observation establishes authority.
    Every future executor must revalidate ownership, ledger state and bytes.
    """
    codec._trusted(manifest, expected_digest)
    codec._integer(now, 1, 4102444800)
    if not isinstance(limits, RepairPlanLimits) or type(deleted) is not bool:
        raise ValueError("invalid repair planning policy")
    if (
        not isinstance(targets, tuple)
        or not 1 <= len(targets) <= limits.max_targets
        or len(set(targets)) != len(targets)
    ):
        raise ValueError("invalid repair planning targets")
    output_bytes = sum(manifest.fragment_size(index) for index in targets)
    read_bytes = sum(
        4 * manifest.fragment_size(start)
        for start in range(0, len(manifest.fragments), 6)
    )
    codec._cancel(check_cancel)
    evidence = _inventories(inventories, check_cancel)
    available = _inputs(manifest, evidence, check_cancel)
    filters = {item.source_id: item.eligible_request_ids for item in evidence}
    usable = tuple(
        item
        for item in available
        if filters[item.source_id] is None
        or item.request_id in (filters[item.source_id] or ())
    )
    reason = None
    chosen: list[RepairInput] = []
    if deleted or now >= manifest.expires_at:
        reason = "deleted" if deleted else "expired"
    elif read_bytes > limits.max_read_bytes or output_bytes > limits.max_write_bytes:
        reason = "byte_budget"
    elif any(item.fragment_index in targets for item in available):
        reason = "target_already_observed"
    else:
        for start in range(0, len(manifest.fragments), 6):
            codec._cancel(check_cancel)
            selected = _stripe(start, usable, check_cancel)
            if not selected:
                reason = "insufficient_distinct_indices_or_domains"
                chosen.clear()
                break
            chosen.extend(selected)
    codec._cancel(check_cancel)
    inputs = tuple(chosen)
    return RepairCandidate(
        manifest.digest,
        "blocked" if reason else "candidate",
        reason,
        tuple(sorted(targets)),
        inputs,
        tuple(
            inputs[offset : offset + limits.max_inflight]
            for offset in range(0, len(inputs), limits.max_inflight)
        ),
        read_bytes,
        output_bytes,
        limits,
        evidence,
    )
