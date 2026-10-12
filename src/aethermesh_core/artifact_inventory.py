"""Bounded read-only reconciliation of admitted, explicitly selected local files.

Observations are sequential and ephemeral, not retention leases, publisher
approval, a filesystem scan, recovery writes or permission to reclaim charges.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .artifact_admission import AdmissionConflict, ArtifactAdmission, FragmentAdmission
from .artifact_codec import ArtifactManifest, CheckCancel, _cancel, _integer, _trusted
from .artifact_files import verify_fragment_file


@dataclass(frozen=True)
class InventorySelection:
    request_id: str
    manifest: ArtifactManifest
    path: Path


@dataclass(frozen=True)
class InventoryObservation:
    admission: FragmentAdmission
    status: Literal["verified", "missing", "unverified"]


@dataclass(frozen=True)
class InventoryReport:
    ledger_revision: int
    charged_bytes: int
    read_budget_bytes: int
    observations: tuple[InventoryObservation, ...]
    publisher_authorized: Literal[False] = False
    retention_confirmed: Literal[False] = False


def _preflight(
    records: tuple[FragmentAdmission, ...], selections: tuple[InventorySelection, ...]
) -> dict[str, InventorySelection]:
    if not isinstance(selections, tuple) or len(selections) > 192:
        raise ValueError("inventory selections must be a bounded tuple")
    chosen = {item.request_id: item for item in selections}
    if len(chosen) != len(selections) or set(chosen) != {
        record.request_id for record in records
    }:
        raise ValueError("select every admitted request exactly once")
    for record in records:
        item = chosen[record.request_id]
        _trusted(item.manifest, record.manifest_digest)
        if (
            item.manifest.project != record.project
            or item.manifest.fragment_size(record.fragment_index) != record.size
            or item.manifest.fragments[record.fragment_index]
            != (record.sha256, record.transfer_digest)
        ):
            raise ValueError("inventory manifest and admission binding mismatch")
        if not isinstance(item.path, Path) or not item.path.is_absolute():
            raise ValueError("inventory requires explicitly selected absolute paths")
    return chosen


def _observe(
    record: FragmentAdmission,
    item: InventorySelection,
    check_cancel: CheckCancel | None,
) -> InventoryObservation:
    cancelled = False

    def check() -> None:
        nonlocal cancelled
        try:
            _cancel(check_cancel)
        except BaseException:
            cancelled = True
            raise

    try:
        verify_fragment_file(
            item.manifest,
            record.fragment_index,
            item.path,
            expected_digest=record.manifest_digest,
            max_read_bytes=record.size,
            check_cancel=check,
        )
    except (OSError, ValueError) as error:
        if cancelled:
            raise
        status: Literal["missing", "unverified"] = (
            "missing" if isinstance(error, FileNotFoundError) else "unverified"
        )
        return InventoryObservation(record, status)
    return InventoryObservation(record, "verified")


def reconcile_inventory(
    ledger: ArtifactAdmission,
    selections: tuple[InventorySelection, ...],
    *,
    max_read_bytes: int,
    check_cancel: CheckCancel | None = None,
) -> InventoryReport:
    """Recheck every admitted request without changing files or ledger metadata.

    Require a complete caller-selected mapping and aggregate logical read budget
    before any fragment open. Existing verified_observation state is never used
    as evidence of current bytes. Missing/unsafe/corrupt/unreadable files remain
    charged. Callback exceptions always abort; no partial report is returned.

    Fail if the ledger changes during the pass. This fences report metadata, not
    filesystem writers: file observations occur at different times and must be
    refreshed before serving. Unselected/orphan files are outside this report.
    """
    _integer(max_read_bytes, 0, 64 * 1024 * 1024)
    _cancel(check_cancel)
    snapshot = ledger.snapshot()
    chosen = _preflight(snapshot.records, selections)
    if snapshot.charged_bytes > max_read_bytes:
        raise ValueError("aggregate inventory read budget exceeded")
    observations = tuple(
        _observe(record, chosen[record.request_id], check_cancel)
        for record in snapshot.records
    )
    _cancel(check_cancel)
    if ledger.snapshot() != snapshot:
        raise AdmissionConflict("inventory changed during reconciliation")
    return InventoryReport(
        snapshot.revision, snapshot.charged_bytes, snapshot.charged_bytes, observations
    )
