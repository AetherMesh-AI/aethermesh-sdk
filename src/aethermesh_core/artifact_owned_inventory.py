"""Read-only owned inventory; local observations never authorize retention or repair."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import artifact_admission as admission
from . import artifact_codec as codec
from . import artifact_directory as directories
from . import artifact_inventory as inventory


@dataclass(frozen=True)
class OwnedInventorySelection:
    request_id: str
    manifest: codec.ArtifactManifest


@dataclass(frozen=True)
class OwnedFragmentObservation:
    admission: admission.FragmentAdmission
    path: Path
    status: Literal["verified", "missing", "unverified", "quarantined"]


@dataclass(frozen=True)
class OwnedInventoryReport:
    ledger: admission.AdmissionSnapshot
    directory_identity: tuple[int, int]
    database_identity: tuple[int, int]
    read_budget_bytes: int
    observed_regular_bytes: int
    observations: tuple[OwnedFragmentObservation, ...]
    publisher_authorized: Literal[False] = False
    retention_confirmed: Literal[False] = False
    repair_authorized: Literal[False] = False


def audit_owned_inventory(
    path: Path,
    selections: tuple[OwnedInventorySelection, ...],
    *,
    ledger_directory: Path,
    store_id: str,
    limits: admission.AdmissionLimits,
    max_read_bytes: int,
    check_cancel: codec.CheckCancel | None = None,
) -> OwnedInventoryReport:
    """Audit all admissions inside the intact owned directory under its writer lock.

    Require every manifest and the full charged read budget before payload I/O.
    Derive final paths locally; never inspect quarantine payloads. Unknown links,
    unfinished staging or unsafe accounting block the complete audit. No metadata,
    payload or charge changes occur. Reverify before any later serving/repair use.
    """
    codec._integer(max_read_bytes, 0, 64 * 1024 * 1024)
    if (
        not isinstance(selections, tuple)
        or len(selections) > 192
        or any(not isinstance(item, OwnedInventorySelection) for item in selections)
    ):
        raise ValueError("owned inventory requires bounded explicit selections")
    codec._cancel(check_cancel)
    with directories._owned_ledger(path, ledger_directory, store_id, limits) as (
        ledger,
        directory,
    ):
        return _audit_locked(
            path,
            selections,
            ledger=ledger,
            directory=directory,
            ledger_directory=ledger_directory,
            store_id=store_id,
            limits=limits,
            max_read_bytes=max_read_bytes,
            check_cancel=check_cancel,
        )


def _audit_locked(
    path: Path,
    selections: tuple[OwnedInventorySelection, ...],
    *,
    ledger: admission.ArtifactAdmission,
    directory: int,
    ledger_directory: Path,
    store_id: str,
    limits: admission.AdmissionLimits,
    max_read_bytes: int,
    check_cancel: codec.CheckCancel | None = None,
    transactional: bool = False,
) -> OwnedInventoryReport:
    """Shared audit body; caller owns the directory lock and optional write transaction."""
    snapshot_reader = ledger._snapshot if transactional else ledger.snapshot
    snapshot = snapshot_reader()
    chosen = inventory._preflight(
        snapshot.records,
        tuple(
            inventory.InventorySelection(
                item.request_id,
                item.manifest,
                path / (item.request_id + ".fragment"),
            )
            for item in selections
        ),
    )
    if snapshot.charged_bytes > max_read_bytes:
        raise ValueError("aggregate owned-inventory read budget exceeded")
    sizes = {record.request_id: record.size for record in snapshot.records}
    before = directories._account(directory, sizes, check_cancel)
    entries, total, blocked = before
    if blocked:
        raise ValueError("owned inventory requires explicit directory recovery")
    quarantined = {entry.request_id for entry in entries if entry.phase == "quarantine"}
    observations = []
    for record in snapshot.records:
        codec._cancel(check_cancel)
        selected = chosen[record.request_id]
        if record.request_id in quarantined:
            observed = OwnedFragmentObservation(
                record, path / (record.request_id + ".quarantine"), "quarantined"
            )
        else:
            result = inventory._observe(record, selected, check_cancel)
            observed = OwnedFragmentObservation(record, selected.path, result.status)
        observations.append(observed)
    if directories._account(directory, sizes, check_cancel) != before:
        raise ValueError("owned directory accounting changed during audit")
    codec._cancel(check_cancel)
    directories._validate_binding(ledger, directory, ledger_directory, store_id, limits)
    if snapshot_reader() != snapshot:
        raise admission.AdmissionConflict("owned inventory changed during audit")
    owner = os.fstat(directory)
    database = (ledger_directory / "admission.sqlite3").stat()
    return OwnedInventoryReport(
        snapshot,
        (owner.st_dev, owner.st_ino),
        (database.st_dev, database.st_ino),
        snapshot.charged_bytes,
        total,
        tuple(observations),
    )
