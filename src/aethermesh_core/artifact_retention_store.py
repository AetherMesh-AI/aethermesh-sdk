"""Explicit durable local retention intent in the original owned admission ledger.

No network claim, serving, deletion, reclamation, restoration or future-byte guarantee.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import astuple, dataclass
from pathlib import Path
from typing import Literal

from . import _artifact_retention_db as storage
from . import artifact_directory as directories
from .artifact_admission import AdmissionConflict, AdmissionLimits, ArtifactAdmission
from .artifact_codec import CheckCancel, _cancel, _integer
from .artifact_owned_inventory import OwnedInventorySelection, _audit_locked
from .artifact_retention import RetentionPlan, _digest, retention_terms
from .network.config import validate_fingerprint

RetentionStoreLimits = storage.RetentionStoreLimits
RetentionStoreSnapshot = storage.RetentionStoreSnapshot


@dataclass(frozen=True)
class RetentionCommitReceipt:
    state: RetentionStoreSnapshot
    operation: storage.RetentionOperation
    replayed: bool
    verification_performed: bool
    durable_committed: Literal[True] = True
    retention_confirmed: Literal[False] = False
    publisher_authorized: Literal[False] = False
    serving_authorized: Literal[False] = False


@dataclass(frozen=True)
class RetentionStore:
    path: Path
    ledger_directory: Path
    store_id: str
    limits: AdmissionLimits
    retention_limits: RetentionStoreLimits

    def __post_init__(self) -> None:
        validate_fingerprint(self.store_id)
        if not isinstance(self.limits, AdmissionLimits) or not isinstance(
            self.retention_limits, RetentionStoreLimits
        ):
            raise TypeError("explicit admission and retention limits required")

    def _open(self) -> AbstractContextManager[tuple[ArtifactAdmission, int]]:
        return directories._owned_ledger(
            self.path, self.ledger_directory, self.store_id, self.limits
        )

    def _state(self, ledger: ArtifactAdmission) -> RetentionStoreSnapshot:
        snapshot = ledger._snapshot()
        if ledger._db.execute("SELECT version FROM catalog").fetchone()[0] != 2:
            raise ValueError("retention is not explicitly enabled")
        state = storage.read_state(ledger, snapshot)
        if state.limits != self.retention_limits:
            raise ValueError("frozen retention limits mismatch")
        return state

    def _fence(self, ledger: ArtifactAdmission, directory: int) -> None:
        directories._validate_binding(
            ledger, directory, self.ledger_directory, self.store_id, self.limits
        )

    def enable(
        self, *, expected_ledger_revision: int, at: int
    ) -> RetentionStoreSnapshot:
        """Explicit atomic version-1 to version-2 metadata enablement, never reset.

        Repeating enablement with the same frozen policy only reads existing state.
        Missing/damaged version-2 tables fail during open and are never recreated.
        """
        _integer(expected_ledger_revision, 0, 4096)
        _integer(at, 1, 4102444800)
        with self._open() as (ledger, directory):
            with ledger._db:
                ledger._db.execute("BEGIN IMMEDIATE")
                snapshot = ledger._snapshot()
                if ledger._db.execute("SELECT version FROM catalog").fetchone()[0] == 2:
                    state = self._state(ledger)
                    self._fence(ledger, directory)
                    return state
                if snapshot.revision != expected_ledger_revision:
                    raise AdmissionConflict("retention enablement revision conflict")
                revision = ledger._advance(snapshot)
                ledger._db.execute(
                    "CREATE TABLE retention_catalog (version INTEGER, owner TEXT, policy TEXT, enabled_revision INTEGER, enabled_at INTEGER, clock INTEGER, last_revision INTEGER, checksum TEXT)"
                )
                ledger._db.execute(
                    "CREATE TABLE retention_plans (intent_id TEXT PRIMARY KEY, request_id TEXT UNIQUE, body BLOB)"
                )
                ledger._db.execute(
                    "CREATE TABLE retention_operations (operation_id TEXT PRIMARY KEY, intent_id TEXT, request_digest TEXT, record_digest TEXT, revision INTEGER UNIQUE)"
                )
                ledger._db.execute(
                    "INSERT INTO retention_catalog VALUES (1, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        storage.owner(ledger),
                        self.retention_limits.canonical,
                        revision,
                        at,
                        at,
                        revision,
                        "0" * 64,
                    ),
                )
                ledger._db.execute(
                    "UPDATE catalog SET version=2, revision=?", (revision,)
                )
                storage.seal(ledger)
                state = self._state(ledger)
                self._fence(ledger, directory)
            self._fence(ledger, directory)
            return state

    def snapshot(self) -> RetentionStoreSnapshot:
        """Read historical metadata only; no current-byte or availability claim."""
        with self._open() as (ledger, directory), ledger._db:
            ledger._db.execute("BEGIN")
            result = self._state(ledger)
            self._fence(ledger, directory)
            return result

    def _request(
        self,
        plan: RetentionPlan,
        selections: tuple[OwnedInventorySelection, ...],
        expected_revision: int,
        expected_ledger_revision: int,
        max_read_bytes: int,
    ) -> str:
        _integer(expected_revision, 0, plan.terms.max_records)
        _integer(expected_ledger_revision, 0, 4096)
        _integer(max_read_bytes, 0, 64 * 1024 * 1024)
        if (
            not plan.records
            or not isinstance(selections, tuple)
            or len(selections) > 192
            or any(not isinstance(item, OwnedInventorySelection) for item in selections)
        ):
            raise ValueError("retention commit requires a bounded complete request")
        if plan.records[-1].action == "expire" and (selections or max_read_bytes):
            raise ValueError("expiry performs no payload verification")
        if plan.terms.store_id != self.store_id or plan.terms.limits != self.limits:
            raise ValueError("retention request store binding mismatch")
        return _digest(
            {
                "plan": plan.encode().decode("ascii"),
                "expected_revision": expected_revision,
                "expected_ledger_revision": expected_ledger_revision,
                "max_read_bytes": max_read_bytes,
                "selections": sorted(
                    (row.request_id, row.manifest.digest) for row in selections
                ),
            }
        )

    def _derive(
        self,
        ledger: ArtifactAdmission,
        directory: int,
        state: RetentionStoreSnapshot,
        plan: RetentionPlan,
        selections: tuple[OwnedInventorySelection, ...],
        expected_revision: int,
        max_read_bytes: int,
        check_cancel: CheckCancel | None,
    ) -> None:
        old = next(
            (item for item in state.plans if item.intent_id == plan.intent_id), None
        )
        record = plan.records[-1]
        if record.at < state.clock_high_water:
            raise AdmissionConflict("retention clock moved backward")
        if old is None:
            if any(
                item.terms.admission.request_id == plan.terms.admission.request_id
                for item in state.plans
            ):
                raise AdmissionConflict(
                    "admission already has a lifetime retention intent"
                )
            old = RetentionPlan(plan.intent_id, plan.terms)
        if (
            old.terms != plan.terms
            or old.records != plan.records[:-1]
            or len(old.records) != expected_revision
        ):
            raise AdmissionConflict("retention prepared prefix or revision conflict")
        evidence = None
        until = None
        if record.action != "expire":
            evidence = _audit_locked(
                self.path,
                selections,
                ledger=ledger,
                directory=directory,
                ledger_directory=self.ledger_directory,
                store_id=self.store_id,
                limits=self.limits,
                max_read_bytes=max_read_bytes,
                check_cancel=check_cancel,
                transactional=True,
            )
            if not old.records:
                manifest = next(
                    (
                        item.manifest
                        for item in selections
                        if item.request_id == plan.terms.admission.request_id
                    ),
                    None,
                )
                if manifest is None:
                    raise AdmissionConflict("retention admission is not selected")
                terms = retention_terms(
                    manifest,
                    evidence,
                    plan.terms.admission.request_id,
                    store_id=self.store_id,
                    limits=self.limits,
                    max_duration_seconds=plan.terms.max_duration_seconds,
                    max_records=plan.terms.max_records,
                )
                if terms != plan.terms:
                    raise AdmissionConflict(
                        "retention prepared terms differ from current owned evidence"
                    )
            until = record.until
        derived = old.advance(
            record.action,
            operation_id=record.operation_id,
            expected_revision=expected_revision,
            at=record.at,
            until=until,
            evidence=evidence,
            check_cancel=check_cancel,
        )
        if derived != plan:
            raise AdmissionConflict(
                "retention prepared evidence differs from fresh owned audit"
            )

    def _write(
        self,
        ledger: ArtifactAdmission,
        state: RetentionStoreSnapshot,
        plan: RetentionPlan,
        request: str,
    ) -> storage.RetentionOperation:
        record = plan.records[-1]
        revision = state.ledger.revision + 1
        _integer(revision, 1, 4096)
        operation = storage.RetentionOperation(
            record.operation_id, plan.intent_id, request, record.digest, revision
        )
        ledger._db.execute(
            "INSERT INTO retention_plans VALUES (?, ?, ?) ON CONFLICT(intent_id) DO UPDATE SET body=excluded.body",
            (plan.intent_id, plan.terms.admission.request_id, plan.encode()),
        )
        ledger._db.execute(
            "INSERT INTO retention_operations VALUES (?, ?, ?, ?, ?)",
            astuple(operation),
        )
        ledger._db.execute(
            "UPDATE retention_catalog SET clock=?, last_revision=?",
            (record.at, revision),
        )
        ledger._db.execute("UPDATE catalog SET revision=?", (revision,))
        storage.seal(ledger)
        return operation

    def _finish(
        self,
        ledger: ArtifactAdmission,
        directory: int,
        receipt: RetentionCommitReceipt,
        check_cancel: CheckCancel | None,
    ) -> RetentionCommitReceipt:
        # An exception after commit means an uncertain acknowledgment, not rollback.
        _cancel(check_cancel)
        self._fence(ledger, directory)
        return receipt

    def commit(
        self,
        plan: RetentionPlan,
        selections: tuple[OwnedInventorySelection, ...],
        *,
        expected_revision: int,
        expected_ledger_revision: int,
        max_read_bytes: int,
        check_cancel: CheckCancel | None = None,
    ) -> RetentionCommitReceipt:
        """Atomically persist one prepared step after fresh owned verification.

        Exact replay reads the original operation and CURRENT history without
        re-verifying bytes or renewing responsibility. After uncertain outcomes,
        repeat the identical request against the intact original store.
        """
        _cancel(check_cancel)
        if not isinstance(plan, RetentionPlan):
            raise TypeError("prepared retention plan required")
        plan = RetentionPlan.decode(plan.encode())
        request = self._request(
            plan,
            selections,
            expected_revision,
            expected_ledger_revision,
            max_read_bytes,
        )
        with self._open() as (ledger, directory):
            with ledger._db:
                ledger._db.execute("BEGIN IMMEDIATE")
                state = self._state(ledger)
                old = next(
                    (
                        item
                        for item in state.operations
                        if item.operation_id == plan.records[-1].operation_id
                    ),
                    None,
                )
                if old is not None:
                    if old.intent_id != plan.intent_id or old.request_digest != request:
                        raise AdmissionConflict(
                            "retention operation reused with different request"
                        )
                    self._fence(ledger, directory)
                    return RetentionCommitReceipt(state, old, True, False)
                if state.ledger.revision != expected_ledger_revision:
                    raise AdmissionConflict("retention admission revision conflict")
                self._derive(
                    ledger,
                    directory,
                    state,
                    plan,
                    selections,
                    expected_revision,
                    max_read_bytes,
                    check_cancel,
                )
                _cancel(check_cancel)
                self._fence(ledger, directory)
                operation = self._write(ledger, state, plan, request)
                after = self._state(ledger)
                if (
                    after.ledger.records != state.ledger.records
                    or after.ledger.charged_bytes != state.ledger.charged_bytes
                ):
                    raise AdmissionConflict("retention changed payload accounting")
                self._fence(ledger, directory)
                receipt = RetentionCommitReceipt(
                    after, operation, False, plan.records[-1].action != "expire"
                )
            return self._finish(ledger, directory, receipt, check_cancel)
