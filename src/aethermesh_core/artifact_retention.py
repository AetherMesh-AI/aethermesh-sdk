"""Pure local retention record preparation; no durable commit or custody promise."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal

from .artifact_admission import AdmissionLimits, FragmentAdmission
from .artifact_codec import ArtifactManifest, CheckCancel, _cancel, _integer, _trusted
from .artifact_owned_inventory import OwnedInventoryReport
from .artifact_repair_plan import _validate_report
from .network.config import validate_fingerprint
from .network.errors import ProtocolError
from .network.protocol import _constant, _object, _validate_message

MAX_RETENTION_BYTES = 65536


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value)).hexdigest()


def _identity(value: tuple[int, int]) -> None:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError("retention requires a local device/inode pair")
    _integer(value[0], 0, 2**63 - 1)
    _integer(value[1], 1, 2**63 - 1)


@dataclass(frozen=True)
class RetentionTerms:
    store_id: str
    limits: AdmissionLimits
    directory_identity: tuple[int, int]
    database_identity: tuple[int, int]
    admission: FragmentAdmission
    artifact_expires_at: int
    max_duration_seconds: int
    max_records: int

    def __post_init__(self) -> None:
        validate_fingerprint(self.store_id)
        if not isinstance(self.limits, AdmissionLimits) or not isinstance(
            self.admission, FragmentAdmission
        ):
            raise TypeError("retention requires typed quota and admission bindings")
        _identity(self.directory_identity)
        _identity(self.database_identity)
        _integer(self.artifact_expires_at, 1, 4102444800)
        _integer(self.max_duration_seconds, 1, 604800)
        _integer(self.max_records, 2, 64)
        if self.admission.size > self.limits.max_bytes:
            raise ValueError("retention binding exceeds existing quota")


def retention_terms(
    manifest: ArtifactManifest,
    report: OwnedInventoryReport,
    request_id: str,
    *,
    store_id: str,
    limits: AdmissionLimits,
    max_duration_seconds: int,
    max_records: int,
) -> RetentionTerms:
    """Prepare terms from a supplied local audit; this does not perform a new audit."""
    _trusted(manifest, manifest.digest)
    if not isinstance(report, OwnedInventoryReport):
        raise TypeError("owned inventory evidence required")
    _validate_report(report)
    rows = [row for row in report.ledger.records if row.request_id == request_id]
    if len(rows) != 1:
        raise ValueError("retention admission missing")
    row = rows[0]
    expected = (
        row.request_id,
        manifest.project,
        manifest.digest,
        row.fragment_index,
        manifest.fragment_size(row.fragment_index),
        *manifest.fragments[row.fragment_index],
    )
    if row.binding != expected:
        raise ValueError("retention manifest binding mismatch")
    return RetentionTerms(
        store_id,
        limits,
        report.directory_identity,
        report.database_identity,
        row,
        manifest.expires_at,
        max_duration_seconds,
        max_records,
    )


def _evidence(
    terms: RetentionTerms, report: OwnedInventoryReport
) -> tuple[str, int, int, int]:
    if not isinstance(report, OwnedInventoryReport):
        raise TypeError("owned inventory evidence required")
    _validate_report(report)
    if (
        report.directory_identity != terms.directory_identity
        or report.database_identity != terms.database_identity
        or report.ledger.charged_bytes > terms.limits.max_bytes
        or len(report.ledger.records) > terms.limits.max_objects
    ):
        raise ValueError("retention ownership or quota evidence mismatch")
    matches = [
        row
        for row in report.observations
        if row.admission.binding == terms.admission.binding
    ]
    if len(matches) != 1 or matches[0].status != "verified":
        raise ValueError("retention requires verified selected-fragment evidence")
    payload = {
        "ledger": asdict(report.ledger),
        "directory": report.directory_identity,
        "database": report.database_identity,
        "read_budget": report.read_budget_bytes,
        "observed_bytes": report.observed_regular_bytes,
        "observations": [
            (row.admission.checksum, row.status) for row in report.observations
        ],
    }
    return (
        _digest(payload),
        report.ledger.revision,
        matches[0].admission.revision,
        report.ledger.charged_bytes,
    )


@dataclass(frozen=True)
class RetentionRecord:
    operation_id: str
    action: Literal["accept", "renew", "expire"]
    at: int
    until: int
    evidence_digest: str
    ledger_revision: int
    admission_revision: int
    charged_bytes: int
    previous_digest: str

    def __post_init__(self) -> None:
        for value in (self.operation_id, self.evidence_digest, self.previous_digest):
            validate_fingerprint(value)
        if self.action not in ("accept", "renew", "expire"):
            raise ValueError("unsupported retention action")
        _integer(self.at, 1, 4102444800)
        _integer(self.until, 1, 4102444800)
        _integer(self.ledger_revision, 1, 4096)
        _integer(self.admission_revision, 1, self.ledger_revision)
        _integer(self.charged_bytes, 1, 64 * 1024 * 1024)

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


def _transition(
    terms: RetentionTerms, old: RetentionRecord | None, new: RetentionRecord
) -> None:
    if (
        new.charged_bytes > terms.limits.max_bytes
        or new.charged_bytes < terms.admission.size
        or new.admission_revision < terms.admission.revision
    ):
        raise ValueError("retention cannot change quota accounting")
    if old is not None and (
        old.action == "expire"
        or new.at < old.at
        or new.ledger_revision < old.ledger_revision
        or new.admission_revision < old.admission_revision
        or new.charged_bytes < old.charged_bytes
    ):
        raise ValueError(
            "terminal, backward-clock or regressed-accounting retention record"
        )
    if new.action == "expire":
        if (
            old is None
            or new.at < old.until
            or new.until != old.until
            or (
                new.evidence_digest,
                new.ledger_revision,
                new.admission_revision,
                new.charged_bytes,
            )
            != (
                old.evidence_digest,
                old.ledger_revision,
                old.admission_revision,
                old.charged_bytes,
            )
        ):
            raise ValueError("expiry preserves the last evidence and all charges")
    elif (
        new.action != ("accept" if old is None else "renew")
        or not new.at
        < new.until
        <= min(terms.artifact_expires_at, new.at + terms.max_duration_seconds)
        or (old is not None and (new.at >= old.until or new.until <= old.until))
    ):
        raise ValueError("invalid retention acceptance or renewal interval")


@dataclass(frozen=True)
class RetentionPlan:
    intent_id: str
    terms: RetentionTerms
    records: tuple[RetentionRecord, ...] = ()

    def __post_init__(self) -> None:
        validate_fingerprint(self.intent_id)
        if not isinstance(self.terms, RetentionTerms) or not isinstance(
            self.records, tuple
        ):
            raise TypeError("typed retention terms and records required")
        if len(self.records) > self.terms.max_records:
            raise ValueError("retention record budget exhausted")
        if (
            len(self.records) == self.terms.max_records
            and self.records[-1].action != "expire"
        ):
            raise ValueError("retention record budget reserves a terminal expiry slot")
        previous = self.root_digest
        old = None
        seen = set()
        for row in self.records:
            if (
                not isinstance(row, RetentionRecord)
                or row.previous_digest != previous
                or row.operation_id in seen
            ):
                raise ValueError("invalid retention record chain")
            _transition(self.terms, old, row)
            seen.add(row.operation_id)
            previous, old = row.digest, row

    @property
    def root_digest(self) -> str:
        return _digest(
            {"version": 1, "intent_id": self.intent_id, "terms": asdict(self.terms)}
        )

    @property
    def durable_committed(self) -> Literal[False]:
        return False

    @property
    def retention_confirmed(self) -> Literal[False]:
        return False

    def encode(self) -> bytes:
        payload = {"version": 1, **asdict(self)}
        raw = _json({**payload, "checksum": _digest(payload)})
        if len(raw) > MAX_RETENTION_BYTES:
            raise ValueError("retention byte budget exceeded")
        return raw

    @classmethod
    def decode(cls, raw: bytes) -> RetentionPlan:
        """Decode only bounded canonical records; checksums are not authority."""
        if type(raw) is not bytes or not 0 < len(raw) <= MAX_RETENTION_BYTES:
            raise ValueError("retention byte budget exceeded")
        try:
            data = json.loads(raw, object_pairs_hook=_object, parse_constant=_constant)
            _validate_message(data)
            if (
                set(data) != {"version", "intent_id", "terms", "records", "checksum"}
                or type(data["version"]) is not int
                or data["version"] != 1
            ):
                raise ValueError("retention version or fields mismatch")
            checksum = data.pop("checksum")
            validate_fingerprint(checksum)
            if _digest(data) != checksum:
                raise ValueError("retention envelope checksum mismatch")
            terms = dict(data["terms"])
            terms["limits"] = AdmissionLimits(**terms["limits"])
            terms["admission"] = FragmentAdmission(**terms["admission"])
            for key in ("directory_identity", "database_identity"):
                terms[key] = tuple(terms[key])
            if not isinstance(data["records"], list) or len(data["records"]) > 64:
                raise ValueError("retention record bound")
            result = cls(
                data["intent_id"],
                RetentionTerms(**terms),
                tuple(RetentionRecord(**row) for row in data["records"]),
            )
            if result.encode() != raw:
                raise ValueError("retention records must be canonical")
            return result
        except (TypeError, KeyError, RecursionError, ProtocolError) as error:
            raise ValueError("invalid retention record encoding") from error

    def advance(
        self,
        action: Literal["accept", "renew", "expire"],
        *,
        operation_id: str,
        expected_revision: int,
        at: int,
        until: int | None = None,
        evidence: OwnedInventoryReport | None = None,
        check_cancel: CheckCancel | None = None,
    ) -> RetentionPlan:
        """Prepare an explicit transition; never write, advertise, delete or release.

        Evidence is a caller-supplied observation, not a fresh custody check. A
        future backend must re-audit under intact ownership and commit atomically.
        """
        _cancel(check_cancel)
        _integer(expected_revision, 0, self.terms.max_records)
        old = self.records[-1] if self.records else None
        if action == "expire":
            if old is None or until is not None or evidence is not None:
                raise ValueError(
                    "expiry requires existing intent, without new evidence or duration"
                )
            proof, revision, admitted_revision, charged = (
                old.evidence_digest,
                old.ledger_revision,
                old.admission_revision,
                old.charged_bytes,
            )
            until = old.until
        else:
            if evidence is None or until is None:
                raise ValueError(
                    "acceptance and renewal require explicit interval and evidence"
                )
            proof, revision, admitted_revision, charged = _evidence(
                self.terms, evidence
            )
        previous = old.digest if old else self.root_digest
        proposed = RetentionRecord(
            operation_id,
            action,
            at,
            until,
            proof,
            revision,
            admitted_revision,
            charged,
            previous,
        )
        for row in self.records:
            _cancel(check_cancel)
            if row.operation_id == operation_id:
                if replace(proposed, previous_digest=row.previous_digest) != row:
                    raise ValueError(
                        "retention operation ID reused with different intent"
                    )
                return self
        if expected_revision != len(self.records):
            raise ValueError("retention revision conflict")
        result = replace(self, records=self.records + (proposed,))
        result.encode()
        _cancel(check_cancel)
        return result
