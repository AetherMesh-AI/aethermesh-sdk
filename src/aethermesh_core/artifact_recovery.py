"""Explicit recovery of positively verified staging; never reclaim admission.

Partial/corrupt files and ambiguous ownership remain untouched. A verified
staging alias is removed only after confirming the same verified final inode.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import artifact_admission as admission
from .artifact_codec import (
    MAX_FRAGMENT_BYTES,
    ArtifactManifest,
    CheckCancel,
    _cancel,
    _integer,
    _trusted,
)
from .artifact_directory import _account, _close_known, _owned_ledger
from .artifact_files import _stamp
from .artifact_ingestion import (
    _existing,
    _finish_publication,
    _publish,
    _same_file,
    _verify,
)
from .network.config import validate_fingerprint


@dataclass(frozen=True)
class FragmentRecovery:
    request_id: str
    path: Path
    outcome: Literal["published_staging", "removed_verified_alias", "already_published"]
    bytes_read: int
    integrity_verified: Literal[True] = True
    retention_confirmed: Literal[False] = False
    publisher_authorized: Literal[False] = False
    charge_released: Literal[False] = False


def _admitted(
    snapshot: admission.AdmissionSnapshot,
    request_id: str,
    manifest: ArtifactManifest,
    index: int,
    expected_digest: str,
) -> admission.FragmentAdmission:
    validate_fingerprint(request_id)
    record = next(
        (row for row in snapshot.records if row.request_id == request_id), None
    )
    if record is None or record.binding != (
        request_id,
        manifest.project,
        expected_digest,
        index,
        manifest.fragment_size(index),
        *manifest.fragments[index],
    ):
        raise ValueError("recovery requires the exact existing admission binding")
    return record


def recover_verified_fragment(
    path: Path,
    manifest: ArtifactManifest,
    index: int,
    *,
    expected_digest: str,
    request_id: str,
    ledger_directory: Path,
    store_id: str,
    limits: admission.AdmissionLimits,
    max_read_bytes: int,
    check_cancel: CheckCancel | None = None,
) -> FragmentRecovery:
    """Explicitly finish a verified publication or acknowledge a clean final.

    No reservation is created or changed. Exact positive ownership, bounded
    directory accounting and complete content/transfer verification precede any
    link/unlink. A partial file or different-inode final is never removed.

    Cancellation is checked until the final mutation boundary. Later sync/close
    errors may occur after publication or alias removal; retry this explicit
    operation to reverify state. Charges are never released on any outcome.
    """
    _trusted(manifest, expected_digest)
    _integer(max_read_bytes, 1, MAX_FRAGMENT_BYTES)
    size = manifest.fragment_size(index)
    if size > max_read_bytes:
        raise ValueError("recovery read budget exceeded")
    _cancel(check_cancel)
    with _owned_ledger(path, ledger_directory, store_id, limits) as (ledger, directory):
        snapshot = ledger.snapshot()
        record = _admitted(snapshot, request_id, manifest, index, expected_digest)
        entries, _, blocked = _account(
            directory,
            {row.request_id: row.size for row in snapshot.records},
            check_cancel,
            recovering=request_id,
        )
        if blocked:
            raise ValueError("ambiguous directory state requires operator recovery")
        names = {entry.name for entry in entries}
        if request_id + ".quarantine" in names:
            raise ValueError("quarantined request requires explicit disposition")
        final, stage = request_id + ".fragment", request_id + ".staging"
        if stage not in names:
            if final not in names:
                raise ValueError("no admitted bytes available for recovery")
            _existing(directory, final, manifest, index, check_cancel)
            outcome: Literal[
                "published_staging", "removed_verified_alias", "already_published"
            ] = "already_published"
        else:
            fd: int | None = None
            try:
                fd = os.open(
                    stage, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
                )
                verified = _verify(fd, directory, stage, manifest, index, check_cancel)
                if final in names:
                    _same_file(fd, directory, final)
                os.fsync(fd)
                _cancel(check_cancel)
                if ledger.snapshot() != snapshot:
                    raise admission.AdmissionConflict(
                        "admission changed before recovery commit"
                    )
                _cancel(check_cancel)
                if _stamp(os.fstat(fd)) != _stamp(verified):
                    raise ValueError("verified staging changed before recovery")
                _same_file(fd, directory, stage)
                if final in names:
                    _finish_publication(directory, fd, stage, final)
                    outcome = "removed_verified_alias"
                else:
                    _publish(directory, fd, stage, final, verified)
                    outcome = "published_staging"
            finally:
                _close_known(fd)
        if ledger.snapshot() != snapshot:
            raise admission.AdmissionConflict("admission changed during recovery")
        return FragmentRecovery(record.request_id, path / final, outcome, size)
