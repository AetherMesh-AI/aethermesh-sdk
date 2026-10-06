"""Explicit local node runtime API shared by CLI and tests.

This module is the boundary for local node lifecycle orchestration. Functions here
return structured objects/documents and never print; command-line handlers are
responsible only for parsing arguments, formatting results, and exit codes.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aethermesh_core.local_json_helpers import load_json_mapping
from aethermesh_core.local_restart import LocalRestartError, LocalRestartResult
from aethermesh_core.local_restart import restart_local_node as _restart_local_node
from aethermesh_core.local_runtime_config import (
    configured_runtime_path,
    configured_runtime_ref,
    load_optional_local_runtime_config,
)
from aethermesh_core.local_shutdown import LocalShutdownError, LocalShutdownResult
from aethermesh_core.local_shutdown import shutdown_local_node as _shutdown_local_node
from aethermesh_core.local_startup import LocalStartupError, LocalStartupResult
from aethermesh_core.local_startup import start_local_node as _start_local_node
from aethermesh_core.local_validation import LocalValidationError
from aethermesh_core.local_validation import (
    validate_local_results as _validate_local_results,
)


class LocalRuntimeInspectError(ValueError):
    """Raised when persisted local runtime artifacts cannot be inspected."""


def start_local_node_runtime(
    runtime_dir: str | Path, *, reset_creator_identity: bool = False
) -> LocalStartupResult:
    """Start/initialize one local node runtime and return startup artifacts."""

    return _start_local_node(runtime_dir, reset_creator_identity=reset_creator_identity)


def stop_local_node_runtime(
    runtime_dir: str | Path, *, timeout_seconds: float = 5.0
) -> LocalShutdownResult:
    """Stop one local node runtime and return persisted shutdown state."""

    return _shutdown_local_node(runtime_dir, timeout_seconds=timeout_seconds)


def restart_local_node_runtime(
    runtime_dir: str | Path, *, timeout_seconds: float = 5.0
) -> LocalRestartResult:
    """Restart one local node runtime without changing node identity."""

    return _restart_local_node(runtime_dir, timeout_seconds=timeout_seconds)


def validate_local_node_results(
    *,
    assignment_log_path: str | Path,
    result_log_path: str | Path,
    validation_log_path: str | Path,
) -> dict[str, object]:
    """Validate local result messages and persist structured validation output."""

    return _validate_local_results(
        assignment_log_path=assignment_log_path,
        result_log_path=result_log_path,
        validation_log_path=validation_log_path,
    )


def inspect_local_node_runtime(runtime_dir: str | Path) -> dict[str, object]:
    """Inspect local identity, manifest, receipt, lineage, and attribution refs."""

    root = Path(runtime_dir)
    config = load_optional_local_runtime_config(root, LocalRuntimeInspectError)
    identity_path, manifest_path, identity, manifest = _load_identity_and_manifest(
        root, config
    )
    node = _required_mapping(identity, "node", "identity")
    manifest_node = _required_mapping(manifest, "node", "manifest")
    node_id = _required_text(node, "node_id", "identity.node")
    creator_node_id = _required_text(node, "creator_node_id", "identity.node")
    manifest_node_id = _required_text(manifest_node, "node_id", "manifest.node")
    manifest_creator_node_id = _required_text(
        manifest_node, "creator_node_id", "manifest.node"
    )
    references = identity.get("references")
    lineage = identity.get("lineage")
    attribution = identity.get("contribution_attribution")
    if not isinstance(references, dict):
        raise LocalRuntimeInspectError("identity references must be an object")
    if not isinstance(lineage, dict):
        raise LocalRuntimeInspectError("identity lineage must be an object")
    if not isinstance(attribution, dict):
        raise LocalRuntimeInspectError(
            "identity contribution_attribution must be an object"
        )

    receipt_refs = _artifact_refs(
        root, configured_runtime_ref(config, "validation_receipts")
    )
    lineage_refs = _artifact_refs(root, configured_runtime_ref(config, "lineage"))
    contribution_refs = _artifact_refs(
        root, configured_runtime_ref(config, "contribution_attribution")
    )
    return {
        "node_id": node_id,
        "creator_node_id": creator_node_id,
        "identity_path": _relative_ref(root, identity_path),
        "manifest_path": _relative_ref(root, manifest_path),
        "manifest_matches_identity": manifest_node_id == node_id
        and manifest_creator_node_id == creator_node_id,
        "manifest_refs": _string_list(references.get("manifest_refs")),
        "validation_receipt_refs": receipt_refs,
        "identity_validation_receipt_refs": _string_list(
            references.get("validation_receipt_refs")
        ),
        "lineage_refs": lineage_refs,
        "identity_lineage_links": _string_list(lineage.get("lineage_links")),
        "contribution_refs": contribution_refs,
        "identity_contribution_refs": _string_list(
            attribution.get("contribution_refs")
        ),
        "attribution_creator_node_id": attribution.get("creator_node_id"),
        "attribution_node_id": attribution.get("attribution_node_id"),
    }


def local_node_status(runtime_dir: str | Path) -> dict[str, object]:
    """Return a fresh, read-only status snapshot for one local startup runtime.

    This reports only artifacts produced by the local runtime. It intentionally
    does not infer peer, reward, or network-wide state.
    """

    root = Path(runtime_dir)
    config = load_optional_local_runtime_config(root, LocalRuntimeInspectError)
    _identity_path, manifest_path, identity, manifest = _load_identity_and_manifest(
        root, config
    )
    node = _required_mapping(identity, "node", "identity")
    runtime_version = manifest.get("runtime_version")
    if not isinstance(runtime_version, dict):
        raise LocalRuntimeInspectError("manifest runtime_version must be an object")

    receipt_refs = _artifact_refs(
        root, configured_runtime_ref(config, "validation_receipts")
    )
    lineage_refs = _artifact_refs(root, configured_runtime_ref(config, "lineage"))
    contribution_refs = _artifact_refs(
        root, configured_runtime_ref(config, "contribution_attribution")
    )
    latest_receipt = _load_latest_artifact(root, receipt_refs)
    latest_lineage = _load_latest_artifact(root, lineage_refs)
    start_timestamp = _optional_text(latest_lineage, "timestamp")
    return {
        "status": "local_ready",
        "mode": "local-only-no-p2p",
        "observed_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "local_start_timestamp": start_timestamp,
        "uptime_seconds": _uptime_seconds(start_timestamp),
        "creator_node_id": _required_text(node, "creator_node_id", "identity.node"),
        "node_id": _required_text(node, "node_id", "identity.node"),
        "manifest_id": None,
        "manifest_path": _relative_ref(root, manifest_path),
        "runtime_version": dict(runtime_version),
        "active_work_count": 0,
        "last_validation_result": _validation_result(latest_receipt),
        "last_error_summary": _error_summary(latest_receipt),
        "lineage_root_or_parent_run_id": _lineage_parent(latest_lineage),
        "contribution_attribution_id": _attribution_id(latest_lineage),
        "validation_receipt_refs": receipt_refs,
        "lineage_refs": lineage_refs,
        "contribution_attribution_refs": contribution_refs,
    }


def _load_runtime_json(path: Path, label: str) -> dict[str, Any]:
    return load_json_mapping(path, label, LocalRuntimeInspectError)


def _load_identity_and_manifest(
    root: Path, config: Any
) -> tuple[Path, Path, dict[str, Any], dict[str, Any]]:
    identity_path = configured_runtime_path(root, config, "identity")
    manifest_path = configured_runtime_path(root, config, "manifest")
    return (
        identity_path,
        manifest_path,
        _load_runtime_json(identity_path, "identity"),
        _load_runtime_json(manifest_path, "manifest"),
    )


def _relative_ref(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _required_mapping(
    document: dict[str, Any], field_name: str, label: str
) -> dict[str, Any]:
    value = document.get(field_name)
    if not isinstance(value, dict):
        raise LocalRuntimeInspectError(
            f"{label} field {field_name!r} must be an object"
        )
    return value


def _required_text(document: dict[str, Any], field_name: str, label: str) -> str:
    value = document.get(field_name)
    if not isinstance(value, str) or value == "":
        raise LocalRuntimeInspectError(
            f"{label} field {field_name!r} must be a non-empty string"
        )
    return value


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise LocalRuntimeInspectError("runtime reference fields must be string lists")
    return list(value)


def _artifact_refs(root: Path, directory_ref: str) -> list[str]:
    path = root / directory_ref
    if not path.exists():
        return []
    return [
        f"{directory_ref}/{child.name}"
        for child in sorted(path.iterdir())
        if child.is_file() and child.suffix == ".json"
    ]


def _load_latest_artifact(root: Path, refs: list[str]) -> dict[str, Any] | None:
    if not refs:
        return None
    return _load_runtime_json(root / refs[-1], "runtime artifact")


def _optional_text(document: dict[str, Any] | None, field_name: str) -> str | None:
    if document is None:
        return None
    value = document.get(field_name)
    return value if isinstance(value, str) and value else None


def _uptime_seconds(start_timestamp: str | None) -> int | None:
    if start_timestamp is None:
        return None
    try:
        started_at = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))  # noqa: FURB162 - justification: retain legacy malformed-Z rejection before uptime calculation.
    except ValueError:
        return None
    if started_at.tzinfo is None:
        return None
    return max(0, int(time.time() - started_at.timestamp()))


def _validation_result(document: dict[str, Any] | None) -> object | None:
    if document is None:
        return None
    result = document.get("validation_result")
    return result if isinstance(result, (str, dict)) else None


def _error_summary(document: dict[str, Any] | None) -> str | None:
    if document is None:
        return None
    for field_name in ("error", "reason"):
        value = document.get(field_name)
        if isinstance(value, str) and value:
            return value
    result = document.get("validation_result")
    if isinstance(result, dict):
        reason = result.get("reason")
        if isinstance(reason, str) and reason:
            return reason
    return None


def _lineage_parent(document: dict[str, Any] | None) -> str | None:
    if document is None:
        return None
    for field_name in ("lineage_root_id", "parent_run_id"):
        value = document.get(field_name)
        if isinstance(value, str) and value:
            return value
    return None


def _attribution_id(document: dict[str, Any] | None) -> str | None:
    if document is None:
        return None
    attribution = document.get("contribution_attribution")
    if not isinstance(attribution, dict):
        return None
    value = attribution.get("attribution_node_id")
    return value if isinstance(value, str) and value else None


__all__ = [
    "LocalRestartError",
    "LocalRestartResult",
    "LocalRuntimeInspectError",
    "LocalShutdownError",
    "LocalShutdownResult",
    "LocalStartupError",
    "LocalStartupResult",
    "LocalValidationError",
    "inspect_local_node_runtime",
    "local_node_status",
    "restart_local_node_runtime",
    "start_local_node_runtime",
    "stop_local_node_runtime",
    "validate_local_node_results",
]
