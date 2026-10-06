"""Reusable local runtime service for AetherMesh frontends.

The diagnostic CLI and headless local API use this module instead of owning
node status logic independently. It is intentionally small and localhost-first:
it manages local config/data paths, identity initialization, status reporting,
and honest empty peer/job views for the current local-only prototype.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from aethermesh_core.capability_record import (
    CapabilityRecordError,
    validate_capability_record,
)
from aethermesh_core.identity import (
    deterministic_machine_node_id,
    deterministic_machine_node_name,
    load_or_create_identity,
)
from aethermesh_core.job_result_schema import (
    JOB_RESULT_SCHEMA_VERSION,
    MAX_INLINE_OUTPUT_PAYLOAD_BYTES,
    validate_job_result_document,
)
from aethermesh_core.json_io import atomic_create_json, atomic_write_json
from aethermesh_core.local_audit_event import (
    LocalAuditEventError,
    append_local_audit_event,
)
from aethermesh_core.local_json_helpers import canonical_json_hash
from aethermesh_core.models import Job, JobResult, NodeIdentity
from aethermesh_core.result_hash import canonical_result_document_hash
from aethermesh_core.runner import LocalRunner, run_local_job
from aethermesh_core.validation import validate_job_result
from aethermesh_core.validation_receipt_schema import (
    VALIDATION_RECEIPT_SCHEMA_VERSION,
    capture_validator_software_metadata,
    validate_validator_software_metadata,
)

CONFIG_SCHEMA_VERSION = 1
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 7280
DEFAULT_LOCAL_CAPABILITIES = (
    "echo",
    "hash",
    "basic_compute",
    "schema_transform",
    "keyword_extract",
    "text_chunk",
    "text_embed",
    "text_stats",
)
PUBLIC_VERSION = "0.2.0-alpha"
CAPABILITY_LIST_SCHEMA_VERSION = 1
LOCAL_CAPABILITY_WORKER_CAPACITY = 1
MANIFEST_INSPECTION_SCHEMA_VERSION = 1
CAPABILITY_RECORD_INSPECTION_SCHEMA_VERSION = 1
VALIDATION_RECEIPT_ID_PREFIX = "local-validation-receipt-"
LOCAL_JOB_SUBMISSION_SCHEMA_VERSION = 1
RESULT_REPORT_PREFLIGHT_SCHEMA_VERSION = 1
MAX_REJECTED_RESULT_REPORT_BYTES = 16 * 1024
MAX_REJECTED_RESULT_REPORT_REASON_CHARS = 512
LOCAL_JOB_STATES = frozenset(
    {"created", "queued", "running", "succeeded", "failed", "canceled"}
)
LOCAL_JOB_STATE_TRANSITIONS = frozenset(
    {
        ("created", "queued"),
        ("queued", "running"),
        ("running", "succeeded"),
        ("running", "failed"),
        ("created", "canceled"),
        ("queued", "canceled"),
        ("running", "canceled"),
    }
)
LOCAL_JOB_TERMINAL_STATES = frozenset({"succeeded", "failed", "canceled"})
MAX_LOCAL_INPUT_PAYLOAD_BYTES = 64 * 1024
MAX_LOCAL_ATTRIBUTION_METADATA_BYTES = 4 * 1024
_ATTRIBUTION_METADATA_RESERVED_FIELDS = frozenset(
    {
        "job_id",
        "creator_node_id",
        "worker_node_id",
        "validated_contribution_units",
    }
)
RESOURCE_HINT_FIELDS = frozenset(
    {
        "cpu_class",
        "ram_range",
        "disk_needs",
        "expected_duration",
        "network_sensitivity",
        "accelerator_type",
        "energy_profile",
        "operator_cost_label",
        "operator_notes",
    }
)
ECONOMIC_HINT_PATTERN = re.compile(
    r"\b(?:tokens?|rewards?|stak(?:e|es|ed|ing)|yields?|exchange\s+values?|"
    r"settlements?|payments?|payouts?|prices?|pricing)\b",
    re.IGNORECASE,
)

# These definitions are the local runtime's source of truth. A configured work
# type is enabled only when it is registered here; provenance entries describe
# the local artifact contracts implemented by the Phase 1 runtime.
LOCAL_CAPABILITY_DEFINITIONS = (
    ("work.echo", "Run deterministic local echo jobs.", "echo"),
    ("work.hash", "Run deterministic local SHA-256 hash jobs.", "hash"),
    (
        "work.basic_compute",
        "Run bounded deterministic arithmetic jobs.",
        "basic_compute",
    ),
    (
        "work.schema_transform",
        "Run declared-schema local transform jobs.",
        "schema_transform",
    ),
    (
        "work.keyword_extract",
        "Run deterministic local keyword extraction jobs.",
        "keyword_extract",
    ),
    ("work.text_chunk", "Run deterministic local text chunking jobs.", "text_chunk"),
    (
        "work.text_embed",
        "Run deterministic local text embedding helper jobs.",
        "text_embed",
    ),
    ("work.text_stats", "Run deterministic local text statistics jobs.", "text_stats"),
)
SUPPORTED_RUNTIME_JOB_TYPES = tuple(
    definition[2] for definition in LOCAL_CAPABILITY_DEFINITIONS
)
LOCAL_PROVENANCE_CAPABILITY_DEFINITIONS = (
    (
        "provenance.creator_node_id",
        "Preserve a creator node ID in local runtime identity artifacts.",
        "enabled",
    ),
    (
        "provenance.manifest",
        "Create and validate local startup and batch manifests.",
        "enabled",
    ),
    (
        "provenance.validation_receipt",
        "Write local validation receipts; these are not consensus evidence.",
        "enabled",
    ),
    (
        "provenance.lineage_reference",
        "Write and inspect local startup lineage references.",
        "enabled",
    ),
    (
        "provenance.contribution_attribution",
        "Record local validation-gated contribution attribution fields.",
        "enabled",
    ),
    (
        "provenance.end_to_end_runtime_lineage",
        "Bind a flow receipt to preserved creator and startup lineage evidence.",
        "disabled",
    ),
)


class RuntimeServiceError(ValueError):
    """Raised when local runtime state cannot be safely loaded or written."""


class _ResultReportPreflightError(RuntimeServiceError):
    """Internal structured rejection for a local result-report candidate."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ValidationReceiptNotFoundError(RuntimeServiceError):
    """Raised when a requested local validation receipt is not stored."""


class ResultReportNotFoundError(RuntimeServiceError):
    """Raised when a requested local result report is not stored."""


@dataclass(frozen=True)
class RuntimePaths:
    """Filesystem paths used by one local AetherMesh runtime."""

    home: Path
    config_path: Path
    data_dir: Path
    log_dir: Path
    identity_path: Path
    pid_path: Path
    events_path: Path

    @classmethod
    def from_home(cls, home: str | Path) -> RuntimePaths:
        root = Path(home).expanduser()
        return cls(
            home=root,
            config_path=root / "config.json",
            data_dir=root / "data",
            log_dir=root / "logs",
            identity_path=root / "identity.json",
            pid_path=root / "node.pid",
            events_path=root / "logs" / "events.log",
        )


class NodeRuntimeService:
    """Central service for local node config, lifecycle status, peers, and jobs."""

    def __init__(self, paths: RuntimePaths) -> None:
        self.paths = paths

    @classmethod
    def default(cls) -> NodeRuntimeService:
        return cls.from_home(_default_home())

    @classmethod
    def from_home(cls, home: str | Path) -> NodeRuntimeService:
        return cls(RuntimePaths.from_home(home))

    def load_config(self) -> dict[str, Any]:
        """Load local node config, returning a default view when missing."""

        if not self.paths.config_path.exists():
            return self._default_config(node_id=None)
        try:
            with self.paths.config_path.open("r", encoding="utf-8") as handle:
                document = json.load(handle)
        except json.JSONDecodeError as exc:
            raise RuntimeServiceError(f"config JSON is malformed: {exc.msg}") from exc
        except OSError as exc:
            raise RuntimeServiceError(f"could not read config file: {exc}") from exc
        if not isinstance(document, dict):
            raise RuntimeServiceError("config JSON must be an object")
        if document.get("version") != CONFIG_SCHEMA_VERSION:
            raise RuntimeServiceError("config JSON must contain version 1")
        return document

    def initialize_local_node_data(self) -> dict[str, Any]:
        """Create local runtime directories, identity, config, and log seed data."""

        self.paths.home.mkdir(parents=True, exist_ok=True)
        self.paths.data_dir.mkdir(parents=True, exist_ok=True)
        self.paths.log_dir.mkdir(parents=True, exist_ok=True)
        existing = self.load_config() if self.paths.config_path.exists() else None
        base_config = existing or self._default_config(node_id=None)
        persistence_enabled = _config_identity_persistence_enabled(base_config)
        identity_path = _config_identity_path(base_config, self.paths.identity_path)
        if persistence_enabled:
            identity = load_or_create_identity(identity_path)
        else:
            node_id = deterministic_machine_node_id()
            identity = NodeIdentity(
                node_id=node_id,
                node_name=deterministic_machine_node_name(node_id=node_id),
            )
        node_name = identity.node_name or deterministic_machine_node_name(
            node_id=identity.node_id
        )
        config = self._default_config(node_id=identity.node_id, node_name=node_name)
        if existing is not None:
            config = _merge_config(existing, config)
            config.setdefault("node", {})["node_id"] = identity.node_id
            config.setdefault("node", {})["node_name"] = node_name
        config.setdefault("identity", {})["persist"] = persistence_enabled
        config.setdefault("identity", {})["path"] = str(identity_path)
        self._write_config(config)
        if not self.paths.events_path.exists():
            self.paths.events_path.write_text("", encoding="utf-8")
        self._append_event("initialized local node data")
        return {
            "initialized": True,
            "node_id": identity.node_id,
            "node_name": node_name,
            "home": str(self.paths.home),
            "config_path": str(self.paths.config_path),
            "data_dir": str(self.paths.data_dir),
            "log_dir": str(self.paths.log_dir),
            "identity_path": str(identity_path),
            "identity_persisted": persistence_enabled,
        }

    def get_node_status(self) -> dict[str, Any]:
        """Return an honest local node status snapshot."""

        config = self.load_config()
        node_id = _config_node_id(config)
        node_name = _config_node_name(config)
        running, pid, started_at = self._runtime_marker()
        uptime_seconds: int | None = None
        if running and started_at is not None:
            uptime_seconds = max(0, int(time.time() - started_at))
        jobs = self.list_jobs()
        peers = self.list_peers()
        return {
            "initialized": self.paths.config_path.exists()
            and (
                not _config_identity_persistence_enabled(config)
                or _config_identity_path(config, self.paths.identity_path).exists()
            ),
            "node_id": node_id,
            "node_name": node_name,
            "status": "running" if running else "stopped",
            "version": _package_version(),
            "uptime_seconds": uptime_seconds,
            "pid": pid if running else None,
            "config_path": str(self.paths.config_path),
            "data_dir": str(self.paths.data_dir),
            "log_dir": str(self.paths.log_dir),
            "api": {
                "host": _config_api_host(config),
                "port": _config_api_port(config),
                "localhost_only": _config_api_host(config)
                in {"127.0.0.1", "localhost"},
            },
            "peer_count": len(peers["peers"]),
            "job_counts": {
                "current": len(jobs["current"]),
                "completed": len(jobs["completed"]),
                "failed": len(jobs["failed"]),
            },
            "capabilities": self.list_capabilities()["capabilities"],
            "package": self.package_info(),
            "network_health": self.network_health(),
            "system": self.system_info(),
        }

    def start_node_runtime(self) -> dict[str, Any]:
        """Prepare local runtime state before a foreground API/node process starts."""

        config = self.load_config() if self.paths.config_path.exists() else None
        identity_missing = (
            config is not None
            and _config_identity_persistence_enabled(config)
            and not _config_identity_path(config, self.paths.identity_path).exists()
        )
        if config is None or identity_missing:
            self.initialize_local_node_data()
        self.mark_runtime_started()
        status = self.get_node_status()
        self._append_event("node runtime started")
        return status

    def mark_runtime_started(self) -> None:
        self.paths.home.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.paths.pid_path,
            {"version": 1, "pid": os.getpid(), "started_at": int(time.time())},
        )

    def mark_runtime_stopped(self) -> None:
        try:
            self.paths.pid_path.unlink()
        except FileNotFoundError:
            pass
        self._append_event("node runtime stopped")

    def list_peers(self) -> dict[str, Any]:
        """Return known peers without inventing network state."""

        return {
            "bootstrap_status": "not_configured",
            "peer_count": 0,
            "peers": [],
            "note": "No peer discovery source is configured for the local daemon yet.",
        }

    def list_capabilities(self) -> dict[str, Any]:
        """Return the versioned local capability response used by ``/capabilities``.

        Response shape::

            {"schema_version": 1, "network_mode": "local-only-no-p2p",
             "capabilities": [{"identifier": str, "description": str,
                                "status": "enabled" | "disabled",
                                "creator_node_id": str | None,
                                "capability_manifest_id": str,
                                "resource_hints": {str: str} (optional),
                                "availability": {"status": "available" | "busy" |
                                                 "degraded" | "unavailable",
                                                 "reason": str | None,
                                                 "worker_capacity": dict,
                                                 "validation_receipt_refs": list},
                                "schema_version": 1}], "advertised": False}

        Work entries are enabled from ``config.capabilities.enabled_work_types``;
        provenance entries come from the registered local artifact contracts.
        Optional resource hints are advisory local metadata, not economic terms.
        No entry represents remote discovery, consensus, or network advertising.
        """

        config = self.load_config()
        enabled_work_types = _config_enabled_work_types(config)
        resource_hints = _config_capability_resource_hints(config)
        creator_node_id = _config_node_id(config)
        current_workers = len(self.list_jobs()["current"])
        capabilities = [
            {
                "identifier": identifier,
                "description": description,
                "status": "enabled" if work_type in enabled_work_types else "disabled",
                **_capability_provenance(identifier, creator_node_id, resource_hints),
                "availability": self._work_capability_availability(
                    work_type=work_type,
                    enabled=work_type in enabled_work_types,
                    creator_node_id=creator_node_id,
                    current_workers=current_workers,
                ),
                "schema_version": CAPABILITY_LIST_SCHEMA_VERSION,
                "work_type": work_type,
            }
            for identifier, description, work_type in LOCAL_CAPABILITY_DEFINITIONS
        ]
        capabilities.extend(
            {
                "identifier": identifier,
                "description": description,
                "status": (
                    "disabled"
                    if identifier == "provenance.creator_node_id"
                    and not _config_identity_persistence_enabled(config)
                    else status
                ),
                **_capability_provenance(identifier, creator_node_id, resource_hints),
                "availability": _metadata_capability_availability(
                    enabled=(
                        identifier != "provenance.creator_node_id"
                        or _config_identity_persistence_enabled(config)
                    )
                    and status == "enabled",
                    creator_node_id=creator_node_id,
                    current_workers=current_workers,
                ),
                "schema_version": CAPABILITY_LIST_SCHEMA_VERSION,
            }
            for identifier, description, status in LOCAL_PROVENANCE_CAPABILITY_DEFINITIONS
        )
        return {
            "schema_version": CAPABILITY_LIST_SCHEMA_VERSION,
            "network_mode": "local-only-no-p2p",
            "capabilities": capabilities,
            "advertised": False,
            "note": "Capabilities are local-only registered definitions and are not advertised to a live network.",
        }

    @staticmethod
    def _work_capability_availability(
        *,
        work_type: str,
        enabled: bool,
        creator_node_id: str | None,
        current_workers: int,
    ) -> dict[str, Any]:
        """Report local worker readiness without making network claims."""

        capacity = {
            "current": current_workers,
            "maximum": LOCAL_CAPABILITY_WORKER_CAPACITY,
        }
        if not enabled:
            return _availability(
                "unavailable", "disabled in local configuration", capacity
            )
        if creator_node_id is None:
            return _availability(
                "unavailable", "local node identity is missing", capacity
            )
        if current_workers >= LOCAL_CAPABILITY_WORKER_CAPACITY:
            return _availability("busy", "local worker capacity is in use", capacity)
        try:
            job = Job(
                job_id="local-capability-check",
                job_type=work_type,
                payload=_capability_check_payload(work_type),
            )
            result = LocalRunner(NodeIdentity(node_id=creator_node_id)).run(job)
            validation = validate_job_result(job, result)
        except (KeyError, TypeError, ValueError):
            return _availability(
                "degraded", "local capability validation failed", capacity
            )
        if not validation.valid:
            return _availability(
                "degraded", "local capability validation failed", capacity
            )
        return _availability("available", None, capacity)

    def inspect_capability_records(self) -> dict[str, Any]:
        """Return local capability records without treating them as network trust."""

        directory = self.paths.data_dir / "capability-records"
        store_status = "missing"
        paths: list[Path] = []
        if directory.is_symlink():
            store_status = "invalid"
        elif directory.is_dir():
            store_status = "available"
            paths = sorted(directory.glob("*.json"))
        local_node_id = _config_node_id(self.load_config())
        records = [
            self._capability_record_summary(path, local_node_id) for path in paths
        ]
        return {
            "schema_version": CAPABILITY_RECORD_INSPECTION_SCHEMA_VERSION,
            "network_mode": "local-only-no-p2p",
            "store_status": store_status,
            "record_count": len(records),
            "records": records,
            "note": "Local capability records only; validation is not network consensus.",
        }

    def emit_capability_advertisement(self) -> dict[str, Any]:
        """Write an honest, deterministic local capability advertisement artifact."""

        config = self.load_config()
        creator_node_id = _config_node_id(config)
        if creator_node_id is None:
            raise RuntimeServiceError(
                "capability advertisement requires initialized local node identity"
            )
        enabled_work_types = _config_enabled_work_types(config)
        current_workers = len(self.list_jobs()["current"])
        capability_manifest_dir = self.paths.data_dir / "capability-manifests"
        advertised_capabilities: list[dict[str, Any]] = []
        for identifier, description, work_type in LOCAL_CAPABILITY_DEFINITIONS:
            if work_type not in enabled_work_types:
                continue
            availability = self._work_capability_availability(
                work_type=work_type,
                enabled=True,
                creator_node_id=creator_node_id,
                current_workers=current_workers,
            )
            if availability["status"] != "available":
                continue
            manifest_ref = f"data/capability-manifests/{identifier}.json"
            manifest = _capability_advertisement_manifest(
                identifier=identifier,
                description=description,
                work_type=work_type,
                creator_node_id=creator_node_id,
            )
            atomic_write_json(capability_manifest_dir / f"{identifier}.json", manifest)
            advertised_capabilities.append(
                _advertised_capability(
                    identifier=identifier,
                    description=description,
                    work_type=work_type,
                    creator_node_id=creator_node_id,
                    manifest_ref=manifest_ref,
                )
            )
        advertisement = {
            "schema_version": 1,
            "scope": "local-only-no-p2p",
            "prototype_status": "prototype-local-validation-required",
            "creator_node_id": creator_node_id,
            "node_manifest_ref": "config.json",
            "validation_receipt_format": (
                f"local-validation-receipt-schema-v{VALIDATION_RECEIPT_SCHEMA_VERSION}"
            ),
            "capabilities": advertised_capabilities,
            "note": (
                "Local prototype advertisement only; it is not peer discovery, "
                "network consensus, or reward eligibility."
            ),
        }
        atomic_write_json(
            self.paths.data_dir / "capability-advertisement.json", advertisement
        )
        return advertisement

    def _capability_record_summary(
        self, path: Path, local_node_id: str | None
    ) -> dict[str, Any]:
        record_ref = f"data/capability-records/{path.name}"
        empty: dict[str, Any] = {
            "capability_id": None,
            "capability_version": None,
            "creator_node_id": None,
            "manifest_refs": [],
            "validation": {"status": None, "receipt_ids": [], "receipt_evidence": []},
            "validation_receipt_refs": [],
            "lineage": {
                "source_manifest_ref": None,
                "prior_capability_id": None,
                "local_build_artifact_ref": None,
            },
            "contribution_attribution": {
                "creator_node_id": None,
                "maintainer_node_id": None,
                "work_receipt_ids": [],
            },
        }
        if path.is_symlink():
            return {
                "record_ref": record_ref,
                "state": "invalid",
                **empty,
                "errors": ["capability record must not be a symbolic link"],
            }
        if local_node_id is None:
            return {
                "record_ref": record_ref,
                "state": "invalid",
                **empty,
                "errors": ["local node identity is unavailable"],
            }
        try:
            document = self._load_local_job_document(path, "capability record")
            record = validate_capability_record(
                document,
                local_node_id=local_node_id,
                local_schema_root=self.paths.home,
            )
        except (RuntimeServiceError, CapabilityRecordError) as exc:
            return {
                "record_ref": record_ref,
                "state": "invalid",
                **empty,
                "errors": [str(exc).split(":", 1)[0]],
            }

        validation = record["validation"]
        validation_status = validation["status"]
        state = {
            "unvalidated": "advertised",
            "passed": "validated",
            "failed": "invalid",
        }[validation_status]
        lineage = record["lineage"]
        attribution = record["contribution_attribution"]
        return {
            "record_ref": record_ref,
            "state": state,
            "capability_id": record["capability_id"],
            "capability_version": record["capability_version"],
            "creator_node_id": record["creator_node_id"],
            "manifest_refs": record["manifest_refs"],
            "validation": validation,
            "validation_receipt_refs": validation["receipt_ids"],
            "lineage": {
                "source_manifest_ref": lineage["source_manifest_ref"],
                "prior_capability_id": lineage.get("prior_capability_id"),
                "local_build_artifact_ref": lineage.get("local_build_artifact_ref"),
            },
            "contribution_attribution": {
                "creator_node_id": attribution["creator_node_id"],
                "maintainer_node_id": attribution.get("maintainer_node_id"),
                "work_receipt_ids": attribution["work_receipt_ids"],
            },
            "errors": [],
        }

    def inspect_model_manifests(self) -> dict[str, Any]:
        """Return read-only, redacted summaries of locally registered experts."""

        directory = self.paths.data_dir / "model-manifests"
        paths = sorted(directory.glob("*.json")) if directory.exists() else []
        manifests = [self._model_manifest_summary(path) for path in paths]
        return {
            "schema_version": MANIFEST_INSPECTION_SCHEMA_VERSION,
            "network_mode": "local-only-no-p2p",
            "manifest_count": len(manifests),
            "manifests": manifests,
            "note": "Local inspection only; validation status is not network consensus.",
        }

    def _model_manifest_summary(self, path: Path) -> dict[str, Any]:
        manifest_ref = f"data/model-manifests/{path.name}"
        errors: list[str] = []
        if path.is_symlink():
            return {
                "manifest_ref": manifest_ref,
                "inspection_status": "degraded",
                "errors": ["model manifest must not be a symbolic link"],
            }
        try:
            document = self._load_local_job_document(path, "model manifest")
        except RuntimeServiceError as exc:
            error = str(exc)
            if error.startswith("could not read model manifest:"):
                error = "could not read model manifest"
            return {
                "manifest_ref": manifest_ref,
                "inspection_status": "degraded",
                "errors": [error],
            }

        def required_string(name: str) -> str | None:
            value = document.get(name)
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{name} must be a non-empty string")
                return None
            return value

        manifest_id = required_string("manifest_id")
        creator_node_id = required_string("creator_node_id")
        version = document.get("version")
        if not isinstance(version, (str, int)) or isinstance(version, bool):
            errors.append("version must be a string or integer")
            version = None
        expert_type = required_string("expert_type")
        capability_tags = document.get("capability_tags", [])
        if not isinstance(capability_tags, list) or not all(
            isinstance(tag, str) and tag.strip() for tag in capability_tags
        ):
            errors.append("capability_tags must be a list of non-empty strings")
            capability_tags = []
        artifact_ref = document.get("artifact_ref")
        if not _safe_local_artifact_ref(artifact_ref):
            errors.append("artifact_ref must be a safe local relative reference")
            artifact_ref = None
        timestamps = document.get("timestamps", {})
        if not isinstance(timestamps, dict):
            errors.append("timestamps must be an object")
            timestamps = {}
        timestamp_values: dict[str, str | int | None] = {}
        for name in ("created_at", "updated_at"):
            value = timestamps.get(name)
            if value is not None and (
                not isinstance(value, (str, int)) or isinstance(value, bool)
            ):
                errors.append(f"timestamps.{name} must be a string or integer")
                value = None
            timestamp_values[name] = value
        validation = document.get("validation", {})
        if not isinstance(validation, dict):
            errors.append("validation must be an object")
            validation = {}
        status = validation.get("status", "not_validated")
        receipt_refs = validation.get("receipt_refs", [])
        if status not in {"not_validated", "pending", "passed", "failed"}:
            errors.append("validation.status is invalid")
            status = "unknown"
        if not isinstance(receipt_refs, list) or not all(
            _safe_local_artifact_ref(ref) for ref in receipt_refs
        ):
            errors.append("validation.receipt_refs contains an unsafe reference")
            receipt_refs = []
        lineage = document.get("lineage", {})
        if not isinstance(lineage, dict):
            errors.append("lineage must be an object")
            lineage = {}
        parent_manifest_ids = lineage.get("parent_manifest_ids", [])
        if not isinstance(parent_manifest_ids, list) or not all(
            isinstance(item, str) and item.strip() for item in parent_manifest_ids
        ):
            errors.append("lineage.parent_manifest_ids must be a string list")
            parent_manifest_ids = []
        attribution = document.get("contribution_attribution", {})
        if not isinstance(attribution, dict):
            errors.append("contribution_attribution must be an object")
            attribution = {}
        attribution_creator_node_id = attribution.get("creator_node_id")
        if attribution_creator_node_id is not None and (
            not isinstance(attribution_creator_node_id, str)
            or not attribution_creator_node_id.strip()
        ):
            errors.append(
                "contribution_attribution.creator_node_id must be a non-empty string"
            )
            attribution_creator_node_id = None
        contributor_node_ids = attribution.get("contributor_node_ids", [])
        if not isinstance(contributor_node_ids, list) or not all(
            isinstance(item, str) and item.strip() for item in contributor_node_ids
        ):
            errors.append(
                "contribution_attribution.contributor_node_ids must be a string list"
            )
            contributor_node_ids = []
        attribution_source = attribution.get("source")
        if attribution_source is not None and (
            not isinstance(attribution_source, str) or not attribution_source.strip()
        ):
            errors.append("contribution_attribution.source must be a non-empty string")
            attribution_source = None

        # Whitelist fields deliberately: manifests may contain credentials or
        # absolute implementation paths that are not part of this local API.
        return {
            "manifest_id": manifest_id,
            "version": version,
            "expert_type": expert_type,
            "capability_tags": capability_tags,
            "artifact_ref": artifact_ref,
            "manifest_ref": manifest_ref,
            "creator_node_id": creator_node_id,
            "timestamps": timestamp_values,
            "validation": {"status": status, "receipt_refs": receipt_refs},
            "lineage": {"parent_manifest_ids": parent_manifest_ids},
            "contribution_attribution": {
                "creator_node_id": attribution_creator_node_id,
                "contributor_node_ids": contributor_node_ids,
                "source": attribution_source,
            },
            "inspection_status": "degraded" if errors else "ok",
            "errors": errors,
        }

    def package_info(self) -> dict[str, Any]:
        """Return installed package metadata for SDK clients and local diagnostics."""

        return {
            "name": "aethermesh",
            "version": _package_version(),
            "source": "installed",
        }

    def network_health(self) -> dict[str, Any]:
        """Return honest local-only network health for UI frontends."""

        config = self.load_config()
        peers = self.list_peers()
        host = _config_api_host(config)
        return {
            "status": "local_only",
            "peer_count": peers["peer_count"],
            "api_reachable": True,
            "localhost_only": host in {"127.0.0.1", "localhost"},
            "note": "Public peer networking is not configured for this local prototype.",
        }

    def list_jobs(self) -> dict[str, Any]:
        """Return local job buckets without faking work."""

        return {
            "current": [],
            "completed": [],
            "failed": [],
            "validation_status": "not_active",
            "note": "No persistent daemon job queue is active yet.",
        }

    def submit_local_job_status(self, request: dict[str, Any]) -> dict[str, Any]:
        """Return one explicit local-only outcome for every API job submission."""

        try:
            accepted = self.submit_local_job(request)
        except RuntimeServiceError as exc:
            return self._submission_status(
                status="rejected",
                message=str(exc),
                validation_state="rejected",
                request=request,
            )
        except OSError:
            return self._submission_status(
                status="failed",
                message="Local submission could not be recorded.",
                validation_state="passed",
                request=request,
            )
        return self._submission_status(
            status="accepted",
            message="Local submission passed required checks and was recorded.",
            validation_state="passed",
            request=request,
            job_id=accepted["job_id"],
            manifest_ref=accepted["manifest_ref"],
        )

    @staticmethod
    def _submission_status(
        *,
        status: str,
        message: str,
        validation_state: str,
        request: object,
        job_id: str | None = None,
        manifest_ref: str | None = None,
    ) -> dict[str, Any]:
        """Build the concise machine-readable local submission status shape."""

        request_data = request if isinstance(request, dict) else {}
        creator_node_id = request_data.get("creator_node_id")
        supplied_job_id = request_data.get("job_id")
        known_job_id = job_id or (
            supplied_job_id if isinstance(supplied_job_id, str) else None
        )
        return {
            "schema_version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
            "status": status,
            "job_id": known_job_id,
            "job_type": request_data.get("job_type"),
            "requested_capability": request_data.get("requested_capability"),
            "creator_node_id": (
                creator_node_id if isinstance(creator_node_id, str) else None
            ),
            "manifest_ref": manifest_ref,
            "lineage": (
                {
                    "job_id": known_job_id,
                    "parent_refs": request_data.get("lineage_parent_refs"),
                }
                if known_job_id is not None
                else None
            ),
            "contribution_attribution": (
                {
                    "job_id": known_job_id,
                    "creator_node_id": creator_node_id,
                    "metadata": request_data.get("attribution_metadata"),
                }
                if known_job_id is not None and isinstance(creator_node_id, str)
                else None
            ),
            "validation": {"state": validation_state, "receipt_ref": None},
            "lineage_ref": (
                f"local-lineage-{known_job_id}" if manifest_ref is not None else None
            ),
            "attribution_ref": (
                f"local-contribution-{known_job_id}"
                if manifest_ref is not None
                else None
            ),
            "message": message,
            "network_mode": "local-only-no-p2p",
        }

    def submit_local_job(self, request: dict[str, Any]) -> dict[str, Any]:
        """Validate and queue one local job request without dispatching it."""

        if not isinstance(request, dict):
            raise RuntimeServiceError("job submission must be a JSON object")
        schema_version = request.get("schema_version")
        if (
            not isinstance(schema_version, int)
            or isinstance(schema_version, bool)
            or schema_version != LOCAL_JOB_SUBMISSION_SCHEMA_VERSION
        ):
            raise RuntimeServiceError("job submission schema_version must be integer 1")
        supplied_job_id = request.get("job_id")
        if supplied_job_id is not None and not self._is_local_job_id(supplied_job_id):
            raise RuntimeServiceError(
                "job submission job_id must be a local-job- followed by 32 lowercase hex characters"
            )
        job_id = supplied_job_id or f"local-job-{uuid4().hex}"
        manifest_path = self.paths.data_dir / "job-submissions" / f"{job_id}.json"
        manifest_ref = f"data/job-submissions/{job_id}.json"
        creator_node_id = request.get("creator_node_id")
        if not _safe_local_identifier(creator_node_id):
            raise RuntimeServiceError(
                "job submission creator_node_id must be a safe non-empty identifier"
            )
        requester_identity = _requester_identity(request.get("requester_identity"))
        job_type = request.get("job_type")
        if not isinstance(job_type, str) or not job_type.strip():
            raise RuntimeServiceError(
                "job submission job_type must be a non-empty string"
            )
        if job_type not in SUPPORTED_RUNTIME_JOB_TYPES:
            supported_types = ", ".join(SUPPORTED_RUNTIME_JOB_TYPES)
            error = RuntimeServiceError(
                f"job submission job_type unsupported: {job_type!r}; "
                f"supported local types: {supported_types}"
            )
            self._append_event(
                "rejected unsupported local job submission "
                f"creator_node_id={creator_node_id} job_type={job_type!r} reason={error}"
            )
            raise error
        try:
            requested_capability = self._requested_local_capability(
                request.get("requested_capability"), job_type
            )
        except RuntimeServiceError as exc:
            self._append_event(
                "rejected local job submission "
                f"creator_node_id={creator_node_id} "
                f"requested_capability={_rejection_log_capability(request.get('requested_capability'))} "
                f"reason={exc}"
            )
            raise
        input_payload, payload_hash = _validated_input_payload(
            request.get("input_payload")
        )
        expected_output_shape = _expected_output_shape(
            job_type, input_payload["content"]
        )
        local_safety = _local_safety_metadata(request.get("local_safety"))
        validation_mode = request.get("requested_validation_mode")
        if not isinstance(validation_mode, str) or not validation_mode.strip():
            raise RuntimeServiceError(
                "job submission requested_validation_mode must be a non-empty string"
            )
        lineage_parent_refs = request.get("lineage_parent_refs")
        if not isinstance(lineage_parent_refs, list) or not all(
            _safe_local_artifact_ref(ref) for ref in lineage_parent_refs
        ):
            raise RuntimeServiceError(
                "job submission lineage_parent_refs must be a list of safe local references"
            )
        attribution_metadata = _attribution_metadata(
            request.get("attribution_metadata")
        )
        try:
            json.dumps(request, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise RuntimeServiceError(
                "job submission must contain JSON-compatible data"
            ) from exc
        submission_fingerprint = canonical_json_hash(request, prefix="sha256:")
        if manifest_path.exists():
            return self._existing_local_submission(
                manifest_path, job_id, submission_fingerprint
            )
        try:
            atomic_create_json(
                manifest_path,
                {
                    "version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
                    "manifest_type": "local_job_submission",
                    "network_mode": "local-only-no-p2p",
                    "submitted_at": int(time.time()),
                    "initial_state": "created",
                    "submission_fingerprint": submission_fingerprint,
                    "job": {
                        "job_id": job_id,
                        "job_type": job_type,
                        "requested_capability": requested_capability,
                        "input_payload": input_payload,
                        "input_payload_hash": payload_hash,
                        "expected_output_shape": expected_output_shape,
                        "local_safety": local_safety,
                    },
                    "creator_node_id": creator_node_id,
                    "requester_identity": requester_identity,
                    "requested_validation_mode": validation_mode,
                    "lineage": {"job_id": job_id, "parent_refs": lineage_parent_refs},
                    "contribution_attribution": {
                        "job_id": job_id,
                        "creator_node_id": creator_node_id,
                        "metadata": attribution_metadata,
                    },
                },
            )
        except FileExistsError:
            return self._existing_local_submission(
                manifest_path, job_id, submission_fingerprint
            )
        try:
            local_node_id = self._local_node_id_for_submission()
            attribution_metadata_hash = canonical_json_hash(
                attribution_metadata, prefix="sha256:"
            )
            append_local_audit_event(
                self.paths.data_dir / "audit" / "job-submissions.jsonl",
                {
                    "schema_version": 2,
                    "event_id": f"local-audit-{job_id}-submitted",
                    "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "event_type": "job_submitted",
                    "actor_node_id": local_node_id,
                    "local_node_id": local_node_id,
                    "creator_node_id": creator_node_id,
                    "local_run_id": job_id,
                    "event_sequence": 1,
                    "job_id": job_id,
                    "work_id": job_id,
                    "manifest_id": job_id,
                    "manifest_ref": manifest_ref,
                    "hashes": {
                        "manifest_hash": canonical_json_hash(
                            json.loads(manifest_path.read_text(encoding="utf-8")),
                            prefix="sha256:",
                        )
                    },
                    "lineage_refs": lineage_parent_refs,
                    "validation_expectation": validation_mode,
                    "validation_status": "pending",
                    "contribution_attribution": {
                        "job_id": job_id,
                        "creator_node_id": creator_node_id,
                        "metadata_hash": attribution_metadata_hash,
                    },
                    "attribution_metadata_hash": attribution_metadata_hash,
                },
            )
        except (LocalAuditEventError, OSError, RuntimeServiceError, ValueError) as exc:
            try:
                manifest_path.unlink()
            except OSError:
                pass
            raise RuntimeServiceError(
                f"could not write local job submission audit event: {exc}"
            ) from exc
        record: dict[str, Any] = {
            "version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
            "job_id": job_id,
            "status": "created",
            "creator_node_id": creator_node_id,
            "requested_capability": requested_capability,
            "manifest_ref": manifest_ref,
            "lineage": {"job_id": job_id, "parent_refs": lineage_parent_refs},
            "contribution_attribution": {
                "job_id": job_id,
                "creator_node_id": creator_node_id,
                "metadata": attribution_metadata,
            },
            "timestamps": {"created_at": int(time.time())},
            "state_audit_refs": [],
            "worker_node_id": None,
            "executor_node_id": None,
            "validation": None,
            "result": None,
            "error": None,
        }
        atomic_create_json(self._job_status_path(job_id), record)
        self._transition_local_job_state(job_id, "queued")
        self._append_event(f"accepted local job submission {job_id}")
        return {
            "schema_version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
            "job_id": job_id,
            "status": "queued",
            "manifest_ref": manifest_ref,
            "requested_capability": requested_capability,
            "next_validation_expectation": "pending_requested_local_validation",
            "network_mode": "local-only-no-p2p",
        }

    def _local_node_id_for_submission(self) -> str:
        """Load the persistent local identity needed for a submission audit record."""

        if not self.paths.config_path.exists():
            self.initialize_local_node_data()
        local_node_id = _config_node_id(self.load_config())
        if local_node_id is None:
            raise RuntimeServiceError(
                "local node identity is unavailable for job audit"
            )
        return local_node_id

    def _existing_local_submission(
        self, manifest_path: Path, job_id: str, submission_fingerprint: str
    ) -> dict[str, Any]:
        """Return an exact retry's state or reject a conflicting local job ID."""

        manifest = self._load_local_job_document(
            manifest_path, "job submission manifest"
        )
        if (
            manifest.get("job", {}).get("job_id") == job_id
            and manifest.get("submission_fingerprint") == submission_fingerprint
        ):
            return {**self.get_local_job_status(job_id), "idempotent_retry": True}
        self._append_event(f"rejected duplicate local job submission {job_id}")
        raise RuntimeServiceError("local job ID already exists with different content")

    def _requested_local_capability(
        self, value: object, job_type: str
    ) -> dict[str, str]:
        """Resolve one job capability against this node's local capability manifest."""

        if not isinstance(value, dict) or set(value) != {"identifier"}:
            raise RuntimeServiceError(
                "job submission requested_capability must be an object with identifier"
            )
        identifier = value.get("identifier")
        if not isinstance(identifier, str) or not re.fullmatch(
            r"work\.[a-z][a-z0-9_]*", identifier
        ):
            raise RuntimeServiceError(
                "job submission requested_capability.identifier must be a canonical work capability identifier"
            )
        capability = next(
            (
                entry
                for entry in self.list_capabilities()["capabilities"]
                if entry["identifier"] == identifier
            ),
            None,
        )
        if capability is None:
            raise RuntimeServiceError(
                "job submission requested_capability.identifier is not present in the local node capability manifest"
            )
        if capability.get("work_type") != job_type:
            raise RuntimeServiceError(
                "job submission requested_capability.identifier does not match job_type"
            )
        if capability["status"] != "enabled":
            raise RuntimeServiceError(
                "job submission requested_capability.identifier is disabled in the local node capability manifest"
            )
        return {"identifier": identifier}

    def get_local_job_status(self, job_id: str) -> dict[str, Any]:
        """Read one submission and its optional local execution evidence."""

        missing = {
            "schema_version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
            "job_id": job_id,
            "status": "not_found",
            "error": "local job not found",
        }
        if not self._is_local_job_id(job_id):
            return missing
        manifest_path = self.paths.data_dir / "job-submissions" / f"{job_id}.json"
        if not manifest_path.exists():
            return missing
        manifest = self._load_local_job_document(
            manifest_path, "job submission manifest"
        )
        if manifest.get("job", {}).get("job_id") != job_id:
            raise RuntimeServiceError(
                "job submission manifest job_id does not match its path"
            )
        status_path = self.paths.data_dir / "job-status" / f"{job_id}.json"
        status = (
            self._load_local_job_document(status_path, "job status record")
            if status_path.exists()
            else {}
        )
        return {
            "schema_version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
            "job_id": job_id,
            "status": status.get("status", "queued"),
            "manifest_ref": f"data/job-submissions/{job_id}.json",
            "creator_node_id": manifest["creator_node_id"],
            "requested_capability": manifest["job"]["requested_capability"],
            "requester_identity": manifest.get("requester_identity"),
            "worker_node_id": status.get("worker_node_id"),
            "executor_node_id": status.get("executor_node_id"),
            "lineage": manifest["lineage"],
            "contribution_attribution": status.get(
                "contribution_attribution", manifest["contribution_attribution"]
            ),
            "timestamps": status.get("timestamps"),
            "state_audit_refs": status.get("state_audit_refs", []),
            "validation": status.get("validation"),
            "result": status.get("result"),
            "error": status.get("error"),
            "network_mode": "local-only-no-p2p",
        }

    def get_local_job_result(self, job_id: str) -> dict[str, Any]:
        """Load one stored local result report by its immutable job ID."""

        if not self._is_local_job_id(job_id):
            raise RuntimeServiceError("job_id must be a local job ID")
        result_path = self.paths.data_dir / "job-results" / f"{job_id}.json"
        if not result_path.exists():
            raise ResultReportNotFoundError("local result report not found")
        result = self._load_local_job_document(result_path, "job result record")
        try:
            validate_job_result_document(result)
        except ValueError as exc:
            raise RuntimeServiceError("job result record violates its schema") from exc
        if result["job_id"] != job_id:
            raise RuntimeServiceError("job result record does not match its job ID")
        return result

    def preflight_local_result_report(self, report: object) -> dict[str, Any]:
        """Check an incoming report without creating accepted work evidence."""

        try:
            checked = validate_job_result_document(report)
            job_id = checked["job_id"]
            if not self._is_local_job_id(job_id):
                raise _ResultReportPreflightError(
                    "invalid_work_id", "result report job_id must be a local job ID"
                )
            manifest_path = self.paths.data_dir / "job-submissions" / f"{job_id}.json"
            if not manifest_path.exists():
                raise _ResultReportPreflightError(
                    "missing_manifest",
                    "result report does not reference an existing local work manifest",
                )
            manifest = self._load_local_job_document(
                manifest_path, "job submission manifest"
            )
            status_path = self._job_status_path(job_id)
            if not status_path.exists():
                raise _ResultReportPreflightError(
                    "missing_work_state",
                    "result report does not reference existing local work state",
                )
            status = self._load_local_job_document(status_path, "job status record")
            self._validate_result_report_manifest_binding(
                checked, manifest, status, job_id
            )
        except _ResultReportPreflightError as exc:
            return self._reject_local_result_report(report, exc.code, str(exc))
        except ValueError as exc:
            return self._reject_local_result_report(
                report, "invalid_result_report", str(exc)
            )

        return {
            "schema_version": RESULT_REPORT_PREFLIGHT_SCHEMA_VERSION,
            "status": "ready_for_validation",
            "job_id": job_id,
            "manifest_ref": f"data/job-submissions/{job_id}.json",
            "network_mode": "local-only-no-p2p",
        }

    def _validate_result_report_manifest_binding(
        self,
        report: dict[str, Any],
        manifest: dict[str, Any],
        status: dict[str, Any],
        job_id: str,
    ) -> None:
        """Require a candidate report to match its immutable local work manifest."""

        manifest_job = manifest.get("job")
        lineage = manifest.get("lineage")
        if not isinstance(manifest_job, dict) or not isinstance(lineage, dict):
            raise _ResultReportPreflightError(
                "invalid_manifest", "referenced local work manifest is invalid"
            )
        manifest_id = canonical_json_hash(manifest, prefix="sha256:")
        expected_receipt_id = self._receipt_id_for_job(job_id)
        expected_executor = status.get("executor_node_id")
        submitted_at = manifest.get("submitted_at")
        if not isinstance(submitted_at, int) or isinstance(submitted_at, bool):
            raise _ResultReportPreflightError(
                "invalid_manifest", "referenced local work manifest has invalid timing"
            )
        expected_created_at = _utc_timestamp_from_unix_seconds(submitted_at)
        if manifest_job.get("job_id") != job_id:
            raise _ResultReportPreflightError(
                "invalid_manifest",
                "referenced local work manifest does not match work ID",
            )
        manifest_ref = f"data/job-submissions/{job_id}.json"
        report_manifest_link = report["references"]["manifest_hash"]
        if report_manifest_link is None:
            report_manifest_link = report["references"].get("manifest_ref")
        if (
            report_manifest_link not in {manifest_id, manifest_ref}
            or report["manifest_id"] != report_manifest_link
        ):
            raise _ResultReportPreflightError(
                "manifest_mismatch",
                "result report manifest reference does not match local work manifest",
            )
        if (
            status.get("job_id") != job_id
            or not _safe_local_identifier(expected_executor)
            or status.get("worker_node_id") != expected_executor
            or report["result_id"] != f"local-result-{job_id}"
            or report["creator_node_id"] != manifest.get("creator_node_id")
            or report["capability"]
            != manifest_job.get("requested_capability", {}).get("identifier")
            or report["task_id"] != job_id
            or report["created_at"] != expected_created_at
            or report["validation_receipt_id"] != expected_receipt_id
            or report["references"]["validation_receipt_ids"] != [expected_receipt_id]
            or report["executor_node_id"] != expected_executor
            or report["validator_node_id"] != expected_executor
            or report["lineage"]["input_manifest_ids"] != [report_manifest_link]
            or report["lineage"]["parent_job_ids"] != lineage.get("parent_refs")
        ):
            raise _ResultReportPreflightError(
                "manifest_mismatch",
                "result report provenance does not match local work manifest",
            )
        if (
            report["validation_status"] not in {"pending", "not_run"}
            or report["result_hash"] is not None
        ):
            raise _ResultReportPreflightError(
                "premature_validation",
                "result report must remain unvalidated until local validation completes",
            )

    def _reject_local_result_report(
        self, report: object, reason_code: str, reason: str
    ) -> dict[str, Any]:
        """Persist bounded rejected input without changing accepted evidence."""

        attempt_id = f"local-rejected-result-report-{uuid4().hex}"
        bounded_reason = reason[:MAX_REJECTED_RESULT_REPORT_REASON_CHARS]
        rejection = {
            "schema_version": RESULT_REPORT_PREFLIGHT_SCHEMA_VERSION,
            "attempt_id": attempt_id,
            "status": "rejected",
            "reason_code": reason_code,
            "reason": bounded_reason,
            "processed_at": _utc_timestamp(),
            "report": _bounded_rejected_result_report(report),
            "network_mode": "local-only-no-p2p",
        }
        atomic_create_json(
            self.paths.data_dir / "rejected-result-reports" / f"{attempt_id}.json",
            rejection,
        )
        return {
            "schema_version": RESULT_REPORT_PREFLIGHT_SCHEMA_VERSION,
            "status": "rejected",
            "attempt_id": attempt_id,
            "reason_code": reason_code,
            "reason": bounded_reason,
            "network_mode": "local-only-no-p2p",
        }

    def list_local_job_results(self) -> dict[str, Any]:
        """List redacted summaries of stored local result reports deterministically.

        This inspection boundary excludes output payloads, failure details, and
        local artifact paths. Detail reads remain available for one validated
        report through ``get_local_job_result``.
        """

        result_directory = self.paths.data_dir / "job-results"
        reports: list[dict[str, Any]] = []
        if not result_directory.exists():
            return {"schema_version": 1, "total": 0, "result_reports": reports}
        for result_path in sorted(result_directory.glob("*.json")):
            job_id = result_path.stem
            # Ignore unrelated filenames rather than interpreting them as reports.
            if not self._is_local_job_id(job_id):
                continue
            result = self.get_local_job_result(job_id)
            reports.append(
                {
                    "result_id": result["result_id"],
                    "job_id": result["job_id"],
                    "status": result["status"],
                    "capability": result["capability"],
                    "timestamps": {
                        "created_at": result["created_at"],
                        "started_at": result["started_at"],
                        "finished_at": result["finished_at"],
                        "reported_at": result["reported_at"],
                    },
                    "validation": {
                        "status": result["validation_status"],
                        "receipt_id": result["validation_receipt_id"],
                        "receipt_ids": result["references"]["validation_receipt_ids"],
                    },
                    "manifest": {
                        "id": result["manifest_id"],
                        "hash": result["references"]["manifest_hash"],
                    },
                    "lineage": result["lineage"],
                    "creator_node_id": result["creator_node_id"],
                    "contribution": result["contribution"],
                    "result_hash": result["result_hash"],
                }
            )
        return {"schema_version": 1, "total": len(reports), "result_reports": reports}

    def inspect_local_audit_events(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        start_time: int | None = None,
        end_time: int | None = None,
        node_id: str | None = None,
        event_type: str | None = None,
        manifest_id: str | None = None,
        receipt_id: str | None = None,
        lineage_id: str | None = None,
        contribution_attribution_id: str | None = None,
    ) -> dict[str, Any]:
        """Read derived local job audit events without mutating stored evidence."""
        filters = {
            "event_type": event_type,
            "node_id": node_id,
            "manifest_id": manifest_id,
            "receipt_id": receipt_id,
            "lineage_id": lineage_id,
            "contribution_attribution_id": contribution_attribution_id,
        }
        for name, value in filters.items():
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise RuntimeServiceError(f"{name} must be a non-empty string")
        for name, time_value in (("start_time", start_time), ("end_time", end_time)):
            if time_value is not None and (
                not isinstance(time_value, int) or isinstance(time_value, bool)
            ):
                raise RuntimeServiceError(f"{name} must be an integer Unix timestamp")
        if start_time is not None and end_time is not None and start_time > end_time:
            raise RuntimeServiceError("start_time must not be greater than end_time")
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise RuntimeServiceError("limit must be an integer between 1 and 100")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise RuntimeServiceError("offset must be a non-negative integer")
        if event_type is not None and event_type not in {
            "job_submitted",
            "job_executed",
        }:
            raise RuntimeServiceError(
                "event_type must be job_submitted or job_executed"
            )
        if manifest_id is not None and not self._is_local_job_id(manifest_id):
            raise RuntimeServiceError("manifest_id must be a local job ID")
        if receipt_id is not None:
            self._job_id_from_receipt_id(receipt_id)
        if lineage_id is not None and not self._is_local_lineage_id(lineage_id):
            raise RuntimeServiceError("lineage_id must be a local lineage ID")
        if (
            contribution_attribution_id is not None
            and not self._is_local_attribution_id(contribution_attribution_id)
        ):
            raise RuntimeServiceError(
                "contribution_attribution_id must be a local contribution attribution ID"
            )
        directory = self.paths.data_dir / "job-submissions"
        events = (
            [
                event
                for path in sorted(directory.glob("*.json"))
                for event in self._audit_events_for_manifest(path)
            ]
            if directory.exists()
            else []
        )

        def matches(event: dict[str, Any]) -> bool:
            artifacts = event["artifacts"]
            return (
                (start_time is None or event["timestamp"] >= start_time)
                and (end_time is None or event["timestamp"] <= end_time)
                and (event_type is None or event["event_type"] == event_type)
                and (
                    node_id is None
                    or node_id in {event["actor_node_id"], event["creator_node_id"]}
                )
                and (manifest_id is None or artifacts["manifest_id"] == manifest_id)
                and (receipt_id is None or artifacts["receipt_id"] == receipt_id)
                and (lineage_id is None or artifacts["lineage_id"] == lineage_id)
                and (
                    contribution_attribution_id is None
                    or artifacts["contribution_attribution_id"]
                    == contribution_attribution_id
                )
            )

        events = [event for event in events if matches(event)]
        events.sort(
            key=lambda event: (event["timestamp"], event["event_id"]), reverse=True
        )
        return {
            "schema_version": 1,
            "network_mode": "local-only-no-p2p",
            "query": {
                **filters,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
                "offset": offset,
            },
            "total_matching": len(events),
            "events": events[offset : offset + limit],
        }

    def _audit_events_for_manifest(self, manifest_path: Path) -> list[dict[str, Any]]:
        manifest = self._load_local_job_document(
            manifest_path, "job submission manifest"
        )
        job_id = manifest.get("job", {}).get("job_id")
        creator = manifest.get("creator_node_id")
        submitted_at = manifest.get("submitted_at")
        lineage = manifest.get("lineage")
        attribution = manifest.get("contribution_attribution")
        if (
            not self._is_local_job_id(job_id)
            or manifest_path.stem != job_id
            or not isinstance(creator, str)
            or not creator
            or not isinstance(submitted_at, int)
            or isinstance(submitted_at, bool)
            or not _provenance_matches_job(lineage, attribution, job_id, creator)
        ):
            raise RuntimeServiceError(
                "job submission manifest has invalid audit evidence"
            )
        lineage = cast(dict[str, Any], lineage)
        artifacts: dict[str, Any] = {
            "manifest_id": job_id,
            "manifest_ref": f"data/job-submissions/{job_id}.json",
            "receipt_id": None,
            "receipt_ref": None,
            "lineage_id": f"local-lineage-{job_id}",
            "lineage_parent_refs": list(lineage["parent_refs"]),
            "contribution_attribution_id": f"local-contribution-{job_id}",
            "contribution_attribution": attribution,
        }
        events = [
            {
                "event_id": f"local-audit-{job_id}-submitted",
                "timestamp": submitted_at,
                "event_type": "job_submitted",
                "actor_node_id": creator,
                "creator_node_id": creator,
                "artifacts": artifacts,
                "validation_status": "pending",
            }
        ]
        status_path = self.paths.data_dir / "job-status" / f"{job_id}.json"
        if not status_path.exists():
            return events
        status = self._load_local_job_document(status_path, "job status record")
        validation = status.get("validation")
        if not isinstance(validation, dict):
            return events
        receipt_ref = validation.get("receipt_ref")
        expected_receipt_ref = f"data/job-validation-receipts/{job_id}.json"
        worker = status.get("worker_node_id")
        if (
            status.get("job_id") != job_id
            or not isinstance(worker, str)
            or not worker
            or not isinstance(validation, dict)
            or receipt_ref != expected_receipt_ref
        ):
            raise RuntimeServiceError("job status has invalid audit evidence")
        receipt = self._load_local_job_document(
            self.paths.home / expected_receipt_ref, "validation receipt"
        )
        exported_receipt = self.get_local_validation_receipt(work_id=job_id)
        validated_at = receipt.get("validated_at")
        receipt_validation = receipt.get("validation")
        status_attribution = status.get("contribution_attribution")
        if (
            receipt.get("job_id") != job_id
            or receipt.get("receipt_id") != self._receipt_id_for_job(job_id)
            or not _is_utc_timestamp(validated_at)
            or not isinstance(receipt_validation, dict)
            or not isinstance(receipt_validation.get("valid"), bool)
            or validation.get("passed") is not receipt_validation["valid"]
            or not isinstance(status_attribution, dict)
            or status_attribution.get("job_id") != job_id
            or status_attribution.get("creator_node_id") != creator
        ):
            raise RuntimeServiceError("validation receipt has invalid audit evidence")
        events.append(
            {
                "event_id": f"local-audit-{job_id}-executed",
                # Audit event v1 uses integer Unix seconds for sorting and filters;
                # the receipt keeps the canonical ISO 8601 completion timestamp.
                "timestamp": _utc_timestamp_to_unix_seconds(cast(str, validated_at)),
                "event_type": "job_executed",
                "actor_node_id": worker,
                "creator_node_id": creator,
                "artifacts": {
                    **artifacts,
                    "receipt_id": receipt["receipt_id"],
                    "receipt_ref": receipt_ref,
                    "validation_method": exported_receipt["validation_method"],
                    "contribution_attribution": status_attribution,
                },
                "validation_status": (
                    "passed" if receipt_validation["valid"] else "failed"
                ),
            }
        )
        return events

    def contribution_summary(self) -> dict[str, Any]:
        """Read local job evidence into a deterministic, non-mutating summary."""

        manifests = self.paths.data_dir / "job-submissions"
        statuses = self.paths.data_dir / "job-status"
        receipts = self.paths.data_dir / "job-validation-receipts"
        job_ids = sorted(
            {
                path.stem
                for directory in (manifests, statuses, receipts)
                if directory.exists()
                for path in directory.glob("*.json")
            }
        )
        items = [self._contribution_summary_item(job_id) for job_id in job_ids]
        accepted_work_count = sum(
            1 for item in items if item["acceptance_status"] == "accepted"
        )
        validation_status_counts = {
            status: sum(1 for item in items if item["validation_status"] == status)
            for status in ("accepted", "rejected", "pending", "unavailable", "invalid")
        }
        validated_at_values = sorted(
            item["validated_at"]
            for item in items
            if item["validation_status"] in {"accepted", "rejected"}
            and _is_utc_timestamp(item["validated_at"])
        )
        current_node_id = _config_node_id(self.load_config())
        return {
            "schema_version": LOCAL_JOB_SUBMISSION_SCHEMA_VERSION,
            "network_mode": "local-only-no-p2p",
            "summary_status": "empty" if not items else "recorded",
            "current_node_identity": {
                "node_id": current_node_id,
            },
            "contribution_count": len(items),
            "accepted_work_count": accepted_work_count,
            "non_accepted_work_count": len(items) - accepted_work_count,
            "accepted_count": validation_status_counts["accepted"],
            "rejected_count": validation_status_counts["rejected"],
            "pending_count": validation_status_counts["pending"],
            "unavailable_count": validation_status_counts["unavailable"],
            "invalid_count": validation_status_counts["invalid"],
            "latest_receipt_time": (
                validated_at_values[-1] if validated_at_values else None
            ),
            "items": items,
        }

    def get_local_validation_receipt(
        self,
        *,
        receipt_id: str | None = None,
        work_id: str | None = None,
        latest: bool = False,
    ) -> dict[str, Any]:
        """Return one stored local receipt without creating validation evidence."""

        selectors = sum(value is not None for value in (receipt_id, work_id)) + int(
            latest
        )
        if selectors != 1:
            raise RuntimeServiceError(
                "exactly one of receipt_id, work_id, or latest is required"
            )
        if receipt_id is not None:
            job_id = self._job_id_from_receipt_id(receipt_id)
        elif work_id is not None:
            if not self._is_local_job_id(work_id):
                raise RuntimeServiceError("work_id must be a local job ID")
            job_id = work_id
        else:
            job_id = self._latest_validation_receipt_work_id()

        receipt_ref = f"data/job-validation-receipts/{job_id}.json"
        receipt_path = self.paths.home / receipt_ref
        if not receipt_path.exists():
            raise ValidationReceiptNotFoundError("local validation receipt not found")
        receipt = self._load_local_job_document(receipt_path, "validation receipt")
        expected_receipt_id = self._receipt_id_for_job(job_id)
        result_ref = receipt.get("result_ref")
        validator_id = receipt.get("validator_id")
        executor_node_id = receipt.get("executor_node_id")
        validation = receipt.get("validation")
        if receipt.get("version") != 6:
            raise RuntimeServiceError("validation receipt has unsupported version")
        if receipt.get("job_id") != job_id:
            raise RuntimeServiceError("validation receipt does not match its work ID")
        if receipt.get("validation_receipt_id") != expected_receipt_id:
            raise RuntimeServiceError("validation receipt has invalid receipt ID")
        expected_result_ref = f"data/job-results/{job_id}.json"
        if result_ref != expected_result_ref:
            raise RuntimeServiceError(
                "validation receipt does not match its work result"
            )
        if not isinstance(validator_id, str) or not validator_id:
            raise RuntimeServiceError("validation receipt has no validator identity")
        if not isinstance(executor_node_id, str) or not executor_node_id:
            raise RuntimeServiceError("validation receipt has no executor identity")
        try:
            validate_validator_software_metadata(
                receipt.get("validator_software"),
                receipt_schema_version=receipt["version"],
            )
        except (KeyError, ValueError) as exc:
            raise RuntimeServiceError(
                "validation receipt has missing or invalid validator software metadata"
            ) from exc
        if not isinstance(validation, dict) or not isinstance(
            validation.get("valid"), bool
        ):
            raise RuntimeServiceError(
                "validation receipt has invalid validation evidence"
            )
        receipt_status = receipt.get("status")
        rejection_reason = receipt.get("rejection_reason")
        if receipt_status not in {"accepted", "rejected"}:
            raise RuntimeServiceError(
                "validation receipt status must be accepted or rejected"
            )
        validation_method = _validated_runtime_validation_method(
            receipt.get("validation_method")
        )

        manifest_ref = f"data/job-submissions/{job_id}.json"
        status_ref = f"data/job-status/{job_id}.json"
        manifest_path = self.paths.home / manifest_ref
        status_path = self.paths.home / status_ref
        if not manifest_path.exists() or not status_path.exists():
            raise RuntimeServiceError(
                "validation receipt has incomplete local evidence"
            )
        manifest = self._load_local_job_document(
            manifest_path, "job submission manifest"
        )
        status = self._load_local_job_document(status_path, "job status record")
        result = self._load_local_job_document(
            self.paths.home / expected_result_ref, "job result record"
        )
        _, expected_payload_hash = _validated_input_payload(
            manifest.get("job", {}).get("input_payload")
        )
        lineage = manifest.get("lineage")
        attribution = status.get("contribution_attribution")
        requester_identity = _requester_identity(manifest.get("requester_identity"))
        if (
            not isinstance(manifest.get("creator_node_id"), str)
            or manifest.get("job", {}).get("job_id") != job_id
            or status.get("job_id") != job_id
            or receipt.get("requester_identity") != requester_identity
            or receipt.get("manifest_ref") != manifest_ref
            or manifest.get("job", {}).get("input_payload_hash")
            != expected_payload_hash
            or receipt.get("input_payload_hash") != expected_payload_hash
            or receipt.get("contribution_attribution") != attribution
            or receipt.get("result_hash") != result.get("result_hash")
            or receipt.get("model_expert_id") != result.get("model_expert_id")
            or receipt.get("expert_version") != result.get("expert_version")
            or not _provenance_matches_job(
                lineage, attribution, job_id, manifest.get("creator_node_id")
            )
        ):
            raise RuntimeServiceError(
                "validation receipt has incomplete provenance evidence"
            )
        lineage = cast(dict[str, Any], lineage)
        if (
            validation_method.get("manifest_ref") != manifest_ref
            or validation_method.get("creator_node_id") != manifest["creator_node_id"]
            or validation_method.get("work_id") != job_id
            or validation_method.get("lineage_parent_refs")
            != lineage.get("parent_refs")
            or validation_method.get("contribution_attribution") != attribution
        ):
            raise RuntimeServiceError(
                "validation method does not match receipt provenance"
            )
        receipt_execution = receipt.get("execution")
        executor_started_at = (
            receipt_execution.get("executor_started_at")
            if isinstance(receipt_execution, dict)
            else None
        )
        executor_finished_at = (
            receipt_execution.get("executor_finished_at")
            if isinstance(receipt_execution, dict)
            else None
        )
        duration_ms = (
            receipt_execution.get("duration_ms")
            if isinstance(receipt_execution, dict)
            else None
        )
        result_lineage = result.get("lineage")
        manifest_capability = manifest.get("job", {}).get("requested_capability")
        capability = receipt.get("capability")
        try:
            validate_job_result_document(result)
        except ValueError as exc:
            raise RuntimeServiceError("job result record violates its schema") from exc
        _validate_stored_output_payload(
            result["output_payload"],
            root=self.paths.home,
            job_id=job_id,
            expected_output_hash=receipt.get("output_hash"),
            require_payload=result.get("status") == "succeeded",
        )
        if (
            not isinstance(manifest_capability, dict)
            or capability != manifest_capability.get("identifier")
            or result.get("capability") != capability
        ):
            raise RuntimeServiceError(
                "validation receipt capability does not match manifest result"
            )
        if (
            not isinstance(receipt_execution, dict)
            or receipt_execution.get("executor_node_id") != executor_node_id
            or executor_started_at != result.get("started_at")
            or executor_finished_at != result.get("finished_at")
            or not _is_utc_timestamp_before_or_at_timestamp(
                executor_started_at, executor_finished_at
            )
            or not _is_utc_timestamp_before_or_at(
                executor_finished_at, receipt_execution.get("executed_at")
            )
            or result.get("job_id") != job_id
            or result.get("executor_node_id") != executor_node_id
            or executor_node_id != validator_id
            or executor_node_id != status.get("executor_node_id")
            or result.get("creator_node_id") != manifest.get("creator_node_id")
            or not isinstance(result_lineage, dict)
            or result_lineage.get("parent_job_ids") != lineage.get("parent_refs")
            or (
                duration_ms is not None
                and (
                    not isinstance(duration_ms, int)
                    or isinstance(duration_ms, bool)
                    or duration_ms
                    != _duration_ms(
                        cast(str, executor_started_at), cast(str, executor_finished_at)
                    )
                    or duration_ms != result.get("duration_ms")
                )
            )
        ):
            raise RuntimeServiceError(
                "validation receipt has invalid executor timing evidence"
            )
        expected_status = (
            "accepted"
            if validation["valid"] and result.get("status") == "succeeded"
            else "rejected"
        )
        if receipt_status != expected_status:
            raise RuntimeServiceError(
                "validation receipt status does not match validation evidence"
            )
        if receipt_status == "rejected" and (
            not isinstance(rejection_reason, str) or not rejection_reason.strip()
        ):
            raise RuntimeServiceError(
                "rejected validation receipt has no rejection reason"
            )
        if receipt_status == "accepted" and rejection_reason is not None:
            raise RuntimeServiceError(
                "accepted validation receipt has a rejection reason"
            )
        next_local_action = receipt.get("next_local_action")
        if not isinstance(next_local_action, str) or not next_local_action.strip():
            raise RuntimeServiceError("validation receipt has no next local action")
        if status.get("validation", {}).get("receipt_ref") != receipt_ref:
            raise RuntimeServiceError(
                "job status does not reference validation receipt"
            )

        validated_at = receipt.get("validated_at")
        if not _is_utc_timestamp(validated_at):
            raise RuntimeServiceError(
                "validation receipt has missing or invalid validated_at timestamp"
            )
        exported_receipt = {
            "schema_version": 5,
            "network_mode": "local-only-no-p2p",
            "validation_scope": "local-only-not-consensus",
            "receipt_id": expected_receipt_id,
            "validation_receipt_id": expected_receipt_id,
            "job_id": job_id,
            "work_id": job_id,
            "capability": capability,
            "model_expert_id": result["model_expert_id"],
            "expert_version": result["expert_version"],
            "creator_node_id": manifest["creator_node_id"],
            "executor_node_id": executor_node_id,
            "requester_identity": requester_identity,
            "manifest_ref": manifest_ref,
            "input_payload_hash": expected_payload_hash,
            "result_hash": result["result_hash"],
            "status": receipt_status,
            "rejection_reason": rejection_reason,
            "next_local_action": next_local_action,
            "validation_status": "passed" if validation["valid"] else "failed",
            "validation": validation,
            "validation_method": validation_method,
            "validated_at": validated_at,
            "executor_started_at": receipt_execution["executor_started_at"],
            "executor_finished_at": receipt_execution["executor_finished_at"],
            "validator_identity": validator_id,
            "validator_software": receipt["validator_software"],
            "lineage_parent_ids": lineage["parent_refs"],
            "contribution_attribution": attribution,
            "evidence": {
                "receipt_ref": receipt_ref,
                "result_ref": result_ref,
                "status_ref": status_ref,
            },
        }
        # Older local receipts did not record a duration. Preserve them without
        # inventing one; newly captured receipts include the observed value.
        if duration_ms is not None:
            exported_receipt["duration_ms"] = duration_ms
        return exported_receipt

    def list_local_validation_receipts(self) -> dict[str, Any]:
        """List shaped local receipt evidence without creating or changing it.

        Each item uses the same persisted-evidence checks as the detail lookup.
        The list retains auditable provenance while omitting timing and result
        detail; callers can use its stable receipt_id for the full receipt.
        """

        directory = self.paths.data_dir / "job-validation-receipts"
        job_ids = (
            sorted(
                path.stem
                for path in directory.glob("*.json")
                if self._is_local_job_id(path.stem)
            )
            if directory.exists()
            else []
        )
        receipts = [
            self.get_local_validation_receipt(work_id=job_id) for job_id in job_ids
        ]
        return {
            "schema_version": 1,
            "network_mode": "local-only-no-p2p",
            "total": len(receipts),
            "validation_receipts": [
                {
                    "receipt_id": receipt["receipt_id"],
                    "validation_receipt_id": receipt["validation_receipt_id"],
                    "work_id": receipt["work_id"],
                    "creator_node_id": receipt["creator_node_id"],
                    "manifest_ref": receipt["manifest_ref"],
                    "lineage_parent_ids": receipt["lineage_parent_ids"],
                    "contribution_attribution": receipt["contribution_attribution"],
                    "status": receipt["status"],
                    "validation_status": receipt["validation_status"],
                    "validated_at": receipt["validated_at"],
                    "validation_summary": receipt["validation"],
                }
                for receipt in receipts
            ],
        }

    def trace_local_job_attribution(self, job_id: str) -> dict[str, Any]:
        """Return one validation-gated local job attribution chain.

        This is an inspection-only join over persisted evidence. Every link is
        checked directly; attribution is never inferred from loose metadata.
        """
        if not self._is_local_job_id(job_id):
            raise RuntimeServiceError("job_id must be a local job ID")
        manifest_ref = f"data/job-submissions/{job_id}.json"
        manifest_path = self.paths.home / manifest_ref
        if not manifest_path.exists():
            raise RuntimeServiceError("trace missing required local job manifest")
        manifest = self._load_local_job_document(
            manifest_path, "job submission manifest"
        )
        manifest_job = manifest.get("job")
        creator_node_id = manifest.get("creator_node_id")
        if (
            not isinstance(manifest_job, dict)
            or manifest_job.get("job_id") != job_id
            or not isinstance(creator_node_id, str)
            or not creator_node_id
        ):
            raise RuntimeServiceError("trace has invalid local job manifest linkage")

        result = self.get_local_job_result(job_id)
        try:
            receipt = self.get_local_validation_receipt(work_id=job_id)
        except ValidationReceiptNotFoundError:
            raise
        except RuntimeServiceError as exc:
            raise RuntimeServiceError(
                "trace validation receipt is missing or has invalid lineage evidence"
            ) from exc
        status_path = self._job_status_path(job_id)
        if not status_path.exists():
            raise RuntimeServiceError("trace missing required local job status")
        status = self._load_local_job_document(status_path, "job status record")
        contribution = status.get("contribution_attribution")
        expected_receipt_id = self._receipt_id_for_job(job_id)
        expected_result_id = f"local-result-{job_id}"
        manifest_id = canonical_json_hash(manifest, prefix="sha256:")
        contribution_id = f"local-contribution-{job_id}"

        if receipt["status"] != "accepted" or receipt["validation_status"] != "passed":
            raise RuntimeServiceError(
                "trace requires an accepted passed validation receipt"
            )
        if (
            result.get("result_id") != expected_result_id
            or result.get("job_id") != job_id
            or result.get("validation_receipt_id") != expected_receipt_id
            or result.get("references", {}).get("validation_receipt_ids")
            != [expected_receipt_id]
            or result.get("manifest_id") != manifest_id
            or result.get("references", {}).get("manifest_hash") != manifest_id
            or result.get("creator_node_id") != creator_node_id
        ):
            raise RuntimeServiceError(
                "trace result does not match job manifest lineage"
            )
        if (
            receipt["receipt_id"] != expected_receipt_id
            or receipt["job_id"] != job_id
            or receipt["manifest_ref"] != manifest_ref
            or receipt["result_hash"] != result.get("result_hash")
            or receipt["creator_node_id"] != creator_node_id
        ):
            raise RuntimeServiceError(
                "trace validation receipt does not match result lineage"
            )
        if (
            not isinstance(contribution, dict)
            or contribution.get("job_id") != job_id
            or contribution.get("creator_node_id") != creator_node_id
            or contribution != receipt["contribution_attribution"]
        ):
            raise RuntimeServiceError(
                "trace contribution does not match validation receipt"
            )

        return {
            "schema_version": 1,
            "trace_scope": "local-only-validation-gated-attribution",
            "job_id": job_id,
            "chain": [
                {
                    "record_type": "job",
                    "job_id": job_id,
                    "result_id": expected_result_id,
                    "manifest_ref": manifest_ref,
                },
                {
                    "record_type": "result",
                    "result_id": expected_result_id,
                    "job_id": result["job_id"],
                    "validation_receipt_id": result["validation_receipt_id"],
                    "manifest_id": result["manifest_id"],
                },
                {
                    "record_type": "validation_receipt",
                    "validation_receipt_id": receipt["receipt_id"],
                    "job_id": receipt["job_id"],
                    "result_id": expected_result_id,
                    "manifest_ref": receipt["manifest_ref"],
                    "contribution_id": contribution_id,
                },
                {
                    "record_type": "contribution",
                    "contribution_id": contribution_id,
                    "job_id": contribution["job_id"],
                    "validation_receipt_id": expected_receipt_id,
                    "manifest_ref": manifest_ref,
                    "creator_node_id": contribution["creator_node_id"],
                },
                {
                    "record_type": "manifest",
                    "manifest_id": manifest_id,
                    "manifest_ref": manifest_ref,
                    "job_id": manifest_job["job_id"],
                    "creator_node_id": creator_node_id,
                },
                {"record_type": "creator_node", "creator_node_id": creator_node_id},
            ],
        }

    @staticmethod
    def _receipt_id_for_job(job_id: str) -> str:
        return f"{VALIDATION_RECEIPT_ID_PREFIX}{job_id}"

    def _job_id_from_receipt_id(self, receipt_id: str) -> str:
        if not isinstance(receipt_id, str) or not receipt_id.startswith(
            VALIDATION_RECEIPT_ID_PREFIX
        ):
            raise RuntimeServiceError(
                "receipt_id must be a local validation receipt ID"
            )
        job_id = receipt_id.removeprefix(VALIDATION_RECEIPT_ID_PREFIX)
        if not self._is_local_job_id(job_id):
            raise RuntimeServiceError(
                "receipt_id must be a local validation receipt ID"
            )
        return job_id

    def _latest_validation_receipt_work_id(self) -> str:
        directory = self.paths.data_dir / "job-validation-receipts"
        candidates = (
            [
                path
                for path in directory.glob("*.json")
                if self._is_local_job_id(path.stem)
            ]
            if directory.exists()
            else []
        )
        if not candidates:
            raise ValidationReceiptNotFoundError("local validation receipt not found")
        return max(
            candidates, key=lambda path: (path.stat().st_mtime_ns, path.name)
        ).stem

    def _contribution_summary_item(self, job_id: str) -> dict[str, Any]:
        manifest_ref = f"data/job-submissions/{job_id}.json"
        status_ref = f"data/job-status/{job_id}.json"
        expected_receipt_ref = f"data/job-validation-receipts/{job_id}.json"
        expected_receipt_path = self.paths.home / expected_receipt_ref
        manifest_path = self.paths.data_dir / "job-submissions" / f"{job_id}.json"
        status_path = self.paths.data_dir / "job-status" / f"{job_id}.json"
        evidence_errors: list[str] = []
        manifest = self._load_summary_document(
            manifest_path, "job submission manifest", evidence_errors
        )
        status = self._load_summary_document(
            status_path, "job status record", evidence_errors
        )
        status_validation = status.get("validation")
        validation = status_validation if isinstance(status_validation, dict) else {}
        receipt_ref = validation.get("receipt_ref")
        receipt: dict[str, Any] = {}
        validation_method: object = None
        if isinstance(receipt_ref, str) and receipt_ref:
            if receipt_ref != expected_receipt_ref:
                evidence_errors.append(
                    "validation receipt reference does not match work item"
                )
            else:
                receipt = self._load_summary_document(
                    expected_receipt_path,
                    "validation receipt",
                    evidence_errors,
                )
                if receipt:
                    if receipt.get("version") != 6:
                        evidence_errors.append(
                            "validation receipt has unsupported version"
                        )
                    if not _is_utc_timestamp(receipt.get("validated_at")):
                        evidence_errors.append(
                            "validation receipt has missing or invalid validated_at timestamp"
                        )
                    try:
                        validation_method = _validated_runtime_validation_method(
                            receipt.get("validation_method")
                        )
                    except RuntimeServiceError as exc:
                        evidence_errors.append(str(exc))
        elif expected_receipt_path.exists():
            evidence_errors.append(
                "job status record has no validation receipt reference"
                if status
                else "validation receipt has no matching job status reference"
            )
            receipt = self._load_summary_document(
                expected_receipt_path,
                "validation receipt",
                evidence_errors,
            )
        elif status and status.get("status") in LOCAL_JOB_TERMINAL_STATES:
            evidence_errors.append(
                "job status record has no validation receipt reference"
            )

        receipt_passed = (
            isinstance(receipt.get("validation"), dict)
            and receipt["validation"].get("valid") is True
        )
        manifest_job = manifest.get("job")
        if manifest:
            if not isinstance(manifest_job, dict):
                evidence_errors.append(
                    "job submission manifest has invalid job evidence"
                )
            elif manifest_job.get("job_id") != job_id:
                evidence_errors.append(
                    "job submission manifest does not match work item"
                )
        if status and status.get("job_id") != job_id:
            evidence_errors.append("job status record does not match work item")
        if receipt and receipt.get("job_id") != job_id:
            evidence_errors.append("validation receipt does not match work item")
        if receipt and receipt.get("validation_receipt_id") != self._receipt_id_for_job(
            job_id
        ):
            evidence_errors.append("validation receipt has invalid receipt ID")
        expected_result_ref = f"data/job-results/{job_id}.json"
        if receipt and receipt.get("result_ref") != expected_result_ref:
            evidence_errors.append("validation receipt does not match work result")
        accepted = (
            not evidence_errors
            and status.get("status") == "succeeded"
            and validation.get("passed") is True
            and receipt_passed
        )
        attribution = status.get("contribution_attribution")
        if not isinstance(attribution, dict):
            manifest_attribution = manifest.get("contribution_attribution")
            attribution = (
                manifest_attribution if isinstance(manifest_attribution, dict) else {}
            )
        creator_node_id = attribution.get("creator_node_id")
        contributing_node_id = attribution.get(
            "worker_node_id", status.get("worker_node_id")
        )
        if manifest and not isinstance(creator_node_id, str):
            evidence_errors.append("contribution attribution has no creator node ID")
        if manifest and attribution.get("job_id") != job_id:
            evidence_errors.append("contribution attribution does not match work item")
        elif manifest and creator_node_id != manifest.get("creator_node_id"):
            evidence_errors.append(
                "contribution attribution creator does not match manifest"
            )
        if status.get("status") == "succeeded" and not isinstance(
            contributing_node_id, str
        ):
            evidence_errors.append(
                "contribution attribution has no contributing node ID"
            )
        elif status and contributing_node_id != status.get("worker_node_id"):
            evidence_errors.append(
                "contribution attribution worker does not match job status"
            )
        if receipt and receipt.get("validator_id") != status.get("worker_node_id"):
            evidence_errors.append("validation receipt validator does not match worker")
        lineage = manifest.get("lineage")
        if not isinstance(lineage, dict):
            lineage = {}
            if manifest:
                evidence_errors.append(
                    "job submission manifest has invalid lineage evidence"
                )
        lineage_links = lineage.get("parent_refs", [])
        if manifest and lineage.get("job_id") != job_id:
            evidence_errors.append("job submission lineage does not match work item")
        if not isinstance(lineage_links, list):
            evidence_errors.append("job submission manifest has invalid lineage links")
            lineage_links = []
        provenance_matches = _provenance_matches_job(
            lineage, attribution, job_id, manifest.get("creator_node_id")
        )
        if (
            receipt
            and isinstance(validation_method, dict)
            and (
                validation_method.get("manifest_ref") != manifest_ref
                or validation_method.get("creator_node_id")
                != manifest.get("creator_node_id")
                or validation_method.get("work_id") != job_id
                or validation_method.get("lineage_parent_refs") != lineage_links
                or validation_method.get("contribution_attribution") != attribution
            )
        ):
            evidence_errors.append(
                "validation method does not match receipt provenance"
            )
        receipt_validation = receipt.get("validation")
        receipt_valid = (
            receipt_validation.get("valid")
            if isinstance(receipt_validation, dict)
            else None
        )
        if receipt and not isinstance(receipt_valid, bool):
            evidence_errors.append("validation receipt has invalid validation evidence")
        if receipt and validation.get("passed") is not receipt_valid:
            evidence_errors.append("job status validation does not match receipt")
        expected_receipt_status = "accepted" if receipt_valid is True else "rejected"
        if receipt and receipt.get("status") != expected_receipt_status:
            evidence_errors.append("validation receipt status is inconsistent")
        accepted = accepted and not evidence_errors
        if evidence_errors:
            validation_status = (
                "unavailable"
                if not manifest
                or not status_path.exists()
                or (
                    not receipt
                    and receipt_ref == expected_receipt_ref
                    and not expected_receipt_path.exists()
                )
                else "invalid"
            )
        elif receipt_passed:
            validation_status = "accepted"
        elif (
            isinstance(receipt_validation, dict)
            and receipt_validation.get("valid") is False
        ):
            validation_status = "rejected"
        else:
            validation_status = "pending"
        return {
            "work_item_id": job_id,
            "status": status.get("status", "incomplete"),
            "acceptance_status": "accepted"
            if accepted
            else "degraded"
            if evidence_errors
            else "not_accepted",
            "validation_status": validation_status,
            "creator_node_id": creator_node_id,
            "contributing_node_id": contributing_node_id,
            "contribution_attribution": attribution,
            "attribution_id": (
                f"local-contribution-{job_id}" if provenance_matches else None
            ),
            "manifest_ref": manifest_ref,
            "manifest_id": (
                canonical_json_hash(manifest, prefix="sha256:") if manifest else None
            ),
            "status_ref": status_ref if status_path.exists() else None,
            "validation_receipt_ref": receipt_ref
            if isinstance(receipt_ref, str)
            else None,
            "validation_receipt_id": receipt.get("validation_receipt_id"),
            "lineage_links": lineage_links,
            "lineage_parent_ids": lineage_links,
            "lineage_id": f"local-lineage-{job_id}" if provenance_matches else None,
            "validated_at": receipt.get("validated_at"),
            "timestamps": {"submitted_at": manifest.get("submitted_at")},
            "evidence_errors": evidence_errors,
        }

    @staticmethod
    def _load_summary_document(
        path: Path, label: str, evidence_errors: list[str]
    ) -> dict[str, Any]:
        if not path.exists():
            evidence_errors.append(f"missing {label}: {path.name}")
            return {}
        try:
            return NodeRuntimeService._load_local_job_document(path, label)
        except RuntimeServiceError as exc:
            evidence_errors.append(str(exc))
            return {}

    def execute_submitted_local_job(
        self, job_id: str, worker_node_id: str
    ) -> dict[str, Any]:
        """Run a queued local submission; this is not a daemon or remote boundary."""

        before = self.get_local_job_status(job_id)
        if before["status"] == "not_found":
            raise RuntimeServiceError("local job not found")
        if before["status"] != "queued":
            raise RuntimeServiceError("local job is not queued")
        if not isinstance(worker_node_id, str) or not worker_node_id.strip():
            raise RuntimeServiceError("worker_node_id must be a non-empty string")
        manifest = self._load_local_job_document(
            self.paths.data_dir / "job-submissions" / f"{job_id}.json",
            "job submission manifest",
        )
        job_data = manifest["job"]
        requested_capability = job_data.get("requested_capability")
        if (
            not isinstance(requested_capability, dict)
            or set(requested_capability) != {"identifier"}
            or not isinstance(requested_capability.get("identifier"), str)
            or not requested_capability["identifier"]
        ):
            raise RuntimeServiceError(
                "job submission manifest requested_capability is invalid"
            )
        capability = requested_capability["identifier"]
        input_payload, payload_hash = _validated_input_payload(
            job_data.get("input_payload")
        )
        if job_data.get("input_payload_hash") != payload_hash:
            raise RuntimeServiceError(
                "job submission manifest input_payload hash is invalid"
            )
        self._append_job_execution_started_audit_event(
            job_id=job_id,
            worker_node_id=worker_node_id,
            manifest=manifest,
            manifest_ref=f"data/job-submissions/{job_id}.json",
            manifest_hash=canonical_json_hash(manifest, prefix="sha256:"),
            payload_hash=payload_hash,
        )
        self._transition_local_job_state(
            job_id,
            "running",
            updates={
                "worker_node_id": worker_node_id,
                "executor_node_id": worker_node_id,
            },
        )
        job = Job(
            job_id=job_id,
            job_type=job_data["job_type"],
            payload=input_payload["content"],
        )
        local_safety = job_data.get("local_safety")
        if local_safety is not None and not isinstance(local_safety, dict):
            raise RuntimeServiceError("job submission manifest local_safety is invalid")
        executor_started_at = _utc_timestamp()
        try:
            result = run_local_job(
                job,
                NodeIdentity(node_id=worker_node_id),
                timeout_seconds=(
                    local_safety.get("timeout_seconds")
                    if local_safety is not None
                    else None
                ),
                cancellation_requested=(
                    local_safety.get("cancellation_requested", False)
                    if local_safety is not None
                    else False
                ),
            )
        except Exception as exc:  # noqa: BLE001 - justification: isolate arbitrary executor failures without persisting their messages.
            result = JobResult(
                job_id=job_id,
                node_id=worker_node_id,
                status="failed",
                output=None,
                # Preserve an auditable error class without persisting arbitrary
                # executor text, which can expose local paths or implementation
                # details in a receipt.
                error=f"local execution error: {type(exc).__name__}",
                contribution_units=0,
            )
        finally:
            executor_finished_at = _utc_timestamp()
        validation = validate_job_result(job, result)
        validated_at = _validation_completed_at()
        result_ref = f"data/job-results/{job_id}.json"
        receipt_ref = f"data/job-validation-receipts/{job_id}.json"
        manifest_ref = f"data/job-submissions/{job_id}.json"
        manifest_id = canonical_json_hash(manifest, prefix="sha256:")
        output_manifest_id = canonical_json_hash(
            {"output": result.output}, prefix="sha256:"
        )
        execution_metadata = {
            "executor_name": LocalRunner.EXECUTOR_NAME,
            "executor_version": LocalRunner.EXECUTOR_VERSION,
            "input_digest": payload_hash,
            "output_digest": output_manifest_id,
            "executor_started_at": executor_started_at,
            "executor_finished_at": executor_finished_at,
            "duration_ms": _duration_ms(executor_started_at, executor_finished_at),
            "executed_at": int(time.time()),
            "creator_node_id": manifest["creator_node_id"],
            "executor_node_id": worker_node_id,
            "lineage_parent_refs": manifest["lineage"]["parent_refs"],
        }
        succeeded = result.status == "completed" and validation.valid
        output_payload = _output_payload_record(
            result.output, job_id=job_id, root=self.paths.home, store=succeeded
        )
        error = result.error or (
            None if succeeded else f"validation failed: {validation.reason}"
        )
        result_document = {
            "schema_version": JOB_RESULT_SCHEMA_VERSION,
            "result_id": f"local-result-{job_id}",
            "job_id": job_id,
            "task_id": job_id,
            "capability": capability,
            "model_expert_id": (
                f"local-runner:{LocalRunner.EXECUTOR_NAME}@{LocalRunner.EXECUTOR_VERSION}"
            ),
            "expert_version": LocalRunner.EXECUTOR_VERSION,
            "creator_node_id": manifest["creator_node_id"],
            "executor_node_id": worker_node_id,
            "manifest_id": manifest_id,
            "output_payload": output_payload,
            "references": {
                "manifest_hash": manifest_id,
                "manifest_ref": None,
                "artifact_refs": [output_manifest_id] if succeeded else [],
                "validation_receipt_ids": [self._receipt_id_for_job(job_id)],
                "log_refs": [],
            },
            "created_at": _utc_timestamp_from_unix_seconds(manifest["submitted_at"]),
            "status": "succeeded" if succeeded else "failed",
            "exit_code": 0 if succeeded else 1,
            "started_at": executor_started_at,
            "finished_at": executor_finished_at,
            # This is audit metadata generated locally just before persisting
            # the report; it is deliberately excluded from result_hash.
            "reported_at": _utc_timestamp(),
            "duration_ms": _duration_ms(executor_started_at, executor_finished_at),
            "summary": _result_summary(result.output if succeeded else error),
            "error_summary": None if succeeded else _result_summary(error),
            "validation_status": "passed" if validation.valid else "failed",
            "validation_receipt_id": self._receipt_id_for_job(job_id),
            "validator_node_id": worker_node_id,
            "failure_reasons": {
                "execution": result.error,
                "validation": None if validation.valid else validation.reason,
                "malformed_input": None,
                "missing_artifact": None,
            },
            "lineage": {
                "input_manifest_ids": [manifest_id],
                "output_manifest_ids": [output_manifest_id] if succeeded else [],
                "parent_job_ids": manifest["lineage"]["parent_refs"],
                "parent_task_ids": [],
                "artifact_ids": [output_manifest_id] if succeeded else [],
            },
            "contribution": {
                "creator_node_id": manifest["creator_node_id"],
                "executor_node_id": worker_node_id,
                "validator_node_id": worker_node_id,
                "upstream_lineage_sources": manifest["lineage"]["parent_refs"],
                "local_operator_id": None,
            },
        }
        result_document["result_hash"] = canonical_result_document_hash(result_document)
        validate_job_result_document(result_document)
        atomic_create_json(
            self.paths.data_dir / "job-results" / f"{job_id}.json", result_document
        )
        contribution_attribution = {
            **manifest["contribution_attribution"],
            "worker_node_id": worker_node_id,
            "executor_node_id": worker_node_id,
            "validated_contribution_units": result.contribution_units
            if succeeded
            else 0,
        }
        atomic_create_json(
            self.paths.data_dir / "job-validation-receipts" / f"{job_id}.json",
            {
                "version": 6,
                "job_id": job_id,
                "capability": capability,
                "model_expert_id": result_document["model_expert_id"],
                "expert_version": result_document["expert_version"],
                "receipt_id": self._receipt_id_for_job(job_id),
                "validation_receipt_id": self._receipt_id_for_job(job_id),
                "manifest_ref": manifest_ref,
                "input_payload_hash": payload_hash,
                "output_hash": execution_metadata["output_digest"],
                "result_hash": result_document["result_hash"],
                "status": "accepted" if succeeded else "rejected",
                "rejection_reason": None if succeeded else validation.reason,
                "next_local_action": (
                    "retain this local receipt, manifest, and result artifact for replay"
                    if succeeded
                    else "inspect the local result artifact and validation reason; correct "
                    "the work or input before submitting a new local job"
                ),
                "result_ref": result_ref,
                "validator_id": worker_node_id,
                "validator_software": capture_validator_software_metadata(
                    validator_name="deterministic_local_result_check",
                    receipt_schema_version=6,
                ),
                "executor_node_id": worker_node_id,
                "requester_identity": manifest.get("requester_identity"),
                "execution": execution_metadata,
                "creator_node_id": manifest["creator_node_id"],
                "lineage_parent_refs": manifest["lineage"]["parent_refs"],
                "contribution_attribution": contribution_attribution,
                "validation_method": {
                    "kind": "deterministic_local_result_check",
                    "description": (
                        f"Ran the deterministic local {job.job_type} validator against "
                        "the assigned job and executor result. The validator checks "
                        "completion, work identity, contribution units, payload validity, "
                        "and expected output in order; "
                        f"outcome: {validation.reason}."
                    ),
                    "manifest_ref": manifest_ref,
                    "creator_node_id": manifest["creator_node_id"],
                    "work_id": job_id,
                    "lineage_parent_refs": manifest["lineage"]["parent_refs"],
                    "contribution_attribution": contribution_attribution,
                },
                "validation": {
                    **validation.to_dict(),
                    "execution_outcome": result.status,
                },
                # Captured locally immediately after validation completes. This is
                # audit timing, not distributed or consensus time.
                "validated_at": validated_at,
            },
        )
        self._append_validation_receipt_created_audit_event(
            job_id=job_id,
            worker_node_id=worker_node_id,
            manifest=manifest,
            manifest_id=manifest_id,
            manifest_ref=manifest_ref,
            receipt_ref=receipt_ref,
            validation_result="accepted" if succeeded else "rejected",
            contribution_attribution=contribution_attribution,
        )
        self._append_result_reported_audit_event(
            job_id=job_id,
            worker_node_id=worker_node_id,
            manifest=manifest,
            manifest_id=manifest_id,
            manifest_ref=manifest_ref,
            validation_receipt_ref=receipt_ref,
            result_status=result_document["status"],
        )
        succeeded = result.status == "completed" and validation.valid
        error = result.error or (
            None if succeeded else f"validation failed: {validation.reason}"
        )
        self._append_job_execution_finished_audit_event(
            job_id=job_id,
            worker_node_id=worker_node_id,
            manifest=manifest,
            manifest_ref=manifest_ref,
            manifest_id=manifest_id,
            finished_at=executor_finished_at,
            duration_ms=_duration_ms(executor_started_at, executor_finished_at),
            terminal_status=(
                "completed"
                if succeeded
                else "validation-failed"
                if result.status == "completed"
                else "failed"
            ),
            validation_receipt_ref=receipt_ref,
            validator_node_id=worker_node_id,
            # The result artifact is written for both successful and failed
            # executions, and remains useful terminal evidence when validation
            # rejects an otherwise completed result.
            output_artifact_refs=[result_ref],
            error_summary=None if succeeded else _result_summary(error),
            contribution_attribution=contribution_attribution,
        )
        self._transition_local_job_state(
            job_id,
            "succeeded" if succeeded else "failed",
            updates={
                "result": {
                    "ref": result_ref,
                    "summary": result.output if succeeded else None,
                },
                "validation": {
                    "receipt_ref": receipt_ref,
                    "passed": validation.valid,
                    "reason": validation.reason,
                },
                "contribution_attribution": contribution_attribution,
                "error": error,
                "execution_status": {
                    "schema_version": 1,
                    "work_id": job_id,
                    "status": "success" if succeeded else "failure",
                    "creator_node_id": manifest["creator_node_id"],
                    "executor_node_id": worker_node_id,
                    "manifest_ref": f"data/job-submissions/{job_id}.json",
                    "input_lineage_ref": f"data/job-submissions/{job_id}.json#lineage",
                    "output_ref": result_ref,
                    "validation_receipt_ref": receipt_ref,
                    "contribution_attribution": contribution_attribution,
                    "failure_reason": None if succeeded else validation.reason,
                    "error_summary": None if succeeded else error,
                    "retry_eligible": False,
                },
            },
        )
        self._append_event(f"executed local job submission {job_id}")
        return self.get_local_job_status(job_id)

    def _append_validation_receipt_created_audit_event(
        self,
        *,
        job_id: str,
        worker_node_id: str,
        manifest: dict[str, Any],
        manifest_id: str,
        manifest_ref: str,
        receipt_ref: str,
        validation_result: str,
        contribution_attribution: dict[str, Any],
    ) -> None:
        """Append evidence only after the local validation receipt is durable."""

        creator_node_id = manifest.get("creator_node_id")
        lineage = manifest.get("lineage")
        if not isinstance(creator_node_id, str) or not creator_node_id.strip():
            raise RuntimeServiceError(
                "validation receipt audit requires a creator node ID"
            )
        if not isinstance(lineage, dict) or not isinstance(
            lineage.get("parent_refs"), list
        ):
            raise RuntimeServiceError(
                "validation receipt audit requires manifest lineage"
            )
        metadata = contribution_attribution.get("metadata")
        if not isinstance(metadata, dict):
            raise RuntimeServiceError(
                "validation receipt audit requires contribution attribution metadata"
            )
        audit_attribution = {
            "job_id": job_id,
            "creator_node_id": creator_node_id,
            "worker_node_id": worker_node_id,
            "executor_node_id": worker_node_id,
            "metadata_hash": canonical_json_hash(metadata, prefix="sha256:"),
        }
        try:
            append_local_audit_event(
                self.paths.data_dir / "audit" / "validation-receipt-creations.jsonl",
                {
                    "schema_version": 2,
                    "event_id": f"local-audit-{job_id}-validation-receipt-created",
                    "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "event_type": "validation_receipt_created",
                    "actor_node_id": worker_node_id,
                    "creator_node_id": creator_node_id,
                    "local_run_id": job_id,
                    "event_sequence": 3,
                    "work_id": job_id,
                    "manifest_id": manifest_id,
                    "manifest_ref": manifest_ref,
                    "validation_receipt_id": self._receipt_id_for_job(job_id),
                    "validation_receipt_ref": receipt_ref,
                    "validation_result": validation_result,
                    "validator_node_id": worker_node_id,
                    "validator_name": "deterministic_local_result_check",
                    "lineage_refs": [manifest_ref, *lineage["parent_refs"]],
                    "contribution_attribution_ids": [f"local-contribution-{job_id}"],
                    "contribution_attribution": audit_attribution,
                },
            )
        except (LocalAuditEventError, OSError, ValueError) as exc:
            raise RuntimeServiceError(
                f"could not write local validation receipt creation audit event: {exc}"
            ) from exc

    def _append_job_execution_started_audit_event(
        self,
        *,
        job_id: str,
        worker_node_id: str,
        manifest: dict[str, Any],
        manifest_ref: str,
        manifest_hash: str,
        payload_hash: str,
    ) -> None:
        """Append the durable local start receipt before invoking the work runner."""

        creator_node_id = manifest.get("creator_node_id")
        lineage = manifest.get("lineage")
        attribution = manifest.get("contribution_attribution")
        if not isinstance(creator_node_id, str) or not creator_node_id.strip():
            raise RuntimeServiceError(
                "job submission manifest creator_node_id is required for execution audit"
            )
        if not _provenance_matches_job(lineage, attribution, job_id, creator_node_id):
            raise RuntimeServiceError(
                "job submission manifest lineage or contribution attribution is invalid "
                "for execution audit"
            )
        lineage = cast(dict[str, Any], lineage)
        attribution = cast(dict[str, Any], attribution)
        metadata_hash = canonical_json_hash(attribution["metadata"], prefix="sha256:")
        try:
            append_local_audit_event(
                self.paths.data_dir / "audit" / "job-executions.jsonl",
                {
                    "schema_version": 2,
                    "event_id": f"local-audit-{job_id}-execution-started",
                    "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "event_type": "job.execution.started",
                    "actor_node_id": worker_node_id,
                    "creator_node_id": creator_node_id,
                    "local_run_id": job_id,
                    "event_sequence": 2,
                    "job_id": job_id,
                    "work_id": job_id,
                    "executing_node_id": worker_node_id,
                    "manifest_id": job_id,
                    "manifest_ref": manifest_ref,
                    "hashes": {
                        "manifest_hash": manifest_hash,
                        "input_payload_hash": payload_hash,
                    },
                    "lineage_refs": lineage["parent_refs"],
                    "contribution_attribution": {
                        "job_id": job_id,
                        "creator_node_id": creator_node_id,
                        "executing_node_id": worker_node_id,
                        "metadata_hash": metadata_hash,
                    },
                    "attribution_metadata_hash": metadata_hash,
                },
            )
        except (LocalAuditEventError, OSError, ValueError) as exc:
            raise RuntimeServiceError(
                f"could not write local job execution start audit event: {exc}"
            ) from exc

    def _append_result_reported_audit_event(
        self,
        *,
        job_id: str,
        worker_node_id: str,
        manifest: dict[str, Any],
        manifest_id: str,
        manifest_ref: str,
        validation_receipt_ref: str,
        result_status: str,
    ) -> None:
        """Append one deterministic, reference-only audit event for a local report."""

        creator_node_id = manifest.get("creator_node_id")
        receipt_path = self.paths.home / validation_receipt_ref
        manifest_path = self.paths.home / manifest_ref
        if not isinstance(creator_node_id, str) or not creator_node_id.strip():
            raise RuntimeServiceError("result report audit requires a creator node ID")
        if not manifest_path.is_file() or not receipt_path.is_file():
            raise RuntimeServiceError(
                "result report audit requires existing manifest and validation receipt"
            )
        receipt = self._load_local_job_document(receipt_path, "validation receipt")
        if receipt.get("validation_receipt_id") != self._receipt_id_for_job(job_id):
            raise RuntimeServiceError(
                "result report audit receipt does not match work ID"
            )
        audit_path = self.paths.data_dir / "audit" / "result-reports.jsonl"
        event_id = f"local-audit-{job_id}-result-reported"
        try:
            if audit_path.exists() and any(
                json.loads(line).get("event_id") == event_id
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ):
                return
            append_local_audit_event(
                audit_path,
                {
                    "schema_version": 2,
                    "event_id": event_id,
                    "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "event_type": "result.reported",
                    "actor_node_id": worker_node_id,
                    "creator_node_id": creator_node_id,
                    "local_run_id": job_id,
                    "event_sequence": 5,
                    "reporting_node_id": worker_node_id,
                    "work_id": job_id,
                    "manifest_id": manifest_id,
                    "manifest_ref": manifest_ref,
                    "validation_receipt_id": receipt["validation_receipt_id"],
                    "validation_receipt_ref": validation_receipt_ref,
                    "lineage_refs": [manifest_ref],
                    "contribution_attribution_ids": [f"local-contribution-{job_id}"],
                    "contribution_attribution_refs": [f"data/job-status/{job_id}.json"],
                    "result_status": result_status,
                },
            )
        except (LocalAuditEventError, OSError, ValueError) as exc:
            raise RuntimeServiceError(
                f"could not write local result report audit event: {exc}"
            ) from exc

    def _append_job_execution_finished_audit_event(
        self,
        *,
        job_id: str,
        worker_node_id: str | None,
        manifest: dict[str, Any],
        manifest_ref: str,
        manifest_id: str,
        finished_at: str,
        duration_ms: int,
        terminal_status: str,
        validation_receipt_ref: str | None,
        validator_node_id: str | None,
        output_artifact_refs: list[str],
        error_summary: str | None,
        contribution_attribution: dict[str, Any],
    ) -> None:
        """Append one terminal local execution receipt before persisting terminal state."""

        creator_node_id = manifest.get("creator_node_id")
        lineage = manifest.get("lineage")
        if not isinstance(creator_node_id, str) or not creator_node_id.strip():
            raise RuntimeServiceError(
                "job submission manifest creator_node_id is required for execution audit"
            )
        if not _provenance_matches_job(
            lineage, contribution_attribution, job_id, creator_node_id
        ):
            raise RuntimeServiceError(
                "job execution finish lineage or contribution attribution is invalid"
            )
        lineage = cast(dict[str, Any], lineage)
        attribution_metadata = contribution_attribution.get("metadata")
        if not isinstance(attribution_metadata, dict):
            raise RuntimeServiceError(
                "job execution finish contribution attribution metadata is invalid"
            )
        metadata_hash = canonical_json_hash(attribution_metadata, prefix="sha256:")
        audit_attribution = {
            "job_id": job_id,
            "creator_node_id": creator_node_id,
            "worker_node_id": worker_node_id or "not-assigned",
            "validator_node_id": validator_node_id or "not-used",
            "metadata_hash": metadata_hash,
        }
        actor_node_id = worker_node_id or self._local_node_id_for_submission()
        audit_finished_at = datetime.fromisoformat(
            finished_at.removesuffix("Z") + "+00:00"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        audit_path = self.paths.data_dir / "audit" / "job-executions.jsonl"
        event_id = f"local-audit-{job_id}-execution-finished"
        try:
            if audit_path.exists():
                for line in audit_path.read_text(encoding="utf-8").splitlines():
                    if json.loads(line).get("event_id") == event_id:
                        return
            append_local_audit_event(
                audit_path,
                {
                    "schema_version": 2,
                    "event_id": f"local-audit-{job_id}-execution-finished",
                    "timestamp": audit_finished_at,
                    "event_type": "job.execution.finished",
                    "actor_node_id": actor_node_id,
                    "creator_node_id": creator_node_id,
                    "local_run_id": job_id,
                    "event_sequence": 4,
                    "job_id": job_id,
                    "work_id": job_id,
                    "execution_id": f"local-execution-{job_id}",
                    "worker_node_id": worker_node_id,
                    "manifest_id": manifest_id,
                    "manifest_ref": manifest_ref,
                    "lineage_refs": lineage["parent_refs"],
                    "terminal_status": terminal_status,
                    "finished_at": audit_finished_at,
                    "duration_ms": duration_ms,
                    "validation_receipt_ref": validation_receipt_ref,
                    "validator_node_id": validator_node_id,
                    "output_artifact_refs": output_artifact_refs,
                    "error_summary": error_summary,
                    "contribution_attribution_ids": [f"local-contribution-{job_id}"],
                    "contribution_attribution": audit_attribution,
                },
            )
        except (LocalAuditEventError, OSError, ValueError) as exc:
            raise RuntimeServiceError(
                f"could not write local job execution finish audit event: {exc}"
            ) from exc

    def cancel_submitted_local_job(self, job_id: str) -> dict[str, Any]:
        """Cancel an unstarted local job without altering its manifest evidence."""

        before = self.get_local_job_status(job_id)
        if before["status"] == "not_found":
            raise RuntimeServiceError("local job not found")
        manifest = self._load_local_job_document(
            self.paths.data_dir / "job-submissions" / f"{job_id}.json",
            "job submission manifest",
        )
        finished_at = _utc_timestamp()
        self._append_job_execution_finished_audit_event(
            job_id=job_id,
            worker_node_id=before["worker_node_id"],
            manifest=manifest,
            manifest_ref=before["manifest_ref"],
            manifest_id=canonical_json_hash(manifest, prefix="sha256:"),
            finished_at=finished_at,
            duration_ms=0,
            terminal_status="cancelled",
            validation_receipt_ref=None,
            validator_node_id=None,
            output_artifact_refs=[],
            error_summary="local job cancelled before completion",
            contribution_attribution=before["contribution_attribution"],
        )
        self._transition_local_job_state(job_id, "canceled")
        self._append_event(f"canceled local job submission {job_id}")
        return self.get_local_job_status(job_id)

    def _job_status_path(self, job_id: str) -> Path:
        return self.paths.data_dir / "job-status" / f"{job_id}.json"

    def _transition_local_job_state(
        self, job_id: str, target: str, *, updates: dict[str, Any] | None = None
    ) -> None:
        """Persist one allowed transition and append a local audit entry first."""

        record = self._load_local_job_document(
            self._job_status_path(job_id), "job status record"
        )
        current = record.get("status")
        if current not in LOCAL_JOB_STATES:
            raise RuntimeServiceError("job status record has an invalid state")
        if target not in LOCAL_JOB_STATES:
            raise RuntimeServiceError("target job state is invalid")
        if current in LOCAL_JOB_TERMINAL_STATES:
            raise RuntimeServiceError("terminal local job cannot be restarted in place")
        if (current, target) not in LOCAL_JOB_STATE_TRANSITIONS:
            raise RuntimeServiceError(
                f"invalid local job state transition: {current} -> {target}"
            )
        timestamp = int(time.time())
        audit_ref = f"data/job-state-receipts/{job_id}.jsonl"
        self._append_job_state_audit(
            job_id,
            current,
            target,
            timestamp,
            record["creator_node_id"],
            record["manifest_ref"],
        )
        next_record = dict(record)
        if updates is not None:
            next_record.update(updates)
        next_record["status"] = target
        next_record["timestamps"] = {**record["timestamps"], f"{target}_at": timestamp}
        next_record["state_audit_refs"] = [*record["state_audit_refs"], audit_ref]
        atomic_write_json(self._job_status_path(job_id), next_record)

    def _append_job_state_audit(
        self,
        job_id: str,
        previous_state: str,
        state: str,
        timestamp: int,
        creator_node_id: object,
        manifest_ref: object,
    ) -> None:
        path = self.paths.data_dir / "job-state-receipts" / f"{job_id}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "version": 1,
            "job_id": job_id,
            "previous_state": previous_state,
            "state": state,
            "timestamp": timestamp,
            "creator_node_id": creator_node_id,
            "manifest_ref": manifest_ref,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n"
            )

    @staticmethod
    def _is_local_job_id(job_id: object) -> bool:
        prefix = "local-job-"
        suffix = job_id[len(prefix) :] if isinstance(job_id, str) else ""
        return (
            isinstance(job_id, str)
            and job_id.startswith(prefix)
            and len(suffix) == 32
            and all(character in "0123456789abcdef" for character in suffix)
        )

    @classmethod
    def _is_local_lineage_id(cls, lineage_id: object) -> bool:
        prefix = "local-lineage-"
        return (
            isinstance(lineage_id, str)
            and lineage_id.startswith(prefix)
            and cls._is_local_job_id(lineage_id.removeprefix(prefix))
        )

    @classmethod
    def _is_local_attribution_id(cls, attribution_id: object) -> bool:
        prefix = "local-contribution-"
        return (
            isinstance(attribution_id, str)
            and attribution_id.startswith(prefix)
            and cls._is_local_job_id(attribution_id.removeprefix(prefix))
        )

    @staticmethod
    def _load_local_job_document(path: Path, label: str) -> dict[str, Any]:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeServiceError(f"{label} JSON is malformed: {exc.msg}") from exc
        except OSError as exc:
            raise RuntimeServiceError(f"could not read {label}: {exc}") from exc
        if not isinstance(document, dict):
            raise RuntimeServiceError(f"{label} JSON must be an object")
        return document

    def health(self) -> dict[str, Any]:
        config = self.load_config()
        return {
            "ok": True,
            "service": "aethermesh-local-node",
            "version": _package_version(),
            "status": self.get_node_status()["status"],
            "bind_host": _config_api_host(config),
            "port": _config_api_port(config),
            "config_path": str(self.paths.config_path),
        }

    def recent_logs(self, limit: int = 100) -> dict[str, Any]:
        if not self.paths.events_path.exists():
            return {"events": []}
        lines = self.paths.events_path.read_text(encoding="utf-8").splitlines()
        return {"events": lines[-limit:]}

    def system_info(self) -> dict[str, Any]:
        self.paths.data_dir.mkdir(parents=True, exist_ok=True)
        disk = shutil.disk_usage(self.paths.data_dir)
        return {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "processor": platform.processor() or platform.machine(),
            "memory_total_bytes": _memory_total_bytes(),
            "disk_data_path_total_bytes": disk.total,
            "disk_data_path_free_bytes": disk.free,
        }

    def _default_config(
        self, node_id: str | None, node_name: str | None = None
    ) -> dict[str, Any]:
        return {
            "version": CONFIG_SCHEMA_VERSION,
            "node": {
                "node_id": node_id,
                "node_name": node_name,
                "status": "local_only",
            },
            "paths": {
                "home": str(self.paths.home),
                "data_dir": str(self.paths.data_dir),
                "log_dir": str(self.paths.log_dir),
            },
            "api": {"host": DEFAULT_API_HOST, "port": DEFAULT_API_PORT},
            "identity": {"persist": False, "path": str(self.paths.identity_path)},
            "capabilities": {"enabled_work_types": list(DEFAULT_LOCAL_CAPABILITIES)},
        }

    def _write_config(self, document: dict[str, Any]) -> None:
        try:
            atomic_write_json(self.paths.config_path, document)
        except OSError as exc:
            raise RuntimeServiceError(f"could not write config file: {exc}") from exc

    def _runtime_marker(self) -> tuple[bool, int | None, int | None]:
        if not self.paths.pid_path.exists():
            return False, None, None
        try:
            with self.paths.pid_path.open("r", encoding="utf-8") as handle:
                document = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return False, None, None
        pid = document.get("pid")
        started_at = document.get("started_at")
        if not isinstance(pid, int) or isinstance(pid, bool):
            return False, None, None
        if not _pid_is_alive(pid):
            return False, pid, None
        return True, pid, started_at if isinstance(started_at, int) else None

    def _append_event(self, message: str) -> None:
        self.paths.log_dir.mkdir(parents=True, exist_ok=True)
        line = f"{int(time.time())} {message}\n"
        with self.paths.events_path.open("a", encoding="utf-8") as handle:
            handle.write(line)


def _default_home() -> Path:
    override = os.environ.get("AETHERMESH_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".aethermesh"


def _merge_config(existing: dict[str, Any], default: dict[str, Any]) -> dict[str, Any]:
    merged = dict(default)
    for key, value in existing.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged[key])
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


def _capability_manifest_id(identifier: str) -> str:
    """Return the stable local manifest identity for one registered capability."""

    return f"local-capability-{identifier.replace('.', '-')}-v1"


def _capability_provenance(
    identifier: str,
    creator_node_id: str | None,
    resource_hints: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Build shared local provenance and optional advisory metadata."""

    manifest_id = _capability_manifest_id(identifier)
    return {
        "creator_node_id": creator_node_id,
        "capability_manifest_id": manifest_id,
        "lineage": {"capability_manifest_id": manifest_id},
        "contribution_attribution": {"creator_node_id": creator_node_id},
        **(
            {"resource_hints": resource_hints[identifier]}
            if identifier in resource_hints
            else {}
        ),
    }


def _capability_advertisement_manifest(
    *,
    identifier: str,
    description: str,
    work_type: str,
    creator_node_id: str,
) -> dict[str, Any]:
    """Build the local manifest that backs one emitted capability claim."""

    return {
        "schema_version": 1,
        "manifest_id": _capability_manifest_id(identifier),
        "creator_node_id": creator_node_id,
        "capability_id": identifier,
        "task_type": work_type,
        "description": description,
        "required_inputs": _capability_check_payload(work_type),
        "expected_outputs": f"local-job-result-schema-v{JOB_RESULT_SCHEMA_VERSION}",
        "validation": {
            "method": "local-deterministic-job-result-validation",
            "receipt_format": (
                f"local-validation-receipt-schema-v{VALIDATION_RECEIPT_SCHEMA_VERSION}"
            ),
        },
        "lineage": {
            "node_manifest_ref": "config.json",
            "result_manifest_template": "data/job-results/{job_id}.json",
        },
        "contribution_attribution": {"creator_node_id": creator_node_id},
    }


def _advertised_capability(
    *,
    identifier: str,
    description: str,
    work_type: str,
    creator_node_id: str,
    manifest_ref: str,
) -> dict[str, Any]:
    """Return one validation-gated local advertisement entry."""

    return {
        "capability_id": identifier,
        "task_types": [work_type],
        "description": description,
        "scope": "local-only-no-p2p",
        "prototype_status": "prototype-local-validation-required",
        "runtime_limits": {"max_concurrent_jobs": LOCAL_CAPABILITY_WORKER_CAPACITY},
        "required_inputs": _capability_check_payload(work_type),
        "expected_outputs": f"local-job-result-schema-v{JOB_RESULT_SCHEMA_VERSION}",
        "validation_requirements": {
            "method": "local-deterministic-job-result-validation",
            "receipt_format": (
                f"local-validation-receipt-schema-v{VALIDATION_RECEIPT_SCHEMA_VERSION}"
            ),
            "required": True,
        },
        "lineage": {
            "node_manifest_ref": "config.json",
            "capability_manifest_ref": manifest_ref,
            "validation_receipt_format": (
                f"local-validation-receipt-schema-v{VALIDATION_RECEIPT_SCHEMA_VERSION}"
            ),
            "result_manifest_template": "data/job-results/{job_id}.json",
        },
        "contribution_attribution": {"creator_node_id": creator_node_id},
    }


def _availability(
    status: str, reason: str | None, worker_capacity: dict[str, int]
) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "worker_capacity": worker_capacity,
        "validation_receipt_refs": [],
    }


def _metadata_capability_availability(
    *, enabled: bool, creator_node_id: str | None, current_workers: int
) -> dict[str, Any]:
    capacity = {"current": current_workers, "maximum": LOCAL_CAPABILITY_WORKER_CAPACITY}
    if not enabled:
        return _availability("unavailable", "disabled in local configuration", capacity)
    if creator_node_id is None:
        return _availability("unavailable", "local node identity is missing", capacity)
    return _availability("available", None, capacity)


def _capability_check_payload(work_type: str) -> dict[str, Any]:
    payloads: dict[str, dict[str, Any]] = {
        "echo": {"message": "availability-check"},
        "hash": {"value": "availability-check"},
        "basic_compute": {"operation": "add", "operands": [1, 2]},
        "schema_transform": {
            "record": {"ready": True},
            "schema": {"fields": {"ready": "boolean"}},
        },
        "keyword_extract": {"text": "availability check"},
        "text_chunk": {"text": "availability check"},
        "text_embed": {"text": "availability check"},
        "text_stats": {"text": "availability check"},
    }
    return payloads[work_type]


def _config_node_id(config: dict[str, Any]) -> str | None:
    node = config.get("node")
    if not isinstance(node, dict):
        return None
    node_id = node.get("node_id")
    return node_id if isinstance(node_id, str) and node_id else None


def _config_enabled_work_types(config: dict[str, Any]) -> set[str]:
    capabilities = config.get("capabilities")
    if capabilities is None:
        return set(DEFAULT_LOCAL_CAPABILITIES)
    if not isinstance(capabilities, dict):
        raise RuntimeServiceError("config JSON field 'capabilities' must be an object")
    value = capabilities.get("enabled_work_types", list(DEFAULT_LOCAL_CAPABILITIES))
    if not isinstance(value, list) or not all(
        isinstance(work_type, str) and work_type for work_type in value
    ):
        raise RuntimeServiceError(
            "config JSON field 'capabilities.enabled_work_types' must be a list of non-empty strings"
        )
    return set(value)


def _config_capability_resource_hints(
    config: dict[str, Any],
) -> dict[str, dict[str, str]]:
    """Load optional non-economic, advisory hints for registered capabilities."""

    capabilities = config.get("capabilities")
    if capabilities is None:
        return {}
    if not isinstance(capabilities, dict):
        raise RuntimeServiceError("config JSON field 'capabilities' must be an object")
    raw_hints = capabilities.get("resource_hints", {})
    if not isinstance(raw_hints, dict):
        raise RuntimeServiceError(
            "config JSON field 'capabilities.resource_hints' must be an object"
        )
    registered_identifiers = {
        identifier for identifier, _, _ in LOCAL_CAPABILITY_DEFINITIONS
    } | {identifier for identifier, _, _ in LOCAL_PROVENANCE_CAPABILITY_DEFINITIONS}
    normalized: dict[str, dict[str, str]] = {}
    for identifier, hints in raw_hints.items():
        if identifier not in registered_identifiers:
            raise RuntimeServiceError(
                "capabilities.resource_hints keys must be registered capability identifiers"
            )
        if not isinstance(hints, dict):
            raise RuntimeServiceError(
                f"capabilities.resource_hints.{identifier} must be an object"
            )
        normalized_hints: dict[str, str] = {}
        for field, value in hints.items():
            if field not in RESOURCE_HINT_FIELDS:
                raise RuntimeServiceError(
                    f"capabilities.resource_hints.{identifier}.{field} is not an allowed advisory field"
                )
            if not isinstance(value, str) or not value.strip():
                raise RuntimeServiceError(
                    f"capabilities.resource_hints.{identifier}.{field} must be a non-empty string"
                )
            if ECONOMIC_HINT_PATTERN.search(value):
                raise RuntimeServiceError(
                    f"capabilities.resource_hints.{identifier}.{field} must not imply token economics or financial settlement"
                )
            normalized_hints[field] = value
        normalized[identifier] = normalized_hints
    return normalized


def _config_identity_persistence_enabled(config: dict[str, Any]) -> bool:
    identity = config.get("identity")
    if identity is None:
        return False
    if not isinstance(identity, dict):
        raise RuntimeServiceError("config JSON field 'identity' must be an object")
    persist = identity.get("persist", False)
    if not isinstance(persist, bool):
        raise RuntimeServiceError(
            "config JSON field 'identity.persist' must be a boolean"
        )
    return persist


def _config_identity_path(config: dict[str, Any], default_path: Path) -> Path:
    identity = config.get("identity")
    if not isinstance(identity, dict):
        return default_path
    configured_path = identity.get("path")
    if configured_path is None:
        return default_path
    if not isinstance(configured_path, str) or not configured_path.strip():
        raise RuntimeServiceError(
            "config JSON field 'identity.path' must be a non-empty string"
        )
    path = Path(configured_path).expanduser()
    return path if path.is_absolute() else default_path.parent / path


def _config_node_name(config: dict[str, Any]) -> str | None:
    node = config.get("node")
    if not isinstance(node, dict):
        return None
    node_name = node.get("node_name")
    return node_name if isinstance(node_name, str) and node_name else None


def _config_api_host(config: dict[str, Any]) -> str:
    api = config.get("api")
    host = api.get("host") if isinstance(api, dict) else None
    if isinstance(host, str):
        return host
    return DEFAULT_API_HOST


def _config_api_port(config: dict[str, Any]) -> int:
    api = config.get("api")
    port = api.get("port") if isinstance(api, dict) else None
    if isinstance(port, int) and not isinstance(port, bool):
        return port
    return DEFAULT_API_PORT


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _validated_runtime_validation_method(value: object) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("kind"), str)
        or not value["kind"]
        or not isinstance(value.get("description"), str)
        or not value["description"].strip()
    ):
        raise RuntimeServiceError("validation receipt has no validation method")
    return value


def _provenance_matches_job(
    lineage: object,
    attribution: object,
    job_id: object,
    creator_node_id: object,
) -> bool:
    """Return whether local lineage and attribution retain one job identity."""
    return (
        isinstance(lineage, dict)
        and lineage.get("job_id") == job_id
        and isinstance(lineage.get("parent_refs"), list)
        and isinstance(attribution, dict)
        and attribution.get("job_id") == job_id
        and attribution.get("creator_node_id") == creator_node_id
    )


def _output_payload_record(
    output: object, *, job_id: str, root: Path, store: bool
) -> dict[str, object]:
    """Choose a bounded inline output or a deterministic local payload artifact."""

    if not store:
        return {"inline_payload": None, "payload_ref": None, "payload_digest": None}
    payload_document = {"payload": output}
    try:
        encoded = json.dumps(
            output, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RuntimeServiceError("local job output must be JSON-compatible") from exc
    if len(encoded) <= MAX_INLINE_OUTPUT_PAYLOAD_BYTES:
        return {
            "inline_payload": output,
            "payload_ref": None,
            "payload_digest": None,
        }
    payload_ref = f"data/job-output-payloads/{job_id}.json"
    atomic_create_json(root / payload_ref, payload_document)
    return {
        "inline_payload": None,
        "payload_ref": payload_ref,
        "payload_digest": canonical_json_hash(payload_document, prefix="sha256:"),
    }


def _validate_stored_output_payload(
    output_payload: object,
    *,
    root: Path,
    job_id: str,
    expected_output_hash: object,
    require_payload: bool,
) -> None:
    """Verify local output evidence before returning a validation receipt."""

    if not isinstance(output_payload, dict):
        raise RuntimeServiceError("job result has invalid output payload evidence")
    inline_payload = output_payload.get("inline_payload")
    payload_ref = output_payload.get("payload_ref")
    payload_digest = output_payload.get("payload_digest")
    if inline_payload is not None:
        output = inline_payload
    elif payload_ref is None:
        if require_payload:
            raise RuntimeServiceError(
                "successful job result has no output payload evidence"
            )
        return
    else:
        expected_ref = f"data/job-output-payloads/{job_id}.json"
        if payload_ref != expected_ref or not _safe_local_artifact_ref(payload_ref):
            raise RuntimeServiceError(
                "job result has an unsafe output payload reference"
            )
        try:
            stored = NodeRuntimeService._load_local_job_document(
                root / payload_ref, "job output payload artifact"
            )
        except RuntimeServiceError as exc:
            raise RuntimeServiceError(
                "job result output payload artifact is not locally retrievable"
            ) from exc
        if set(stored) != {"payload"}:
            raise RuntimeServiceError("job result output payload artifact is malformed")
        if payload_digest != canonical_json_hash(stored, prefix="sha256:"):
            raise RuntimeServiceError(
                "job result output payload digest does not match artifact"
            )
        output = stored["payload"]
    if expected_output_hash != canonical_json_hash(
        {"output": output}, prefix="sha256:"
    ):
        raise RuntimeServiceError(
            "job result output payload does not match validation receipt"
        )


def _safe_local_artifact_ref(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    path = Path(value)
    return (
        not path.is_absolute()
        and not value.startswith("~")
        and "://" not in value
        and not any(character in value for character in ("?", "#", "@"))
        and "\\" not in value
        and ".." not in path.parts
        and not path.name.lower().endswith((".key", ".pem", ".env"))
    )


def _safe_local_identifier(value: object) -> bool:
    """Allow readable local identifiers while rejecting path and URI-shaped values."""

    return (
        isinstance(value, str)
        and bool(value.strip())
        and not any(character in value for character in ("/", "\\", "?", "#", "@"))
        and not value.startswith(("~", "."))
        and "://" not in value
        and ".." not in value
    )


def _rejection_log_capability(value: object) -> str:
    """Serialize a bounded submitted capability for local rejection diagnostics."""

    try:
        rendered = json.dumps(value, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return "<malformed>"
    return rendered[:160]


def _local_safety_metadata(value: object) -> dict[str, object] | None:
    """Validate optional local-only execution controls stored with one job."""

    if value is None:
        return None
    if (
        not isinstance(value, dict)
        or not value
        or set(value) - {"timeout_seconds", "cancellation_requested"}
    ):
        raise RuntimeServiceError(
            "local_safety must be a non-empty object with timeout_seconds or cancellation_requested"
        )
    metadata: dict[str, object] = {}
    if "timeout_seconds" in value:
        timeout = value["timeout_seconds"]
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout < 0
            or timeout > 60
        ):
            raise RuntimeServiceError(
                "local_safety.timeout_seconds must be a number from 0 through 60"
            )
        metadata["timeout_seconds"] = timeout
    if "cancellation_requested" in value:
        cancellation = value["cancellation_requested"]
        if not isinstance(cancellation, bool):
            raise RuntimeServiceError(
                "local_safety.cancellation_requested must be a boolean"
            )
        metadata["cancellation_requested"] = cancellation
    return metadata


def _expected_output_shape(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Declare the deterministic output contract stored with each local manifest."""

    shapes: dict[str, dict[str, Any]] = {
        "echo": {"type": "string"},
        "hash": {"type": "object", "required": ["algorithm", "digest"]},
        "basic_compute": {"type": "object", "required": ["operation", "result"]},
        "schema_transform": {
            "type": "object",
            "fields": payload.get("schema", {}).get("fields")
            if isinstance(payload.get("schema"), dict)
            else None,
        },
    }
    return shapes.get(job_type, {"type": "deterministic-local-output"})


def _validated_input_payload(value: object) -> tuple[dict[str, Any], str]:
    """Validate and hash the bounded, canonical local work input object."""

    if not isinstance(value, dict):
        raise RuntimeServiceError("job submission input_payload must be a JSON object")
    allowed_fields = {"payload_type", "content", "parameters"}
    if set(value) - allowed_fields or not {"payload_type", "content"} <= set(value):
        raise RuntimeServiceError(
            "job submission input_payload requires payload_type and content only"
        )
    payload_type = value["payload_type"]
    content = value["content"]
    parameters = value.get("parameters")
    if not isinstance(payload_type, str) or not payload_type.strip():
        raise RuntimeServiceError(
            "job submission input_payload.payload_type must be a non-empty string"
        )
    if not isinstance(content, dict):
        raise RuntimeServiceError(
            "job submission input_payload.content must be a JSON object"
        )
    if parameters is not None and not isinstance(parameters, dict):
        raise RuntimeServiceError(
            "job submission input_payload.parameters must be a JSON object"
        )
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        if json.loads(canonical) != value:
            raise ValueError("canonical JSON does not preserve the payload")
    except (TypeError, ValueError, RecursionError) as exc:
        raise RuntimeServiceError(
            "job submission input_payload must contain JSON-compatible data"
        ) from exc
    encoded = canonical.encode("utf-8")
    if len(encoded) > MAX_LOCAL_INPUT_PAYLOAD_BYTES:
        raise RuntimeServiceError(
            "job submission input_payload exceeds the 65536-byte local limit"
        )
    return value, f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _attribution_metadata(value: object) -> dict[str, Any]:
    """Validate bounded descriptive metadata without allowing evidence overrides."""

    if not isinstance(value, dict):
        raise RuntimeServiceError(
            "job submission attribution_metadata must be a JSON object"
        )
    reserved = sorted(_ATTRIBUTION_METADATA_RESERVED_FIELDS & set(value))
    if reserved:
        raise RuntimeServiceError(
            "job submission attribution_metadata must not contain reserved "
            f"provenance fields: {', '.join(reserved)}"
        )
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        if json.loads(canonical) != value:
            raise ValueError("canonical JSON does not preserve the metadata")
    except (TypeError, ValueError, RecursionError) as exc:
        raise RuntimeServiceError(
            "job submission attribution_metadata must contain JSON-compatible data"
        ) from exc
    encoded = canonical.encode("utf-8")
    if len(encoded) > MAX_LOCAL_ATTRIBUTION_METADATA_BYTES:
        raise RuntimeServiceError(
            "job submission attribution_metadata exceeds the 4096-byte local limit"
        )
    return value


def _requester_identity(value: object) -> dict[str, str] | None:
    """Validate optional request-origin identity without conflating provenance roles."""

    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeServiceError(
            "requester_identity must be null or an object with one requester identity"
        )
    requesting_node_id = value.get("requesting_node_id")
    local_requester_identity = value.get("local_requester_identity")
    status = value.get("status")
    if set(value) == {"requesting_node_id"} and _safe_local_identifier(
        requesting_node_id
    ):
        return {"requesting_node_id": str(requesting_node_id)}
    if set(value) == {"local_requester_identity"} and _safe_local_identifier(
        local_requester_identity
    ):
        return {"local_requester_identity": str(local_requester_identity)}
    if set(value) == {"status"} and status == "unknown":
        return {"status": "unknown"}
    raise RuntimeServiceError(
        "requester_identity must contain exactly one of requesting_node_id, "
        "local_requester_identity, or status 'unknown'"
    )


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validation_completed_at() -> str:
    """Capture local receipt timing separately from deterministic report metadata."""

    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _utc_timestamp_from_unix_seconds(timestamp: int) -> str:
    """Format a locally stored Unix-second lifecycle timestamp as UTC."""

    return (
        datetime.fromtimestamp(timestamp, UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _duration_ms(started_at: str, finished_at: str) -> int:
    """Return the exact millisecond duration required by the result schema."""

    format_string = "%Y-%m-%dT%H:%M:%S.%fZ"
    return int(
        (
            datetime.strptime(finished_at, format_string).replace(tzinfo=UTC)
            - datetime.strptime(started_at, format_string).replace(tzinfo=UTC)
        ).total_seconds()
        * 1000
    )


def _result_summary(value: object) -> str:
    """Serialize a bounded, non-empty local execution summary."""

    if isinstance(value, str) and value:
        return value[:512]
    summary = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return (summary or "local execution completed")[:512]


def _bounded_rejected_result_report(report: object) -> dict[str, object]:
    """Keep a debuggable report copy while bounding local rejection-log growth."""

    try:
        encoded = json.dumps(
            report, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError):
        return {"unserializable_type": type(report).__name__}
    digest = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
    if len(encoded) <= MAX_REJECTED_RESULT_REPORT_BYTES:
        return {"original": json.loads(encoded), "sha256": digest, "truncated": False}
    return {
        "original_prefix": encoded[:MAX_REJECTED_RESULT_REPORT_BYTES].decode(
            "utf-8", errors="replace"
        ),
        "sha256": digest,
        "truncated": True,
        "original_size_bytes": len(encoded),
    }


def _is_utc_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        return False
    return parsed.tzinfo is UTC


def _utc_timestamp_to_unix_seconds(value: str) -> int:
    """Convert a validated canonical UTC timestamp to audit-event Unix seconds."""

    return int(
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
        .replace(tzinfo=UTC)
        .timestamp()
    )


def _is_utc_timestamp_before_or_at(value: object, epoch_seconds: object) -> bool:
    if not _is_utc_timestamp(value):
        return False
    if not isinstance(epoch_seconds, int) or isinstance(epoch_seconds, bool):
        return False
    parsed = datetime.strptime(cast(str, value), "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=UTC
    )
    # Completion timestamps currently have whole-second precision, so their value
    # represents the full UTC second in which completion was recorded.
    return parsed.timestamp() < epoch_seconds + 1


def _is_utc_timestamp_before_or_at_timestamp(earlier: object, later: object) -> bool:
    if not _is_utc_timestamp(earlier) or not _is_utc_timestamp(later):
        return False
    format_string = "%Y-%m-%dT%H:%M:%S.%fZ"
    return datetime.strptime(cast(str, earlier), format_string).replace(
        tzinfo=UTC
    ) <= datetime.strptime(cast(str, later), format_string).replace(tzinfo=UTC)


def _package_version() -> str:
    return PUBLIC_VERSION


def _memory_total_bytes() -> int | None:
    if hasattr(os, "sysconf"):
        names = os.sysconf_names
        if "SC_PAGE_SIZE" in names and "SC_PHYS_PAGES" in names:
            try:
                return int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
            except (OSError, ValueError):
                return None
    return None
