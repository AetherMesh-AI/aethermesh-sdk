"""Versioned JSON manifest loading for local batch simulation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeGuard

from aethermesh_core.models import Job
from aethermesh_core.scheduler import (
    DEFAULT_LOCAL_CAPABILITIES,
    SUPPORTED_LOCAL_JOB_TYPES,
    NodeStatus,
    ScheduledNode,
)

_LOCAL_JOB_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,127}\Z")
_CONTENT_ADDRESSED_JOB_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")


class ManifestError(ValueError):
    """Raised when a local job manifest cannot be loaded or validated."""


@dataclass(frozen=True)
class LocalJobBatch:
    """Validated local batch inputs for the local simulation path."""

    nodes: list[ScheduledNode]
    jobs: list[Job]

    @property
    def node_ids(self) -> list[str]:
        """Return manifest node IDs in deterministic manifest order."""

        return [node.node_id for node in self.nodes]


def load_job_manifest(path: str | Path) -> LocalJobBatch:
    """Load and validate a version 1 local job-batch manifest."""

    document = _load_manifest_document(path)
    return LocalJobBatch(
        nodes=_parse_nodes(document.get("nodes")),
        jobs=_parse_jobs(document.get("jobs")),
    )


def load_manifest_jobs(path: str | Path) -> list[Job]:
    """Load only jobs from a version 1 manifest, ignoring any node roster."""

    document = _load_manifest_document(path)
    return _parse_jobs(document.get("jobs"))


def _load_manifest_document(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    try:
        raw_manifest = manifest_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"could not read manifest: {exc}") from exc

    try:
        document = json.loads(raw_manifest)
    except json.JSONDecodeError as exc:
        raise ManifestError(f"manifest JSON is malformed: {exc.msg}") from exc

    if not isinstance(document, dict):
        raise ManifestError("manifest must be a JSON object")

    version = document.get("version")
    if not isinstance(version, int) or isinstance(version, bool):
        raise ManifestError("manifest version must be integer 1")
    if version != 1:
        raise ManifestError(f"unsupported manifest version: {version}")
    return document


def _parse_nodes(value: Any) -> list[ScheduledNode]:
    if not isinstance(value, list) or not value:
        raise ManifestError("manifest nodes must be a non-empty list")

    nodes: list[ScheduledNode] = []
    seen_node_ids: set[str] = set()
    for index, entry in enumerate(value):
        node = _parse_node_entry(entry, index)
        node_id = node.node_id
        if node_id in seen_node_ids:
            raise ManifestError(f"manifest contains duplicate node id: {node_id}")
        seen_node_ids.add(node_id)
        nodes.append(node)
    return nodes


def _parse_node_entry(entry: Any, index: int) -> ScheduledNode:
    if isinstance(entry, str):
        if not entry.strip():
            raise ManifestError(f"manifest nodes[{index}] must be a non-empty string")
        return ScheduledNode(node_id=entry, status=NodeStatus.AVAILABLE)

    if not isinstance(entry, dict):
        raise ManifestError(
            f"manifest nodes[{index}] must be a non-empty string or JSON object"
        )

    node_id = entry.get("node_id")
    if not isinstance(node_id, str) or not node_id.strip():
        raise ManifestError(
            f"manifest nodes[{index}].node_id must be a non-empty string"
        )

    raw_status = entry.get("status", NodeStatus.AVAILABLE.value)
    if not isinstance(raw_status, str):
        raise ManifestError(f"manifest nodes[{index}].status must be a string")
    try:
        status = NodeStatus(raw_status)
    except ValueError as exc:
        supported_statuses = ", ".join(status.value for status in NodeStatus)
        raise ManifestError(
            f"manifest nodes[{index}].status must be one of: {supported_statuses}"
        ) from exc

    capabilities = _parse_capabilities(entry.get("capabilities"), index)
    return ScheduledNode(node_id=node_id, status=status, capabilities=capabilities)


def _parse_capabilities(value: Any, node_index: int) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_LOCAL_CAPABILITIES
    if not isinstance(value, list) or not value:
        raise ManifestError(
            f"manifest nodes[{node_index}].capabilities must be a non-empty list"
        )

    capabilities: list[str] = []
    seen: set[str] = set()
    for capability_index, capability in enumerate(value):
        if not isinstance(capability, str) or not capability.strip():
            raise ManifestError(
                "manifest "
                f"nodes[{node_index}].capabilities[{capability_index}] "
                "must be a non-empty string"
            )
        if capability in seen:
            raise ManifestError(
                f"manifest nodes[{node_index}].capabilities contains duplicate: "
                f"{capability}"
            )
        seen.add(capability)
        capabilities.append(capability)
    return tuple(sorted(capabilities))


def _parse_jobs(value: Any) -> list[Job]:
    if not isinstance(value, list) or not value:
        raise ManifestError("manifest jobs must be a non-empty list")

    jobs: list[Job] = []
    seen_job_ids: set[str] = set()
    for index, entry in enumerate(value):
        job = _parse_job_entry(entry, index)
        if job.job_id in seen_job_ids:
            raise ManifestError(
                f"manifest contains duplicate active job_id: {job.job_id}"
            )
        seen_job_ids.add(job.job_id)
        jobs.append(job)
    return jobs


def _parse_job_entry(entry: Any, index: int) -> Job:
    if not isinstance(entry, dict):
        raise ManifestError(f"manifest jobs[{index}] must be a JSON object")

    job_id = entry.get("job_id")
    if not _is_local_job_id(job_id):
        raise ManifestError(
            f"manifest jobs[{index}].job_id must be a local ID or sha256 content ID"
        )
    job_type = entry.get("job_type")
    if not isinstance(job_type, str) or not job_type.strip():
        raise ManifestError(
            f"manifest jobs[{index}].job_type must be a non-empty string"
        )
    if job_type not in SUPPORTED_LOCAL_JOB_TYPES:
        supported_types = ", ".join(SUPPORTED_LOCAL_JOB_TYPES)
        raise ManifestError(
            f"manifest jobs[{index}].job_type must be one of: {supported_types}"
        )

    payload = entry.get("payload", {})
    if not isinstance(payload, dict):
        raise ManifestError(f"manifest jobs[{index}].payload must be a JSON object")

    return Job(job_id=job_id, job_type=job_type, payload=dict(payload))


def _is_local_job_id(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(
        _LOCAL_JOB_ID.fullmatch(value) or _CONTENT_ADDRESSED_JOB_ID.fullmatch(value)
    )
