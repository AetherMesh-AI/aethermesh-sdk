"""Assignment-only local dispatch for manifest-backed batches."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from aethermesh_core.message_bus import LocalMessageBus, send_numbered_message
from aethermesh_core.messages import MeshMessage
from aethermesh_core.models import Job
from aethermesh_core.node_registry import NodeRegistry
from aethermesh_core.scheduler import (
    JobAssignment,
    LocalScheduler,
    NodeStatus,
    ScheduledNode,
)


@dataclass(frozen=True)
class LocalDispatchResult:
    """Structured result for local assignment-only dispatch."""

    manifest_path: str
    message_log_path: str
    jobs: list[Job]
    nodes: list[ScheduledNode]
    assignments: list[JobAssignment]
    messages: list[MeshMessage]
    node_roster: list[dict[str, int | str | list[str]]]
    command: str = "dispatch-local-batch"
    peer_log_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize a deterministic, intentionally small CLI summary."""

        assigned_node_ids = sorted(
            {assignment.node_id for assignment in self.assignments}
        )
        payload: dict[str, Any] = {
            "command": self.command,
            "manifest_path": self.manifest_path,
            "message_log_path": self.message_log_path,
            "job_count": len(self.jobs),
            "assignment_count": len(self.assignments),
            "message_count": len(self.messages),
            "nodes": [
                {
                    "node_id": node.node_id,
                    "status": node.status.value,
                    "capabilities": list(node.capabilities),
                }
                for node in self.nodes
            ],
            "assignments": [assignment.to_dict() for assignment in self.assignments],
            "assigned_node_ids": assigned_node_ids,
        }
        if self.peer_log_path is not None:
            payload["peer_log_path"] = self.peer_log_path
        return payload


def dispatch_local_batch(
    *,
    manifest_path: str,
    message_log_path: str,
    nodes: Sequence[ScheduledNode],
    jobs: list[Job],
    command: str = "dispatch-local-batch",
    peer_log_path: str | None = None,
) -> LocalDispatchResult:
    """Build a local dispatch log with heartbeats and job assignments only.

    This function does not run workers, validate results, write ledgers, or persist
    any files. Callers can persist ``messages`` only after this returns
    successfully, which prevents assignment failures from truncating an existing
    message log.
    """

    if not nodes:
        raise ValueError("nodes must contain at least one node")

    registry = NodeRegistry()
    for node in nodes:
        registered_node = registry.register(node)
        if registered_node.status == NodeStatus.AVAILABLE:
            registry.record_heartbeat(registered_node.node_id)

    scheduled_nodes = registry.scheduled_nodes()
    message_bus = LocalMessageBus()
    for node in scheduled_nodes:
        message_bus.register_node(node.node_id)
    message_bus.register_node("local-scheduler")

    for heartbeat_payload in _node_heartbeat_payloads(registry):
        send_numbered_message(
            message_bus,
            message_type="node_heartbeat",
            sender_node_id=str(heartbeat_payload["node_id"]),
            recipient_node_id=None,
            payload=heartbeat_payload,
            correlation_id=None,
        )

    scheduler = LocalScheduler(scheduled_nodes)
    assignments = scheduler.assign_jobs(jobs)
    job_by_id = {job.job_id: job for job in jobs}
    for assignment in assignments:
        job = job_by_id[assignment.job_id]
        send_numbered_message(
            message_bus,
            message_type="job_assigned",
            sender_node_id="local-scheduler",
            recipient_node_id=assignment.node_id,
            payload={
                "job_id": job.job_id,
                "job_type": job.job_type,
                "payload": dict(job.payload),
                "node_id": assignment.node_id,
                "manifest_ref": manifest_path,
            },
            correlation_id=job.job_id,
        )

    return LocalDispatchResult(
        manifest_path=manifest_path,
        message_log_path=message_log_path,
        jobs=list(jobs),
        nodes=scheduled_nodes,
        assignments=assignments,
        messages=message_bus.log(),
        node_roster=registry.to_roster(),
        command=command,
        peer_log_path=peer_log_path,
    )


def _node_heartbeat_payloads(
    registry: Any,
    *,
    include_capabilities: bool = True,
) -> list[dict[str, int | str | list[str]]]:
    payloads: list[dict[str, int | str | list[str]]] = []
    for entry in registry.to_roster():
        if entry["status"] != NodeStatus.AVAILABLE.value:
            continue
        heartbeat_sequence = entry["heartbeat_sequence"]
        heartbeat_count = entry["heartbeat_count"]
        if not isinstance(heartbeat_sequence, int) or not isinstance(
            heartbeat_count, int
        ):
            raise ValueError("registry heartbeat fields must be integers")  # noqa: TRY004 - justification: public validation callers catch ValueError.
        capabilities = entry["capabilities"]
        if not isinstance(capabilities, list) or not all(
            isinstance(capability, str) for capability in capabilities
        ):
            raise ValueError("registry capabilities field must be a list of strings")
        payload: dict[str, int | str | list[str]] = {
            "node_id": str(entry["node_id"]),
            "status": str(entry["status"]),
            "heartbeat_sequence": heartbeat_sequence,
            "heartbeat_count": heartbeat_count,
        }
        if include_capabilities:
            payload["capabilities"] = list(capabilities)
        payloads.append(payload)
    return payloads
