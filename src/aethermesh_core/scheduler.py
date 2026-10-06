"""Deterministic local scheduler for the AetherMesh prototype."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from enum import Enum


class NoAvailableNodesError(ValueError):
    """Raised when local job assignment has jobs but no available nodes."""


DEFAULT_LOCAL_CAPABILITIES = (
    "echo",
    "keyword_extract",
    "text_chunk",
    "text_embed",
    "text_retrieve",
    "text_stats",
)

# A local job's type is its required worker capability. Keeping this constrained
# makes local routing explicit and prevents manifests from claiming work no
# bundled worker can perform.
SUPPORTED_LOCAL_JOB_TYPES = DEFAULT_LOCAL_CAPABILITIES


class NodeStatus(str, Enum):
    """Availability states needed by the local scheduler."""

    AVAILABLE = "available"
    OFFLINE = "offline"


@dataclass(frozen=True)
class ScheduledNode:
    """Local scheduler view of a node."""

    node_id: str
    status: NodeStatus = NodeStatus.AVAILABLE
    capabilities: tuple[str, ...] = field(default=DEFAULT_LOCAL_CAPABILITIES)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "capabilities", _normalize_capabilities(self.capabilities)
        )


@dataclass(frozen=True)
class ScheduledJob:
    """Minimal scheduler view of a job."""

    job_id: str
    job_type: str


@dataclass(frozen=True)
class JobAssignment:
    """Structured local assignment of one job to one node."""

    job_id: str
    node_id: str

    def to_dict(self) -> dict[str, str]:
        """Serialize the assignment into a JSON-compatible dictionary."""

        return asdict(self)


class LocalScheduler:
    """In-memory deterministic scheduler for local prototype jobs."""

    def __init__(self, nodes: Iterable[str | ScheduledNode]) -> None:
        self.nodes = tuple(_coerce_node(node) for node in nodes)

    def assign_jobs(self, jobs: Iterable[object]) -> list[JobAssignment]:
        """Assign jobs to available capable nodes with deterministic fair ordering."""

        job_list = [_coerce_job(job) for job in jobs]
        if not job_list:
            return []

        available_nodes = [
            node for node in self.nodes if node.status == NodeStatus.AVAILABLE
        ]
        if not available_nodes:
            raise NoAvailableNodesError("no available nodes for local job assignment")

        manifest_order_index = {
            node.node_id: index for index, node in enumerate(self.nodes)
        }
        assignment_count_by_node = {node.node_id: 0 for node in self.nodes}
        assignments: list[JobAssignment] = []
        for job in job_list:
            capable_nodes = [
                node for node in available_nodes if job.job_type in node.capabilities
            ]
            if not capable_nodes:
                raise NoAvailableNodesError(
                    "no available capable node for "
                    f"job_id={job.job_id} job_type={job.job_type}"
                )
            selected_node = min(
                capable_nodes,
                key=lambda node: (
                    assignment_count_by_node[node.node_id],
                    manifest_order_index[node.node_id],
                ),
            )
            assignment_count_by_node[selected_node.node_id] += 1
            assignments.append(
                JobAssignment(job_id=job.job_id, node_id=selected_node.node_id)
            )
        return assignments


def _coerce_node(node: str | ScheduledNode) -> ScheduledNode:
    if isinstance(node, ScheduledNode):
        return node
    return ScheduledNode(node_id=node)


def _coerce_job(job: object) -> ScheduledJob:
    if isinstance(job, ScheduledJob):
        return job
    job_id = getattr(job, "job_id", None)
    job_type = getattr(job, "job_type", None)
    if isinstance(job_id, str) and isinstance(job_type, str):
        return ScheduledJob(job_id=job_id, job_type=job_type)
    return ScheduledJob(job_id=str(job), job_type=DEFAULT_LOCAL_CAPABILITIES[0])


def _normalize_capabilities(capabilities: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(capabilities)))
