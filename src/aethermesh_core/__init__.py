"""AetherMesh SDK local foundations; aethermesh_core imports remain compatible."""

from aethermesh_core.execution import (
    ExecutionAssignmentError,
    ExecutionReceipt,
    LocalExecutor,
    PreparedWorkAssignment,
)
from aethermesh_core.ledger import (
    ContributionLedger,
    ContributionRecord,
    ContributionSummary,
)
from aethermesh_core.messages import MeshMessage, SUPPORTED_MESSAGE_TYPES
from aethermesh_core.models import Job, JobResult, NodeIdentity
from aethermesh_core.node_service import (
    InboxProcessResult,
    LocalNodeService,
    ProcessedAssignment,
)
from aethermesh_core.runner import LocalRunner
from aethermesh_core.scheduler import (
    JobAssignment,
    LocalScheduler,
    NoAvailableNodesError,
    NodeStatus,
    ScheduledNode,
)
from aethermesh_core.simulation import (
    LocalSimulationResult,
    SimulationJobAssignment,
    run_local_simulation,
)
from aethermesh_core.validation import ValidationResult, validate_job_result

__version__ = "0.2.0-alpha"

__all__ = [
    "ContributionLedger",
    "ContributionRecord",
    "ContributionSummary",
    "ExecutionAssignmentError",
    "ExecutionReceipt",
    "InboxProcessResult",
    "Job",
    "JobAssignment",
    "JobResult",
    "LocalNodeService",
    "LocalExecutor",
    "LocalRunner",
    "LocalScheduler",
    "LocalSimulationResult",
    "MeshMessage",
    "NoAvailableNodesError",
    "NodeIdentity",
    "NodeStatus",
    "ProcessedAssignment",
    "PreparedWorkAssignment",
    "SUPPORTED_MESSAGE_TYPES",
    "ScheduledNode",
    "SimulationJobAssignment",
    "ValidationResult",
    "__version__",
    "run_local_simulation",
    "validate_job_result",
]
