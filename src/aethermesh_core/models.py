"""Core data models for the local AetherMesh prototype."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class NodeIdentity:
    """Identity for one local node."""

    node_id: str
    node_name: str | None = None

    @classmethod
    def ephemeral(cls) -> NodeIdentity:
        """Create a caller-usable ephemeral identity for local demo runs."""

        return cls(node_id=f"node-{uuid4().hex}")


@dataclass(frozen=True)
class Job:
    """A small in-memory job assigned to a local node."""

    job_id: str
    job_type: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class JobResult:
    """Structured result emitted after local job execution."""

    job_id: str
    node_id: str
    status: str
    output: Any
    error: str | None
    contribution_units: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize the result into a JSON-compatible dictionary."""

        return asdict(self)
