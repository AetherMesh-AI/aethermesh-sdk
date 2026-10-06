"""Local mesh message envelopes for deterministic simulation output."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUPPORTED_MESSAGE_TYPES = frozenset(
    {
        "node_heartbeat",
        "job_assigned",
        "job_result_reported",
        "job_validated",
        "contribution_recorded",
    }
)


@dataclass(frozen=True)
class MeshMessage:
    """JSON-compatible message envelope for local mesh communication records."""

    message_id: str
    message_type: str
    sender_node_id: str
    recipient_node_id: str | None
    payload: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string("message_id", self.message_id)
        _require_supported_message_type(self.message_type)
        _require_non_empty_string("sender_node_id", self.sender_node_id)
        if self.recipient_node_id is not None:
            _require_non_empty_string("recipient_node_id", self.recipient_node_id)
        if not isinstance(self.payload, dict):
            raise ValueError("payload must be a dictionary")  # noqa: TRY004 - justification: public validation callers catch ValueError.
        _validate_json_compatible("payload", self.payload)
        if self.correlation_id is not None:
            _require_non_empty_string("correlation_id", self.correlation_id)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the message into a JSON-compatible dictionary."""

        return {
            "message_id": self.message_id,
            "message_type": self.message_type,
            "sender_node_id": self.sender_node_id,
            "recipient_node_id": self.recipient_node_id,
            "payload": dict(self.payload),
            "correlation_id": self.correlation_id,
        }


def _require_supported_message_type(message_type: str) -> None:
    _require_non_empty_string("message_type", message_type)
    if message_type not in SUPPORTED_MESSAGE_TYPES:
        supported = ", ".join(sorted(SUPPORTED_MESSAGE_TYPES))
        raise ValueError(f"message_type must be one of: {supported}")


def _require_non_empty_string(field_name: str, value: object) -> None:
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{field_name} must be a non-empty string")


def _validate_json_compatible(field_name: str, value: object) -> None:
    if value is None or isinstance(value, str | int | float | bool):
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_compatible(f"{field_name}[{index}]", item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field_name} keys must be strings")  # noqa: TRY004 - justification: public validation callers catch ValueError.
            _validate_json_compatible(f"{field_name}.{key}", item)
        return
    raise ValueError(f"{field_name} must contain only JSON-compatible values")


def message_from_mapping(entry: object) -> MeshMessage:
    """Build a validated MeshMessage from a JSON-like mapping."""

    if not isinstance(entry, dict):
        raise ValueError("must be an object")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    message_id = entry.get("message_id")
    message_type = entry.get("message_type")
    sender_node_id = entry.get("sender_node_id")
    recipient_node_id = entry.get("recipient_node_id")
    payload = entry.get("payload", {})
    correlation_id = entry.get("correlation_id")
    if not isinstance(message_id, str):
        raise ValueError("message_id must be a non-empty string")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    if not isinstance(message_type, str):
        raise ValueError("message_type must be a non-empty string")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    if not isinstance(sender_node_id, str):
        raise ValueError("sender_node_id must be a non-empty string")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    return MeshMessage(
        message_id=message_id,
        message_type=message_type,
        sender_node_id=sender_node_id,
        recipient_node_id=recipient_node_id,
        payload=payload,
        correlation_id=correlation_id,
    )
