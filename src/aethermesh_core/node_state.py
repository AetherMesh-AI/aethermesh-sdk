"""Versioned local node processing state persistence."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

NODE_STATE_VERSION = 1
_REQUIRED_FIELDS = {
    "version",
    "node_id",
    "processed_message_ids",
    "processed_assignment_count",
}


class NodeStatePersistenceError(ValueError):
    """Raised when a local node-state JSON file cannot be safely loaded or saved."""


@dataclass(frozen=True)
class LocalNodeProcessingState:
    """Durable local record of assignment messages already processed by one node."""

    node_id: str
    processed_message_ids: list[str]
    extra_fields: dict[str, Any]

    @property
    def processed_assignment_count(self) -> int:
        return len(self.processed_message_ids)

    def to_document(self) -> dict[str, Any]:
        return {
            **self.extra_fields,
            "version": NODE_STATE_VERSION,
            "node_id": self.node_id,
            "processed_message_ids": list(self.processed_message_ids),
            "processed_assignment_count": self.processed_assignment_count,
        }


_EMPTY_EXTRA_FIELDS: dict[str, Any] = {}


def empty_node_processing_state(node_id: str) -> LocalNodeProcessingState:
    """Build an empty version 1 processing state for a local node."""

    if not isinstance(node_id, str) or node_id == "":
        raise NodeStatePersistenceError("node state node_id must be a non-empty string")
    return LocalNodeProcessingState(
        node_id=node_id,
        processed_message_ids=[],
        extra_fields=dict(_EMPTY_EXTRA_FIELDS),
    )


def load_node_processing_state(
    path: str | Path,
    *,
    expected_node_id: str,
) -> LocalNodeProcessingState:
    """Load and validate an existing version 1 local node-state document."""

    state_path = Path(path)
    if not state_path.exists():
        return empty_node_processing_state(expected_node_id)

    try:
        with state_path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except json.JSONDecodeError as exc:
        raise NodeStatePersistenceError(
            f"node state JSON is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise NodeStatePersistenceError(
            f"could not read node state file: {exc}"
        ) from exc

    if not isinstance(document, dict):
        raise NodeStatePersistenceError("node state JSON must be an object")
    missing = sorted(_REQUIRED_FIELDS.difference(document))
    if missing:
        fields = ", ".join(missing)
        raise NodeStatePersistenceError(
            f"node state JSON is missing required field(s): {fields}"
        )
    version = document.get("version")
    if version != NODE_STATE_VERSION or isinstance(version, bool):
        raise NodeStatePersistenceError("node state JSON must contain version 1")

    node_id = document.get("node_id")
    if not isinstance(node_id, str) or node_id == "":
        raise NodeStatePersistenceError(
            "node state field 'node_id' must be a non-empty string"
        )
    if node_id != expected_node_id:
        raise NodeStatePersistenceError(
            f"node state belongs to node '{node_id}', not '{expected_node_id}'"
        )

    raw_message_ids = document.get("processed_message_ids")
    if not isinstance(raw_message_ids, list):
        raise NodeStatePersistenceError(
            "node state field 'processed_message_ids' must be a list"
        )
    processed_message_ids: list[str] = []
    seen: set[str] = set()
    for index, message_id in enumerate(raw_message_ids):
        if not isinstance(message_id, str) or message_id == "":
            raise NodeStatePersistenceError(
                f"node state processed_message_ids[{index}] must be a non-empty string"
            )
        if message_id in seen:
            raise NodeStatePersistenceError(
                f"node state contains duplicate processed message id: {message_id}"
            )
        processed_message_ids.append(message_id)
        seen.add(message_id)

    processed_assignment_count = document.get("processed_assignment_count")
    if not isinstance(processed_assignment_count, int) or isinstance(
        processed_assignment_count, bool
    ):
        raise NodeStatePersistenceError(
            "node state field 'processed_assignment_count' must be an integer"
        )
    if processed_assignment_count != len(processed_message_ids):
        raise NodeStatePersistenceError(
            "node state processed_assignment_count must equal the number of unique processed message ids"
        )

    extra_fields = {
        key: value for key, value in document.items() if key not in _REQUIRED_FIELDS
    }
    return LocalNodeProcessingState(
        node_id=node_id,
        processed_message_ids=processed_message_ids,
        extra_fields=extra_fields,
    )


def save_node_processing_state(
    path: str | Path,
    state: LocalNodeProcessingState,
) -> None:
    """Write a local node-state JSON document via temp-file then atomic replace."""

    state_path = Path(path)
    parent = state_path.parent
    temp_name: str | None = None
    try:
        parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=parent,
            prefix=f".{state_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_name = handle.name
            json.dump(state.to_document(), handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_name, state_path)
    except (OSError, TypeError, ValueError) as exc:
        if temp_name is not None:
            _remove_temp_file(temp_name)
        raise NodeStatePersistenceError(
            f"could not write node state file: {exc}"
        ) from exc


def _remove_temp_file(path: str | Path) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        return
    except OSError:
        return
