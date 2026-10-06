"""File-backed local transport inboxes and outboxes for mesh messages.

Version 1 local transport inboxes live at ``<transport-dir>/inboxes/<node-id>.json``
and opt-in outboxes live at ``<transport-dir>/outboxes/<node-id>.json`` where
``<node-id>`` is URL-quoted for safe deterministic filenames. Empty inboxes are
omitted: materialization writes files only for recipients with addressed messages
in the source message log.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from aethermesh_core.json_io import atomic_write_json
from aethermesh_core.message_log import (
    build_collected_outbox_message_log_document,
    load_message_log_messages,
    write_message_log,
)
from aethermesh_core.messages import MeshMessage, message_from_mapping

LOCAL_TRANSPORT_INBOX_VERSION = 1
LOCAL_TRANSPORT_OUTBOX_VERSION = 1


class LocalTransportError(ValueError):
    """Raised when a file-backed local transport artifact cannot be loaded or saved."""


def materialize_local_inboxes(
    *, message_log_path: str | Path, transport_dir: str | Path
) -> dict[str, Any]:
    """Materialize addressed messages from a message log into per-node inbox files."""

    messages = load_message_log_messages(message_log_path)
    inboxes: dict[str, list[MeshMessage]] = {}
    seen_message_ids: set[str] = set()
    for message in messages:
        if message.message_id in seen_message_ids:
            raise LocalTransportError(
                f"duplicate message_id in source log: {message.message_id}"
            )
        seen_message_ids.add(message.message_id)
        if message.recipient_node_id is None:
            continue
        inboxes.setdefault(message.recipient_node_id, []).append(message)

    inbox_paths: dict[str, str] = {}
    for node_id in sorted(inboxes):
        path = write_local_inbox(
            transport_dir=transport_dir,
            node_id=node_id,
            messages=inboxes[node_id],
            source_message_log_path=message_log_path,
        )
        inbox_paths[node_id] = str(path)

    return {
        "command": "materialize-local-inboxes",
        "message_log_path": str(message_log_path),
        "transport_dir": str(transport_dir),
        "inbox_count": len(inbox_paths),
        "message_count": sum(len(messages) for messages in inboxes.values()),
        "node_ids": sorted(inbox_paths),
        "inbox_paths": inbox_paths,
    }


def write_local_inbox(
    *,
    transport_dir: str | Path,
    node_id: str,
    messages: Iterable[MeshMessage],
    source_message_log_path: str | Path | None = None,
) -> Path:
    """Write one version 1 local transport inbox with an atomic replace."""

    _require_node_id(node_id)
    message_list = list(messages)
    _validate_unique_messages_for_node(node_id, message_list)
    document: dict[str, Any] = {
        "version": LOCAL_TRANSPORT_INBOX_VERSION,
        "node_id": node_id,
        "messages": [message.to_dict() for message in message_list],
    }
    if source_message_log_path is not None:
        document["source_message_log_path"] = str(source_message_log_path)

    path = local_inbox_path(transport_dir, node_id)
    _write_inbox_document(path, document)
    return path


def write_local_outbox(
    *,
    transport_dir: str | Path,
    node_id: str,
    source_inbox_path: str | Path,
    processed_assignment_count: int,
    messages: Iterable[MeshMessage],
) -> Path:
    """Write one version 1 local transport outbox with an atomic replace."""

    _require_node_id(node_id)
    if not isinstance(processed_assignment_count, int) or isinstance(
        processed_assignment_count, bool
    ):
        raise LocalTransportError("processed_assignment_count must be an integer")
    if processed_assignment_count < 0:
        raise LocalTransportError("processed_assignment_count must be non-negative")
    message_list = list(messages)
    _validate_unique_messages_from_node(node_id, message_list)
    document: dict[str, Any] = {
        "version": LOCAL_TRANSPORT_OUTBOX_VERSION,
        "node_id": node_id,
        "source_inbox_path": str(source_inbox_path),
        "processed_assignment_count": processed_assignment_count,
        "messages": [message.to_dict() for message in message_list],
    }

    path = local_outbox_path(transport_dir, node_id)
    _write_outbox_document(path, document)
    return path


def load_local_inbox(*, transport_dir: str | Path, node_id: str) -> list[MeshMessage]:
    """Load and validate only ``node_id``'s local transport inbox messages."""

    _require_node_id(node_id)
    path = local_inbox_path(transport_dir, node_id)
    document = _load_inbox_document(path)
    document_node_id = document.get("node_id")
    if document_node_id != node_id:
        raise LocalTransportError(
            f"local transport inbox node_id mismatch: expected {node_id}, found {document_node_id}"
        )
    entries = document.get("messages")
    if not isinstance(entries, list):
        raise LocalTransportError(
            "local transport inbox field 'messages' must be a list"
        )

    messages: list[MeshMessage] = []
    seen_ids: set[str] = set()
    for index, entry in enumerate(entries):
        message = _message_from_inbox_entry(entry, index)
        if message.message_id in seen_ids:
            raise LocalTransportError(
                f"duplicate message_id in local transport inbox: {message.message_id}"
            )
        seen_ids.add(message.message_id)
        if message.recipient_node_id != node_id:
            raise LocalTransportError(
                f"local transport inbox entry {index} recipient_node_id must be {node_id}"
            )
        messages.append(message)
    return messages


def collect_local_outboxes(
    *, transport_dir: str | Path, message_log_path: str | Path
) -> dict[str, Any]:
    """Collect per-node local transport outboxes into one message log.

    The collection pass is read-only with respect to outbox files. Missing
    transport directories, missing outbox directories, and empty outbox
    directories are successful no-ops that still write a valid empty message log.
    """

    outboxes_dir = Path(transport_dir) / "outboxes"
    outbox_documents: list[tuple[str, list[MeshMessage]]] = []
    seen_node_ids: set[str] = set()

    if outboxes_dir.exists():
        if not outboxes_dir.is_dir():
            raise LocalTransportError(
                f"local transport outboxes path is not a directory: {outboxes_dir}"
            )
        for path in sorted(outboxes_dir.glob("*.json")):
            path_node_id = _node_id_from_transport_filename(path)
            document = _load_outbox_document(path)
            document_node_id = document.get("node_id")
            if not isinstance(document_node_id, str):
                raise LocalTransportError(
                    "local transport outbox node_id must be a string"
                )
            if document_node_id != path_node_id:
                raise LocalTransportError(
                    "local transport outbox node_id mismatch: "
                    f"expected {path_node_id}, found {document_node_id}"
                )
            if document_node_id in seen_node_ids:
                raise LocalTransportError(
                    f"duplicate local transport outbox node_id: {document_node_id}"
                )
            seen_node_ids.add(document_node_id)
            messages = _messages_from_outbox_document(document, document_node_id)
            outbox_documents.append((document_node_id, messages))

    outbox_documents.sort(key=lambda item: item[0])
    node_ids = [node_id for node_id, _messages in outbox_documents]
    messages = [
        message
        for _node_id, node_messages in outbox_documents
        for message in node_messages
    ]
    document = build_collected_outbox_message_log_document(
        messages=messages,
        transport_dir=transport_dir,
        output_path=message_log_path,
        outbox_count=len(outbox_documents),
        node_ids=node_ids,
    )
    write_message_log(message_log_path, document)
    return dict(document["metadata"])


def local_inbox_path(transport_dir: str | Path, node_id: str) -> Path:
    """Return the deterministic inbox path for ``node_id``."""

    _require_node_id(node_id)
    return Path(transport_dir) / "inboxes" / f"{quote(node_id, safe='-._~')}.json"


def local_outbox_path(transport_dir: str | Path, node_id: str) -> Path:
    """Return the deterministic outbox path for ``node_id``."""

    _require_node_id(node_id)
    return Path(transport_dir) / "outboxes" / f"{quote(node_id, safe='-._~')}.json"


def _load_inbox_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise LocalTransportError(f"local transport inbox file does not exist: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except json.JSONDecodeError as exc:
        raise LocalTransportError(
            f"local transport inbox JSON is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise LocalTransportError(
            f"could not read local transport inbox: {exc}"
        ) from exc

    if not isinstance(document, dict):
        raise LocalTransportError("local transport inbox JSON must be an object")
    version = document.get("version")
    if version != LOCAL_TRANSPORT_INBOX_VERSION or isinstance(version, bool):
        raise LocalTransportError("local transport inbox JSON must contain version 1")
    return document


def _load_outbox_document(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except json.JSONDecodeError as exc:
        raise LocalTransportError(
            f"local transport outbox JSON is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise LocalTransportError(
            f"could not read local transport outbox: {exc}"
        ) from exc

    if not isinstance(document, dict):
        raise LocalTransportError("local transport outbox JSON must be an object")
    version = document.get("version")
    if version != LOCAL_TRANSPORT_OUTBOX_VERSION or isinstance(version, bool):
        raise LocalTransportError("local transport outbox JSON must contain version 1")
    return document


def _messages_from_outbox_document(
    document: dict[str, Any], node_id: str
) -> list[MeshMessage]:
    entries = document.get("messages")
    if not isinstance(entries, list):
        raise LocalTransportError(
            "local transport outbox field 'messages' must be a list"
        )

    messages: list[MeshMessage] = []
    seen_ids: set[str] = set()
    for index, entry in enumerate(entries):
        message = _message_from_outbox_entry(entry, index)
        if message.message_id in seen_ids:
            raise LocalTransportError(
                f"duplicate message_id in local transport outbox: {message.message_id}"
            )
        seen_ids.add(message.message_id)
        if message.sender_node_id != node_id:
            raise LocalTransportError(
                f"local transport outbox entry {index} sender_node_id must be {node_id}"
            )
        messages.append(message)
    return messages


def _node_id_from_transport_filename(path: Path) -> str:
    if path.suffix != ".json":
        raise LocalTransportError(
            f"local transport outbox filename is unsupported: {path.name}"
        )
    stem = path.stem
    node_id = unquote(stem)
    _require_node_id(node_id)
    if quote(node_id, safe="-._~") != stem:
        raise LocalTransportError(
            f"local transport outbox filename is not a safe node id: {path.name}"
        )
    return node_id


def _message_from_inbox_entry(entry: Any, index: int) -> MeshMessage:
    try:
        return message_from_mapping(entry)
    except ValueError as exc:
        raise LocalTransportError(
            f"local transport inbox entry {index} is invalid: {exc}"
        ) from exc


def _message_from_outbox_entry(entry: Any, index: int) -> MeshMessage:
    try:
        return message_from_mapping(entry)
    except ValueError as exc:
        raise LocalTransportError(
            f"local transport outbox entry {index} is invalid: {exc}"
        ) from exc


def _validate_unique_messages_for_node(
    node_id: str, messages: list[MeshMessage]
) -> None:
    seen_ids: set[str] = set()
    for index, message in enumerate(messages):
        if message.message_id in seen_ids:
            raise LocalTransportError(
                f"duplicate message_id in local transport inbox: {message.message_id}"
            )
        seen_ids.add(message.message_id)
        if message.recipient_node_id != node_id:
            raise LocalTransportError(
                f"local transport inbox entry {index} recipient_node_id must be {node_id}"
            )


def _validate_unique_messages_from_node(
    node_id: str, messages: list[MeshMessage]
) -> None:
    seen_ids: set[str] = set()
    for index, message in enumerate(messages):
        if message.message_id in seen_ids:
            raise LocalTransportError(
                f"duplicate message_id in local transport outbox: {message.message_id}"
            )
        seen_ids.add(message.message_id)
        if message.sender_node_id != node_id:
            raise LocalTransportError(
                f"local transport outbox entry {index} sender_node_id must be {node_id}"
            )


def _write_inbox_document(path: Path, document: dict[str, Any]) -> None:
    try:
        atomic_write_json(path, document)
    except (OSError, TypeError, ValueError) as exc:
        raise LocalTransportError(
            f"could not write local transport inbox: {exc}"
        ) from exc


def _write_outbox_document(path: Path, document: dict[str, Any]) -> None:
    try:
        atomic_write_json(path, document)
    except (OSError, TypeError, ValueError) as exc:
        raise LocalTransportError(
            f"could not write local transport outbox: {exc}"
        ) from exc


def _require_node_id(node_id: object) -> None:
    if not isinstance(node_id, str) or node_id == "":
        raise LocalTransportError("node_id must be a non-empty string")
