"""Bounded JSON framing for the first authenticated peer protocol.

The codec checks JSON structure only. Session owners validate message envelopes,
negotiation, request ordering, authorization, and operation-specific payloads.
"""

from __future__ import annotations

import asyncio
import json
import math
import struct
from typing import Any, NoReturn

from .errors import ConnectionClosed, ProtocolError

PROTOCOL_VERSION = 2
SUPPORTED_PROTOCOL_VERSIONS = (2, 1)
MAX_FRAME_BYTES = 65536
_MAX_NESTING = 16
_CLOSE_TIMEOUT = 2.0
Message = dict[str, Any]


def _object(pairs: list[tuple[str, Any]]) -> Message:
    result: Message = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("Duplicate JSON object key")
        result[key] = value
    return result


def _constant(value: str) -> NoReturn:
    raise ProtocolError("Non-finite JSON number")


def _validate_json(value: Any, depth: int = 0) -> None:
    if isinstance(value, (dict, list)):
        if depth >= _MAX_NESTING:
            raise ProtocolError("JSON nesting exceeds 16 levels")
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ProtocolError("JSON object keys must be strings")
                _validate_json(key)
                _validate_json(item, depth + 1)
        else:
            for item in value:
                _validate_json(item, depth + 1)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ProtocolError("Non-finite JSON number")
    elif isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ProtocolError("Invalid Unicode in JSON string") from exc
    elif value is not None and not isinstance(value, (int, bool)):
        raise ProtocolError("Unsupported JSON value")


def _validate_message(message: Any) -> None:
    if not isinstance(message, dict):
        raise ProtocolError("A frame must contain a JSON object")
    _validate_json(message)


async def read_frame(reader: asyncio.StreamReader) -> Message:
    """Read one object, rejecting invalid lengths before reading a payload."""
    try:
        header = await reader.readexactly(4)
        length = struct.unpack("!I", header)[0]
        if not 0 < length <= MAX_FRAME_BYTES:
            raise ProtocolError("Invalid frame length")
        payload = await reader.readexactly(length)
    except (asyncio.IncompleteReadError, OSError) as exc:
        raise ConnectionClosed("Peer stream closed during a frame") from exc
    try:
        message = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_object, parse_constant=_constant
        )
        _validate_message(message)
    except (ValueError, RecursionError) as exc:
        raise ProtocolError("Invalid JSON frame") from exc
    return dict(message)


async def write_frame(writer: asyncio.StreamWriter, message: Message) -> None:
    """Validate and encode one object, then honor the stream's backpressure."""
    _validate_message(message)
    try:
        payload = json.dumps(
            message, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (ValueError, RecursionError) as exc:
        raise ProtocolError("Invalid JSON frame") from exc
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError("Frame exceeds maximum size")
    try:
        writer.write(struct.pack("!I", len(payload)) + payload)
        await writer.drain()
    except OSError as exc:
        raise ConnectionClosed("Peer stream closed during a write") from exc


async def close_writer(writer: asyncio.StreamWriter) -> None:
    """Bound graceful TLS shutdown, aborting failed or cancelled shutdowns."""
    try:
        writer.close()
        async with asyncio.timeout(_CLOSE_TIMEOUT):
            await writer.wait_closed()
    except (OSError, RuntimeError):
        writer.transport.abort()
    except asyncio.CancelledError:
        writer.transport.abort()
        # StreamWriter shares one protocol close future. An earlier cancelled
        # close can leave that future cancelled even for a new cleanup caller.
        # Only propagate cancellation requested on this caller's own task.
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise
