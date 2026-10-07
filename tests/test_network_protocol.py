"""Adversarial framing tests; no network or optional asyncio plugin required."""

import asyncio
import json
import struct
import unittest
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

from aethermesh_core.network.errors import (
    AuthenticationError,
    ConnectionClosed,
    PeerError,
    ProtocolError,
    RemoteError,
    RequestTimeout,
)
from aethermesh_core.network.protocol import (
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    close_writer,
    read_frame,
    write_frame,
)


def _reader(payload: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack("!I", len(payload)) + payload)
    reader.feed_eof()
    return reader


def _writer() -> Any:
    writer = Mock()
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    return writer


class FrameTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip_preserves_json_values_and_frame_boundary(self) -> None:
        message = {
            "type": "hello",
            "text": "hello \u2603",
            "nested": [{"values": [None, True, False, 42, -9, 2.5]}],
            "empty": {},
        }
        writer = _writer()
        await write_frame(writer, message)
        frame = writer.write.call_args.args[0]
        self.assertEqual(PROTOCOL_VERSION, 2)
        self.assertEqual(MAX_FRAME_BYTES, 65536)
        self.assertEqual(struct.unpack("!I", frame[:4])[0], len(frame) - 4)
        reader = asyncio.StreamReader()
        reader.feed_data(frame + struct.pack("!I", 2) + b"{}")
        reader.feed_eof()
        self.assertEqual(await read_frame(reader), message)
        self.assertEqual(await read_frame(reader), {})
        writer.drain.assert_awaited_once_with()

    async def test_bad_lengths_are_rejected_before_payload_read(self) -> None:
        for length in (0, MAX_FRAME_BYTES + 1, 2**32 - 1):
            with self.subTest(length=length):
                reader = Mock()
                reader.readexactly = AsyncMock(return_value=struct.pack("!I", length))
                with self.assertRaisesRegex(ProtocolError, "length"):
                    await read_frame(reader)
                reader.readexactly.assert_awaited_once_with(4)

    async def test_exact_maximum_payload_and_one_byte_over(self) -> None:
        message = {"p": "a" * (MAX_FRAME_BYTES - 8)}
        writer = _writer()
        await write_frame(writer, message)
        frame = writer.write.call_args.args[0]
        self.assertEqual(len(frame), MAX_FRAME_BYTES + 4)
        self.assertEqual(await read_frame(_reader(frame[4:])), message)
        writer.reset_mock()
        message["p"] += "a"
        with self.assertRaisesRegex(ProtocolError, "maximum"):
            await write_frame(writer, message)
        writer.write.assert_not_called()

    async def test_incomplete_header_payload_eof_and_read_failure(self) -> None:
        for data in (b"", b"\x00\x00", struct.pack("!I", 5) + b"{"):
            with self.subTest(data=data):
                reader = asyncio.StreamReader()
                reader.feed_data(data)
                reader.feed_eof()
                with self.assertRaises(ConnectionClosed):
                    await read_frame(reader)
        reader = Mock()
        reader.readexactly = AsyncMock(side_effect=ConnectionResetError("reset"))
        with self.assertRaises(ConnectionClosed):
            await read_frame(reader)

    async def test_rejects_unsafe_or_nonobject_json(self) -> None:
        payloads = (
            b"{} trailing",
            b'{"key":',
            b"\xff",
            b"\xef\xbb\xbf{}",
            b"null",
            b"[]",
            b"1",
            b'"string"',
            b'{"x":1,"x":2}',
            b'{"nested":{"x":1,"x":2}}',
            b'{"x":NaN}',
            b'{"x":Infinity}',
            b'{"x":-Infinity}',
            b'{"x":1e999}',
            b'{"x":"\\ud800"}',
            b'{"\\ud800":1}',
            b'{"x":' + b"9" * 5000 + b"}",
            b'{"x":' + b"[" * 1200 + b"]" * 1200 + b"}",
        )
        for payload in payloads:
            with self.subTest(payload=payload[:50]), self.assertRaises(ProtocolError):
                await read_frame(_reader(payload))

    async def test_nesting_limit_for_objects_and_arrays(self) -> None:
        for container in (dict, list):
            value: Any = None
            for _ in range(15):
                value = {"x": value} if container is dict else [value]
            message = {"x": value}
            writer = _writer()
            await write_frame(writer, message)
            self.assertEqual(
                await read_frame(_reader(json.dumps(message).encode())), message
            )
            too_deep = {"x": message}
            with self.assertRaisesRegex(ProtocolError, "nesting"):
                await write_frame(writer, too_deep)
            with self.assertRaisesRegex(ProtocolError, "nesting"):
                await read_frame(_reader(json.dumps(too_deep).encode()))

    async def test_invalid_outbound_values_never_write(self) -> None:
        cycle: dict[str, Any] = {}
        cycle["cycle"] = cycle
        messages: list[Any] = [
            [],
            {1: "integer key"},
            {"x": (1, 2)},
            {"x": set()},
            {"x": b"bytes"},
            {"x": float("nan")},
            {"x": float("inf")},
            {"x": float("-inf")},
            {"x": "\ud800"},
            {"\ud800": "invalid key encoding"},
            {"x": 10**5000},
            cycle,
        ]
        for index, message in enumerate(messages):
            with self.subTest(index=index):
                writer = _writer()
                with self.assertRaises(ProtocolError):
                    await write_frame(writer, message)
                writer.write.assert_not_called()

    async def test_write_and_drain_failures_are_typed(self) -> None:
        for method in ("write", "drain"):
            writer = _writer()
            getattr(writer, method).side_effect = BrokenPipeError("closed")
            with self.assertRaises(ConnectionClosed):
                await write_frame(writer, {})

    async def test_serialization_recursion_failure_is_typed(self) -> None:
        writer = _writer()
        with (
            patch(
                "aethermesh_core.network.protocol.json.dumps",
                side_effect=RecursionError,
            ),
            self.assertRaises(ProtocolError),
        ):
            await write_frame(writer, {})
        writer.write.assert_not_called()

    async def test_graceful_close_and_abort_on_io_failure(self) -> None:
        writer = _writer()
        await close_writer(writer)
        writer.close.assert_called_once_with()
        writer.wait_closed.assert_awaited_once_with()
        writer.transport.abort.assert_not_called()
        for method in ("close", "wait_closed"):
            failed = _writer()
            getattr(failed, method).side_effect = OSError("close failed")
            await close_writer(failed)
            failed.transport.abort.assert_called_once_with()
        unexpected = _writer()
        unexpected.wait_closed.side_effect = RuntimeError("transport failed")
        await close_writer(unexpected)
        unexpected.transport.abort.assert_called_once_with()

    async def test_shutdown_timeout_is_bounded_and_aborted(self) -> None:
        writer = _writer()

        async def blocked() -> None:
            await asyncio.Event().wait()

        writer.wait_closed.side_effect = blocked
        with patch("aethermesh_core.network.protocol._CLOSE_TIMEOUT", 0.001):
            await asyncio.wait_for(close_writer(writer), timeout=1)
        writer.transport.abort.assert_called_once_with()

    async def test_cancelled_shutdown_aborts_and_propagates(self) -> None:
        writer = _writer()
        waiting = asyncio.Event()

        async def blocked() -> None:
            waiting.set()
            await asyncio.Event().wait()

        writer.wait_closed.side_effect = blocked
        closing = asyncio.create_task(close_writer(writer))
        await waiting.wait()
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        writer.transport.abort.assert_called_once_with()

    async def test_cancelled_shared_transport_waiter_does_not_cancel_retry(
        self,
    ) -> None:
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport = Mock(spec=asyncio.Transport)
        writer = asyncio.StreamWriter(
            transport, protocol, reader, asyncio.get_running_loop()
        )
        waiting = asyncio.Event()
        wait_closed = writer.wait_closed

        async def wait_for_transport() -> None:
            waiting.set()
            await wait_closed()

        with patch.object(writer, "wait_closed", side_effect=wait_for_transport):
            closing = asyncio.create_task(close_writer(writer))
            await waiting.wait()
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            transport.abort.assert_called_once_with()
            protocol.connection_lost(None)
            await close_writer(writer)
        transport.close.assert_called_with()
        self.assertEqual(transport.abort.call_count, 2)

    async def test_timed_out_shared_transport_waiter_does_not_cancel_retry(
        self,
    ) -> None:
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport = Mock(spec=asyncio.Transport)
        writer = asyncio.StreamWriter(
            transport, protocol, reader, asyncio.get_running_loop()
        )
        with patch("aethermesh_core.network.protocol._CLOSE_TIMEOUT", 0.001):
            await asyncio.wait_for(close_writer(writer), timeout=1)
        transport.abort.assert_called_once_with()
        protocol.connection_lost(None)
        await close_writer(writer)
        self.assertEqual(transport.abort.call_count, 2)

    async def test_cancellation_is_not_reclassified_as_network_failure(self) -> None:
        reader = Mock()
        reader.readexactly = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await read_frame(reader)
        writer = _writer()
        writer.drain.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await write_frame(writer, {})


class ErrorTests(unittest.TestCase):
    def test_stable_error_types_and_remote_code(self) -> None:
        for error in (
            AuthenticationError,
            ConnectionClosed,
            ProtocolError,
            RequestTimeout,
            RemoteError,
        ):
            self.assertTrue(issubclass(error, PeerError))
        error = RemoteError("unsupported_operation", "Not supported")
        self.assertEqual(error.code, "unsupported_operation")
        self.assertEqual(str(error), "Not supported")
        self.assertEqual(error.args, ("Not supported",))
        self.assertIsInstance(cast(Exception, error), PeerError)
