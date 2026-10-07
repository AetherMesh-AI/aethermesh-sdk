"""Client lifecycle, contracts and cancellation independent of socket timing."""

import asyncio
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from aethermesh_core.network import (
    AuthenticationError,
    ConnectionClosed,
    Limits,
    PeerClient,
    PeerEndpoint,
    PeerError,
    ProtocolError,
    RemoteError,
    RequestTimeout,
    TLSIdentity,
)

IDENTITY = TLSIdentity(Path("certificate.pem"), Path("key.pem"), Path("ca.pem"))
PIN = hashlib.sha256(b"public test certificate").hexdigest()
ENDPOINT = PeerEndpoint("127.0.0.1", 12345, "localhost", PIN)
WELCOME = {
    "type": "welcome",
    "version": 1,
    "project": "test",
    "capabilities": ["status"],
}


def frame(message):
    data = json.dumps(message).encode()
    return len(data).to_bytes(4, "big") + data


def writer_mock():
    writer = MagicMock()
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    writer.is_closing.return_value = False
    return writer


class ClientContractTests(unittest.TestCase):
    def test_welcome_requires_exact_supported_contract(self):
        client = PeerClient(IDENTITY, project_id="test")
        self.assertEqual(client._welcome(WELCOME), ("status",))
        for change in (
            {"extra": True},
            {"type": "other"},
            {"version": True},
            {"version": 2},
            {"project": "other"},
            {"capabilities": {}},
            {"capabilities": ["status", {}]},
            {"capabilities": ["status", "status"]},
            {"capabilities": []},
            {"capabilities": ["status", "inference"]},
        ):
            with self.subTest(change=change), self.assertRaises(ProtocolError):
                client._welcome(WELCOME | change)

    def test_replies_have_bounded_ids_and_sanitized_errors(self):
        client = PeerClient(IDENTITY, project_id="test")
        client._next_id = 3
        self.assertEqual(
            client._reply({"type": "result", "id": 1, "result": {}}), (1, {})
        )
        _, error = client._reply(
            {
                "type": "error",
                "id": 2,
                "code": "busy",
                "message": "private peer content",
            }
        )
        self.assertIsInstance(error, RemoteError)
        self.assertEqual(error.code, "busy")
        self.assertNotIn("private", str(error))
        for message in (
            {},
            {"type": "result", "id": True, "result": {}},
            {"type": "result", "id": 0, "result": {}},
            {"type": "result", "id": 3, "result": {}},
            {"type": "result", "id": 1, "result": []},
            {"type": "result", "id": 1, "result": {}, "extra": 1},
            {"type": "error", "id": 1, "code": "unexpected", "message": "bad"},
            {"type": "error", "id": 1, "code": "busy", "message": "x" * 257},
            {"type": "other", "id": 1},
        ):
            with self.subTest(message=message), self.assertRaises(ProtocolError):
                client._reply(message)


class ClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = PeerClient(IDENTITY, project_id="test")
        self.reader = asyncio.StreamReader()
        self.writer = writer_mock()
        self.reader.feed_data(frame(WELCOME))
        self.open_patch = patch(
            "aethermesh_core.network.client.asyncio.open_connection",
            AsyncMock(return_value=(self.reader, self.writer)),
        )
        self.open_mock = self.open_patch.start()
        self.pin_patch = patch(
            "aethermesh_core.network.client.peer_fingerprint", return_value=PIN
        )
        self.pin_mock = self.pin_patch.start()
        self.context_patch = patch.object(
            TLSIdentity, "client_context", return_value=None
        )
        self.context_patch.start()

    async def asyncTearDown(self):
        await self.client.close()
        self.context_patch.stop()
        self.pin_patch.stop()
        self.open_patch.stop()

    async def test_connect_context_status_reply_and_close(self):
        self.assertFalse(self.client.connected)
        self.assertEqual(self.client.capabilities, ())
        async with self.client as client:
            await client.connect(ENDPOINT)
            self.assertTrue(client.connected)
            self.assertEqual(client.capabilities, ("status",))
            with self.assertRaises(PeerError):
                await client.connect(ENDPOINT)
            request = asyncio.create_task(client.request("status"))
            await asyncio.sleep(0)
            self.reader.feed_data(
                frame({"type": "result", "id": 1, "result": {"ok": True}})
            )
            self.assertEqual(await request, {"ok": True})
        self.assertFalse(self.client.connected)
        self.assertEqual(self.client.capabilities, ())
        self.writer.close.assert_called()
        self.writer.wait_closed.assert_awaited()

    async def test_client_input_validation(self):
        for timeout in (True, None, 0, -1, float("inf"), float("nan"), 3601, 10**1000):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                await self.client.request("status", timeout=timeout)
        for operation in (None, "", "x" * 65):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                await self.client.request(operation)
        with self.assertRaises(ValueError):
            await self.client.request("status", [])
        with self.assertRaises(ConnectionClosed):
            await self.client.request("status")

    async def test_connect_pin_failure_and_recover(self):
        self.pin_mock.return_value = "0" * 64
        with self.assertRaises(AuthenticationError):
            await self.client.connect(ENDPOINT)
        self.assertFalse(self.client.connected)
        self.writer.close.assert_called()
        self.pin_mock.return_value = PIN
        await self.client.connect(ENDPOINT)
        self.assertTrue(self.client.connected)

    async def test_remote_error_and_replies_after_timeout(self):
        await self.client.connect(ENDPOINT)
        request = asyncio.create_task(self.client.request("missing", {"text": "hello"}))
        await asyncio.sleep(0)
        self.reader.feed_data(
            frame(
                {
                    "type": "error",
                    "id": 1,
                    "code": "unsupported_operation",
                    "message": "secret",
                }
            )
        )
        with self.assertRaises(RemoteError) as caught:
            await request
        self.assertEqual(caught.exception.code, "unsupported_operation")
        with self.assertRaises(RequestTimeout):
            await self.client.request("status", timeout=0.001)
        self.reader.feed_data(frame({"type": "result", "id": 2, "result": {}}))
        await asyncio.sleep(0.01)
        self.assertTrue(self.client.connected)
        self.assertEqual(self.client._pending, {})
        sent = [
            json.loads(call.args[0][4:]) for call in self.writer.write.call_args_list
        ]
        self.assertIn({"type": "cancel", "id": 2}, sent)

    async def test_cancel_request_and_close_pending(self):
        await self.client.connect(ENDPOINT)
        request = asyncio.create_task(self.client.request("status"))
        await asyncio.sleep(0)
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        pending = asyncio.create_task(self.client.request("status"))
        await asyncio.sleep(0)
        await self.client.close()
        with self.assertRaises(ConnectionClosed):
            await pending
        self.assertEqual(self.client._pending, {})

    async def test_protocol_failure_disconnects_and_fails_pending(self):
        await self.client.connect(ENDPOINT)
        request = asyncio.create_task(self.client.request("status"))
        await asyncio.sleep(0)
        self.reader.feed_data(frame({"type": "result", "id": 300, "result": {}}))
        with self.assertRaises(ProtocolError):
            await request
        await asyncio.sleep(0)
        self.assertFalse(self.client.connected)

    async def test_eof_disconnects_pending(self):
        await self.client.connect(ENDPOINT)
        request = asyncio.create_task(self.client.request("status"))
        await asyncio.sleep(0)
        self.reader.feed_eof()
        with self.assertRaises(ConnectionClosed):
            await request
        self.assertFalse(self.client.connected)

    async def test_local_budgets_and_cancellation_before_submission(self):
        self.client = PeerClient(
            IDENTITY, project_id="test", limits=Limits(max_inflight=1, max_requests=1)
        )
        await self.client.connect(ENDPOINT)
        first = asyncio.create_task(self.client.request("status"))
        await asyncio.sleep(0)
        with self.assertRaises(PeerError):
            await self.client.request("status")
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        with self.assertRaises(PeerError):
            await self.client.request("status")
        await self.client._cancel_request(0, self.writer)

    async def test_failed_cancel_closes_connection(self):
        await self.client.connect(ENDPOINT)
        with patch(
            "aethermesh_core.network.client.write_frame", AsyncMock(side_effect=OSError)
        ):
            await self.client._cancel_request(1, self.writer)
        self.assertFalse(self.client.connected)

    async def test_cancel_for_previous_session_does_not_touch_new_one(self):
        await self.client.connect(ENDPOINT)
        another_writer = writer_mock()
        await self.client._cancel_request(1, another_writer)
        self.assertTrue(self.client.connected)
        another_writer.write.assert_not_called()

    async def test_backpressured_requests_recheck_session_and_capacity(self):
        await self.client.connect(ENDPOINT)
        await self.client._send_lock.acquire()
        task = asyncio.create_task(self.client.request("status"))
        await asyncio.sleep(0)
        await self.client.close()
        self.client._send_lock.release()
        with self.assertRaises(ConnectionClosed):
            await task

    async def test_timeout_waiting_for_send_lock(self):
        await self.client.connect(ENDPOINT)
        await self.client._send_lock.acquire()
        try:
            with self.assertRaises(RequestTimeout):
                await self.client.request("status", timeout=0.001)
        finally:
            self.client._send_lock.release()

    async def test_request_send_failure_closes_and_consumes_pending_error(self):
        await self.client.connect(ENDPOINT)
        with (
            patch(
                "aethermesh_core.network.client.write_frame",
                AsyncMock(side_effect=OSError),
            ),
            self.assertRaises(ConnectionClosed),
        ):
            await self.client.request("status")
        self.assertFalse(self.client.connected)

    async def test_read_oserror_closes_transport(self):
        with patch(
            "aethermesh_core.network.client.read_frame", AsyncMock(side_effect=OSError)
        ):
            self.client._writer = self.writer
            await self.client._receive(self.reader, self.writer)
        self.assertFalse(self.client.connected)
        self.writer.close.assert_called()

    async def test_shutdown_ignores_already_finished_future(self):
        finished = asyncio.get_running_loop().create_future()
        finished.set_result({})
        self.client._pending[1] = finished
        self.client._fail_pending(ConnectionClosed("shutdown"))
        self.assertEqual(finished.result(), {})
        self.assertEqual(self.client._pending, {})

    async def test_stale_cancel_failure_does_not_close_replacement(self):
        await self.client.connect(ENDPOINT)
        replacement = writer_mock()

        async def fail_old_write(*args):
            self.client._writer = replacement
            raise OSError("old transport ended")

        with patch("aethermesh_core.network.client.write_frame", fail_old_write):
            await self.client._cancel_request(1, self.writer)
        self.assertIs(self.client._writer, replacement)
        self.assertTrue(self.client.connected)

    async def test_real_codec_write_failure_closes_session(self):
        await self.client.connect(ENDPOINT)
        self.writer.drain.side_effect = BrokenPipeError
        with self.assertRaises(ConnectionClosed):
            await self.client.request("status")
        self.assertFalse(self.client.connected)
        self.writer.close.assert_called()

    async def test_stale_submission_failure_preserves_replacement(self):
        await self.client.connect(ENDPOINT)
        replacement = writer_mock()

        async def fail_old_submission(*args):
            self.client._writer = replacement
            raise ConnectionClosed("old session")

        with (
            patch("aethermesh_core.network.client.write_frame", fail_old_submission),
            self.assertRaises(ConnectionClosed),
        ):
            await self.client.request("status")
        self.assertIs(self.client._writer, replacement)
        self.assertTrue(self.client.connected)
