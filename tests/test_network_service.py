"""Service contracts and bounded lifecycle tests, including real mutual TLS."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from aethermesh_core.network.config import Limits, TLSIdentity, certificate_fingerprint
from aethermesh_core.network.errors import (
    AuthenticationError,
    ConnectionClosed,
    PeerError,
    ProtocolError,
    RemoteError,
)
from aethermesh_core.network.profile import NodeProfile
from aethermesh_core.network.protocol import close_writer, read_frame, write_frame
from aethermesh_core.network.service import PeerService, _Session
from tests.network_test_support import TestPKI


def _writer() -> Mock:
    writer = Mock(spec=asyncio.StreamWriter)
    writer.transport = Mock()
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    return writer


def _request(request_id: int = 1, **changes: Any) -> dict[str, Any]:
    return {
        "type": "request",
        "id": request_id,
        "operation": "status",
        "payload": {},
        **changes,
    }


class PeerServiceLogicTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.identity = TLSIdentity(Path("certificate"), Path("key"), Path("ca"))
        self.service = PeerService(
            self.identity,
            project_id="project.test-1",
            allowed_peers=frozenset({"a" * 64}),
            capabilities=frozenset({"echo"}),
        )

    def test_constructor_validates_and_never_opens_listener(self) -> None:
        with patch("asyncio.start_server") as start:
            PeerService(self.identity, project_id="a", allowed_peers=frozenset())
            start.assert_not_called()
        for project in ("", "a/b", "a" * 129, "é", None):
            with self.subTest(project=project), self.assertRaises(ValueError):
                PeerService(
                    self.identity, project_id=project, allowed_peers=frozenset()
                )
        for peers in ({"a" * 64}, frozenset({"x"}), frozenset({None})):
            with self.subTest(peers=peers), self.assertRaises((ValueError, TypeError)):
                PeerService(self.identity, project_id="a", allowed_peers=peers)
        for capabilities in ({"echo"}, frozenset({"inference"})):
            with self.subTest(capabilities=capabilities), self.assertRaises(ValueError):
                PeerService(
                    self.identity,
                    project_id="a",
                    allowed_peers=frozenset(),
                    capabilities=capabilities,
                )
        with self.assertRaises(PeerError):
            _ = self.service.address

    async def test_start_validation_and_explicit_ssl_configuration(self) -> None:
        for host, port in (
            ("", 0),
            (None, 0),
            ("localhost", True),
            ("localhost", 65536),
        ):
            with self.subTest(host=host, port=port), self.assertRaises(ValueError):
                await self.service.start(host, port)
        server = Mock(spec=asyncio.Server)
        server.sockets = [Mock()]
        server.sockets[0].getsockname.return_value = ("127.0.0.1", 12345)
        server.wait_closed = AsyncMock()
        context = Mock()
        with (
            patch.object(TLSIdentity, "server_context", return_value=context),
            patch("asyncio.start_server", new=AsyncMock(return_value=server)) as start,
        ):
            await self.service.start()
            self.assertEqual(self.service.address, ("127.0.0.1", 12345))
            self.assertIs(start.call_args.kwargs["ssl"], context)
            self.assertEqual(start.call_args.kwargs["ssl_handshake_timeout"], 5.0)
            self.assertEqual(start.call_args.kwargs["ssl_shutdown_timeout"], 30.0)
            with self.assertRaises(PeerError):
                await self.service.start()
            old_callback = start.call_args.args[0]
            await self.service.close()
            await self.service.close()
            await self.service.start()
            late_writer = _writer()
            old_callback(asyncio.StreamReader(), late_writer)
            late_writer.transport.abort.assert_called_once_with()
            self.assertFalse(self.service._connections)
            await self.service.close()
        server.close.assert_called()

    async def test_serve_requires_start_and_context_manager_closes(self) -> None:
        await self.service.close()
        with self.assertRaises(PeerError):
            await self.service.serve_forever()
        with (
            patch.object(self.service, "start", new=AsyncMock()) as start,
            patch.object(self.service, "close", new=AsyncMock()) as close,
        ):
            async with self.service as entered:
                self.assertIs(entered, self.service)
            start.assert_awaited_once_with()
            close.assert_awaited_once_with()

    async def test_serve_forever_closes_when_cancelled(self) -> None:
        server = Mock(spec=asyncio.Server)
        self.service._stopped = Mock(spec=asyncio.Event)
        self.service._stopped.wait = AsyncMock(side_effect=asyncio.CancelledError)
        self.service._server = server
        with patch.object(self.service, "_close", new=AsyncMock()) as close:
            with self.assertRaises(asyncio.CancelledError):
                await self.service.serve_forever()
            close.assert_awaited_once_with(server)

    async def test_old_listener_cannot_close_a_replacement(self) -> None:
        old_server = Mock(spec=asyncio.Server)
        new_server = Mock(spec=asyncio.Server)
        self.service._server = new_server
        await self.service._close(old_server)
        new_server.close.assert_not_called()
        self.assertIs(self.service._server, new_server)

    async def test_cancelling_close_does_not_cancel_cleanup(self) -> None:
        server = Mock(spec=asyncio.Server)
        gate = asyncio.Event()
        server.wait_closed = AsyncMock(side_effect=gate.wait)
        self.service._server = server
        connection = asyncio.create_task(asyncio.sleep(3600))
        self.service._connections.add(connection)
        closing = asyncio.create_task(self.service.close())
        await asyncio.sleep(0)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertFalse(self.service._shutdown_task.cancelled())
        gate.set()
        await self.service.close()
        self.assertTrue(connection.cancelled())
        self.assertIsNone(self.service._server)

    async def test_accept_rejects_closed_capacity_and_unapproved_identity(self) -> None:
        reader = asyncio.StreamReader()
        writer = _writer()
        self.service._accept(reader, writer, generation=self.service._generation)
        writer.transport.abort.assert_called_once_with()
        self.service._closing = False
        self.service.limits = Limits(max_connections=1)
        busy = asyncio.create_task(asyncio.sleep(3600))
        self.service._connections.add(busy)
        writer.reset_mock()
        self.service._accept(reader, writer, generation=self.service._generation)
        writer.transport.abort.assert_called_once_with()
        busy.cancel()
        await asyncio.gather(busy, return_exceptions=True)
        self.service._connections.clear()
        for identity in ("b" * 64, AuthenticationError("missing identity")):
            writer.reset_mock()
            with patch(
                "aethermesh_core.network.service.peer_fingerprint",
                **(
                    {"side_effect": identity}
                    if isinstance(identity, Exception)
                    else {"return_value": identity}
                ),
            ):
                self.service._accept(
                    reader, writer, generation=self.service._generation
                )
            writer.transport.abort.assert_called_once_with()
        self.assertFalse(self.service._connections)

    async def test_shutdown_closes_an_admitted_task_cancelled_before_it_starts(
        self,
    ) -> None:
        self.service._closing = False
        writer = _writer()
        server = Mock(spec=asyncio.Server)
        server.wait_closed = AsyncMock()
        with patch(
            "aethermesh_core.network.service.peer_fingerprint", return_value="a" * 64
        ):
            self.service._accept(
                asyncio.StreamReader(), writer, generation=self.service._generation
            )
        task = next(iter(self.service._connections))
        task.cancel()
        await self.service._shutdown(server)
        writer.close.assert_called_once_with()
        self.assertFalse(self.service._writers)
        self.assertFalse(self.service._connections)
        self.assertTrue(self.service._stopped.is_set())

    async def test_status_echo_and_operation_errors(self) -> None:
        self.assertEqual(
            await self.service._operate("status", {}),
            {"protocol_version": 1, "capabilities": ["echo", "status"]},
        )
        self.assertEqual(
            await self.service._operate("echo", {"text": "hi"}), {"text": "hi"}
        )
        self.assertEqual(
            await self.service._operate("echo", {"text": "", "delay_ms": 1}),
            {"text": ""},
        )
        for operation, payload, code in (
            ("inference", {}, "unsupported_operation"),
            ("status", {"key": "secret"}, "invalid_request"),
            ("echo", {}, "invalid_request"),
            ("echo", {"text": "", "extra": 1}, "invalid_request"),
            ("echo", {"text": None}, "invalid_request"),
            ("echo", {"text": "é" * 2049}, "invalid_request"),
            ("echo", {"text": "", "delay_ms": True}, "invalid_request"),
            ("echo", {"text": "", "delay_ms": -1}, "invalid_request"),
            ("echo", {"text": "", "delay_ms": 1001}, "invalid_request"),
        ):
            with self.subTest(operation=operation, payload=payload):
                with self.assertRaises(RemoteError) as raised:
                    await self.service._operate(operation, payload)
                self.assertEqual(raised.exception.code, code)
                self.assertNotIn("secret", str(raised.exception))
        default = PeerService(self.identity, project_id="x", allowed_peers=frozenset())
        with self.assertRaises(RemoteError) as raised:
            await default._operate("echo", {"text": "hello"})
        self.assertEqual(raised.exception.code, "unsupported_operation")

    async def test_malformed_envelopes_and_replay_are_rejected(self) -> None:
        for message in (
            {},
            _request(True),
            _request(0),
            _request(-1),
            _request("1"),
            _request(1, type="unknown"),
            _request(1, operation=None),
            _request(1, payload=[]),
            _request(1, extra=True),
            {"type": "cancel", "id": 1, "extra": 0},
            {"type": "request", "id": 1},
        ):
            with self.subTest(message=message), self.assertRaises(ProtocolError):
                await self.service._receive(_Session(_writer()), message)
        session = _Session(_writer(), last_id=3)
        for request_id in (2, 3):
            with self.assertRaises(ProtocolError):
                await self.service._receive(session, _request(request_id))

    async def test_request_results_errors_and_busy_limits(self) -> None:
        session = _Session(_writer())
        with patch.object(self.service, "_send", new=AsyncMock()) as send:
            self.assertTrue(await self.service._receive(session, _request()))
            await asyncio.gather(*session.requests.values())
            self.assertEqual(send.call_args.args[1]["result"]["protocol_version"], 1)
            self.assertFalse(session.requests)
            self.assertFalse(session.responding)
            self.assertTrue(
                await self.service._receive(session, _request(2, operation="unknown"))
            )
            await asyncio.gather(*session.requests.values())
            self.assertEqual(send.call_args.args[1]["code"], "unsupported_operation")
            self.service.limits = Limits(max_requests=2)
            self.assertFalse(await self.service._receive(session, _request(3)))
            self.assertEqual(send.call_args.args[1]["code"], "busy")
            self.service.limits = Limits(max_inflight=1)
            busy = asyncio.create_task(asyncio.sleep(3600))
            session.requests[4] = busy
            self.assertTrue(await self.service._receive(session, _request(4)))
            self.assertEqual(send.call_args.args[1]["message"], "peer is busy")
            busy.cancel()
            await asyncio.gather(busy, return_exceptions=True)

    async def test_cancel_running_request_and_ignore_completed_response(self) -> None:
        session = _Session(_writer())
        with patch.object(self.service, "_send", new=AsyncMock()) as send:
            await self.service._receive(
                session,
                _request(1, operation="echo", payload={"text": "", "delay_ms": 1000}),
            )
            task = session.requests[1]
            self.assertTrue(
                await self.service._receive(session, {"type": "cancel", "id": 1})
            )
            self.assertTrue(task.cancelled())
            self.assertEqual(send.call_args.args[1]["code"], "cancelled")
            self.assertEqual(send.await_count, 1)
            await self.service._receive(session, {"type": "cancel", "id": 1})
            self.assertEqual(send.await_count, 1)
            responding = asyncio.create_task(asyncio.sleep(3600))
            session.requests[2] = responding
            session.responding.add(2)
            await self.service._receive(session, {"type": "cancel", "id": 2})
            self.assertFalse(responding.cancelled())
            self.assertEqual(send.await_count, 1)
            responding.cancel()
            await asyncio.gather(responding, return_exceptions=True)

    async def test_failed_response_aborts_transport_and_write_lock_is_bounded(
        self,
    ) -> None:
        session = _Session(_writer())
        with patch.object(
            self.service, "_send", new=AsyncMock(side_effect=ConnectionClosed("gone"))
        ):
            await self.service._receive(session, _request())
            await asyncio.gather(*session.requests.values(), return_exceptions=True)
            session.writer.transport.abort.assert_called_once_with()
            self.assertFalse(session.requests)
        self.service.limits = Limits(io_timeout=0.01)
        await session.write_lock.acquire()
        try:
            with self.assertRaises(TimeoutError):
                await self.service._send(session, {"type": "result"})
        finally:
            session.write_lock.release()
        with patch(
            "aethermesh_core.network.service.write_frame", new=AsyncMock()
        ) as write:
            await self.service._send(session, {"type": "result"})
            write.assert_awaited_once_with(session.writer, {"type": "result"})

    async def test_invalid_negotiation_disconnects_without_a_welcome(self) -> None:
        hello = {"type": "hello", "versions": [1], "project": self.service.project_id}
        for message in (
            {},
            {**hello, "extra": True},
            {**hello, "type": "request"},
            {**hello, "project": "other"},
            {**hello, "versions": 1},
            {**hello, "versions": []},
            {**hello, "versions": [1] * 17},
            {**hello, "versions": [True]},
            {**hello, "versions": [3]},
        ):
            with self.subTest(message=message):
                writer = _writer()
                with (
                    patch(
                        "aethermesh_core.network.service.read_frame",
                        new=AsyncMock(return_value=message),
                    ),
                    patch.object(self.service, "_send", new=AsyncMock()) as send,
                ):
                    await self.service._connection(
                        asyncio.StreamReader(), writer, "a" * 64
                    )
                send.assert_not_awaited()
                writer.close.assert_called_once_with()

    async def test_negotiation_timeout_and_disconnect_cancel_work(self) -> None:
        self.service.limits = Limits(handshake_timeout=0.01, io_timeout=0.01)

        async def wait_for_input(*_args: Any) -> dict[str, Any]:
            await asyncio.sleep(3600)
            return {}

        writer = _writer()
        with patch(
            "aethermesh_core.network.service.read_frame", side_effect=wait_for_input
        ):
            await self.service._connection(asyncio.StreamReader(), writer, "a" * 64)
        writer.close.assert_called_once_with()
        hello = {"type": "hello", "versions": [1], "project": self.service.project_id}
        request = _request(
            1, operation="echo", payload={"text": "private", "delay_ms": 1000}
        )
        with (
            patch(
                "aethermesh_core.network.service.read_frame",
                new=AsyncMock(side_effect=[hello, request, ConnectionClosed("gone")]),
            ),
            patch.object(self.service, "_send", new=AsyncMock()) as send,
        ):
            await self.service._connection(asyncio.StreamReader(), _writer(), "a" * 64)
            self.assertEqual(send.await_count, 1)
        self.service.limits = Limits(max_requests=1)
        with (
            patch(
                "aethermesh_core.network.service.read_frame",
                new=AsyncMock(side_effect=[hello, _request(1), _request(2)]),
            ),
            patch.object(self.service, "_send", new=AsyncMock()) as send,
        ):
            await self.service._connection(asyncio.StreamReader(), _writer(), "a" * 64)
            self.assertEqual(send.call_args.args[1]["code"], "busy")

    def test_profile_and_bindings_are_validated_without_io(self) -> None:
        profile = NodeProfile("node-existing", "quiet-fox")
        binding = {"a" * 64: "node-client"}
        service = PeerService(
            self.identity,
            project_id="test",
            allowed_peers=frozenset(binding),
            node_profile=profile,
            expected_peer_ids=binding,
        )
        self.assertIs(service.node_profile, profile)
        with self.assertRaises(AttributeError):
            service.node_profile = NodeProfile("replacement")
        with self.assertRaises(AttributeError):
            service.expected_peer_ids = {}
        self.assertEqual(service.peers, ())
        binding["a" * 64] = "replacement"
        self.assertEqual(service.expected_peer_ids["a" * 64], "node-client")
        with self.assertRaises(TypeError):
            service.expected_peer_ids["a" * 64] = "replacement"
        for options in (
            {"node_profile": {}},
            {"expected_peer_ids": [("a" * 64, "node-client")]},
            {"expected_peer_ids": {"invalid": "node-client"}},
            {"expected_peer_ids": {"a" * 64: "bad id"}},
            {"expected_peer_ids": {"b" * 64: "node-client"}},
        ):
            with (
                self.subTest(options=options),
                self.assertRaises((TypeError, ValueError)),
            ):
                PeerService(
                    self.identity,
                    project_id="test",
                    allowed_peers=frozenset({"a" * 64}),
                    **options,
                )

    async def test_negotiation_selects_highest_common_and_legacy_shape(self) -> None:
        for versions, selected in (([1], 1), ([1, 2], 2), ([99, 2, 1], 2)):
            for shared in (False, True):
                with self.subTest(versions=versions, shared=shared):
                    profile = (
                        NodeProfile("node-service", "quiet-fox") if shared else None
                    )
                    self.service = PeerService(
                        self.identity,
                        project_id="project.test-1",
                        allowed_peers=frozenset({"a" * 64}),
                        capabilities=frozenset({"echo"}),
                        node_profile=profile,
                    )
                    session = _Session(_writer())
                    hello = {
                        "type": "hello",
                        "versions": versions,
                        "project": self.service.project_id,
                    }
                    messages = [hello, {"type": "identify", "node": None}]
                    with (
                        patch(
                            "aethermesh_core.network.service.read_frame",
                            new=AsyncMock(side_effect=messages),
                        ),
                        patch.object(self.service, "_send", new=AsyncMock()) as send,
                    ):
                        await self.service._negotiate(
                            asyncio.StreamReader(), session, "a" * 64
                        )
                    welcome = {
                        "type": "welcome",
                        "version": selected,
                        "project": self.service.project_id,
                        "capabilities": ["echo", "status"],
                    }
                    if selected == 2:
                        welcome["node"] = profile.to_dict() if profile else None
                        self.assertEqual(send.call_args.args[1], {"type": "ready"})
                    self.assertEqual(send.call_args_list[0].args[1], welcome)
                    self.assertEqual(send.await_count, selected)
                    peer = self.service.peers[-1]
                    self.assertEqual(peer.fingerprint, "a" * 64)
                    self.assertEqual(peer.protocol_version, selected)
                    self.assertIsNone(peer.profile)
                    self.assertFalse(peer.identity_pinned)
                    status = await self.service._operate(
                        "status", {}, protocol_version=selected
                    )
                    expected = {
                        "protocol_version": selected,
                        "capabilities": ["echo", "status"],
                    }
                    if selected == 2:
                        expected["node"] = profile.to_dict() if profile else None
                    self.assertEqual(status, expected)

    async def test_binding_rejects_absent_mismatched_or_legacy_identity(self) -> None:
        self.service = PeerService(
            self.identity,
            project_id="test",
            allowed_peers=frozenset({"a" * 64}),
            expected_peer_ids={"a" * 64: "expected-client"},
        )
        for version, profile in (
            (1, None),
            (2, None),
            (2, NodeProfile("other", "expected-client").to_dict()),
        ):
            with self.subTest(version=version, profile=profile):
                hello = {"type": "hello", "versions": [version], "project": "test"}
                writer = _writer()
                with (
                    patch(
                        "aethermesh_core.network.service.read_frame",
                        new=AsyncMock(
                            side_effect=[hello, {"type": "identify", "node": profile}]
                        ),
                    ),
                    patch.object(self.service, "_send", new=AsyncMock()) as send,
                    patch.object(self.service, "_receive", new=AsyncMock()) as receive,
                ):
                    await self.service._connection(
                        asyncio.StreamReader(), writer, "a" * 64
                    )
                self.assertEqual(send.await_count, 0 if version == 1 else 1)
                receive.assert_not_awaited()
                self.assertEqual(self.service.peers, ())
                writer.close.assert_called_once_with()

    async def test_matching_identity_is_bound_to_its_own_certificate(self) -> None:
        self.service = PeerService(
            self.identity,
            project_id="test",
            allowed_peers=frozenset({"a" * 64, "b" * 64}),
            expected_peer_ids={"a" * 64: "expected-client"},
        )
        profile = NodeProfile("expected-client", "same-name")
        for pin in ("a" * 64, "b" * 64, "a" * 64):
            session = _Session(_writer())
            with (
                patch(
                    "aethermesh_core.network.service.read_frame",
                    new=AsyncMock(
                        side_effect=[
                            {"type": "hello", "versions": [2, 1], "project": "test"},
                            {"type": "identify", "node": profile.to_dict()},
                        ]
                    ),
                ),
                patch.object(self.service, "_send", new=AsyncMock()),
            ):
                await self.service._negotiate(asyncio.StreamReader(), session, pin)
        self.assertEqual(len(self.service.peers), 3)
        self.assertEqual(
            [peer.fingerprint for peer in self.service.peers],
            ["a" * 64, "b" * 64, "a" * 64],
        )
        self.assertEqual([peer.profile for peer in self.service.peers], [profile] * 3)
        self.assertEqual(
            [peer.identity_pinned for peer in self.service.peers], [True, False, True]
        )
        server = Mock(spec=asyncio.Server)
        server.wait_closed = AsyncMock()
        await self.service._shutdown(server)
        self.assertEqual(self.service.peers, ())

    async def test_invalid_identify_never_admits_requests(self) -> None:
        hello = {
            "type": "hello",
            "versions": [2, 1],
            "project": self.service.project_id,
        }
        for identify in (
            {},
            {"type": "identify"},
            {"type": "identify", "node": None, "extra": True},
            {"type": "status", "node": None},
            {"type": "identify", "node": {}},
            {
                "type": "identify",
                "node": {
                    "node_id": "valid",
                    "node_name": None,
                    "hardware": None,
                    "secret": "hidden",
                },
            },
            _request(),
        ):
            with self.subTest(identify=identify):
                with (
                    patch(
                        "aethermesh_core.network.service.read_frame",
                        new=AsyncMock(side_effect=[hello, identify]),
                    ),
                    patch.object(self.service, "_send", new=AsyncMock()) as send,
                    patch.object(self.service, "_receive", new=AsyncMock()) as receive,
                ):
                    await self.service._connection(
                        asyncio.StreamReader(), _writer(), "a" * 64
                    )
                self.assertEqual(send.await_count, 1)
                receive.assert_not_awaited()
                self.assertEqual(self.service.peers, ())

    async def test_application_deadline_bounds_identify_writes_and_entire_exchange(
        self,
    ) -> None:
        self.service.limits = Limits(handshake_timeout=0.02, io_timeout=1)
        hello = {
            "type": "hello",
            "versions": [2, 1],
            "project": self.service.project_id,
        }
        identify = {"type": "identify", "node": None}
        for stalled in ("identify", "welcome", "ready", "cumulative"):
            reads = 0

            async def read(
                _reader: asyncio.StreamReader, stalled: str = stalled
            ) -> dict[str, Any]:
                nonlocal reads
                reads += 1
                if stalled == "identify" and reads == 2:
                    await asyncio.sleep(3600)
                if stalled == "cumulative":
                    await asyncio.sleep(0.012)
                return hello if reads == 1 else identify

            async def send(
                _session: _Session, message: dict[str, Any], stalled: str = stalled
            ) -> None:
                if message["type"] == stalled:
                    await asyncio.sleep(3600)
                if stalled == "cumulative":
                    await asyncio.sleep(0.012)

            with (
                self.subTest(stalled=stalled),
                patch("aethermesh_core.network.service.read_frame", side_effect=read),
                patch.object(self.service, "_send", side_effect=send),
                patch.object(self.service, "_receive", new=AsyncMock()) as receive,
            ):
                writer = _writer()
                await asyncio.wait_for(
                    self.service._connection(asyncio.StreamReader(), writer, "a" * 64),
                    1,
                )
                receive.assert_not_awaited()
                writer.close.assert_called_once_with()
                self.assertEqual(self.service.peers, ())
                if stalled == "cumulative":
                    self.assertEqual(reads, 1)


class PeerServiceTLSTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.pki = TestPKI.create(Path(cls.temporary.name))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    async def asyncSetUp(self) -> None:
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="tls-test",
            allowed_peers=frozenset(
                {certificate_fingerprint(self.pki.client.certificate)}
            ),
            capabilities=frozenset({"echo"}),
            limits=Limits(io_timeout=0.5),
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(
            *self.service.address,
            ssl=self.pki.client.identity().client_context(),
            server_hostname="localhost",
        )
        self.addAsyncCleanup(close_writer, writer)
        await write_frame(
            writer, {"type": "hello", "versions": [1], "project": "tls-test"}
        )
        welcome = await read_frame(reader)
        self.assertEqual(welcome["capabilities"], ["echo", "status"])
        self.assertEqual(welcome["version"], 1)
        return reader, writer

    async def test_real_tls_status_cancel_disconnect_and_restart(self) -> None:
        reader, writer = await self.connect()
        await write_frame(writer, _request())
        self.assertEqual((await read_frame(reader))["result"]["protocol_version"], 1)
        await write_frame(
            writer,
            _request(2, operation="echo", payload={"text": "secret", "delay_ms": 1000}),
        )
        await write_frame(writer, {"type": "cancel", "id": 2})
        self.assertEqual((await read_frame(reader))["code"], "cancelled")
        await write_frame(
            writer, _request(3, operation="echo", payload={"text": "hello"})
        )
        self.assertEqual((await read_frame(reader))["result"], {"text": "hello"})
        await write_frame(
            writer,
            _request(
                4, operation="echo", payload={"text": "pending", "delay_ms": 1000}
            ),
        )
        await asyncio.sleep(0.01)
        await self.service.close()
        self.assertFalse(self.service._connections)
        with self.assertRaises(ConnectionClosed):
            await read_frame(reader)
        await self.service.start()
        reader, writer = await self.connect()
        await write_frame(writer, _request())
        self.assertEqual((await read_frame(reader))["id"], 1)
        await write_frame(writer, _request())
        with self.assertRaises(ConnectionClosed):
            await read_frame(reader)

    async def test_cancelling_foreground_wait_closes_active_client(self) -> None:
        reader, _writer = await self.connect()
        serving = asyncio.create_task(self.service.serve_forever())
        await asyncio.sleep(0)
        serving.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(serving, 1)
        self.assertFalse(self.service._connections)
        with self.assertRaises(ConnectionClosed):
            await read_frame(reader)

    async def test_explicit_close_unblocks_foreground_wait(self) -> None:
        await self.connect()
        serving = asyncio.create_task(self.service.serve_forever())
        await asyncio.sleep(0)
        await asyncio.wait_for(self.service.close(), 1)
        await asyncio.wait_for(serving, 1)

    async def test_shutdown_bounds_a_stalled_pre_tls_connection(self) -> None:
        await self.service.close()
        self.service.limits = Limits(handshake_timeout=0.05, io_timeout=0.05)
        await self.service.start()
        # Suppress asyncio's expected handshake timeout diagnostic for this
        # deliberately stalled connection; no application session was created.
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, _context: None)
        self.addCleanup(loop.set_exception_handler, previous_handler)
        reader, writer = await asyncio.open_connection(*self.service.address)
        self.addAsyncCleanup(close_writer, writer)
        await asyncio.sleep(0.01)
        self.assertFalse(self.service._connections)
        await asyncio.wait_for(self.service.close(), 1)
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
        self.assertFalse(self.service._writers)

    async def test_real_tls_max_connections_rejects_excess(self) -> None:
        self.service.limits = Limits(max_connections=1)
        await self.connect()
        reader, writer = await asyncio.open_connection(
            *self.service.address,
            ssl=self.pki.client.identity().client_context(),
            server_hostname="localhost",
        )
        self.addAsyncCleanup(close_writer, writer)
        with self.assertRaises(ConnectionClosed):
            await read_frame(reader)
        self.assertEqual(len(self.service._connections), 1)

    async def test_real_tls_unapproved_client_pin_is_rejected(self) -> None:
        reader, writer = await asyncio.open_connection(
            *self.service.address,
            ssl=self.pki.unauthorized.identity().client_context(),
            server_hostname="localhost",
        )
        self.addAsyncCleanup(close_writer, writer)
        with self.assertRaises(ConnectionClosed):
            await read_frame(reader)
        self.assertFalse(self.service._connections)

    async def connect_profile(
        self, profile: NodeProfile | None, identity: TLSIdentity | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(
            *self.service.address,
            ssl=(identity or self.pki.client.identity()).client_context(),
            server_hostname="localhost",
        )
        self.addAsyncCleanup(close_writer, writer)
        await write_frame(
            writer, {"type": "hello", "versions": [2, 1], "project": "tls-test"}
        )
        welcome = await read_frame(reader)
        self.assertEqual(
            welcome,
            {
                "type": "welcome",
                "version": 2,
                "project": "tls-test",
                "capabilities": ["echo", "status"],
                "node": self.service.node_profile.to_dict()
                if self.service.node_profile
                else None,
            },
        )
        await write_frame(
            writer, {"type": "identify", "node": profile.to_dict() if profile else None}
        )
        self.assertEqual(await read_frame(reader), {"type": "ready"})
        return reader, writer

    async def test_real_tls_duplicate_claims_remain_distinct_authenticated_sessions(
        self,
    ) -> None:
        await self.service.close()
        first_pin = certificate_fingerprint(self.pki.client.certificate)
        second_pin = certificate_fingerprint(self.pki.unauthorized.certificate)
        server_profile = NodeProfile("service-node", "service-name")
        client_profile = NodeProfile("duplicate-id", "duplicate-name")
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="tls-test",
            allowed_peers=frozenset({first_pin, second_pin}),
            capabilities=frozenset({"echo"}),
            node_profile=server_profile,
            expected_peer_ids={first_pin: "duplicate-id"},
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        sessions = [
            await self.connect_profile(client_profile),
            await self.connect_profile(
                client_profile, self.pki.unauthorized.identity()
            ),
            await self.connect_profile(client_profile),
        ]
        self.assertEqual(len(self.service.peers), 3)
        self.assertEqual(
            [peer.fingerprint for peer in self.service.peers],
            [first_pin, second_pin, first_pin],
        )
        self.assertEqual(
            [peer.profile for peer in self.service.peers], [client_profile] * 3
        )
        self.assertEqual(
            [peer.identity_pinned for peer in self.service.peers], [True, False, True]
        )
        for reader, writer in sessions:
            await write_frame(writer, _request())
            self.assertEqual(
                await read_frame(reader),
                {
                    "type": "result",
                    "id": 1,
                    "result": {
                        "protocol_version": 2,
                        "capabilities": ["echo", "status"],
                        "node": server_profile.to_dict(),
                    },
                },
            )
        snapshot = self.service.peers
        await close_writer(sessions[0][1])
        async with asyncio.timeout(1):
            while len(self.service.peers) == 3:
                await asyncio.sleep(0)
        self.assertEqual(len(self.service.peers), 2)
        self.assertEqual(len(snapshot), 3)
        self.assertEqual(
            [peer.fingerprint for peer in self.service.peers], [second_pin, first_pin]
        )
        await self.service.close()
        self.assertEqual(self.service.peers, ())
        await self.service.start()
        await self.connect_profile(client_profile)
        self.assertEqual(len(self.service.peers), 1)

    async def test_real_tls_legacy_and_v2_sessions_keep_independent_status_shapes(
        self,
    ) -> None:
        await self.service.close()
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="tls-test",
            allowed_peers=frozenset(
                {certificate_fingerprint(self.pki.client.certificate)}
            ),
            capabilities=frozenset({"echo"}),
            node_profile=NodeProfile("service-id", "service-name"),
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        legacy = await self.connect()
        modern = await self.connect_profile(None)
        self.assertEqual([peer.protocol_version for peer in self.service.peers], [1, 2])
        for (reader, writer), version in ((legacy, 1), (modern, 2)):
            await write_frame(writer, _request())
            status = (await read_frame(reader))["result"]
            self.assertEqual(status["protocol_version"], version)
            self.assertEqual("node" in status, version == 2)
        await self.service.close()
        self.assertEqual(self.service.peers, ())

    async def test_real_tls_identify_is_required_before_status_or_registry_admission(
        self,
    ) -> None:
        self.service.limits = Limits(handshake_timeout=0.04, io_timeout=1)
        for send_request in (False, True):
            with self.subTest(send_request=send_request):
                reader, writer = await asyncio.open_connection(
                    *self.service.address,
                    ssl=self.pki.client.identity().client_context(),
                    server_hostname="localhost",
                )
                self.addAsyncCleanup(close_writer, writer)
                await write_frame(
                    writer, {"type": "hello", "versions": [2, 1], "project": "tls-test"}
                )
                self.assertEqual((await read_frame(reader))["version"], 2)
                self.assertEqual(self.service.peers, ())
                if send_request:
                    await write_frame(writer, _request())
                with self.assertRaises(ConnectionClosed):
                    await asyncio.wait_for(read_frame(reader), 1)
                self.assertEqual(self.service.peers, ())
