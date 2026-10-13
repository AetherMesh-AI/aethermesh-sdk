"""Explicit connection fallback preserves trust and never replays application work."""

import asyncio
import socket
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from aethermesh_core.network import (
    AuthenticationError,
    ConnectionClosed,
    EndpointTimeout,
    EndpointUnavailable,
    Limits,
    PeerClient,
    PeerEndpoint,
    PeerService,
    ProtocolError,
    RequestTimeout,
    TLSIdentity,
    candidates,
    certificate_fingerprint,
    connect_peer_candidates,
)
from tests.network_test_support import TestPKI

IDENTITY = TLSIdentity(Path("certificate"), Path("key"), Path("trust"))
ENDPOINTS = (
    PeerEndpoint("127.0.0.1", 12345, "localhost", "a" * 64),
    PeerEndpoint("127.0.0.1", 12346, "localhost", "a" * 64),
)


class CandidateContractTests(unittest.IsolatedAsyncioTestCase):
    async def call(self, **kwargs):
        return await connect_peer_candidates(
            kwargs.pop("identity", IDENTITY),
            kwargs.pop("endpoints", ENDPOINTS),
            **({"project_id": "test", "timeout": 1} | kwargs),
        )

    async def test_validation_precedes_client_creation(self):
        invalid = [
            {"identity": None},
            {"limits": object()},
            *(
                {"timeout": value}
                for value in (True, 0, -1, float("nan"), float("inf"), 3601, "1")
            ),
            *(
                {"endpoints": value}
                for value in (
                    (),
                    list(ENDPOINTS),
                    (None,),
                    ENDPOINTS * 5,
                    (ENDPOINTS[0],) * 2,
                )
            ),
            {"endpoints": (ENDPOINTS[0], replace(ENDPOINTS[1], fingerprint="b" * 64))},
            {
                "endpoints": (
                    ENDPOINTS[0],
                    replace(ENDPOINTS[1], expected_node_id="b" * 64),
                )
            },
        ]
        with patch.object(candidates, "PeerClient") as create:
            for kwargs in invalid:
                with (
                    self.subTest(kwargs=kwargs),
                    self.assertRaises((ValueError, TypeError)),
                ):
                    await self.call(**kwargs)
        create.assert_not_called()
        with self.assertRaises(ValueError):
            await self.call(project_id="invalid project")

    async def test_transport_fallback_hands_off_only_successful_session(self):
        for failure in (
            EndpointUnavailable("closed"),
            EndpointTimeout("attempt timeout"),
        ):
            with self.subTest(failure=type(failure).__name__):
                failed = MagicMock(
                    connect=AsyncMock(side_effect=failure), close=AsyncMock()
                )
                good = MagicMock(connect=AsyncMock(), close=AsyncMock())
                with patch.object(
                    candidates, "PeerClient", side_effect=(failed, good)
                ) as create:
                    connected = await self.call()
                self.assertIs(connected.client, good)
                self.assertEqual(connected.endpoint, ENDPOINTS[1])
                self.assertEqual(connected.attempted_count, 2)
                failed.close.assert_awaited_once()
                good.close.assert_not_called()
                self.assertEqual(create.call_count, 2)
                self.assertTrue(
                    all(
                        call.kwargs["project_id"] == "test"
                        for call in create.call_args_list
                    )
                )
                async with connected as same:
                    self.assertIs(same, connected)
                good.close.assert_awaited_once()
                good.request.assert_not_called()

    async def test_security_protocol_and_configuration_failures_are_terminal(self):
        for failure in (
            AuthenticationError("bad pin"),
            ProtocolError("wrong project"),
            ConnectionClosed("closed during negotiation"),
            RequestTimeout("negotiation timeout"),
            ValueError("config"),
            RuntimeError("unexpected"),
        ):
            with self.subTest(failure=type(failure).__name__):
                client = MagicMock(
                    connect=AsyncMock(side_effect=failure), close=AsyncMock()
                )
                with (
                    patch.object(
                        candidates, "PeerClient", return_value=client
                    ) as create,
                    self.assertRaises(type(failure)),
                ):
                    await self.call()
                create.assert_called_once()
                client.close.assert_awaited_once()

    async def test_exhaustion_is_bounded_and_static(self):
        clients = [
            MagicMock(
                connect=AsyncMock(
                    side_effect=EndpointUnavailable("private endpoint detail")
                ),
                close=AsyncMock(),
            )
            for _ in ENDPOINTS
        ]
        with (
            patch.object(candidates, "PeerClient", side_effect=clients) as create,
            self.assertRaises(ConnectionClosed) as caught,
        ):
            await self.call()
        self.assertNotIn("private", str(caught.exception))
        self.assertEqual(create.call_count, 2)
        for client in clients:
            client.close.assert_awaited_once()

    async def test_one_total_deadline_stops_stalled_attempt(self):
        started = asyncio.Event()

        async def stall(endpoint):
            started.set()
            await asyncio.Event().wait()

        client = MagicMock(connect=AsyncMock(side_effect=stall), close=AsyncMock())
        with (
            patch.object(candidates, "PeerClient", return_value=client) as create,
            self.assertRaises(RequestTimeout),
        ):
            await self.call(timeout=0.02)
        self.assertTrue(started.is_set())
        create.assert_called_once()
        client.close.assert_awaited_once()

    async def test_cancellation_propagates_and_closes_owned_client(self):
        started = asyncio.Event()

        async def stall(endpoint):
            started.set()
            await asyncio.Event().wait()

        client = MagicMock(connect=AsyncMock(side_effect=stall), close=AsyncMock())
        with patch.object(candidates, "PeerClient", return_value=client) as create:
            task = asyncio.create_task(self.call())
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        create.assert_called_once()
        client.close.assert_awaited_once()

    async def test_expired_before_attempt_or_after_connect_never_hands_off(self):
        now = asyncio.get_running_loop().time()
        for clock, attempts in (((now, now + 2), 0), ((now, now, now + 2, now + 2), 1)):
            with self.subTest(attempts=attempts):
                fake_loop = MagicMock(time=MagicMock(side_effect=clock))
                client = MagicMock(connect=AsyncMock(), close=AsyncMock())
                with (
                    patch.object(
                        candidates.asyncio, "get_running_loop", return_value=fake_loop
                    ),
                    patch.object(
                        candidates, "PeerClient", return_value=client
                    ) as create,
                    self.assertRaises(RequestTimeout),
                ):
                    await self.call()
                self.assertEqual(create.call_count, attempts)
                self.assertEqual(client.close.await_count, attempts)

    async def test_deadline_is_not_reset_between_attempts(self):
        now = asyncio.get_running_loop().time()
        fake_loop = MagicMock(
            time=MagicMock(side_effect=(now, now, now + 0.5, now + 1.1))
        )
        failed = MagicMock(
            connect=AsyncMock(side_effect=EndpointUnavailable("closed")),
            close=AsyncMock(),
        )
        with (
            patch.object(
                candidates.asyncio, "get_running_loop", return_value=fake_loop
            ),
            patch.object(candidates, "PeerClient", return_value=failed) as create,
            self.assertRaises(RequestTimeout),
        ):
            await self.call()
        create.assert_called_once()
        failed.close.assert_awaited_once()

    async def test_deadline_exhausted_by_failed_attempt_is_terminal(self):
        now = asyncio.get_running_loop().time()
        clock = MagicMock(time=MagicMock(side_effect=(now, now, now + 2)))
        client = MagicMock(
            connect=AsyncMock(side_effect=EndpointTimeout("attempt")), close=AsyncMock()
        )
        with (
            patch.object(candidates.asyncio, "get_running_loop", return_value=clock),
            patch.object(candidates, "PeerClient", return_value=client) as create,
            self.assertRaises(RequestTimeout),
        ):
            await self.call()
        create.assert_called_once()
        client.close.assert_awaited_once()

    async def test_cancellation_during_receiver_cleanup_closes_transport(self):
        from tests.test_network_client import writer_mock

        client = PeerClient(IDENTITY, project_id="test")
        writer = writer_mock()
        cancelling = asyncio.Event()

        async def receiver():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelling.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(receiver())
        await asyncio.sleep(0)
        client._writer, client._receiver = writer, task
        closing = asyncio.create_task(client.close())
        await asyncio.wait_for(cancelling.wait(), 1)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertTrue(task.done())
        writer.close.assert_called_once()
        self.assertFalse(client.connected)


class CandidateLoopbackTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.pki = TestPKI.create(Path(cls.temp.name))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    async def asyncSetUp(self):
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="candidates",
            allowed_peers=frozenset(
                {certificate_fingerprint(self.pki.client.certificate)}
            ),
            capabilities=frozenset({"echo"}),
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        self.endpoint = PeerEndpoint(
            *self.service.address,
            "localhost",
            certificate_fingerprint(self.pki.server.certificate),
        )

    async def test_closed_address_then_real_tls_and_explicit_single_request(self):
        reserved = socket.socket()
        self.addCleanup(reserved.close)
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
        result = await connect_peer_candidates(
            self.pki.client.identity(),
            (replace(self.endpoint, port=port), self.endpoint),
            project_id="candidates",
            timeout=3,
            limits=Limits(handshake_timeout=0.5),
        )
        async with result:
            self.assertEqual(result.attempted_count, 2)
            self.assertEqual(
                await result.client.request("echo", {"text": "once"}, timeout=1),
                {"text": "once"},
            )
        self.assertFalse(result.client.connected)
        self.assertIsNone(result.client._receiver)

    async def test_real_identity_and_project_errors_never_try_second_address(self):
        for project, pin, error in (
            ("candidates", "0" * 64, AuthenticationError),
            ("wrong", self.endpoint.fingerprint, ConnectionClosed),
        ):
            with self.subTest(project=project):
                original = PeerClient.connect
                attempts = []

                async def connect(
                    client, endpoint, attempts=attempts, original=original
                ):
                    attempts.append(endpoint)
                    return await original(client, endpoint)

                first = replace(self.endpoint, fingerprint=pin)
                with (
                    patch.object(PeerClient, "connect", connect),
                    self.assertRaises(error),
                ):
                    await connect_peer_candidates(
                        self.pki.client.identity(),
                        (first, replace(first, host="localhost")),
                        project_id=project,
                        timeout=3,
                    )
                self.assertEqual(attempts, [first])

    async def stalled_endpoint(self, *, tls):
        from aethermesh_core.network.protocol import close_writer, read_frame

        entered = asyncio.Event()
        tasks = set()

        async def handler(reader, writer):
            task = asyncio.current_task()
            tasks.add(task)
            try:
                if tls:
                    await read_frame(reader)
                entered.set()
                await reader.read()
            finally:
                await close_writer(writer)
                tasks.discard(task)

        server = await asyncio.start_server(
            handler,
            "127.0.0.1",
            0,
            ssl=self.pki.server.identity().server_context() if tls else None,
        )

        async def cleanup():
            server.close()
            await server.wait_closed()
            pending = tuple(tasks)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        self.addAsyncCleanup(cleanup)
        return replace(self.endpoint, port=server.sockets[0].getsockname()[1]), entered

    async def test_real_transport_timeout_can_fall_back_but_negotiation_timeout_cannot(
        self,
    ):
        raw, started = await self.stalled_endpoint(tls=False)
        result = await connect_peer_candidates(
            self.pki.client.identity(),
            (raw, self.endpoint),
            project_id="candidates",
            timeout=3,
            limits=Limits(handshake_timeout=0.5),
        )
        self.assertTrue(started.is_set())
        self.assertEqual(result.attempted_count, 2)
        await result.close()
        negotiated, started = await self.stalled_endpoint(tls=True)
        original = PeerClient.connect
        attempts = []

        async def connect(client, endpoint):
            attempts.append(endpoint)
            return await original(client, endpoint)

        with (
            patch.object(PeerClient, "connect", connect),
            self.assertRaises(RequestTimeout) as caught,
        ):
            await connect_peer_candidates(
                self.pki.client.identity(),
                (negotiated, self.endpoint),
                project_id="candidates",
                timeout=3,
                limits=Limits(handshake_timeout=0.5),
            )
        self.assertNotIsInstance(caught.exception, EndpointTimeout)
        self.assertTrue(started.is_set())
        self.assertEqual(attempts, [negotiated])

    async def test_real_cancellation_stops_without_fallback(self):
        endpoint, started = await self.stalled_endpoint(tls=True)
        original = PeerClient.connect
        clients = []

        async def connect(client, selected):
            clients.append(client)
            return await original(client, selected)

        with patch.object(PeerClient, "connect", connect):
            task = asyncio.create_task(
                connect_peer_candidates(
                    self.pki.client.identity(),
                    (endpoint, self.endpoint),
                    project_id="candidates",
                    timeout=3,
                )
            )
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(clients), 1)
        self.assertFalse(clients[0].connected)
        self.assertIsNone(clients[0]._receiver)
