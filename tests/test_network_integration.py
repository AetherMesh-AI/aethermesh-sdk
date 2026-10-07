"""Real TLS acceptance checks across independent SDK/service processes."""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from aethermesh_core.network import (
    AuthenticationError,
    ConnectionClosed,
    PeerClient,
    PeerEndpoint,
    PeerError,
    RemoteError,
    RequestTimeout,
    TLSIdentity,
    certificate_fingerprint,
)
from aethermesh_core.network.protocol import close_writer, read_frame, write_frame
from tests.network_test_support import TestPKI

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "integration-project"


class NetworkProcessTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory(prefix="aethermesh-test-pki-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.pki = TestPKI.create(Path(cls.directory.name))
        cls.server_pin = certificate_fingerprint(cls.pki.server.certificate)
        cls.client_pin = certificate_fingerprint(cls.pki.client.certificate)

    async def asyncSetUp(self) -> None:
        self.processes: list[asyncio.subprocess.Process] = []
        self.addAsyncCleanup(self.stop_processes)
        await self.start_service()

    async def start_service(self, *, echo: bool = True, port: int = 0) -> None:
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(ROOT / "src")
        args = [
            sys.executable,
            "-m",
            "aethermesh_core.network.cli",
            "serve",
            "--certificate",
            str(self.pki.server.certificate),
            "--private-key",
            str(self.pki.server.private_key),
            "--trust-store",
            str(self.pki.ca),
            "--allow-peer",
            self.client_pin,
            "--project",
            PROJECT,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]
        if echo:
            args.append("--enable-echo")
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=ROOT,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.processes.append(process)
        self.process = process
        self.assertNotEqual(process.pid, os.getpid())
        assert process.stdout is not None
        line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not line:
            _, errors = await asyncio.wait_for(process.communicate(), timeout=5)
            self.fail(f"Peer service did not become ready: {errors.decode()}")
        readiness = json.loads(line)
        self.assertEqual(readiness["project"], PROJECT)
        self.assertEqual(readiness["host"], "127.0.0.1")
        self.assertEqual(
            readiness["capabilities"], ["echo", "status"] if echo else ["status"]
        )
        self.endpoint = PeerEndpoint(
            readiness["host"], readiness["port"], "localhost", self.server_pin
        )

    async def stop_processes(self) -> None:
        for process in reversed(self.processes):
            await self.stop_process(process)

    async def stop_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is None:
            process.terminate()
        try:
            await asyncio.wait_for(process.communicate(), timeout=5)
        except TimeoutError:
            process.kill()
            await asyncio.wait_for(process.communicate(), timeout=5)
            self.fail("Peer service did not terminate within five seconds")

    def client(
        self, identity: TLSIdentity | None = None, *, project: str = PROJECT
    ) -> PeerClient:
        client = PeerClient(identity or self.pki.client.identity(), project_id=project)
        self.addAsyncCleanup(client.close)
        return client

    async def raw_peer(
        self, *, hello: dict[str, Any] | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                self.endpoint.host,
                self.endpoint.port,
                ssl=self.pki.client.identity().client_context(),
                server_hostname=self.endpoint.server_name,
                ssl_handshake_timeout=3,
            ),
            timeout=5,
        )
        self.addAsyncCleanup(close_writer, writer)
        self.assertEqual(writer.get_extra_info("ssl_object").version(), "TLSv1.3")
        await write_frame(
            writer,
            hello or {"type": "hello", "versions": [1], "project": PROJECT},
        )
        return reader, writer

    async def negotiated_peer(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await self.raw_peer()
        self.assertEqual(
            await asyncio.wait_for(read_frame(reader), timeout=3),
            {
                "type": "welcome",
                "version": 1,
                "project": PROJECT,
                "capabilities": ["echo", "status"],
            },
        )
        return reader, writer

    async def assert_closed(self, reader: asyncio.StreamReader) -> None:
        try:
            self.assertEqual(await asyncio.wait_for(reader.read(1), timeout=3), b"")
        except ConnectionResetError:
            # A TLS transport may report the peer's immediate abort as a reset.
            pass

    async def test_separate_process_status_echo_and_unsupported_operation(self) -> None:
        client = self.client()
        await client.connect(self.endpoint)
        self.assertTrue(client.connected)
        self.assertEqual(client.capabilities, ("echo", "status"))
        status = await client.request("status")
        self.assertEqual(status["protocol_version"], 1)
        self.assertEqual(status["capabilities"], ["echo", "status"])
        self.assertEqual(
            await client.request("echo", {"text": "hello, independent peer 🌐"}),
            {"text": "hello, independent peer 🌐"},
        )
        with self.assertRaises(RemoteError) as caught:
            await client.request("inference", {"prompt": "not implemented"})
        self.assertEqual(caught.exception.code, "unsupported_operation")
        self.assertTrue(client.connected)
        await client.close()
        self.assertFalse(client.connected)

    async def test_echo_is_explicit_opt_in(self) -> None:
        await self.stop_process(self.process)
        await self.start_service(echo=False)
        client = self.client()
        await client.connect(self.endpoint)
        self.assertEqual(client.capabilities, ("status",))
        with self.assertRaises(RemoteError) as caught:
            await client.request("echo", {"text": "disabled"})
        self.assertEqual(caught.exception.code, "unsupported_operation")

    async def test_timeout_and_cancel_leave_session_usable(self) -> None:
        client = self.client()
        await client.connect(self.endpoint)
        with self.assertRaises(RequestTimeout):
            await client.request(
                "echo", {"text": "late", "delay_ms": 1000}, timeout=0.03
            )
        pending = asyncio.create_task(
            client.request("echo", {"text": "cancel me", "delay_ms": 1000})
        )
        await asyncio.sleep(0.03)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertEqual(
            await client.request("echo", {"text": "still connected"}),
            {"text": "still connected"},
        )
        self.assertTrue(client.connected)

    async def test_disconnect_and_explicit_reconnect_after_process_restart(
        self,
    ) -> None:
        client = self.client()
        endpoint = self.endpoint
        await client.connect(endpoint)
        pending = asyncio.create_task(
            client.request("echo", {"text": "interrupted", "delay_ms": 1000})
        )
        await asyncio.sleep(0.03)
        await self.stop_process(self.process)
        with self.assertRaises(ConnectionClosed):
            await pending
        self.assertFalse(client.connected)
        await self.start_service(port=endpoint.port)
        self.assertEqual(self.endpoint, endpoint)
        await client.connect(endpoint)
        self.assertEqual(
            await client.request("echo", {"text": "reconnected"}),
            {"text": "reconnected"},
        )

    async def test_server_certificate_pin_mismatch(self) -> None:
        endpoint = PeerEndpoint(
            self.endpoint.host, self.endpoint.port, "localhost", self.client_pin
        )
        client = self.client()
        with self.assertRaises(AuthenticationError):
            await client.connect(endpoint)
        self.assertFalse(client.connected)

    async def test_server_hostname_mismatch(self) -> None:
        endpoint = PeerEndpoint(
            self.endpoint.host, self.endpoint.port, "wrong.invalid", self.server_pin
        )
        with self.assertRaises(AuthenticationError):
            await self.client().connect(endpoint)

    async def test_client_without_certificate_is_rejected(self) -> None:
        context = ssl.create_default_context(cafile=str(self.pki.ca))
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.endpoint.host,
                    self.endpoint.port,
                    ssl=context,
                    server_hostname="localhost",
                    ssl_handshake_timeout=3,
                ),
                timeout=5,
            )
        except (ssl.SSLError, ConnectionResetError):
            return
        self.addAsyncCleanup(close_writer, writer)
        # TLS 1.3 can deliver the server's missing-client-cert alert after the
        # local handshake call returns. Either way no protocol welcome is sent.
        try:
            await write_frame(
                writer, {"type": "hello", "versions": [1], "project": PROJECT}
            )
            await self.assert_closed(reader)
        except (ConnectionClosed, ssl.SSLError):
            pass

    async def test_ca_signed_but_unallowlisted_client_is_rejected(self) -> None:
        client = self.client(self.pki.unauthorized.identity())
        with self.assertRaises(PeerError):
            await client.connect(self.endpoint)
        self.assertFalse(client.connected)

    async def test_untrusted_server_and_client_chains_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="aethermesh-foreign-pki-") as directory:
            foreign = await asyncio.to_thread(TestPKI.create, Path(directory))
            wrong_trust = TLSIdentity(
                self.pki.client.certificate, self.pki.client.private_key, foreign.ca
            )
            with self.assertRaises(AuthenticationError):
                await self.client(wrong_trust).connect(self.endpoint)
            wrong_client = TLSIdentity(
                foreign.client.certificate, foreign.client.private_key, self.pki.ca
            )
            with self.assertRaises(PeerError):
                await self.client(wrong_client).connect(self.endpoint)

    async def test_project_and_version_mismatch_close_authenticated_session(
        self,
    ) -> None:
        for hello in (
            {"type": "hello", "versions": [1], "project": "different-project"},
            {"type": "hello", "versions": [2], "project": PROJECT},
        ):
            with self.subTest(hello=hello):
                reader, _ = await self.raw_peer(hello=hello)
                await self.assert_closed(reader)
        with self.assertRaises(PeerError):
            await self.client(project="different-project").connect(self.endpoint)

    async def test_duplicate_request_ids_close_authenticated_session(self) -> None:
        for delay in (0, 1000):
            with self.subTest(delay=delay):
                reader, writer = await self.negotiated_peer()
                request = {
                    "type": "request",
                    "id": 1,
                    "operation": "echo",
                    "payload": {"text": "first", "delay_ms": delay},
                }
                await write_frame(writer, request)
                if delay == 0:
                    self.assertEqual(
                        (await asyncio.wait_for(read_frame(reader), timeout=3))["type"],
                        "result",
                    )
                await write_frame(writer, request)
                await self.assert_closed(reader)

    async def test_invalid_frame_lengths_close_without_reading_body(self) -> None:
        for size in (0, 65537, 2**32 - 1):
            with self.subTest(size=size):
                reader, writer = await self.negotiated_peer()
                writer.write(struct.pack("!I", size))
                await writer.drain()
                await self.assert_closed(reader)

    async def test_malformed_json_and_utf8_close_authenticated_session(self) -> None:
        for payload in (
            b'{"type":"cancel","id":1,"id":2}',
            b'{"type":"request","id":NaN}',
            b"[]",
            b"{broken",
            b"\xff",
        ):
            with self.subTest(payload=payload):
                reader, writer = await self.negotiated_peer()
                writer.write(struct.pack("!I", len(payload)) + payload)
                await writer.drain()
                await self.assert_closed(reader)

    async def test_wire_cancellation_acknowledgement(self) -> None:
        reader, writer = await self.negotiated_peer()
        await write_frame(
            writer,
            {
                "type": "request",
                "id": 1,
                "operation": "echo",
                "payload": {"text": "cancelled", "delay_ms": 1000},
            },
        )
        await write_frame(writer, {"type": "cancel", "id": 1})
        response = await asyncio.wait_for(read_frame(reader), timeout=3)
        self.assertEqual(response["type"], "error")
        self.assertEqual(response["id"], 1)
        self.assertEqual(response["code"], "cancelled")
        await write_frame(
            writer,
            {"type": "request", "id": 2, "operation": "status", "payload": {}},
        )
        response = await asyncio.wait_for(read_frame(reader), timeout=3)
        self.assertEqual(response["type"], "result")
        self.assertEqual(response["id"], 2)

    async def test_inflight_limit_returns_busy_without_overloading_peer(self) -> None:
        reader, writer = await self.negotiated_peer()
        for request_id in range(1, 10):
            await write_frame(
                writer,
                {
                    "type": "request",
                    "id": request_id,
                    "operation": "echo",
                    "payload": {"text": "bounded", "delay_ms": 1000},
                },
            )
        response = await asyncio.wait_for(read_frame(reader), timeout=3)
        self.assertEqual(response["type"], "error")
        self.assertEqual(response["id"], 9)
        self.assertEqual(response["code"], "busy")
