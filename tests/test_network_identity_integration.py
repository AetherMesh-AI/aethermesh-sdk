"""Identity/privacy acceptance evidence over real TLS and independent processes."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.identity import HardwareIdentityInputs, load_or_create_identity
from aethermesh_core.network import (
    AuthenticationError,
    ConnectionClosed,
    NodeProfile,
    PeerClient,
    PeerEndpoint,
    PeerError,
    PeerService,
    ProtocolError,
    TLSIdentity,
    certificate_fingerprint,
    load_node_profile,
)
from aethermesh_core.network.protocol import close_writer, read_frame, write_frame
from tests.network_test_support import TestPKI

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "identity-integration"
HARDWARE = HardwareIdentityInputs(
    cpu_architecture="arm64",
    cpu_vendor="private-cpu-vendor",
    cpu_brand_or_chip_name="private-chip-model",
    physical_core_count=12,
    logical_thread_count=24,
    permanent_mac_addresses=("aa:bb:cc:dd:ee:ff",),
    gpu_vendor="private-gpu-vendor",
    gpu_model_or_chip_name="private-gpu-model",
    gpu_device_id_if_available="private-device-id",
    gpu_count=1,
    max_gpu_vram_gb=24,
    total_installed_ram_gb=96,
)


class IdentityNetworkTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory(prefix="aethermesh-identity-pki-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.pki = TestPKI.create(Path(cls.directory.name))
        cls.server_pin = certificate_fingerprint(cls.pki.server.certificate)
        cls.client_pin = certificate_fingerprint(cls.pki.client.certificate)

    async def asyncSetUp(self) -> None:
        directory = tempfile.TemporaryDirectory(prefix="aethermesh-saved-identity-")
        self.addCleanup(directory.cleanup)
        self.home = Path(directory.name)
        self.processes: list[asyncio.subprocess.Process] = []
        self.addAsyncCleanup(self.stop_processes)

    def saved_profile(self, name: str = "identity.json") -> tuple[Path, NodeProfile]:
        path = self.home / name
        identity = load_or_create_identity(path, hardware_inputs=HARDWARE)
        profile = load_node_profile(path)
        self.assertEqual(profile.node_id, identity.node_id)
        self.assertEqual(profile.node_name, identity.node_name)
        self.assertIsNone(profile.hardware)
        return path, profile

    def client(
        self,
        profile: NodeProfile | None = None,
        *,
        identity: TLSIdentity | None = None,
    ) -> PeerClient:
        client = PeerClient(
            identity or self.pki.client.identity(),
            project_id=PROJECT,
            node_profile=profile,
        )
        self.addAsyncCleanup(client.close)
        return client

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
            self.fail("Identity service did not terminate within five seconds")

    def environment(self) -> dict[str, str]:
        return dict(os.environ, PYTHONPATH=str(ROOT / "src"))

    async def process_service(
        self,
        path: Path,
        *,
        expected_client: str | None = None,
    ) -> tuple[asyncio.subprocess.Process, PeerEndpoint]:
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
            "--node-identity",
            str(path),
            "--enable-echo",
        ]
        if expected_client is not None:
            args.extend(["--expect-peer-id", f"{self.client_pin}={expected_client}"])
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=ROOT,
            env=self.environment(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.processes.append(process)
        self.assertNotEqual(process.pid, os.getpid())
        assert process.stdout is not None
        readiness = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not readiness:
            _, errors = await asyncio.wait_for(process.communicate(), timeout=5)
            self.fail(f"Identity service did not become ready: {errors.decode()}")
        record = json.loads(readiness)
        return process, PeerEndpoint(
            record["host"], record["port"], "localhost", self.server_pin
        )

    async def local_service(
        self,
        profile: NodeProfile | None,
        *,
        expected_client: str | None = None,
    ) -> PeerEndpoint:
        service = PeerService(
            self.pki.server.identity(),
            project_id=PROJECT,
            allowed_peers=frozenset({self.client_pin}),
            node_profile=profile,
            expected_peer_ids=(
                {self.client_pin: expected_client}
                if expected_client is not None
                else None
            ),
        )
        self.addAsyncCleanup(service.close)
        await service.start()
        return PeerEndpoint(*service.address, "localhost", self.server_pin)

    async def test_saved_identity_and_name_survive_service_process_restarts(
        self,
    ) -> None:
        path, profile = self.saved_profile()
        before = path.read_bytes()
        modified = path.stat().st_mtime_ns
        for _ in range(2):
            process, endpoint = await self.process_service(
                path, expected_client=profile.node_id
            )
            client = self.client(profile)
            await client.connect(replace(endpoint, expected_node_id=profile.node_id))
            info = client.peer_info
            assert info is not None
            self.assertEqual(info.fingerprint, self.server_pin)
            self.assertEqual(info.profile, profile)
            self.assertTrue(info.identity_pinned)
            self.assertEqual(client.protocol_version, 2)
            self.assertEqual(
                (await client.request("status"))["node"], profile.to_dict()
            )
            self.assertEqual(
                await client.request("echo", {"text": "same saved identity"}),
                {"text": "same saved identity"},
            )
            await client.close()
            self.assertIsNone(client.peer_info)
            self.assertIsNone(client.protocol_version)
            await self.stop_process(process)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(path.stat().st_mtime_ns, modified)

    async def test_legacy_nameless_file_is_not_regenerated_or_rewritten(self) -> None:
        path, original = self.saved_profile()
        document = json.loads(path.read_text())
        document["node"].pop("node_name")
        path.write_text(json.dumps(document), encoding="utf-8")
        before = path.read_bytes()
        process, endpoint = await self.process_service(path)
        client = self.client()
        await client.connect(endpoint)
        info = client.peer_info
        assert info is not None and info.profile is not None
        self.assertEqual(info.profile.node_id, original.node_id)
        self.assertIsNone(info.profile.node_name)
        self.assertFalse(info.identity_pinned)
        self.assertEqual(
            (await client.request("status"))["node"], info.profile.to_dict()
        )
        await client.close()
        await self.stop_process(process)
        self.assertEqual(path.read_bytes(), before)

    async def test_expected_claims_do_not_replace_certificate_authentication(
        self,
    ) -> None:
        path, profile = self.saved_profile()
        _, endpoint = await self.process_service(path, expected_client=profile.node_id)
        bound = replace(endpoint, expected_node_id=profile.node_id)
        # A CA-signed but unallowlisted certificate cannot inherit trust by
        # claiming the authorized node's exact ID and display name.
        spoof = self.client(profile, identity=self.pki.unauthorized.identity())
        with self.assertRaises(PeerError):
            await spoof.connect(bound)
        self.assertIsNone(spoof.peer_info)
        for claim in (None, NodeProfile("different-id", profile.node_name)):
            client = self.client(claim)
            with self.subTest(claim=claim), self.assertRaises(PeerError):
                await client.connect(bound)
            self.assertFalse(client.connected)
            self.assertIsNone(client.peer_info)
        for bad_endpoint in (
            replace(bound, expected_node_id="different-server-id"),
            replace(bound, fingerprint=self.client_pin),
        ):
            client = self.client(profile)
            with (
                self.subTest(endpoint=bad_endpoint),
                self.assertRaises(AuthenticationError),
            ):
                await client.connect(bad_endpoint)
            self.assertFalse(client.connected)
        good = self.client(profile)
        await good.connect(bound)
        self.assertEqual((await good.request("status"))["node"], profile.to_dict())

    async def test_expected_server_id_rejects_missing_profile(self) -> None:
        endpoint = await self.local_service(None)
        client = self.client()
        with self.assertRaises(AuthenticationError):
            await client.connect(replace(endpoint, expected_node_id="required-id"))
        self.assertIsNone(client.peer_info)

    async def test_opt_in_demo_runs_in_an_independent_client_process(self) -> None:
        path, profile = self.saved_profile()
        _, endpoint = await self.process_service(path, expected_client=profile.node_id)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(ROOT / "examples" / "network_client.py"),
            "--certificate",
            str(self.pki.client.certificate),
            "--private-key",
            str(self.pki.client.private_key),
            "--trust-store",
            str(self.pki.ca),
            "--peer-fingerprint",
            self.server_pin,
            "--project",
            PROJECT,
            "--port",
            str(endpoint.port),
            "--node-identity",
            str(path),
            "--expected-node-id",
            profile.node_id,
            "--text",
            "independent identity client",
            cwd=self.home,
            env=self.environment(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.processes.append(process)
        output, errors = await asyncio.wait_for(process.communicate(), timeout=10)
        self.assertEqual(process.returncode, 0, errors.decode())
        records = [json.loads(line) for line in output.splitlines()]
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["peer_info"]["profile"], profile.to_dict())
        self.assertEqual(records[0]["peer_info"]["fingerprint"], self.server_pin)
        self.assertTrue(records[0]["peer_info"]["identity_pinned"])
        self.assertEqual(records[1]["node"], profile.to_dict())
        self.assertEqual(records[2], {"text": "independent identity client"})

    async def test_demo_identity_errors_do_not_disclose_paths_or_file_contents(
        self,
    ) -> None:
        for contents in (None, '{"private-file-content": invalid-json}'):
            path = self.home / "private-identity-basename.json"
            if contents is not None:
                path.write_text(contents, encoding="utf-8")
            with self.subTest(contents=contents):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(ROOT / "examples" / "network_client.py"),
                    "--certificate",
                    str(self.pki.client.certificate),
                    "--private-key",
                    str(self.pki.client.private_key),
                    "--trust-store",
                    str(self.pki.ca),
                    "--peer-fingerprint",
                    self.server_pin,
                    "--project",
                    PROJECT,
                    "--port",
                    "1",
                    "--node-identity",
                    str(path),
                    cwd=self.home,
                    env=self.environment(),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                self.processes.append(process)
                output, errors = await asyncio.wait_for(
                    process.communicate(), timeout=10
                )
                self.assertEqual(process.returncode, 1)
                self.assertEqual(output, b"")
                message = errors.decode()
                self.assertIn("Peer request failed", message)
                self.assertIn("verify", message)
                self.assertIn("node identity", message)
                for forbidden in (
                    str(path),
                    path.name,
                    str(self.home),
                    "private-file-content",
                    "invalid-json",
                ):
                    self.assertNotIn(forbidden, message)
                if contents is None:
                    self.assertFalse(path.exists())
                else:
                    self.assertEqual(path.read_text(encoding="utf-8"), contents)

    async def test_wire_frames_contain_only_opted_in_coarse_metadata(self) -> None:
        path, saved = self.saved_profile()
        document = json.loads(path.read_text())
        document["local_metadata"] = {"note": "private-local-metadata"}
        path.write_text(json.dumps(document), encoding="utf-8")
        before = path.read_bytes()
        with patch(
            "aethermesh_core.identity.collect_hardware_identity_inputs",
            return_value=HARDWARE,
        ) as probe:
            private = load_node_profile(path)
            probe.assert_not_called()
            shared = load_node_profile(path, include_hardware=True)
            probe.assert_called_once()
        self.assertEqual(shared.node_id, saved.node_id)
        self.assertEqual(path.read_bytes(), before)
        self.assertIsNone(private.hardware)
        frames: list[dict[str, Any]] = []

        async def record(writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
            frames.append(json.loads(json.dumps(message)))
            await write_frame(writer, message)

        # Capture framed message objects without replacing the TLS transport.
        # Each side still runs the production serializer, TLS and session logic.
        with (
            patch("aethermesh_core.network.client.write_frame", side_effect=record),
            patch("aethermesh_core.network.service.write_frame", side_effect=record),
        ):
            endpoint = await self.local_service(shared)
            client = self.client(shared)
            await client.connect(endpoint)
            status = await client.request("status")
            await client.close()
        self.assertEqual(status["node"], shared.to_dict())
        kinds = [frame["type"] for frame in frames]
        self.assertEqual(
            kinds, ["hello", "welcome", "identify", "ready", "request", "result"]
        )
        self.assertEqual(set(frames[0]), {"type", "versions", "project"})
        self.assertEqual(frames[0]["versions"], [2, 1])
        for value in (frames[1]["node"], frames[2]["node"], status["node"]):
            self.assertEqual(set(value), {"node_id", "node_name", "hardware"})
            self.assertEqual(
                value["hardware"],
                {
                    "cpu_architecture": "aarch64",
                    "ram_gb_bucket": "64to127",
                    "gpu_available": True,
                    "gpu_vram_gb_bucket": "16to31",
                },
            )
        serialized = json.dumps(frames)
        for forbidden in (
            "permanent_mac_addresses",
            "aa:bb:cc:dd:ee:ff",
            "private-cpu-vendor",
            "private-chip-model",
            "private-gpu-vendor",
            "private-gpu-model",
            "private-device-id",
            "private-local-metadata",
            "local_metadata",
            "private_key",
            "BEGIN PRIVATE KEY",
            "provenance",
            "manifest_refs",
        ):
            self.assertNotIn(forbidden, serialized)

    async def scripted_peer(
        self,
        handler: Callable[
            [asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]
        ],
    ) -> tuple[PeerEndpoint, asyncio.Future[None]]:
        completed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        tasks: set[asyncio.Task[None]] = set()

        async def serve(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                await handler(reader, writer)
            except (AssertionError, PeerError, OSError, KeyError, TypeError) as exc:
                completed.set_exception(exc)
            else:
                completed.set_result(None)
            finally:
                await close_writer(writer)

        def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            task = asyncio.create_task(serve(reader, writer))
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        server = await asyncio.start_server(
            accept, "127.0.0.1", 0, ssl=self.pki.server.identity().server_context()
        )

        async def close() -> None:
            server.close()
            await server.wait_closed()
            for task in tuple(tasks):
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self.addAsyncCleanup(close)
        address = server.sockets[0].getsockname()
        return PeerEndpoint(
            address[0], address[1], "localhost", self.server_pin
        ), completed

    async def test_v1_server_receives_no_profile_or_identify_frame(self) -> None:
        _, profile = self.saved_profile()
        received: list[dict[str, Any]] = []

        async def legacy(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            received.append(await read_frame(reader))
            await write_frame(
                writer,
                {
                    "type": "welcome",
                    "version": 1,
                    "project": PROJECT,
                    "capabilities": ["status"],
                },
            )
            request = await read_frame(reader)
            received.append(request)
            self.assertEqual(request["type"], "request")
            self.assertEqual(request["operation"], "status")
            await write_frame(
                writer,
                {
                    "type": "result",
                    "id": request["id"],
                    "result": {"protocol_version": 1, "capabilities": ["status"]},
                },
            )
            with self.assertRaises(ConnectionClosed):
                await read_frame(reader)

        endpoint, completed = await self.scripted_peer(legacy)
        client = self.client(profile)
        await client.connect(endpoint)
        self.assertEqual(client.protocol_version, 1)
        info = client.peer_info
        assert info is not None
        self.assertEqual(info.fingerprint, self.server_pin)
        self.assertIsNone(info.profile)
        self.assertFalse(info.identity_pinned)
        self.assertEqual(
            await client.request("status"),
            {
                "protocol_version": 1,
                "capabilities": ["status"],
            },
        )
        await client.close()
        await asyncio.wait_for(completed, timeout=3)
        self.assertEqual(
            received[0], {"type": "hello", "versions": [2, 1], "project": PROJECT}
        )
        self.assertNotIn(profile.node_id, json.dumps(received))
        self.assertNotIn("identify", json.dumps(received))

    async def test_v1_fallback_is_rejected_when_node_binding_is_required(self) -> None:
        _, profile = self.saved_profile()

        async def legacy(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            self.assertEqual((await read_frame(reader))["type"], "hello")
            await write_frame(
                writer,
                {
                    "type": "welcome",
                    "version": 1,
                    "project": PROJECT,
                    "capabilities": ["status"],
                },
            )
            with self.assertRaises(ConnectionClosed):
                await read_frame(reader)

        endpoint, completed = await self.scripted_peer(legacy)
        client = self.client(profile)
        with self.assertRaises(AuthenticationError):
            await client.connect(replace(endpoint, expected_node_id=profile.node_id))
        self.assertIsNone(client.peer_info)
        await asyncio.wait_for(completed, timeout=3)

    async def test_status_cannot_silently_replace_handshake_profile(self) -> None:
        profile = NodeProfile("server-id", "server-name")

        async def dishonest(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            await read_frame(reader)
            await write_frame(
                writer,
                {
                    "type": "welcome",
                    "version": 2,
                    "project": PROJECT,
                    "capabilities": ["status"],
                    "node": profile.to_dict(),
                },
            )
            self.assertEqual(
                await read_frame(reader), {"type": "identify", "node": None}
            )
            await write_frame(writer, {"type": "ready"})
            request = await read_frame(reader)
            await write_frame(
                writer,
                {
                    "type": "result",
                    "id": request["id"],
                    "result": {
                        "protocol_version": 2,
                        "capabilities": ["status"],
                        "node": NodeProfile("impostor-id", "server-name").to_dict(),
                    },
                },
            )
            with self.assertRaises(ConnectionClosed):
                await read_frame(reader)

        endpoint, completed = await self.scripted_peer(dishonest)
        client = self.client()
        await client.connect(replace(endpoint, expected_node_id=profile.node_id))
        with self.assertRaises(ProtocolError):
            await client.request("status")
        self.assertFalse(client.connected)
        self.assertIsNone(client.peer_info)
        await asyncio.wait_for(completed, timeout=3)
