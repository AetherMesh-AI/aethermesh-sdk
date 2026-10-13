"""Cancellation after a real TLS content response reaches transport backpressure."""

from __future__ import annotations

import asyncio
import contextvars
import socket
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.network import (
    ContentStore,
    Limits,
    PeerClient,
    PeerEndpoint,
    PeerService,
    RemoteError,
    TransferStore,
    certificate_fingerprint,
    download_transfer,
)
from tests.network_test_support import TestPKI


class ObservedService(PeerService):
    """Observe real drain pressure without replacing it with a synthetic wait."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.blocked = asyncio.Event()
        self.cancel_received = asyncio.Event()
        self.blocked_id = 0
        self.buffered_bytes = 0
        self.cancelled_while_pressured = False
        self.fetch_tasks: list[asyncio.Task[bytes]] = []
        self.responses: list[int] = []
        self.pressured_writers: list[asyncio.StreamWriter] = []
        self.sending = contextvars.ContextVar("sending", default=0)

    def _accept(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        generation: object,
    ) -> None:
        self.pressured_writers.append(writer)
        writer.get_extra_info("socket").setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, 4096
        )
        writer.transport.set_write_buffer_limits(high=1024, low=0)
        original_drain = writer.drain

        async def observed_drain() -> None:
            size = writer.transport.get_write_buffer_size()
            if (
                self.sending.get()
                and writer._protocol._paused
                and size > 0
                and not self.blocked.is_set()
            ):
                self.blocked_id = self.sending.get()
                self.buffered_bytes = size
                # Cancel synchronously at the actual flow-control boundary.
                # Waiting for an observer event first permits the OS to drain
                # this response before the observer gets its event-loop turn.
                target = self.fetch_tasks[self.blocked_id - 1]
                self.cancelled_while_pressured = target.cancel()
                self.blocked.set()
            await original_drain()

        writer.drain = observed_drain
        super()._accept(reader, writer, generation=generation)

    async def _send(self, session: Any, message: dict[str, Any]) -> None:
        is_content = message.get("type") == "result" and "data" in message["result"]
        token = self.sending.set(message["id"] if is_content else 0)
        try:
            await super()._send(session, message)
            if message.get("type") in {"result", "error"}:
                self.responses.append(message["id"])
        finally:
            self.sending.reset(token)

    async def _receive(self, session: Any, message: dict[str, Any]) -> bool:
        result = await super()._receive(session, message)
        if message.get("type") == "cancel" and message["id"] == self.blocked_id:
            self.cancel_received.set()
        return result


class ContentBackpressureTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_during_blocked_response_has_no_duplicate_or_budget_refund(
        self,
    ) -> None:
        await self.exercise_pressure(chunked=False)

    async def test_chunk_cancellation_during_real_backpressure(self) -> None:
        await self.exercise_pressure(chunked=True)

    async def exercise_pressure(self, *, chunked: bool) -> None:
        with tempfile.TemporaryDirectory(prefix="content-pressure-") as directory:
            pki = TestPKI.create(Path(directory))
            blob = bytes(range(256)) * 128
            count = 64
            store = ContentStore(
                "pressure", (blob,), max_served_bytes=count * len(blob)
            )
            transfer_store = TransferStore(
                "pressure", blob, max_served_bytes=count * len(blob)
            )
            service = ObservedService(
                pki.server.identity(),
                project_id="pressure",
                allowed_peers=frozenset(
                    {certificate_fingerprint(pki.client.certificate)}
                ),
                content_store=None if chunked else store,
                transfer_store=transfer_store if chunked else None,
                limits=Limits(max_inflight=count),
            )
            client = PeerClient(
                pki.client.identity(),
                project_id="pressure",
                limits=Limits(max_inflight=count),
            )
            tasks: list[asyncio.Task[bytes]] = []
            paused_transport: asyncio.Transport | None = None
            try:
                await service.start()
                await client.connect(
                    PeerEndpoint(
                        *service.address,
                        "localhost",
                        certificate_fingerprint(pki.server.certificate),
                    )
                )
                writer = client._writer
                assert writer is not None
                # Pause the real socket reader, not only SSL application
                # delivery. Keep the negotiated TCP receive window unchanged:
                # shrinking it mid-session causes slow window recovery on Linux.
                paused_transport = writer.transport._ssl_protocol._transport
                paused_transport.pause_reading()

                def fetch():
                    if chunked:
                        return client.fetch_chunk(
                            transfer_store.manifest, 0, timeout=30
                        )
                    return client.fetch_content(store.references[0], timeout=30)

                tasks = [asyncio.create_task(fetch()) for _ in range(count)]
                service.fetch_tasks = tasks
                await asyncio.wait_for(service.blocked.wait(), 5)
                self.assertGreater(service.buffered_bytes, 0)
                cancelled_id = service.blocked_id
                self.assertTrue(service.cancelled_while_pressured)
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(tasks[cancelled_id - 1], 5)
                await asyncio.wait_for(service.cancel_received.wait(), 5)
                self.assertNotIn(cancelled_id, client._pending)
                # The frame already owns the write. Cancellation interrupts
                # the caller, while the server finishes that single frame.
                # It may drain into kernel buffers even with reading paused.
                # Restore ordinary sender throughput after observing pressure;
                # receiver window sizing has not changed during the session.
                for responding in service.pressured_writers:
                    responding.get_extra_info("socket").setsockopt(
                        socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576
                    )
                    responding.transport.set_write_buffer_limits()
                paused_transport.resume_reading()
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 30
                )
                self.assertEqual(
                    sum(isinstance(item, asyncio.CancelledError) for item in results), 1
                )
                self.assertEqual(
                    [type(item).__name__ for item in results],
                    [
                        "CancelledError" if index == cancelled_id else "bytes"
                        for index in range(1, count + 1)
                    ],
                )
                self.assertTrue(
                    all(
                        item == blob
                        for index, item in enumerate(results, 1)
                        if index != cancelled_id
                    )
                )
                await client.request("status")
                self.assertEqual(
                    sorted(value for value in service.responses if value <= count),
                    list(range(1, count + 1)),
                )
                with self.assertRaises(RemoteError) as exhausted:
                    await fetch()
                self.assertEqual(exhausted.exception.code, "resource_limit")
            finally:
                if paused_transport is not None:
                    paused_transport.resume_reading()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await client.close()
                await service.close()
            self.assertEqual(service.peers, ())


class DownloadBackpressureTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_download_cancellation_owns_only_its_pressured_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            pki = TestPKI.create(root / "pki")
            blob = bytes(range(256)) * 128
            store = TransferStore("pressure", blob, max_served_bytes=4 * 1024 * 1024)
            service = ObservedService(
                pki.server.identity(),
                project_id="pressure",
                allowed_peers=frozenset(
                    {certificate_fingerprint(pki.client.certificate)}
                ),
                transfer_store=store,
                limits=Limits(max_inflight=64),
            )
            await service.start()
            endpoint = PeerEndpoint(
                *service.address,
                "localhost",
                certificate_fingerprint(pki.server.certificate),
            )
            original = PeerClient.fetch_chunk
            paused = []
            children = []
            owned = []

            def client_factory(*args, **kwargs):
                client = PeerClient(
                    *args, **(kwargs | {"limits": Limits(max_inflight=64)})
                )
                owned.append(client)
                return client

            async def pressured_fetch(client, manifest, index, *, timeout):
                writer = client._writer
                transport = writer.transport._ssl_protocol._transport
                paused.append(transport)
                transport.pause_reading()
                # Deliberately saturate this owned session with bounded genuine
                # chunk requests. This is test instrumentation, not API prefetch.
                task = asyncio.current_task()
                service.fetch_tasks = [task] * 65  # Manifest is request 1.
                children.extend(
                    asyncio.create_task(
                        original(client, manifest, index, timeout=timeout)
                    )
                    for _ in range(64)
                )
                try:
                    return (await asyncio.gather(*children))[0]
                finally:
                    # Release the deliberate sender throttle once cancellation
                    # has been observed. Linux otherwise drains the test's 64
                    # queued replies slowly even after the reader resumes.
                    for responding in service.pressured_writers:
                        responding.get_extra_info("socket").setsockopt(
                            socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576
                        )
                        responding.transport.set_write_buffer_limits()
                    transport.resume_reading()
                    for child in children:
                        child.cancel()
                    await asyncio.gather(*children, return_exceptions=True)

            task = None
            try:
                async with PeerClient(
                    pki.client.identity(), project_id="pressure"
                ) as other:
                    await other.connect(endpoint)
                    with (
                        patch(
                            "aethermesh_core.network.download.PeerClient",
                            client_factory,
                        ),
                        patch.object(PeerClient, "fetch_chunk", pressured_fetch),
                    ):
                        task = asyncio.create_task(
                            download_transfer(
                                pki.client.identity(),
                                endpoint,
                                project_id="pressure",
                                manifest_sha256=store.manifest.digest,
                                staging=root / "stage",
                                output=root / "out",
                                timeout=15,
                            )
                        )
                        await asyncio.wait_for(service.blocked.wait(), 5)
                        self.assertGreater(service.buffered_bytes, 0)
                        self.assertTrue(service.cancelled_while_pressured)
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(task, 5)
                    self.assertFalse((root / "out").exists())
                    self.assertTrue(all(child.done() for child in children))
                    self.assertFalse(owned[0].connected)
                    self.assertIsNone(owned[0]._receiver)
                    self.assertEqual(owned[0]._pending, {})
                    await other.request("status")
                    result = await download_transfer(
                        pki.client.identity(),
                        endpoint,
                        project_id="pressure",
                        manifest_sha256=store.manifest.digest,
                        staging=root / "stage",
                        output=root / "out",
                        resume=True,
                    )
                    self.assertEqual(result.output.read_bytes(), blob)
            finally:
                for transport in paused:
                    transport.resume_reading()
                if task is not None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await service.close()
