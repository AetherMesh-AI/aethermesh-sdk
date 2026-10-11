"""Cancellation after a real TLS content response reaches transport backpressure."""

from __future__ import annotations

import asyncio
import contextvars
import socket
import tempfile
import unittest
from pathlib import Path
from typing import Any

from aethermesh_core.network import (
    ContentStore,
    Limits,
    PeerClient,
    PeerEndpoint,
    PeerService,
    RemoteError,
    certificate_fingerprint,
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
        with tempfile.TemporaryDirectory(prefix="content-pressure-") as directory:
            pki = TestPKI.create(Path(directory))
            blob = bytes(range(256)) * 128
            count = 64
            store = ContentStore(
                "pressure", (blob,), max_served_bytes=count * len(blob)
            )
            service = ObservedService(
                pki.server.identity(),
                project_id="pressure",
                allowed_peers=frozenset(
                    {certificate_fingerprint(pki.client.certificate)}
                ),
                content_store=store,
                limits=Limits(max_inflight=count),
            )
            client = PeerClient(
                pki.client.identity(),
                project_id="pressure",
                limits=Limits(max_inflight=count),
            )
            tasks: list[asyncio.Task[bytes]] = []
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
                writer.get_extra_info("socket").setsockopt(
                    socket.SOL_SOCKET, socket.SO_RCVBUF, 4096
                )
                writer.transport.pause_reading()
                tasks = [
                    asyncio.create_task(
                        client.fetch_content(store.references[0], timeout=30)
                    )
                    for _ in range(count)
                ]
                await asyncio.wait_for(service.blocked.wait(), 5)
                self.assertGreater(service.buffered_bytes, 0)
                cancelled_id = service.blocked_id
                self.assertNotIn(cancelled_id, service.responses)
                tasks[cancelled_id - 1].cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await tasks[cancelled_id - 1]
                await asyncio.wait_for(service.cancel_received.wait(), 5)
                self.assertNotIn(cancelled_id, client._pending)
                self.assertNotIn(cancelled_id, service.responses)
                # Tiny buffers force the observation; restore throughput before
                # recovery so Linux TCP window growth is not the test deadline.
                writer.get_extra_info("socket").setsockopt(
                    socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576
                )
                for responding in service.pressured_writers:
                    responding.get_extra_info("socket").setsockopt(
                        socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576
                    )
                    responding.transport.set_write_buffer_limits()
                writer.transport.resume_reading()
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
                    await client.fetch_content(store.references[0])
                self.assertEqual(exhausted.exception.code, "resource_limit")
            finally:
                if client._writer is not None:
                    client._writer.transport.resume_reading()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await client.close()
                await service.close()
            self.assertEqual(service.peers, ())
