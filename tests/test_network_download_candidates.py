"""Candidate downloads keep one transfer session and explicit resume boundaries."""

import asyncio
import socket
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from aethermesh_core.network import (
    AuthenticationError,
    ConnectionClosed,
    DownloadError,
    Limits,
    PeerClient,
    ProtocolError,
    RequestTimeout,
    download_transfer,
)
from tests import test_network_download as download_tests


class CandidateDownloadTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = download_tests.DownloadTests.asyncSetUp

    async def fetch(self, endpoints=None, **kwargs):
        return await download_transfer(
            self.pki.client.identity(),
            endpoints or (self.endpoint,),
            **(self.args | kwargs),
        )

    async def test_fallback_cancel_corrupt_resume_and_no_overwrite(self):
        async def interrupt(progress):
            if progress.verified_chunks == 2:
                raise asyncio.CancelledError()

        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            endpoints = (
                replace(self.endpoint, port=unavailable.getsockname()[1]),
                self.endpoint,
            )
            with self.assertRaises(asyncio.CancelledError):
                await self.fetch(
                    endpoints,
                    limits=Limits(handshake_timeout=0.1),
                    on_progress=interrupt,
                )
            self.assertFalse(self.args["output"].exists())
            (self.args["staging"] / "001.chunk").write_bytes(b"corrupt")
            result = await self.fetch(
                endpoints, limits=Limits(handshake_timeout=0.1), resume=True
            )
        self.assertEqual((result.chunks_reused, result.chunks_fetched), (1, 2))
        self.assertEqual(result.output.read_bytes(), self.blob)
        self.assertTrue(result.integrity_verified)
        self.assertFalse(result.publisher_authorized)
        with self.assertRaises(DownloadError) as caught:
            await self.fetch(resume=True)
        self.assertEqual(caught.exception.code, "output_exists")
        self.assertEqual(result.output.read_bytes(), self.blob)

    async def test_no_fallback_after_negotiation_or_application_failure(self):
        original = PeerClient.connect
        calls = []

        async def observe(client, endpoint):
            calls.append(endpoint)
            await original(client, endpoint)

        endpoints = (self.endpoint, replace(self.endpoint, port=12345))
        for failure in (ConnectionClosed("EOF"), ProtocolError("manifest")):
            calls.clear()
            with (
                patch.object(PeerClient, "connect", observe),
                patch.object(
                    PeerClient, "fetch_manifest", AsyncMock(side_effect=failure)
                ),
                self.assertRaises(type(failure)) as caught,
            ):
                await self.fetch(endpoints)
            self.assertIs(caught.exception, failure)
            self.assertEqual(calls, [self.endpoint])
            self.assertFalse(self.args["output"].exists())
        for kwargs, error in (
            ({"project_id": "wrong"}, ConnectionClosed),
            ({}, AuthenticationError),
        ):
            calls.clear()
            selected = (
                endpoints
                if kwargs
                else tuple(replace(e, fingerprint="0" * 64) for e in endpoints)
            )
            with patch.object(PeerClient, "connect", observe), self.assertRaises(error):
                await self.fetch(selected, **kwargs)
            self.assertEqual(len(calls), 1)

    async def test_selection_time_counts_against_transfer_deadline(self):
        from aethermesh_core.network import download

        original = download.connect_peer_candidates
        loop = asyncio.get_running_loop()
        elapsed = 0

        async def consume(*args, **kwargs):
            nonlocal elapsed
            connection = await original(*args, **kwargs)
            elapsed = 31
            return connection

        with (
            patch.object(download, "asyncio", wraps=asyncio) as timing,
            patch.object(download, "connect_peer_candidates", consume),
            patch.object(PeerClient, "fetch_manifest", AsyncMock()) as fetch,
            self.assertRaises(RequestTimeout),
        ):
            timing.get_running_loop.return_value = SimpleNamespace(
                time=lambda: loop.time() + elapsed
            )
            await self.fetch(timeout=30)
        fetch.assert_not_awaited()
        self.assertFalse(self.args["output"].exists())

    async def test_deadline_expired_before_selection(self):
        from aethermesh_core.network import download

        loop = asyncio.get_running_loop()
        now = loop.time()
        with (
            patch.object(download, "asyncio", wraps=asyncio) as timing,
            patch.object(download, "connect_peer_candidates", AsyncMock()) as connect,
            self.assertRaises(RequestTimeout),
        ):
            timing.get_running_loop.return_value = MagicMock(
                time=MagicMock(side_effect=[now, now + 2])
            )
            await self.fetch(timeout=1)
        connect.assert_not_awaited()

    async def test_absolute_deadline_cancels_selection(self):
        from aethermesh_core.network import download

        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def stalled(*args, **kwargs):
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        with (
            patch.object(download, "connect_peer_candidates", stalled),
            self.assertRaises(RequestTimeout),
        ):
            await self.fetch(timeout=0.02)
        self.assertTrue(entered.is_set())
        self.assertTrue(cancelled.is_set())
        self.assertFalse(self.args["output"].exists())

    async def test_transfer_cancellation_and_error_survive_cleanup_failure(self):
        original = PeerClient.close
        clients = []
        cleanup = ValueError("cleanup")

        async def failed_close(client):
            clients.append(client)
            await original(client)
            raise cleanup

        for failure in (asyncio.CancelledError(), ProtocolError("manifest")):
            with (
                patch.object(
                    PeerClient, "fetch_manifest", AsyncMock(side_effect=failure)
                ),
                patch.object(PeerClient, "close", failed_close),
                self.assertRaises(type(failure)) as caught,
            ):
                await self.fetch()
            self.assertIs(caught.exception, failure)
            self.assertIs(caught.exception.__cause__, cleanup)
            self.assertFalse(clients[-1].connected)
            self.assertIsNone(clients[-1]._receiver)
            self.assertFalse(self.args["output"].exists())

    async def test_validation_precedes_connection(self):
        for endpoints in (
            (),
            (self.endpoint,) * 2,
            (self.endpoint, replace(self.endpoint, fingerprint="0" * 64)),
        ):
            with (
                patch.object(PeerClient, "connect", AsyncMock()) as connect,
                self.assertRaises(ValueError),
            ):
                await download_transfer(
                    self.pki.client.identity(), endpoints, **self.args
                )
            connect.assert_not_awaited()
        with self.assertRaises(TypeError):
            await self.fetch(limits="invalid")
