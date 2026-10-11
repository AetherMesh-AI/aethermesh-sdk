"""Bounded content contracts and real authenticated loopback exchange."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.network import (
    CONTENT_OPERATION,
    MAX_CONTENT_BYTES,
    ConnectionClosed,
    ContentRef,
    ContentStore,
    PeerClient,
    PeerEndpoint,
    PeerService,
    ProtocolError,
    RemoteError,
    RequestTimeout,
    certificate_fingerprint,
)
from aethermesh_core.network.content import validate_content_result
from tests.network_test_support import TestPKI


class ContentContractTests(unittest.TestCase):
    def test_snapshot_and_budget(self) -> None:
        blob = bytes(range(256)) * 128
        store = ContentStore("sdk", (b"", blob, blob), max_served_bytes=len(blob))
        self.assertEqual(len(store.references), 2)
        for ref, expected in zip(store.references, (b"", blob), strict=True):
            self.assertEqual(
                validate_content_result(store.get(ref.to_dict()), ref.to_dict()),
                expected,
            )
        with self.assertRaises(RemoteError) as caught:
            store.get(store.references[1].to_dict())
        self.assertEqual(caught.exception.code, "resource_limit")
        self.assertEqual(ContentStore("sdk", ()).references, ())

    def test_configuration_limits(self) -> None:
        for blobs in (
            [b"a"],
            (b"a",) * 65,
            (bytearray(b"a"),),
            (b"x" * 32769,),
            (b"x" * 32768,) * 33,
        ):
            with self.subTest(blobs_type=type(blobs)), self.assertRaises(ValueError):
                ContentStore("sdk", blobs)
        for budget in (-1, True, 67108865):
            with self.subTest(budget=budget), self.assertRaises(ValueError):
                ContentStore("sdk", (), max_served_bytes=budget)
        for size in (-1, True, 32769):
            with self.subTest(size=size), self.assertRaises(ValueError):
                ContentRef("sdk", "a" * 64, size)

    def test_invalid_requests_and_missing_content(self) -> None:
        store = ContentStore("sdk", (b"a",))
        ref = store.references[0].to_dict()
        for payload in (
            {},
            {**ref, "path": "../../secret"},
            {**ref, "project": "other"},
            {**ref, "sha256": "../x"},
            {**ref, "size": False},
        ):
            with (
                self.subTest(payload=payload),
                self.assertRaises(RemoteError) as caught,
            ):
                store.get(payload)
            self.assertEqual(caught.exception.code, "invalid_request")
        for payload in ({**ref, "sha256": "a" * 64}, {**ref, "size": 2}):
            with self.assertRaises(RemoteError) as caught:
                store.get(payload)
            self.assertEqual(caught.exception.code, "not_found")

    def test_untrusted_results(self) -> None:
        store = ContentStore("sdk", (b"a",))
        ref = store.references[0].to_dict()
        good = store.get(ref)
        for bad in (
            {},
            {**good, "extra": 1},
            {**good, "project": "eidolon"},
            {**good, "size": True},
            {**good, "sha256": "b" * 64},
            {**good, "data": []},
            {**good, "data": "a" * 5},
            {**good, "data": "!!!!"},
            {**good, "data": "é"},
            {**good, "data": "YR=="},
            {**good, "data": "Yg=="},
            {**good, "data": ""},
        ):
            with self.subTest(bad=bad), self.assertRaises(ProtocolError):
                validate_content_result(bad, ref)
        with self.assertRaises(ProtocolError):
            validate_content_result(good, {})


class ControlledService(PeerService):
    mode = "honest"

    async def _operate(
        self, operation: str, payload: dict[str, Any], *, protocol_version: int = 1
    ) -> dict[str, Any]:
        if operation == CONTENT_OPERATION:
            self.entered.set()
            if self.mode == "delay":
                await asyncio.Event().wait()
        result = await super()._operate(
            operation, payload, protocol_version=protocol_version
        )
        if operation == CONTENT_OPERATION and self.mode == "corrupt":
            result["data"] = base64.b64encode(b"wrong").decode("ascii")
        return result


class ContentTLSTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory(prefix="content-tls-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.pki = TestPKI.create(Path(cls.directory.name))
        cls.client_pin = certificate_fingerprint(cls.pki.client.certificate)
        cls.server_pin = certificate_fingerprint(cls.pki.server.certificate)

    async def asyncSetUp(self) -> None:
        self.blob = bytes(range(256)) * 128
        self.store = ContentStore("sdk", (self.blob, b""))
        self.ref = self.store.references[0]
        self.service = ControlledService(
            self.pki.server.identity(),
            project_id="sdk",
            allowed_peers=frozenset({self.client_pin}),
            content_store=self.store,
        )
        self.service.entered = asyncio.Event()
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        self.endpoint = PeerEndpoint(
            *self.service.address, "localhost", self.server_pin
        )
        self.client = PeerClient(self.pki.client.identity(), project_id="sdk")
        self.addAsyncCleanup(self.client.close)
        await self.client.connect(self.endpoint)

    async def test_roundtrip_and_missing(self) -> None:
        self.assertIn(CONTENT_OPERATION, self.client.capabilities)
        self.assertEqual(await self.client.fetch_content(self.ref), self.blob)
        self.assertEqual(await self.client.fetch_content(self.store.references[1]), b"")
        with self.assertRaises(ValueError):
            await self.client.fetch_content(
                ContentRef("eidolon", self.ref.sha256, self.ref.size)
            )
        with self.assertRaises(RemoteError) as caught:
            await self.client.fetch_content(ContentRef("sdk", "a" * 64, 1))
        self.assertEqual(caught.exception.code, "not_found")
        with self.assertRaises(RemoteError) as caught:
            await self.client.request(
                CONTENT_OPERATION, {**self.ref.to_dict(), "project": "eidolon"}
            )
        self.assertEqual(caught.exception.code, "invalid_request")
        self.assertTrue(self.client.connected)

    async def test_corrupt_response_closes_session(self) -> None:
        self.service.mode = "corrupt"
        with self.assertRaises(ProtocolError):
            await self.client.fetch_content(self.ref)
        self.assertFalse(self.client.connected)

    async def test_timeout_cancel_and_shutdown(self) -> None:
        self.service.mode = "delay"
        with self.assertRaises(RequestTimeout):
            await self.client.fetch_content(self.ref, timeout=0.03)
        self.service.entered.clear()
        task = asyncio.create_task(self.client.fetch_content(self.ref))
        await asyncio.wait_for(self.service.entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.client.request("status")
        self.service.entered.clear()
        task = asyncio.create_task(self.client.fetch_content(self.ref))
        await asyncio.wait_for(self.service.entered.wait(), 2)
        await self.service.close()
        with self.assertRaises(ConnectionClosed):
            await task
        self.assertEqual(self.service.peers, ())

    async def test_v1_and_opt_out(self) -> None:
        await self.client.close()
        with patch("aethermesh_core.network.client.SUPPORTED_PROTOCOL_VERSIONS", (1,)):
            await self.client.connect(self.endpoint)
        self.assertEqual(self.client.protocol_version, 1)
        self.assertNotIn(CONTENT_OPERATION, self.client.capabilities)
        with self.assertRaises(RemoteError):
            await self.client.fetch_content(self.ref)
        await self.client.close()
        await self.service.close()
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="sdk",
            allowed_peers=frozenset({self.client_pin}),
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        await self.client.connect(
            PeerEndpoint(*self.service.address, "localhost", self.server_pin)
        )
        self.assertEqual(self.client.capabilities, ("status",))
        with self.assertRaises(RemoteError):
            await self.client.fetch_content(self.ref)

    async def test_budget_survives_restart(self) -> None:
        store = ContentStore("sdk", (b"a",), max_served_bytes=1)
        service = PeerService(
            self.pki.server.identity(),
            project_id="sdk",
            allowed_peers=frozenset({self.client_pin}),
            content_store=store,
        )
        self.addAsyncCleanup(service.close)
        for attempt in range(2):
            await service.start()
            async with PeerClient(
                self.pki.client.identity(), project_id="sdk"
            ) as client:
                await client.connect(
                    PeerEndpoint(*service.address, "localhost", self.server_pin)
                )
                if attempt == 0:
                    self.assertEqual(
                        await client.fetch_content(store.references[0]), b"a"
                    )
                else:
                    with self.assertRaises(RemoteError) as caught:
                        await client.fetch_content(store.references[0])
                    self.assertEqual(caught.exception.code, "resource_limit")
            await service.close()

    async def test_service_configuration_and_wrong_project(self) -> None:
        for store in (object(), ContentStore("eidolon", ())):
            with self.assertRaises((TypeError, ValueError)):
                PeerService(
                    self.pki.server.identity(),
                    project_id="sdk",
                    allowed_peers=frozenset({self.client_pin}),
                    content_store=store,
                )
        async with PeerClient(
            self.pki.client.identity(), project_id="eidolon"
        ) as client:
            with self.assertRaises(ConnectionClosed):
                await client.connect(self.endpoint)
        async with PeerClient(
            self.pki.unauthorized.identity(), project_id="sdk"
        ) as client:
            with self.assertRaises(ConnectionClosed):
                await client.connect(self.endpoint)

    async def test_reconnect_and_concurrent_fetches(self) -> None:
        self.assertEqual(
            await asyncio.gather(
                *(self.client.fetch_content(self.ref) for _ in range(4))
            ),
            [self.blob] * 4,
        )
        await self.client.close()
        await self.client.connect(self.endpoint)
        self.assertEqual(await self.client.fetch_content(self.ref), self.blob)
        self.assertEqual(hashlib.sha256(self.blob).hexdigest(), self.ref.sha256)
        self.assertEqual(len(self.blob), MAX_CONTENT_BYTES)
