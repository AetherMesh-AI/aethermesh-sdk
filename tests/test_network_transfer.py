"""Manifest, budget, malicious-response and live TLS transfer boundaries."""

from __future__ import annotations

import base64
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.network import (
    PeerClient,
    PeerEndpoint,
    PeerService,
    ProtocolError,
    RemoteError,
    TransferManifest,
    TransferStore,
    certificate_fingerprint,
)
from aethermesh_core.network.transfer import (
    CHUNK_OPERATION,
    MANIFEST_OPERATION,
    MAX_TRANSFER_BYTES,
    validate_transfer_result,
)
from tests.network_test_support import TestPKI


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ManifestTests(unittest.TestCase):
    def test_canonical_roundtrip_and_boundaries(self):
        for data in (b"", b"x", bytes(range(256)) * 32768):
            with self.subTest(size=len(data)):
                store = TransferStore(
                    "project", data, max_served_bytes=64 * 1024 * 1024
                )
                manifest = store.manifest
                self.assertEqual(
                    manifest,
                    TransferManifest.decode(
                        manifest.encode(), "project", manifest.digest
                    ),
                )
                self.assertEqual(len(manifest.chunks), (len(data) + 32767) // 32768)
                request = {"project": "project", "sha256": manifest.digest}
                result = store.get(MANIFEST_OPERATION, request)
                self.assertEqual(
                    validate_transfer_result(MANIFEST_OPERATION, result, request),
                    manifest.encode(),
                )
                parts = []
                for index in range(len(manifest.chunks)):
                    payload = {
                        **manifest.chunk(index).to_dict(),
                        "manifest": manifest.digest,
                        "index": index,
                    }
                    result = store.get(CHUNK_OPERATION, payload)
                    parts.append(
                        validate_transfer_result(CHUNK_OPERATION, result, payload)
                    )
                self.assertEqual(b"".join(parts), data)

    def test_invalid_manifests(self):
        m = TransferStore("p", b"a", max_served_bytes=1000).manifest
        for size, chunks in (
            (True, ()),
            (-1, ()),
            (MAX_TRANSFER_BYTES + 1, ()),
            (1, []),
            (1, ()),
            (1, ("bad",)),
        ):
            with self.subTest(size=size, chunks=chunks), self.assertRaises(ValueError):
                TransferManifest("p", digest(b""), size, chunks)
        for index in (-1, True, 1):
            with self.assertRaises(ValueError):
                m.chunk(index)
        for data in (
            b"[1]",
            b"{}",
            b"null",
            b"{",
            m.encode() + b"\n",
            b"x" * 32769,
            m.encode().replace(b'"version":1', b'"version":true'),
            m.encode().replace(b'"chunk_size":32768', b'"chunk_size":1'),
            m.encode().replace(b'"chunks":[', b'"extra":1,"chunks":['),
            m.encode().replace(b'"version":1', b'"version":1,"version":1'),
            m.encode().replace(b'"project":"p"', b'"project":"q"'),
            m.encode().replace(
                b'"chunks":["' + m.chunks[0].encode() + b'"]', b'"chunks":null'
            ),
        ):
            with self.subTest(data=data[:40]), self.assertRaises(ValueError):
                TransferManifest.decode(data, "p", digest(data))
        with self.assertRaises(ValueError):
            TransferManifest.decode(m.encode(), "p", "0" * 64)
        for data, budget in (
            (bytearray(b"x"), 100),
            (b"x" * (MAX_TRANSFER_BYTES + 1), 100),
            (b"", True),
            (b"", -1),
            (b"", 67108865),
        ):
            with self.assertRaises(ValueError):
                TransferStore("p", data, max_served_bytes=budget)

    def test_invalid_requests_results_and_budget(self):
        store = TransferStore("p", b"data", max_served_bytes=4)
        m = store.manifest
        request = {**m.chunk(0).to_dict(), "manifest": m.digest, "index": 0}
        for op, payload in (
            ("unknown", {}),
            (MANIFEST_OPERATION, {}),
            (CHUNK_OPERATION, {}),
            (CHUNK_OPERATION, {**request, "index": True}),
            (CHUNK_OPERATION, {**request, "manifest": "0" * 64}),
        ):
            with self.assertRaises(RemoteError):
                store.get(op, payload)
        result = store.get(CHUNK_OPERATION, request)
        with self.assertRaises(RemoteError) as exhausted:
            store.get(CHUNK_OPERATION, request)
        self.assertEqual(exhausted.exception.code, "resource_limit")
        for req, res, op in (
            ({}, result, CHUNK_OPERATION),
            ({**request, "index": True}, result, CHUNK_OPERATION),
            ({**request, "index": 256}, result, CHUNK_OPERATION),
            ({**request, "manifest": "bad"}, result, CHUNK_OPERATION),
            (request, {**result, "data": "Yg=="}, CHUNK_OPERATION),
            ({}, result, MANIFEST_OPERATION),
            ({"project": "p", "sha256": m.digest}, {}, MANIFEST_OPERATION),
        ):
            with self.assertRaises(ProtocolError):
                validate_transfer_result(op, res, req)
        data = b"{" + b'"a":[' * 2000 + b"0" + b"]}" * 2000
        request = {"project": "p", "sha256": digest(data)}
        result = {**request, "size": len(data), "data": base64.b64encode(data).decode()}
        with self.assertRaises(ProtocolError):
            validate_transfer_result(MANIFEST_OPERATION, result, request)


class TransferPeerTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_transfer_compatibility_and_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            pki = TestPKI.create(Path(directory))
            store = TransferStore("p", b"a" * 32768 + b"b", max_served_bytes=100000)
            kwargs = {
                "project_id": "p",
                "allowed_peers": frozenset(
                    {certificate_fingerprint(pki.client.certificate)}
                ),
            }
            for wrong, error in (
                (object(), TypeError),
                (TransferStore("q", b"", max_served_bytes=0), ValueError),
            ):
                with self.assertRaises(error):
                    PeerService(pki.server.identity(), transfer_store=wrong, **kwargs)
            service = PeerService(pki.server.identity(), transfer_store=store, **kwargs)
            self.assertEqual(service._session_capabilities(1), frozenset({"status"}))
            async with (
                service,
                PeerClient(pki.client.identity(), project_id="p") as client,
            ):
                endpoint = PeerEndpoint(
                    *service.address,
                    "localhost",
                    certificate_fingerprint(pki.server.certificate),
                )
                await client.connect(endpoint)
                manifest = await client.fetch_manifest(store.manifest.digest)
                self.assertEqual(await client.fetch_chunk(manifest, 1), b"b")
                with self.assertRaises(ValueError):
                    await client.fetch_chunk(
                        TransferStore("q", b"a", max_served_bytes=100).manifest, 0
                    )
                with self.assertRaises(ValueError):
                    await client.fetch_manifest("bad")
                with self.assertRaises(RemoteError):
                    await client.fetch_manifest("0" * 64)
                # Unknown capabilities remain a hard failure for v1 negotiation.
                with self.assertRaises(ProtocolError):
                    client._welcome(
                        {
                            "type": "welcome",
                            "version": 1,
                            "project": "p",
                            "capabilities": ["status", CHUNK_OPERATION],
                        }
                    )
                status = await client.request("status")
                self.assertIn(CHUNK_OPERATION, status["capabilities"])


class TransferCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_whitelist_v1_and_bad_peer(self):
        with tempfile.TemporaryDirectory() as root:
            pki = TestPKI.create(Path(root))
            store = TransferStore("p", b"opaque", max_served_bytes=100000)
            service = PeerService(
                pki.server.identity(),
                project_id="p",
                allowed_peers=frozenset(
                    {certificate_fingerprint(pki.client.certificate)}
                ),
                transfer_store=store,
            )
            async with (
                service,
                PeerClient(pki.client.identity(), project_id="p") as client,
            ):
                endpoint = PeerEndpoint(
                    *service.address,
                    "localhost",
                    certificate_fingerprint(pki.server.certificate),
                )
                # Freeze the pre-transfer known-capability set. Strict old v2
                # negotiation rejects the opt-in service rather than misreading it.
                with (
                    patch(
                        "aethermesh_core.network.client.TRANSFER_OPERATIONS",
                        frozenset(),
                    ),
                    self.assertRaises(ProtocolError),
                ):
                    await client.connect(endpoint)
                self.assertFalse(client.connected)
                with patch(
                    "aethermesh_core.network.client.SUPPORTED_PROTOCOL_VERSIONS", (1,)
                ):
                    await client.connect(endpoint)
                self.assertEqual(client.capabilities, ("status",))
                with self.assertRaises(RemoteError):
                    await client.fetch_manifest(store.manifest.digest)
                await client.close()
                await client.connect(endpoint)
                original = store.get

                def corrupt(operation, payload):
                    result = original(operation, payload)
                    result["data"] = "eA=="
                    return result

                with (
                    patch.object(store, "get", side_effect=corrupt),
                    self.assertRaises(ProtocolError),
                ):
                    await client.fetch_chunk(store.manifest, 0)
                self.assertFalse(client.connected)
                await client.connect(endpoint)
                with (
                    patch.object(store, "get", side_effect=corrupt),
                    self.assertRaises(ProtocolError),
                ):
                    await client.fetch_manifest(store.manifest.digest)
                self.assertFalse(client.connected)
