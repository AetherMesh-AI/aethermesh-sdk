"""Public download lifecycle, immutable progress and explicit publication outcomes."""

from __future__ import annotations

import asyncio
import dataclasses
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.network import (
    AuthenticationError,
    DownloadError,
    PeerClient,
    PeerEndpoint,
    PeerService,
    RequestTimeout,
    TransferStore,
    certificate_fingerprint,
    download_transfer,
)
from aethermesh_core.network.transfer_files import publish_transfer
from tests.network_test_support import TestPKI


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.pki = TestPKI.create(self.root / "pki")
        self.blob = b"a" * 32768 + b"b" * 32768 + b"c"
        self.store = TransferStore("download", self.blob, max_served_bytes=1_000_000)
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="download",
            allowed_peers=frozenset(
                {certificate_fingerprint(self.pki.client.certificate)}
            ),
            transfer_store=self.store,
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        self.endpoint = PeerEndpoint(
            *self.service.address,
            "localhost",
            certificate_fingerprint(self.pki.server.certificate),
        )
        self.args = {
            "project_id": "download",
            "manifest_sha256": self.store.manifest.digest,
            "staging": self.root / "stage",
            "output": self.root / "out",
            "timeout": 5,
        }

    async def fetch(self, **kwargs):
        return await download_transfer(
            self.pki.client.identity(), self.endpoint, **(self.args | kwargs)
        )

    async def test_progress_failure_releases_lock_and_explicit_resume(self):
        seen = []

        async def fail(progress):
            seen.append(progress)
            if progress.phase == "chunk":
                raise RuntimeError("private callback details")

        async with PeerClient(
            self.pki.client.identity(), project_id="download"
        ) as other:
            await other.connect(self.endpoint)
            with self.assertRaises(DownloadError) as failed:
                await self.fetch(on_progress=fail)
            self.assertEqual(failed.exception.code, "callback_failed")
            self.assertFalse(failed.exception.publication_attempted)
            self.assertNotIn("private", str(failed.exception))
            self.assertIsInstance(failed.exception.__cause__, RuntimeError)
            await other.request("status")
        self.assertFalse(self.args["output"].exists())
        self.assertEqual(
            (self.args["staging"] / "000.chunk").read_bytes(), self.blob[:32768]
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            seen[-1].verified_bytes = 0
        resumed = []

        async def observe(progress):
            self.assertFalse(self.args["output"].exists())
            resumed.append(progress)

        result = await self.fetch(resume=True, on_progress=observe)
        self.assertEqual(result.chunks_fetched, 2)
        self.assertEqual(result.chunks_reused, 1)
        self.assertTrue(result.integrity_verified)
        self.assertFalse(result.publisher_authorized)
        self.assertEqual(result.manifest, self.store.manifest)
        self.assertEqual(result.output.read_bytes(), self.blob)
        self.assertEqual(
            [p.phase for p in resumed],
            ["manifest", "chunk", "chunk", "chunk", "verified"],
        )
        self.assertEqual(
            [p.verified_bytes for p in resumed], [0, 32768, 65536, 65537, 65537]
        )
        self.assertEqual([p.chunks_reused for p in resumed], [0, 1, 1, 1, 1])
        with self.assertRaises(DownloadError) as exists:
            await self.fetch(resume=True)
        self.assertEqual(exists.exception.code, "output_exists")
        self.assertFalse(exists.exception.publication_attempted)

    async def test_cancel_cached_and_final_callbacks_then_restart(self):
        async def final_cancel(progress):
            if progress.phase == "verified":
                raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.fetch(on_progress=final_cancel)
        self.assertFalse(self.args["output"].exists())

        async def cached_cancel(progress):
            if progress.chunks_reused == 1:
                asyncio.current_task().cancel()

        with self.assertRaises(asyncio.CancelledError):
            await self.fetch(resume=True, on_progress=cached_cancel)
        self.assertFalse(self.args["output"].exists())
        (self.args["staging"] / "001.chunk").write_bytes(b"corrupt")
        result = await self.fetch(resume=True)
        self.assertEqual((result.chunks_fetched, result.chunks_reused), (1, 2))
        self.assertEqual(result.output.read_bytes(), self.blob)

    async def test_deadlines_callback_wait_and_connection_cleanup(self):
        async def stalled(progress):
            await asyncio.sleep(10)

        with self.assertRaises(RequestTimeout):
            await self.fetch(timeout=0.05, on_progress=stalled)
        self.assertFalse(self.args["output"].exists())
        close = PeerClient.close

        async def slow_close(client):
            await close(client)
            await asyncio.sleep(0.1)

        with (
            patch.object(PeerClient, "close", slow_close),
            self.assertRaises(RequestTimeout),
        ):
            await self.fetch(timeout=0.08)
        self.assertFalse(self.args["output"].exists())
        result = await self.fetch(resume=True)
        self.assertEqual(result.chunks_fetched, 0)

    async def test_invalid_inputs_and_local_states(self):
        for kwargs in [
            {"timeout": True},
            {"timeout": "1"},
            {"timeout": float("nan")},
            {"timeout": 0},
            {"timeout": 3601},
            {"resume": 1},
            {"staging": "stage"},
            {"output": "out"},
            {"on_progress": 3},
            {"project_id": ""},
            {"manifest_sha256": "bad"},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                await self.fetch(**kwargs)
        with self.assertRaises(DownloadError) as missing:
            await self.fetch(output=self.root / "absent" / "file")
        self.assertEqual(missing.exception.code, "local_io")
        with self.assertRaises(DownloadError) as invalid:
            await self.fetch(output=self.root / ".." / "file")
        self.assertEqual(invalid.exception.code, "invalid_state")
        bad = dataclasses.replace(self.endpoint, fingerprint="0" * 64)
        with self.assertRaises(AuthenticationError):
            await download_transfer(self.pki.client.identity(), bad, **self.args)
        with self.assertRaises(DownloadError) as state:
            await self.fetch(resume=True)
        self.assertEqual(state.exception.code, "local_io")

    async def test_publication_failures_never_claim_rollback(self):
        async def ready(progress):
            if progress.phase == "verified":
                self.args["output"].write_bytes(b"competing local writer")

        with self.assertRaises(DownloadError) as raced:
            await self.fetch(on_progress=ready)
        self.assertEqual(raced.exception.code, "output_exists")
        self.assertTrue(raced.exception.publication_attempted)
        self.assertEqual(self.args["output"].read_bytes(), b"competing local writer")
        self.args["output"].unlink()
        for error in (OSError("fsync"), TimeoutError("fsync deadline")):

            def after_link(directory, name, data, error=error):
                publish_transfer(directory, name, data)
                raise error

            with (
                patch("aethermesh_core.network.download.publish_transfer", after_link),
                self.assertRaises(DownloadError) as failed,
            ):
                await self.fetch(resume=True)
            self.assertEqual(failed.exception.code, "publication_failed")
            self.assertTrue(failed.exception.publication_attempted)
            self.assertEqual(self.args["output"].read_bytes(), self.blob)
            self.args["output"].unlink()

    async def test_empty_manifest_progress_and_result(self):
        empty = TransferStore("download", b"", max_served_bytes=1000)
        await self.service.close()
        self.service = PeerService(
            self.pki.server.identity(),
            project_id="download",
            allowed_peers=frozenset(
                {certificate_fingerprint(self.pki.client.certificate)}
            ),
            transfer_store=empty,
        )
        self.addAsyncCleanup(self.service.close)
        await self.service.start()
        self.endpoint = dataclasses.replace(
            self.endpoint, host=self.service.address[0], port=self.service.address[1]
        )
        seen = []

        async def observe(p):
            seen.append(p)

        result = await self.fetch(
            manifest_sha256=empty.manifest.digest, on_progress=observe
        )
        self.assertEqual([p.phase for p in seen], ["manifest", "verified"])
        self.assertEqual((result.chunks_fetched, result.chunks_reused), (0, 0))
        self.assertEqual(result.output.read_bytes(), b"")
