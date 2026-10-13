"""Explicit transfer CLI deadlines, output boundary and sharing opt-in."""

from __future__ import annotations

import asyncio
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from aethermesh_core.network import DownloadError, TransferStore, cli, download
from tests import test_network_cli as diagnostics


class TransferCLITests(unittest.IsolatedAsyncioTestCase):
    async def test_alternatives_keep_identity_order_and_shared_options(self):
        args = diagnostics.PeerDiagnosticsTests().args()
        args[0] = "fetch-transfer"
        args += [
            "--manifest-sha256",
            "a" * 64,
            "--staging",
            "stage",
            "--output",
            "out",
            "--alternate",
            "127.0.0.2",
            "2345",
            "alternate.example",
            "--alternate",
            "127.0.0.3",
            "3456",
            "another.example",
            "--resume",
        ]
        parsed = cli._parser().parse_args(args)
        manifest = TransferStore("test", b"data", max_served_bytes=0).manifest
        result = Mock(manifest=manifest, chunks_fetched=1)
        with (
            patch.object(
                cli, "download_transfer", AsyncMock(return_value=result)
            ) as fetch,
            redirect_stdout(io.StringIO()),
        ):
            await cli._fetch_transfer(parsed)
        endpoints = fetch.await_args.args[1]
        self.assertEqual(
            [e.host for e in endpoints], [parsed.host, "127.0.0.2", "127.0.0.3"]
        )
        self.assertEqual([e.port for e in endpoints], [parsed.port, 2345, 3456])
        self.assertTrue(
            all(e.fingerprint == parsed.peer_fingerprint for e in endpoints)
        )
        self.assertTrue(
            all(e.expected_node_id == parsed.expected_node_id for e in endpoints)
        )
        self.assertEqual(fetch.await_args.kwargs["timeout"], parsed.timeout)
        self.assertTrue(fetch.await_args.kwargs["resume"])
        for invalid in (
            [["host", "1", "name"]] * 8,
            [["host", "secret-port", "name"]],
            [["host", "0", "name"]],
        ):
            parsed.alternate = invalid
            with (
                patch.object(cli, "download_transfer", AsyncMock()) as fetch,
                self.assertRaises(ValueError),
            ):
                await cli._fetch_transfer(parsed)
            fetch.assert_not_awaited()

    async def test_fetch_success_resume_failure_and_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            manifest = TransferStore("test", b"data", max_served_bytes=1000).manifest
            args = diagnostics.PeerDiagnosticsTests().args()
            args[0] = "fetch-transfer"
            args += [
                "--manifest-sha256",
                manifest.digest,
                "--staging",
                str(root / "stage"),
                "--output",
                str(root / "out"),
            ]
            parsed = cli._parser().parse_args(args)
            client = Mock()
            client.connect = AsyncMock()
            client.close = AsyncMock()
            client.fetch_manifest = AsyncMock(return_value=manifest)
            client.fetch_chunk = AsyncMock(return_value=b"data")
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            record = io.StringIO()
            with (
                patch.object(download, "PeerClient", return_value=client),
                redirect_stdout(record),
            ):
                await cli._fetch_transfer(parsed)
            self.assertEqual((root / "out").read_bytes(), b"data")
            self.assertEqual(json.loads(record.getvalue())["chunks_fetched"], 1)
            (root / "out").unlink()
            parsed.resume = True
            record = io.StringIO()
            with (
                patch.object(download, "PeerClient", return_value=client),
                redirect_stdout(record),
            ):
                await cli._fetch_transfer(parsed)
            self.assertEqual(json.loads(record.getvalue())["chunks_fetched"], 0)
            with (
                patch.object(download, "PeerClient") as constructed,
                self.assertRaises(DownloadError) as exists,
            ):
                await cli._fetch_transfer(parsed)
            constructed.assert_not_called()
            self.assertEqual(exists.exception.code, "output_exists")
            (root / "out").unlink()
            for failure in (
                cli.ProtocolError("bad"),
                asyncio.CancelledError(),
                cli.RequestTimeout("late"),
            ):
                client.fetch_manifest.side_effect = failure
                with (
                    patch.object(download, "PeerClient", return_value=client),
                    self.assertRaises(type(failure)),
                ):
                    await cli._fetch_transfer(parsed)
                self.assertFalse((root / "out").exists())
            client.fetch_manifest.side_effect = None
            (root / "stage" / "000.chunk").unlink()
            client.fetch_chunk.side_effect = cli.RemoteError(
                "resource_limit", "no budget"
            )
            with (
                patch.object(download, "PeerClient", return_value=client),
                self.assertRaises(cli.RemoteError),
            ):
                await cli._fetch_transfer(parsed)
            self.assertFalse((root / "out").exists())
            self.assertTrue((root / "stage" / "manifest.json").exists())
            client.fetch_chunk.side_effect = None

            async def stall(*args, **kwargs):
                await asyncio.Event().wait()

            client.fetch_manifest.side_effect = stall
            parsed.timeout = 0.01
            with (
                patch.object(download, "PeerClient", return_value=client),
                self.assertRaises(cli.RequestTimeout),
            ):
                await cli._fetch_transfer(parsed)
            self.assertFalse((root / "out").exists())

    async def test_serve_transfer_selected_snapshot_and_budget(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            source = root / "input"
            source.write_bytes(b"x" * 65536)
            args = diagnostics.PeerCLIParserTests().arguments(
                "--share-transfer", str(source), "--content-budget", "100000"
            )
            service = Mock()
            service.start = AsyncMock()
            service.serve_forever = AsyncMock()
            service.close = AsyncMock()
            service.address = ("127.0.0.1", 1234)
            service.project_id = "test"
            service.capabilities = {"status"}
            record = io.StringIO()
            with (
                patch.object(cli, "PeerService", return_value=service) as constructor,
                redirect_stdout(record),
            ):
                await cli._serve(cli._parser().parse_args(args))
            self.assertEqual(
                constructor.call_args.kwargs["transfer_store"].manifest.size, 65536
            )
            self.assertEqual(len(json.loads(record.getvalue())["manifest_sha256"]), 64)
            service.close.assert_awaited_once()
            for extra in (["--share-file", str(source)], ["--content-budget", "-1"]):
                with self.assertRaises(ValueError):
                    await cli._serve(cli._parser().parse_args([*args, *extra]))


class TransferMainTests(unittest.TestCase):
    def test_download_errors_are_static_and_publication_uncertainty_is_visible(self):
        args = diagnostics.PeerDiagnosticsTests().args()
        args[0] = "fetch-transfer"
        args += ["--manifest-sha256", "a" * 64, "--staging", "stage", "--output", "out"]
        for code, expected in (
            ("output_exists", "output_exists"),
            ("invalid_state", "invalid_configuration"),
            ("local_io", "local_io_error"),
            ("callback_failed", "callback_failed"),
            ("publication_failed", "publication_failed"),
        ):
            for attempted in (False, True):
                error = DownloadError(code, publication_attempted=attempted)
                error.__cause__ = OSError("secret local path")
                stderr = io.StringIO()
                with (
                    patch.object(cli, "_fetch_transfer", AsyncMock(side_effect=error)),
                    redirect_stderr(stderr),
                ):
                    self.assertEqual(cli.main(args), 1)
                report = {"error": expected}
                if attempted:
                    report["publication_attempted"] = True
                self.assertEqual(json.loads(stderr.getvalue()), report)

    def test_main_dispatch_and_static_errors(self):
        args = diagnostics.PeerDiagnosticsTests().args()
        args[0] = "fetch-transfer"
        args += ["--manifest-sha256", "a" * 64, "--staging", "stage", "--output", "out"]
        for error, code in (
            (None, 0),
            (OSError("secret"), 1),
            (KeyboardInterrupt(), 130),
        ):
            stderr = io.StringIO()
            with (
                patch.object(
                    cli, "_fetch_transfer", AsyncMock(side_effect=error)
                ) as fetch,
                redirect_stderr(stderr),
            ):
                self.assertEqual(cli.main(args), code)
                fetch.assert_awaited_once()
            if code == 1:
                self.assertEqual(
                    json.loads(stderr.getvalue()), {"error": "local_io_error"}
                )
