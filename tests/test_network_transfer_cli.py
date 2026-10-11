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

from aethermesh_core.network import TransferStore, cli
from tests import test_network_cli as diagnostics


class TransferCLITests(unittest.IsolatedAsyncioTestCase):
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
            client.fetch_manifest = AsyncMock(return_value=manifest)
            client.fetch_chunk = AsyncMock(return_value=b"data")
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            record = io.StringIO()
            with (
                patch.object(cli, "PeerClient", return_value=client),
                redirect_stdout(record),
            ):
                await cli._fetch_transfer(parsed)
            self.assertEqual((root / "out").read_bytes(), b"data")
            self.assertEqual(json.loads(record.getvalue())["chunks_fetched"], 1)
            (root / "out").unlink()
            parsed.resume = True
            record = io.StringIO()
            with (
                patch.object(cli, "PeerClient", return_value=client),
                redirect_stdout(record),
            ):
                await cli._fetch_transfer(parsed)
            self.assertEqual(json.loads(record.getvalue())["chunks_fetched"], 0)
            with (
                patch.object(cli, "PeerClient") as constructed,
                self.assertRaises(FileExistsError),
            ):
                await cli._fetch_transfer(parsed)
            constructed.assert_not_called()
            (root / "out").unlink()
            for failure in (
                cli.ProtocolError("bad"),
                asyncio.CancelledError(),
                cli.RequestTimeout("late"),
            ):
                client.fetch_manifest.side_effect = failure
                with (
                    patch.object(cli, "PeerClient", return_value=client),
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
                patch.object(cli, "PeerClient", return_value=client),
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
                patch.object(cli, "PeerClient", return_value=client),
                self.assertRaises(TimeoutError),
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
