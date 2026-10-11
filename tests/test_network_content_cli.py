"""Explicit selected-file CLI sharing and fail-closed fetch output."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from aethermesh_core.network import cli
from tests import test_network_cli as diagnostics_tests


class ContentCLIParserTests(unittest.TestCase):
    def test_fetch_requires_reference_output_and_sanitizes_failures(self) -> None:
        args = diagnostics_tests.PeerDiagnosticsTests().args()
        args[0] = "fetch"
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
            cli.main(args)
        self.assertEqual(caught.exception.code, 2)
        self.assertNotIn("private-cert.pem", stderr.getvalue())
        args.extend(["--sha256", "a" * 64, "--size", "1", "--output", "chosen"])
        for failure, expected in ((None, 0), (FileExistsError("private path"), 1)):
            stderr = io.StringIO()
            with (
                patch.object(cli, "_fetch", AsyncMock(side_effect=failure)) as fetch,
                redirect_stderr(stderr),
            ):
                self.assertEqual(cli.main(args), expected)
                fetch.assert_awaited_once()
            if failure:
                self.assertEqual(
                    json.loads(stderr.getvalue()), {"error": "output_exists"}
                )


class ContentCLIAsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory(prefix="content-cli-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()

    def fetch_args(self, output: Path) -> list[str]:
        args = diagnostics_tests.PeerDiagnosticsTests().args()
        args[0] = "fetch"
        return [
            *args,
            "--sha256",
            hashlib.sha256(b"verified").hexdigest(),
            "--size",
            "8",
            "--output",
            str(output),
        ]

    async def test_fetch_publishes_only_complete_verified_bytes(self) -> None:
        client = Mock()
        client.connect = AsyncMock()
        client.fetch_content = AsyncMock(return_value=b"verified")
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        output = self.root / "new"
        record = io.StringIO()
        with (
            patch.object(cli, "PeerClient", return_value=client),
            redirect_stdout(record),
        ):
            await cli._fetch(cli._parser().parse_args(self.fetch_args(output)))
        self.assertEqual(output.read_bytes(), b"verified")
        self.assertEqual(
            json.loads(record.getvalue()),
            {
                "project": "test",
                "sha256": hashlib.sha256(b"verified").hexdigest(),
                "size": 8,
                "saved": True,
                "integrity_verified": True,
                "publisher_authorized": False,
            },
        )
        client.__aexit__.assert_awaited_once()
        self.assertEqual(client.fetch_content.call_args.kwargs, {"timeout": 10.0})

    async def test_failed_or_cancelled_fetch_never_creates_output(self) -> None:
        for failure in (
            cli.ProtocolError("corrupt"),
            cli.RequestTimeout("late"),
            asyncio.CancelledError(),
        ):
            output = self.root / "new"
            client = Mock()
            client.connect = AsyncMock()
            client.fetch_content = AsyncMock(side_effect=failure)
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            with (
                patch.object(cli, "PeerClient", return_value=client),
                self.assertRaises(type(failure)),
            ):
                await cli._fetch(cli._parser().parse_args(self.fetch_args(output)))
            self.assertEqual(list(self.root.iterdir()), [])
            client.__aexit__.assert_awaited_once()

    async def test_existing_and_unsafe_outputs_fail_before_connect(self) -> None:
        output = self.root / "existing"
        output.write_bytes(b"keep")
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        for path in (output, alias / "new", self.root / ".." / "new"):
            with (
                patch.object(cli, "PeerClient") as client,
                self.assertRaises((OSError, ValueError)),
            ):
                await cli._fetch(cli._parser().parse_args(self.fetch_args(path)))
            client.assert_not_called()
        self.assertEqual(output.read_bytes(), b"keep")

    async def test_serve_requires_explicit_budget_and_reads_only_selected_file(
        self,
    ) -> None:
        selected = self.root / "selected"
        selected.write_bytes(b"shared")
        for extra in (
            ("--share-file", str(selected)),
            ("--content-budget", "100"),
            ("--share-file", str(selected), "--content-budget", "-1"),
        ):
            args = cli._parser().parse_args(
                diagnostics_tests.PeerCLIParserTests().arguments(*extra)
            )
            with (
                patch.object(cli, "read_selected") as read,
                self.assertRaises(ValueError),
            ):
                await cli._serve(args)
            read.assert_not_called()
        service = Mock(
            project_id="test",
            capabilities=frozenset({"status", "content.get.v1"}),
            address=("127.0.0.1", 4321),
        )
        service.start = AsyncMock()
        service.close = AsyncMock()
        service.serve_forever = AsyncMock()
        record = io.StringIO()
        args = cli._parser().parse_args(
            diagnostics_tests.PeerCLIParserTests().arguments(
                "--share-file", str(selected), "--content-budget", "6"
            )
        )
        with (
            patch.object(cli, "PeerService", return_value=service) as constructor,
            redirect_stdout(record),
        ):
            await cli._serve(args)
        store = constructor.call_args.kwargs["content_store"]
        self.assertEqual(
            json.loads(record.getvalue())["content"], [store.references[0].to_dict()]
        )
        self.assertNotIn(str(selected), record.getvalue())
        self.assertEqual(selected.read_bytes(), b"shared")
        service.close.assert_awaited_once()
