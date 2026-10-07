"""The peer CLI stays foreground-only and reports sanitized failures."""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from aethermesh_core.network import cli
from aethermesh_core.network.errors import AuthenticationError
from aethermesh_core.network.profile import NodeProfile


class PeerCLIParserTests(unittest.TestCase):
    def arguments(self, *extra: str) -> list[str]:
        return [
            "serve",
            "--certificate",
            "cert.pem",
            "--private-key",
            "private.pem",
            "--trust-store",
            "ca.pem",
            "--allow-peer",
            "a" * 64,
            "--project",
            "test",
            *extra,
        ]

    def test_parser_requires_explicit_identity_and_trust(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            cli._parser().parse_args(["serve"])
        args = cli._parser().parse_args(
            self.arguments("--allow-peer", "b" * 64, "--enable-echo")
        )
        self.assertEqual(args.allow_peer, ["a" * 64, "b" * 64])
        self.assertEqual(args.certificate, Path("cert.pem"))
        self.assertEqual(args.private_key, Path("private.pem"))
        self.assertEqual(args.trust_store, Path("ca.pem"))
        self.assertTrue(args.enable_echo)
        self.assertEqual((args.host, args.port), ("127.0.0.1", 0))

    def test_parser_does_not_echo_invalid_values(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as result:
            cli.main(self.arguments("--port", "private-secret"))
        self.assertEqual(result.exception.code, 2)
        self.assertNotIn("private-secret", stderr.getvalue())
        self.assertIn("Invalid peer command arguments", stderr.getvalue())

    def test_main_success_interrupt_and_sanitized_failures(self) -> None:
        for failure, result in (
            (None, 0),
            (KeyboardInterrupt(), 0),
            (AuthenticationError("private-key.pem"), 1),
            (OSError("private prompt"), 1),
            (ValueError("secret"), 1),
        ):
            with self.subTest(failure=failure):
                stderr = io.StringIO()
                serve = AsyncMock(side_effect=failure)
                with patch.object(cli, "_serve", serve), redirect_stderr(stderr):
                    self.assertEqual(cli.main(self.arguments()), result)
                serve.assert_awaited_once()
                self.assertNotIn("private", stderr.getvalue())
                self.assertNotIn("secret", stderr.getvalue())
                if result:
                    self.assertIn("verify configuration", stderr.getvalue())
                else:
                    self.assertEqual(stderr.getvalue(), "")


class PeerCLIAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_foreground_readiness_and_finally_cleanup(self) -> None:
        for enable_echo in (False, True):
            with self.subTest(enable_echo=enable_echo):
                service = Mock()
                service.start = AsyncMock()
                service.close = AsyncMock()
                service.serve_forever = AsyncMock()
                service.address = ("127.0.0.1", 4321)
                service.project_id = "test"
                service.capabilities = frozenset(
                    {"status", "echo"} if enable_echo else {"status"}
                )
                args = argparse.Namespace(
                    certificate=Path("cert.pem"),
                    private_key=Path("key.pem"),
                    trust_store=Path("ca.pem"),
                    project="test",
                    allow_peer=["a" * 64],
                    enable_echo=enable_echo,
                    host="127.0.0.1",
                    port=0,
                    node_identity=None,
                    create_node_identity=False,
                    share_hardware=False,
                    expect_peer_id=[],
                )
                output = io.StringIO()
                with (
                    patch.object(
                        cli, "PeerService", return_value=service
                    ) as constructor,
                    redirect_stdout(output),
                ):
                    await cli._serve(args)
                self.assertEqual(
                    json.loads(output.getvalue()),
                    {
                        "host": "127.0.0.1",
                        "port": 4321,
                        "project": "test",
                        "capabilities": sorted(service.capabilities),
                    },
                )
                self.assertEqual(
                    constructor.call_args.kwargs["capabilities"],
                    frozenset({"echo"}) if enable_echo else frozenset(),
                )
                service.start.assert_awaited_once_with("127.0.0.1", 0)
                service.serve_forever.assert_awaited_once_with()
                service.close.assert_awaited_once_with()
                service.start.side_effect = OSError("inaccessible certificate")
                service.close.reset_mock()
                with (
                    patch.object(cli, "PeerService", return_value=service),
                    self.assertRaises(OSError),
                ):
                    await cli._serve(args)
                service.close.assert_awaited_once_with()
                service.start.side_effect = None
                service.serve_forever.side_effect = asyncio.CancelledError
                service.close.reset_mock()
                with (
                    patch.object(cli, "PeerService", return_value=service),
                    redirect_stdout(io.StringIO()),
                    self.assertRaises(asyncio.CancelledError),
                ):
                    await cli._serve(args)
                service.close.assert_awaited_once_with()

    async def test_explicit_profile_flags_load_selected_identity_and_readiness(
        self,
    ) -> None:
        for create, hardware in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            with self.subTest(create=create, hardware=hardware):
                extras = [
                    "--node-identity",
                    "selected.json",
                    "--expect-peer-id",
                    f"{'a' * 64}=client-node",
                ]
                if create:
                    extras.append("--create-node-identity")
                if hardware:
                    extras.append("--share-hardware")
                args = cli._parser().parse_args(PeerCLIParserTests().arguments(*extras))
                profile = NodeProfile("existing-node", "saved-name")
                service = Mock(
                    project_id="test",
                    capabilities=frozenset({"status"}),
                    address=("127.0.0.1", 1234),
                )
                service.start = AsyncMock()
                service.close = AsyncMock()
                service.serve_forever = AsyncMock()
                output = io.StringIO()
                with (
                    patch.object(
                        cli, "load_node_profile", return_value=profile
                    ) as load,
                    patch.object(
                        cli, "PeerService", return_value=service
                    ) as constructor,
                    redirect_stdout(output),
                ):
                    await cli._serve(args)
                load.assert_called_once_with(
                    Path("selected.json"), create=create, include_hardware=hardware
                )
                self.assertEqual(
                    constructor.call_args.kwargs["expected_peer_ids"],
                    {"a" * 64: "client-node"},
                )
                self.assertIs(constructor.call_args.kwargs["node_profile"], profile)
                self.assertEqual(
                    json.loads(output.getvalue())["node"], profile.to_dict()
                )
                service.close.assert_awaited_once_with()

    async def test_invalid_identity_options_do_not_load_or_create_files(self) -> None:
        for extras in (
            ("--create-node-identity",),
            ("--share-hardware",),
            ("--expect-peer-id", "private-invalid"),
            (
                "--expect-peer-id",
                "a" * 64 + "=one",
                "--expect-peer-id",
                "a" * 64 + "=two",
            ),
            ("--expect-peer-id", "b" * 64 + "=unallowed"),
            ("--expect-peer-id", "a" * 64 + "="),
            ("--expect-peer-id", "a" * 64 + "=private invalid"),
            ("--expect-peer-id", "private-invalid=valid"),
        ):
            with self.subTest(extras=extras):
                args = cli._parser().parse_args(PeerCLIParserTests().arguments(*extras))
                with (
                    patch.object(cli, "load_node_profile") as load,
                    patch("asyncio.start_server") as start,
                    self.assertRaises(ValueError),
                ):
                    await cli._serve(args)
                load.assert_not_called()
                start.assert_not_called()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing" / "identity.json"
            args = cli._parser().parse_args(
                PeerCLIParserTests().arguments(
                    "--node-identity",
                    str(path),
                    "--create-node-identity",
                    "--expect-peer-id",
                    "b" * 64 + "=unallowed",
                )
            )
            with self.assertRaises(ValueError):
                await cli._serve(args)
            self.assertFalse(path.parent.exists())

    async def test_selected_missing_identity_never_creates_without_explicit_permission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing" / "identity.json"
            args = cli._parser().parse_args(
                PeerCLIParserTests().arguments("--node-identity", str(path))
            )
            with self.assertRaises((OSError, ValueError)):
                await cli._serve(args)
            self.assertFalse(path.parent.exists())
