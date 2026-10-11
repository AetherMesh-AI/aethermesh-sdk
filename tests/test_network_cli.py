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
                    share_file=None,
                    content_budget=None,
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


class PeerDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    def args(self, *extra: str) -> list[str]:
        return [
            "check",
            "--certificate",
            "private-cert.pem",
            "--private-key",
            "private-key.pem",
            "--trust-store",
            "private-ca.pem",
            "--peer-fingerprint",
            "a" * 64,
            "--project",
            "test",
            "--port",
            "1234",
            *extra,
        ]

    async def test_check_reports_only_negotiated_facts(self) -> None:
        for version in (1, 2):
            client = Mock(
                project_id="test", protocol_version=version, capabilities=("status",)
            )
            client.connect = AsyncMock()
            client.request = AsyncMock(
                return_value={"protocol_version": version, "capabilities": ["status"]}
            )
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            output = io.StringIO()
            with (
                patch.object(cli, "PeerClient", return_value=client) as constructor,
                redirect_stdout(output),
            ):
                await cli._check(
                    cli._parser().parse_args(
                        self.args("--expected-node-id", "known-peer")
                    )
                )
            self.assertEqual(
                json.loads(output.getvalue()),
                {
                    "project": "test",
                    "fingerprint": "a" * 64,
                    "protocol_version": version,
                    "capabilities": ["status"],
                    "status": "ok",
                },
            )
            self.assertEqual(constructor.call_args.kwargs, {"project_id": "test"})
            self.assertEqual(
                client.connect.call_args.args[0].expected_node_id, "known-peer"
            )
            client.request.assert_awaited_once_with("status", timeout=10.0)
            client.__aexit__.assert_awaited_once()

    async def test_invalid_deadlines_make_no_connection(self) -> None:
        for timeout in ("nan", "inf", "0", "-1", "3601"):
            with (
                patch.object(cli, "PeerClient") as constructor,
                self.assertRaises(ValueError),
            ):
                await cli._check(
                    cli._parser().parse_args(self.args("--timeout", timeout))
                )
            constructor.assert_not_called()

    async def test_legacy_status_lies_fail_without_success_output(self) -> None:
        client = Mock(project_id="test", protocol_version=1, capabilities=("status",))
        client.connect = AsyncMock()
        client.request = AsyncMock(
            return_value={"protocol_version": 2, "capabilities": ["status"]}
        )
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        output = io.StringIO()
        with (
            patch.object(cli, "PeerClient", return_value=client),
            redirect_stdout(output),
            self.assertRaises(cli.ProtocolError),
        ):
            await cli._check(cli._parser().parse_args(self.args()))
        self.assertEqual(output.getvalue(), "")
        client.__aexit__.assert_awaited_once()

    async def test_deadline_cancels_work_and_closes_client(self) -> None:
        for phase in ("connect", "request"):
            entered = asyncio.Event()

            async def stalled(
                *args: object, event: asyncio.Event = entered, **kwargs: object
            ) -> None:
                event.set()
                await asyncio.Event().wait()

            client = Mock()
            client.connect = AsyncMock()
            client.request = AsyncMock()
            getattr(client, phase).side_effect = stalled
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            with (
                patch.object(cli, "PeerClient", return_value=client),
                self.assertRaises(TimeoutError),
            ):
                await cli._check(
                    cli._parser().parse_args(self.args("--timeout", "0.01"))
                )
            self.assertTrue(entered.is_set())
            self.assertEqual(client.request.await_count, int(phase == "request"))
            client.__aexit__.assert_awaited_once()


class PeerDiagnosticsMainTests(unittest.TestCase):
    def test_diagnostic_failures_are_static_and_machine_readable(self) -> None:
        for error, expected in (
            (cli.AuthenticationError("private secret"), "authentication_failed"),
            (cli.RequestTimeout("private secret"), "timeout"),
            (TimeoutError("private secret"), "timeout"),
            (cli.ProtocolError("private secret"), "protocol_error"),
            (cli.ConnectionClosed("private secret"), "connection_closed"),
            (cli.RemoteError("private", "secret"), "remote_error"),
            (ValueError("private secret"), "invalid_configuration"),
            (OSError("private secret"), "peer_unavailable"),
        ):
            stdout, stderr = io.StringIO(), io.StringIO()
            with (
                patch.object(cli, "_check", AsyncMock(side_effect=error)),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                self.assertEqual(cli.main(PeerDiagnosticsTests().args()), 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(json.loads(stderr.getvalue()), {"error": expected})

    def test_fingerprint_is_read_only_and_establishes_no_trust(self) -> None:
        output = io.StringIO()
        with (
            patch.object(
                cli, "certificate_fingerprint", return_value="a" * 64
            ) as fingerprint,
            redirect_stdout(output),
        ):
            self.assertEqual(
                cli.main(["fingerprint", "--certificate", "selected.pem"]), 0
            )
        fingerprint.assert_called_once_with(Path("selected.pem"))
        self.assertEqual(
            json.loads(output.getvalue()),
            {"sha256": "a" * 64, "trust_established": False},
        )
        with patch.object(
            cli, "certificate_fingerprint", side_effect=KeyboardInterrupt
        ):
            self.assertEqual(
                cli.main(["fingerprint", "--certificate", "selected.pem"]), 130
            )
        with patch.object(cli, "_check", AsyncMock()):
            self.assertEqual(cli.main(PeerDiagnosticsTests().args()), 0)

    def test_check_usage_never_repeats_bad_values(self) -> None:
        for extra in (
            ("--timeout", "private-secret"),
            ("--port", "private-secret"),
            ("--unknown-secret",),
        ):
            stderr = io.StringIO()
            with redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
                cli.main(PeerDiagnosticsTests().args(*extra))
            self.assertEqual(caught.exception.code, 2)
            self.assertNotIn("secret", stderr.getvalue())


class PeerStatusCloseTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from tests.network_test_support import TestPKI

        cls.directory = tempfile.TemporaryDirectory(prefix="diagnostic-close-tls-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.pki = TestPKI.create(Path(cls.directory.name))

    async def test_status_then_immediate_tls_close(self) -> None:
        from aethermesh_core.network.protocol import (
            close_writer,
            read_frame,
            write_frame,
        )

        for version, malformed in ((1, False), (2, False), (1, True)):
            completed = asyncio.Event()

            async def respond(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
                selected: int = version,
                bad: bool = malformed,
                done: asyncio.Event = completed,
            ) -> None:
                try:
                    await read_frame(reader)
                    welcome = {
                        "type": "welcome",
                        "project": "test",
                        "version": selected,
                        "capabilities": ["status"],
                    }
                    if selected == 2:
                        welcome["node"] = None
                    await write_frame(writer, welcome)
                    if selected == 2:
                        await read_frame(reader)
                        await write_frame(writer, {"type": "ready"})
                    request = await read_frame(reader)
                    result = {
                        "protocol_version": True if bad else selected,
                        "capabilities": ["status"],
                    }
                    if selected == 2:
                        result["node"] = None
                    await write_frame(
                        writer,
                        {"type": "result", "id": request["id"], "result": result},
                    )
                finally:
                    await close_writer(writer)
                    done.set()

            server = await asyncio.start_server(
                respond, "127.0.0.1", 0, ssl=self.pki.server.identity().server_context()
            )
            try:
                args = cli._parser().parse_args(
                    PeerDiagnosticsTests().args(
                        "--certificate",
                        str(self.pki.client.certificate),
                        "--private-key",
                        str(self.pki.client.private_key),
                        "--trust-store",
                        str(self.pki.ca),
                        "--peer-fingerprint",
                        cli.certificate_fingerprint(self.pki.server.certificate),
                        "--port",
                        str(server.sockets[0].getsockname()[1]),
                    )
                )
                output = io.StringIO()
                with redirect_stdout(output):
                    if malformed:
                        with self.assertRaises(cli.ProtocolError):
                            await cli._check(args)
                    else:
                        await cli._check(args)
                if malformed:
                    self.assertEqual(output.getvalue(), "")
                else:
                    report = json.loads(output.getvalue())
                    self.assertEqual(report["protocol_version"], version)
                    self.assertEqual(report["capabilities"], ["status"])
                    self.assertEqual(report["status"], "ok")
            finally:
                server.close()
                await asyncio.wait_for(completed.wait(), 5)
                await asyncio.wait_for(server.wait_closed(), 5)

    async def test_eof_clearing_live_properties_cannot_change_report_or_skip_validation(
        self,
    ) -> None:
        for malformed in (False, True):
            client = Mock(
                project_id="test", protocol_version=1, capabilities=("status",)
            )
            client.connect = AsyncMock()

            async def delivered(
                *args: object,
                bad: bool = malformed,
                session: Mock = client,
                **kwargs: object,
            ) -> dict[str, object]:
                session.protocol_version = None
                session.capabilities = ()
                return {
                    "protocol_version": True if bad else 1,
                    "capabilities": ["status"],
                }

            client.request = AsyncMock(side_effect=delivered)
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=False)
            output = io.StringIO()
            with (
                patch.object(cli, "PeerClient", return_value=client),
                redirect_stdout(output),
            ):
                if malformed:
                    with self.assertRaises(cli.ProtocolError):
                        await cli._check(
                            cli._parser().parse_args(PeerDiagnosticsTests().args())
                        )
                else:
                    await cli._check(
                        cli._parser().parse_args(PeerDiagnosticsTests().args())
                    )
            if malformed:
                self.assertEqual(output.getvalue(), "")
            else:
                self.assertEqual(json.loads(output.getvalue())["protocol_version"], 1)
                self.assertEqual(
                    json.loads(output.getvalue())["capabilities"], ["status"]
                )
