"""Foreground-only diagnostic peer command."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from .client import PeerClient
from .config import PeerEndpoint, TLSIdentity, certificate_fingerprint
from .content import ContentRef, ContentStore
from .content_files import publish_new, read_selected, require_absent, selected_parent
from .errors import (
    AuthenticationError,
    ConnectionClosed,
    PeerError,
    ProtocolError,
    RemoteError,
    RequestTimeout,
)
from .profile import NodeProfile, load_node_profile
from .service import PeerService


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        # argparse normally repeats rejected values; a mistyped credential or
        # prompt must not be echoed to logs by argument validation.
        self.print_usage(sys.stderr)
        self.exit(2, "Invalid peer command arguments; use --help for usage.\n")


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(description="Run an opt-in AetherMesh diagnostic peer")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser(
        "serve", help="serve in the foreground until interrupted"
    )
    serve.add_argument("--certificate", required=True, type=Path)
    serve.add_argument("--private-key", required=True, type=Path)
    serve.add_argument("--trust-store", required=True, type=Path)
    serve.add_argument("--allow-peer", required=True, action="append", metavar="SHA256")
    serve.add_argument("--project", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--enable-echo", action="store_true")
    serve.add_argument(
        "--node-identity",
        type=Path,
        help="share the ID and name from an explicitly selected saved node identity",
    )
    serve.add_argument(
        "--create-node-identity",
        action="store_true",
        help="allow creation of the selected node identity if missing",
    )
    serve.add_argument(
        "--share-hardware",
        action="store_true",
        help="include coarse hardware metadata from the selected identity",
    )
    serve.add_argument(
        "--expect-peer-id",
        action="append",
        default=[],
        metavar="SHA256=NODE_ID",
        help="bind an allowed certificate fingerprint to an expected node ID",
    )
    serve.add_argument(
        "--share-file",
        type=Path,
        help="snapshot one selected regular file (at most 32 KiB)",
    )
    serve.add_argument(
        "--content-budget",
        type=int,
        help="explicit lifetime content bytes served (0..67108864); requires --share-file",
    )
    check = commands.add_parser("check", help="check an explicitly configured peer")
    fetch = commands.add_parser(
        "fetch", help="fetch verified small content to a new file"
    )
    for command in (check, fetch):
        for name in ("certificate", "private-key", "trust-store"):
            command.add_argument(f"--{name}", required=True, type=Path)
        command.add_argument("--peer-fingerprint", required=True)
        command.add_argument("--project", required=True)
        command.add_argument("--host", default="127.0.0.1")
        command.add_argument("--port", required=True, type=int)
        command.add_argument("--server-name", default="localhost")
        command.add_argument("--expected-node-id")
        command.add_argument(
            "--timeout",
            type=float,
            default=10.0,
            help="total connection/request deadline in seconds (0 < value <= 3600)",
        )
    fetch.add_argument("--sha256", required=True)
    fetch.add_argument("--size", type=int, required=True)
    fetch.add_argument(
        "--output",
        type=Path,
        required=True,
        help="new local file; existing paths and symlink components are rejected",
    )
    fingerprint = commands.add_parser(
        "fingerprint", help="hash a local certificate; this does not establish trust"
    )
    fingerprint.add_argument("--certificate", required=True, type=Path)
    return parser


async def _serve(arguments: argparse.Namespace) -> None:
    if (
        arguments.create_node_identity or arguments.share_hardware
    ) and arguments.node_identity is None:
        raise ValueError(
            "node identity creation and hardware sharing require a selected path"
        )
    expected_peer_ids: dict[str, str] = {}
    for binding in arguments.expect_peer_id:
        pin, separator, node_id = binding.partition("=")
        if not separator or pin in expected_peer_ids:
            raise ValueError("invalid or duplicate peer node identity binding")
        expected_peer_ids[pin] = node_id

    content_store = None

    def configured_service(node_profile: NodeProfile | None = None) -> PeerService:
        return PeerService(
            TLSIdentity(
                arguments.certificate, arguments.private_key, arguments.trust_store
            ),
            project_id=arguments.project,
            allowed_peers=frozenset(arguments.allow_peer),
            capabilities=frozenset({"echo"}) if arguments.enable_echo else frozenset(),
            expected_peer_ids=expected_peer_ids,
            node_profile=node_profile,
            content_store=content_store,
        )

    # Constructors are I/O-free. Preflight trust configuration before any explicit
    # identity creation, then fix the profile for the lifetime of the final service.
    service = configured_service()
    if (arguments.share_file is None) != (arguments.content_budget is None):
        raise ValueError("sharing requires both a selected file and a serving budget")
    if arguments.share_file is not None:
        # Validate the budget before reading a selected file.
        ContentStore(arguments.project, (), max_served_bytes=arguments.content_budget)
        content_store = ContentStore(
            arguments.project,
            (read_selected(arguments.share_file),),
            max_served_bytes=arguments.content_budget,
        )
    node_profile = None
    if arguments.node_identity is not None:
        node_profile = load_node_profile(
            arguments.node_identity,
            create=arguments.create_node_identity,
            include_hardware=arguments.share_hardware,
        )
    if node_profile is not None or content_store is not None:
        service = configured_service(node_profile)
    try:
        await service.start(arguments.host, arguments.port)
        host, port = service.address
        readiness = {
            "host": host,
            "port": port,
            "project": service.project_id,
            "capabilities": sorted(service.capabilities),
        }
        if content_store is not None:
            readiness["content"] = [ref.to_dict() for ref in content_store.references]
            readiness["content_budget_bytes"] = arguments.content_budget
        if node_profile is not None:
            readiness["node"] = node_profile.to_dict()
        print(json.dumps(readiness, sort_keys=True), flush=True)
        await service.serve_forever()
    finally:
        await service.close()


def _connection_inputs(
    arguments: argparse.Namespace,
) -> tuple[TLSIdentity, PeerEndpoint, float]:
    timeout = arguments.timeout
    if not math.isfinite(timeout) or not 0 < timeout <= 3600:
        raise ValueError("invalid check deadline")
    identity = TLSIdentity(
        arguments.certificate, arguments.private_key, arguments.trust_store
    )
    endpoint = PeerEndpoint(
        arguments.host,
        arguments.port,
        arguments.server_name,
        arguments.peer_fingerprint,
        expected_node_id=arguments.expected_node_id,
    )
    return identity, endpoint, timeout


async def _check(arguments: argparse.Namespace) -> None:
    identity, endpoint, timeout = _connection_inputs(arguments)
    async with PeerClient(identity, project_id=arguments.project) as client:
        async with asyncio.timeout(timeout):
            await client.connect(endpoint)
            # Freeze negotiated facts before awaiting a reply: the receive task
            # may observe EOF immediately after delivering that reply.
            version = client.protocol_version
            capabilities = client.capabilities
            project = client.project_id
            status = await client.request("status", timeout=timeout)
            # V2 status is checked by PeerClient. Legacy V1 needs an exact check
            # here before diagnostics claim agreement with negotiated facts.
            if version == 1 and (
                type(status.get("protocol_version")) is not int
                or status != {"protocol_version": 1, "capabilities": list(capabilities)}
            ):
                raise ProtocolError("invalid legacy status")
            report = {
                "project": project,
                "fingerprint": endpoint.fingerprint,
                "protocol_version": version,
                "capabilities": list(capabilities),
                "status": "ok",
            }
    print(json.dumps(report, sort_keys=True))


async def _fetch(arguments: argparse.Namespace) -> None:
    identity, endpoint, timeout = _connection_inputs(arguments)
    reference = ContentRef(arguments.project, arguments.sha256, arguments.size)
    # Pin the chosen physical parent directory before any network I/O. Do not
    # create even a temporary output until the complete response is verified.
    with selected_parent(arguments.output) as (directory, name):
        require_absent(directory, name)
        async with PeerClient(identity, project_id=arguments.project) as client:
            async with asyncio.timeout(timeout):
                await client.connect(endpoint)
                data = await client.fetch_content(reference, timeout=timeout)
        publish_new(directory, name, data)
    print(
        json.dumps(
            {
                **reference.to_dict(),
                "saved": True,
                "integrity_verified": True,
                "publisher_authorized": False,
            },
            sort_keys=True,
        )
    )


def _failure(error: Exception, *, content: bool = False) -> str:
    # Never include exception strings, supplied paths/arguments or peer text.
    if isinstance(error, FileExistsError):
        return "output_exists"
    if isinstance(error, AuthenticationError):
        return "authentication_failed"
    if isinstance(error, (RequestTimeout, TimeoutError)):
        return "timeout"
    if isinstance(error, ProtocolError):
        return "protocol_error"
    if isinstance(error, ConnectionClosed):
        return "connection_closed"
    if isinstance(error, RemoteError):
        return "remote_error"
    if isinstance(error, ValueError):
        return "invalid_configuration"
    if content and isinstance(error, OSError):
        return "local_io_error"
    return "peer_unavailable"


def main(argv: Sequence[str] | None = None) -> int:
    """Start a foreground peer, without exposing configuration in failures."""
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "serve":
            asyncio.run(_serve(arguments))
        elif arguments.command == "check":
            asyncio.run(_check(arguments))
        elif arguments.command == "fetch":
            asyncio.run(_fetch(arguments))
        else:
            print(
                json.dumps(
                    {
                        "sha256": certificate_fingerprint(arguments.certificate),
                        "trust_established": False,
                    },
                    sort_keys=True,
                )
            )
    except KeyboardInterrupt:
        return 0 if arguments.command == "serve" else 130
    except (PeerError, OSError, ValueError) as exc:
        if arguments.command == "serve":
            print(
                "Unable to run peer: verify configuration and endpoint availability.",
                file=sys.stderr,
            )
        else:
            print(
                json.dumps(
                    {"error": _failure(exc, content=arguments.command == "fetch")},
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
