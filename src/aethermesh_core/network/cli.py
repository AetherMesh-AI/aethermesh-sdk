"""Foreground-only diagnostic peer command."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from .config import TLSIdentity
from .errors import PeerError
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
        )

    # Constructors are I/O-free. Preflight trust configuration before any explicit
    # identity creation, then fix the profile for the lifetime of the final service.
    service = configured_service()
    node_profile = None
    if arguments.node_identity is not None:
        node_profile = load_node_profile(
            arguments.node_identity,
            create=arguments.create_node_identity,
            include_hardware=arguments.share_hardware,
        )
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
        if node_profile is not None:
            readiness["node"] = node_profile.to_dict()
        print(json.dumps(readiness, sort_keys=True), flush=True)
        await service.serve_forever()
    finally:
        await service.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Start a foreground peer, without exposing configuration in failures."""
    arguments = _parser().parse_args(argv)
    try:
        asyncio.run(_serve(arguments))
    except KeyboardInterrupt:
        return 0
    except (PeerError, OSError, ValueError):
        print(
            "Unable to run peer: verify configuration and endpoint availability.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
