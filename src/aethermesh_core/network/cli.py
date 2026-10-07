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
    return parser


async def _serve(arguments: argparse.Namespace) -> None:
    service = PeerService(
        TLSIdentity(
            arguments.certificate, arguments.private_key, arguments.trust_store
        ),
        project_id=arguments.project,
        allowed_peers=frozenset(arguments.allow_peer),
        capabilities=frozenset({"echo"}) if arguments.enable_echo else frozenset(),
    )
    try:
        await service.start(arguments.host, arguments.port)
        host, port = service.address
        print(
            json.dumps(
                {
                    "host": host,
                    "port": port,
                    "project": service.project_id,
                    "capabilities": sorted(service.capabilities),
                },
                sort_keys=True,
            ),
            flush=True,
        )
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
