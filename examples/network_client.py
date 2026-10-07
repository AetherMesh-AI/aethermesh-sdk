"""Connect an installed SDK to an explicitly configured diagnostic peer.

Run ``python examples/network_client.py --help`` for required identity and pin
arguments. This example never generates identities or starts a service.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from aethermesh_core.network import PeerClient, PeerEndpoint, PeerError, TLSIdentity


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--certificate", required=True, type=Path)
    result.add_argument("--private-key", required=True, type=Path)
    result.add_argument("--trust-store", required=True, type=Path)
    result.add_argument("--peer-fingerprint", required=True)
    result.add_argument("--project", required=True)
    result.add_argument("--host", default="127.0.0.1")
    result.add_argument("--port", type=int, required=True)
    result.add_argument("--server-name", default="localhost")
    result.add_argument("--text", help="Request the peer's optional diagnostic echo")
    return result


async def run(args: argparse.Namespace) -> None:
    identity = TLSIdentity(args.certificate, args.private_key, args.trust_store)
    endpoint = PeerEndpoint(
        args.host, args.port, args.server_name, args.peer_fingerprint
    )
    async with PeerClient(identity, project_id=args.project) as client:
        await client.connect(endpoint)
        print(json.dumps(await client.request("status"), sort_keys=True))
        if args.text is not None:
            print(
                json.dumps(
                    await client.request("echo", {"text": args.text}), sort_keys=True
                )
            )


def main() -> int:
    args = parser().parse_args()
    try:
        asyncio.run(run(args))
    except (PeerError, OSError, ValueError) as exc:
        print(f"Peer request failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
