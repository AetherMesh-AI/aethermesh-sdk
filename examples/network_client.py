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

from aethermesh_core.network import (
    PeerClient,
    PeerEndpoint,
    PeerError,
    TLSIdentity,
    load_node_profile,
)


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
    result.add_argument(
        "--node-identity", type=Path, help="Read an existing local identity"
    )
    result.add_argument(
        "--share-hardware",
        action="store_true",
        help="Share current coarse hardware fields",
    )
    result.add_argument(
        "--expected-node-id", help="Bind the server pin to this node ID"
    )
    result.add_argument("--text", help="Request the peer's optional diagnostic echo")
    return result


async def run(args: argparse.Namespace) -> None:
    if args.share_hardware and args.node_identity is None:
        raise ValueError("--share-hardware requires --node-identity")
    profile = (
        load_node_profile(args.node_identity, include_hardware=args.share_hardware)
        if args.node_identity is not None
        else None
    )
    identity = TLSIdentity(args.certificate, args.private_key, args.trust_store)
    endpoint = PeerEndpoint(
        args.host,
        args.port,
        args.server_name,
        args.peer_fingerprint,
        expected_node_id=args.expected_node_id,
    )
    async with PeerClient(
        identity, project_id=args.project, node_profile=profile
    ) as client:
        await client.connect(endpoint)
        if args.node_identity is not None or args.expected_node_id is not None:
            info = client.peer_info
            assert info is not None
            print(
                json.dumps(
                    {
                        "peer_info": {
                            "fingerprint": info.fingerprint,
                            "protocol_version": info.protocol_version,
                            "profile": info.profile.to_dict() if info.profile else None,
                            "identity_pinned": info.identity_pinned,
                        }
                    },
                    sort_keys=True,
                )
            )
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
    except (PeerError, OSError, ValueError):
        print(
            "Peer request failed; verify TLS, node identity, endpoint, and project configuration.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
