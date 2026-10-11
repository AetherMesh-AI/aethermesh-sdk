"""Explicit loopback content host/client for an installed aethermesh package.

Requires operator-supplied TLS files and pins. No credentials are generated.
Run with --help. Content is never written, extracted or executed by the client.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from aethermesh_core.network import (
    MAX_CONTENT_BYTES,
    ContentRef,
    ContentStore,
    PeerClient,
    PeerEndpoint,
    PeerService,
    TLSIdentity,
)


async def run(args: argparse.Namespace) -> None:
    identity = TLSIdentity(args.certificate, args.private_key, args.trust_store)
    if args.mode == "serve":
        # Local operator selects one file. Bounded read detects oversized input
        # without reading an arbitrarily large file into memory.
        with args.file.open("rb") as stream:
            data = stream.read(MAX_CONTENT_BYTES + 1)
        store = ContentStore(args.project, (data,), max_served_bytes=args.budget)
        service = PeerService(
            identity,
            project_id=args.project,
            allowed_peers=frozenset({args.peer_fingerprint}),
            content_store=store,
        )
        async with service:
            print(
                json.dumps(
                    {
                        "host": service.address[0],
                        "port": service.address[1],
                        "reference": store.references[0].to_dict(),
                    }
                ),
                flush=True,
            )
            await service.serve_forever()
    else:
        ref = ContentRef(args.project, args.sha256, args.size)
        async with PeerClient(identity, project_id=args.project) as client:
            await client.connect(
                PeerEndpoint("127.0.0.1", args.port, "localhost", args.peer_fingerprint)
            )
            data = await client.fetch_content(ref, timeout=args.timeout)
            print(
                json.dumps(
                    {
                        "project": args.project,
                        "size": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                        "integrity_verified": True,
                        "publisher_authorized": False,
                    }
                )
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("serve", "fetch"))
    for name in ("certificate", "private-key", "trust-store"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--peer-fingerprint", required=True)
    parser.add_argument("--file", type=Path)
    parser.add_argument("--budget", type=int, default=1048576)
    parser.add_argument("--port", type=int)
    parser.add_argument("--sha256")
    parser.add_argument("--size", type=int)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if args.mode == "serve" and args.file is None:
        parser.error("serve requires --file")
    if args.mode == "fetch" and None in (args.port, args.sha256, args.size):
        parser.error("fetch requires --port, --sha256 and --size")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
