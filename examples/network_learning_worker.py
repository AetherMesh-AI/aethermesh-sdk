"""Explicit loopback-only evaluator process for the governed-learning example.

The parent supplies operator-managed TLS paths or explicitly generated ephemeral
test credentials. EOF on stdin closes the service and prints the consumed budget.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from aethermesh_core.network import (
    EvaluationBudget,
    Limits,
    PeerService,
    RouterEvaluator,
    TLSIdentity,
)


async def run(args: argparse.Namespace) -> None:
    evaluator = RouterEvaluator(EvaluationBudget(max_rows=2, max_total_rows=48))
    service = PeerService(
        TLSIdentity(args.certificate, args.private_key, args.trust_store),
        project_id=args.project,
        allowed_peers=frozenset({args.allow_peer}),
        limits=Limits(max_connections=2, max_inflight=1, max_requests=64),
        router_evaluator=evaluator,
    )
    await service.start("127.0.0.1", 0)
    try:
        print(
            json.dumps(
                {
                    "host": service.address[0],
                    "port": service.address[1],
                    "pid": os.getpid(),
                    "capabilities": sorted(service.capabilities),
                }
            ),
            flush=True,
        )
        # A local parent-owned control pipe is not a network administration API.
        await asyncio.to_thread(sys.stdin.buffer.read, 1)
    finally:
        await service.close()
    print(
        json.dumps({"rows_used": evaluator.rows_used, "pid": os.getpid()}), flush=True
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--private-key", type=Path, required=True)
    parser.add_argument("--trust-store", type=Path, required=True)
    parser.add_argument("--allow-peer", required=True)
    parser.add_argument("--project", required=True)
    asyncio.run(run(parser.parse_args()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
