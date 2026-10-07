"""Run the bounded, local, multi-process governed-router learning pilot.

Copy this file and both network_learning_{trainer,worker}.py helpers together;
only an installed aethermesh package and Python's standard library are needed.
Use --ephemeral-test-pki explicitly for disposable development certificates
(OpenSSL is required), or --identity-dir with operator-provided ca.pem and
coordinator/evaluator-a/evaluator-b .pem/.key files. All listeners use loopback.
This demonstration neither proves independent operators nor sandboxes processes.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from aethermesh_core.network import (
    PeerClient,
    PeerEndpoint,
    TLSIdentity,
    certificate_fingerprint,
)
from aethermesh_core.network.governance import (
    EvaluationSuite,
    GovernancePolicy,
    GovernedRouter,
    TrainingProvenance,
)

PROJECT = "governed-learning-pilot"
TRAINING = [[-2, -2, 0], [2, 2, 1], [-2, 0, 0], [2, 0, 1], [0, -2, 0], [0, 2, 1]]
SUITE = EvaluationSuite(
    heldout=(
        ((-4, -1), 0),
        ((-3, -2), 0),
        ((-1, -4), 0),
        ((-2, -3), 0),
        ((4, 1), 1),
        ((3, 2), 1),
        ((1, 4), 1),
        ((2, 3), 1),
    ),
    regression=(((-1, 0), 0), ((0, -1), 0), ((1, 0), 1), ((0, 1), 1)),
)
INITIAL = (0, 0, -1)


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def identity(root: Path, name: str) -> TLSIdentity:
    return TLSIdentity(root / f"{name}.pem", root / f"{name}.key", root / "ca.pem")


def create_test_pki(root: Path) -> None:
    """Explicit development-only generation; caller deletes the entire directory."""

    def openssl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True, timeout=15)

    openssl(
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout",
        str(root / "ca.key"),
        "-out",
        str(root / "ca.pem"),
        "-days",
        "1",
        "-subj",
        "/CN=AetherMesh disposable learning pilot CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE,pathlen:0",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    )
    extensions = root / "leaf.ext"
    extensions.write_text(
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
        "extendedKeyUsage=serverAuth,clientAuth\n"
        "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
        "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n",
        encoding="ascii",
    )
    for serial, name in enumerate(("coordinator", "evaluator-a", "evaluator-b"), 1):
        openssl(
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-keyout",
            str(root / f"{name}.key"),
            "-out",
            str(root / f"{name}.csr"),
            "-subj",
            f"/CN=AetherMesh disposable {name}",
        )
        openssl(
            "x509",
            "-req",
            "-in",
            str(root / f"{name}.csr"),
            "-CA",
            str(root / "ca.pem"),
            "-CAkey",
            str(root / "ca.key"),
            "-set_serial",
            str(serial),
            "-days",
            "1",
            "-extfile",
            str(extensions),
            "-out",
            str(root / f"{name}.pem"),
        )
    for key in root.glob("*.key"):
        key.chmod(0o600)


async def stop_process(process: asyncio.subprocess.Process) -> dict[str, Any]:
    """Drain and reap every child, with a bounded forced-stop fallback."""
    if process.stdin is not None:
        process.stdin.close()
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
    except TimeoutError:
        process.kill()
        await process.communicate()
        raise RuntimeError("a pilot child exceeded its shutdown deadline") from None
    if process.returncode != 0:
        raise RuntimeError(
            f"pilot child failed (last 4096 stderr characters): {stderr.decode(errors='replace')[-4096:]}"
        )
    return json.loads(stdout) if stdout else {}


async def stop_processes(
    processes: list[asyncio.subprocess.Process], primary: BaseException | None
) -> list[dict[str, Any]]:
    """Reap every child and preserve an already-active primary failure."""
    outcomes = await asyncio.gather(
        *(stop_process(process) for process in processes), return_exceptions=True
    )
    results = []
    failures = []
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            failures.append(outcome)
        else:
            results.append(outcome)
    if primary is not None:
        for failure in failures:
            primary.add_note(
                f"Child cleanup failed: {type(failure).__name__}: {failure}"
            )
    elif failures:
        first, *remaining = failures
        for failure in remaining:
            first.add_note(
                f"Additional child cleanup failure: {type(failure).__name__}: {failure}"
            )
        raise first
    return results


async def start_worker(
    root: Path, name: str, processes: list[asyncio.subprocess.Process]
) -> tuple[asyncio.subprocess.Process, dict[str, Any]]:
    tls = identity(root, name)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).with_name("network_learning_worker.py")),
        "--certificate",
        str(tls.certificate),
        "--private-key",
        str(tls.private_key),
        "--trust-store",
        str(tls.trust_store),
        "--allow-peer",
        certificate_fingerprint(identity(root, "coordinator").certificate),
        "--project",
        PROJECT,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    processes.append(process)
    assert process.stdout is not None
    line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
    if not line:
        await stop_process(process)
        raise RuntimeError("evaluator did not announce readiness")
    return process, json.loads(line)


async def train() -> dict[str, Any]:
    """Launch only the training helper; no evaluation rows, labels or feedback."""
    helper = Path(__file__).with_name("network_learning_trainer.py")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(helper),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps({"rows": TRAINING}).encode()),
            timeout=10,
        )
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.communicate()
        raise
    if process.returncode != 0:
        raise RuntimeError(f"trainer failed: {stderr.decode(errors='replace')[:1000]}")
    result: dict[str, Any] = json.loads(stdout)
    if result["training_data_digest"] != digest({"rows": TRAINING}):
        raise RuntimeError("trainer input provenance mismatch")
    result["source_digest"] = hashlib.sha256(helper.read_bytes()).hexdigest()
    return result


async def pilot(root: Path) -> dict[str, Any]:
    trained = await train()
    provenance = TrainingProvenance(
        trained["source_digest"], trained["training_data_digest"]
    )
    workers = ("evaluator-a", "evaluator-b")
    policy = GovernancePolicy(
        PROJECT,
        tuple(
            (certificate_fingerprint(identity(root, name).certificate), name)
            for name in workers
        ),
        "local-trainer",
        microbatch_rows=2,
        max_rounds=4,
    )
    controller = GovernedRouter(policy, SUITE, INITIAL)
    before = controller.route((1, 1))
    initial_id = controller.active_id
    processes: list[asyncio.subprocess.Process] = []
    records: list[dict[str, Any]] = []
    cleanup: list[dict[str, Any]] = []
    measured_payloads: list[int] = []
    observations: list[dict[str, Any]] = []
    try:
        async with AsyncExitStack() as stack:
            clients = []
            for name in workers:
                process, ready = await start_worker(root, name, processes)
                records.append(
                    {
                        "operator": name,
                        "pid": process.pid,
                        "fingerprint": certificate_fingerprint(
                            identity(root, name).certificate
                        ),
                    }
                )
                client = await stack.enter_async_context(
                    PeerClient(identity(root, "coordinator"), project_id=PROJECT)
                )
                await client.connect(
                    PeerEndpoint(
                        ready["host"],
                        ready["port"],
                        "localhost",
                        records[-1]["fingerprint"],
                    )
                )
                clients.append(client)
            plans = (
                ("bad_candidate", (0, 0, 1)),
                ("gain_with_regression", (1, 1, 2)),
                ("learned_candidate", tuple(trained["weights"])),
                ("poison_after_promotion", (1, 1, 2)),
            )
            for name, weights in plans:
                controller.propose(weights, provenance)
                quarantined_route = controller.route((1, 1))
                tasks = controller.tasks
                measured_payloads.extend(
                    len(json.dumps(task).encode()) for task in tasks
                )
                for index in range(len(tasks)):
                    await asyncio.gather(
                        *(controller.collect(client, index) for client in clients)
                    )
                receipt = controller.finish()
                observations.append(
                    {
                        "name": name,
                        "decision": receipt["decision"],
                        "reason": receipt["reason"],
                        "baseline_correct": receipt["baseline_correct"],
                        "candidate_correct": receipt["candidate_correct"],
                        "regressions": receipt["regressions"],
                        "quarantined_route": quarantined_route,
                        "active_route": controller.route((1, 1)),
                    }
                )
            promoted_id = controller.active_id
            after = controller.route((1, 1))
            controller.rollback(promoted_id)
    finally:
        # Reap every child. Preserve a primary pilot exception, with secondary
        # cleanup failures attached as notes; otherwise fail on cleanup errors.
        cleanup = await stop_processes(processes, sys.exception())
    if [item["decision"] for item in observations] != [
        "rejected",
        "rejected",
        "promoted",
        "rejected",
    ]:
        raise RuntimeError("pilot did not produce the expected governed decisions")
    return {
        "version": 1,
        "scope": "local deterministic toy pilot; not public consensus or an OS sandbox",
        "project": PROJECT,
        "coordinator_pid": os.getpid(),
        "trainer": trained,
        "evaluators": [
            {**record, **usage} for record, usage in zip(records, cleanup, strict=True)
        ],
        "limits": {
            "microbatch_rows": 2,
            "max_rows_per_evaluator": 48,
            "candidate_rounds": 4,
            "max_task_payload_bytes": max(measured_payloads),
        },
        "heldout_cases": len(SUITE.heldout),
        "regression_cases": len(SUITE.regression),
        "observations": observations,
        "routing": {
            "probe": [1, 1],
            "before": before,
            "after_promotion": after,
            "after_rollback": controller.route((1, 1)),
        },
        "initial_id": initial_id,
        "promoted_id": promoted_id,
        "active_id": controller.active_id,
        "receipts": controller.receipts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    credentials = parser.add_mutually_exclusive_group(required=True)
    credentials.add_argument(
        "--ephemeral-test-pki",
        action="store_true",
        help="Explicitly generate and then delete disposable test certificates",
    )
    credentials.add_argument(
        "--identity-dir",
        type=Path,
        help="Directory containing operator-provided TLS identities; never modified",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Write the JSON result and audit receipts here",
    )
    args = parser.parse_args()
    if args.ephemeral_test_pki:
        with tempfile.TemporaryDirectory(
            prefix="aethermesh-learning-pki-"
        ) as directory:
            root = Path(directory)
            create_test_pki(root)
            result = asyncio.run(pilot(root))
        result["credential_mode"] = "ephemeral_test"
        result["ephemeral_credentials_deleted"] = not root.exists()
    else:
        result = asyncio.run(pilot(args.identity_dir.resolve()))
        result["credential_mode"] = "operator_provided"
    args.output.write_text(
        json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "artifact": str(args.output),
                "decisions": [item["decision"] for item in result["observations"]],
                "routing": result["routing"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
