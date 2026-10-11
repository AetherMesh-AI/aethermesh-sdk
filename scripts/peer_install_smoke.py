"""Two-process installed CLI acceptance with disposable loopback TLS fixtures.

Run with the wheel environment's Python and -I. This script loads only the
repository's test-PKI helper by path; SDK imports must resolve to the installed
package. No fixture or certificate is persisted beyond this run.
"""

from __future__ import annotations

import asyncio
import json
import os
import runpy
import socket
import sys
import tempfile
from pathlib import Path

import aethermesh_core

ROOT = Path(__file__).resolve().parents[1]


async def stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        process.terminate()
    try:
        await asyncio.wait_for(process.communicate(), 5)
    except TimeoutError:
        process.kill()
        await asyncio.wait_for(process.communicate(), 5)
        raise RuntimeError("installed CLI did not terminate promptly") from None


async def command(*args: str, cwd: Path) -> tuple[int | None, bytes, bytes]:
    process = await asyncio.create_subprocess_exec(
        *args, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        return process.returncode, stdout, stderr
    finally:
        await stop(process)


async def smoke() -> None:
    if Path(aethermesh_core.__file__).resolve().is_relative_to(ROOT):
        raise RuntimeError("acceptance requires a wheel installed outside the checkout")
    peer = str(
        Path(sys.executable).parent
        / ("aethermesh-peer.exe" if os.name == "nt" else "aethermesh-peer")
    )
    fixture = runpy.run_path(str(ROOT / "tests" / "network_test_support.py"))["TestPKI"]
    with tempfile.TemporaryDirectory(prefix="peer-installed-tls-") as directory:
        cwd = Path(directory)
        pki = fixture.create(cwd / "pki")
        pins = []
        for identity in (pki.server, pki.client):
            code, out, err = await command(
                peer, "fingerprint", "--certificate", str(identity.certificate), cwd=cwd
            )
            assert code == 0 and not err
            record = json.loads(out)
            assert record["trust_established"] is False
            pins.append(record["sha256"])
        server = await asyncio.create_subprocess_exec(
            peer,
            "serve",
            "--certificate",
            str(pki.server.certificate),
            "--private-key",
            str(pki.server.private_key),
            "--trust-store",
            str(pki.ca),
            "--allow-peer",
            pins[1],
            "--project",
            "installed-check",
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            assert server.stdout is not None
            ready = json.loads(await asyncio.wait_for(server.stdout.readline(), 10))
            assert ready["host"] == "127.0.0.1" and ready["capabilities"] == ["status"]
            args = [
                peer,
                "check",
                "--certificate",
                str(pki.client.certificate),
                "--private-key",
                str(pki.client.private_key),
                "--trust-store",
                str(pki.ca),
                "--peer-fingerprint",
                pins[0],
                "--project",
                "installed-check",
                "--port",
                str(ready["port"]),
            ]
            code, out, err = await command(*args, cwd=cwd)
            assert code == 0 and not err
            assert json.loads(out) == {
                "project": "installed-check",
                "fingerprint": pins[0],
                "protocol_version": 2,
                "capabilities": ["status"],
                "status": "ok",
            }

            async def rejected(extra: list[str], expected: str) -> None:
                code, out, err = await command(*args, *extra, cwd=cwd)
                assert code == 1 and not out
                assert json.loads(err) == {"error": expected}
                assert str(cwd).encode() not in err

            await rejected(["--peer-fingerprint", pins[1]], "authentication_failed")
            await rejected(["--project", "wrong-project"], "connection_closed")
            # Hold an unused local TCP endpoint without listening: no port race.
            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                await rejected(
                    ["--port", str(reserved.getsockname()[1])], "connection_closed"
                )
            stalled_writers: list[asyncio.StreamWriter] = []
            stalled = await asyncio.start_server(
                lambda reader, writer: stalled_writers.append(writer), "127.0.0.1", 0
            )
            try:
                await rejected(
                    [
                        "--port",
                        str(stalled.sockets[0].getsockname()[1]),
                        "--timeout",
                        "0.05",
                    ],
                    "timeout",
                )
            finally:
                stalled.close()
                await stalled.wait_closed()
                for writer in stalled_writers:
                    writer.close()
                    await writer.wait_closed()
        finally:
            await stop(server)
    print(
        "Installed CLI loopback acceptance passed: status, fingerprint, pin/project rejection, unavailable peer, deadline."
    )


if __name__ == "__main__":
    asyncio.run(smoke())
