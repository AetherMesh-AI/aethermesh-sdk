"""Two-process installed CLI acceptance with disposable loopback TLS fixtures.

Run with the wheel environment's Python and -I. This script loads only the
repository's test-PKI helper by path; SDK imports must resolve to the installed
package. No fixture or certificate is persisted beyond this run.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import runpy
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any

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


async def content_smoke(peer: str, pki: Any, pins: list[str], cwd: Path) -> None:
    """Installed CLI transfers one selected binary file; never executes it."""
    source = cwd / "selected.bin"
    blob = bytes(range(256)) * 128
    source.write_bytes(blob)
    digest = hashlib.sha256(blob).hexdigest()
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
        "installed-content",
        "--share-file",
        str(source),
        "--content-budget",
        str(len(blob)),
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert server.stdout is not None
        ready = json.loads(await asyncio.wait_for(server.stdout.readline(), 10))
        reference = {
            "project": "installed-content",
            "sha256": digest,
            "size": len(blob),
        }
        assert ready["content"] == [reference]
        assert ready["content_budget_bytes"] == len(blob)
        args = [
            peer,
            "fetch",
            "--certificate",
            str(pki.client.certificate),
            "--private-key",
            str(pki.client.private_key),
            "--trust-store",
            str(pki.ca),
            "--peer-fingerprint",
            pins[0],
            "--project",
            "installed-content",
            "--port",
            str(ready["port"]),
            "--sha256",
            digest,
            "--size",
            str(len(blob)),
        ]
        output = cwd / "received.bin"
        code, out, err = await command(*args, "--output", str(output), cwd=cwd)
        assert code == 0 and not err
        assert json.loads(out) == {
            **reference,
            "saved": True,
            "integrity_verified": True,
            "publisher_authorized": False,
        }
        assert output.read_bytes() == blob and output.stat().st_mode & 0o777 == 0o600
        assert source.read_bytes() == blob
        for target, error in (
            (output, "output_exists"),
            (cwd / "budget-exhausted", "remote_error"),
        ):
            code, out, err = await command(*args, "--output", str(target), cwd=cwd)
            assert code == 1 and not out and json.loads(err) == {"error": error}
        assert output.read_bytes() == blob and not (cwd / "budget-exhausted").exists()
        alias = cwd / "alias"
        alias.symlink_to(output)
        code, out, err = await command(*args, "--output", str(alias), cwd=cwd)
        assert code == 1 and not out and json.loads(err) == {"error": "output_exists"}
        assert alias.is_symlink() and output.read_bytes() == blob
        assert not list(cwd.glob(".aethermesh-content-*"))
    finally:
        await stop(server)
    print(
        "Installed small-content acceptance passed: 32 KiB binary integrity, private output, no overwrite/symlink following, finite budget."
    )


async def smoke() -> None:
    if Path(aethermesh_core.__file__).resolve().is_relative_to(ROOT):
        raise RuntimeError("acceptance requires a wheel installed outside the checkout")
    peer = str(
        Path(sys.executable).parent
        / ("aethermesh-peer.exe" if os.name == "nt" else "aethermesh-peer")
    )
    fixture = runpy.run_path(str(ROOT / "tests" / "network_test_support.py"))["TestPKI"]
    with tempfile.TemporaryDirectory(prefix="peer-installed-tls-") as directory:
        cwd = Path(directory).resolve()
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
        malformed = cwd / "not-a-certificate.pem"
        malformed.write_text(
            "-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n",
            encoding="ascii",
        )
        code, out, err = await command(
            peer, "fingerprint", "--certificate", str(malformed), cwd=cwd
        )
        assert (
            code == 1
            and not out
            and json.loads(err) == {"error": "authentication_failed"}
        )
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

            async def rejected(
                extra: list[str], expected: str | tuple[str, ...]
            ) -> None:
                code, out, err = await command(*args, *extra, cwd=cwd)
                assert code == 1 and not out
                codes = (expected,) if isinstance(expected, str) else expected
                assert json.loads(err) in [{"error": code} for code in codes], (
                    codes,
                    err,
                )
                assert str(cwd).encode() not in err

            await rejected(["--peer-fingerprint", pins[1]], "authentication_failed")
            await rejected(["--project", "wrong-project"], "connection_closed")
            # Hold an unused local TCP endpoint without listening: no port race.
            # Linux refuses this connection; macOS can leave SYNs unanswered.
            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                await rejected(
                    ["--port", str(reserved.getsockname()[1]), "--timeout", "0.1"],
                    ("connection_closed", "timeout"),
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
                # Newer asyncio waits for accepted transports in wait_closed;
                # close those transports first, including the stalled handshake.
                for writer in stalled_writers:
                    writer.close()
                async with asyncio.timeout(5):
                    for writer in stalled_writers:
                        await writer.wait_closed()
                    await stalled.wait_closed()
        finally:
            await stop(server)
        await content_smoke(peer, pki, pins, cwd)
    print(
        "Installed CLI loopback acceptance passed: status, fingerprint, pin/project rejection, unavailable peer, deadline."
    )


if __name__ == "__main__":
    asyncio.run(smoke())
