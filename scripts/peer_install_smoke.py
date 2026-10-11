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


async def download_api_smoke(peer: str, pki: Any, pins: list[str], cwd: Path) -> None:
    """Installed public API cancellation, callback failure and explicit resume."""
    from aethermesh_core.network import (
        DownloadError,
        DownloadProgress,
        PeerEndpoint,
        TransferStore,
        download_transfer,
    )

    blob = b"".join(bytes([index]) * 32768 for index in range(256))
    source = cwd / "api-source"
    source.write_bytes(blob)
    manifest = TransferStore("api-download", blob, max_served_bytes=0).manifest
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
        "api-download",
        "--share-transfer",
        str(source),
        "--content-budget",
        str(32 * 1024 * 1024),
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert server.stdout is not None
        ready = json.loads(await asyncio.wait_for(server.stdout.readline(), 10))
        endpoint = PeerEndpoint("127.0.0.1", ready["port"], "localhost", pins[0])
        staging, output = cwd / "api-stage", cwd / "api-output"

        # End one installed SDK process after cooperative cancellation, then
        # resume in this independent installed process using only checkpoints.
        cancelled_code = """
import asyncio, sys
from pathlib import Path
from aethermesh_core.network import TLSIdentity, PeerEndpoint, download_transfer
async def interrupt(progress):
    if progress.chunks_fetched == 3:
        raise asyncio.CancelledError()
async def main():
    try:
        await download_transfer(
            TLSIdentity(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])),
            PeerEndpoint('127.0.0.1', int(sys.argv[4]), 'localhost', sys.argv[5]),
            project_id='api-download', manifest_sha256=sys.argv[6],
            staging=Path(sys.argv[7]), output=Path(sys.argv[8]), on_progress=interrupt)
    except asyncio.CancelledError:
        return 130
    return 1
sys.exit(asyncio.run(main()))
"""
        code, out, err = await command(
            sys.executable,
            "-I",
            "-c",
            cancelled_code,
            str(pki.client.certificate),
            str(pki.client.private_key),
            str(pki.ca),
            str(ready["port"]),
            pins[0],
            manifest.digest,
            str(staging),
            str(output),
            cwd=cwd,
        )
        assert code == 130 and not out and not err and not output.exists()

        async def fail_final(progress: DownloadProgress) -> None:
            if progress.phase == "verified":
                raise RuntimeError("application callback failed")

        try:
            await download_transfer(
                pki.client.identity(),
                endpoint,
                project_id="api-download",
                manifest_sha256=manifest.digest,
                staging=staging,
                output=output,
                resume=True,
                on_progress=fail_final,
            )
        except DownloadError as error:
            assert error.code == "callback_failed" and not error.publication_attempted
        else:
            raise AssertionError("expected callback failure")
        assert not output.exists()
        seen: list[DownloadProgress] = []

        async def observe(progress: DownloadProgress) -> None:
            seen.append(progress)

        result = await download_transfer(
            pki.client.identity(),
            endpoint,
            project_id="api-download",
            manifest_sha256=manifest.digest,
            staging=staging,
            output=output,
            resume=True,
            on_progress=observe,
        )
        assert result.chunks_fetched == 0 and result.chunks_reused == 256
        assert result.integrity_verified and not result.publisher_authorized
        assert result.output.read_bytes() == blob
        assert len(seen) == 258 and seen[-1].verified_bytes == len(blob)
        assert [p.verified_chunks for p in seen] == [0, *range(1, 257), 256]
    finally:
        await stop(server)
    print(
        "Installed public download API passed: 8 MiB distinct chunks, cancellation, callback failure, verified resume and typed progress."
    )


async def transfer_smoke(peer: str, pki: Any, pins: list[str], cwd: Path) -> None:
    """Restart both CLI processes; exact budgets prove no verified chunk is resent."""
    from aethermesh_core.network import TransferStore

    blob = b"".join(bytes([index]) * 32768 for index in range(256))  # Distinct chunks.
    source = cwd / "transfer-source"
    source.write_bytes(blob)
    manifest = TransferStore("resume", blob, max_served_bytes=0).manifest
    stage = cwd / "checkpoints"
    output = cwd / "resumed-output"
    server_args = [
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
        "resume",
        "--share-transfer",
        str(source),
    ]
    client_args = [
        peer,
        "fetch-transfer",
        "--certificate",
        str(pki.client.certificate),
        "--private-key",
        str(pki.client.private_key),
        "--trust-store",
        str(pki.ca),
        "--peer-fingerprint",
        pins[0],
        "--project",
        "resume",
        "--manifest-sha256",
        manifest.digest,
        "--staging",
        str(stage),
        "--output",
        str(output),
        "--timeout",
        "12",
    ]
    for resumed in (False, True):
        chunk_count = 252 if resumed else 3
        budget = len(manifest.encode()) + chunk_count * 32768
        server = await asyncio.create_subprocess_exec(
            *server_args,
            "--content-budget",
            str(budget),
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            assert server.stdout is not None
            ready = json.loads(await asyncio.wait_for(server.stdout.readline(), 10))
            assert ready["manifest_sha256"] == manifest.digest
            args = [*client_args, "--port", str(ready["port"])]
            if resumed:
                args.append("--resume")
            code, out, err = await command(*args, cwd=cwd)
            if resumed:
                assert code == 0 and not err
                report = json.loads(out)
                assert (
                    report["chunks_fetched"] == 252
                    and report["publisher_authorized"] is False
                )
                assert (
                    output.read_bytes() == blob
                    and output.stat().st_mode & 0o777 == 0o600
                )
            else:
                assert (
                    code == 1
                    and not out
                    and json.loads(err) == {"error": "remote_error"}
                )
                assert not output.exists() and len(list(stage.glob("*.chunk"))) == 3
                crash = """
import os, sys
from pathlib import Path
from unittest.mock import patch
from aethermesh_core.network import TransferManifest
from aethermesh_core.network.transfer_files import checkpoint, _save
stage, source, expected = sys.argv[1:]
manifest = TransferManifest.decode((Path(stage) / 'manifest.json').read_bytes(), 'resume', expected)
def crash_before_unlink(*args, **kwargs):
    os._exit(73)
with checkpoint(Path(stage), manifest, resume=True) as directory:
    with patch('aethermesh_core.network.content_files.os.unlink', crash_before_unlink):
        _save(directory, '003.chunk', Path(source).read_bytes()[98304:131072])
"""
                code, out, err = await command(
                    sys.executable,
                    "-I",
                    "-c",
                    crash,
                    str(stage),
                    str(source),
                    manifest.digest,
                    cwd=cwd,
                )
                assert code == 73 and not out and not err
                assert len(list(stage.glob("*.chunk"))) == 4
                remnants = list(stage.glob(".aethermesh-content-*"))
                assert len(remnants) == 1 and remnants[0].stat().st_nlink == 2
        finally:
            await stop(server)
    assert stage.stat().st_mode & 0o777 == 0o700
    assert not list(stage.glob(".aethermesh-content-*"))
    print(
        "Installed resume acceptance passed: 8 MiB, process restart, crash recovery, exactly 252 missing chunks fetched, private verified output."
    )


async def bundle_smoke(peer: str, pki: Any, pins: list[str], cwd: Path) -> None:
    """Export owned refs, transfer opaque bytes, inspect claims without ingestion."""
    fixture = runpy.run_path(str(ROOT / "tests" / "bundle_test_support.py"))
    source = cwd / "owned-source.git"
    ref, oid = fixture["create_repository"](source)
    original_ref = (source / ref).read_bytes()
    bundle = cwd / "owned.bundle"
    code, out, err = await command(
        peer,
        "bundle-export",
        "--project",
        "bundle-test",
        "--source",
        str(source),
        "--ref",
        ref,
        "--expected-oid",
        oid,
        "--output",
        str(bundle),
        cwd=cwd,
    )
    assert code == 0 and not err
    exported = json.loads(out)
    assert exported["claimed_oid"] == oid and exported["pack_validated"] is False
    assert exported["publisher_authorized"] is False
    assert (source / ref).read_bytes() == original_ref
    data = bundle.read_bytes()
    server = await asyncio.create_subprocess_exec(
        peer,
        "serve",
        "--project",
        "bundle-test",
        "--certificate",
        str(pki.server.certificate),
        "--private-key",
        str(pki.server.private_key),
        "--trust-store",
        str(pki.ca),
        "--allow-peer",
        pins[1],
        "--share-transfer",
        str(bundle),
        "--content-budget",
        "1048576",
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert server.stdout is not None
        ready = json.loads(await asyncio.wait_for(server.stdout.readline(), 10))
        received = cwd / "received.bundle"
        code, out, err = await command(
            peer,
            "fetch-transfer",
            "--project",
            "bundle-test",
            "--certificate",
            str(pki.client.certificate),
            "--private-key",
            str(pki.client.private_key),
            "--trust-store",
            str(pki.ca),
            "--peer-fingerprint",
            pins[0],
            "--port",
            str(ready["port"]),
            "--manifest-sha256",
            ready["manifest_sha256"],
            "--staging",
            str(cwd / "bundle-stage"),
            "--output",
            str(received),
            cwd=cwd,
        )
        assert code == 0 and not err and received.read_bytes() == data
        code, out, err = await command(
            peer,
            "bundle-inspect",
            "--project",
            "bundle-test",
            "--file",
            str(received),
            "--sha256",
            exported["sha256"],
            cwd=cwd,
        )
        assert code == 0 and not err and json.loads(out) == exported
        assert (source / ref).read_bytes() == original_ref
    finally:
        await stop(server)
    print(
        "Installed bundle acceptance passed: pinned owned export, peer transfer, bounded header claims; no pack ingestion."
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
        await transfer_smoke(peer, pki, pins, cwd)
        await bundle_smoke(peer, pki, pins, cwd)
        await download_api_smoke(peer, pki, pins, cwd)
    print(
        "Installed CLI loopback acceptance passed: status, fingerprint, pin/project rejection, unavailable peer, deadline."
    )


if __name__ == "__main__":
    asyncio.run(smoke())
