"""Ephemeral TLS identities for tests; never reuse them outside a test run."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from aethermesh_core.network import TLSIdentity


@dataclass(frozen=True)
class TestIdentity:
    """Paths to one short-lived CA-signed test identity."""

    certificate: Path
    private_key: Path
    trust_store: Path

    def identity(self) -> TLSIdentity:
        return TLSIdentity(self.certificate, self.private_key, self.trust_store)


@dataclass(frozen=True)
class TestPKI:
    """One temporary CA and three independently identifiable peers."""

    # A fixture data object, not a pytest test container.
    __test__ = False

    ca: Path
    server: TestIdentity
    client: TestIdentity
    unauthorized: TestIdentity

    @classmethod
    def create(cls, root: Path) -> TestPKI:
        """Create test credentials under a caller-owned temporary directory.

        OpenSSL is a required integration-test dependency. Missing or broken
        installations deliberately fail rather than silently reducing coverage.
        The caller must remove the complete directory after use.
        """
        root.mkdir(parents=True, exist_ok=True)
        ca = root / "ca.pem"
        ca_key = root / "ca.key"
        _openssl(
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca),
            "-days",
            "1",
            "-subj",
            "/CN=AetherMesh ephemeral integration CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        )
        extension_file = root / "leaf.ext"
        extension_file.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature\n"
            "extendedKeyUsage=serverAuth,clientAuth\n"
            "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
            "subjectKeyIdentifier=hash\n"
            "authorityKeyIdentifier=keyid,issuer\n",
            encoding="ascii",
        )
        peers = []
        for serial, name in enumerate(("server", "client", "unauthorized"), 1):
            key = root / f"{name}.key"
            csr = root / f"{name}.csr"
            certificate = root / f"{name}.pem"
            _openssl(
                "req",
                "-new",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(csr),
                "-subj",
                f"/CN=AetherMesh test {name}",
            )
            _openssl(
                "x509",
                "-req",
                "-in",
                str(csr),
                "-CA",
                str(ca),
                "-CAkey",
                str(ca_key),
                "-set_serial",
                str(serial),
                "-days",
                "1",
                "-extfile",
                str(extension_file),
                "-out",
                str(certificate),
            )
            peers.append(TestIdentity(certificate, key, ca))
        return cls(ca, peers[0], peers[1], peers[2])


def _openssl(*args: str) -> None:
    subprocess.run(
        ["openssl", *args],
        check=True,
        capture_output=True,
        timeout=15,
    )
