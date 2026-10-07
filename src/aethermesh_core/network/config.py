"""Explicit peer addresses, resource bounds, and operator-provided TLS identities.

Trust is deliberately local configuration: this module neither discovers peers
nor generates, enrolls, or silently trusts identities.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from .errors import AuthenticationError


def _reject_encrypted_key() -> NoReturn:
    # Never permit OpenSSL to prompt on stdin in an application or daemon.
    raise ValueError("Encrypted TLS private keys are not supported")


@dataclass(frozen=True)
class TLSIdentity:
    """PEM certificate chain, unencrypted key, and explicit peer CA trust store."""

    certificate: Path
    private_key: Path
    trust_store: Path

    def client_context(self) -> ssl.SSLContext:
        """Require TLS 1.3, a valid server chain, and hostname verification."""
        return self._context(server=False)

    def server_context(self) -> ssl.SSLContext:
        """Require TLS 1.3 and a valid client certificate chain."""
        return self._context(server=True)

    def _context(self, *, server: bool) -> ssl.SSLContext:
        protocol = ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT
        context = ssl.SSLContext(protocol)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        try:
            context.load_verify_locations(cafile=str(self.trust_store))
            context.load_cert_chain(
                str(self.certificate),
                str(self.private_key),
                password=_reject_encrypted_key,
            )
        except (OSError, ValueError) as exc:
            raise AuthenticationError(
                "Unable to load the configured TLS identity"
            ) from exc
        return context


def certificate_fingerprint(path: Path) -> str:
    """Return the first PEM certificate's SHA-256 DER fingerprint, in lowercase."""
    try:
        pem = path.read_text(encoding="ascii")
        match = re.search(
            r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", pem, re.DOTALL
        )
        if match is None:
            raise ValueError("Missing PEM certificate")
        der = ssl.PEM_cert_to_DER_cert(match.group())
    except (OSError, ValueError) as exc:
        raise AuthenticationError("Unable to read the configured certificate") from exc
    return hashlib.sha256(der).hexdigest()


def peer_fingerprint(writer: asyncio.StreamWriter) -> str:
    """Read the verified TLS peer's leaf identity; plaintext has no identity."""
    tls = writer.get_extra_info("ssl_object")
    if tls is None:
        raise AuthenticationError("A TLS peer certificate is required")
    certificate = tls.getpeercert(binary_form=True)
    if not certificate:
        raise AuthenticationError("A TLS peer certificate is required")
    return hashlib.sha256(certificate).hexdigest()


def _host(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 253
        or any(character.isspace() or ord(character) < 33 for character in value)
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{field} must be a nonempty hostname or IP address")


def validate_project_id(value: str) -> None:
    """Require a bounded, unambiguous application project namespace."""
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[A-Za-z0-9._-]{1,128}", value) is None
    ):
        raise ValueError(
            "project_id must contain 1 to 128 ASCII letters, digits, . _ or -"
        )


def validate_fingerprint(value: str) -> None:
    """Require canonical SHA-256 pins, suitable for exact allowlist comparison."""
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("fingerprint must be 64 lowercase SHA-256 hexadecimal digits")


@dataclass(frozen=True)
class PeerEndpoint:
    """An explicitly chosen peer and its out-of-band leaf certificate pin."""

    host: str
    port: int
    server_name: str
    fingerprint: str

    def __post_init__(self) -> None:
        _host(self.host, "host")
        _host(self.server_name, "server_name")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port must be an integer from 1 to 65535")
        validate_fingerprint(self.fingerprint)


@dataclass(frozen=True)
class Limits:
    """Per-instance resource ceilings; all timeouts are measured in seconds."""

    max_connections: int = 16
    max_inflight: int = 8
    max_requests: int = 1024
    handshake_timeout: float = 5.0
    io_timeout: float = 30.0

    def __post_init__(self) -> None:
        for name, maximum in (
            ("max_connections", 1024),
            ("max_inflight", 1024),
            ("max_requests", 1_000_000),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be an integer from 1 to {maximum}")
        for name in ("handshake_timeout", "io_timeout"):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not 0 < value <= 3600
                or not math.isfinite(value)
            ):
                raise ValueError(
                    f"{name} must be finite and between 0 and 3600 seconds"
                )
