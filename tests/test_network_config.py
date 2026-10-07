"""Trust configuration and hard resource-bound tests."""

import hashlib
import ssl
import subprocess
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from aethermesh_core.network.config import (
    Limits,
    PeerEndpoint,
    TLSIdentity,
    certificate_fingerprint,
    peer_fingerprint,
    validate_fingerprint,
    validate_project_id,
)
from aethermesh_core.network.errors import AuthenticationError
from tests.network_test_support import TestPKI


class TLSIdentityTests(unittest.TestCase):
    def test_contexts_require_explicit_mutual_tls13_and_client_hostname(self) -> None:
        identity = TLSIdentity(Path("identity.pem"), Path("key.pem"), Path("ca.pem"))
        with (
            patch.object(ssl.SSLContext, "load_verify_locations") as trust,
            patch.object(ssl.SSLContext, "load_cert_chain") as chain,
        ):
            client = identity.client_context()
            server = identity.server_context()
        self.assertEqual(client.protocol, ssl.PROTOCOL_TLS_CLIENT)
        self.assertEqual(server.protocol, ssl.PROTOCOL_TLS_SERVER)
        self.assertTrue(client.check_hostname)
        self.assertFalse(server.check_hostname)
        for context in (client, server):
            self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_3)
            self.assertEqual(context.maximum_version, ssl.TLSVersion.TLSv1_3)
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertEqual(context.cert_store_stats()["x509_ca"], 0)
        self.assertEqual(trust.call_count, 2)
        trust.assert_called_with(cafile="ca.pem")
        self.assertEqual(chain.call_count, 2)
        self.assertEqual(chain.call_args.args, ("identity.pem", "key.pem"))
        password = chain.call_args.kwargs["password"]
        with self.assertRaisesRegex(ValueError, "Encrypted TLS private keys"):
            password()
        with self.assertRaises(FrozenInstanceError):
            identity.trust_store = Path("other.pem")

    def test_bad_trust_store_and_certificate_fail_closed(self) -> None:
        identity = TLSIdentity(Path("identity.pem"), Path("key.pem"), Path("ca.pem"))
        for failing in ("load_verify_locations", "load_cert_chain"):
            for error in (
                OSError("missing"),
                ssl.SSLError("bad PEM"),
                ValueError("bad"),
            ):
                with (
                    self.subTest(failing=failing, error=type(error)),
                    patch.object(ssl.SSLContext, "load_verify_locations") as trust,
                    patch.object(ssl.SSLContext, "load_cert_chain") as chain,
                ):
                    target = trust if failing == "load_verify_locations" else chain
                    target.side_effect = error
                    with self.assertRaises(AuthenticationError):
                        identity.client_context()
                    with self.assertRaises(AuthenticationError):
                        identity.server_context()

    def test_encrypted_private_keys_fail_without_an_interactive_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pki = TestPKI.create(root)
            encrypted_key = root / "encrypted.key"
            subprocess.run(
                [
                    "openssl",
                    "pkey",
                    "-in",
                    str(pki.client.private_key),
                    "-aes-256-cbc",
                    "-passout",
                    "pass:ephemeral-test-passphrase",
                    "-out",
                    str(encrypted_key),
                ],
                check=True,
                capture_output=True,
                timeout=15,
            )
            script = (
                "import sys; from pathlib import Path; "
                "sys.path.insert(0, sys.argv[1]); "
                "from aethermesh_core.network.config import TLSIdentity; "
                "from aethermesh_core.network.errors import AuthenticationError; "
                "identity = TLSIdentity(*(Path(p) for p in sys.argv[2:]))\n"
                "for create in (identity.client_context, identity.server_context):\n"
                "    try: create()\n"
                "    except AuthenticationError: pass\n"
                "    else: raise AssertionError('encrypted key was accepted')\n"
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    script,
                    str(Path(__file__).resolve().parents[1] / "src"),
                    str(pki.client.certificate),
                    str(encrypted_key),
                    str(pki.ca),
                ],
                stdin=subprocess.DEVNULL,
                check=False,
                capture_output=True,
                timeout=5,
            )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        self.assertEqual(completed.stderr, b"")
        self.assertEqual(completed.stdout, b"")

    def test_fingerprint_hashes_first_leaf_der_in_a_pem_chain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "identity.pem"
            path.write_text(
                "-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n"
                "-----BEGIN CERTIFICATE-----\neHl6\n-----END CERTIFICATE-----\n",
                encoding="ascii",
            )
            self.assertEqual(
                certificate_fingerprint(path), hashlib.sha256(b"abc").hexdigest()
            )

    def test_missing_bad_encoding_and_malformed_certificates_are_typed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.pem"
            with self.assertRaises(AuthenticationError):
                certificate_fingerprint(path)
            for content in (
                b"no certificate",
                b"\xff",
                b"-----BEGIN CERTIFICATE-----\na\n-----END CERTIFICATE-----",
            ):
                path.write_bytes(content)
                with self.assertRaises(AuthenticationError):
                    certificate_fingerprint(path)

    def test_peer_fingerprint_requires_tls_and_a_certificate(self) -> None:
        writer = Mock()
        writer.get_extra_info.return_value = None
        with self.assertRaises(AuthenticationError):
            peer_fingerprint(writer)
        tls = Mock()
        writer.get_extra_info.return_value = tls
        for missing in (None, b""):
            tls.getpeercert.return_value = missing
            with self.assertRaises(AuthenticationError):
                peer_fingerprint(writer)
        tls.getpeercert.return_value = b"peer DER bytes"
        self.assertEqual(
            peer_fingerprint(writer), hashlib.sha256(b"peer DER bytes").hexdigest()
        )
        tls.getpeercert.assert_called_with(binary_form=True)
        writer.get_extra_info.assert_called_with("ssl_object")


class EndpointTests(unittest.TestCase):
    def test_valid_pinned_endpoints_are_frozen(self) -> None:
        for host in ("localhost", "127.0.0.1", "::1", "peer.example.org"):
            endpoint = PeerEndpoint(host, 443, "localhost", "a" * 64)
            self.assertEqual(endpoint.host, host)
            self.assertEqual(endpoint.port, 443)
            self.assertEqual(endpoint.server_name, "localhost")
            self.assertEqual(endpoint.fingerprint, "a" * 64)
            with self.assertRaises(FrozenInstanceError):
                endpoint.port = 80
        self.assertEqual(PeerEndpoint("a" * 253, 1, "x", "0" * 64).port, 1)
        self.assertEqual(PeerEndpoint("x", 65535, "x", "f" * 64).port, 65535)

    def test_invalid_hosts_server_names_and_ports(self) -> None:
        for field in ("host", "server_name"):
            for value in (
                None,
                "",
                "a" * 254,
                "two words",
                "x\t",
                "\x00",
                "a/b",
                "a\\b",
            ):
                data: dict[str, Any] = {
                    "host": "localhost",
                    "port": 443,
                    "server_name": "localhost",
                    "fingerprint": "a" * 64,
                }
                data[field] = value
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    PeerEndpoint(**data)
        for port in (True, False, None, "443", 443.0, 0, -1, 65536):
            with self.subTest(port=port), self.assertRaises(ValueError):
                PeerEndpoint("localhost", port, "localhost", "a" * 64)

    def test_fingerprint_requires_canonical_lowercase_hex(self) -> None:
        for value in (
            None,
            "",
            "a" * 63,
            "a" * 65,
            "A" * 64,
            "g" * 64,
            "a" * 63 + "\n",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                PeerEndpoint("localhost", 443, "localhost", value)


class NamespaceTests(unittest.TestCase):
    def test_project_namespaces_are_bounded_and_ascii(self) -> None:
        for value in ("x", "Project-1.0_private", "a" * 128):
            self.assertIsNone(validate_project_id(value))
        for value in (None, "", "a" * 129, "a b", "a/b", "hello\n", "café"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_project_id(value)

    def test_canonical_fingerprint_helper(self) -> None:
        self.assertIsNone(validate_fingerprint("0123456789abcdef" * 4))
        with self.assertRaises(ValueError):
            validate_fingerprint("aa")


class LimitsTests(unittest.TestCase):
    def test_defaults_boundaries_and_immutability(self) -> None:
        limits = Limits()
        self.assertEqual(
            (limits.max_connections, limits.max_inflight, limits.max_requests),
            (16, 8, 1024),
        )
        self.assertEqual((limits.handshake_timeout, limits.io_timeout), (5.0, 30.0))
        self.assertEqual(Limits(1, 1, 1, 0.001, 1).max_requests, 1)
        self.assertEqual(Limits(1024, 1024, 1_000_000, 3600, 3600).io_timeout, 3600)
        with self.assertRaises(FrozenInstanceError):
            limits.max_connections = 0

    def test_integer_limits_reject_noninteger_nonpositive_and_oversized(self) -> None:
        for name, ceiling in (
            ("max_connections", 1024),
            ("max_inflight", 1024),
            ("max_requests", 1_000_000),
        ):
            for value in (True, False, None, "1", 1.0, 0, -1, ceiling + 1):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaises(ValueError),
                ):
                    Limits(**{name: value})

    def test_timeouts_reject_nonfinite_nonpositive_and_oversized(self) -> None:
        for name in ("handshake_timeout", "io_timeout"):
            for value in (
                True,
                False,
                None,
                "1",
                0,
                -1,
                3601,
                10**1000,
                -(10**1000),
                float("nan"),
                float("inf"),
                float("-inf"),
            ):
                with (
                    self.subTest(name=name, value=value),
                    self.assertRaises(ValueError),
                ):
                    Limits(**{name: value})
