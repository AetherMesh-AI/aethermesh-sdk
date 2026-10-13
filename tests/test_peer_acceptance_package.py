"""Offline package integrity, honest evidence and runbook argument compatibility."""

import hashlib
import io
import json
import os
import shlex
import socket
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.network import TransferManifest, cli
from scripts import prepare_peer_acceptance as package
from scripts.prepare_peer_acceptance import prepare


class AcceptancePackageTests(unittest.TestCase):
    def test_deterministic_private_bounded_package_has_no_network_or_acceptance(self):
        with (
            tempfile.TemporaryDirectory() as temp,
            patch.object(socket, "socket", side_effect=AssertionError("offline only")),
        ):
            root = Path(temp).resolve()
            first = prepare(root / "first", "acceptance-fixture")
            second = prepare(root / "second", "acceptance-fixture")
            self.assertEqual(first, second)
            self.assertEqual(
                {p.name for p in (root / "first").iterdir()},
                {"fixture.bin", "manifest.json", "evidence.json"},
            )
            data = (root / "first" / "fixture.bin").read_bytes()
            self.assertEqual(len(data), 8388608)
            self.assertEqual(
                hashlib.sha256(data).hexdigest(), first["fixture"]["sha256"]
            )
            manifest = TransferManifest.decode(
                (root / "first" / "manifest.json").read_bytes(),
                "acceptance-fixture",
                first["fixture"]["manifest_sha256"],
            )
            self.assertEqual(len(set(manifest.chunks)), 256)
            for index, digest in enumerate(manifest.chunks):
                self.assertEqual(
                    data[index * 32768 : (index + 1) * 32768], bytes([index]) * 32768
                )
                self.assertEqual(
                    hashlib.sha256(
                        data[index * 32768 : (index + 1) * 32768]
                    ).hexdigest(),
                    digest,
                )
            self.assertEqual(
                json.loads((root / "first" / "evidence.json").read_bytes()), first
            )
            self.assertEqual((root / "first").stat().st_mode & 0o777, 0o700)
            for name in ("fixture.bin", "manifest.json", "evidence.json"):
                self.assertEqual((root / "first" / name).stat().st_mode & 0o777, 0o600)
            self.assertFalse(first["network_activity_performed"])
            self.assertFalse(first["two_computer_acceptance"])
            self.assertFalse(first["publisher_authorized"])
            self.assertTrue(
                all(value is None for value in first["required_inputs"].values())
            )
            self.assertTrue(
                all(
                    row["status"] == "not_run" and row["exit_code"] is None
                    for row in first["observations"].values()
                )
            )
            with self.assertRaises(FileExistsError):
                prepare(root / "first", "acceptance-fixture")
            self.assertEqual((root / "first" / "fixture.bin").read_bytes(), data)

    def test_invalid_project_or_indirect_parent_creates_nothing(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            with self.assertRaises(ValueError):
                prepare(root / "invalid", "")
            self.assertFalse((root / "invalid").exists())
            (root / "alias").symlink_to(root, target_is_directory=True)
            with self.assertRaises(ValueError):
                prepare(root / "alias" / "kit", "acceptance-fixture")
            self.assertFalse((root / "kit").exists())

    def test_runbook_cli_examples_parse_without_execution(self):
        root = Path(__file__).resolve().parents[1]
        commands = []
        for name in ("two-peer-operations.md", "two-computer-acceptance-checklist.md"):
            text = (root / "docs" / name).read_text().replace("\\\n", " ")
            for line in text.splitlines():
                if line.startswith("aethermesh-peer "):
                    commands.append(shlex.split(line)[1:])
        self.assertGreaterEqual(len(commands), 7)
        with patch.object(socket, "socket", side_effect=AssertionError("parse only")):
            for command in commands:
                with self.subTest(command=command[0]):
                    parsed = cli._parser().parse_args(command)
                    self.assertEqual(parsed.command, command[0])

    def test_generator_cli_and_partial_failure_are_honest_and_static(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            args = [
                "prepare",
                "--project",
                "acceptance-fixture",
                "--output",
                str(root / "kit"),
            ]
            out = io.StringIO()
            with patch("sys.argv", args), redirect_stdout(out):
                self.assertEqual(package.main(), 0)
            self.assertEqual(json.loads(out.getvalue())["acceptance_status"], "not_run")
            original_open = os.open
            count = 0

            def fail_second_file(*args, **kwargs):
                nonlocal count
                count += 1
                if count == 2:
                    raise OSError("private path diagnostic")
                return original_open(*args, **kwargs)

            args[-1] = str(root / "partial")
            out, err = io.StringIO(), io.StringIO()
            with (
                patch("sys.argv", args),
                patch.object(package.os, "open", fail_second_file),
                redirect_stdout(out),
                redirect_stderr(err),
            ):
                self.assertEqual(package.main(), 1)
            self.assertFalse(out.getvalue())
            self.assertNotIn("private path diagnostic", err.getvalue())
            self.assertEqual(
                {p.name for p in (root / "partial").iterdir()}, {"fixture.bin"}
            )
            self.assertEqual((root / "partial" / "fixture.bin").stat().st_size, 8388608)
