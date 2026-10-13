"""Opaque header claims and strictly pinned trusted-local bundle export."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

from aethermesh_core.network import cli, export_bundle, inspect_bundle
from aethermesh_core.network.bundle import validate_branch, validate_oid
from aethermesh_core.network.bundle_export import _Git, _objects, _selected_oid
from tests.bundle_test_support import create_repository, fixture_git


class HeaderTests(unittest.TestCase):
    def test_selected_revision_claims_are_optional_paired_and_not_pack_validation(self):
        oid, ref = "a" * 40, "refs/heads/main"
        data = f"# v2 git bundle\n{oid} {ref}\n\nPACKnot a real pack".encode()
        kwargs = {
            "project_id": "p",
            "expected_sha256": hashlib.sha256(data).hexdigest(),
        }
        with patch("subprocess.run", side_effect=AssertionError("no Git ingestion")):
            selected = inspect_bundle(
                data, **kwargs, expected_ref=ref, expected_oid=oid
            )
            self.assertEqual(selected, inspect_bundle(data, **kwargs))
            self.assertFalse(selected.to_dict()["pack_validated"])
            self.assertFalse(selected.to_dict()["publisher_authorized"])
            for expected in (
                {"expected_ref": ref},
                {"expected_oid": oid},
                {"expected_ref": "HEAD", "expected_oid": oid},
                {"expected_ref": ref, "expected_oid": "0" * 40},
                {"expected_ref": "refs/heads/other", "expected_oid": oid},
                {"expected_ref": ref, "expected_oid": "b" * 40},
            ):
                with self.subTest(expected=expected), self.assertRaises(ValueError):
                    inspect_bundle(data, **kwargs, **expected)

    def test_cli_selected_claims_fail_closed_without_echoing_peer_claims(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp).resolve() / "selected.bundle"
            oid, ref = "a" * 40, "refs/heads/main"
            data = f"# v2 git bundle\n{oid} {ref}\n\nPACKopaque".encode()
            path.write_bytes(data)
            args = [
                "bundle-inspect",
                "--project",
                "p",
                "--file",
                str(path),
                "--sha256",
                hashlib.sha256(data).hexdigest(),
            ]
            for extra, success in (
                (["--expected-ref", ref, "--expected-oid", oid], True),
                (["--expected-ref", ref], False),
                (["--expected-oid", oid], False),
                (
                    [
                        "--expected-ref",
                        "refs/heads/private-claim",
                        "--expected-oid",
                        oid,
                    ],
                    False,
                ),
                (["--expected-ref", ref, "--expected-oid", "b" * 40], False),
            ):
                out, err = io.StringIO(), io.StringIO()
                with redirect_stdout(out), redirect_stderr(err):
                    self.assertEqual(cli.main(args + extra), 0 if success else 1)
                if success:
                    self.assertFalse(err.getvalue())
                    self.assertFalse(json.loads(out.getvalue())["pack_validated"])
                    self.assertFalse(json.loads(out.getvalue())["publisher_authorized"])
                else:
                    self.assertFalse(out.getvalue())
                    self.assertEqual(
                        json.loads(err.getvalue()), {"error": "invalid_configuration"}
                    )
                self.assertEqual(path.read_bytes(), data)

    def test_opaque_payload_claims_and_rejected_headers(self):
        oid = "a" * 40
        valid = (
            f"# v2 git bundle\n{oid} refs/heads/main\n\n".encode()
            + b"PACKnot a real pack"
        )
        with patch(
            "subprocess.run", side_effect=AssertionError("inspection must not run Git")
        ):
            info = inspect_bundle(
                valid, project_id="p", expected_sha256=hashlib.sha256(valid).hexdigest()
            )
        self.assertFalse(info.to_dict()["pack_validated"])
        self.assertFalse(info.to_dict()["publisher_authorized"])
        self.assertEqual(info.claimed_oid, oid)
        malformed = [
            b"",
            b"x" * 16384 + b"\n\nPACK",
            valid.replace(b"v2", b"v3"),
            valid.replace(b"PACK", b"junk"),
            valid.replace(b"\n\n", b"\n-a prerequisite\n\n"),
            valid.replace(b"\n\n", b"\n" + oid.encode() + b" refs/heads/other\n\n"),
            valid.replace(b"refs/heads/main", b"main"),
            valid.replace(b"refs/heads/main", b"refs/heads/../bad"),
            valid.replace(b" refs", b"  refs"),
            valid.replace(b"refs/heads/main", b"refs/heads/\xff"),
            valid.replace(oid.encode(), b"0" * 40),
        ]
        for data in malformed:
            with self.subTest(data=data[:30]), self.assertRaises(ValueError):
                inspect_bundle(
                    data,
                    project_id="p",
                    expected_sha256=hashlib.sha256(data).hexdigest(),
                )
        with self.assertRaises(ValueError):
            inspect_bundle(valid, project_id="p", expected_sha256="0" * 64)
        for data in (bytearray(valid), b"x" * (8388608 + 1)):
            with self.assertRaises(ValueError):
                inspect_bundle(data, project_id="p", expected_sha256="0" * 64)
        for ref in (
            None,
            "main",
            "HEAD",
            "refs/heads/a..b",
            "refs/heads/a/",
            "refs/heads/.a",
            "refs/heads/a.lock",
            "refs/heads/a.",
            "refs/heads/a//b",
            "refs/heads/a~1",
        ):
            with self.assertRaises(ValueError):
                validate_branch(ref)
        for oid in (None, "a" * 39, "A" * 40, "0" * 40):
            with self.assertRaises(ValueError):
                validate_oid(oid)


class ExportTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory(prefix="bundle-owned-", dir="/tmp")
        self.addCleanup(root.cleanup)
        self.root = Path(root.name).resolve()
        self.repo = self.root / "source.git"
        self.ref, self.oid = create_repository(self.repo)
        self.output = self.root / "export.bundle"

    def export(self, **kwargs):
        return export_bundle(
            self.repo,
            self.output,
            project_id="p",
            ref=self.ref,
            expected_oid=self.oid,
            **kwargs,
        )

    def test_export_ignores_source_configuration_hooks_and_preserves_refs(self):
        marker = self.root / "executed"
        trap = self.root / "trap"
        trap.write_text(f"#!/bin/sh\ntouch {marker}\n")
        trap.chmod(0o700)
        with (self.repo / "config").open("a") as f:
            f.write(
                f'[core]\nhooksPath={trap}\nfsmonitor={trap}\n[filter "trap"]\nclean={trap}\nsmudge={trap}\n[alias]\nbundle={trap}\n'
            )
        before = (self.repo / self.ref).read_bytes()
        with patch.dict(
            os.environ,
            {
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "core.fsmonitor",
                "GIT_CONFIG_VALUE_0": str(trap),
                "GIT_OBJECT_DIRECTORY": "/not/selected",
            },
        ):
            info = self.export()
        self.assertEqual((self.repo / self.ref).read_bytes(), before)
        self.assertFalse(marker.exists())
        self.assertEqual(info.claimed_oid, self.oid)
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o600)
        # This verifies only our locally generated fixture bundle, never peer bytes.
        fixture_git(self.repo, "bundle", "verify", str(self.output))
        with self.assertRaises(FileExistsError):
            self.export()
        record = io.StringIO()
        with redirect_stdout(record):
            self.assertEqual(
                cli.main(
                    [
                        "bundle-inspect",
                        "--project",
                        "p",
                        "--file",
                        str(self.output),
                        "--sha256",
                        info.sha256,
                    ]
                ),
                0,
            )
        self.assertEqual(json.loads(record.getvalue()), info.to_dict())

    def test_pin_is_stable_when_source_branch_moves(self):
        real = _Git.run
        tree = fixture_git(self.repo, "rev-parse", self.oid + "^{tree}").decode()
        newer = fixture_git(
            self.repo, "commit-tree", tree, "-p", self.oid, data=b"new tip\n"
        ).decode()

        def move(git, *args, **kwargs):
            if args[0] == "pack-objects":
                fixture_git(self.repo, "update-ref", self.ref, newer)
            return real(git, *args, **kwargs)

        with patch.object(_Git, "run", move):
            info = self.export()
        self.assertEqual(info.claimed_oid, self.oid)
        self.assertEqual((self.repo / self.ref).read_text().strip(), newer)
        self.output.unlink()
        with self.assertRaises(ValueError):
            self.export()
        self.assertFalse(self.output.exists())

    def test_rejects_unsupported_sources_and_ambiguous_refs(self):
        for name in (
            "shallow",
            "commondir",
            "info/grafts",
            "objects/info/alternates",
            "objects/info/http-alternates",
            "objects/pack/x.promisor",
        ):
            path = self.repo / name
            path.parent.mkdir(exist_ok=True)
            path.write_text("")
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.export()
            path.unlink()
        config = (self.repo / "config").read_text()
        for extra in (
            "[extensions]\nobjectFormat=sha256\n",
            '[remote "origin"]\npromisor=true\n',
            '[remote "origin"]\npartialclonefilter=blob:none\n',
            "[include]\npath=/does/not/exist\n",
            "[core]\nrepositoryformatversion=1\n",
            "[bad syntax",
        ):
            (self.repo / "config").write_text(config + extra)
            with self.assertRaises(ValueError):
                self.export()
        (self.repo / "config").write_text(config)
        (self.repo / "packed-refs").write_text(
            f"{self.oid} {self.ref}\n{self.oid} {self.ref}\n"
        )
        with self.assertRaises(ValueError):
            self.export()
        (self.repo / "packed-refs").write_text(f"# pack-refs\n{self.oid} {self.ref}\n")
        (self.repo / self.ref).unlink()
        self.assertEqual(_selected_oid(self.repo, self.ref), self.oid)
        (self.repo / "packed-refs").write_text(f"{self.oid}  {self.ref}\n")
        with self.assertRaises(ValueError):
            self.export()
        (self.repo / "packed-refs").write_text(f"bad {self.ref}\n")
        with self.assertRaises(ValueError):
            self.export()
        (self.repo / "packed-refs").unlink()
        with self.assertRaises(ValueError):
            self.export()
        (self.repo / self.ref).write_text("ref: refs/heads/other\n")
        with self.assertRaises(ValueError):
            self.export()

    def test_paths_noncommit_and_object_store_bounds(self):
        alias = self.repo / "objects" / "alias"
        alias.symlink_to(self.root)
        with self.assertRaises(ValueError):
            self.export()
        alias.unlink()
        nested = self.repo / "objects" / "pack" / "nested"
        nested.mkdir(parents=True)
        with self.assertRaises(ValueError):
            self.export()
        nested.rmdir()
        entry = Mock()
        entry.name = "a"
        entry.stat.return_value = (self.repo / "HEAD").stat()
        entries = Mock()
        entries.__enter__ = Mock(return_value=iter([entry] * 10001))
        entries.__exit__ = Mock(return_value=False)
        with (
            patch(
                "aethermesh_core.network.bundle_export.os.scandir", return_value=entries
            ),
            self.assertRaises(ValueError),
        ):
            _objects(self.repo)
        blob = fixture_git(
            self.repo, "hash-object", "-w", "--stdin", data=b"not a commit"
        ).decode()
        (self.repo / self.ref).write_text(blob + "\n")
        with self.assertRaises(ValueError):
            export_bundle(
                self.repo, self.output, project_id="p", ref=self.ref, expected_oid=blob
            )
        normal = self.root / "normal"
        normal.mkdir()
        (normal / ".git").write_text("gitdir: elsewhere\n")
        with self.assertRaises(ValueError):
            export_bundle(
                normal, self.output, project_id="p", ref=self.ref, expected_oid=self.oid
            )
        (normal / ".git").unlink()
        self.repo.rename(normal / ".git")
        (normal / ".git" / self.ref).write_text(self.oid + "\n")
        self.assertEqual(
            export_bundle(
                normal, self.output, project_id="p", ref=self.ref, expected_oid=self.oid
            ).claimed_oid,
            self.oid,
        )

    def test_output_cannot_modify_source_metadata(self):
        before = (self.repo / self.ref).read_bytes()
        for target in (
            self.repo / "refs/heads/new",
            self.repo / "objects/new",
            self.repo / "new.bundle",
        ):
            with self.assertRaises(ValueError):
                export_bundle(
                    self.repo,
                    target,
                    project_id="p",
                    ref=self.ref,
                    expected_oid=self.oid,
                )
            self.assertFalse(target.exists())
        self.assertEqual((self.repo / self.ref).read_bytes(), before)

    def test_process_limits_failures_and_cli(self):
        for timeout in (True, 0, -1, 31, float("nan"), "1"):
            with self.assertRaises(ValueError):
                self.export(timeout=timeout)
        with patch("shutil.which", return_value=None), self.assertRaises(ValueError):
            self.export()
        with tempfile.TemporaryDirectory(dir=self.root) as root:
            runner = _Git(Path(root), 5)
            with self.assertRaises(ValueError):
                runner.run("version", limit=8)
            with (
                patch(
                    "subprocess.run", side_effect=subprocess.TimeoutExpired("git", 1)
                ),
                self.assertRaises(TimeoutError),
            ):
                runner.run("version")
            runner.deadline = 0
            with self.assertRaises(TimeoutError):
                runner.run("version")
        with self.assertRaises(TimeoutError):
            self.export(timeout=0.001)
        self.assertFalse(self.output.exists())
        args = [
            "bundle-export",
            "--project",
            "p",
            "--source",
            str(self.repo),
            "--ref",
            self.ref,
            "--expected-oid",
            self.oid,
            "--output",
            str(self.output),
        ]
        with redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(args), 0)
        errors = io.StringIO()
        with redirect_stderr(errors):
            self.assertEqual(cli.main(args), 1)
        self.assertEqual(json.loads(errors.getvalue()), {"error": "output_exists"})
