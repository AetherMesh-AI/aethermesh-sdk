"""Private, bounded, durable checkpoints and explicit restart behavior."""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from aethermesh_core.network import TransferStore
from aethermesh_core.network.content_files import selected_parent
from aethermesh_core.network.transfer_files import (
    _private,
    _read,
    _remove_orphan,
    _save,
    assemble,
    checkpoint,
    download_chunks,
    publish_transfer,
    verified_chunk,
)


class TransferFilesTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_rehashes_and_only_fetches_missing(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            blob = b"a" * 32768 + b"b" * 32768 + b"c"
            manifest = TransferStore("p", blob, max_served_bytes=100000).manifest
            stage = root / "stage"
            client = AsyncMock()
            client.fetch_chunk.side_effect = [blob[:32768], asyncio.CancelledError()]
            with checkpoint(stage, manifest, resume=False) as fd:
                with self.assertRaises(asyncio.CancelledError):
                    await download_chunks(client, manifest, fd, timeout=1)
                self.assertEqual(verified_chunk(fd, manifest, 0), blob[:32768])
            # Process-owned lock is released. A crash orphan is never a checkpoint.
            orphan = stage / (".aethermesh-content-" + "a" * 32)
            orphan.write_bytes(b"partial")
            orphan.chmod(0o600)
            client.fetch_chunk.reset_mock(side_effect=True)
            client.fetch_chunk.side_effect = [blob[32768:65536], b"c"]
            with checkpoint(stage, manifest, resume=True) as fd:
                self.assertEqual(
                    await download_chunks(client, manifest, fd, timeout=1), 2
                )
                self.assertEqual(
                    [call.args[1] for call in client.fetch_chunk.call_args_list], [1, 2]
                )
                self.assertEqual(assemble(fd, manifest), blob)
                self.assertEqual(
                    await download_chunks(client, manifest, fd, timeout=1), 0
                )
            self.assertFalse(orphan.exists())
            (stage / "001.chunk").write_bytes(b"corrupt")
            client.fetch_chunk.reset_mock(side_effect=True)
            client.fetch_chunk.return_value = blob[32768:65536]
            with checkpoint(stage, manifest, resume=True) as fd:
                self.assertEqual(
                    await download_chunks(client, manifest, fd, timeout=1), 1
                )
                self.assertEqual(client.fetch_chunk.call_args.args[1], 1)
                with selected_parent(root / "out") as (directory, name):
                    publish_transfer(directory, name, assemble(fd, manifest))
                    with self.assertRaises(FileExistsError):
                        publish_transfer(directory, name, b"x")
            self.assertEqual((root / "out").read_bytes(), blob)
            self.assertEqual((root / "out").stat().st_mode & 0o777, 0o600)

    async def test_private_paths_lock_and_manifest_binding(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            stage = root / "stage"
            manifest = TransferStore("p", b"x", max_served_bytes=1000).manifest
            with (
                checkpoint(stage, manifest, resume=False),
                self.assertRaises(BlockingIOError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass
            with (
                self.assertRaises(FileExistsError),
                checkpoint(stage, manifest, resume=False),
            ):
                pass
            alias = root / "alias"
            alias.symlink_to(stage)
            with self.assertRaises(OSError), checkpoint(alias, manifest, resume=True):
                pass
            with (
                self.assertRaises(ValueError),
                checkpoint(root / ".." / "bad", manifest, resume=False),
            ):
                pass
            other = TransferStore("q", b"x", max_served_bytes=1000).manifest
            with self.assertRaises(ValueError), checkpoint(stage, other, resume=True):
                pass
            (stage / "manifest.json").write_bytes(b"bad")
            with (
                self.assertRaises(ValueError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass
            (stage / "manifest.json").unlink()
            (stage / "000.chunk").write_bytes(b"x")
            with (
                self.assertRaises(ValueError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass
            (stage / "000.chunk").unlink()
            (stage / "unexpected").touch()
            with (
                self.assertRaises(ValueError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass
            (stage / "unexpected").unlink()
            for index in range(259):
                (stage / str(index)).touch()
            with (
                self.assertRaises(ValueError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass
            stage.chmod(0o755)
            with (
                self.assertRaises(ValueError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass

    async def test_bad_chunk_types_sizes_and_assembly(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            stage = root / "stage"
            manifest = TransferStore("p", b"x", max_served_bytes=1000).manifest
            with checkpoint(stage, manifest, resume=False) as fd:
                self.assertIsNone(_read(fd, "absent"))
                with self.assertRaises(ValueError):
                    assemble(fd, manifest)
                name = stage / "000.chunk"
                name.symlink_to(root / "outside")
                with self.assertRaises(OSError):
                    verified_chunk(fd, manifest, 0)
                name.unlink()
                os.mkfifo(name, 0o600)
                with self.assertRaises(ValueError):
                    _read(fd, "000.chunk")
                name.unlink()
                name.write_bytes(b"x")
                name.chmod(0o644)
                with self.assertRaises(ValueError):
                    _read(fd, "000.chunk")
                name.chmod(0o600)
                os.link(name, stage / "extra")
                with self.assertRaises(ValueError):
                    _read(fd, "000.chunk")
                (stage / "extra").unlink()
                name.write_bytes(b"x" * 32769)
                with self.assertRaises(ValueError):
                    _read(fd, "000.chunk")
                name.write_bytes(b"xx")
                self.assertIsNone(verified_chunk(fd, manifest, 0))
                _save(fd, "000.chunk", b"x")
                # Same-sized corrupt bytes are also discarded.
                name.write_bytes(b"y")
                self.assertIsNone(verified_chunk(fd, manifest, 0))
                _save(fd, "000.chunk", b"x")
                wrong = type(manifest)("p", "0" * 64, 1, manifest.chunks)
                with self.assertRaises(ValueError):
                    assemble(fd, wrong)
                with (
                    patch("os.getuid", return_value=os.getuid() + 1),
                    self.assertRaises(ValueError),
                ):
                    _private(name.stat())
                with patch(
                    "aethermesh_core.network.transfer_files.os.fdopen"
                ) as opened:
                    opened.return_value.__enter__.return_value.read.return_value = (
                        b"x" * 32769
                    )
                    with self.assertRaises(ValueError):
                        _read(fd, "000.chunk")
            empty = TransferStore("p", b"", max_served_bytes=0).manifest
            with checkpoint(root / "empty", empty, resume=False) as fd:
                self.assertEqual(assemble(fd, empty), b"")

    async def test_anchored_directory_race_and_fsync_failure(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            stage = root / "stage"
            manifest = TransferStore("p", b"x", max_served_bytes=1000).manifest
            with checkpoint(stage, manifest, resume=False) as fd:
                stage.rename(root / "moved")
                stage.symlink_to(root / "elsewhere")
                _save(fd, "000.chunk", b"x")
                self.assertEqual(assemble(fd, manifest), b"x")
            self.assertEqual((root / "moved" / "000.chunk").read_bytes(), b"x")
            with selected_parent(root / "out") as (fd, name):
                # Fsync failure before the link leaves no final file.
                with (
                    patch("os.fsync", side_effect=OSError("disk")),
                    self.assertRaises(OSError),
                ):
                    publish_transfer(fd, name, b"x")
                self.assertFalse((root / "out").exists())
                # Failure after link leaves complete output, never a rollback claim.
                with (
                    patch("os.fsync", side_effect=[None, OSError("directory")]),
                    self.assertRaises(OSError),
                ):
                    publish_transfer(fd, name, b"x")
                self.assertEqual((root / "out").read_bytes(), b"x")


class CheckpointCrashTests(unittest.TestCase):
    def test_published_double_link_recovers_but_external_link_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            manifest = TransferStore("p", b"x", max_served_bytes=1000).manifest
            stage = root / "stage"
            with checkpoint(stage, manifest, resume=False) as fd:
                _save(fd, "000.chunk", b"x")
            pending = stage / (".aethermesh-content-" + "0" * 32)
            os.link(stage / "000.chunk", pending)
            with checkpoint(stage, manifest, resume=True) as fd:
                self.assertEqual(assemble(fd, manifest), b"x")
            self.assertFalse(pending.exists())
            os.link(stage / "manifest.json", pending)
            with checkpoint(stage, manifest, resume=True) as fd:
                self.assertEqual(assemble(fd, manifest), b"x")
            self.assertFalse(pending.exists())
            external = root / "outside"
            external.write_bytes(b"x")
            external.chmod(0o600)
            os.link(external, pending)
            with (
                self.assertRaises(ValueError),
                checkpoint(stage, manifest, resume=True),
            ):
                pass
            self.assertEqual(external.read_bytes(), b"x")
            with selected_parent(pending) as (fd, name), self.assertRaises(ValueError):
                _remove_orphan(fd, name, {"missing"})
