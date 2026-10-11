"""No-follow snapshots and atomic no-overwrite publication of verified content."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from aethermesh_core.network.content_files import (
    publish_new,
    read_selected,
    require_absent,
    selected_parent,
)


class ContentFileTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory(prefix="content-paths-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()

    def test_binary_and_empty_snapshots_and_owner_only_output(self) -> None:
        for data in (b"", bytes(range(256)) * 128):
            source = self.root / "input"
            source.write_bytes(data)
            self.assertEqual(read_selected(source), data)
            output = self.root / "new"
            with selected_parent(output) as (parent, name):
                require_absent(parent, name)
                publish_new(parent, name, data)
            self.assertEqual(output.read_bytes(), data)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            output.unlink()
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ["input"])

    def test_reject_symlink_components_traversal_and_nonregular_input(self) -> None:
        selected = self.root / "input"
        selected.write_bytes(b"selected")
        link = self.root / "link"
        link.symlink_to(selected)
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        oversized = self.root / "large"
        oversized.write_bytes(b"x" * 32769)
        for path in (
            link,
            alias / "input",
            self.root / ".." / "input",
            self.root,
            fifo,
            oversized,
        ):
            with self.subTest(path=path.name), self.assertRaises((OSError, ValueError)):
                read_selected(path)
        for path in (Path("."), Path("/"), self.root / ".." / "new", alias / "new"):
            with (
                self.subTest(path=str(path)),
                self.assertRaises((OSError, ValueError)),
                selected_parent(path),
            ):
                self.fail("unsafe path accepted")
        self.assertEqual(selected.read_bytes(), b"selected")

    def test_no_overwrite_even_when_created_after_preflight(self) -> None:
        output = self.root / "new"
        with selected_parent(output) as (parent, name):
            require_absent(parent, name)
            output.write_bytes(b"keep")
            with self.assertRaises(FileExistsError):
                publish_new(parent, name, b"replacement")
            with self.assertRaises(FileExistsError):
                require_absent(parent, name)
        self.assertEqual(output.read_bytes(), b"keep")
        output.unlink()
        output.symlink_to(self.root / "missing")
        with selected_parent(output) as (parent, name):
            with self.assertRaises(FileExistsError):
                require_absent(parent, name)
            with self.assertRaises(FileExistsError):
                publish_new(parent, name, b"replacement")
        self.assertTrue(output.is_symlink())
        self.assertFalse((self.root / "missing").exists())
        self.assertEqual(list(self.root.iterdir()), [output])

    def test_parent_descriptor_pins_directory_across_replacement(self) -> None:
        original = self.root / "original"
        original.mkdir()
        replacement = self.root / "replacement"
        replacement.mkdir()
        with selected_parent(original / "new") as (parent, name):
            moved = self.root / "moved"
            original.rename(moved)
            original.symlink_to(replacement, target_is_directory=True)
            publish_new(parent, name, b"verified")
        self.assertEqual((moved / "new").read_bytes(), b"verified")
        self.assertEqual(list(replacement.iterdir()), [])

    def test_failed_publication_cleans_temporary_and_preserves_destination(
        self,
    ) -> None:
        output = self.root / "new"
        with selected_parent(output) as (parent, name):
            for operation in ("fsync", "link"):
                with (
                    patch(f"os.{operation}", side_effect=OSError("failure")),
                    self.assertRaises(OSError),
                ):
                    publish_new(parent, name, b"verified")
                self.assertEqual(list(self.root.iterdir()), [])
            with self.assertRaises(ValueError):
                publish_new(parent, name, b"x" * 32769)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_growth_is_bounded_and_unsupported_platform_fails_closed(self) -> None:
        source = self.root / "growing"
        source.write_bytes(b"x" * 32769)
        observed = Mock(st_mode=stat.S_IFREG, st_size=0)
        with patch("os.fstat", return_value=observed), self.assertRaises(ValueError):
            read_selected(source)
        with (
            patch("os.supports_dir_fd", set()),
            self.assertRaises(ValueError),
            selected_parent(source),
        ):
            self.fail("unsafe fallback")

    def test_relative_directory_path(self) -> None:
        path = self.root / "input"
        path.write_bytes(b"relative")
        relative = Path(os.path.relpath(path, self.root.parent))
        fd = os.open(self.root.parent, os.O_RDONLY)
        # Open the relative anchor without changing global process cwd.
        original_open = os.open

        def anchor(name: object, flags: int, *args: object, **kwargs: object) -> int:
            if name == ".":
                return os.dup(fd)
            return original_open(name, flags, *args, **kwargs)

        try:
            with (
                patch("os.open", side_effect=anchor) as opened,
                patch("os.supports_dir_fd", {opened, os.link}),
            ):
                self.assertEqual(read_selected(relative), b"relative")
        finally:
            os.close(fd)

    def test_wrapper_constructor_failure_closes_raw_descriptors(self) -> None:
        source = self.root / "input"
        source.write_bytes(b"selected")
        with (
            patch("os.fdopen", side_effect=OSError("wrapper failed")) as wrapper,
            self.assertRaises(OSError),
        ):
            read_selected(source)
        with self.assertRaises(OSError):
            os.fstat(wrapper.call_args.args[0])
        with selected_parent(self.root / "output") as (parent, name):
            with (
                patch("os.fdopen", side_effect=OSError("wrapper failed")) as wrapper,
                self.assertRaises(OSError),
            ):
                publish_new(parent, name, b"verified")
            with self.assertRaises(OSError):
                os.fstat(wrapper.call_args.args[0])
        self.assertEqual(list(self.root.iterdir()), [source])
