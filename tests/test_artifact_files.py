"""Bounded file observation, admission ordering and mutation rejection."""

import dataclasses
import os
import tempfile
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.artifact_files import verify_fragment_file
from tests.test_artifact_codec import package


class FragmentFileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.pkg = package(bytes(range(256)) * 513)
        self.path = self.root / "fragment"
        self.path.write_bytes(self.pkg.fragments[4])

    def verify(self, **kwargs):
        args = {
            "expected_digest": self.pkg.manifest.digest,
            "max_read_bytes": len(self.pkg.fragments[4]),
        }
        return verify_fragment_file(self.pkg.manifest, 4, self.path, **(args | kwargs))

    def test_short_reads_keep_exact_transport_boundaries(self):
        read = os.read
        requests = []

        def short(fd, count):
            requests.append(count)
            return read(fd, min(count, 101))

        with patch("aethermesh_core.artifact_files.os.read", side_effect=short):
            result = self.verify()
        self.assertEqual(
            result.transfer,
            self.pkg.manifest.transfer_manifest(4, self.pkg.fragments[4]),
        )
        self.assertEqual(result.bytes_read, len(self.pkg.fragments[4]))
        self.assertTrue(result.integrity_verified)
        self.assertFalse(result.publisher_authorized)
        self.assertFalse(result.retention_confirmed)
        self.assertLessEqual(max(requests), 32768)
        self.assertGreater(len(requests), 2)

    def test_budget_and_cancellation_admitted_before_open(self):
        for kwargs in (
            {"max_read_bytes": 1},
            {"max_read_bytes": True},
            {"expected_digest": "0" * 64},
        ):
            with (
                self.subTest(kwargs=kwargs),
                patch("aethermesh_core.artifact_files.selected_parent") as opened,
            ):
                with self.assertRaises(ValueError):
                    self.verify(**kwargs)
                opened.assert_not_called()

        def cancel():
            raise InterruptedError("cancelled")

        with patch("aethermesh_core.artifact_files.selected_parent") as opened:
            with self.assertRaises(InterruptedError):
                self.verify(check_cancel=cancel)
            opened.assert_not_called()

    def test_wrong_file_size_and_fifo_fail_before_read(self):
        for content in (b"", self.pkg.fragments[4] + b"extra"):
            self.path.write_bytes(content)
            with patch("aethermesh_core.artifact_files.os.read") as read:
                with self.assertRaisesRegex(ValueError, "exact size"):
                    self.verify()
                read.assert_not_called()
        self.path.unlink()
        os.mkfifo(self.path)
        with patch("aethermesh_core.artifact_files.os.read") as read:
            with self.assertRaisesRegex(ValueError, "regular file"):
                self.verify()
            read.assert_not_called()

    def test_symlinks_and_parent_traversal_rejected(self):
        target = self.root / "target"
        self.path.rename(target)
        self.path.symlink_to(target)
        with self.assertRaises(OSError):
            self.verify()
        self.path.unlink()
        directory = self.root / "real"
        directory.mkdir()
        (directory / "file").write_bytes(self.pkg.fragments[4])
        alias = self.root / "alias"
        alias.symlink_to(directory, target_is_directory=True)
        self.path = alias / "file"
        with self.assertRaises(OSError):
            self.verify()
        self.path = directory / ".." / "target"
        with self.assertRaises(ValueError):
            self.verify()

    def test_corruption_and_transfer_binding_rejected(self):
        self.path.write_bytes(b"!" * len(self.pkg.fragments[4]))
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.verify()
        self.path.write_bytes(self.pkg.fragments[4])
        refs = list(self.pkg.manifest.fragments)
        refs[4] = (refs[4][0], "0" * 64)
        manifest = dataclasses.replace(self.pkg.manifest, fragments=tuple(refs))
        with self.assertRaisesRegex(ValueError, "transfer binding"):
            verify_fragment_file(
                manifest,
                4,
                self.path,
                expected_digest=manifest.digest,
                max_read_bytes=5_000_000,
            )

    def test_truncation_replacement_and_midread_cancellation_close_fd(self):
        for operation in ("truncate", "replace", "cancel"):
            self.path.write_bytes(self.pkg.fragments[4])
            calls = []

            def change(operation=operation, calls=calls):
                calls.append(1)
                if len(calls) == 3:
                    if operation == "truncate":
                        os.truncate(self.path, 0)
                    elif operation == "replace":
                        replacement = self.root / "replacement"
                        replacement.write_bytes(self.pkg.fragments[4])
                        replacement.replace(self.path)
                    else:
                        raise InterruptedError("cancelled")

            close = os.close
            closed = []

            def record(fd, closed=closed, close=close):
                closed.append(fd)
                close(fd)

            with (
                self.subTest(operation=operation),
                patch("aethermesh_core.artifact_files.os.close", side_effect=record),
                self.assertRaises((ValueError, InterruptedError)),
            ):
                self.verify(check_cancel=change)
            self.assertTrue(closed)
            for fd in closed:
                with self.assertRaises(OSError):
                    os.fstat(fd)

    def test_actual_input_descriptor_closes_on_read_and_stat_failures(self):
        read = os.read
        for mode in ("read", "stat"):
            observed = []

            def capture(fd, count, mode=mode, observed=observed):
                observed.append(fd)
                if mode == "read":
                    raise OSError("injected read error")
                return read(fd, count)

            with patch("aethermesh_core.artifact_files.os.read", side_effect=capture):
                if mode == "stat":
                    with (
                        patch(
                            "aethermesh_core.artifact_files.os.stat",
                            side_effect=OSError("injected stat error"),
                        ),
                        self.assertRaises(OSError),
                    ):
                        self.verify()
                else:
                    with self.assertRaises(OSError):
                        self.verify()
            self.assertTrue(observed)
            for fd in set(observed):
                with self.assertRaises(OSError):
                    os.fstat(fd)

    def test_final_return_cancellation_closes_actual_input(self):
        from aethermesh_core.artifact_files import _read_fragment

        completed = []
        descriptors = []

        def observe(fd, *args):
            descriptors.append(fd)
            result = _read_fragment(fd, *args)
            completed.append(True)
            return result

        def cancel():
            if completed:
                raise InterruptedError("cancel just before result")

        with (
            patch("aethermesh_core.artifact_files._read_fragment", side_effect=observe),
            self.assertRaises(InterruptedError),
        ):
            self.verify(check_cancel=cancel)
        self.assertEqual(len(descriptors), 1)
        with self.assertRaises(OSError):
            os.fstat(descriptors[0])

    def test_same_size_mutation_of_already_read_bytes_is_rejected(self):
        read = os.read
        changed = []
        descriptors = []

        def mutate(fd, count):
            descriptors.append(fd)
            data = read(fd, count)
            if not changed:
                changed.append(True)
                with self.path.open("r+b") as stream:
                    stream.write(bytes([self.pkg.fragments[4][0] ^ 1]))
            return data

        with (
            patch("aethermesh_core.artifact_files.os.read", side_effect=mutate),
            self.assertRaisesRegex(ValueError, "changed"),
        ):
            self.verify()
        self.assertTrue(descriptors)
        with self.assertRaises(OSError):
            os.fstat(descriptors[0])

    def test_maximum_current_fragment_has_bounded_python_allocations(self):
        pkg = package(bytes(range(256)) * 32768)
        self.path.write_bytes(pkg.fragments[0])
        root = pkg.manifest.digest
        tracemalloc.start()
        try:
            result = verify_fragment_file(
                pkg.manifest,
                0,
                self.path,
                expected_digest=root,
                max_read_bytes=5_000_000,
            )
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(result.bytes_read, 2 * 1024 * 1024)
        self.assertLess(peak, 512 * 1024)
        # Observation grants no future capability: changing the file invalidates it.
        self.path.write_bytes(b"!" * result.bytes_read)
        with self.assertRaises(ValueError):
            verify_fragment_file(
                pkg.manifest,
                0,
                self.path,
                expected_digest=root,
                max_read_bytes=5_000_000,
            )
