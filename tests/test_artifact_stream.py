"""Streaming recovery, quota concurrency, cleanup and publication fault evidence."""

import dataclasses
import errno
import itertools
import os
import sys
import tempfile
import threading
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.artifact_stream import StagingQuota, publish_artifact_files
from tests.test_artifact_codec import package


class StreamRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.inputs = self.root / "inputs"
        self.inputs.mkdir()
        self.pkg = package(bytes(range(256)) * 1024 + b"x")
        self.quota = StagingQuota(max_bytes=8 * 1024 * 1024, max_jobs=1)
        self.paths = self.store(self.pkg)

    def store(self, pkg):
        paths = []
        for i, content in enumerate(pkg.fragments):
            path = self.inputs / str(i)
            path.write_bytes(content)
            paths.append((i, path))
        return tuple(paths)

    def selected(self):
        return tuple(entry for entry in self.paths if entry[0] % 6 in (0, 2, 4, 5))

    def publish(self, **kwargs):
        args = {
            "expected_digest": self.pkg.manifest.digest,
            "max_read_bytes": 32 * 1024 * 1024,
            "staging": self.quota,
        }
        return publish_artifact_files(
            self.pkg.manifest, self.selected(), self.root / "out", **(args | kwargs)
        )

    def assert_clean(self):
        self.assertEqual(list(self.root.iterdir()), [self.inputs])
        self.assertEqual((self.quota.usage().bytes, self.quota.usage().jobs), (0, 0))

    def test_all_subsets_multistripe_and_padding(self):
        for indices in itertools.combinations(range(6), 4):
            with self.subTest(indices=indices):
                result = publish_artifact_files(
                    self.pkg.manifest,
                    tuple(self.paths[i] for i in indices),
                    self.root / "out",
                    expected_digest=self.pkg.manifest.digest,
                    max_read_bytes=32 * 1024 * 1024,
                    staging=self.quota,
                )
                self.assertEqual(
                    result.output.read_bytes(), bytes(range(256)) * 1024 + b"x"
                )
                self.assertEqual(result.output.stat().st_mode & 0o777, 0o600)
                self.assertFalse(result.publisher_authorized)
                result.output.unlink()
        self.pkg = package(bytes(range(39)), fragment_bytes=3)
        self.paths = self.store(self.pkg)
        self.assertEqual(self.publish().output.read_bytes(), bytes(range(39)))
        self.assertFalse(self.quota.usage().blocked)

    def test_admission_before_file_io_and_exact_read_budget(self):
        cost = (
            2 * sum(self.pkg.manifest.fragment_size(i) for i, _ in self.selected())
            + self.pkg.manifest.source_size
        )
        with patch("aethermesh_core.artifact_stream.selected_parent") as opened:
            with self.assertRaisesRegex(ValueError, "read budget"):
                self.publish(max_read_bytes=cost - 1)
            opened.assert_not_called()
        with (
            self.quota.reserve(8 * 1024 * 1024),
            patch("aethermesh_core.artifact_stream.selected_parent") as opened,
        ):
            with self.assertRaisesRegex(ValueError, "quota"):
                self.publish()
            opened.assert_not_called()
        result = self.publish(max_read_bytes=cost)
        self.assertTrue(result.integrity_verified)

    def test_selection_and_policy_rejections(self):
        for selected in (
            (),
            self.paths,
            list(self.selected()),
            (self.paths[0],) * 4,
            ((True, self.paths[0][1]),) + self.selected()[1:],
            ((0, "path"),) + self.selected()[1:],
            ((0,),) + self.selected()[1:],
        ):
            with self.subTest(selected=selected), self.assertRaises(ValueError):
                publish_artifact_files(
                    self.pkg.manifest,
                    selected,
                    self.root / "out",
                    expected_digest=self.pkg.manifest.digest,
                    max_read_bytes=32 * 1024 * 1024,
                    staging=self.quota,
                )
        with self.assertRaises(TypeError):
            self.publish(staging=None)
        with self.assertRaises(ValueError):
            StagingQuota(max_bytes=True, max_jobs=1)
        self.assert_clean()

    def test_corrupt_inputs_are_rejected_before_decode(self):
        self.paths[0][1].write_bytes(b"!" * len(self.pkg.fragments[0]))
        with patch("aethermesh_core.artifact_stream._decode_stripe") as decode:
            with self.assertRaisesRegex(ValueError, "input fragment integrity"):
                self.publish()
            decode.assert_not_called()
        self.assert_clean()

    def test_unsafe_inputs_and_read_failure_close_actual_descriptor(self):
        first = self.paths[0][1]
        for mode in ("size", "fifo", "symlink"):
            first.unlink()
            if mode == "size":
                first.write_bytes(b"bad")
            elif mode == "fifo":
                os.mkfifo(first)
            else:
                first.symlink_to(self.paths[2][1])
            with self.subTest(mode=mode), self.assertRaises((ValueError, OSError)):
                self.publish()
            self.assert_clean()
        first.unlink()
        self.paths = self.store(self.pkg)
        observed = []

        def fail(fd, count):
            observed.append(fd)
            raise OSError("read failed")

        with (
            patch("aethermesh_core.artifact_stream.os.read", side_effect=fail),
            self.assertRaises(OSError),
        ):
            self.publish()
        self.assertTrue(observed)
        with self.assertRaises(OSError):
            os.fstat(observed[0])
        self.assert_clean()

    def test_publication_race_preserves_competing_destination(self):
        fsync = os.fsync

        def compete(fd):
            fsync(fd)
            (self.root / "out").write_bytes(b"competitor")

        with (
            patch("aethermesh_core.artifact_stream.os.fsync", side_effect=compete),
            self.assertRaises(FileExistsError),
        ):
            self.publish()
        self.assertEqual((self.root / "out").read_bytes(), b"competitor")
        self.assertFalse(list(self.root.glob(".aethermesh-stage-*")))
        self.assertEqual(self.quota.usage().jobs, 0)

    def test_unselected_reference_and_nonzero_padding_rejected(self):
        refs = list(self.pkg.manifest.fragments)
        refs[1] = ("0" * 64, refs[1][1])
        bad = dataclasses.replace(self.pkg.manifest, fragments=tuple(refs))
        with self.assertRaisesRegex(ValueError, "decoded stripe integrity"):
            publish_artifact_files(
                bad,
                self.selected(),
                self.root / "out",
                expected_digest=bad.digest,
                max_read_bytes=32 * 1024 * 1024,
                staging=self.quota,
            )
        self.assert_clean()
        original = package(b"abcd")
        self.paths = self.store(original)
        self.pkg = dataclasses.replace(
            original, manifest=dataclasses.replace(original.manifest, source_size=1)
        )
        with self.assertRaisesRegex(ValueError, "padding"):
            self.publish()
        self.assert_clean()

    def test_cleanup_interruption_blocks_new_admission(self):
        with (
            patch(
                "aethermesh_core.artifact_stream.os.unlink",
                side_effect=KeyboardInterrupt("interrupted cleanup"),
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.publish()
        self.assertTrue(self.quota.usage().blocked)
        with self.assertRaises(ValueError), self.quota.reserve(1):
            self.fail("uncertain cleanup allowed admission")
        for path in self.root.glob(".aethermesh-stage-*"):
            path.unlink()

    def test_staged_output_mutation_during_final_verification_rejected(self):
        from aethermesh_core.artifact_stream import _read_fragment

        calls = []

        def mutate(fd, *args):
            result = _read_fragment(fd, *args)
            calls.append(fd)
            if len(calls) == 5:
                os.pwrite(fd, b"!", 0)
            return result

        with (
            patch("aethermesh_core.artifact_stream._read_fragment", side_effect=mutate),
            self.assertRaisesRegex(ValueError, "staged original changed"),
        ):
            self.publish()
        self.assert_clean()

    def test_interrupt_after_owned_creation_closes_specific_descriptor(self):
        observed = []

        def interrupt(frame, event, arg):
            if (
                event == "line"
                and frame.f_code is publish_artifact_files.__code__
                and type(frame.f_locals.get("fd")) is int
            ):
                observed.append(
                    (frame.f_locals["fd"], self.root / frame.f_locals["temporary"])
                )
                raise KeyboardInterrupt("after descriptor assignment")
            return interrupt

        previous_trace = sys.gettrace()
        try:
            sys.settrace(interrupt)
            with self.assertRaises(KeyboardInterrupt):
                self.publish()
        finally:
            sys.settrace(previous_trace)
        self.assertEqual(len(observed), 1)
        fd, path = observed[0]
        with self.assertRaises(OSError):
            os.fstat(fd)
        self.assertFalse(path.exists())
        self.assertFalse(self.quota.usage().blocked)
        self.assert_clean()

    def test_unknown_creation_ownership_blocks_admission_and_preserves_path(self):
        opened = []
        real_open = os.open

        def interrupted(name, flags, *args, **kwargs):
            fd = real_open(name, flags, *args, **kwargs)
            if str(name).startswith(".aethermesh-stage-"):
                opened.append((fd, self.root / name))
                raise KeyboardInterrupt("descriptor not returned to caller")
            return fd

        # Preserve the directory-safety capability check while wrapping open.
        with (
            patch(
                "aethermesh_core.artifact_stream.os.open", side_effect=interrupted
            ) as mock_open,
            patch("os.supports_dir_fd", os.supports_dir_fd | {mock_open}),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.publish()
        self.assertEqual(len(opened), 1)
        fd, path = opened[0]
        try:
            self.assertEqual(os.fstat(fd).st_ino, path.stat().st_ino)
            self.assertTrue(self.quota.usage().blocked)
            with self.assertRaisesRegex(ValueError, "quota"):
                self.publish()
            self.assertFalse((self.root / "out").exists())
        finally:
            # Only the injector knows this otherwise-unidentified descriptor.
            os.close(fd)
            path.unlink()

    def test_creation_collision_preserves_unowned_file_and_blocks_quota(self):
        path = self.root / ".aethermesh-stage-collision"
        path.write_bytes(b"unowned")
        with (
            patch(
                "aethermesh_core.artifact_stream.secrets.token_hex",
                return_value="collision",
            ),
            self.assertRaises(FileExistsError),
        ):
            self.publish()
        self.assertEqual(path.read_bytes(), b"unowned")
        self.assertTrue(self.quota.usage().blocked)
        self.assertFalse((self.root / "out").exists())

    def test_partial_reads_and_writes(self):
        read, write = os.read, os.pwrite

        def short_read(fd, count):
            return read(fd, min(count, 101))

        def short_write(fd, data, offset):
            return write(fd, data[:113], offset)

        with (
            patch("aethermesh_core.artifact_stream.os.read", side_effect=short_read),
            patch("aethermesh_core.artifact_stream.os.pwrite", side_effect=short_write),
        ):
            result = self.publish()
        self.assertEqual(result.output.read_bytes(), bytes(range(256)) * 1024 + b"x")

    def test_disk_error_and_final_cancellation_leave_no_output(self):
        for failure in (OSError(errno.ENOSPC, "full"), 0):
            with (
                patch(
                    "aethermesh_core.artifact_stream.os.pwrite",
                    side_effect=failure if isinstance(failure, OSError) else None,
                    return_value=failure,
                ),
                self.assertRaises(OSError),
            ):
                self.publish()
            self.assert_clean()
        synced = []
        fsync = os.fsync

        def mark(fd):
            fsync(fd)
            synced.append(True)

        def cancel():
            if synced:
                raise InterruptedError("cancelled before link")

        with (
            patch("aethermesh_core.artifact_stream.os.fsync", side_effect=mark),
            self.assertRaises(InterruptedError),
        ):
            self.publish(check_cancel=cancel)
        self.assert_clean()

    def test_same_size_change_after_verification_and_bad_original(self):
        write = os.pwrite
        changed = []

        def mutate(fd, data, offset):
            if not changed:
                changed.append(True)
                self.paths[0][1].write_bytes(b"!" * len(self.pkg.fragments[0]))
            return write(fd, data, offset)

        with (
            patch("aethermesh_core.artifact_stream.os.pwrite", side_effect=mutate),
            self.assertRaises(ValueError),
        ):
            self.publish()
        self.assert_clean()
        self.paths = self.store(self.pkg)
        self.pkg = dataclasses.replace(
            self.pkg,
            manifest=dataclasses.replace(self.pkg.manifest, source_sha256="0" * 64),
        )
        with self.assertRaisesRegex(ValueError, "original integrity"):
            self.publish()
        self.assert_clean()

    def test_no_overwrite_and_cleanup_uncertainty_blocks_admission(self):
        result = self.publish()
        with self.assertRaises(FileExistsError):
            self.publish()
        original = result.output.read_bytes()
        result.output.unlink()
        unlink = os.unlink

        def fail_stage(name, *args, **kwargs):
            if str(name).startswith(".aethermesh-stage-"):
                raise OSError("cleanup failed")
            return unlink(name, *args, **kwargs)

        with (
            patch("aethermesh_core.artifact_stream.os.unlink", side_effect=fail_stage),
            self.assertRaises(OSError),
        ):
            self.publish()
        self.assertTrue(self.quota.usage().blocked)
        self.assertEqual((self.root / "out").read_bytes(), original)
        with self.assertRaisesRegex(ValueError, "quota"):
            self.publish()
        for path in self.root.glob(".aethermesh-stage-*"):
            path.unlink()

    def test_quota_serializes_competing_reservations(self):
        for quota in (
            StagingQuota(max_bytes=100, max_jobs=1),
            StagingQuota(max_bytes=1, max_jobs=4),
        ):
            entered = threading.Event()
            release = threading.Event()

            def holder(quota=quota, entered=entered, release=release):
                with quota.reserve(1):
                    entered.set()
                    release.wait(5)

            thread = threading.Thread(target=holder)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                with self.assertRaises(ValueError), quota.reserve(1):
                    self.fail("over-admitted")
            finally:
                release.set()
                thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(quota.usage().jobs, 0)

    def test_maximum_artifact_bounded_python_allocations(self):
        self.pkg = package(bytes(range(256)) * 32768)
        self.paths = self.store(self.pkg)
        tracemalloc.start()
        try:
            result = self.publish()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 2 * 1024 * 1024)
        self.assertEqual(result.output.stat().st_size, 8 * 1024 * 1024)
        self.assertEqual(result.output.read_bytes(), bytes(range(256)) * 32768)
