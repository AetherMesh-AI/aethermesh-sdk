"""Opaque erasure profile interoperability, rejection, and publication checks."""

import dataclasses
import hashlib
import itertools
import json
import tempfile
import unittest
from pathlib import Path

from aethermesh_core.artifact_codec import (
    ArtifactLimits,
    ArtifactManifest,
    RepairLimits,
    package_artifact,
    publish_artifact,
    reconstruct_artifact,
    repair_artifact_fragments,
)
from aethermesh_core.network import TransferStore


def package(data=bytes(range(16)), **kwargs):
    return package_artifact(
        data,
        project="artifact",
        kind="dataset",
        data_class="synthetic",
        expires_at=100,
        limits=ArtifactLimits(**kwargs),
    )


def supplied(pkg, indices=None):
    return tuple(
        (i, pkg.fragments[i])
        for i in (range(len(pkg.fragments)) if indices is None else indices)
    )


class ArtifactCodecTests(unittest.TestCase):
    def test_independent_known_answer_and_every_four_of_six(self):
        pkg = package()
        # Literal bit-serial GF(0x11d) / Vandermonde oracle, independent of codec.
        self.assertEqual(
            pkg.fragments,
            tuple(
                bytes(row)
                for row in (
                    (0, 1, 2, 3),
                    (4, 5, 6, 7),
                    (8, 9, 10, 11),
                    (12, 13, 14, 15),
                    (20, 21, 22, 23),
                    (67, 66, 65, 64),
                )
            ),
        )
        for indices in itertools.combinations(range(6), 4):
            with self.subTest(indices=indices):
                self.assertEqual(
                    reconstruct_artifact(
                        pkg.manifest,
                        supplied(pkg, indices),
                        expected_digest=pkg.manifest.digest,
                    ),
                    bytes(range(16)),
                )

    def test_multiple_stripes_padding_and_identical_hashes(self):
        for data in (bytes(range(39)), b"\x00" * 39, b"x"):
            pkg = package(data, fragment_bytes=3)
            indices = tuple(
                i
                for i in range(len(pkg.fragments))
                if i % 6 not in ((i // 6) % 6, (i // 6 + 1) % 6)
            )
            self.assertEqual(
                reconstruct_artifact(
                    pkg.manifest,
                    supplied(pkg, indices),
                    expected_digest=pkg.manifest.digest,
                ),
                data,
            )
            self.assertTrue(all(len(x) <= 3 for x in pkg.fragments))
            self.assertEqual(
                ArtifactManifest.decode(
                    pkg.manifest.encode(),
                    project="artifact",
                    expected_digest=pkg.manifest.digest,
                ),
                pkg.manifest,
            )

    def test_manifest_rejects_untrusted_shapes_versions_and_encodings(self):
        pkg = package()
        value = json.loads(pkg.manifest.encode())
        for changes in (
            {"version": True},
            {"version": 2},
            {"codec": "other"},
            {"project": "other"},
            {"source_size": True},
            {"source_size": 0},
            {"fragment_bytes": 5_000_001},
            {"fragments": []},
            {"fragments": [None]},
            {"fragments": [["0" * 64, "0" * 64]] * 193},
            {"kind": "private"},
            {"data_class": "private"},
            {"expires_at": False},
            {"source_sha256": None},
        ):
            raw = json.dumps(
                value | changes, sort_keys=True, separators=(",", ":")
            ).encode()
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ArtifactManifest.decode(
                    raw,
                    project="artifact",
                    expected_digest=hashlib.sha256(raw).hexdigest(),
                )
        for raw in (
            b"[]",
            b"{}",
            b"null",
            b"\xff",
            b"{",
            json.dumps(value, indent=2).encode(),
            b" " * 32769,
        ):
            with self.subTest(raw=raw[:20]), self.assertRaises(ValueError):
                ArtifactManifest.decode(
                    raw,
                    project="artifact",
                    expected_digest=hashlib.sha256(raw).hexdigest(),
                )
        with self.assertRaises(ValueError):
            ArtifactManifest.decode(
                pkg.manifest.encode(), project="artifact", expected_digest="0" * 64
            )

    def test_integrity_duplicate_and_insufficient_rejections(self):
        pkg = package()
        cases = (
            (),
            supplied(pkg, (0, 1, 2)),
            supplied(pkg, (0, 0, 1, 2)),
            ((0, b"bad"),),
            ((True, pkg.fragments[0]),),
            supplied(pkg) + ((0, pkg.fragments[0]),),
            ((0,),),
            [supplied(pkg)[0]],
        )
        for fragments in cases:
            with self.subTest(fragments=fragments), self.assertRaises(ValueError):
                reconstruct_artifact(
                    pkg.manifest, fragments, expected_digest=pkg.manifest.digest
                )
        for manifest in (
            dataclasses.replace(pkg.manifest, source_sha256="0" * 64),
            dataclasses.replace(
                pkg.manifest,
                fragments=((pkg.manifest.fragments[0][0], "0" * 64),)
                + pkg.manifest.fragments[1:],
            ),
            dataclasses.replace(
                pkg.manifest,
                fragments=pkg.manifest.fragments[:5] + (("0" * 64, "0" * 64),),
            ),
        ):
            with self.assertRaises(ValueError):
                reconstruct_artifact(
                    manifest,
                    supplied(pkg, (0, 1, 2, 3)),
                    expected_digest=manifest.digest,
                )
        with self.assertRaises(ValueError):
            reconstruct_artifact(pkg.manifest, supplied(pkg), expected_digest="0" * 64)

    def test_budgets_cancellation_and_atomic_publication(self):
        for kwargs in (
            {"max_source_bytes": 1},
            {"max_encoded_bytes": 1},
            {"fragment_bytes": 1, "max_fragments": 6},
        ):
            with self.assertRaises(ValueError):
                package(bytes(range(32)), **kwargs)
        for kwargs in ({"fragment_bytes": True}, {"max_fragments": 193}):
            with self.assertRaises(ValueError):
                ArtifactLimits(**kwargs)
        for data in (b"", bytearray(b"x")):
            with self.assertRaises(ValueError):
                package(data)
        pkg = package(b"x" * 140_000)
        calls = []

        def cancel():
            calls.append(1)
            if len(calls) == 3:
                raise InterruptedError("cancelled")

        with self.assertRaises(InterruptedError):
            reconstruct_artifact(
                pkg.manifest,
                supplied(pkg),
                expected_digest=pkg.manifest.digest,
                check_cancel=cancel,
            )
        self.assertEqual(len(calls), 3)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory).resolve() / "output"
            publish_artifact(
                pkg.manifest, supplied(pkg), output, expected_digest=pkg.manifest.digest
            )
            self.assertEqual(output.read_bytes(), b"x" * 140_000)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                publish_artifact(
                    pkg.manifest,
                    supplied(pkg),
                    output,
                    expected_digest=pkg.manifest.digest,
                )
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_bounded_repair_expiry_and_deletion(self):
        pkg = package()
        args = {
            "expected_digest": pkg.manifest.digest,
            "limits": RepairLimits(),
            "now": 1,
        }
        self.assertEqual(
            repair_artifact_fragments(
                pkg.manifest, supplied(pkg, (0, 1, 2, 3)), (4, 5), **args
            ),
            supplied(pkg, (4, 5)),
        )
        for changes, targets in (
            ({"deleted": True}, (4,)),
            ({"now": 100}, (4,)),
            ({}, ()),
            ({}, (4, 4)),
            ({}, (0,)),
            ({"limits": RepairLimits(max_read_bytes=1)}, (4,)),
            ({"limits": RepairLimits(max_write_bytes=1)}, (4,)),
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                repair_artifact_fragments(
                    pkg.manifest,
                    supplied(pkg, (0, 1, 2, 3)),
                    targets,
                    **(args | changes),
                )

    def test_maximum_fragment_transport_is_153_chunks(self):
        store = TransferStore("artifact", b"x" * 5_000_000, max_served_bytes=0)
        self.assertEqual(len(store.manifest.chunks), 153)
