"""Disposable authenticated loopback erasure-fragment acceptance."""

import tempfile
import unittest
from pathlib import Path

from scripts.peer_install_smoke import artifact_smoke
from tests.network_test_support import TestPKI


class ArtifactNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_selected_fragment_download_and_verified_reconstruction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            pki = TestPKI.create(root / "pki")
            await artifact_smoke(pki, root)
            self.assertEqual(
                (root / "artifact-output").read_bytes(),
                bytes(range(256)) * 1100 + b"synthetic-only",
            )
