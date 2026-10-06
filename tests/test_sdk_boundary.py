"""SDK packaging boundaries and a no-network public-import example."""

from __future__ import annotations

import contextlib
import io
import json
import runpy
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SdkBoundaryTests(unittest.TestCase):
    def test_public_example_executes_and_rejects_tampered_output(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            runpy.run_path(str(ROOT / "examples" / "sdk_smoke.py"))
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["mode"], "local-only-no-p2p")
        self.assertEqual(payload["result"]["job_id"], "sdk-echo")
        self.assertEqual(payload["result"]["node_id"], "sdk-example")
        self.assertEqual(payload["result"]["status"], "completed")
        self.assertEqual(payload["result"]["output"], "hello mesh")

    def test_api_extra_preserves_old_ui_dependency_alias(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        extras = project["project"]["optional-dependencies"]
        self.assertEqual(extras["api"], extras["ui"])
        self.assertEqual(extras["api"], ["fastapi>=0.111", "uvicorn>=0.30"])
        self.assertEqual(project["project"]["name"], "aethermesh")

    def test_sdk_workflows_do_not_require_desktop_workspace(self) -> None:
        self.assertFalse((ROOT / "desktop").exists())
        self.assertFalse((ROOT / "package.json").exists())
        self.assertFalse((ROOT / "package-lock.json").exists())
        self.assertFalse((ROOT / ".github/workflows/desktop-release.yml").exists())
        self.assertFalse(
            (ROOT / ".github/actions/publish-prerelease/action.yml").exists()
        )
        workflow = (ROOT / ".github/workflows/pr-quality-gates.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("desktop-tests:", workflow)
        self.assertNotIn("desktop-runtime-sidecar:", workflow)
        self.assertNotIn("npm install", workflow)
        self.assertIn("jscpd", workflow)


if __name__ == "__main__":
    unittest.main()
