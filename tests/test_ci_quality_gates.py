from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from types import ModuleType
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
QUALITY_GATES_PATH = ROOT / "scripts" / "ci_quality_gates.py"


def load_quality_gates_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "aethermesh_ci_quality_gates", QUALITY_GATES_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load scripts/ci_quality_gates.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class WorkflowSecurityTests(unittest.TestCase):
    def test_push_only_release_workflow_can_request_contents_write(self) -> None:
        module = load_quality_gates_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_dir = root / ".github" / "workflows"
            workflow_dir.mkdir(parents=True)
            (workflow_dir / "release.yml").write_text(
                """
name: Main Alpha Release
on:
  push:
    branches: [main]
permissions:
  contents: write
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
""".lstrip(),
                encoding="utf-8",
            )

            with mock.patch.object(module, "ROOT", root):
                exit_code = module.command_workflow_security(Namespace())

        self.assertEqual(exit_code, 0)

    def test_pull_request_workflow_cannot_request_contents_write(self) -> None:
        module = load_quality_gates_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_dir = root / ".github" / "workflows"
            workflow_dir.mkdir(parents=True)
            (workflow_dir / "unsafe.yml").write_text(
                """
name: Unsafe PR Workflow
on:
  pull_request:
permissions:
  contents: write
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
""".lstrip(),
                encoding="utf-8",
            )

            with mock.patch.object(module, "ROOT", root):
                exit_code = module.command_workflow_security(Namespace())

        self.assertEqual(exit_code, 1)


class LocalParityGateTests(unittest.TestCase):
    def test_install_smoke_requires_exactly_one_wheel(self) -> None:
        module = load_quality_gates_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "dist").mkdir()
            with mock.patch.object(module, "ROOT", root):
                exit_code = module.command_install_smoke(Namespace(dist="dist"))

        self.assertEqual(exit_code, 1)

    def test_install_smoke_runs_public_example_in_isolated_interpreter(self) -> None:
        module = load_quality_gates_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "dist").mkdir()
            (root / "dist" / "sdk.whl").touch()
            for returncodes, expected_calls in [
                ([0, 0, 0], 3),
                ([1], 1),
                ([0, 2], 2),
                ([0, 0, 3], 3),
            ]:
                with self.subTest(returncodes=returncodes):
                    results = [
                        mock.Mock(returncode=code, stdout="") for code in returncodes
                    ]
                    with (
                        mock.patch.object(module, "ROOT", root),
                        mock.patch.object(module.venv, "EnvBuilder"),
                        mock.patch.object(module, "run", side_effect=results) as run,
                    ):
                        result = module.command_install_smoke(Namespace(dist="dist"))
                    self.assertEqual(result, returncodes[-1])
                    self.assertEqual(run.call_count, expected_calls)
                    if expected_calls == 3:
                        example_call = run.call_args_list[-1]
                        self.assertEqual(
                            example_call.args[0][1:],
                            ["-I", str(root / "examples" / "sdk_smoke.py")],
                        )
                        self.assertNotEqual(example_call.kwargs["cwd"], root)

    def test_flaky_tests_runs_all_three_hash_seeds(self) -> None:
        module = load_quality_gates_module()
        completed = mock.Mock(returncode=0, stdout="ok\n")

        with mock.patch.object(module, "run", return_value=completed) as run:
            exit_code = module.command_flaky_tests(Namespace())

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            [call.kwargs["env"]["PYTHONHASHSEED"] for call in run.call_args_list],
            ["1", "2", "3"],
        )
        self.assertTrue(
            all(call.args[0][-2:] == ["-q", "tests"] for call in run.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
