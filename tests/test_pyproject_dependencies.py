import tomllib
import unittest
from pathlib import Path


class PyprojectDependencyTests(unittest.TestCase):
    def test_runtime_dependency_floors_match_rhda_recommendations(self) -> None:
        pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

        self.assertIn("typer>=0.26.8", pyproject["project"]["dependencies"])
        self.assertIn("rich>=15.0.0", pyproject["project"]["dependencies"])
        self.assertIn(
            "typer>=0.26.8",
            pyproject["tool"]["aethermesh"]["dependency_justifications"],
        )
        self.assertIn(
            "rich>=15.0.0", pyproject["tool"]["aethermesh"]["dependency_justifications"]
        )

    def test_legacy_node_console_entrypoint_remains_compatible(self) -> None:
        pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

        self.assertEqual(
            pyproject["project"]["scripts"]["aethermesh-node"],
            "aethermesh_core.app_cli:app",
        )


if __name__ == "__main__":
    unittest.main()
