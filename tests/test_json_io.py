import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from aethermesh_core.json_io import atomic_write_json


class JsonIoTests(unittest.TestCase):
    def test_atomic_write_json_uses_target_parent_temp_file_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "deep" / "nested" / "artifact.json"
            calls: list[dict[str, object]] = []
            real_named_temporary_file = tempfile.NamedTemporaryFile

            def capture_named_temporary_file(*args, **kwargs):
                calls.append({"args": args, "kwargs": dict(kwargs)})
                return real_named_temporary_file(*args, **kwargs)

            atomic_write_json(path, {"z": 1, "a": 2})
            raw = path.read_text(encoding="utf-8")
            self.assertEqual(raw, '{\n  "a": 2,\n  "z": 1\n}\n')
            self.assertEqual(json.loads(raw), {"z": 1, "a": 2})

            with mock.patch(
                "aethermesh_core.json_io.tempfile.NamedTemporaryFile",
                side_effect=capture_named_temporary_file,
            ):
                atomic_write_json(path, {"a": 3})

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["args"], ("w",))
            self.assertEqual(calls[0]["kwargs"]["encoding"], "utf-8")
            self.assertEqual(calls[0]["kwargs"]["dir"], path.parent)
            self.assertEqual(calls[0]["kwargs"]["prefix"], ".artifact.json.")
            self.assertEqual(calls[0]["kwargs"]["suffix"], ".tmp")
            self.assertEqual(calls[0]["kwargs"]["delete"], False)
            self.assertEqual(
                sorted(child.name for child in path.parent.iterdir()), ["artifact.json"]
            )

    def test_atomic_write_json_preserves_existing_file_and_removes_temp_on_replace_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "artifact.json"
            path.write_text('{"original": true}\n', encoding="utf-8")

            with (
                mock.patch(
                    "aethermesh_core.json_io.os.replace",
                    side_effect=OSError("replace failed"),
                ),
                self.assertRaisesRegex(OSError, "replace failed"),
            ):
                atomic_write_json(path, {"a": 1})

            self.assertEqual(path.read_text(encoding="utf-8"), '{"original": true}\n')
            self.assertEqual(
                sorted(child.name for child in path.parent.iterdir()), ["artifact.json"]
            )


if __name__ == "__main__":
    unittest.main()
