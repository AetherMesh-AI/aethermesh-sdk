import json
import tempfile
import unittest
from pathlib import Path

from aethermesh_core.job_manifest import (
    ManifestError,
    load_job_manifest,
    load_manifest_jobs,
)
from aethermesh_core.scheduler import NodeStatus


class JobManifestTests(unittest.TestCase):
    def test_loads_manifest_with_nodes_and_jobs(self) -> None:
        batch = self._load(
            {
                "version": 1,
                "nodes": ["local-node-a", "local-node-b"],
                "jobs": [
                    {
                        "job_id": "echo-1",
                        "job_type": "echo",
                        "payload": {"message": "hello mesh"},
                    },
                    {
                        "job_id": "text-stats-1",
                        "job_type": "text_stats",
                        "payload": {"text": "hello mesh\nhello node"},
                    },
                ],
            }
        )

        self.assertEqual(batch.node_ids, ["local-node-a", "local-node-b"])
        self.assertEqual(
            [node.status for node in batch.nodes],
            [NodeStatus.AVAILABLE, NodeStatus.AVAILABLE],
        )
        self.assertEqual(
            [node.capabilities for node in batch.nodes],
            [
                (
                    "echo",
                    "keyword_extract",
                    "text_chunk",
                    "text_embed",
                    "text_retrieve",
                    "text_stats",
                ),
                (
                    "echo",
                    "keyword_extract",
                    "text_chunk",
                    "text_embed",
                    "text_retrieve",
                    "text_stats",
                ),
            ],
        )
        self.assertEqual(batch.jobs[0].job_id, "echo-1")
        self.assertEqual(batch.jobs[0].job_type, "echo")
        self.assertEqual(batch.jobs[0].payload, {"message": "hello mesh"})
        self.assertEqual(batch.jobs[1].payload, {"text": "hello mesh\nhello node"})

    def test_missing_payload_defaults_to_empty_object(self) -> None:
        batch = self._load(
            {
                "version": 1,
                "nodes": ["local-node-a"],
                "jobs": [{"job_id": "echo-1", "job_type": "echo"}],
            }
        )

        self.assertEqual(batch.jobs[0].payload, {})

    def test_loads_mixed_string_and_object_nodes_with_statuses(self) -> None:
        batch = self._load(
            {
                "version": 1,
                "nodes": [
                    "local-node-a",
                    {"node_id": "local-node-b", "status": "offline"},
                    {"node_id": "local-node-c"},
                ],
                "jobs": [self._job()],
            }
        )

        self.assertEqual(
            batch.node_ids, ["local-node-a", "local-node-b", "local-node-c"]
        )
        self.assertEqual(
            [node.status for node in batch.nodes],
            [NodeStatus.AVAILABLE, NodeStatus.OFFLINE, NodeStatus.AVAILABLE],
        )
        self.assertEqual(
            [node.capabilities for node in batch.nodes],
            [
                (
                    "echo",
                    "keyword_extract",
                    "text_chunk",
                    "text_embed",
                    "text_retrieve",
                    "text_stats",
                ),
                (
                    "echo",
                    "keyword_extract",
                    "text_chunk",
                    "text_embed",
                    "text_retrieve",
                    "text_stats",
                ),
                (
                    "echo",
                    "keyword_extract",
                    "text_chunk",
                    "text_embed",
                    "text_retrieve",
                    "text_stats",
                ),
            ],
        )

    def test_object_nodes_can_declare_sorted_capabilities(self) -> None:
        batch = self._load(
            {
                "version": 1,
                "nodes": [
                    {
                        "node_id": "local-node-a",
                        "capabilities": ["text_stats", "echo"],
                    },
                    {
                        "node_id": "local-node-b",
                        "capabilities": ["future_job"],
                    },
                ],
                "jobs": [self._job()],
            }
        )

        self.assertEqual(
            [node.capabilities for node in batch.nodes],
            [("echo", "text_stats"), ("future_job",)],
        )

    def test_invalid_capabilities_are_rejected(self) -> None:
        cases = [
            ({"capabilities": []}, "capabilities must be a non-empty list"),
            ({"capabilities": "echo"}, "capabilities must be a non-empty list"),
            ({"capabilities": [""]}, "capabilities\\[0\\] must be a non-empty string"),
            (
                {"capabilities": ["   "]},
                "capabilities\\[0\\] must be a non-empty string",
            ),
            ({"capabilities": [123]}, "capabilities\\[0\\] must be a non-empty string"),
            (
                {"capabilities": ["echo", "echo"]},
                "capabilities contains duplicate: echo",
            ),
        ]
        for patch, message in cases:
            node = {"node_id": "local-node-a"} | patch
            with (
                self.subTest(patch=patch),
                self.assertRaisesRegex(ManifestError, message),
            ):
                self._load({"version": 1, "nodes": [node], "jobs": [self._job()]})

    def test_malformed_json_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text("not-json", encoding="utf-8")

            with self.assertRaisesRegex(ManifestError, "manifest JSON is malformed"):
                load_job_manifest(path)

    def test_unsupported_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "unsupported manifest version: 2"):
            self._load({"version": 2, "nodes": ["local-node-a"], "jobs": [self._job()]})

    def test_empty_nodes_are_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "nodes must be a non-empty list"):
            self._load({"version": 1, "nodes": [], "jobs": [self._job()]})

    def test_duplicate_nodes_are_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "duplicate node id: local-node-a"):
            self._load(
                {
                    "version": 1,
                    "nodes": ["local-node-a", "local-node-a"],
                    "jobs": [self._job()],
                }
            )

    def test_duplicate_nodes_are_rejected_across_mixed_entries(self) -> None:
        with self.assertRaisesRegex(ManifestError, "duplicate node id: local-node-a"):
            self._load(
                {
                    "version": 1,
                    "nodes": [
                        "local-node-a",
                        {"node_id": "local-node-a", "status": "offline"},
                    ],
                    "jobs": [self._job()],
                }
            )

    def test_invalid_node_object_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ManifestError, "nodes\\[0\\].node_id must be a non-empty string"
        ):
            self._load(
                {
                    "version": 1,
                    "nodes": [{"status": "available"}],
                    "jobs": [self._job()],
                }
            )

    def test_unknown_node_status_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ManifestError, "nodes\\[0\\].status must be one of: available, offline"
        ):
            self._load(
                {
                    "version": 1,
                    "nodes": [{"node_id": "local-node-a", "status": "busy"}],
                    "jobs": [self._job()],
                }
            )

    def test_non_string_node_status_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ManifestError, "nodes\\[0\\].status must be a string"
        ):
            self._load(
                {
                    "version": 1,
                    "nodes": [{"node_id": "local-node-a", "status": False}],
                    "jobs": [self._job()],
                }
            )

    def test_empty_jobs_are_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "jobs must be a non-empty list"):
            self._load({"version": 1, "nodes": ["local-node-a"], "jobs": []})

    def test_invalid_payload_is_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "payload must be a JSON object"):
            self._load(
                {
                    "version": 1,
                    "nodes": ["local-node-a"],
                    "jobs": [
                        {"job_id": "echo-1", "job_type": "echo", "payload": "bad"}
                    ],
                }
            )

    def test_unsupported_job_type_is_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "job_type must be one of"):
            self._load(
                {
                    "version": 1,
                    "nodes": ["local-node-a"],
                    "jobs": [{"job_id": "future-1", "job_type": "future_job"}],
                }
            )

    def test_invalid_job_entry_is_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, r"jobs\[0\] must be a JSON object"):
            self._load({"version": 1, "nodes": ["local-node-a"], "jobs": ["bad"]})

    def test_invalid_job_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(ManifestError, "job_id must be a local ID"):
            self._load(
                {
                    "version": 1,
                    "nodes": ["local-node-a"],
                    "jobs": [{"job_id": "", "job_type": "echo"}],
                }
            )

    def test_malformed_duplicate_and_content_addressed_job_ids(self) -> None:
        for job_id in ("has spaces", "../job", "UPPERCASE", "sha256:bad"):
            with (
                self.subTest(job_id=job_id),
                self.assertRaisesRegex(ManifestError, "job_id must be a local ID"),
            ):
                self._load(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [{"job_id": job_id, "job_type": "echo"}],
                    }
                )
        with self.assertRaisesRegex(ManifestError, "duplicate active job_id: echo-1"):
            self._load(
                {
                    "version": 1,
                    "nodes": ["local-node-a"],
                    "jobs": [self._job(), self._job()],
                }
            )
        content_id = "sha256:" + "a" * 64
        batch = self._load(
            {
                "version": 1,
                "nodes": ["local-node-a"],
                "jobs": [{"job_id": content_id, "job_type": "echo"}],
            }
        )
        self.assertEqual(batch.jobs[0].job_id, content_id)

    def test_non_object_top_level_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text("[]", encoding="utf-8")

            with self.assertRaisesRegex(
                ManifestError, "manifest must be a JSON object"
            ):
                load_job_manifest(path)

    def test_manifest_error_messages_are_stable(self) -> None:
        cases = [
            ([], "manifest must be a JSON object"),
            (
                {"version": True, "nodes": ["local-node-a"], "jobs": [self._job()]},
                "manifest version must be integer 1",
            ),
            (
                {"version": 1, "nodes": [], "jobs": [self._job()]},
                "manifest nodes must be a non-empty list",
            ),
            (
                {"version": 1, "nodes": [""], "jobs": [self._job()]},
                "manifest nodes[0] must be a non-empty string",
            ),
            (
                {
                    "version": 1,
                    "nodes": [{"node_id": "local-node-a", "capabilities": [""]}],
                    "jobs": [self._job()],
                },
                "manifest nodes[0].capabilities[0] must be a non-empty string",
            ),
            (
                {"version": 1, "nodes": ["local-node-a"], "jobs": []},
                "manifest jobs must be a non-empty list",
            ),
            (
                {
                    "version": 1,
                    "nodes": ["local-node-a"],
                    "jobs": [{"job_id": "echo-1", "job_type": ""}],
                },
                "manifest jobs[0].job_type must be a non-empty string",
            ),
        ]
        for document, expected_message in cases:
            with (
                self.subTest(expected_message=expected_message),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                path = Path(temp_dir) / "manifest.json"
                path.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaises(ManifestError) as cm:
                    load_job_manifest(path)
                self.assertEqual(str(cm.exception), expected_message)

    def test_string_node_entries_default_to_available_status(self) -> None:
        batch = self._load(
            {"version": 1, "nodes": ["local-node-a"], "jobs": [self._job()]}
        )

        self.assertEqual(batch.nodes[0].status, NodeStatus.AVAILABLE)

    def test_load_manifest_jobs_ignores_manifest_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hi"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            jobs = load_manifest_jobs(path)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_id, "echo-1")
        self.assertEqual(jobs[0].payload, {"message": "hi"})

    def _load(self, document: dict[str, object]):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            return load_job_manifest(path)

    def _job(self) -> dict[str, object]:
        return {"job_id": "echo-1", "job_type": "echo", "payload": {"message": "hi"}}


if __name__ == "__main__":
    unittest.main()
