import unittest

from aethermesh_core.contribution import score_validated_contribution
from aethermesh_core.models import Job, JobResult
from aethermesh_core.validation import (
    _INTEGER,
    _array,
    _SchemaNode,
    _validate_declared_output_schema,
    validate_job_result,
)


class ValidationTests(unittest.TestCase):
    def test_phase_one_simple_job_validation_is_strict(self) -> None:
        valid_job = Job(job_id="hash-1", job_type="hash", payload={"value": {"a": 1}})
        valid_result = JobResult(
            job_id="hash-1",
            node_id="node-a",
            status="completed",
            output={
                "algorithm": "sha256",
                "digest": "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862",
            },
            error=None,
            contribution_units=1,
        )
        self.assertTrue(validate_job_result(valid_job, valid_result).valid)
        self.assertEqual(
            validate_job_result(
                valid_job,
                valid_result.__class__(**{**valid_result.to_dict(), "output": {}}),
            ).reason,
            "output_schema.v1.hash.output: missing required field algorithm",
        )
        malformed = Job(job_id="compute-1", job_type="basic_compute", payload={})
        malformed_result = JobResult(
            job_id="compute-1",
            node_id="node-a",
            status="completed",
            output={},
            error=None,
            contribution_units=1,
        )
        self.assertEqual(
            validate_job_result(malformed, malformed_result).reason,
            "malformed_basic_compute_payload",
        )

    def test_completed_echo_result_with_matching_output_is_valid(self) -> None:
        job = Job(job_id="echo-1", job_type="echo", payload={"message": "hello"})
        result = JobResult(
            job_id="echo-1",
            node_id="node-a",
            status="completed",
            output="hello",
            error=None,
            contribution_units=1,
        )

        validation = validate_job_result(job, result)

        self.assertTrue(validation.valid)
        self.assertEqual(validation.reason, "ok")
        self.assertEqual(
            validation.to_dict(),
            {
                "job_id": "echo-1",
                "result_job_id": "echo-1",
                "valid": True,
                "reason": "ok",
            },
        )

    def test_mismatched_job_id_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={"message": "hello"}),
            JobResult(
                job_id="echo-other",
                node_id="node-a",
                status="completed",
                output="hello",
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "job_id_mismatch")

    def test_mismatched_executor_node_id_is_invalid_when_declared(self) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={"message": "hello"}),
            JobResult(
                job_id="echo-1",
                node_id="other-node",
                status="completed",
                output="hello",
                error=None,
                contribution_units=1,
            ),
            expected_node_id="node-a",
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "result_node_id_mismatch")

    def test_unsupported_job_type_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(job_id="job-1", job_type="unsupported", payload={"message": "hello"}),
            JobResult(
                job_id="job-1",
                node_id="node-a",
                status="completed",
                output="hello",
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "unsupported_job_type")

    def test_non_completed_result_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={"message": "hello"}),
            JobResult(
                job_id="echo-1",
                node_id="node-a",
                status="failed",
                output=None,
                error="boom",
                contribution_units=0,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "result_not_completed")

    def test_missing_payload_message_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={}),
            JobResult(
                job_id="echo-1",
                node_id="node-a",
                status="completed",
                output="",
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "missing_payload_message")

    def test_non_string_payload_message_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={"message": 123}),
            JobResult(
                job_id="echo-1",
                node_id="node-a",
                status="completed",
                output=123,
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "missing_payload_message")

    def test_incorrect_output_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={"message": "expected"}),
            JobResult(
                job_id="echo-1",
                node_id="node-a",
                status="completed",
                output="actual",
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "output_mismatch")

    def test_declared_output_schema_rejects_partial_malformed_and_unknown_results(
        self,
    ) -> None:
        job = Job(
            job_id="text-stats-1", job_type="text_stats", payload={"text": "hello"}
        )
        cases = (
            (
                "missing required field",
                {"character_count": 5, "word_count": 1, "line_count": 1},
                "output_schema.v1.text_stats.output: missing required field normalized_preview",
            ),
            (
                "wrong field type",
                {
                    "character_count": "5",
                    "word_count": 1,
                    "line_count": 1,
                    "normalized_preview": "hello",
                },
                "output_schema.v1.text_stats.output.character_count: expected int",
            ),
            (
                "unknown field",
                {
                    "character_count": 5,
                    "word_count": 1,
                    "line_count": 1,
                    "normalized_preview": "hello",
                    "unverified": True,
                },
                "output_schema.v1.text_stats.output: unknown field unverified",
            ),
            (
                "malformed structured output",
                "{not valid result JSON}",
                "output_schema.v1.text_stats.output: expected object",
            ),
        )

        for name, output, reason in cases:
            with self.subTest(name=name):
                validation = validate_job_result(
                    job,
                    JobResult(
                        job_id="text-stats-1",
                        node_id="node-a",
                        status="completed",
                        output=output,
                        error=None,
                        contribution_units=1,
                    ),
                )
                self.assertFalse(validation.valid)
                self.assertEqual(validation.reason, reason)

    def test_completed_result_with_unexpected_contribution_units_is_invalid(
        self,
    ) -> None:
        validation = validate_job_result(
            Job(job_id="echo-1", job_type="echo", payload={"message": "hello"}),
            JobResult(
                job_id="echo-1",
                node_id="node-a",
                status="completed",
                output="hello",
                error=None,
                contribution_units=999,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "unexpected_contribution_units")

    def test_completed_text_stats_result_with_matching_output_is_valid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="text-stats-1",
                job_type="text_stats",
                payload={"text": "hello mesh\nhello node"},
            ),
            JobResult(
                job_id="text-stats-1",
                node_id="node-a",
                status="completed",
                output={
                    "character_count": len("hello mesh\nhello node"),
                    "word_count": 4,
                    "line_count": 2,
                    "normalized_preview": "hello mesh hello node",
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertEqual(
            validation.to_dict(),
            {
                "job_id": "text-stats-1",
                "result_job_id": "text-stats-1",
                "valid": True,
                "reason": "ok",
            },
        )

    def test_text_stats_recomputes_expected_output(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="text-stats-1", job_type="text_stats", payload={"text": "hello"}
            ),
            JobResult(
                job_id="text-stats-1",
                node_id="node-a",
                status="completed",
                output={
                    "character_count": 999,
                    "word_count": 1,
                    "line_count": 1,
                    "normalized_preview": "hello",
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "output_mismatch")

    def test_text_stats_malformed_payload_is_invalid(self) -> None:
        missing = validate_job_result(
            Job(job_id="text-stats-missing", job_type="text_stats", payload={}),
            JobResult(
                job_id="text-stats-missing",
                node_id="node-a",
                status="completed",
                output=None,
                error=None,
                contribution_units=1,
            ),
        )
        non_string = validate_job_result(
            Job(
                job_id="text-stats-non-string",
                job_type="text_stats",
                payload={"text": 123},
            ),
            JobResult(
                job_id="text-stats-non-string",
                node_id="node-a",
                status="completed",
                output=None,
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(missing.valid)
        self.assertEqual(missing.reason, "missing_payload_text")
        self.assertFalse(non_string.valid)
        self.assertEqual(non_string.reason, "missing_payload_text")

    def test_completed_keyword_extract_result_with_matching_output_is_valid(
        self,
    ) -> None:
        validation = validate_job_result(
            Job(
                job_id="keyword-1",
                job_type="keyword_extract",
                payload={
                    "text": "AetherMesh nodes process useful local work for the mesh.",
                    "limit": 5,
                },
            ),
            JobResult(
                job_id="keyword-1",
                node_id="node-a",
                status="completed",
                output={
                    "keywords": [
                        {"term": "aethermesh", "count": 1},
                        {"term": "local", "count": 1},
                        {"term": "mesh", "count": 1},
                        {"term": "nodes", "count": 1},
                        {"term": "process", "count": 1},
                    ],
                    "unique_terms": 7,
                    "total_terms": 7,
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertTrue(validation.valid)
        self.assertEqual(validation.reason, "ok")

    def test_keyword_extract_rejects_changed_output(self) -> None:
        job = Job(
            job_id="keyword-1",
            job_type="keyword_extract",
            payload={"text": "alpha beta beta", "limit": 2},
        )
        valid_output = {
            "keywords": [
                {"term": "beta", "count": 2},
                {"term": "alpha", "count": 1},
            ],
            "unique_terms": 2,
            "total_terms": 3,
        }
        cases = [
            (
                "changed_order",
                {
                    "keywords": list(reversed(valid_output["keywords"])),
                    "unique_terms": 2,
                    "total_terms": 3,
                },
            ),
            (
                "missing_terms",
                {
                    "keywords": [valid_output["keywords"][0]],
                    "unique_terms": 2,
                    "total_terms": 3,
                },
            ),
            (
                "changed_count",
                {
                    "keywords": [
                        {"term": "beta", "count": 1},
                        {"term": "alpha", "count": 1},
                    ],
                    "unique_terms": 2,
                    "total_terms": 3,
                },
            ),
            (
                "incorrect_totals",
                {
                    "keywords": valid_output["keywords"],
                    "unique_terms": 99,
                    "total_terms": 3,
                },
            ),
            ("malformed_output", ["beta", "alpha"]),
        ]

        for name, output in cases:
            with self.subTest(name=name):
                validation = validate_job_result(
                    job,
                    JobResult(
                        job_id="keyword-1",
                        node_id="node-a",
                        status="completed",
                        output=output,
                        error=None,
                        contribution_units=1,
                    ),
                )
                self.assertFalse(validation.valid)
                if name == "malformed_output":
                    self.assertTrue(
                        validation.reason.startswith(
                            "output_schema.v1.keyword_extract.output:"
                        )
                    )
                else:
                    self.assertEqual(validation.reason, "output_mismatch")

    def test_keyword_extract_rejects_output_from_different_payload(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="keyword-1",
                job_type="keyword_extract",
                payload={"text": "alpha beta"},
            ),
            JobResult(
                job_id="keyword-1",
                node_id="node-a",
                status="completed",
                output={
                    "keywords": [{"term": "gamma", "count": 1}],
                    "unique_terms": 1,
                    "total_terms": 1,
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "output_mismatch")

    def test_keyword_extract_malformed_payload_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="keyword-blank", job_type="keyword_extract", payload={"text": ""}
            ),
            JobResult(
                job_id="keyword-blank",
                node_id="node-a",
                status="completed",
                output={"keywords": [], "unique_terms": 0, "total_terms": 0},
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "malformed_keyword_extract_payload")

    def test_failed_keyword_extract_result_earns_zero_credit(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="keyword-1",
                job_type="keyword_extract",
                payload={"text": "alpha"},
            ),
            JobResult(
                job_id="keyword-1",
                node_id="node-a",
                status="failed",
                output=None,
                error="keyword_extract payload requires non-empty string field: text",
                contribution_units=0,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "result_not_completed")

    def test_completed_text_chunk_result_with_matching_output_is_valid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="chunk-1",
                job_type="text_chunk",
                payload={"text": "alpha beta gamma", "max_chars": 10},
            ),
            JobResult(
                job_id="chunk-1",
                node_id="node-a",
                status="completed",
                output={
                    "chunks": [
                        {"index": 0, "text": "alpha beta", "character_count": 10},
                        {"index": 1, "text": " gamma", "character_count": 6},
                    ],
                    "chunk_count": 2,
                    "character_count": 16,
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertTrue(validation.valid)
        self.assertEqual(validation.reason, "ok")

    def test_text_chunk_rejects_changed_output(self) -> None:
        job = Job(
            job_id="chunk-1",
            job_type="text_chunk",
            payload={"text": "alpha beta gamma", "max_chars": 10},
        )
        valid_output = {
            "chunks": [
                {"index": 0, "text": "alpha beta", "character_count": 10},
                {"index": 1, "text": " gamma", "character_count": 6},
            ],
            "chunk_count": 2,
            "character_count": 16,
        }
        cases = [
            (
                "changed_text",
                {
                    **valid_output,
                    "chunks": [{"index": 0, "text": "alpha", "character_count": 5}],
                },
            ),
            (
                "changed_order",
                {**valid_output, "chunks": list(reversed(valid_output["chunks"]))},
            ),
            ("changed_chunk_count", {**valid_output, "chunk_count": 99}),
            ("changed_total", {**valid_output, "character_count": 99}),
            ("malformed_output", ["alpha beta", " gamma"]),
        ]

        for name, output in cases:
            with self.subTest(name=name):
                validation = validate_job_result(
                    job,
                    JobResult(
                        job_id="chunk-1",
                        node_id="node-a",
                        status="completed",
                        output=output,
                        error=None,
                        contribution_units=1,
                    ),
                )
                self.assertFalse(validation.valid)
                if name == "malformed_output":
                    self.assertTrue(
                        validation.reason.startswith(
                            "output_schema.v1.text_chunk.output:"
                        )
                    )
                else:
                    self.assertEqual(validation.reason, "output_mismatch")

    def test_text_chunk_malformed_payload_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="chunk-bad",
                job_type="text_chunk",
                payload={"text": "hello", "max_chars": 0},
            ),
            JobResult(
                job_id="chunk-bad",
                node_id="node-a",
                status="completed",
                output={"chunks": [], "chunk_count": 0, "character_count": 0},
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "malformed_text_chunk_payload")

    def test_failed_text_chunk_result_earns_zero_credit(self) -> None:
        validation = validate_job_result(
            Job(job_id="chunk-1", job_type="text_chunk", payload={"text": "alpha"}),
            JobResult(
                job_id="chunk-1",
                node_id="node-a",
                status="failed",
                output=None,
                error="text_chunk payload requires string field: text",
                contribution_units=0,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "result_not_completed")

    def test_completed_text_embed_result_with_matching_output_is_valid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="embed-1",
                job_type="text_embed",
                payload={
                    "text": "AetherMesh nodes process useful local work for the mesh.",
                    "dimensions": 8,
                },
            ),
            JobResult(
                job_id="embed-1",
                node_id="node-a",
                status="completed",
                output={
                    "dimensions": 8,
                    "token_count": 9,
                    "unique_terms": 9,
                    "vector": [4, 1, 1, 1, 0, 0, 1, 1],
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertTrue(validation.valid)
        self.assertEqual(validation.reason, "ok")

    def test_text_embed_rejects_changed_output(self) -> None:
        job = Job(
            job_id="embed-1",
            job_type="text_embed",
            payload={"text": "alpha beta beta", "dimensions": 4},
        )
        valid_output = {
            "dimensions": 4,
            "token_count": 3,
            "unique_terms": 2,
            "vector": [0, 2, 1, 0],
        }
        cases = [
            ("changed_dimensions", {**valid_output, "dimensions": 8}),
            ("changed_token_count", {**valid_output, "token_count": 99}),
            ("changed_unique_terms", {**valid_output, "unique_terms": 99}),
            ("changed_vector", {**valid_output, "vector": [3, 0, 0, 0]}),
            ("malformed_output", [0, 2, 1, 0]),
        ]

        for name, output in cases:
            with self.subTest(name=name):
                validation = validate_job_result(
                    job,
                    JobResult(
                        job_id="embed-1",
                        node_id="node-a",
                        status="completed",
                        output=output,
                        error=None,
                        contribution_units=1,
                    ),
                )
                self.assertFalse(validation.valid)
                if name == "malformed_output":
                    self.assertTrue(
                        validation.reason.startswith(
                            "output_schema.v1.text_embed.output:"
                        )
                    )
                else:
                    self.assertEqual(validation.reason, "output_mismatch")

    def test_text_embed_malformed_payload_is_invalid(self) -> None:
        cases = [
            Job(job_id="embed-missing", job_type="text_embed", payload={}),
            Job(job_id="embed-blank", job_type="text_embed", payload={"text": "  "}),
            Job(
                job_id="embed-non-string", job_type="text_embed", payload={"text": 123}
            ),
            Job(
                job_id="embed-bool-dimensions",
                job_type="text_embed",
                payload={"text": "hello", "dimensions": True},
            ),
            Job(
                job_id="embed-small-dimensions",
                job_type="text_embed",
                payload={"text": "hello", "dimensions": 1},
            ),
            Job(
                job_id="embed-large-dimensions",
                job_type="text_embed",
                payload={"text": "hello", "dimensions": 65},
            ),
        ]

        for job in cases:
            with self.subTest(job_id=job.job_id):
                validation = validate_job_result(
                    job,
                    JobResult(
                        job_id=job.job_id,
                        node_id="node-a",
                        status="completed",
                        output=None,
                        error=None,
                        contribution_units=1,
                    ),
                )
                self.assertFalse(validation.valid)
                self.assertEqual(validation.reason, "malformed_text_embed_payload")

    def test_failed_text_embed_result_earns_zero_credit(self) -> None:
        validation = validate_job_result(
            Job(job_id="embed-1", job_type="text_embed", payload={"text": "alpha"}),
            JobResult(
                job_id="embed-1",
                node_id="node-a",
                status="failed",
                output=None,
                error="text_embed payload requires non-empty string field: text",
                contribution_units=0,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "result_not_completed")

    def test_completed_text_retrieve_result_with_matching_output_is_valid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="retrieve-1",
                job_type="text_retrieve",
                payload={
                    "query": "Mesh mesh retrieval",
                    "documents": [
                        {"id": "doc-c", "text": "Retrieval systems rank context."},
                        {"id": "doc-b", "text": "Retrieval helps mesh nodes."},
                        {"id": "doc-a", "text": "Mesh workers process tasks."},
                    ],
                    "limit": 2,
                },
            ),
            JobResult(
                job_id="retrieve-1",
                node_id="node-a",
                status="completed",
                output={
                    "query_terms": ["mesh", "retrieval"],
                    "matches": [
                        {
                            "id": "doc-b",
                            "score": 1.0,
                            "matched_term_count": 2,
                            "matched_terms": ["mesh", "retrieval"],
                        },
                        {
                            "id": "doc-a",
                            "score": 0.5,
                            "matched_term_count": 1,
                            "matched_terms": ["mesh"],
                        },
                    ],
                },
                error=None,
                contribution_units=1,
            ),
        )

        self.assertTrue(validation.valid)
        self.assertEqual(validation.reason, "ok")

    def test_text_retrieve_contribution_rejects_malformed_output_shapes(self) -> None:
        job = Job(
            job_id="retrieve-1",
            job_type="text_retrieve",
            payload={
                "query": "alpha beta",
                "documents": [
                    {"id": "doc-b", "text": "alpha only"},
                    {"id": "doc-a", "text": "alpha beta"},
                ],
            },
        )
        base_output = {
            "query_terms": ["alpha", "beta"],
            "matches": [
                {
                    "id": "doc-a",
                    "score": 1.0,
                    "matched_term_count": 2,
                    "matched_terms": ["alpha", "beta"],
                }
            ],
        }
        malformed_outputs = [
            None,
            {**base_output, "query_terms": "alpha"},
            {**base_output, "matches": "not-a-list"},
            {**base_output, "query_terms": ["alpha", 3]},
            {**base_output, "matches": ["not-a-dict"]},
            {**base_output, "matches": [{**base_output["matches"][0], "id": 123}]},
            {
                **base_output,
                "matches": [{**base_output["matches"][0], "matched_term_count": -1}],
            },
            {
                **base_output,
                "matches": [{**base_output["matches"][0], "matched_terms": "alpha"}],
            },
            {
                **base_output,
                "matches": [
                    {**base_output["matches"][0], "matched_terms": ["alpha", 3]}
                ],
            },
            {
                **base_output,
                "matches": [{**base_output["matches"][0], "score": True}],
            },
            {
                **base_output,
                "matches": [{**base_output["matches"][0], "score": -0.1}],
            },
        ]

        for output in malformed_outputs:
            with self.subTest(output=output):
                self.assertEqual(
                    score_validated_contribution(
                        job,
                        JobResult(
                            job_id="retrieve-1",
                            node_id="node-a",
                            status="completed",
                            output=output,
                            error=None,
                            contribution_units=0,
                        ),
                    ),
                    0,
                )

    def test_text_retrieve_rejects_changed_output(self) -> None:
        job = Job(
            job_id="retrieve-1",
            job_type="text_retrieve",
            payload={
                "query": "alpha beta",
                "documents": [
                    {"id": "doc-b", "text": "alpha only"},
                    {"id": "doc-a", "text": "alpha beta"},
                ],
            },
        )
        valid_output = {
            "query_terms": ["alpha", "beta"],
            "matches": [
                {
                    "id": "doc-a",
                    "score": 1.0,
                    "matched_term_count": 2,
                    "matched_terms": ["alpha", "beta"],
                },
                {
                    "id": "doc-b",
                    "score": 0.5,
                    "matched_term_count": 1,
                    "matched_terms": ["alpha"],
                },
            ],
        }
        cases = [
            (
                "changed_score",
                {
                    **valid_output,
                    "matches": [
                        {**valid_output["matches"][0], "score": 0.5},
                        valid_output["matches"][1],
                    ],
                },
            ),
            (
                "changed_order",
                {**valid_output, "matches": list(reversed(valid_output["matches"]))},
            ),
            ("changed_terms", {**valid_output, "query_terms": ["beta", "alpha"]}),
            ("malformed_output", ["doc-a", "doc-b"]),
        ]

        for name, output in cases:
            with self.subTest(name=name):
                validation = validate_job_result(
                    job,
                    JobResult(
                        job_id="retrieve-1",
                        node_id="node-a",
                        status="completed",
                        output=output,
                        error=None,
                        contribution_units=1,
                    ),
                )
                self.assertFalse(validation.valid)
                if name == "malformed_output":
                    self.assertTrue(
                        validation.reason.startswith(
                            "output_schema.v1.text_retrieve.output:"
                        )
                    )
                else:
                    self.assertEqual(validation.reason, "output_mismatch")

    def test_text_retrieve_malformed_payload_is_invalid(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="retrieve-bad",
                job_type="text_retrieve",
                payload={"query": "alpha", "documents": []},
            ),
            JobResult(
                job_id="retrieve-bad",
                node_id="node-a",
                status="completed",
                output={"query_terms": ["alpha"], "matches": []},
                error=None,
                contribution_units=1,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "malformed_text_retrieve_payload")

    def test_failed_text_retrieve_result_earns_zero_credit(self) -> None:
        validation = validate_job_result(
            Job(
                job_id="retrieve-1",
                job_type="text_retrieve",
                payload={
                    "query": "alpha",
                    "documents": [{"id": "doc-1", "text": "alpha"}],
                },
            ),
            JobResult(
                job_id="retrieve-1",
                node_id="node-a",
                status="failed",
                output=None,
                error="text_retrieve payload requires non-empty list field: documents",
                contribution_units=0,
            ),
        )

        self.assertFalse(validation.valid)
        self.assertEqual(validation.reason, "result_not_completed")

    def test_text_retrieve_rejects_wrong_job_id_and_contribution_units(self) -> None:
        job = Job(
            job_id="retrieve-1",
            job_type="text_retrieve",
            payload={
                "query": "alpha",
                "documents": [{"id": "doc-1", "text": "alpha"}],
            },
        )
        output = {
            "query_terms": ["alpha"],
            "matches": [
                {
                    "id": "doc-1",
                    "score": 1.0,
                    "matched_term_count": 1,
                    "matched_terms": ["alpha"],
                }
            ],
        }

        wrong_job = validate_job_result(
            job,
            JobResult("other-job", "node-a", "completed", output, None, 1),
        )
        wrong_units = validate_job_result(
            job,
            JobResult("retrieve-1", "node-a", "completed", output, None, 2),
        )

        self.assertEqual(wrong_job.reason, "job_id_mismatch")
        self.assertEqual(wrong_units.reason, "unexpected_contribution_units")

    def test_declared_output_schema_covers_array_and_null_rules(self) -> None:
        self.assertEqual(
            _validate_declared_output_schema("test", _array(_INTEGER), "not-an-array"),
            "output_schema.v1.test.output: expected array",
        )
        self.assertEqual(
            _validate_declared_output_schema(
                "test", _array(_SchemaNode("NoneType", (type(None),))), ["not-null"]
            ),
            "output_schema.v1.test.output[0]: expected NoneType",
        )
        self.assertEqual(
            _validate_declared_output_schema("test", _array(_INTEGER), ["wrong"]),
            "output_schema.v1.test.output[0]: expected int",
        )
        self.assertEqual(
            _validate_declared_output_schema("test", _INTEGER, True),
            "output_schema.v1.test.output: expected int",
        )


if __name__ == "__main__":
    unittest.main()
