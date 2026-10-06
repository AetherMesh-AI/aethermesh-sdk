import unittest

from aethermesh_core.contribution import (
    ECHO_CONTRIBUTION_UNITS,
    KEYWORD_EXTRACT_MAX_UNITS,
    TEXT_CHUNK_MAX_UNITS,
    TEXT_EMBED_MAX_UNITS,
    TEXT_RETRIEVE_CONTRIBUTION_UNITS,
    TEXT_STATS_MAX_UNITS,
    score_validated_contribution,
)
from aethermesh_core.models import Job, JobResult, NodeIdentity
from aethermesh_core.runner import LocalRunner
from aethermesh_core.validation import validate_job_result


class ContributionScoringTests(unittest.TestCase):
    def test_echo_scores_one_unit_after_validation(self) -> None:
        job, result = _run(Job("echo-1", "echo", {"message": "hello"}))

        self.assertTrue(validate_job_result(job, result).valid)
        self.assertEqual(
            score_validated_contribution(job, result), ECHO_CONTRIBUTION_UNITS
        )

    def test_text_stats_scores_by_capped_character_buckets(self) -> None:
        small_job, small_result = _run(
            Job("stats-small", "text_stats", {"text": "x" * 100})
        )
        large_job, large_result = _run(
            Job("stats-large", "text_stats", {"text": "x" * 1000})
        )
        empty_job, empty_result = _run(Job("stats-empty", "text_stats", {"text": ""}))

        self.assertEqual(score_validated_contribution(small_job, small_result), 2)
        self.assertEqual(score_validated_contribution(empty_job, empty_result), 1)
        self.assertEqual(
            score_validated_contribution(large_job, large_result), TEXT_STATS_MAX_UNITS
        )

    def test_keyword_extract_scores_by_capped_extracted_unique_keyword_buckets(
        self,
    ) -> None:
        job, result = _run(
            Job(
                "keywords",
                "keyword_extract",
                {"text": "alpha beta gamma delta epsilon zeta", "limit": 6},
            )
        )
        capped_job, capped_result = _run(
            Job(
                "keywords-cap",
                "keyword_extract",
                {"text": " ".join(f"term{i}" for i in range(30)), "limit": 30},
            )
        )

        self.assertEqual(score_validated_contribution(job, result), 3)
        self.assertEqual(
            score_validated_contribution(capped_job, capped_result),
            KEYWORD_EXTRACT_MAX_UNITS,
        )

    def test_text_chunk_scores_by_capped_chunk_count_buckets(self) -> None:
        job, result = _run(
            Job("chunks", "text_chunk", {"text": "abcdefghij", "max_chars": 2})
        )
        capped_job, capped_result = _run(
            Job("chunks-cap", "text_chunk", {"text": "x" * 40, "max_chars": 1})
        )
        empty_job, empty_result = _run(Job("chunks-empty", "text_chunk", {"text": ""}))

        self.assertEqual(score_validated_contribution(job, result), 4)
        self.assertEqual(score_validated_contribution(empty_job, empty_result), 1)
        self.assertEqual(
            score_validated_contribution(capped_job, capped_result),
            TEXT_CHUNK_MAX_UNITS,
        )

    def test_text_embed_scores_by_capped_token_and_dimension_buckets(self) -> None:
        job, result = _run(
            Job("embed", "text_embed", {"text": "alpha beta beta", "dimensions": 4})
        )
        capped_job, capped_result = _run(
            Job(
                "embed-cap",
                "text_embed",
                {"text": " ".join(f"term{i}" for i in range(80)), "dimensions": 64},
            )
        )

        self.assertEqual(score_validated_contribution(job, result), 3)
        self.assertEqual(
            score_validated_contribution(capped_job, capped_result),
            TEXT_EMBED_MAX_UNITS,
        )

    def test_text_retrieve_scores_one_unit_after_validation(self) -> None:
        job, result = _run(
            Job(
                "retrieve",
                "text_retrieve",
                {
                    "query": "alpha beta",
                    "documents": [
                        {"id": "doc-a", "text": "alpha beta"},
                        {"id": "doc-b", "text": "alpha"},
                    ],
                },
            )
        )

        self.assertEqual(
            score_validated_contribution(job, result),
            TEXT_RETRIEVE_CONTRIBUTION_UNITS,
        )

    def test_invalid_failed_unsupported_mismatched_or_malformed_results_score_zero(
        self,
    ) -> None:
        job = Job("echo-1", "echo", {"message": "hello"})
        malformed = JobResult("echo-1", "node-a", "completed", {"not": "echo"}, None, 1)
        failed = JobResult("echo-1", "node-a", "failed", None, "boom", 0)
        mismatch = JobResult("other", "node-a", "completed", "hello", None, 1)
        unsupported = Job("unsupported", "render_frame", {})
        unsupported_result = JobResult(
            "unsupported", "node-a", "completed", {}, None, 1
        )

        self.assertFalse(validate_job_result(job, malformed).valid)
        self.assertEqual(score_validated_contribution(job, malformed), 0)
        self.assertEqual(score_validated_contribution(job, failed), 0)
        self.assertEqual(score_validated_contribution(job, mismatch), 0)
        self.assertEqual(
            score_validated_contribution(unsupported, unsupported_result), 0
        )

    def test_malformed_structured_outputs_score_zero_without_hidden_state(self) -> None:
        cases = [
            (
                Job("stats", "text_stats", {}),
                JobResult("stats", "node-a", "completed", [], None, 1),
            ),
            (
                Job("stats", "text_stats", {}),
                JobResult(
                    "stats", "node-a", "completed", {"character_count": True}, None, 1
                ),
            ),
            (
                Job("keywords", "keyword_extract", {}),
                JobResult("keywords", "node-a", "completed", [], None, 1),
            ),
            (
                Job("keywords", "keyword_extract", {}),
                JobResult(
                    "keywords", "node-a", "completed", {"keywords": "alpha"}, None, 1
                ),
            ),
            (
                Job("keywords", "keyword_extract", {}),
                JobResult(
                    "keywords",
                    "node-a",
                    "completed",
                    {"keywords": [{"term": "a"}, {"term": "a"}]},
                    None,
                    1,
                ),
            ),
            (
                Job("chunks", "text_chunk", {}),
                JobResult("chunks", "node-a", "completed", [], None, 1),
            ),
            (
                Job("chunks", "text_chunk", {}),
                JobResult(
                    "chunks",
                    "node-a",
                    "completed",
                    {"chunks": [], "chunk_count": True},
                    None,
                    1,
                ),
            ),
            (
                Job("chunks", "text_chunk", {}),
                JobResult(
                    "chunks",
                    "node-a",
                    "completed",
                    {"chunks": "not-list", "chunk_count": 1},
                    None,
                    1,
                ),
            ),
            (
                Job("chunks", "text_chunk", {}),
                JobResult(
                    "chunks",
                    "node-a",
                    "completed",
                    {"chunks": [], "chunk_count": 1},
                    None,
                    1,
                ),
            ),
            (
                Job("embed", "text_embed", {}),
                JobResult("embed", "node-a", "completed", [], None, 1),
            ),
            (
                Job("embed", "text_embed", {}),
                JobResult(
                    "embed",
                    "node-a",
                    "completed",
                    {"token_count": True, "dimensions": 2, "vector": [0, 0]},
                    None,
                    1,
                ),
            ),
            (
                Job("embed", "text_embed", {}),
                JobResult(
                    "embed",
                    "node-a",
                    "completed",
                    {"token_count": 1, "dimensions": 2, "vector": [1]},
                    None,
                    1,
                ),
            ),
        ]

        for job, result in cases:
            with self.subTest(job_type=job.job_type):
                self.assertEqual(score_validated_contribution(job, result), 0)


def _run(job: Job) -> tuple[Job, JobResult]:
    result = LocalRunner(NodeIdentity("node-a")).run(job)
    assert validate_job_result(job, result).valid
    return job, result


if __name__ == "__main__":
    unittest.main()
