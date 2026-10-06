import unittest

from scripts import release_policy


class ReleasePolicyTests(unittest.TestCase):
    def test_sequence_starts_at_116_and_ignores_legacy_tags(self) -> None:
        self.assertEqual(
            release_policy.next_build(["v0.2.0-alpha-deadbeef", "v0.1.0"]), 116
        )

    def test_sequence_is_shared_across_numbered_tags(self) -> None:
        tags = ["v0.2.0-alpha.116", "v0.2.0-alpha.120", "v0.2.0-alpha-hash"]
        self.assertEqual(release_policy.next_build(tags), 121)
        self.assertEqual(release_policy.latest_numbered_tag(tags), "v0.2.0-alpha.120")

    def test_required_checks_reject_missing_pending_skipped_and_failing(self) -> None:
        required = {
            "contexts": ["legacy", "pending"],
            "checks": [{"context": "tests"}, {"context": "skipped"}],
        }
        checks = {
            "check_runs": [
                {"name": "tests", "conclusion": "success"},
                {"name": "skipped", "conclusion": "skipped"},
                {"name": "pending", "conclusion": None},
            ]
        }
        statuses = {"statuses": [{"context": "legacy", "state": "failure"}]}

        self.assertEqual(
            release_policy.verify_required_checks(required, checks, statuses),
            [
                "legacy: check=missing, status=failure",
                "pending: check=None, status=missing",
                "skipped: check=skipped, status=missing",
            ],
        )

    def test_required_checks_accept_successful_checks_and_statuses(self) -> None:
        required = {"contexts": ["legacy"], "checks": [{"context": "tests"}]}
        checks = {"check_runs": [{"name": "tests", "conclusion": "success"}]}
        statuses = {"statuses": [{"context": "legacy", "state": "success"}]}
        self.assertEqual(
            release_policy.verify_required_checks(required, checks, statuses), []
        )

    def test_release_check_policy_records_required_contexts(self) -> None:
        self.assertEqual(
            set(release_policy.required_checks_document()["contexts"]),
            {
                "Python tests with 100% coverage",
                "Duplicate code with 0% threshold",
                "Test",
                "Line Coverage",
                "Branch Coverage",
                "Ruff Check",
                "Ruff Format",
                "Strict Type Check",
                "Duplicate Code",
                "Complexity",
                "Dead Code",
                "Bandit",
                "Pip Audit",
                "Secret Scan",
                "Test Integrity",
                "Dependency Review",
                "Build Install",
                "Python - Workflow Security Check",
                "Python - PR Size Check",
                "Python - Flaky Test Check",
                "Python - Artifact Provenance Check",
            },
        )


if __name__ == "__main__":
    unittest.main()
