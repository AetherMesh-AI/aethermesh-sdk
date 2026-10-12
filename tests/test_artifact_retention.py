"""Retention preparation never commits custody, deletes data or releases charges."""

import builtins
import json
import socket
import unittest
from dataclasses import replace
from unittest.mock import patch

from aethermesh_core import artifact_retention as retention
from aethermesh_core.artifact_admission import AdmissionLimits
from tests.test_artifact_codec import package
from tests.test_artifact_repair_plan import inventory


class RetentionContractTests(unittest.TestCase):
    def setUp(self):
        self.pkg = package()
        self.report = inventory(self.pkg, 0, (0,)).report
        self.terms = retention.retention_terms(
            self.pkg.manifest,
            self.report,
            self.report.ledger.records[0].request_id,
            store_id="a" * 64,
            limits=AdmissionLimits(16, 4),
            max_duration_seconds=50,
            max_records=8,
        )
        self.plan = retention.RetentionPlan("b" * 64, self.terms)

    def accept(self, **kwargs):
        return self.plan.advance(
            "accept",
            **(
                {
                    "operation_id": "c" * 64,
                    "expected_revision": 0,
                    "at": 1,
                    "until": 40,
                    "evidence": self.report,
                }
                | kwargs
            ),
        )

    def test_explicit_lifecycle_roundtrip_and_no_io_authority(self):
        before = repr(self.report)
        with (
            patch.object(builtins, "open") as opened,
            patch.object(socket, "socket") as network,
        ):
            accepted = self.accept()
            renewed = accepted.advance(
                "renew",
                operation_id="d" * 64,
                expected_revision=1,
                at=20,
                until=60,
                evidence=self.report,
            )
            expired = renewed.advance(
                "expire", operation_id="e" * 64, expected_revision=2, at=60
            )
            decoded = retention.RetentionPlan.decode(expired.encode())
        opened.assert_not_called()
        network.assert_not_called()
        self.assertEqual(decoded, expired)
        self.assertEqual(
            [row.action for row in decoded.records], ["accept", "renew", "expire"]
        )
        self.assertEqual([row.charged_bytes for row in decoded.records], [4, 4, 4])
        self.assertEqual(
            decoded.records[-1].evidence_digest, renewed.records[-1].evidence_digest
        )
        self.assertFalse(decoded.durable_committed or decoded.retention_confirmed)
        self.assertEqual(repr(self.report), before)
        self.assertEqual(retention.RetentionPlan.decode(self.plan.encode()), self.plan)

    def test_retry_is_exact_and_does_not_resurrect_terminal_intent(self):
        accepted = self.accept()
        self.assertEqual(
            accepted,
            accepted.advance(
                "accept",
                operation_id="c" * 64,
                expected_revision=0,
                at=1,
                until=40,
                evidence=self.report,
            ),
        )
        expired = accepted.advance(
            "expire", operation_id="d" * 64, expected_revision=1, at=40
        )
        self.assertEqual(
            expired,
            expired.advance(
                "accept",
                operation_id="c" * 64,
                expected_revision=0,
                at=1,
                until=40,
                evidence=self.report,
            ),
        )
        self.assertEqual(
            expired,
            expired.advance(
                "expire", operation_id="d" * 64, expected_revision=1, at=40
            ),
        )
        with self.assertRaisesRegex(ValueError, "reused"):
            accepted.advance(
                "accept",
                operation_id="c" * 64,
                expected_revision=0,
                at=1,
                until=41,
                evidence=self.report,
            )
        with self.assertRaisesRegex(ValueError, "terminal"):
            expired.advance(
                "renew",
                operation_id="e" * 64,
                expected_revision=2,
                at=41,
                until=60,
                evidence=self.report,
            )

    def test_intervals_clock_rollback_and_revision_conflicts(self):
        for changed in (
            {"at": 0},
            {"until": 1},
            {"until": 52},
            {"expected_revision": 1},
            {"at": True},
            {"evidence": None},
            {"until": None},
        ):
            with (
                self.subTest(changed=changed),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.accept(**changed)
        accepted = self.accept(at=10)
        for action, at, until in (
            ("renew", 9, 45),
            ("renew", 40, 60),
            ("renew", 20, 40),
            ("accept", 20, 60),
            ("renew", 60, 101),
            ("unknown", 20, 60),
        ):
            with (
                self.subTest(action=action, at=at, until=until),
                self.assertRaises(ValueError),
            ):
                accepted.advance(
                    action,
                    operation_id="d" * 64,
                    expected_revision=1,
                    at=at,
                    until=until,
                    evidence=self.report,
                )
        for plan, at, until, evidence in (
            (self.plan, 40, None, None),
            (accepted, 39, None, None),
            (accepted, 40, 40, None),
            (accepted, 40, None, self.report),
        ):
            with self.subTest(at=at, until=until), self.assertRaises(ValueError):
                plan.advance(
                    "expire",
                    operation_id="d" * 64,
                    expected_revision=len(plan.records),
                    at=at,
                    until=until,
                    evidence=evidence,
                )

    def test_evidence_requires_exact_admission_owner_quota_and_verified_status(self):
        for status in ("missing", "unverified", "quarantined"):
            changed = replace(
                self.report,
                observations=(replace(self.report.observations[0], status=status),),
            )
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.accept(evidence=changed)
        for report in (
            None,
            replace(self.report, directory_identity=(1, 8)),
            replace(self.report, database_identity=(1, 9)),
            inventory(package(b"different"), 0, (0,)).report,
            inventory(self.pkg, 0, (0, 1, 2, 3, 4)).report,
        ):
            with (
                self.subTest(report=report),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.accept(evidence=report)
        narrow = replace(self.terms, limits=AdmissionLimits(16, 1))
        extra = inventory(self.pkg, 0, (0, 1)).report
        with self.assertRaises(ValueError):
            retention.RetentionPlan("b" * 64, narrow).advance(
                "accept",
                operation_id="c" * 64,
                expected_revision=0,
                at=1,
                until=40,
                evidence=extra,
            )

    def test_observation_hash_carries_complete_accounting_and_never_reduces_it(self):
        extra = inventory(self.pkg, 0, (0, 1)).report
        accepted = self.accept()
        renewed = accepted.advance(
            "renew",
            operation_id="d" * 64,
            expected_revision=1,
            at=20,
            until=60,
            evidence=extra,
        )
        self.assertEqual(renewed.records[-1].charged_bytes, 8)
        self.assertNotEqual(
            renewed.records[0].evidence_digest, renewed.records[1].evidence_digest
        )
        with self.assertRaisesRegex(ValueError, "accounting"):
            renewed.advance(
                "renew",
                operation_id="e" * 64,
                expected_revision=2,
                at=30,
                until=70,
                evidence=self.report,
            )
        # The contract cannot establish freshness: an unchanged audit hashes alike.
        self.assertEqual(
            accepted.records[0].ledger_revision, self.report.ledger.revision
        )

    def test_manifest_binding_policy_limits_and_record_budget(self):
        for manifest, request in (
            (package(b"different").manifest, self.terms.admission.request_id),
            (self.pkg.manifest, "f" * 64),
        ):
            with self.subTest(request=request), self.assertRaises(ValueError):
                retention.retention_terms(
                    manifest,
                    self.report,
                    request,
                    store_id="a" * 64,
                    limits=AdmissionLimits(16, 4),
                    max_duration_seconds=50,
                    max_records=8,
                )
        for change in (
            {"max_records": 0},
            {"max_records": 65},
            {"max_duration_seconds": 604801},
            {"limits": AdmissionLimits(1, 1)},
            {"directory_identity": (1,)},
            {"database_identity": (1, 0)},
            {"store_id": "wrong"},
            {"admission": None},
        ):
            with (
                self.subTest(change=change),
                self.assertRaises((ValueError, TypeError)),
            ):
                replace(self.terms, **change)
        plan = replace(self.plan, terms=replace(self.terms, max_records=2))
        accepted = plan.advance(
            "accept",
            operation_id="c" * 64,
            expected_revision=0,
            at=1,
            until=40,
            evidence=self.report,
        )
        with self.assertRaisesRegex(ValueError, "budget"):
            accepted.advance(
                "renew",
                operation_id="d" * 64,
                expected_revision=1,
                at=20,
                until=60,
                evidence=self.report,
            )
        expired = accepted.advance(
            "expire", operation_id="e" * 64, expected_revision=1, at=40
        )
        self.assertEqual(len(expired.records), 2)

    def test_codec_rejects_corruption_reordering_duplicate_ops_and_unknown_versions(
        self,
    ):
        accepted = self.accept()
        row = accepted.records[0]
        for changed in (
            replace(row, previous_digest="f" * 64),
            replace(row, charged_bytes=17),
            replace(row, action="renew"),
            replace(row, action="expire"),
        ):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                replace(accepted, records=(changed,))
        with self.assertRaises(ValueError):
            replace(accepted, records=(row, replace(row, previous_digest=row.digest)))
        with self.assertRaises(TypeError):
            replace(accepted, records=[])
        raw = accepted.encode()
        data = json.loads(raw)
        for changed in (
            {**data, "version": 2},
            {**data, "version": True},
            {**data, "extra": 1},
            {**data, "records": [{}] * 65},
            {**data, "terms": None},
        ):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                retention.RetentionPlan.decode(json.dumps(changed).encode())
        for malformed in (
            b"",
            b"[]",
            b"{",
            b'{"x":1,"x":2}',
            b'{"x":NaN}',
            b"x" * 65537,
            raw + b" ",
            b'{"x":' + b"[" * 20 + b"0" + b"]" * 20 + b"}",
        ):
            with self.subTest(raw=malformed[:30]), self.assertRaises(ValueError):
                retention.RetentionPlan.decode(malformed)

    def test_cancellation_returns_no_partial_proposal(self):
        accepted = self.accept()
        count = 0

        def record():
            nonlocal count
            count += 1

        accepted.advance(
            "expire",
            operation_id="d" * 64,
            expected_revision=1,
            at=40,
            check_cancel=record,
        )
        for stop in range(1, count + 1):
            called = 0

            def cancel(stop=stop):
                nonlocal called
                called += 1
                if called == stop:
                    raise InterruptedError("cancelled")

            with self.subTest(stop=stop), self.assertRaises(InterruptedError):
                accepted.advance(
                    "expire",
                    operation_id="d" * 64,
                    expected_revision=1,
                    at=40,
                    check_cancel=cancel,
                )
        self.assertEqual(len(accepted.records), 1)

    def test_selected_admission_revision_cannot_regress_behind_global_revision(self):
        row = replace(self.report.ledger.records[0], revision=5)
        newer = replace(
            self.report,
            ledger=replace(self.report.ledger, revision=10, records=(row,)),
            observations=(replace(self.report.observations[0], admission=row),),
        )
        accepted = self.accept(evidence=newer)
        self.assertEqual(accepted.records[-1].admission_revision, 5)
        stale_selected = replace(
            self.report, ledger=replace(self.report.ledger, revision=11)
        )
        with self.assertRaisesRegex(ValueError, "accounting"):
            accepted.advance(
                "renew",
                operation_id="d" * 64,
                expected_revision=1,
                at=20,
                until=60,
                evidence=stale_selected,
            )
        pinned = replace(self.plan, terms=replace(self.terms, admission=row))
        with self.assertRaisesRegex(ValueError, "accounting"):
            pinned.advance(
                "accept",
                operation_id="c" * 64,
                expected_revision=0,
                at=1,
                until=40,
                evidence=self.report,
            )
