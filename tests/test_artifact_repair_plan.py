"""Pure candidate planning keeps copied, stale and configured evidence non-authoritative."""

import builtins
import itertools
import os
import socket
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import artifact_repair_plan as planning
from aethermesh_core.artifact_admission import AdmissionSnapshot, FragmentAdmission
from aethermesh_core.artifact_owned_inventory import (
    OwnedFragmentObservation,
    OwnedInventoryReport,
)
from tests.test_artifact_codec import package


def inventory(pkg, source, indices, domain=None, statuses=None):
    records = tuple(
        FragmentAdmission(
            f"{source * 256 + position:064x}",
            pkg.manifest.project,
            pkg.manifest.digest,
            index,
            pkg.manifest.fragment_size(index),
            *pkg.manifest.fragments[index],
            "verified_observation",
            position + 1,
        )
        for position, index in enumerate(indices)
    )
    snapshot = AdmissionSnapshot(
        len(records) + 1, sum(row.size for row in records), records
    )
    observations = tuple(
        OwnedFragmentObservation(
            row,
            Path("/synthetic-fixtures") / str(source) / (row.request_id + ".fragment"),
            "verified" if statuses is None else statuses[position],
        )
        for position, row in enumerate(records)
    )
    report = OwnedInventoryReport(
        snapshot,
        (1, source + 1),
        (1, source + 1000),
        snapshot.charged_bytes,
        snapshot.charged_bytes,
        observations,
    )
    return planning.RepairInventory(
        f"source-{source:02}", f"domain-{source}" if domain is None else domain, report
    )


class RepairPlanTests(unittest.TestCase):
    def setUp(self):
        self.pkg = package()
        self.inputs = tuple(
            inventory(self.pkg, source, (source,)) for source in range(4)
        )
        self.limits = planning.RepairPlanLimits(16, 8, 2, 2)

    def plan(self, inventories=None, targets=(4, 5), **kwargs):
        return planning.plan_artifact_repair(
            self.pkg.manifest,
            self.inputs if inventories is None else inventories,
            targets,
            expected_digest=self.pkg.manifest.digest,
            limits=kwargs.pop("limits", self.limits),
            now=kwargs.pop("now", 1),
            **kwargs,
        )

    def test_pure_plan_batches_and_authority_flags(self):
        before = repr(self.inputs)
        with (
            patch.object(builtins, "open") as opened,
            patch.object(os, "open") as fdopen,
            patch.object(socket, "socket") as network,
            patch.object(planning.codec, "repair_artifact_fragments") as repair,
            patch.object(planning.codec, "reconstruct_artifact") as reconstruct,
        ):
            result = self.plan()
        for operation in (opened, fdopen, network, repair, reconstruct):
            operation.assert_not_called()
        self.assertEqual(result.status, "candidate")
        self.assertIsNone(result.reason)
        self.assertEqual(result.required_read_bytes, 16)
        self.assertEqual(result.required_write_bytes, 8)
        self.assertEqual([len(batch) for batch in result.input_batches], [2, 2])
        self.assertEqual(len({item.fragment_index for item in result.inputs}), 4)
        self.assertEqual(len({item.failure_domain for item in result.inputs}), 4)
        self.assertEqual(result.evidence, self.inputs)
        self.assertTrue(result.requires_revalidation)
        for flag in (
            result.execution_authorized,
            result.retention_confirmed,
            result.publisher_authorized,
            result.failure_domains_verified,
        ):
            self.assertFalse(flag)
        self.assertEqual(repr(self.inputs), before)
        self.assertEqual(
            result, self.plan(tuple(reversed(self.inputs)), targets=(5, 4))
        )

    def test_matching_recovers_assignment_that_greedy_choice_would_miss(self):
        sources = (
            inventory(self.pkg, 0, (0, 1), "a"),
            inventory(self.pkg, 1, (0,), "b"),
            inventory(self.pkg, 2, (2,), "c"),
            inventory(self.pkg, 3, (3,), "d"),
        )
        result = self.plan(sources)
        self.assertEqual(result.status, "candidate")
        self.assertEqual(
            [(item.fragment_index, item.failure_domain) for item in result.inputs],
            [(0, "b"), (1, "a"), (2, "c"), (3, "d")],
        )

    def test_distinct_indices_and_domains_are_both_required(self):
        for sources in (
            tuple(inventory(self.pkg, i, (i % 3,)) for i in range(4)),
            tuple(inventory(self.pkg, i, (i,), "same") for i in range(4)),
            (inventory(self.pkg, 0, (0, 1, 2, 3), "same"),),
        ):
            with self.subTest(sources=sources):
                result = self.plan(sources)
                self.assertEqual(
                    result.reason, "insufficient_distinct_indices_or_domains"
                )
                self.assertEqual(result.inputs, ())
                self.assertEqual(result.input_batches, ())
                self.assertFalse(result.execution_authorized)

    def test_duplicate_ownership_cannot_be_relabelled_as_another_source(self):
        original = self.inputs[0]
        for copied in (
            original,
            replace(original, source_id="copy", failure_domain="other"),
            replace(
                original,
                source_id="copy",
                report=replace(original.report, directory_identity=(2, 20)),
            ),
            replace(
                original,
                source_id="copy",
                report=replace(original.report, database_identity=(2, 20)),
            ),
        ):
            with (
                self.subTest(copied=copied),
                self.assertRaisesRegex(ValueError, "repeated"),
            ):
                self.plan((original, copied) + self.inputs[1:])

    def test_missing_unverified_quarantined_and_other_roots_are_not_sources(self):
        for status in ("missing", "unverified", "quarantined"):
            sources = (inventory(self.pkg, 0, (0,), statuses=(status,)),) + self.inputs[
                1:
            ]
            self.assertEqual(self.plan(sources).status, "blocked")
        unrelated = inventory(package(b"other"), 0, (0,))
        self.assertEqual(self.plan((unrelated,) + self.inputs[1:]).status, "blocked")
        quarantined_target = inventory(self.pkg, 4, (4,), statuses=("quarantined",))
        self.assertEqual(
            self.plan(self.inputs + (quarantined_target,)).status, "candidate"
        )

    def test_explicit_limits_expiry_deletion_and_existing_target(self):
        for limits in (
            replace(self.limits, max_read_bytes=15),
            replace(self.limits, max_write_bytes=7),
        ):
            result = self.plan(limits=limits)
            self.assertEqual(result.reason, "byte_budget")
            self.assertEqual(result.inputs, ())
        self.assertEqual(self.plan(now=100).reason, "expired")
        self.assertEqual(self.plan(deleted=True).reason, "deleted")
        self.assertEqual(self.plan(targets=(0,)).reason, "target_already_observed")
        self.assertEqual(self.plan(now=1), self.plan(now=99))
        for count in (1, 2, 3, 4, 8):
            result = self.plan(limits=replace(self.limits, max_inflight=count))
            self.assertTrue(all(len(batch) <= count for batch in result.input_batches))
            self.assertEqual(
                tuple(item for batch in result.input_batches for item in batch),
                result.inputs,
            )

    def test_all_stripes_are_budgeted_even_for_a_single_target(self):
        self.pkg = package(bytes(range(32)), fragment_bytes=4)
        sources = tuple(inventory(self.pkg, i, (i, i + 6)) for i in range(4))
        limits = planning.RepairPlanLimits(32, 4, 3, 1)
        result = self.plan(sources, targets=(4,), limits=limits)
        self.assertEqual(result.status, "candidate")
        self.assertEqual(result.required_read_bytes, 32)
        self.assertEqual(result.required_write_bytes, 4)
        self.assertEqual(len(result.inputs), 8)
        partial = tuple(inventory(self.pkg, i, (i,)) for i in range(4))
        blocked = self.plan(partial, targets=(4,), limits=limits)
        self.assertEqual(blocked.status, "blocked")
        self.assertEqual(blocked.inputs, ())

    def test_bad_targets_limits_and_root_are_rejected(self):
        for targets in ((), (4, 4), (4, 5, 3), [4], (True,), (6,)):
            with self.subTest(targets=targets), self.assertRaises(ValueError):
                self.plan(targets=targets)
        for field, value in (
            ("max_inflight", 0),
            ("max_inflight", 9),
            ("max_read_bytes", -1),
            ("max_read_bytes", 16 * 1024 * 1024 + 1),
            ("max_targets", 193),
            ("max_write_bytes", True),
        ):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                replace(self.limits, **{field: value})
        with self.assertRaises(ValueError):
            self.plan(deleted=1)
        with self.assertRaises(ValueError):
            self.plan(limits=None)
        with self.assertRaises(ValueError):
            planning.plan_artifact_repair(
                self.pkg.manifest,
                self.inputs,
                (4,),
                expected_digest="0" * 64,
                limits=self.limits,
                now=1,
            )

    def test_report_validation_and_global_candidate_bound(self):
        report = self.inputs[0].report
        malformed = (
            replace(report, directory_identity=(1,)),
            replace(report, ledger=None),
            replace(report, observations=()),
            replace(report, ledger=replace(report.ledger, records=(None,))),
            replace(report, observations=(None,)),
            replace(
                report,
                observations=(replace(report.observations[0], status="unknown"),),
            ),
            replace(report, read_budget_bytes=3),
            replace(report, retention_confirmed=True),
            replace(report, ledger=replace(report.ledger, charged_bytes=5)),
        )
        for changed in malformed:
            with (
                self.subTest(changed=changed),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.plan((replace(self.inputs[0], report=changed),) + self.inputs[1:])
        for inputs in ((), list(self.inputs), self.inputs * 9, (None,)):
            with (
                self.subTest(inputs=inputs),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.plan(inputs)
        sources = (inventory(self.pkg, 0, (0,) * 97), inventory(self.pkg, 1, (1,) * 96))
        with self.assertRaisesRegex(ValueError, "source bound"):
            self.plan(sources)

    def test_matching_artifact_root_requires_exact_record_binding(self):
        source = self.inputs[0]
        record = replace(source.report.ledger.records[0], project="wrong-project")
        report = replace(
            source.report,
            ledger=replace(source.report.ledger, records=(record,)),
            observations=(replace(source.report.observations[0], admission=record),),
        )
        with self.assertRaisesRegex(ValueError, "artifact binding"):
            self.plan((replace(source, report=report),) + self.inputs[1:])

    def test_cancellation_aborts_without_a_partial_candidate(self):
        for stop in (1, 3, 10, 15):
            calls = []

            def cancel(stop=stop, calls=calls):
                calls.append(True)
                if len(calls) == stop:
                    raise RuntimeError("cancel")

            with (
                self.subTest(stop=stop),
                self.assertRaisesRegex(RuntimeError, "cancel"),
            ):
                self.plan(check_cancel=cancel)

    def test_matching_agrees_with_small_independent_exhaustive_oracle(self):
        # Exhaustive distinct-domain assignments for a bounded four-index graph.
        for domains in itertools.product(("a", "b", "c", "d"), repeat=4):
            sources = tuple(
                inventory(self.pkg, i, (i,), domain) for i, domain in enumerate(domains)
            )
            expected = len(set(domains)) == 4
            result = self.plan(sources)
            self.assertEqual(result.status == "candidate", expected)

    def test_multi_edge_matching_agrees_with_exhaustive_permutation_oracle(self):
        # Enumerate every 3-by-3 edge graph, plus a fixed fourth index/domain.
        # The independent oracle tries all assignments, not augmenting paths.
        for edges in itertools.product((False, True), repeat=9):
            sources = tuple(
                inventory(
                    self.pkg,
                    domain,
                    tuple(index for index in range(3) if edges[index * 3 + domain]),
                )
                for domain in range(3)
            ) + (inventory(self.pkg, 3, (3,)),)
            expected = any(
                all(edges[index * 3 + domain] for index, domain in enumerate(order))
                for order in itertools.permutations(range(3))
            )
            with self.subTest(edges=edges):
                result = self.plan(sources)
                self.assertEqual(result.status == "candidate", expected)
                if expected:
                    self.assertEqual(
                        {row.fragment_index for row in result.inputs}, {0, 1, 2, 3}
                    )
                    self.assertEqual(
                        len({row.failure_domain for row in result.inputs}), 4
                    )
                    available = {
                        (row.fragment_index, source.failure_domain)
                        for source in sources
                        for row in source.report.ledger.records
                    }
                    self.assertTrue(
                        all(
                            (row.fragment_index, row.failure_domain) in available
                            for row in result.inputs
                        )
                    )
                    self.assertEqual(result, self.plan(tuple(reversed(sources))))
                else:
                    self.assertEqual(result.inputs, ())
                    self.assertEqual(result.input_batches, ())

    def test_unequal_final_stripe_exact_and_one_byte_short_budgets(self):
        for source_size, final_size in ((17, 1), (20, 1), (21, 2), (25, 3)):
            self.pkg = package(bytes(range(source_size)), fragment_bytes=4)
            sources = tuple(inventory(self.pkg, i, (i, i + 6)) for i in range(4))
            read_bytes = 16 + 4 * final_size
            for targets, write_bytes in (
                ((4, 10), 4 + final_size),
                ((10,), final_size),
            ):
                with self.subTest(source_size=source_size, targets=targets):
                    limits = planning.RepairPlanLimits(read_bytes, write_bytes, 3, 2)
                    result = self.plan(sources, targets=targets, limits=limits)
                    self.assertEqual(result.status, "candidate")
                    self.assertEqual(result.required_read_bytes, read_bytes)
                    self.assertEqual(result.required_write_bytes, write_bytes)
                    self.assertEqual(
                        [row.size for row in result.inputs], [4] * 4 + [final_size] * 4
                    )
                    self.assertEqual(sum(row.size for row in result.inputs), read_bytes)
                    self.assertEqual(
                        [len(batch) for batch in result.input_batches], [3, 3, 2]
                    )
                    for short in (
                        replace(limits, max_read_bytes=read_bytes - 1),
                        replace(limits, max_write_bytes=write_bytes - 1),
                    ):
                        blocked = self.plan(sources, targets=targets, limits=short)
                        self.assertEqual(blocked.reason, "byte_budget")
                        self.assertEqual(blocked.required_read_bytes, read_bytes)
                        self.assertEqual(blocked.required_write_bytes, write_bytes)
                        self.assertEqual(blocked.inputs, ())
                        self.assertEqual(blocked.input_batches, ())
