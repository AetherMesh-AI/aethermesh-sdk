"""Deterministic aggregate budget and correlated failure simulations."""

import dataclasses
import unittest

from aethermesh_core.artifact_placement import (
    NodeBudget,
    PlacementLimits,
    plan_placement,
    simulate_domains,
)
from tests.test_artifact_codec import package


def nodes():
    return tuple(
        NodeBudget(f"n{i}", f"op{i % 2}", f"domain{i}", 100, 1000, 30, 100_000, 100_000)
        for i in range(6)
    )


class PlacementTests(unittest.TestCase):
    def test_complete_aggregate_plan_and_domain_outages(self):
        manifest = package(bytes(range(39)), fragment_bytes=3).manifest
        inventory = nodes()
        plan = plan_placement(
            manifest, inventory, limits=PlacementLimits(max_inflight=8), now=1
        )
        self.assertEqual(plan.status, "complete")
        self.assertEqual(len(plan.assignments), 24)
        self.assertEqual(plan.unplaced, ())
        for reservation in plan.reservations:
            self.assertEqual(reservation.objects, 4)
            self.assertEqual(reservation.storage_bytes, 10)
            self.assertEqual(reservation.memory_bytes, 16)
        for batch in plan.batches:
            self.assertLessEqual(len(batch), 8)
            self.assertEqual(len({node for _, node in batch}), len(batch))
        self.assertEqual(
            simulate_domains(manifest, plan, inventory), ("available",) * 4
        )
        self.assertEqual(
            simulate_domains(
                manifest, plan, inventory, offline=("domain0", "domain1", "domain2")
            ),
            ("temporarily_unavailable",) * 4,
        )
        self.assertEqual(
            simulate_domains(
                manifest, plan, inventory, lost=("domain0", "domain1", "domain2")
            ),
            ("insufficient_retained",) * 4,
        )
        self.assertEqual(
            simulate_domains(manifest, plan, inventory, lost=("domain0", "domain1")),
            ("available",) * 4,
        )
        with self.assertRaises(ValueError):
            simulate_domains(
                manifest, plan, inventory, offline=("domain0",), lost=("domain0",)
            )
        with self.assertRaises(ValueError):
            simulate_domains(
                manifest, dataclasses.replace(plan, manifest_digest="0" * 64), inventory
            )

    def test_resource_and_domain_degradation_is_explicit(self):
        manifest = package().manifest
        for change in (
            {"storage_bytes": 0},
            {"memory_bytes": 0},
            {"object_slots": 0},
            {"ingress_bytes": 0},
            {"egress_bytes": 0},
            {"max_inflight": 0},
            {"failure_domain": "shared"},
            {"operator": "same"},
        ):
            inventory = tuple(dataclasses.replace(node, **change) for node in nodes())
            plan = plan_placement(manifest, inventory, limits=PlacementLimits(), now=1)
            with self.subTest(change=change):
                self.assertEqual(plan.status, "degraded")
                self.assertEqual(plan.assignments, ())
                self.assertEqual(plan.reservations, ())
                self.assertEqual(plan.unplaced, tuple(range(6)))
                self.assertEqual(
                    simulate_domains(manifest, plan, inventory),
                    ("insufficient_retained",),
                )
        plan = plan_placement(
            manifest, nodes(), limits=PlacementLimits(max_transfer_bytes=0), now=1
        )
        self.assertEqual(plan.status, "degraded")

    def test_failed_stripe_rolls_back_capacity_and_smaller_stripe_proceeds(self):
        manifest = package(bytes(range(19)), fragment_bytes=4).manifest
        inventory = tuple(
            dataclasses.replace(node, storage_bytes=2) for node in nodes()
        )
        plan = plan_placement(manifest, inventory, limits=PlacementLimits(), now=1)
        self.assertEqual(plan.unplaced, tuple(range(6)))
        self.assertEqual(tuple(i for i, _ in plan.assignments), tuple(range(6, 12)))
        self.assertTrue(all(x.storage_bytes == 1 for x in plan.reservations))

    def test_expiry_deletion_and_invalid_policies(self):
        manifest = package().manifest
        for changes, status in (
            ({"now": 100}, "expired"),
            ({"deleted": True}, "deleted"),
        ):
            plan = plan_placement(
                manifest, nodes(), **({"limits": PlacementLimits(), "now": 1} | changes)
            )
            self.assertEqual(plan.status, status)
            self.assertFalse(plan.assignments)
        for inventory in (list(nodes()), nodes() + nodes(), (None,), nodes() * 6):
            with self.assertRaises(ValueError):
                plan_placement(manifest, inventory, limits=PlacementLimits(), now=1)
        for changes in ({"limits": None}, {"deleted": 1}, {"now": True}):
            with self.assertRaises(ValueError):
                plan_placement(
                    manifest,
                    nodes(),
                    **({"limits": PlacementLimits(), "now": 1} | changes),
                )
        for changes in ({"max_inflight": False}, {"min_operators": 7}):
            with self.assertRaises(ValueError):
                PlacementLimits(**changes)
        with self.assertRaises(ValueError):
            dataclasses.replace(nodes()[0], storage_bytes=-1)
