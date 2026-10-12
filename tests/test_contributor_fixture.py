"""No contributor name appears without matching simulated execution evidence."""

import copy
import json
import unittest

from aethermesh_core.contributor_fixture import project_contributor_fixture
from tests.test_capability_fixture import FIXTURES


def fixture():
    return json.loads((FIXTURES / "contributor_activity.json").read_bytes())


def project(data, **options):
    return project_contributor_fixture(json.dumps(data).encode(), **options)


class ContributorFixtureTests(unittest.TestCase):
    def test_golden_simulated_only_and_no_selected_queued_activity(self):
        data = fixture()
        result = project(data)
        self.assertEqual(result.names, ("Ash", "Ember"))
        self.assertEqual(result.request_id, data["request_id"])
        self.assertTrue(result.simulated)
        self.assertFalse(result.live_activity_verified)
        data["events"] = []
        self.assertEqual(project(data).names, ())
        data["routes"] = []
        self.assertEqual(project(data).names, ())

    def test_queued_heartbeat_terminal_and_stale(self):
        for terminal in ("completed", "failed", "cancelled"):
            with self.subTest(terminal=terminal):
                data = fixture()
                event = data["events"][0]
                data["events"] = [
                    {**event, "state": "queued", "at": 980},
                    {**event, "sequence": 2, "at": 990},
                    {**event, "sequence": 3, "at": 1000},
                ]
                self.assertEqual(project(data).names, ("Ash",))
                data["events"].append(
                    {**event, "sequence": 4, "state": terminal, "at": 1000}
                )
                self.assertEqual(project(data).names, ())
        data = fixture()
        data["now"] = 1020
        self.assertEqual(project(data).names, ("Ash", "Ember"))
        data["now"] = 1021
        self.assertEqual(project(data).names, ())
        for initial in ("queued", "failed", "cancelled"):
            data = fixture()
            for event in data["events"]:
                event["state"] = initial
            self.assertEqual(project(data).names, ())

    def test_multiple_attempts_one_family_name(self):
        data = fixture()
        data["routes"].append({**data["routes"][0], "attempt_id": "hedge"})
        data["events"].append({**data["events"][0], "attempt_id": "hedge"})
        self.assertEqual(project(data).names, ("Ash", "Ember"))

    def test_route_bindings_and_limits(self):
        for key, value in (
            ("request_id", "b" * 64),
            ("lineage", "unknown"),
            ("artifact", "bad"),
        ):
            data = fixture()
            data["routes"][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                project(data)
        data = fixture()
        data["routes"].append(copy.deepcopy(data["routes"][0]))
        with self.assertRaises(ValueError):
            project(data)
        for key, value in (
            ("routes", [{}] * 33),
            ("events", [{}] * 65),
            ("now", 0),
            ("max_event_age", 3601),
            ("version", 2),
        ):
            data = fixture()
            data[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                project(data)

    def test_event_binding_order_and_state(self):
        for key, value in (
            ("attempt_id", "unknown"),
            ("request_id", "b" * 64),
            ("artifact", "b" * 64),
            ("sequence", 2),
            ("state", "completed"),
            ("state", "unknown"),
            ("at", 1001),
        ):
            data = fixture()
            data["events"][0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                project(data)
        for appended in (
            {"sequence": 1},
            {"sequence": 2, "at": 989},
            {"sequence": 2, "state": "queued"},
        ):
            data = fixture()
            data["events"].append(data["events"][0] | appended)
            with self.subTest(appended=appended), self.assertRaises(ValueError):
                project(data)
        data = fixture()
        event = data["events"][0]
        data["events"].extend(
            [{**event, "sequence": 2, "state": "completed"}, {**event, "sequence": 3}]
        )
        with self.assertRaises(ValueError):
            project(data)

    def test_cancellation(self):
        total = 0

        def record():
            nonlocal total
            total += 1

        project(fixture(), check_cancel=record)
        for stop in range(1, total + 1):
            count = 0

            def cancel(stop=stop):
                nonlocal count
                count += 1
                if count == stop:
                    raise InterruptedError("cancelled")

            with self.subTest(stop=stop), self.assertRaises(InterruptedError):
                project(fixture(), check_cancel=cancel)
