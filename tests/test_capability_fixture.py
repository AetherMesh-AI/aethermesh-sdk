"""Synthetic evidence must never become publication or live capability authority."""

import copy
import decimal
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import capability_fixture as naming

FIXTURES = Path(__file__).parent / "fixtures"


def fixture():
    return json.loads((FIXTURES / "capability_naming.json").read_bytes())


def pins(data):
    return {
        "trusted_report_digests": tuple(
            naming.fixture_digest(data["reports"][key])
            for key in ("baseline", "candidate")
        ),
        "expected_policy_digest": naming.fixture_digest(data["policy"]),
    }


def evaluate(data, **options):
    return naming.evaluate_naming_fixture(
        json.dumps(data).encode(), **(pins(data) | options)
    )


def retax(data):
    identity = naming.fixture_digest(data["taxonomy"])
    data["policy"]["taxonomy"] = identity
    for report in data["reports"].values():
        report["taxonomy"] = identity


class NamingFixtureTests(unittest.TestCase):
    def test_golden_installed_contract_and_purity(self):
        raw = (FIXTURES / "capability_naming.json").read_bytes()
        stored = json.loads((FIXTURES / "capability_naming_pins.json").read_bytes())
        with (
            patch("builtins.open", side_effect=AssertionError("I/O")),
            patch("socket.socket", side_effect=AssertionError("network")),
            patch("subprocess.Popen", side_effect=AssertionError("execution")),
        ):
            result = naming.evaluate_naming_fixture(
                raw,
                trusted_report_digests=tuple(stored["reports"]),
                expected_policy_digest=stored["policy"],
            )
        self.assertEqual(result.status, "eligible")
        self.assertEqual(result.proposed_name, "Ash")
        self.assertEqual(result.measured_capabilities, ("python",))
        self.assertEqual(
            (result.current_internal_version, result.proposed_internal_version), (3, 4)
        )
        self.assertNotEqual(result.baseline_artifact, result.candidate_artifact)
        self.assertTrue(result.simulated)
        self.assertFalse(result.promotion_committed or result.publication_authorized)
        self.assertFalse(result.live_capability_verified or result.namespace_reserved)
        self.assertEqual(result, naming.evaluate_naming_fixture(raw, **pins(fixture())))

    def test_run_count_and_self_description_never_award_gains(self):
        data = fixture()
        before = evaluate(data)
        data["hints"] = {"training_run_count": 999, "self_description": "I am perfect"}
        after = evaluate(data)
        self.assertEqual(before.proposed_name, after.proposed_name)
        self.assertEqual(
            before.proposed_internal_version, after.proposed_internal_version
        )
        self.assertNotEqual(before.input_digest, after.input_digest)
        data["reports"]["candidate"]["metrics"]["accuracy"]["value"] = 0.8
        self.assertEqual(evaluate(data).proposed_internal_version, 3)

    def test_profile_and_report_binding(self):
        cases = [
            ("baseline", "artifact", "6" * 64),
            ("candidate", "parent", "6" * 64),
            ("candidate", "artifact", "1" * 64),
            ("candidate", "lineage", "other"),
            ("candidate", "schema", "6" * 64),
            ("candidate", "intended", "other"),
        ]
        for where, key, value in cases:
            with self.subTest(where=where, key=key):
                data = fixture()
                data[where][key] = value
                self.assertIn("profile_binding", evaluate(data).reasons)
        for which in ("baseline", "candidate"):
            for key in (
                "artifact",
                "baseline",
                "schema",
                "evaluation",
                "dataset",
                "taxonomy",
                "lineage",
            ):
                with self.subTest(which=which, key=key):
                    data = fixture()
                    data["reports"][which][key] = "6" * 64
                    result = evaluate(data)
                    self.assertIn("report_binding", result.reasons)
                    self.assertIsNone(result.proposed_name)
                    self.assertEqual(result.proposed_internal_version, 3)

    def test_independent_fixture_pins_and_evaluator(self):
        for evaluator in ("unapproved", "fixture-trainer"):
            with self.subTest(evaluator=evaluator):
                data = fixture()
                data["policy"]["evaluators"].append("fixture-trainer")
                data["reports"]["candidate"]["evaluator"] = evaluator
                self.assertIn("unverified_fixture_report", evaluate(data).reasons)
        data = fixture()
        old = pins(data)
        data["reports"]["candidate"]["metrics"]["accuracy"]["value"] = 0.95
        self.assertIn("unverified_fixture_report", evaluate(data, **old).reasons)
        data["policy"]["baseline_version"] += 1
        with self.assertRaisesRegex(ValueError, "policy pin"):
            evaluate(data, **old)

    def test_evaluation_and_taxonomy_revision_require_fresh_pair(self):
        for key in ("evaluation", "dataset"):
            data = fixture()
            data["policy"][key] = "6" * 64
            data["reports"]["candidate"][key] = "6" * 64
            self.assertIn("report_binding", evaluate(data).reasons)
            data["reports"]["baseline"][key] = "6" * 64
            self.assertEqual(evaluate(data).status, "eligible")
        data = fixture()
        data["taxonomy"]["revision"] += 1
        self.assertIn("taxonomy_revision", evaluate(data).reasons)
        retax(data)
        self.assertEqual(evaluate(data).status, "eligible")

    def test_metric_evidence_gates(self):
        for metric, field, value, reason in (
            ("accuracy", "value", 0.82, "insufficient_gain"),
            ("accuracy", "value", 0.7, "regression"),
            ("accuracy", "uncertainty", 0.02, "resource_or_uncertainty"),
            ("memory", "value", 3000, "resource_or_uncertainty"),
            ("memory", "value", -1, "resource_or_uncertainty"),
            ("memory", "value", 1100, "regression"),
        ):
            with self.subTest(metric=metric, field=field, value=value):
                data = fixture()
                data["reports"]["candidate"]["metrics"][metric][field] = value
                result = evaluate(data)
                self.assertIn(reason, result.reasons)
                self.assertEqual(result.status, "experimental")
                self.assertEqual(result.proposed_internal_version, 3)
                self.assertIsNone(result.proposed_name)
                self.assertEqual(result.measured_capabilities, ())
        for which in ("baseline", "candidate"):
            data = fixture()
            data["reports"][which]["metrics"].pop("memory")
            self.assertIn("metric_set", evaluate(data).reasons)

    def test_decimal_boundary_lower_gain_and_regression_allowance(self):
        data = fixture()
        for report in data["reports"].values():
            report["metrics"]["accuracy"]["uncertainty"] = 0
        data["reports"]["candidate"]["metrics"]["accuracy"]["value"] = 0.85
        data["policy"]["metrics"][1]["gain"] = 124
        self.assertEqual(evaluate(data).status, "eligible")
        data["policy"]["metrics"][1].update(gain=0, regression=76)
        data["reports"]["candidate"]["metrics"]["memory"]["value"] = 1100
        self.assertEqual(evaluate(data).status, "eligible")

    def test_classification_and_version_ceiling(self):
        for update in ({"minimum": 0.95}, {"maximum": 0.85}, {"metric": "unknown"}):
            data = fixture()
            data["taxonomy"]["rules"][0].update(update)
            retax(data)
            result = evaluate(data)
            self.assertEqual(result.status, "unclassified")
            self.assertIsNone(result.proposed_name)
        data = fixture()
        data["policy"]["baseline_version"] = 4096
        self.assertIn("version_exhausted", evaluate(data).reasons)
        self.assertEqual(evaluate(data).proposed_internal_version, 4096)

    def test_stable_names_collisions_and_reserved_roster(self):
        data = fixture()
        data["taxonomy"]["rules"][0]["words"] = ["Cedar"]
        retax(data)
        self.assertEqual(evaluate(data).proposed_name, "Ash")
        data["namespace"] = [{"lineage": "other", "name": "cEdAr"}]
        self.assertIn("name_unavailable", evaluate(data).reasons)
        data["taxonomy"]["rules"][0]["words"] = ["Compass", "Cedar", "Birch"]
        retax(data)
        self.assertEqual(evaluate(data).proposed_name, "Birch")
        data["namespace"] = [{"lineage": "python-family", "name": "Compass"}]
        self.assertIn("name_unavailable", evaluate(data).reasons)
        self.assertEqual(
            tuple(word for _, word in naming.ACCEPTED_FAMILIES),
            ("Compass", "Loom", "Tempo", "Ash", "Ember", "Lens", "Grove", "Vigil"),
        )

    def test_malformed_bounded_inputs(self):
        for raw in (
            b"",
            b"[]",
            b"{",
            b'{"x":1,"x":2}',
            b'{"x":NaN}',
            b'{"x":1e999}',
            b'{"x":"\\ud800"}',
            b"\xff",
            b"x" * 65537,
            b'{"x":' + b"[" * 20 + b"0" + b"]" * 20 + b"}",
        ):
            with self.subTest(raw=raw[:30]), self.assertRaises(ValueError):
                naming.evaluate_naming_fixture(raw, **pins(fixture()))
        raw = json.dumps(fixture()).encode()
        with self.assertRaisesRegex(ValueError, "aggregate"):
            naming.evaluate_naming_fixture(
                raw + b" " * (65500 - len(raw)), **pins(fixture())
            )
        for trusted in ((), [], ("1" * 64,) * 2, ("wrong",)):
            with self.subTest(trusted=trusted), self.assertRaises(ValueError):
                evaluate(fixture(), trusted_report_digests=trusted)
        with self.assertRaises(ValueError):
            naming.fixture_digest("x" * 65536)

    def test_schema_limits_and_conflicts(self):
        changes = [
            (("version",), 2),
            (("extra",), 1),
            (
                ("namespace",),
                [{"lineage": "a", "name": "Ash"}, {"lineage": "b", "name": "ash"}],
            ),
            (
                ("namespace",),
                [{"lineage": "a", "name": "Ash"}, {"lineage": "a", "name": "Cedar"}],
            ),
            (("namespace",), [{"lineage": "a", "name": "Ash2"}]),
            (("namespace",), [{}] * 33),
            (("hints", "training_run_count"), True),
            (("hints", "self_description"), "x" * 1025),
            (("reports", "candidate", "metrics"), {}),
            (("reports", "candidate", "metrics", "accuracy", "value"), True),
            (("reports", "candidate", "metrics", "accuracy", "value"), 10**13),
            (("reports", "candidate", "metrics", "accuracy", "uncertainty"), -1),
            (("policy", "evaluators"), ["same", "same"]),
            (("policy", "metrics", 0, "direction"), "unknown"),
            (("policy", "metrics", 0, "gain"), -1),
            (("policy", "metrics", 0, "minimum"), 2),
            (("policy", "metrics", 0, "gain"), 0),
            (("taxonomy", "rules", 0, "words"), ["Ash", "ash"]),
            (("taxonomy", "rules", 0, "minimum"), 2),
        ]
        for path, value in changes:
            with self.subTest(path=path, value=value):
                data = fixture()
                target = data
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.assertRaises(ValueError):
                    evaluate(data)
        for section in ("policy", "taxonomy"):
            data = fixture()
            key = "metrics" if section == "policy" else "rules"
            data[section][key].append(copy.deepcopy(data[section][key][0]))
            with self.assertRaises(ValueError):
                evaluate(data)

    def test_cancellation_at_every_checkpoint(self):
        count = 0

        def record():
            nonlocal count
            count += 1

        evaluate(fixture(), check_cancel=record)
        for stop in range(1, count + 1):
            current = 0

            def cancel(stop=stop):
                nonlocal current
                current += 1
                if current == stop:
                    raise InterruptedError("cancelled")

            with self.subTest(stop=stop), self.assertRaises(InterruptedError):
                evaluate(fixture(), check_cancel=cancel)

    def test_gate_decisions_ignore_ambient_decimal_context(self):
        accepted = fixture()
        for report in accepted["reports"].values():
            report["metrics"]["accuracy"]["uncertainty"] = 0
        accepted["reports"]["candidate"]["metrics"]["accuracy"]["value"] = 0.85
        rejected = copy.deepcopy(accepted)
        rejected["reports"]["candidate"]["metrics"]["accuracy"]["value"] = 0.849
        rounded_up = fixture()
        rounded_up["reports"]["candidate"]["metrics"]["accuracy"]["value"] = 0.84
        cases = [accepted, rejected, rounded_up]
        for uncertainty in (1e-30, 5e-324):
            tiny = copy.deepcopy(accepted)
            tiny["reports"]["candidate"]["metrics"]["accuracy"]["uncertainty"] = (
                uncertainty
            )
            self.assertIn("insufficient_gain", evaluate(tiny).reasons)
            cases.append(tiny)
        classified = copy.deepcopy(cases[3])
        classified["policy"]["metrics"][0]["gain"] = 0.04
        classified["taxonomy"]["rules"][0]["minimum"] = 0.85
        retax(classified)
        self.assertEqual(evaluate(classified).status, "unclassified")
        resource = copy.deepcopy(cases[3])
        resource["policy"]["metrics"][0].update(gain=0.04, maximum=0.85)
        self.assertIn("resource_or_uncertainty", evaluate(resource).reasons)
        regression = copy.deepcopy(accepted)
        regression["reports"]["candidate"]["metrics"]["memory"].update(
            value=1024, uncertainty=1e-30
        )
        regression["policy"]["metrics"][1]["uncertainty"] = 1
        self.assertIn("regression", evaluate(regression).reasons)
        cases.extend((classified, resource, regression))
        expected = tuple(evaluate(case) for case in cases)
        self.assertEqual(expected[0].status, "eligible")
        self.assertIn("insufficient_gain", expected[1].reasons)
        self.assertIn("insufficient_gain", expected[2].reasons)
        for precision in (1, 2, 28, 100, 350):
            for rounding in (
                decimal.ROUND_DOWN,
                decimal.ROUND_UP,
                decimal.ROUND_HALF_EVEN,
                decimal.ROUND_FLOOR,
                decimal.ROUND_CEILING,
            ):
                with (
                    self.subTest(precision=precision, rounding=rounding),
                    decimal.localcontext() as context,
                ):
                    context.prec = precision
                    context.rounding = rounding
                    context.Emax = 1
                    context.Emin = -1
                    for signal in context.traps:
                        context.traps[signal] = True
                    self.assertEqual(tuple(evaluate(case) for case in cases), expected)
                    self.assertFalse(any(context.flags.values()))
                    with self.assertRaises(ValueError):
                        naming._number(10**12 + 1)

    def test_json_numeric_decoding_cannot_erase_uncertainty_or_round_a_gain(self):
        data = fixture()
        data["reports"]["baseline"]["metrics"]["accuracy"]["uncertainty"] = 0
        data["reports"]["candidate"]["metrics"]["accuracy"].update(
            value=0.85, uncertainty=1e-30
        )
        raw = json.dumps(data).encode()
        for malformed in (
            raw.replace(b"1e-30", b"1e-400"),
            raw.replace(b"0.85", b"0.849999999999999999999999999999"),
            raw.replace(b"1e-30", b"0e99999999999999999999999999999"),
            raw.replace(b"1e-30", b"0." + b"0" * 1024 + b"1"),
        ):
            with self.subTest(raw=malformed[-100:]), self.assertRaises(ValueError):
                naming.evaluate_naming_fixture(malformed, **pins(data))
        exact = raw.replace(b"0.85", b"0.850000000000000000000000000000")
        self.assertEqual(
            naming.evaluate_naming_fixture(exact, **pins(data)), evaluate(data)
        )
