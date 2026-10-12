"""Bounded, pure naming/promotion fixtures; no training or publication authority."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Literal

from .artifact_codec import CheckCancel, _cancel, _integer
from .network.config import validate_fingerprint, validate_project_id
from .network.errors import ProtocolError
from .network.protocol import _constant, _object, _validate_message

MAX_FIXTURE_BYTES = 65536
ACCEPTED_FAMILIES = (
    ("routing", "Compass"),
    ("planning", "Loom"),
    ("resources", "Tempo"),
    ("python", "Ash"),
    ("typescript", "Ember"),
    ("evaluation", "Lens"),
    ("data", "Grove"),
    ("monitoring", "Vigil"),
)


def fixture_digest(value: Any) -> str:
    """Commit already bounded fixture data; not an untrusted object serializer.

    A digest identifies fixture bytes, never evaluator or publisher authority.
    """
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")
    if len(raw) > MAX_FIXTURE_BYTES:
        raise ValueError("fixture commitment exceeds byte bound")
    return hashlib.sha256(raw).hexdigest()


def _decode(raw: bytes) -> dict[str, Any]:
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_FIXTURE_BYTES:
        raise ValueError("fixture input exceeds byte bound")
    try:
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_object, parse_constant=_constant
        )
        _validate_message(value)
    except (ValueError, RecursionError, ProtocolError) as error:
        raise ValueError("invalid bounded fixture JSON") from error
    return _fields(value, set(value))


def _fields(value: Any, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("unexpected fixture fields")
    return value


def _items(value: Any, maximum: int, minimum: int = 0) -> list[Any]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ValueError("fixture item bound")
    return value


def _identifier(value: Any) -> str:
    validate_project_id(value)
    return str(value)


def _digest(value: Any) -> str:
    validate_fingerprint(value)
    return str(value)


def _number(value: Any) -> Fraction:
    if type(value) not in (int, float):
        raise ValueError("fixture metric must be a finite number")
    result = Fraction(str(value))
    if abs(result) > 10**12:
        raise ValueError("fixture metric magnitude bound")
    return result


def _word(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z]{1,32}", value) is None:
        raise ValueError("fixture name must be one word of 1–32 ASCII letters")
    return value


def _namespace(value: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    words: set[str] = set()
    for item in _items(value, 32):
        item = _fields(item, {"lineage", "name"})
        lineage, word = _identifier(item["lineage"]), _word(item["name"])
        if lineage in result or word.casefold() in words:
            raise ValueError("conflicting fixture namespace")
        result[lineage] = word
        words.add(word.casefold())
    return result


def _profile(value: Any) -> dict[str, Any]:
    row = _fields(
        value, {"artifact", "parent", "lineage", "schema", "trainer", "intended"}
    )
    for key in ("artifact", "schema"):
        _digest(row[key])
    if row["parent"] is not None:
        _digest(row["parent"])
    for key in ("lineage", "trainer", "intended"):
        _identifier(row[key])
    return row


def _report(value: Any) -> dict[str, Any]:
    row = _fields(
        value,
        {
            "artifact",
            "baseline",
            "lineage",
            "schema",
            "evaluation",
            "dataset",
            "taxonomy",
            "evaluator",
            "metrics",
        },
    )
    for key in ("artifact", "baseline", "schema", "evaluation", "dataset", "taxonomy"):
        _digest(row[key])
    for key in ("lineage", "evaluator"):
        _identifier(row[key])
    metrics = row["metrics"]
    if not isinstance(metrics, dict) or not 1 <= len(metrics) <= 64:
        raise ValueError("fixture metric count bound")
    for name, reading in metrics.items():
        _identifier(name)
        _fields(reading, {"value", "uncertainty"})
        _number(reading["value"])
        if _number(reading["uncertainty"]) < 0:
            raise ValueError("negative fixture uncertainty")
    return row


def _taxonomy(value: Any) -> dict[str, dict[str, Any]]:
    taxonomy = _fields(value, {"revision", "rules"})
    _integer(taxonomy["revision"], 1, 4096)
    result = {}
    for rule in _items(taxonomy["rules"], 32, 1):
        _fields(rule, {"label", "metric", "minimum", "maximum", "words"})
        label = _identifier(rule["label"])
        _identifier(rule["metric"])
        if label in result or _number(rule["minimum"]) > _number(rule["maximum"]):
            raise ValueError("conflicting fixture taxonomy")
        words = [_word(word) for word in _items(rule["words"], 16, 1)]
        if len({word.casefold() for word in words}) != len(words):
            raise ValueError("duplicate fixture words")
        result[label] = rule
    return result


def _policy(value: Any) -> dict[str, dict[str, Any]]:
    policy = _fields(
        value,
        {
            "baseline",
            "baseline_version",
            "evaluation",
            "dataset",
            "taxonomy",
            "evaluators",
            "metrics",
        },
    )
    for key in ("baseline", "evaluation", "dataset", "taxonomy"):
        _digest(policy[key])
    _integer(policy["baseline_version"], 0, 4096)
    evaluators = [_identifier(item) for item in _items(policy["evaluators"], 16, 1)]
    if len(set(evaluators)) != len(evaluators):
        raise ValueError("duplicate fixture evaluators")
    result = {}
    for rule in _items(policy["metrics"], 64, 1):
        _fields(
            rule,
            {
                "name",
                "direction",
                "gain",
                "regression",
                "uncertainty",
                "minimum",
                "maximum",
            },
        )
        name = _identifier(rule["name"])
        if name in result or rule["direction"] not in ("higher", "lower"):
            raise ValueError("conflicting fixture metric policy")
        if any(
            _number(rule[key]) < 0 for key in ("gain", "regression", "uncertainty")
        ) or _number(rule["minimum"]) > _number(rule["maximum"]):
            raise ValueError("invalid fixture metric limits")
        result[name] = rule
    if not any(_number(rule["gain"]) > 0 for rule in result.values()):
        raise ValueError("fixture promotion must require a measurable gain")
    return result


@dataclass(frozen=True)
class FixtureDecision:
    status: Literal["eligible", "experimental", "unclassified"]
    reasons: tuple[str, ...]
    proposed_name: str | None
    measured_capabilities: tuple[str, ...]
    current_internal_version: int
    proposed_internal_version: int
    baseline_artifact: str
    candidate_artifact: str
    input_digest: str
    policy_digest: str
    report_digests: tuple[str, str]
    simulated: Literal[True] = True
    promotion_committed: Literal[False] = False
    publication_authorized: Literal[False] = False
    live_capability_verified: Literal[False] = False
    namespace_reserved: Literal[False] = False


def _bindings(
    data: dict[str, Any],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    reports: tuple[dict[str, Any], dict[str, Any]],
    report_ids: tuple[str, str],
    trusted: tuple[str, ...],
) -> list[str]:
    policy = data["policy"]
    reasons = []
    if (
        baseline["artifact"] != policy["baseline"]
        or candidate["parent"] != baseline["artifact"]
        or candidate["artifact"] == baseline["artifact"]
        or any(
            candidate[key] != baseline[key] for key in ("lineage", "schema", "intended")
        )
    ):
        reasons.append("profile_binding")
    if fixture_digest(data["taxonomy"]) != policy["taxonomy"]:
        reasons.append("taxonomy_revision")
    for profile, report, identity in zip(
        (baseline, candidate), reports, report_ids, strict=True
    ):
        if (
            identity not in trusted
            or report["evaluator"] not in policy["evaluators"]
            or report["evaluator"] in (baseline["trainer"], candidate["trainer"])
        ):
            reasons.append("unverified_fixture_report")
        expected = {
            "artifact": profile["artifact"],
            "baseline": baseline["artifact"],
            "lineage": profile["lineage"],
            "schema": profile["schema"],
            "evaluation": policy["evaluation"],
            "dataset": policy["dataset"],
            "taxonomy": policy["taxonomy"],
        }
        if any(report[key] != value for key, value in expected.items()):
            reasons.append("report_binding")
    return reasons


def _metric_reasons(
    rules: dict[str, dict[str, Any]],
    reports: tuple[dict[str, Any], dict[str, Any]],
    check_cancel: CheckCancel | None,
) -> list[str]:
    baseline, candidate = (report["metrics"] for report in reports)
    if set(baseline) != set(rules) or set(candidate) != set(rules):
        return ["metric_set"]
    reasons = []
    for name, rule in rules.items():
        _cancel(check_cancel)
        b, c = baseline[name], candidate[name]
        bv, cv = _number(b["value"]), _number(c["value"])
        bu, cu = _number(b["uncertainty"]), _number(c["uncertainty"])
        if (
            max(bu, cu) > _number(rule["uncertainty"])
            or cv - cu < _number(rule["minimum"])
            or cv + cu > _number(rule["maximum"])
        ):
            reasons.append("resource_or_uncertainty")
        gain = cv - cu - bv - bu if rule["direction"] == "higher" else bv - bu - cv - cu
        if gain < -_number(rule["regression"]):
            reasons.append("regression")
        if _number(rule["gain"]) > 0 and gain < _number(rule["gain"]):
            reasons.append("insufficient_gain")
    return reasons


def _name(
    lineage: str, label: str, words: list[Any], namespace: dict[str, str]
) -> str | None:
    reserved = {word.casefold(): role for role, word in ACCEPTED_FAMILIES}
    candidates = [namespace[lineage]] if lineage in namespace else words
    for candidate in candidates:
        word = _word(candidate)
        if word.casefold() in reserved and reserved[word.casefold()] != label:
            continue
        if lineage in namespace or word.casefold() not in {
            name.casefold() for name in namespace.values()
        }:
            return word
    return None


def evaluate_naming_fixture(
    raw: bytes,
    *,
    trusted_report_digests: tuple[str, ...],
    expected_policy_digest: str,
    check_cancel: CheckCancel | None = None,
) -> FixtureDecision:
    """Pure local proposal from explicitly pinned fixture reports and policy.

    Pin selection is simulated provenance, not independent real-world evaluation.
    No version/name is committed. Callers must not treat this result as a release,
    trained capability, live activity or authorization to publish.
    """
    _cancel(check_cancel)
    if (
        not isinstance(trusted_report_digests, tuple)
        or not 1 <= len(trusted_report_digests) <= 64
        or len(set(trusted_report_digests)) != len(trusted_report_digests)
    ):
        raise ValueError("fixture report pin bound")
    for identity in (*trusted_report_digests, expected_policy_digest):
        _digest(identity)
    decoded = _decode(raw)
    if len(raw) + 64 * (len(trusted_report_digests) + 1) > MAX_FIXTURE_BYTES:
        raise ValueError("aggregate fixture byte bound")
    data = _fields(
        decoded,
        {
            "version",
            "baseline",
            "candidate",
            "reports",
            "taxonomy",
            "policy",
            "namespace",
            "hints",
        },
    )
    _integer(data["version"], 1, 1)
    baseline, candidate = _profile(data["baseline"]), _profile(data["candidate"])
    report_data = _fields(data["reports"], {"baseline", "candidate"})
    reports = (_report(report_data["baseline"]), _report(report_data["candidate"]))
    report_ids = (fixture_digest(reports[0]), fixture_digest(reports[1]))
    taxonomy, rules, namespace = (
        _taxonomy(data["taxonomy"]),
        _policy(data["policy"]),
        _namespace(data["namespace"]),
    )
    hints = _fields(data["hints"], {"training_run_count", "self_description"})
    _integer(hints["training_run_count"], 0, 10**9)
    if (
        not isinstance(hints["self_description"], str)
        or len(hints["self_description"]) > 1024
    ):
        raise ValueError("fixture description bound")
    policy_id = fixture_digest(data["policy"])
    if policy_id != expected_policy_digest:
        raise ValueError("fixture policy pin mismatch")
    reasons = _bindings(
        data, baseline, candidate, reports, report_ids, trusted_report_digests
    )
    labels: list[str] = []
    name = None
    if not reasons:
        reasons.extend(_metric_reasons(rules, reports, check_cancel))
    if not reasons:
        for label, rule in taxonomy.items():
            _cancel(check_cancel)
            reading = reports[1]["metrics"].get(rule["metric"])
            if (
                reading is not None
                and _number(reading["value"]) - _number(reading["uncertainty"])
                >= _number(rule["minimum"])
                and _number(reading["value"]) + _number(reading["uncertainty"])
                <= _number(rule["maximum"])
            ):
                labels.append(label)
        primary = candidate["intended"]
        if primary not in labels:
            reasons.append("unclassified")
        else:
            name = _name(
                candidate["lineage"], primary, taxonomy[primary]["words"], namespace
            )
            if name is None:
                reasons.append("name_unavailable")
    version = data["policy"]["baseline_version"]
    if version == 4096:
        reasons.append("version_exhausted")
    _cancel(check_cancel)
    status: Literal["eligible", "experimental", "unclassified"] = (
        "unclassified"
        if "unclassified" in reasons
        else "experimental"
        if reasons
        else "eligible"
    )
    return FixtureDecision(
        status,
        tuple(sorted(set(reasons))),
        None if reasons else name,
        tuple(sorted(labels)),
        version,
        version if reasons else version + 1,
        baseline["artifact"],
        candidate["artifact"],
        fixture_digest(data),
        policy_id,
        report_ids,
    )
