"""Print simulated proposals from checked-in fixtures; never report live activity."""

import json
from dataclasses import asdict
from pathlib import Path

from aethermesh_core.capability_fixture import evaluate_naming_fixture
from aethermesh_core.contributor_fixture import project_contributor_fixture


def main() -> None:
    fixtures = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
    pins = json.loads((fixtures / "capability_naming_pins.json").read_bytes())
    decision = evaluate_naming_fixture(
        (fixtures / "capability_naming.json").read_bytes(),
        trusted_report_digests=tuple(pins["reports"]),
        expected_policy_digest=pins["policy"],
    )
    activity = project_contributor_fixture(
        (fixtures / "contributor_activity.json").read_bytes()
    )
    print(
        json.dumps(
            {
                "simulated_proposal": asdict(decision),
                "simulated_contributor_names": asdict(activity),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
