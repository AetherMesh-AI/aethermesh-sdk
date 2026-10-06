import unittest

from aethermesh_core.lifecycle import (
    LIFECYCLE_STATE_SPECS,
    LifecycleRecord,
    LifecycleTransitionError,
    LocalNodeLifecycleState,
    allowed_next_states,
    canonical_lifecycle_states,
    recover_lifecycle_state,
    validate_lifecycle_record,
    validate_transition,
)


def _record(
    state: LocalNodeLifecycleState,
    *,
    creator_node_id: str = "node-local-creator",
    active_manifest_ref: str = "manifests/local-batch.json#node:node-local-creator",
    lineage_refs: tuple[str, ...] = ("lineage/root.json",),
    validation_receipt_refs: tuple[str, ...] = ("receipts/validation-1.json",),
    contribution_refs: tuple[str, ...] = ("contributions/contribution-1.json",),
    failure_reason: str | None = None,
    failure_terminal: bool = False,
) -> LifecycleRecord:
    return LifecycleRecord(
        state=state,
        creator_node_id=creator_node_id,
        active_manifest_ref=active_manifest_ref,
        lineage_refs=lineage_refs,
        validation_receipt_refs=validation_receipt_refs,
        contribution_refs=contribution_refs,
        event=f"enter_{state.value}",
        failure_reason=failure_reason,
        failure_terminal=failure_terminal,
    )


class LocalNodeLifecycleTests(unittest.TestCase):
    def test_all_canonical_states_have_purpose_and_next_states(self) -> None:
        expected = (
            LocalNodeLifecycleState.CREATED,
            LocalNodeLifecycleState.CONFIGURED,
            LocalNodeLifecycleState.READY,
            LocalNodeLifecycleState.RUNNING,
            LocalNodeLifecycleState.PAUSED,
            LocalNodeLifecycleState.VALIDATING,
            LocalNodeLifecycleState.COMPLETED,
            LocalNodeLifecycleState.FAILED,
            LocalNodeLifecycleState.RETIRED,
        )

        self.assertEqual(canonical_lifecycle_states(), expected)
        self.assertEqual(set(LIFECYCLE_STATE_SPECS), set(expected))
        for state in expected:
            spec = LIFECYCLE_STATE_SPECS[state]
            self.assertTrue(spec.purpose)
            self.assertEqual(allowed_next_states(state), spec.allowed_next_states)
            self.assertNotIn(state, spec.allowed_next_states)
        self.assertEqual(allowed_next_states(LocalNodeLifecycleState.RETIRED), ())

    def test_non_work_configuration_transition_accepts_required_record_fields(
        self,
    ) -> None:
        validate_transition(
            _record(
                LocalNodeLifecycleState.CREATED,
                lineage_refs=(),
                validation_receipt_refs=(),
                contribution_refs=(),
            ),
            _record(
                LocalNodeLifecycleState.CONFIGURED,
                lineage_refs=(),
                validation_receipt_refs=(),
                contribution_refs=(),
            ),
        )

    def test_valid_local_progression_preserves_required_work_links(self) -> None:
        ready = _record(LocalNodeLifecycleState.READY)
        running = _record(LocalNodeLifecycleState.RUNNING)
        validating = _record(
            LocalNodeLifecycleState.VALIDATING,
            validation_receipt_refs=(
                "receipts/validation-1.json",
                "receipts/validation-2.json",
            ),
        )
        completed = _record(
            LocalNodeLifecycleState.COMPLETED,
            validation_receipt_refs=(
                "receipts/validation-1.json",
                "receipts/validation-2.json",
            ),
            contribution_refs=(
                "contributions/contribution-1.json",
                "contributions/contribution-2.json",
            ),
        )

        validate_transition(ready, running)
        validate_transition(running, validating)
        validate_transition(validating, completed)

    def test_invalid_transitions_are_rejected_as_errors(self) -> None:
        with self.assertRaisesRegex(LifecycleTransitionError, "created -> running"):
            validate_transition(
                _record(LocalNodeLifecycleState.CREATED),
                _record(LocalNodeLifecycleState.RUNNING),
            )

        with self.assertRaisesRegex(
            LifecycleTransitionError, "completed -> validating"
        ):
            validate_transition(
                _record(LocalNodeLifecycleState.COMPLETED),
                _record(LocalNodeLifecycleState.VALIDATING),
            )

        failed = _record(
            LocalNodeLifecycleState.FAILED,
            failure_reason="manifest validation failed",
            failure_terminal=True,
        )
        with self.assertRaisesRegex(LifecycleTransitionError, "only retire"):
            validate_transition(failed, _record(LocalNodeLifecycleState.READY))
        validate_transition(failed, _record(LocalNodeLifecycleState.RETIRED))

    def test_every_record_requires_creator_node_id_and_active_manifest(self) -> None:
        with self.assertRaisesRegex(LifecycleTransitionError, "event"):
            validate_lifecycle_record(
                LifecycleRecord(
                    state=LocalNodeLifecycleState.CREATED,
                    creator_node_id="node-local-creator",
                    active_manifest_ref="manifests/local-batch.json#node:node-local-creator",
                )
            )
        with self.assertRaisesRegex(LifecycleTransitionError, "creator_node_id"):
            validate_lifecycle_record(
                _record(LocalNodeLifecycleState.CREATED, creator_node_id="")
            )
        with self.assertRaisesRegex(LifecycleTransitionError, "active_manifest_ref"):
            validate_lifecycle_record(
                _record(LocalNodeLifecycleState.CONFIGURED, active_manifest_ref="")
            )
        with self.assertRaisesRegex(LifecycleTransitionError, "failure_reason"):
            validate_lifecycle_record(_record(LocalNodeLifecycleState.FAILED))
        with self.assertRaisesRegex(LifecycleTransitionError, "failure_terminal"):
            validate_lifecycle_record(
                _record(LocalNodeLifecycleState.READY, failure_terminal=True)
            )

    def test_work_transitions_cannot_drop_attribution_or_validation_links(self) -> None:
        running = _record(LocalNodeLifecycleState.RUNNING)
        cases = (
            (
                "creator_node_id",
                _record(LocalNodeLifecycleState.PAUSED, creator_node_id="other-node"),
            ),
            (
                "active_manifest_ref",
                _record(
                    LocalNodeLifecycleState.PAUSED, active_manifest_ref="other.json"
                ),
            ),
            (
                "lineage_refs",
                _record(LocalNodeLifecycleState.PAUSED, lineage_refs=()),
            ),
            (
                "validation_receipt_refs",
                _record(LocalNodeLifecycleState.PAUSED, validation_receipt_refs=()),
            ),
            (
                "contribution_refs",
                _record(LocalNodeLifecycleState.PAUSED, contribution_refs=()),
            ),
        )
        for expected_error, next_record in cases:
            with (
                self.subTest(expected_error=expected_error),
                self.assertRaisesRegex(LifecycleTransitionError, expected_error),
            ):
                validate_transition(running, next_record)

    def test_recovery_is_derived_from_persisted_records_and_runtime_marker(
        self,
    ) -> None:
        self.assertEqual(
            recover_lifecycle_state(
                _record(LocalNodeLifecycleState.READY), runtime_active=True
            ),
            LocalNodeLifecycleState.RUNNING,
        )
        self.assertEqual(
            recover_lifecycle_state(
                _record(LocalNodeLifecycleState.READY), runtime_active=False
            ),
            LocalNodeLifecycleState.READY,
        )
        for state in (
            LocalNodeLifecycleState.PAUSED,
            LocalNodeLifecycleState.VALIDATING,
            LocalNodeLifecycleState.COMPLETED,
            LocalNodeLifecycleState.FAILED,
            LocalNodeLifecycleState.RETIRED,
        ):
            with self.subTest(state=state):
                recovered = recover_lifecycle_state(
                    _record(
                        state,
                        failure_reason="validation failed"
                        if state is LocalNodeLifecycleState.FAILED
                        else None,
                    ),
                    runtime_active=True,
                )
                self.assertEqual(recovered, state)


if __name__ == "__main__":
    unittest.main()
