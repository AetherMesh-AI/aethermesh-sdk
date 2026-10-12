# Numeric evaluation to naming proposal bridge

The external `examples/numeric_naming_bridge.py` connects actual numeric evaluator
outputs to the [bounded naming fixture](capability-fixture.md). It retains
`synthetic-quadratic-expert` as the prototype identity. `Numeric` is solely the
fixture's internal one-word alias, not a new accepted production family or a
replacement for Ash/Ember. No contributor activity is emitted.

```sh
PYTHONPATH=src python examples/numeric_naming_bridge.py
```

With an installed SDK, run the same file with `python -I` from any directory.
Keep the trusted adjacent numeric demo and worker files together. The installed
acceptance runs this exact path outside the source checkout.

## What runs

The bridge invokes only the existing numeric worker's `evaluate` operation, three
times: the documented baseline coefficients, existing fitted coefficients, and the
previously documented regression control with its intercept increased by one.
These fixed data-only coefficients are copied from the existing numeric example;
no new checkpoint is trained, no optimizer/solver runs, and no learning journal is
created, claimed or changed. It does not fetch/load a model or execute received code.

The worker actually computes eight rational predictions for each artifact. The
bridge independently recomputes every prediction with the existing coordinator
implementation and checks the exact task ID and result shape before computing
metrics or deriving report pins. It never translates an unverified claimed score.

Expected measured results on the small fixed synthetic suite:

| Artifact | Held-out squared error | Regressed cases | Fixture proposal |
| --- | ---: | ---: | --- |
| Existing baseline | 250 | 0 | Reference only |
| Existing fitted polynomial | 0 | 0 | Eligible; internal fixture version 0 → 1 |
| Regression control | 4 | 1 | Experimental; no name or version increment |

The regression control improves aggregate held-out error but violates the separate
per-case regression gate. Its rejection is based on actual calculated outputs,
not a hand-authored reject flag. Exact arithmetic describes this finite suite;
zero uncertainty is not a statistical claim about unseen workloads. Non-integral
SSE is rejected rather than rounded into the fixture's numeric format.

## Exact bindings and trust

The task commitment includes the prototype, exact baseline and candidate artifact
commitments, feature rows, dataset/suite commitment, schema and source digests for
the worker, original demo and bridge. The full held-out/regression rows—including
labels—are committed as the dataset. Evaluation identity includes the baseline,
dataset, source revisions and metric semantics. Model artifact IDs commit canonical
JSON coefficients. All are printed in the output for inspection and replay.

The adapter checks its locally constructed context, fences source digests before
and after evaluator execution, and rejects changes since module loading. Baseline
and candidate output validation precedes construction of both fixture reports.
Report/policy pins are derived locally only after those checks. Wrong artifacts,
baselines, suite/schema/taxonomy commitments, task IDs, predictions and extra or
missing output fields are rejected. Output includes original predictions, translated
reports, policy, pins and the resulting fixture decisions.

This establishes verified local arithmetic from trusted code, not authenticated
independent evaluator identity, training attestation or publisher authority. The
trainer/evaluator labels are descriptive; the two processes belong to the same
operator. The suite is public, tiny and synthetic. The bridge cannot assign a
production family, allocate a durable capability version, activate an expert or
promote a release. Internal version 0 is a fixture starting point, not a claim
about an existing journal version.

The outer report says `actual_numeric_evaluation=True`, `training_performed=False`,
`evidence_authenticated=False`, `independent_operators_verified=False`,
`public_family_assigned=False` and `production_promotion=False`. Embedded decisions
retain `simulated=True` and all fixture authority flags false: real arithmetic can
feed a simulated proposal policy without making that policy a production authority.

## Bounds and validation

Only three fixed artifacts, eight evaluation rows each and the existing six
bounded rational coefficients are involved. Existing helper limits apply: 16-KiB
stdin/stdout, 4-KiB stderr, five-second wall deadline and two-second CPU ceiling.
Workers run sequentially and are reaped on failure/cancellation. These are not RSS,
security-sandbox or independent-host guarantees. The fixture's own size/schema and
exact arithmetic gates still apply. There is no listener, credential, paid service,
background worker, runtime SDK trainer or app orchestration.

Tests execute the real helper and assert every requested operation is evaluation,
check measured acceptance/rejection and identity bindings, reject altered outputs
and contexts, detect source drift, propagate cancellation without partial proposals,
and reject lossy rational metric conversion. Installed acceptance checks the same
example against the built SDK. Existing repository tests still exercise the original
synthetic trainer as regression coverage; this bridge does not invoke that path.

This closes the local evaluation-to-proposal loop only. Generalized task evidence,
independent provenance, real capability measurement, durable promotion and routed
contributor activity remain separate work. The storage roadmap continues unchanged.
