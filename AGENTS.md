# AetherMesh SDK Agent Notes

The repository owns a headless SDK for apps connecting to a future AetherMesh
network. Read `docs/sdk-scope.md` before choosing work. Keep protocol and local
node foundations; do not add desktop UI, app agent orchestration, OS installers,
model trainers, or a full build scheduler. Public P2P is not implemented.

This repository keeps a Graphify knowledge graph under `graphify-out/`.
Its checked-in snapshot predates the SDK cleanup and is historical navigation,
not a current inventory or scope specification. Verify its claims against code.

When answering architecture questions, planning the next implementation step, or making non-trivial code changes:

1. Prefer querying the graph before reading many files:
   ```bash
   graphify query "<question>"
   ```
2. Use focused graph commands when useful:
   ```bash
   graphify explain "<symbol-or-concept>"
   graphify path "<A>" "<B>"
   ```
3. After code changes land on `main`, refresh the graph with:
   ```bash
   graphify update .
   ```

The automation loop normally handles the post-merge graph refresh. Keep generated Graphify artifacts inside `graphify-out/`.

## PR validation gates

Before opening, updating, or marking a PR as ready, run the repo-native validation gates from the repository root.

Fast local gate for ordinary code/CI edits:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/full_test.py --mode fast --base origin/main --keep-going
```

This must pass before a PR can be considered locally validated. It covers the cheap required checks: tests, 100% branch coverage, ruff, ruff format, strict mypy, test-integrity, dependency-review, workflow-security, and PR-size.

Full PR gate for merge-readiness validation. It runs the useful ordinary full
suite (including security, secret scan, repeatability, build/install, and
artifact provenance) but intentionally omits mutation:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/full_test.py --mode full --base origin/main --keep-going
```

AetherMesh is still early-phase, so mutation is optional and nonblocking until
a future official-maturity self-check enables it. Run mutation only as an
explicit diagnostic opt-in; never infer or report that mutation passed from a
normal full-gate result:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/full_test.py --mode full --include-mutation --base origin/main --keep-going
```

For a direct mutation-debugging loop, run:

```bash
python3 scripts/mutmut_early_fail.py --minimum 95 --max-children "$(sysctl -n hw.ncpu)"
python3 scripts/ci_quality_gates.py mutation-score --minimum 95
```

On Linux CI, use `$(nproc)` instead of `$(sysctl -n hw.ncpu)`. `mutants/` is mutmut's disposable local cache/state: keep it only during an active mutation-debugging loop, never commit it, and remove it before reporting a clean workspace or committing.

The parallel runner owns one new session/process group per built-in check. Its
fail-fast cleanup relies on built-in commands keeping all descendants in that
owned group; do not add a check that starts a detached session/process group.

Do not push a PR update after code changes unless the relevant local gate passes, or the blocker is explicitly reported. Before committing, run `git diff --check`, stage only intentional files, and keep generated/local artifacts out of the commit.
