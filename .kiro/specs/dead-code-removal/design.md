# Design Document

## Overview

This design describes how dead code and stale artifacts are found, removed, and verified in the `python_InfluxDB` project without regressing any currently-passing test. The active code lives in the `ge_pipeline` package (`api.py`, `cli.py`, `config.py`, `data_access.py`, `errors.py`, `influx.py`, `ingestion.py`, `models.py`, `outliers.py`, `retry.py`, `scheduler.py`) and is exercised through the `ge-pipeline` Typer CLI (`ingest`, `backfill`, `serve`, `export`, `setup`), the APScheduler daemon, the FastAPI service, and a `tests/` suite that mixes unit, Hypothesis property, and integration tests.

The removal is not a code-generation task; it is a controlled deletion process. The design therefore centers on three things:

1. A repeatable detection pipeline (vulture + ruff + manual zero-reference grep) that produces an evidence-backed candidate list.
2. A classification scheme that separates provably-dead code from lightly-used utilities that must be preserved.
3. A batched apply/verify/revert loop that keeps the full pytest suite green throughout, anchored to a baseline captured before any change.

The single safety invariant behind the whole effort: **the set of tests passing before removal must remain a subset of the tests passing after every batch.** If a batch violates it, the batch is reverted.

## Detection Pipeline

Detection runs against `ge_pipeline/` and `tests/` and cross-checks the CLI entry point declared in `pyproject.toml` (`ge-pipeline = "ge_pipeline.cli:app"`).

### Tools and their roles

| Tool | Detects | Command (run from repo root, inside `.venv`) |
|------|---------|----------------------------------------------|
| `vulture` | Unused functions, methods, classes, attributes, variables, unreachable code | `vulture ge_pipeline tests --min-confidence 60` |
| `ruff` | Unused imports (F401), unused local variables (F841), unused function arguments, redundant `__all__` entries | `ruff check ge_pipeline tests --select F401,F811,F841,ARG` |
| `grep` (ripgrep) | Zero-reference confirmation across package + tests + CLI | `rg -n "\b<symbol>\b" ge_pipeline tests` |

`vulture` and `ruff` are dev-only and are not added to `pyproject.toml` runtime or test dependencies; they are invoked ad hoc during the removal process. Both are run with output captured to a working notes file so the candidate list is reproducible.

### Why manual grep is required on top of the analyzers

Static analyzers over-report and under-report in ways that matter for this repo:

- **`__all__` re-exports and the CLI factory.** `cli.py` exposes `app` via `__all__` and it is referenced only as a string in `pyproject.toml` (`ge_pipeline.cli:app`) and in `serve` (`uvicorn.run("ge_pipeline.api:app", ...)`). Vulture cannot see string-based references, so `app` and `api:app` must be treated as live regardless of analyzer output.
- **Registry side effects.** `outliers.py` registers detectors at import time (`register_detector("zscore", ...)`, `register_detector("iqr", ...)`) and looks them up by string name in `get_detector`. `ZScoreDetector`/`IQRDetector` may look unused to vulture even though they are reachable via `detect_outliers` / `get_detector`.
- **Protocol implementations.** `OutlierDetector` is a `@runtime_checkable` `Protocol`; classes satisfy it structurally, so vulture may flag protocol methods as unused.
- **Test-only references.** A helper referenced only from `tests/` is *not* dead — it is validated behavior. Grep must include `tests/` before anything is classified dead.

So the rule is: an analyzer hit is a **candidate**, never a verdict. A candidate becomes `Dead_Code` only after a zero-reference grep across `ge_pipeline/`, `tests/`, and the `pyproject.toml` entry points confirms it, and after a manual check that it is not reached by string name or protocol structural typing.

### Detection procedure

```
run vulture over ge_pipeline + tests           -> candidate set V
run ruff (F401/F811/F841/ARG)                  -> candidate set R
for each symbol s in (V | R):
    hits = grep(s) across ge_pipeline, tests, pyproject.toml
    exclude hits that are the definition site itself
    if hits == 0 and s not reached by string-name/protocol:
        mark s as zero-reference candidate
    else:
        record why s is retained (which active reference)
```

## Classification Model

Every candidate is placed in exactly one category. The category determines the action.

```
ClassificationResult
├── symbol: str                     # qualified name, e.g. "retry.compute_backoff_delay"
├── location: str                   # file:line
├── category: Literal[
│       "proven_dead",              # Req 1: zero references anywhere
│       "orphaned_by_dead",         # Req 2.1: only referenced by proven_dead
│       "speculative_branch",       # Req 2.2: branch unreachable from any Active_Path
│       "lightly_used_retain",      # Req 3: >=1 Active_Path reference -> keep
│   ]
├── evidence: str                   # grep summary / reachability note
└── batch: int | None               # removal batch number, or None if retained
```

### Category definitions and this-repo examples

- **`proven_dead`** — zero references across package, tests, and CLI entry points. Removed under Requirement 1. Example shape: a private helper in a module that no other module, test, or CLI command calls.

- **`orphaned_by_dead`** — a helper whose only callers are themselves `proven_dead`. This is transitive: after a `proven_dead` symbol is deleted, re-running detection may surface a helper that is now zero-reference. Removed under Requirement 2.1. This is why detection is re-run after each batch rather than computed once up front.

- **`speculative_branch`** — a branch no `Active_Path` can reach. In this codebase the honest reading is important: the `raise AssertionError("retry loop exited without returning or raising")` lines in `retry.py` are **defensive guards after loops that always return or raise**, and the comment explicitly documents them as unreachable. These are *not* removed blindly — removing a function's trailing statement changes control-flow typing and can break `mypy`/coverage expectations, and they document intent. A branch is only classified `speculative_branch` when it is genuinely inert (e.g. a condition that can never be true given the callers) and its removal leaves behavior identical. When in doubt, it is treated as `lightly_used_retain`.

- **`lightly_used_retain`** — at least one reference from an `Active_Path` (a CLI command, the scheduler, the API, ingestion, or a passing test). Retained under Requirement 3. Example: `_parse_range` / `_resolve_window` in `cli.py` are only called from `export`, and `compute_backoff_delay` is used by `_resolve_delay` inside `retry_with_backoff`; these are lightly used but on active paths, so they stay. Being called from only one place is **not** grounds for removal.

### Classification decision flow

```
grep reference count for symbol s (excluding its own definition):
    0 references
        └── reached only by string name or protocol structural typing?
              yes -> lightly_used_retain (record the indirect reference)
              no  -> proven_dead
    >=1 reference, but every referencing symbol is itself proven_dead
        └── orphaned_by_dead
    >=1 reference from an Active_Path
        └── lightly_used_retain
branch with no reachable Active_Path AND removal preserves behavior
        └── speculative_branch
```

## Removal Workflow (Batched, Verify-and-Revert)

The workflow is a loop over small batches. Batching keeps each verification cheap to attribute: if the suite goes red, the offending change is confined to one batch and is reverted wholesale.

### Baseline capture (Requirement 6.1)

Before any change, the working tree must be clean and the suite green:

```
git status --porcelain          # expect empty (clean tree)
python -m pytest                 # full suite; testpaths=["tests"] per pyproject.toml
```

The passing set from this run is the **Baseline_Green_Set**. It is recorded (test node ids + summary counts) as the reference every later batch is compared against. Integration tests that rely on `testcontainers`/Docker may be skipped in some environments; the baseline records *which* tests passed, skipped, or were deselected so "green" is defined precisely and a later skip is not mistaken for a pass or a regression.

### Batch structure

Batches are grouped so that a failure points at a coherent unit:

- **Batch A — unused imports and trivial local dead code** (ruff F401/F841 hits). Lowest risk, done first.
- **Batch B — proven-dead private helpers**, one module or a small cluster at a time (e.g. one batch per affected module across `api.py`, `data_access.py`, `influx.py`, `ingestion.py`, `models.py`, `outliers.py`, `retry.py`, `scheduler.py`, `config.py`, `errors.py`, `cli.py`).
- **Batch C — orphaned-by-dead helpers** surfaced by re-running detection after Batch B.
- **Batch D — genuinely speculative branches**, if any survive the conservative test above.
- **Batch E — stale artifacts + README** (Requirements 4 and 5).

`__all__` lists are updated in the same batch as the symbol they export so exports never dangle.

### Per-batch loop (Requirements 6.2, 6.3, 6.4)

```
for each batch B in [A, B, C, D, E]:
    apply removals for B                       # edit files / delete files
    update __all__ if an exported symbol was removed
    run: python -m pytest                      # full suite

    if every test in Baseline_Green_Set passes:
        keep B                                 # commit or stage the batch
        re-run detection (vulture/ruff/grep)   # surface newly-orphaned helpers
    else:
        revert B                               # git checkout/restore the batch's files
        record the regression and the symbol involved
        # tree is green again; either split B smaller or reclassify the symbol
        # as lightly_used_retain and move on
```

Revert uses `git restore`/`git checkout -- <paths>` (or `git stash` of the batch) rather than editing back by hand, so the tree returns exactly to the last green state. Because each accepted batch is committed/staged, revert never loses an earlier accepted batch.

### Stale artifact deletion and README update (Requirements 4, 5)

Handled as Batch E, verified the same way (the suite must not depend on these files, which it does not, since `testpaths=["tests"]`):

- Delete `DatabaseSetup.ipynb`, `testFunctions.ipynb`, `testGettingandwritingtimestamps.ipynb`, `IMPROVEMENTS.md`, and `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`.
- Edit `README.md` to remove the `## Notebooks` section (the heading and the three notebook bullet lines). The surrounding "Usage" and "API Reference" sections are left intact.

## Components and Interfaces

The process itself is lightweight; it does not add runtime code to `ge_pipeline`. The "components" are the process artifacts and the commands that produce them.

| Component | Responsibility | Produces / Consumes |
|-----------|----------------|---------------------|
| Detection runner | Invoke vulture + ruff, capture output | Candidate list (raw analyzer output) |
| Reference checker | Grep each candidate across package, tests, `pyproject.toml` | Reference counts + indirect-reference notes |
| Classifier | Assign each candidate a category | `ClassificationResult` table |
| Baseline recorder | Run full suite once at HEAD | Baseline_Green_Set (node ids + counts) |
| Batch executor | Apply a batch, run suite, keep or revert | Green tree after every processed batch |
| Artifact remover | Delete stale files, edit README | Updated repo tree |

Structured pseudocode for the batch executor (the only piece with real control-flow semantics):

```
def process_batch(batch, baseline_green_set):
    apply(batch)                      # deletions/edits for this batch
    result = run_full_suite()         # python -m pytest
    if baseline_green_set.issubset(result.passing):
        commit(batch)
        return "kept"
    else:
        revert(batch)                 # git restore -> back to prior green tree
        record_regression(batch, result)
        return "reverted"
```

## Data Models

The only durable data is the classification table and the baseline record, kept as process notes (not shipped code):

```
Baseline_Green_Set:
    passing:   set[str]     # pytest node ids that passed at HEAD
    skipped:   set[str]     # recorded so skips aren't mistaken for pass/regress
    deselected:set[str]
    summary:   {passed, skipped, failed, errors}

ClassificationResult:      # one row per candidate (see Classification Model)
    symbol, location, category, evidence, batch
```

## Error Handling

- **Analyzer false positive (symbol reached by string/protocol):** caught by the mandatory grep + indirect-reference check before classification; such symbols are reclassified `lightly_used_retain` and never entered into a removal batch.
- **Batch turns the suite red:** the batch is reverted with git so the tree returns to the last green state; the involved symbol is reclassified or the batch is split smaller and retried. This is the direct realization of Requirement 6.4.
- **Environment-dependent tests (Docker/testcontainers integration):** recorded in the baseline as their observed state (pass/skip). A test that was skipped in the baseline is not required to pass later; a test that passed in the baseline must keep passing. This prevents both false alarms and missed regressions.
- **Dangling export after removal:** removing a symbol without updating `__all__` would surface as an `AttributeError`/import failure in the suite; `__all__` is edited in the same batch, and the full-suite run is the backstop.
- **Removing a documented defensive guard:** treated conservatively — a trailing `raise AssertionError(...)` guard in `retry.py` is retained unless it is provably inert and its removal leaves behavior and typing unchanged.

## Testing Strategy

The suite is not modified to add coverage for the removal; instead the **existing full pytest suite is the oracle**. The core testable guarantee is behavioral and universal over batches: no baseline-passing test may regress, and the tree is green after every processed batch. Everything else (which files were deleted, which analyzer hits were found, the README edit) is verified by concrete example checks and inspection.

- **Property enforcement:** realized operationally by re-running the full suite after every batch and comparing against the Baseline_Green_Set, with revert-on-regression. There is no random input generator here; the "for any batch" quantifier is discharged by applying the check to every batch in the sequence.
- **Example checks:** post-removal grep for each removed symbol returns zero hits; each of the five named stale files is absent; `README.md` no longer contains the `## Notebooks` heading or the three notebook filenames.
- **Smoke check:** a single baseline pytest run at HEAD establishes the green set.

Because the feature's changes are deletions, property tests would have no code under test to generate inputs for — the appropriate instrument is the existing suite run as a regression gate, plus example/inspection checks for the file and doc edits.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: No baseline-passing test regresses across any batch

*For any* removal batch applied to a green working tree, the set of tests passing after running the full suite SHALL be a superset of the Baseline_Green_Set (every test that passed before removal still passes).

**Validates: Requirements 3.2, 6.3**

### Property 2: The working tree is green after every processed batch

*For any* removal batch, after it is processed the working tree SHALL be green — either the batch was kept because the full suite passed the baseline, or it was reverted to restore the immediately-preceding green tree.

**Validates: Requirements 6.4**

*The following acceptance criteria were analyzed and classified as EXAMPLE, EDGE_CASE, INTEGRATION, or SMOKE rather than universally-quantified properties, and are verified as noted in the Testing Strategy:*

- *1.1 — INTEGRATION:* run vulture + ruff over `ge_pipeline`/`tests`, inspect the zero-reference report artifact.
- *1.2, 1.3, 2.1, 2.2, 2.3, 3.1 — EXAMPLE:* documented classification table backed by grep evidence; post-removal grep returns zero hits and the suite stays green.
- *4.1–4.5 — EXAMPLE:* assert each of the five named files is absent after deletion.
- *5.1 — EXAMPLE:* `README.md` contains no `## Notebooks` heading or notebook filenames after the edit.
- *6.1 — SMOKE:* a single baseline pytest run at HEAD records the Baseline_Green_Set.
- *6.2 — EXAMPLE:* each batch's workflow includes a full pytest invocation.
