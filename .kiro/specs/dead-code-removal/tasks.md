# Implementation Plan: Dead Code Removal

## Overview

This plan drives a controlled, evidence-backed deletion process rather than feature code generation. It captures a green baseline, runs a detection pipeline (vulture + ruff + manual zero-reference grep), builds a classification table, then removes dead code and stale artifacts in small verify-and-revert batches. The full pytest suite is the oracle: after every batch the set of tests passing before removal must remain a subset of the tests passing after. Any batch that regresses the baseline is reverted with git.

Because the feature's changes are deletions, there is no runtime code under test for random-input property tests. Property enforcement is realized operationally: the two correctness properties (no baseline-passing test regresses; the tree is green after every processed batch) are discharged by re-running the full suite and comparing against the Baseline_Green_Set after each batch. Those checks are captured as the optional verification sub-tasks annotated with their property numbers.

## Tasks

- [x] 1. Capture baseline and set up the working notes
  - [x] 1.1 Verify a clean tree and record the Baseline_Green_Set
    - Run `git status --porcelain` and confirm output is empty (clean tree); stop and resolve if not
    - Run `python -m pytest` from the repo root inside `.venv` (respects `testpaths=["tests"]`)
    - Record the passing test node ids, the skipped node ids, the deselected node ids, and the summary counts (passed/skipped/failed/errors) into a working notes file (e.g. `.kiro/specs/dead-code-removal/removal-notes.md`)
    - This recorded set is the Baseline_Green_Set that every later batch is compared against; skips are recorded so they are not later mistaken for passes or regressions
    - _Requirements: 6.1_

  - [x] 1.2 Smoke check the baseline run
    - **Property 1: No baseline-passing test regresses across any batch (baseline anchor)**
    - **Validates: Requirements 6.1, 6.3**
    - Confirm the baseline pytest run at HEAD completed and the green set is non-empty and recorded before any removal begins

- [x] 2. Run the detection pipeline
  - [x] 2.1 Run static analyzers over the codebase
    - Run `vulture ge_pipeline tests --min-confidence 60` and capture output to the working notes (candidate set V)
    - Run `ruff check ge_pipeline tests --select F401,F811,F841,ARG` and capture output to the working notes (candidate set R)
    - Treat both outputs as candidates only, never verdicts
    - _Requirements: 1.1_

  - [x] 2.2 Confirm references with manual zero-reference grep
    - For each candidate symbol in (V | R), run `rg -n "\b<symbol>\b" ge_pipeline tests pyproject.toml`, excluding the definition site itself
    - Account for indirect references that analyzers miss: string-based entry points (`ge_pipeline.cli:app` in `pyproject.toml`, `uvicorn.run("ge_pipeline.api:app", ...)` in `serve`), the outlier detector registry (`register_detector("zscore"/"iqr", ...)` and `get_detector` string lookups), and the `@runtime_checkable` `OutlierDetector` protocol satisfied structurally
    - Record, per candidate, the reference count and any indirect (string/protocol) reference that keeps it live
    - _Requirements: 1.1, 3.1_

- [x] 3. Build the classification table
  - [x] 3.1 Classify every candidate into exactly one category
    - Produce a `ClassificationResult` table in the working notes with columns symbol, location (file:line), category, evidence, batch
    - Apply the decision flow: 0 references and not reached by string/protocol → `proven_dead`; referenced only by `proven_dead` symbols → `orphaned_by_dead`; branch unreachable from any Active_Path whose removal preserves behavior → `speculative_branch`; ≥1 Active_Path reference (including indirect) → `lightly_used_retain`
    - Explicitly retain lightly-used-on-active-path elements (e.g. `_parse_range`/`_resolve_window` called only from `export`, `compute_backoff_delay` used by `_resolve_delay`); being called from one place is not grounds for removal
    - Treat the trailing `raise AssertionError(...)` guards in `retry.py` conservatively as `lightly_used_retain` unless provably inert with identical behavior and typing after removal
    - Assign each `proven_dead`/`orphaned_by_dead`/`speculative_branch` symbol to a removal batch (A–D)
    - _Requirements: 1.2, 2.1, 2.2, 3.1, 3.2_

- [x] 4. Batch A — remove unused imports and trivial local dead code
  - [x] 4.1 Apply Batch A removals
    - Remove ruff F401 unused imports and F841 unused local variables identified in the classification table
    - Update any `__all__` entry in the same edit if an exported symbol is removed
    - _Requirements: 1.3_

  - [x] 4.2 Verify Batch A against the baseline
    - **Property 1: No baseline-passing test regresses across any batch**
    - **Property 2: The working tree is green after every processed batch**
    - **Validates: Requirements 6.2, 6.3, 6.4**
    - Run `python -m pytest`; if the Baseline_Green_Set is a subset of the passing set, keep the batch and re-run detection (vulture/ruff/grep) to surface newly-orphaned helpers; otherwise `git restore` the batch's files, record the regression and symbol, and reclassify or split

- [x] 5. Batch B — remove proven-dead private helpers per module
  - [x] 5.1 Apply Batch B removals one module (or small cluster) at a time
    - Delete `proven_dead` functions/methods/classes for the affected modules across `api.py`, `data_access.py`, `influx.py`, `ingestion.py`, `models.py`, `outliers.py`, `retry.py`, `scheduler.py`, `config.py`, `errors.py`, `cli.py`
    - Update `__all__` in the same batch as the symbol it exports so exports never dangle
    - _Requirements: 1.3_

  - [x] 5.2 Verify Batch B against the baseline
    - **Property 1: No baseline-passing test regresses across any batch**
    - **Property 2: The working tree is green after every processed batch**
    - **Validates: Requirements 6.2, 6.3, 6.4**
    - Run `python -m pytest`; keep on baseline-subset success and re-run detection to surface newly-orphaned helpers, else `git restore` the batch and record the regression

- [x] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Batch C — remove orphaned-by-dead helpers
  - [x] 7.1 Apply Batch C removals
    - Remove helpers surfaced by the post-Batch-B detection re-run whose only callers were `proven_dead` (now deleted), confirmed zero-reference by grep
    - Update `__all__` in the same batch if an exported symbol is removed
    - _Requirements: 2.1, 2.3_

  - [x] 7.2 Verify Batch C against the baseline
    - **Property 1: No baseline-passing test regresses across any batch**
    - **Property 2: The working tree is green after every processed batch**
    - **Validates: Requirements 6.2, 6.3, 6.4**
    - Run `python -m pytest`; keep on baseline-subset success and re-run detection, else `git restore` the batch and record the regression

- [x] 8. Batch D — remove genuinely speculative branches (conservative)
  - [x] 8.1 Apply Batch D removals
    - Remove only branches classified `speculative_branch` that are provably inert given the callers and whose removal leaves behavior and typing unchanged
    - Leave documented defensive guards (e.g. trailing `raise AssertionError(...)` in `retry.py`) in place; when in doubt, retain
    - _Requirements: 2.2, 2.3_

  - [x] 8.2 Verify Batch D against the baseline
    - **Property 1: No baseline-passing test regresses across any batch**
    - **Property 2: The working tree is green after every processed batch**
    - **Validates: Requirements 6.2, 6.3, 6.4**
    - Run `python -m pytest`; keep on baseline-subset success, else `git restore` the batch and record the regression or reclassify as `lightly_used_retain`

- [x] 9. Batch E — delete stale artifacts and update README
  - [x] 9.1 Delete the stale notebook and documentation artifacts
    - Delete `DatabaseSetup.ipynb`
    - Delete `testFunctions.ipynb`
    - Delete `testGettingandwritingtimestamps.ipynb`
    - Delete `IMPROVEMENTS.md`
    - Delete `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [x] 9.2 Remove the Notebooks section from README.md
    - Remove the `## Notebooks` heading and its notebook bullet lines from `README.md`
    - Leave the surrounding Usage and API Reference sections intact
    - _Requirements: 5.1_

  - [x] 9.3 Verify Batch E against the baseline
    - **Property 2: The working tree is green after every processed batch**
    - **Validates: Requirements 6.2, 6.4**
    - Run `python -m pytest` and confirm the Baseline_Green_Set still passes (the suite does not depend on these files since `testpaths=["tests"]`); revert with `git restore`/`git checkout` if it regresses

- [x] 10. Final verification
  - [x] 10.1 Confirm removals and green suite
    - For each removed symbol, run `rg -n "\b<symbol>\b" ge_pipeline tests pyproject.toml` and confirm zero hits (definition site gone)
    - Confirm the five named stale files are absent
    - Confirm `README.md` no longer contains the `## Notebooks` heading or the three notebook filenames
    - Run `python -m pytest` one final time and confirm the full suite is still green against the Baseline_Green_Set
    - _Requirements: 1.3, 2.3, 4.1, 4.2, 4.3, 4.4, 4.5, 5.1, 6.2, 6.3_

- [x] 11. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP; here they are the per-batch regression-gate verification steps that enforce the correctness properties.
- Each task references specific requirements for traceability.
- Checkpoints ensure incremental validation.
- Detection is re-run after each accepted batch (not once up front) so newly-orphaned helpers surface transitively.
- Revert uses `git restore`/`git checkout -- <paths>` so the tree returns exactly to the last green state; each accepted batch is committed/staged so revert never loses earlier work.
- Property enforcement is operational: with no runtime code under test, the two correctness properties are discharged by comparing each full-suite run against the recorded Baseline_Green_Set rather than by generated-input property tests.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["2.2"] },
    { "id": 3, "tasks": ["3.1"] },
    { "id": 4, "tasks": ["4.1"] },
    { "id": 5, "tasks": ["4.2", "5.1"] },
    { "id": 6, "tasks": ["5.2", "7.1"] },
    { "id": 7, "tasks": ["7.2", "8.1"] },
    { "id": 8, "tasks": ["8.2", "9.1", "9.2"] },
    { "id": 9, "tasks": ["9.3"] },
    { "id": 10, "tasks": ["10.1"] }
  ]
}
```
