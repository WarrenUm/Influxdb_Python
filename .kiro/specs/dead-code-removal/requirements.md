# Requirements Document

## Introduction

This feature covers the removal of dead code and stale artifacts from the python_InfluxDB project. The active codebase resides in the `ge_pipeline` package (12 modules) exposed through the `ge-pipeline` Typer CLI and a scheduler daemon, with a test suite under `tests/` (unit, property-based Hypothesis, and integration tests). Over time the project has accumulated unreachable code, helpers referenced only by other dead code, speculative branches, and stale documentation and notebook artifacts.

The goal is to remove code and files that are provably unused or obsolete while preserving every currently-used behavior. Removal is verified by running the full pytest suite after each removal batch and confirming that all previously-passing tests remain green. This effort explicitly excludes aggressive pruning of lightly-used utilities that lie on active execution paths.

## Glossary

- **Removal_Process**: The overall workflow that identifies, removes, and verifies the elimination of dead code and stale artifacts from the project.
- **Static_Analyzer**: The tooling (for example vulture and ruff) plus zero-reference checks used to detect unreachable or unreferenced code across the `ge_pipeline` package, the `tests/` directory, and the CLI entry points.
- **Dead_Code**: A code element (function, method, class, branch, import, or variable) that is either proven unreachable by the Static_Analyzer with zero references across package, tests, and CLI, or referenced only by other Dead_Code, or a speculative branch that no active path can reach.
- **Active_Path**: Any code reachable from the `ge-pipeline` CLI commands (ingest, backfill, serve, export, setup), the scheduler daemon, or the passing test suite.
- **Lightly_Used_Utility**: A utility that has at least one reference from an Active_Path and is therefore out of scope for removal.
- **Stale_Artifact**: A designated non-code file that is obsolete: the notebooks `DatabaseSetup.ipynb`, `testFunctions.ipynb`, `testGettingandwritingtimestamps.ipynb`, the file `IMPROVEMENTS.md`, and `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`.
- **Test_Suite**: The full pytest suite executed with `testpaths=["tests"]` as configured in `pyproject.toml`.
- **Baseline_Green_Set**: The set of tests that pass before any removal begins.

## Requirements

### Requirement 1

**User Story:** As a maintainer, I want unreachable code identified by static analysis to be removed, so that the codebase contains only reachable code.

#### Acceptance Criteria

1. WHEN the Removal_Process runs the Static_Analyzer across the `ge_pipeline` package, the `tests/` directory, and the CLI entry points, THE Static_Analyzer SHALL report each code element that has zero references.
2. WHERE a code element is reported with zero references by the Static_Analyzer, THE Removal_Process SHALL classify the code element as Dead_Code.
3. WHEN a code element is classified as Dead_Code, THE Removal_Process SHALL remove the code element from the codebase.

### Requirement 2

**User Story:** As a maintainer, I want obsolete code that is only referenced by other dead code to be removed, so that no orphaned helpers remain after the primary dead code is deleted.

#### Acceptance Criteria

1. WHERE a helper is referenced only by code that is classified as Dead_Code, THE Removal_Process SHALL classify the helper as Dead_Code.
2. WHERE a branch is unreachable from any Active_Path, THE Removal_Process SHALL classify the branch as Dead_Code.
3. WHEN a helper or branch is classified as Dead_Code, THE Removal_Process SHALL remove the helper or branch from the codebase.

### Requirement 3

**User Story:** As a maintainer, I want lightly-used utilities on active paths to be preserved, so that removal does not degrade working functionality.

#### Acceptance Criteria

1. WHERE a utility has at least one reference from an Active_Path, THE Removal_Process SHALL classify the utility as a Lightly_Used_Utility.
2. WHERE a code element is classified as a Lightly_Used_Utility, THE Removal_Process SHALL retain the code element in the codebase.

### Requirement 4

**User Story:** As a maintainer, I want stale notebook and documentation artifacts deleted, so that the repository no longer carries obsolete files.

#### Acceptance Criteria

1. THE Removal_Process SHALL delete the file `DatabaseSetup.ipynb`.
2. THE Removal_Process SHALL delete the file `testFunctions.ipynb`.
3. THE Removal_Process SHALL delete the file `testGettingandwritingtimestamps.ipynb`.
4. THE Removal_Process SHALL delete the file `IMPROVEMENTS.md`.
5. THE Removal_Process SHALL delete the file `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`.

### Requirement 5

**User Story:** As a maintainer, I want the README updated to reflect removed artifacts, so that documentation stays consistent with the repository contents.

#### Acceptance Criteria

1. WHEN the Stale_Artifact notebooks are deleted, THE Removal_Process SHALL remove the Notebooks section from `README.md`.

### Requirement 6

**User Story:** As a maintainer, I want the full test suite to remain green throughout the removal, so that currently-used functionality is preserved.

#### Acceptance Criteria

1. WHEN the Removal_Process begins, THE Removal_Process SHALL record the Baseline_Green_Set by running the Test_Suite before any removal.
2. WHEN the Removal_Process completes a removal batch, THE Removal_Process SHALL run the full Test_Suite.
3. WHILE removal batches are in progress, THE Removal_Process SHALL keep every test in the Baseline_Green_Set passing.
4. IF any test in the Baseline_Green_Set fails after a removal batch, THEN THE Removal_Process SHALL revert the removal batch that caused the failure.
