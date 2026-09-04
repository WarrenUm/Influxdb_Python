# Dead Code Removal — Working Notes

## Task 1.1 — Verify a clean tree and record the Baseline_Green_Set

_Requirements: 6.1_

### Step 1: Clean-tree check (`git status --porcelain`)

**Result: TREE IS NOT CLEAN — task stopped before running the baseline pytest.**

Per task instructions, nothing was stashed or discarded. The baseline
pytest run (`python -m pytest`) was **not** executed because the working
tree must be clean first, so the Baseline_Green_Set has **not** been
recorded yet.

Output of `git status --porcelain` at time of check:

```
 M .ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb
 M DatabaseSetup.ipynb
 D InfluxAdmin.py
 M README.md
 D RunTimestampFetch.py
 D latestTime.txt
 D smallFcns.py
 M testFunctions.ipynb
 M testGettingandwritingtimestamps.ipynb
?? .env.example
?? .gitignore
?? .kiro/
?? .venv/
?? IMPROVEMENTS.md
?? ge_pipeline/
?? grafana/
?? pyproject.toml
?? requirements.txt
?? tests/
?? web/
```

Legend:
- ` M` = modified, not staged
- ` D` = deleted, not staged
- `??` = untracked

### Dirty files broken down

Modified (unstaged):
- `.ipynb_checkpoints/DatabaseSetup-checkpoint.ipynb`
- `DatabaseSetup.ipynb`
- `README.md`
- `testFunctions.ipynb`
- `testGettingandwritingtimestamps.ipynb`

Deleted (unstaged):
- `InfluxAdmin.py`
- `RunTimestampFetch.py`
- `latestTime.txt`
- `smallFcns.py`

Untracked:
- `.env.example`
- `.gitignore`
- `.kiro/`
- `.venv/`
- `IMPROVEMENTS.md`
- `ge_pipeline/`
- `grafana/`
- `pyproject.toml`
- `requirements.txt`
- `tests/`
- `web/`

### Baseline_Green_Set

**Not recorded.** Blocked on the dirty working tree. Once the tree is clean
(committed or otherwise resolved by the maintainer — do not stash/discard
automatically), re-run task 1.1 to run `python -m pytest` from the repo root
inside `.venv` and record passing / skipped / deselected node ids and summary
counts here.
