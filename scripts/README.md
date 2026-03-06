# Mutation Testing Scripts

This directory contains scripts that wrap and extend [mutmut 3.x](https://github.com/boxed/mutmut) for this project. The scripts are invoked by the `mutate` service in `docker-compose.yml` and are not intended for direct use on the host (mutmut requires `fork()`, which is unavailable on Windows).

## Changes Compared to Vanilla mutmut

### Runtime patches (`run_mutmut.py`)

Vanilla mutmut is patched at import time before the CLI runs. Five patches are applied:

1. **Selective decorator skip** (Patch 1): vanilla mutmut unconditionally skips all decorated functions and methods ([issue #387](https://github.com/boxed/mutmut/issues/387)). The patch replaces the blanket skip with an allowlist of compatible decorators (`@classmethod`, `@staticmethod`, `@abstractmethod`, `@override`, `@shared_task`, `@rate_limited`, `@functools.wraps`, `@functools.cache`, `@functools.lru_cache`, `@functools.cached_property`). Unknown decorators are still skipped to avoid side effects (e.g., `@app.post("/foo")` would register a route multiple times). Decorator argument nodes are also skipped to prevent mutations like `@shared_task(name="XXnameXX")`.

2. **Decorator stripping** (Patch 2): the `_orig` and `_mutmut_N` function copies created by the trampoline mechanism have their decorators removed. Without this, descriptors like `@classmethod` and `@staticmethod` would be applied twice (once on the trampoline wrapper, once on the copy), causing incorrect method binding.

3. **Descriptor-aware trampoline wrapper** (Patch 3): vanilla mutmut generates a single trampoline template that assumes regular instance methods. The patch generates correct wrapper bodies for `@classmethod` (uses `cls` and `type.__getattribute__`) and `@staticmethod` (uses `ClassName.attr` lookups, no `self_arg`).

4. **`orig_is_unbound` parameter** (Patch 4): the `_mutmut_trampoline` runtime function gains an `orig_is_unbound` keyword argument. When `True` (set for classmethods), the trampoline prepends `cls` to `orig()` calls, because the decorator-stripped copies are plain functions that no longer receive `cls` automatically.

5. **Killed-by test tracking** (Patch 5): vanilla mutmut records only the exit code per mutant (killed, survived, timeout, etc.). The patch injects a `KilledByCollector` pytest plugin that captures the nodeids of failing tests and a `tests_run` counter. Data flows from forked children to the parent process via temp-file IPC (`/tmp/mutmut_killed_by/`), is accumulated in memory, and flushed to `/tmp/mutmut_killed_by_results.json` at exit via an `atexit` handler. By default, pytest runs with `-x` (first-killer mode) for speed; this is configurable via the `_USE_FAIL_FAST` flag.

### Report generation (`generate_mutmut_report.py`)

Vanilla mutmut provides `mutmut results` (a list of surviving mutant names) and `mutmut show <name>` (individual diffs). There is no built-in report aggregation, classification, or JSON export. This script replaces all vanilla post-processing with a single-pass pipeline:

- Reads mutmut's `.meta` files directly (via `SourceFileMutationData`) to load status and duration for every mutant.
- Parses each trampolined source file once via libcst to extract diffs for all non-killed mutants in that file, replacing the vanilla approach of spawning a separate `mutmut show` process per mutant (which took approximately 44 minutes for 3,200 mutants).
- Classifies each surviving mutation by relevancy score (0 = cosmetic, 1 = argument removal, 2 = logic, 3 = fork-immune) using `classify_mutants.py`.
- Detects sync/async mirror pairs (identical mutations on `limiters.py` and `async_limiters.py`) and collapses them in the text report.
- Matches mutations against a manually verified known-benign allowlist.
- Computes per-file survival rates, mutation type distribution, test effectiveness metrics (kills per second), and uncovered function detection from `mutmut-stats.json`.
- Writes four output files: `report.json`, `report.txt`, `mutation-score.txt`, and `stats.json`.

### Classification (`classify_mutants.py`)

Provides the classification engine used by the report generator. Can also be run standalone against pre-generated `diffs.txt` and `results.txt` files.

#### Relevancy scores

Each surviving mutation is assigned a score from 0 (least actionable) to 3 (not actionable at all):

- **Score 0 (cosmetic)**: mutations that change non-behavioral text. Includes string mutations (XX wrap, lowercase, uppercase) inside logger calls, error messages, `NotImplementedError` text, and ASGI response body byte strings. Also includes `exc_info=` boolean swaps and value-to-None replacements on logger format arguments. These survivors do not indicate missing test coverage.
- **Score 1 (argument removal)**: mutations that remove a function or method call argument (net line reduction in the diff). These indicate that the test suite does not verify that a specific argument is passed, but the impact is typically limited to a single call site.
- **Score 2 (logic)**: everything not covered by another category. Includes operator swaps (`<` to `<=`, `+` to `-`, `and` to `or`, etc.), numeric increments (`0` to `1`), index changes, value-to-None replacements outside logger context, boolean swaps (`True` to `False`), keyword swaps (`break` to `return`), unary operator removal (`not x` to `x`), augmented-to-simple assignment (`+=` to `=`), and string method swaps (`.lower()` to `.upper()`). These are the most actionable survivors and typically indicate a genuine gap in test coverage.
- **Score 3 (fork-immune)**: mutations on default parameter values in `def` signatures. These are false survivors that cannot be killed by any test. Python stores defaults in the function object's `__defaults__` tuple at import time, but mutmut's AST mutations only modify the code object inside forked children. The parent process's `__defaults__` is inherited unchanged, so the test suite always sees the original default regardless of the mutation.

#### Detection pipeline

The classifier applies detectors in a fixed priority order, returning at the first match:

1. **Default parameter mutation**: checks whether the diff modifies a value inside a `def` signature's parameter list (score 3).
2. **String mutation**: detects XX wrap (`"foo"` to `"XXfooXX"`), lowercase (`"Foo"` to `"foo"`), and uppercase (`"foo"` to `"FOO"`). Context-aware: if the mutation is inside a `logger.<level>()` call, a `raise` statement, or an ASGI `http.response.body` send, it scores 0; otherwise it scores 2.
3. **Argument removal**: detects net line removal that looks like a function argument was deleted (score 1).
4. **Operator swap**: detects substitution between comparison, arithmetic, bitwise, logical, and augmented assignment operators (score 2).
5. **Boolean swap**: detects `True` to `False` and vice versa. If the swap is on an `exc_info=` keyword argument, it scores 0 (cosmetic); otherwise score 2.
6. **Keyword swap**: detects `break` to `return` and `continue` to `break` (score 2).
7. **Unary removal**: detects removal of `not` or `~` operators (score 2).
8. **Augmented-to-simple assignment**: detects `+=` to `=`, `-=` to `=`, etc. (score 2).
9. **String method swap**: detects symmetric method substitutions like `.lower()` to `.upper()`, `.lstrip()` to `.rstrip()`, `.split()` to `.rsplit()`, etc. (score 2).
10. **Value-to-None**: detects replacement of an expression with `None`. Context-aware: if the replaced value is a logger format argument, it scores 0; otherwise score 2.
11. **Numeric increment**: detects `N` to `N+1` on numeric literals (score 2).
12. **Fallback**: if no detector matches, the raw diff lines are shown with score 2.

#### Additional features

- **Sync/async mirror detection**: mutations that appear identically in both the sync and async implementation (e.g., `limiters.py` and `async_limiters.py`) are grouped together. The mirror key is derived by stripping module prefixes and `Abstract`/`Async` class name prefixes, then matching on score and mutation type.
- **Known-benign allowlist**: a list of `(method_pattern, description, reason)` tuples for mutations that have been manually verified as producing identical behavior (e.g., boundary condition equivalences where both branches compute the same value). These are separated from scored mutations and listed in their own report section.

### Test superfluity analysis (`analyze_test_superfluity.py`)

Identifies tests whose removal would not reduce path coverage or mutation coverage, i.e., tests that are strict subsets of other test collections. The analysis is a graduated-confidence funnel: function-level analysis identifies candidates, line-level analysis confirms or refutes most of them, and full certainty would require branch coverage and a complete kill matrix (no `-x`). No single tier produces a definitive verdict on its own.

#### Classification and confidence

The analysis classifies tests into categories based on available evidence. Tiers are classification labels, not an ordered confidence scale; confidence comes from combining evidence layers (function mapping, killed-by data, line coverage), not from the tier number.

| Category | Required data | Meaning | Definitive? |
|---|---|---|---|
| **Not superfluous** (tier 0) | Killed-by data | Test killed at least one mutant under `-x` ordering. Demonstrably catches mutations. | Yes (proves non-superfluity) |
| **Not superfluous** (tier 1) | Function mapping | Test exercises code outside the mutated source tree (algorithms, Lua, properties). Categorically not superfluous. | Yes (proves non-superfluity) |
| **Candidate** (tier 2) | Function mapping | Zero-kill test whose set of exercised functions is a strict subset of some killing test's function set. Plausible candidate, but two tests may exercise the same function and cover different branches within it. | No |
| **Candidate + line-superfluous** (tier 2 + line data) | Function mapping + per-test line coverage | Tier 2 candidate where every source line it covers is also covered by at least one other test. Strongest available signal, but does not account for branch-level differences within a single line or `-x` kill credit bias. | No (near-definitive) |
| **Unknown** (tier 3) | Function mapping | Zero-kill test whose function set is not a subset of any single killing test. Could be superfluous or not; insufficient data to classify in either direction. Not necessarily lower or higher confidence than tier 2. | No |

#### Function-level analysis (always available)

Uses `tests_by_mangled_function_name` from `stats.json` to build a per-test function coverage matrix. This is the coarsest analysis: it can definitively prove a test is *not* superfluous (tiers 0 and 1), but can only nominate candidates (tier 2) or mark tests as unknown (tier 3). It cannot confirm superfluity because function-level granularity does not distinguish which branches within a function each test covers.

#### Line-level analysis (when `coverage-contexts.json` is present)

Uses per-test line coverage from `coverage.py --show-contexts` to perform precise set-cover analysis. The algorithm is O(n * avg_coverage_size): for each source line, count how many tests cover it; a test is line-superfluous if every line it covers has a coverage count of at least 2 (i.e., at least one other test also covers it). Tests that are both tier 2 function-subset candidates and line-superfluous are the highest-confidence superfluous tests.

Even at this level, two residual uncertainties remain:

1. **Branch coverage gap**: two tests can cover the same line but take different branches on that line (e.g., an `if/else` on a single line). Only `branch = True` in coverage.py would close this gap (future enhancement).
2. **`-x` kill credit bias**: a "zero-kill" test may be the only test capable of killing certain mutants, but it never ran first due to test ordering. A complete kill matrix (running without `-x`) would eliminate this uncertainty, at the cost of significantly longer mutation testing runs.

#### Additional warnings

- **Contract tests** (`tests/contracts/`): even if coverage-redundant, contract tests enforce interface guarantees that all backends must satisfy. The analysis flags them but does not recommend removal.
- **Out-of-scope tests**: algorithm tests, Lua tests, and property tests exercise code outside the mutation scope. They are not superfluous by definition and are excluded from superfluity candidates.

#### Invocation

```bash
# Function-level analysis only (uses existing mutmut results):
python scripts/analyze_test_superfluity.py mutmut-results/

# Collect per-test coverage first, then run full analysis:
docker compose --profile coverage-context up \
    --abort-on-container-exit --exit-code-from coverage-context
python scripts/analyze_test_superfluity.py mutmut-results/
# Auto-detects coverage-contexts.json and upgrades to line-level analysis.

# Explicit coverage file path:
python scripts/analyze_test_superfluity.py mutmut-results/ \
    --coverage mutmut-results/coverage-contexts.json
```

Output files are written to the input directory: `superfluity-report.json` (structured summary with candidate list) and `superfluity-report.txt` (human-readable summary, also printed to stdout).

### Per-test coverage collection (`collect_per_test_coverage.py`)

Wrapper script that runs pytest with `--cov --cov-context=test` and exports the coverage database to `coverage-contexts.json`. This file records which test executed each source line, enabling the line-level superfluity analysis described above. Can be run via the `coverage-context` Docker Compose service or directly on the host if Redis is available.

### Score extraction (`extract_mutation_score.py`)

Parses the final progress line from `mutmut run` output to extract the mutation score. Used as a library by the report generator and retained for standalone use.

## Invocation

Mutation testing scripts are invoked automatically by `docker compose --profile mutate up --build --abort-on-container-exit --exit-code-from mutate`. The `mutate` service in `docker-compose.yml` runs:

1. `python scripts/run_mutmut.py run` (patched mutmut with killed-by tracking)
2. `python scripts/generate_mutmut_report.py` (unified report generation)

Per-test coverage collection is a separate step via `docker compose --profile coverage-context up --abort-on-container-exit --exit-code-from coverage-context`. Superfluity analysis (`analyze_test_superfluity.py`) runs on the host against the output directory and does not require Docker.

All results are written to `./mutmut-results/` on the host via a bind mount.
