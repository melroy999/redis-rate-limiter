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
- Parses each trampolined source file once via libcst to extract diffs for all mutants in that file, replacing the vanilla approach of spawning a separate `mutmut show` process per mutant (which took approximately 44 minutes for 3,200 mutants). Diffs are generated for every mutant (killed, survived, timeout, no tests), not just non-killed ones, so that the output files are self-contained and can be analyzed without access to mutmut's working directory.
- Classifies each surviving mutation by relevancy score (0 = cosmetic, 1 = argument removal, 2 = logic, 3 = fork-immune) using `classify_mutants.py`.
- Detects sync/async mirror pairs (identical mutations on `limiters.py` and `async_limiters.py`) and collapses them in the text report.
- Matches mutations against a manually verified known-benign allowlist.
- Computes per-file survival rates, mutation type distribution, test effectiveness metrics (kills per second), and uncovered function detection from `mutmut-stats.json`.
- Writes five output files: `report.json` (non-killed mutants only), `report-detail.json` (killed-by mappings, test effectiveness), `all-mutations.json` (every mutant with diffs, gitignored), `report.txt`, `mutation-score.txt`, and `stats.json`.

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

### Score extraction (`extract_mutation_score.py`)

Parses the final progress line from `mutmut run` output to extract the mutation score. Used as a library by the report generator and retained for standalone use.

## Invocation

Mutation testing scripts are invoked automatically by `docker compose --profile mutate up --build --abort-on-container-exit --exit-code-from mutate`. The `mutate` service in `docker-compose.yml` runs:

1. `python scripts/run_mutmut.py run` (patched mutmut with killed-by tracking)
2. `python scripts/generate_mutmut_report.py` (unified report generation)

All results are written to `./mutmut-results/` on the host via a bind mount. The `all-mutations.json` file contains every mutant (killed, survived, timeout, no tests) with diffs, classification, and killed-by data; it replaces the need for `mutmut show` commands and is gitignored due to its size. The `report.json` file contains only non-killed mutants and is checked into version control for tracking regressions.
