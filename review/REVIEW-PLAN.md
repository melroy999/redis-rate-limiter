# Full-Context Code Review Plan

The existing REVIEW.md covered ~5-6 core source files (~15-20% of the codebase).
This plan covers everything. Each phase targets something an LLM session typically
cannot do because it requires holding too many files simultaneously or comparing
across distant parts of the codebase.

Each phase should preesnt its findings in a file at .claude\review in this project.

---

## Phase 1: Inventory

Map every file, its role, and line count. Need the full picture before reviewing
anything.

## Phase 2: Lua Scripts

Review all 5 scripts (`consume.lua`, `health.lua`, `acquire.lua`, `renew.lua`,
`schedule.lua`) for correctness, atomicity, and consistency with each other.
They share Redis keys — need all 5 in context together.

## Phase 3: Core Source (skipped files)

Review `base.py`, `scripts.py`, `decorators.py`, `importing.py`. The old review
ignored these entirely.

## Phase 4: Sync/Async Mirror Audit

Line-by-line structural diff of `limiters.py` (~1550 lines) vs
`async_limiters.py` (~1050 lines). Too big for one context window normally.
Looking for real divergences, not just async/await differences.

## Phase 5: Managed Mixin

Both sync and async versions of `managed.py`. Lifecycle correctness, config
persistence, refresh logic. The old review only flagged `_persist_config`
atomicity.

## Phase 6: All 4 Backends

Celery, asyncio, ASGI, threading — each is a separate integration. Check they
all honor the same contract and use the core correctly.

## Phase 7: Prometheus Integration

`integrations/prometheus.py` — metrics correctness, label hygiene, cardinality
risks. Completely untouched by the old review.

## Phase 8: Test Coverage Audit

What's tested, what's NOT. Contract tests vs implementation tests — are the
contracts actually enforced? Are there gaps? Dozens of test files to read.

## Phase 9: Config and Packaging

`pyproject.toml`, `ruff.toml`, `mypy.ini`, CI workflows (`.github/workflows/`).
Misconfigurations hide here and nobody looks.

## Phase 10: Docs vs Reality

Do the architecture docs (`docs/architecture/`) match the actual code? Stale
docs are worse than no docs.

## Phase 11: Examples and Demo

Do the examples (`examples/`) and demo (`demo/`) actually work with the current
API? Stale examples are a classic blind spot.

## Phase 12: Cross-Cutting Concerns

Error handling consistency, logging patterns, naming conventions, public API
surface (`__init__.py` exports). Only visible when you've read everything.

## Phase 13: Write REVIEW.md

Compile all findings from phases 1-12 into a single, comprehensive review
document replacing the current REVIEW.md. All phase findings are present in .claude\review

---

## Execution Notes

- Phases 2+3 can run in parallel (independent file sets).
- Phase 6 can run all 4 backends in parallel.
- Phase 4 requires both limiter files read sequentially for comparison.
- Phase 12 depends on all prior phases completing.
- Phase 13 depends on everything.
