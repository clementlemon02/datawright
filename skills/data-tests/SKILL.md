---
name: data-tests
description: Add data quality tests and transformation unit tests to a pipeline or table, using the framework the repo already has. Use when the user asks to "add tests", "add data quality checks", "make sure this doesn't break again", or after building or fixing a pipeline.
license: MIT
---

# Data tests

Add the few tests that would catch the failures that actually hurt, in the framework the repo already uses.

**Context first:** run `uv run .data-context/dctx.py card <dataset>` for each dataset involved (set it up with the `data-context` skill if it's missing). Before finishing, record anything non-obvious you learned with `dctx set` or `dctx note`.

## 1. Find the framework

Look for what exists: tests in the transformation framework, a data quality tool's config, SQL assertion files, or `pytest` with fixtures. Use it. If there's nothing, propose the lightest option that fits the stack, and ask before adding a dependency.

## 2. Pick the tests

Start with these, in order, and skip what doesn't apply:

1. **Grain**: the key is unique and not null.
2. **Freshness**: the latest partition or timestamp is within the expected lag.
3. **Volume**: today's row count is within a sane range of recent days (not zero, not 10×).
4. **Relationships**: foreign keys exist in the parent table (no orphans).
5. **Accepted values / ranges**: status enums, non-negative amounts, dates not in the future.
6. **Reconciliation**: a count or sum matches the source within a tolerance.

For non-trivial transformation logic (dedup, sessionization, currency or timezone handling), add a **unit test** with a few hand-written input rows and expected output rows.

If this follows a bug fix, the first test is the one that would have caught that bug.

## 3. Run them

Run the tests on dev and show the results. A test that fails on current data is a finding: report it rather than loosening the test until it passes.

## Output

The tests added (files), what each one protects against in one line, and the run results.
