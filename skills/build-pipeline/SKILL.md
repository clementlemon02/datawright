---
name: build-pipeline
description: Build a new data pipeline, model or job end to end - requirements, source profiling, design (grain, keys, incremental strategy), implementation in the repo's own framework, tests, dev validation and a backfill plan. Use when the user asks to "build a pipeline", "add a model/table", "ingest X into Y", or "create a job that...".
license: MIT
---

# Build a pipeline

Build it the way this repo already builds pipelines, and prove it on dev before calling it done. Never run anything against production.

**Context first:** run `uv run .data-context/dctx.py card <dataset>` for each dataset involved (set it up with the `data-context` skill if it's missing). Before finishing, record anything non-obvious you learned with `dctx set` or `dctx note`.

## 1. Pin the requirements

Get these from the user's request; ask only for what's missing:
- What the output is for, and who reads it
- **Grain**: what one row means
- Freshness needed (hourly, daily, near real-time)
- How much history, and whether a backfill is needed

## 2. Profile the sources

With read-only queries on a recent partition or sample:
- Row counts per day, and whether volume is stable
- Is the intended key actually unique? Null rate on keys and join columns
- Value ranges, and odd values (negatives, far-future dates, test rows)
- Late or out-of-order data: how far behind event time does data land?
- How the source changes: append-only, updates in place, or deletes

Report surprises before designing around them.

## 3. Design

- Find the closest existing pipeline in the repo and follow its pattern: layout, naming, framework, config, tests.
- If the vendor's skills for the framework are installed (e.g. dbt's), follow them for how models, tests and configs are written.
- Choose the load strategy: full refresh, append, merge / upsert, or partition overwrite. Pick the simplest that stays correct when the job is rerun (see `pipeline-review`).
- Decide dedup, late-data handling, and what happens on a source schema change.

Show the design in a few lines and get a yes before writing code.

## 4. Implement, test, validate

1. Write the code in the repo's framework and conventions.
2. Add tests with the `data-tests` skill.
3. Run it on dev for one partition or a small range, on the real engine. Verify once there; don't re-validate in other SQL dialects or engines. Reconcile with the source: row counts and a key metric (sum or count distinct) must match, or the difference must be explained.
4. Review with the `pipeline-review` and `sql-review` skills.

## 5. Backfill plan

If history is needed: date ranges in chunks, concurrency limit, an estimate of cost and runtime from the dev run, how to check each chunk, and how to roll back.

Run `dctx build` so the new output is in the index, then record its grain, key and meaning with `dctx set`, so the next task starts with it.

## Output

What was built (files), the evidence it works (the dev-run reconciliation), and the backfill plan. Deploying and running on production is the user's call.
