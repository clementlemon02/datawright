---
name: pipeline-review
description: Review data pipeline code (any orchestrator or framework, batch or streaming) for the bugs that cause duplicate data, bad backfills, silent data loss and stuck runs. Use when the user changes or adds a pipeline, DAG, job or flow, asks to "review this pipeline", or a run is failing or producing wrong or duplicate data.
license: MIT
---

# Pipeline review

Review the changed pipeline code (or the files the user names). Read each file fully first, and identify the orchestrator and engine from the code. Report only real problems, each with `file:line`, what goes wrong, and the fix.

**Context first:** run `uv run .data-context/datawright.py card <dataset>` for each dataset involved (set it up with the `data-context` skill if it's missing). Before finishing, record anything non-obvious you learned with `datawright set` or `datawright note`.

## Checklist

Start with `uv run .data-context/datawright.py review` (or `lint`): its best-practice findings cover rerun safety, several writers, `SELECT *` writes, schedule order, layer order and committed secrets from the code as a whole. Use the list below for what needs judgment.

**Reruns and backfills**
- Each step is idempotent: rerunning the same time range replaces its output (overwrite a partition, `MERGE` / upsert, delete-then-insert in one transaction) instead of appending.
- Steps use the run's scheduled time / data interval, never the current wall-clock time.
- Start dates are fixed values, not computed from "now".
- Backfill / catch-up behavior is set explicitly and matches intent.

**Failure handling**
- Retries with a delay, and timeouts on long or external steps.
- Waiting steps (sensors, polling loops) have a timeout and don't hold a worker slot while idle.
- Concurrency limits where a backfill could overload a source, API or warehouse.
- A partial failure can't leave half-written output that downstream steps read as complete.

**Streaming (if any)**
- Checkpointing / offsets are committed only after output is written (no silent loss on restart).
- Late and out-of-order events are handled by explicit watermarks or windows.
- Sinks are idempotent or transactional, so a replay doesn't duplicate.

**Data passing and secrets**
- Steps pass small values (paths, ids, counts) through the orchestrator, not datasets.
- Credentials come from the orchestrator's connection store or a secrets manager, never hard-coded or committed.

**Orchestrator hygiene**
- Pipeline definition files do no heavy work at import / parse time (DB queries, API calls), since many schedulers re-parse them on a loop.
- The repo has (or this change adds) a test that loads every pipeline definition and fails on import errors and cycles.

## If an orchestrator MCP server is connected

Check the live picture before reporting: recent failed runs, logs for the failing step, import / parse errors. Read only; never trigger, clear, retry or pause runs.

## Output

A short table: severity (bug / risk / nit), `file:line`, problem, fix. Then offer to apply the fixes. If nothing is wrong, say so in one line.
