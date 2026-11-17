---
name: optimize-pipeline
description: Make a slow or expensive pipeline, job or query faster or cheaper, measured before and after, with proof the output didn't change. Use when the user says a job is slow, missing its SLA, costs too much, or asks to "optimize" or "speed up" a pipeline or query.
license: MIT
---

# Optimize a pipeline

Measure first, change one thing at a time, and prove the output is identical.

**Context first:** run `uv run .data-context/datawright.py card <dataset>` for each dataset involved (set it up with the `data-context` skill if it's missing). Before finishing, record anything non-obvious you learned with `datawright set` or `datawright note`.

Start with `uv run .data-context/datawright.py advise` (or `advise <table>`): partition, clustering and distribution keys from how the repo's code filters and joins each table, against its declared layout. Confirm with the warehouse's own stats before changing a big table.

## 1. Measure

- **Runtime**: recent run durations per step, from the orchestrator MCP or logs. Is it slow every time, or getting slower?
- **Cost / work**: data scanned, compute time, shuffle and spill, from the warehouse's query history or profile, or the engine's job metrics.
- **The hotspot**: the one step or query that dominates. Optimize only that.

## 2. Find the fix

Check in this order; the earlier ones usually win:

1. **Do less work**: incremental instead of full refresh; process only new partitions; drop unused columns and outputs.
2. **Scan less**: partition and cluster pruning (filters on the partition column, no functions wrapped around it), selecting only needed columns.
3. **Shuffle less**: filter and pre-aggregate before joins, broadcast small tables, handle skewed keys.
4. **Write less**: avoid rewriting whole tables, fix small-file problems, compact.
5. **Schedule smarter**: trigger on upstream completion instead of fixed times; run less often if the freshness needed allows it.
6. **Right-size compute**: only after the above. Bigger compute for the same bad plan is just a bigger bill.

Use the engine's own tuning features where they matter. Look them up for the engine rather than assuming.

## 3. Prove it

On dev, for the same input partition:
- **Same output**: row count and a checksum or key aggregates match between old and new.
- **Measured gain**: runtime and data scanned or cost, before vs. after.

## Output

The hotspot, the change, before/after numbers, and the output-equivalence check. Don't claim a gain you didn't measure.
