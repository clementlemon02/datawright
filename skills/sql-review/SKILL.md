---
name: sql-review
description: Review SQL for any engine (warehouse queries, transformation models, batch-engine SQL) for wrong results, runaway cost, and unsafe writes. Use when the user writes or changes SQL, asks to "review this query", or a model's row counts or numbers look wrong.
license: MIT
---

# SQL review

Review the changed SQL or the query the user names. Identify the engine and dialect from the code first. Wrong numbers are worse than slow queries, so check correctness first.

**Context first:** run `uv run .data-context/datawright.py card <dataset>` for each dataset involved (set it up with the `data-context` skill if it's missing). Before finishing, record anything non-obvious you learned with `datawright set` or `datawright note`.

## Correctness

- **Join fan-out**: for every join, state the grain of each side. If the join key isn't unique on one side, rows multiply. Flag `DISTINCT` used to hide this.
- **LEFT JOIN turned INNER** by a `WHERE` filter on the right table. The filter belongs in `ON`.
- **NULL traps**: `NOT IN` with a nullable subquery returns nothing; `col != 'x'` drops NULLs; `COUNT(col)` skips NULLs.
- **Dedup that isn't deterministic**: `ROW_NUMBER()` without a full tie-breaking `ORDER BY`.
- **Time**: timezone of timestamps vs. partition dates; half-open ranges (`>= start AND < end`); late-arriving data outside the window.
- **Integer division** and implicit casts that truncate.

## Cost

- Partition / cluster pruning: no functions wrapped around the partition column in filters; a date filter exists on large tables.
- `SELECT *` on wide tables in production models.
- Accidental cross joins; skewed join keys.
- The engine's own tuning rules: how it partitions, clusters or buckets data, which join strategies it picks, and what it bills on (bytes scanned, compute time). Look these up for the engine rather than assuming.

## Safety

- `DELETE` / `UPDATE` / `MERGE` without a bounded `WHERE`.
- DDL against shared or production schemas.
- PII columns selected into places they shouldn't land.

## Verify, don't assume

If a read-only warehouse MCP server is connected, prove the risky claims:
- `EXPLAIN` to check pruning and join strategy.
- `SELECT key, COUNT(*) ... GROUP BY key HAVING COUNT(*) > 1` to prove or rule out fan-out.
- Row counts before vs. after the change on one partition.

Only run read queries. Never run writes, even if the tool allows it.

## Output

A short table: severity (wrong result / cost / safety / nit), location, problem, fix. Put any evidence query you ran under its finding. If nothing is wrong, say so in one line.
