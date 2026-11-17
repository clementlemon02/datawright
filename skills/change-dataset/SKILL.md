---
name: change-dataset
description: Change a table, view, model or column without breaking what depends on it - add, rename, retype or drop a column, change a table's grain or key, rename, move or deprecate a table. Finds every writer, every reader (in any clause, including joins, filters and select *), downstream tables, the Airflow tasks that run them and consumers outside the repo, then picks a safe sequence, backfills and verifies. Use for "rename/drop column X", "change the type of", "can I delete table Y", "deprecate", "migrate", "split this table", or any schema change.
license: MIT
---

# Change a dataset safely

Goal: the change lands and nothing downstream breaks, including the things that break quietly: `select *` readers, positional inserts, dashboards and other teams' jobs. Never run DDL or writes against production; produce the change and the steps, and let a person deploy it.

## 1. Map the impact

1. `uv run .data-context/datawright.py impact <dataset>.<column>` (or `<dataset>` for a table change). It lists writers, the code that reads it in any clause with the Airflow task that runs it, `select *` readers, downstream tables and columns (including catalogs' lineage), the owner and notes. If `.data-context/` is missing, set it up with the `data-context` skill.
2. Open each location it lists. If the change carries downstream (a renamed column copied into another table), run `impact` on that column too.
3. Check what the repo can't see: the warehouse query log over a full business cycle (at least 30 days, to catch month-end jobs; per-engine tables in the `data-context` skill's `references/warehouse-export.md`), BI tools, and the owner. If you can't check one of these, say so in the plan as an open risk.

## 2. Pick the pattern

| Change | Safe sequence |
|---|---|
| Add a nullable column | One step. First fix readers that would break: `select *` into a fixed schema, and `insert into t select …` without a column list |
| Add a required column | Add nullable → backfill → add the constraint or default |
| Rename a column | Add the new name and write both → move every reader → stop writing the old one → drop it. Or keep the old name as a view alias during the move |
| Change a type | Widening (int → bigint, longer varchar) is often in place: check the engine. Otherwise a new column, backfill, move readers, drop the old one |
| Drop a column | Stop every reader → confirm nothing reads it (code and query log) → drop. Keep a snapshot or rely on time travel if the engine has it |
| Change grain or key | Build a new table; never change a table's grain in place |
| Rename or move a table | Create the new one; keep the old name as a view on it until readers have moved |
| Deprecate a table | Record the decision, tell the owners, stop the writers after readers are gone, then drop after a waiting period |

Each step must be deployable on its own, with scheduled runs succeeding between steps. When every reader is in this repo and rebuilds in the same run, the steps can ship as one change; anything outside the repo, or not checked, needs the stepwise sequence.

## 3. Make the change

- Write DDL in the repo's own mechanism (dbt model, migration file, the DDL folder the card's `schema in` points to). Additive changes go to writers first, then readers; removals go last.
- Update every reader `impact` found, and turn positional inserts into explicit column lists while you're there.
- Backfill a new column's history by partition, idempotently, following the backfill plan in `build-pipeline`.

## 4. Verify

- Run the affected jobs on dev (the tasks `impact` named) and compare before and after: row counts and key aggregates match except for the intended change. `datawright review --diff` does the comparison for every changed table (with `dev_name` set in `.data-context/config.yml`): rows, added and removed columns, duplicate keys and the sums that moved.
- Add a test for the new column or constraint with `data-tests`.
- Run `datawright build` and `impact` again. Before any drop, the old column or table must have no readers left.
- Review with `pipeline-review` and `sql-review`.

## 5. Hand over

- The plan as numbered deploys: what each changes, what to check after it, how to roll it back.
- Who to tell: owners of downstream tables and consumers outside the repo.
- `datawright note <dataset> "<what is changing and when>" --kind decision --column <column>`, so agents stop building on a column that's going away.
