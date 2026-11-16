# Connectors: teach dctx your stack

`dctx` reads SQL in most dialects, SQL inside Python, Java, Scala and Kotlin, dbt manifests, Airflow DAGs, Spark and Flink jobs, Kafka topics, and OpenLineage, DataHub and OpenMetadata exports. Everything else, such as an in-house pipeline framework, jobs defined in YAML, another orchestrator or hand-written lineage, it learns from a **connector**: a small Python file in `.data-context/connectors/`, committed with the repo.

## The loop

1. `uv run .data-context/dctx.py coverage` lists what dctx doesn't read yet, largest gaps first, with a hint for each: files that look like lineage, DAG definitions or SQL inside config, DAG tasks it can't link to code, unparsed SQL, and tools it sees but has no reader for.
2. For the largest gap, copy a ready-made connector from this skill's `assets/connectors/`, or write one (below). Read two or three of the files first, so `match` and `extract` fit their real shape.
3. `dctx build`, then `coverage` again. Check a few cards for the tables the connector touched, and that its names line up with the names in code: the coverage line "connector edges: N, M touching a table found in code" shows how many connect.
4. Repeat until the gaps left don't matter. Commit the connectors: they're how every agent on the team gets the same picture.

## Writing one

A connector is a module with two functions. `match(path)` gets each repo path and says whether to read it. `extract(path, text)` gets the file's text and returns records:

| Record | Means |
|---|---|
| `{"sql": text, "line": n}` | SQL to parse like any other: lineage, columns, readers and column uses all follow. Use it for SQL that sits in YAML, JSON or templates |
| `{"produces": "db.table", "line": n, "kind": "my job"}` | This file writes that table (it becomes the table's producer) |
| `{"reads": "db.table", "line": n}` | This file reads that table |
| `{"edge": ["db.source", "db.target"], "kind": "…"}` | Lineage between two datasets, for formats that state it directly |
| `{"column": ["db.table", "col", "type"]}` | A column and its type |
| `{"column_edge": ["db.source.col", "db.target.col"]}` | Column lineage |
| `{"dataset": "db.table", "description": "…", "owner": "…", "kind": "…"}` | What a table is, without saying who writes it (descriptions from schema files, catalogs). A `kind` containing "dashboard" lists it under "Dashboards reached" in `review` |
| `{"env": ["KAFKA_TOPIC_ORDERS", "shop.orders"]}` | What a deploy-time variable is set to, when the deploy config isn't in the repos but something else records it (lineage docs, a runbook). Topics and tables named `env:<VAR>` in code take that value |
| `{"layout": ["db.table", "partition", ["event_date"]]}` | A key declared outside DDL (a framework's metadata file): `partition`, `cluster`, `sort` or `distribution`. `dctx advise` compares it with how the code filters and joins the table |
| `{"contract": ["db.table", "col", "type", required]}` | What a data contract promises consumers. A required column the code stops producing shows on the card and in `lint`, and blocks `review --strict` |
| `{"run": {"dag": "…", "task": "…", "schedule": "…", "lines": [first, last], "file": "other/path"}}` | The job that runs these lines of this file (or of `file`, for a config that schedules a sibling SQL file), shown on cards as "runs in". With `"dataset": "db.table"` instead, it's placed on whatever code produces that table, for schedules defined apart from the code |

Two module settings: `OWNS = True` makes the connector read its files *instead of* the built-in reader. Use it when the framework gives plain SQL a meaning dctx can't see, such as "a SELECT whose destination is its folder". `ROOT` is set by dctx to the repo root, for reading a shared file such as a schedule.

Connectors also get the files in subfolders of `.data-context/imports/`: exports that aren't committed, such as BI definitions. A ZIP there is unpacked into a folder of the same name on each build (a newer ZIP replaces it).

Name Kafka topics `kafka:<topic>`, so they join the topics dctx finds in Flink and Spark code. Give anything that isn't a warehouse table a namespace the same way (`looker:view.orders`, `tableau:workbook.sales`): a name with a colon never stands in for a short table name, so a BI view called `orders` doesn't make `orders` in SQL ambiguous. Names are matched loosely: `mart.orders` lines up with `warehouse.mart.orders` when that's the only match.

```python
"""dctx connector: jobs defined in pipelines/*.yaml as {name, schedule, steps: [{sql}]}."""
import yaml

def match(path):
    return path.startswith("pipelines/") and path.endswith(".yaml")

def extract(path, text):
    job = yaml.safe_load(text)
    for i, step in enumerate(job.get("steps", []), 1):
        if "sql" in step:
            yield {"sql": step["sql"], "line": i}
    yield {"run": {"dag": job["name"], "task": "steps", "schedule": job.get("schedule", "manual"), "lines": [1, 10**9]}}
```

## Rules

- A connector runs during `dctx build`, locally and in CI, with the same trust as the repo's own code. Keep it to reading the file it's given.
- If a connector fails on a file, the build reports it in `lint` and carries on; the error is stored without quoting the file.
- Never put credentials or data values in a connector. Read names and SQL, not rows.
- Something only the warehouse knows (query history, a catalog's API) is an export instead: see [warehouse-export.md](warehouse-export.md).

## Ready-made connectors

| File | Reads |
|---|---|
| [lineage_yaml.py](../assets/connectors/lineage_yaml.py) | Hand-written lineage in YAML or JSON (`from`/`to`, `source`/`target`, `upstream`/`downstream`; names or objects with `fqn`), as teams keep for OpenMetadata or DataHub. Settings for service prefixes, topic names and renames |
| [kafka_connect.py](../assets/connectors/kafka_connect.py) | Kafka Connect configs: Debezium sources (table to topic) and sinks (topic to table) |
| [dbt_yaml.py](../assets/connectors/dbt_yaml.py) | dbt schema files: model, source, seed and snapshot descriptions and columns, for repos without a committed manifest |
| [odcs.py](../assets/connectors/odcs.py) | Data contracts in the Open Data Contract Standard (v3) or the older Data Contract Specification: owner, description and each column's promised type and whether it's required |
| [lookml.py](../assets/connectors/lookml.py) | Looker: views (`sql_table_name`, derived tables, `@{constants}` from `manifest.lkml`), explores and their joins, and LookML dashboards, down to which warehouse column each tile shows. Dashboards made in the Looker UI aren't in the repo: export them through Looker's API as lineage YAML |
| [mybatis.py](../assets/connectors/mybatis.py) | MyBatis mapper XML, the SQL behind Java services: dynamic SQL (`<if>`, `<where>`, `<foreach>`, `<include>`, `<choose>` with every branch) rendered to plain SQL, so its reads and joins count. On one platform's 22 mappers it took join pairs from 90 to 507 |
| [superset.py](../assets/connectors/superset.py) | Superset and Preset: datasets (physical, virtual SQL, calculated columns, metrics), charts and the columns and metrics they use, dashboards. From an export ZIP or `preset-cli` YAML; fetch one with `assets/bi_export.py superset` |
| [metabase.py](../assets/connectors/metabase.py) | Metabase questions and models (query builder, both formats, and SQL with `{{variables}}`, `[[optional]]` clauses and `{{#card}}` references) and dashboards, from `assets/bi_export.py metabase` |
| [dagster.py](../assets/connectors/dagster.py) | Dagster assets (`@asset`, `@dg.asset`, `AssetSpec`), their upstream from `deps`, `ins` and parameters, and `define_asset_job` + `ScheduleDefinition` schedules, even across files. Asset keys become dotted names (`["ANALYTICS", "orders"]` → `analytics.orders`) |

## A framework connector, start to finish

Mozilla's open-source bigquery-etl keeps each query as `sql/<project>/<dataset>/<table>/query.sql`, a bare SELECT whose destination is its folder, with `metadata.yaml` scheduling it in a DAG from `dags.yaml`. A 25-line connector (`OWNS = True`; it hands each query over as `insert overwrite <folder's table> …`, since each run replaces its partition, and passes the partitioning and clustering `metadata.yaml` declares as `layout` records, and turns `metadata.yaml` into a description and a `run` for the sibling query.sql) took that repo from 292 to 566 lineage edges and 1,241 to 3,373 column edges, and linked 149 tables to their DAG and schedule.
