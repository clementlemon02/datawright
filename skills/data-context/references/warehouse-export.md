# Warehouse export

Optional. The repo scan already gives lineage for code in the repo. A warehouse export adds what the repo can't see: column types for every table, and lineage from jobs, BI tools and other teams' code that run against the warehouse.

**Bulk metadata never passes through the model.** Run each query with the engine's CLI and redirect the output to a file in `.data-context/imports/`, then run `datawright build`. Never run these through an MCP tool: the results would flood the context window.

- Use the read-only, non-production connection. Credentials come from the CLI's own config or environment variables; never type secrets into a command.
- `imports/` is gitignored: query text can contain real values. Delete the files after building if that matters.
- `datawright build` auto-detects the SQL dialect; if query logs come from a different engine than the repo's SQL, set `dialect:` in `.data-context/config.yml`.

## Files `datawright build` reads

Header names are case-insensitive; `.csv` or `.tsv`. Any subset is fine.

| File | Columns | Gives |
|---|---|---|
| `columns.csv` | `table_schema`, `table_name`, `column_name`, `data_type`, optional `table_catalog`, `comment` | Schema for every table |
| `tables.csv` | `table_schema`, `table_name`, optional `row_count`, `comment` | Sizes and descriptions |
| `queries.csv` | `query_text` (or `query`, `statement_text`), optional `calls` | Lineage from writes (INSERT, MERGE, CTAS); usage counts from reads |
| `edges.csv` | `source`, `target`, optional `source_column`, `target_column` | Lineage the platform already computed |

## Per engine

Each query below writes one file. Adjust the database, project or region names; check them against your engine's docs if a column name has changed.

### Snowflake
CLI: `snow sql -q "<query>" --format CSV > .data-context/imports/<file>.csv`
- columns: `select table_catalog, table_schema, table_name, column_name, data_type, comment from <db>.information_schema.columns where table_schema <> 'INFORMATION_SCHEMA'`
- queries: `select query_text from snowflake.account_usage.query_history where start_time > dateadd(day, -30, current_timestamp()) and execution_status = 'SUCCESS' and query_type in ('INSERT', 'MERGE', 'CREATE_TABLE_AS_SELECT', 'SELECT') limit 50000` (needs access to the `SNOWFLAKE` database)

### BigQuery
CLI: `bq query --use_legacy_sql=false --format=csv --max_rows=1000000 '<query>' > .data-context/imports/<file>.csv`
- columns: ``select table_catalog, table_schema, table_name, column_name, data_type from `<project>`.`region-<region>`.INFORMATION_SCHEMA.COLUMNS``
- edges (BigQuery records each job's source and destination tables):
  ```sql
  select distinct
    concat(r.project_id, '.', r.dataset_id, '.', r.table_id) as source,
    concat(j.destination_table.project_id, '.', j.destination_table.dataset_id, '.', j.destination_table.table_id) as target
  from `<project>`.`region-<region>`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j, unnest(j.referenced_tables) r
  where j.creation_time > timestamp_sub(current_timestamp(), interval 30 day)
    and j.state = 'DONE' and j.job_type = 'QUERY'
    and not starts_with(j.destination_table.dataset_id, '_')   -- skip anonymous result tables
  ```
  `referenced_tables` is empty for cache hits and for tables with row-level access policies.

### Databricks (Unity Catalog)
CLI: the Databricks SQL CLI, `dbsqlcli -e "<query>" > .data-context/imports/<file>.csv`, or run it in the SQL editor and download the CSV.
- columns: `select table_catalog, table_schema, table_name, column_name, data_type, comment from system.information_schema.columns where table_schema <> 'information_schema'`
- edges (Databricks computes lineage natively): `select distinct source_table_full_name as source, target_table_full_name as target from system.access.table_lineage where source_table_full_name is not null and target_table_full_name is not null and event_time > current_timestamp() - interval 30 days`

### Redshift and PostgreSQL
CLI: `psql "$DATABASE_URL" --csv -c "<query>" > .data-context/imports/<file>.csv`
- columns: `select table_schema, table_name, column_name, data_type from information_schema.columns where table_schema not in ('pg_catalog', 'information_schema')`
- queries, Redshift: `select query_text from sys_query_history where status = 'success' and start_time > dateadd(day, -30, getdate())` (text is cut at 4,000 characters, so long statements may not parse)
- queries, PostgreSQL: `select query, calls from pg_stat_statements` (needs the `pg_stat_statements` extension)

### ClickHouse
CLI: `clickhouse client --query "<query>" --format CSVWithNames > .data-context/imports/<file>.csv`
- columns: `select database as table_schema, table as table_name, name as column_name, type as data_type, comment from system.columns where database not in ('system', 'INFORMATION_SCHEMA', 'information_schema')`
- queries: `select any(query) as query, count() as calls from system.query_log where type = 'QueryFinish' and event_time > now() - interval 30 day group by normalized_query_hash`

### Trino
CLI: `trino --execute "<query>" --output-format CSV_HEADER > .data-context/imports/<file>.csv`
- columns: `select table_catalog, table_schema, table_name, column_name, data_type from <catalog>.information_schema.columns where table_schema <> 'information_schema'`

### DuckDB / MotherDuck
CLI: `duckdb -csv <file.duckdb> "<query>" > .data-context/imports/<file>.csv`
- columns: `select table_catalog, table_schema, table_name, column_name, data_type from information_schema.columns`

### MySQL and StarRocks
CLI: `mysql --batch -e "<query>" > .data-context/imports/<file>.tsv` (tab-separated; connection details from `~/.my.cnf`, never a password on the command line)
- columns: `select table_schema, table_name, column_name, data_type, column_comment as comment from information_schema.columns where table_schema not in ('information_schema', 'mysql', 'sys', 'performance_schema', '_statistics_')`

### Catalogs and OpenLineage
If a lineage tool already tracks what the repo can't see (BI dashboards, other teams' jobs, ML features), save its export as `.json` or `.jsonl` in `.data-context/imports/`. `datawright build` recognises each format by its shape and reads table lineage, column lineage, column types and descriptions. Catalog names are matched to the repo's names, e.g. `warehouse.mart.orders` to `mart.orders`. Cards tag these edges with the tool they came from. Any other tool: export `edges.csv` as above.

- **OpenLineage** (Airflow's OpenLineage provider, Spark, dbt, Marquez): raw run events, one JSON object per line. The simplest source is the client's file transport, for example in Airflow's config `[openlineage] transport = {"type": "file", "log_file_path": "/path/openlineage.jsonl", "append": true}`; copy the file into `imports/`. If you run Marquez, export its stored events instead (check your version's API reference for the events endpoint).
- **DataHub**: `upstreamLineage` and `schemaMetadata` aspects as metadata change proposals. Run an ingestion recipe with a `file` sink (`sink: {type: file, config: {filename: .data-context/imports/datahub.json}}`) for the sources you care about.
- **OpenMetadata**: lineage per table and the table list, from its REST API with a bot token (the API is paginated; check the parameters against your version's API docs):
  ```bash
  curl -H "Authorization: Bearer $OM_TOKEN" "$OM_URL/api/v1/lineage/table/name/<service.db.schema.table>?upstreamDepth=3&downstreamDepth=3" > .data-context/imports/openmetadata_<table>.json
  curl -H "Authorization: Bearer $OM_TOKEN" "$OM_URL/api/v1/tables?fields=columns&limit=1000" > .data-context/imports/openmetadata_tables.json
  ```
