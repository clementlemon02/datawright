# Connections catalog

MCP servers for popular data tools, checked against vendor docs, READMEs and package pages on 2026-11-03. Servers change fast: always re-read the linked source before writing config.

## Rules for every connection

- **Read-only, least-privilege user on a non-production environment.** This is the real guard. Several vendors say their built-in read-only switches are "not a security boundary".
- **Turn on the read-only setting** listed below. Where there is none, block the listed write tools in the agent's config.
- **Connect only what the task needs.** Agents with many overlapping tools pick the wrong one more often.
- **Remote servers** (HTTP + OAuth) need an agent that supports remote MCP, or a bridge such as `mcp-remote`.
- **A CLI works too.** If the tool's CLI is installed and logged in with a read-only user (`bq`, `snow`, `databricks`, `psql`, `duckdb`, `trino`, `dbt`, `af`, `dg`, `prefect`, …), the agent can use it directly, often with fewer tokens than an MCP server. The read-only user is then the only guard, so it must exist. Set that CLI as `query_command` in `.data-context/config.yml` to add `dctx query`, which runs only single SELECTs with a row cap (also an MCP tool via `dctx serve`).

## Warehouses and query engines

| Tool | Server (publisher) | Run | Auth | Read-only |
|---|---|---|---|---|
| Snowflake | Snowflake-managed MCP server (Snowflake). Create it with `CREATE MCP SERVER`. `snowflake-labs-mcp` is deprecated. | Remote: `https://<account_url>/api/v2/databases/<db>/schemas/<schema>/mcp-servers/<name>` | OAuth, or PAT as Bearer | `SYSTEM_EXECUTE_SQL` tool's `read_only` setting (default `true`) |
| BigQuery | BigQuery remote MCP (Google) | Remote: `https://bigquery.googleapis.com/mcp` | OAuth / IAM: `mcp.toolUser`, `bigquery.jobUser`, `bigquery.dataViewer` | Use `execute_sql_readonly`; block `execute_sql` |
| BigQuery (local) | MCP Toolbox (Google) | `npx -y @toolbox-sdk/server --prebuilt bigquery --stdio` | `BIGQUERY_PROJECT` + application default credentials | No switch; block `execute_sql` |
| Databricks | Managed MCP `system.ai.dbsql` (Databricks) | Remote: `https://<workspace>/ai-gateway/mcp-services/system.ai.dbsql` | OAuth | `disallow_writes: true` in `system.ai.dbsql_policy` |
| Redshift | `awslabs.redshift-mcp-server` (AWS) | `uvx awslabs.redshift-mcp-server@latest` | `AWS_PROFILE`, `AWS_REGION` | `ACCESS_MODE=read-only` (default) |
| ClickHouse | `mcp-clickhouse` (ClickHouse) | `uv run --with mcp-clickhouse --python 3.12 mcp-clickhouse` | `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Read-only by default; leave `CLICKHOUSE_ALLOW_WRITE_ACCESS` unset |
| DuckDB / MotherDuck | `mcp-server-motherduck` (MotherDuck) | `uvx mcp-server-motherduck --db-path <file or md:>` | `motherduck_token` (MotherDuck only) | Read-only unless `--read-write` |
| StarRocks | `mcp-server-starrocks` (StarRocks) | `uv run --with mcp-server-starrocks mcp-server-starrocks` | `STARROCKS_URL` | No switch; block `write_query` |
| Trino | `mcp-trino` (community) | `brew install tuannvm/mcp/mcp-trino` | `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_PASSWORD`, `TRINO_CATALOG`, `TRINO_SCHEMA` | Read-only by default; leave `TRINO_ALLOW_WRITE_QUERIES` unset |
| PostgreSQL | `postgres-mcp` (Crystal DBA, community; last release May 2025) | `uvx postgres-mcp --access-mode=restricted` | `DATABASE_URI` | `--access-mode=restricted` |
| MySQL | `@benborla29/mcp-server-mysql` (community) | `npx -y @benborla29/mcp-server-mysql` | `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASS`, `MYSQL_DB` | Writes off by default; leave `ALLOW_*_OPERATION` unset |

## Transformation

| Tool | Server (publisher) | Run | Auth | Read-only |
|---|---|---|---|---|
| dbt | `dbt-mcp` (dbt Labs) | `uvx dbt-mcp` | `DBT_PROJECT_DIR`, `DBT_PATH`; dbt platform adds `DBT_HOST`, `DBT_TOKEN`, `DBT_PROD_ENV_ID` | `DISABLE_SQL` defaults to true. To stop it building models, set `DISABLE_DBT_CLI=true` (blocks all CLI tools) or list specific tools in `DISABLE_TOOLS` |

## Orchestration

| Tool | Server (publisher) | Run | Auth | Read-only |
|---|---|---|---|---|
| Airflow | `apache-airflow-mcp-server` (community) | `uvx --from apache-airflow-mcp-server --with apache-airflow-client==<your Airflow version> airflow-mcp --transport stdio` | `AIRFLOW_MCP_HOST`, `AIRFLOW_MCP_USERNAME` + `AIRFLOW_MCP_PASSWORD` or `AIRFLOW_MCP_TOKEN` | `AIRFLOW_MCP_READ_ONLY=true` |
| Airflow / Astro | `astro-airflow-mcp` (Astronomer) | `uvx astro-airflow-mcp --transport stdio` | `AIRFLOW_API_URL`, `AIRFLOW_USERNAME` + `AIRFLOW_PASSWORD` or `AIRFLOW_AUTH_TOKEN` | `AF_READ_ONLY=true` (documented on PyPI); or block `trigger_dag`, `trigger_dag_and_wait`, `delete_dag_run`, `clear_dag_run`, `pause_dag`, `unpause_dag`, `clear_task_instances` |
| Dagster+ | Dagster+ MCP (Dagster; beta, Dagster+ only) | Remote: `https://mcp.agent.dagster.cloud/mcp` | OAuth, or user token + `Dagster-Cloud-Organization` header | No switch; use a restricted user and block run, asset and alert write actions |
| Prefect | `prefect-mcp` (Prefect; experimental) | `uvx --from prefect-mcp prefect-mcp-server` | `PREFECT_API_URL`, `PREFECT_API_KEY` | Read-only by design |

## Ingestion and streaming

| Tool | Server (publisher) | Run | Auth | Read-only |
|---|---|---|---|---|
| Fivetran | `fivetran-mcp` (Fivetran GitHub org) | `uvx --from git+https://github.com/fivetran/fivetran-mcp fivetran-mcp` (not the unrelated PyPI package of the same name) | `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET` | `FIVETRAN_SCOPE=read` (default) |
| Airbyte | Airbyte MCP (Airbyte; hosted, Standard plan and above) | Remote: `https://mcp.airbyte.com/mcp?read-only=true` | OAuth, or client id + secret | The `?read-only=true` URL |
| Kafka / Confluent | `@confluentinc/mcp-confluent` (Confluent) | `npx @confluentinc/mcp-confluent --config ./config.yaml` | `KAFKA_API_KEY`, `KAFKA_API_SECRET`, `SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET` | No switch; `--block-tools` the write tools: `create-topics`, `delete-topics`, `produce-message`, `alter-topic-config`, and the connector, Flink statement, schema, tag and Tableflow create/update/delete tools |

## Catalog and observability

An MCP server here answers live questions one table at a time. To put a catalog's or OpenLineage's lineage on every `dctx` card, export it in bulk instead (see `data-context` skill, references/warehouse-export.md): it costs no tokens per question.

| Tool | Server (publisher) | Run | Auth | Read-only |
|---|---|---|---|---|
| DataHub | `mcp-server-datahub` (DataHub) | `uvx mcp-server-datahub` | `DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN` | Mutations off by default; leave `TOOLS_IS_MUTATION_ENABLED` unset |
| OpenMetadata | Built-in MCP server (OpenMetadata) | Remote: `https://<server>/mcp` | OAuth, PAT or bot JWT | No switch; use a read-only role and block `create_entity`, `patch_entity` |
| Monte Carlo | Monte Carlo MCP (Monte Carlo; hosted) | Remote: `https://mcp.getmontecarlo.com/mcp` | OAuth, or MCP key | Has a read-only mode (check the docs for the setting); block `update_alert`, `create_or_update_table_monitor`, `create_agent_metric_monitor` |
| Great Expectations | No official server | Use the repo's GX code and CLI | | |

## Tickets and stories

For the `work-ticket` skill. Trackers have write tools (create, edit, transition, comment): block them, or let the agent use them only after the user approves each update.

| Tool | Server (publisher) | Run | Auth | Read-only |
|---|---|---|---|---|
| Jira, Confluence | Atlassian Rovo MCP Server (Atlassian) | Remote: `https://mcp.atlassian.com/v1/mcp/authv2` (Claude Code: `claude mcp add --transport http atlassian <url>`, then `/mcp` to sign in) | OAuth 2.1, or an API token | No switch; block the create, edit, transition and comment tools |
| Linear | Linear MCP (Linear) | Remote: `https://mcp.linear.app/mcp`; check Linear's docs, the endpoint has changed before | OAuth, or an API key | `https://mcp.linear.app/mcp/readonly`, or a restricted API key |
| GitHub Issues | `gh` CLI | `gh issue view <n>`, `gh issue list` | `gh auth login` | Commands that only read; `gh issue comment` and `create` need approval |

## Vendor skills

Official skills that teach agents a tool's mechanics. This kit handles the workflow; these handle the tool.

| Tool | Publisher | Claude Code | Other agents |
|---|---|---|---|
| dbt | dbt Labs | `/plugin marketplace add dbt-labs/dbt-agent-skills` then `/plugin install dbt@dbt-agent-marketplace` | `npx skills add dbt-labs/dbt-agent-skills` |
| Airflow | Astronomer | `/plugin marketplace add astronomer/agents` then `/plugin install astronomer-data@astronomer` (bundles `astro-airflow-mcp`; set it read-only) | `npx skills add astronomer/agents --skill '*'` |

## Sources

- Atlassian Rovo MCP Server: https://support.atlassian.com/rovo/docs/getting-started-with-the-atlassian-remote-mcp-server/ and https://github.com/atlassian/atlassian-mcp-server
- Linear MCP: https://linear.app/changelog/2025-05-01-mcp and https://docs.litellm.ai/docs/mcp_servers/linear

[Snowflake](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp) · [BigQuery](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp) · [Databricks](https://docs.databricks.com/aws/en/agents/mcp-tools/databricks-sql) · [Redshift](https://github.com/awslabs/mcp/tree/main/src/redshift-mcp-server) · [ClickHouse](https://github.com/ClickHouse/mcp-clickhouse) · [MotherDuck](https://github.com/motherduckdb/mcp-server-motherduck) · [StarRocks](https://pypi.org/project/mcp-server-starrocks/) · [Trino](https://github.com/tuannvm/mcp-trino) · [PostgreSQL](https://github.com/crystaldba/postgres-mcp) · [MySQL](https://github.com/benborla/mcp-server-mysql) · [dbt](https://docs.getdbt.com/docs/dbt-ai/setup-local-mcp) · [Airflow](https://pypi.org/project/apache-airflow-mcp-server/) · [Astronomer](https://pypi.org/project/astro-airflow-mcp/) · [Dagster](https://dagster.io/docs/guides/labs/dagster-mcp) · [Prefect](https://github.com/PrefectHQ/prefect-mcp-server) · [Fivetran](https://github.com/fivetran/fivetran-mcp) · [Airbyte](https://docs.airbyte.com/platform/airbyte-mcp/install) · [Confluent](https://github.com/confluentinc/mcp-confluent) · [DataHub](https://github.com/acryldata/mcp-server-datahub) · [OpenMetadata](https://docs.open-metadata.org/v2.0.x/how-to-guides/mcp) · [Monte Carlo](https://docs.getmontecarlo.com/docs/mcp-server) · [dbt agent skills](https://docs.getdbt.com/blog/dbt-agent-skills) · [Astronomer agents](https://github.com/astronomer/agents)
