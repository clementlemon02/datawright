# Landscape: tools to learn from, and real data platforms to fit

Researched November 2026. The principle is **connect, don't absorb** (ARCHITECTURE.md, principle 6): connect to good tools, borrow their ideas, and build only the missing layer. Sources are linked; several are secondary write-ups or vendor material, marked as such.

## What data teams actually struggle with

- **Organisation and ownership, more than technology.** The 2026 State of Data Engineering survey (1,101 respondents) found organisational challenges are the top bottleneck for 52%, ahead of technical debt (25%), and 51% cite lack of ownership. Its top AI uses are code generation, documentation and pipeline debugging ([survey](https://joereis.github.io/practical_data_data_eng_survey); figures from a [secondary write-up](https://dev.to/iblaine/state-of-data-engineering-2026-why-data-teams-spend-60-of-their-time-firefighting-2ka9)).
- **Broken assumptions between systems, more than broken SQL.** An analysis of 1,000 pipeline incidents put code failures at about 10% of data quality issues ([DZone](https://dzone.com/articles/ai-sql-quality-issues)).
- **Visibility and testing.** Only 14% enforce pipeline SLAs organisation-wide, and limited visibility into pipeline health is the top challenge (31%) ([Integrate.io](https://www.integrate.io/blog/september-2025-trends-report-data-quality-and-observability)). Not knowing how to test well is the biggest data quality challenge in another survey ([Synq](https://synq.io/blog/2025-data-quality-benchmark-survey)). Older vendor data puts time spent on data quality at about 40% of the workday ([Monte Carlo, 2022](https://montecarlo.ai/blog-2022-data-quality-survey)). These vendor figures are directional.

These map onto the kit directly: ownership and assumptions are what cards, notes and `impact` carry; visibility is `check` and `on-call`; testing is `data-tests` and `review`.

## Real data platforms

| Company | Their problem and approach | Does the kit help? | What it would need |
|---|---|---|---|
| **Uber** | Tiers every dataset (0 to 5); a tier sets default SLAs and automatic quality checks, and lineage auto-generates tests up and downstream. 2,000+ critical datasets, ~90% of incidents detected ([Uber](https://www.uber.com/ie/en/blog/operational-excellence-data-quality/)) | Partly: `check` derives limits from schedules, `impact` gives up and downstream | **Tiers**: a `tier` per table that sets check limits and how strict `review` is |
| **Airbnb** | Midas certification (a nine-step gold standard), then a 0 to 100 Data Quality Score to reach beyond certified tables ([Airbnb](https://medium.com/airbnb-engineering/data-quality-score-the-next-chapter-of-data-quality-at-airbnb-851dccda19c3)); "Wall" checks to prevent data bugs ([Airbnb](https://medium.com/airbnb-engineering/how-airbnb-built-wall-to-prevent-data-bugs-ad1b081d6e8f)) | Partly: `lint` lists the most-used tables with no grain or owner; `data-tests` | **A readiness score per table** from what datawright knows: owner, grain, tests, freshness check, open gotchas, downstream count |
| **Monzo** | 12,000+ dbt models across 100+ teams, a four-layer "meshy" warehouse, ~40% lower costs ([InfoQ](https://infoq.com/news/2026/05/monzo-data-mesh)); column lineage from audit logs and ZetaSQL to find unused models and every Looker dashboard a change touches ([Monzo](https://medium.com/data-monzo/mapping-our-data-journey-with-column-lineage-56209c00606d)) | Strongly: column uses in any clause, `impact`, unused tables in `lint`, query-log imports | **BI lineage**: a LookML connector first (then Metabase, Superset, Tableau), so dashboards show up in `impact` |
| **Netflix** | A central lineage service fed by Metacat metadata and S3 access logs, finding relationships ingestion missed (secondary: [Atlan](https://atlan.com/metacat-netflix-open-source-metadata-platform), [AIM](https://www.analyticsindiamag.com/how-does-netflix-handle-its-data-engineering-woes/)) | Partly: query-log and catalog imports add what code can't show | Nothing new; storage access logs could be one more import |
| **LinkedIn** | Built DataHub as its metadata platform | Yes: DataHub imports and the MCP server in the catalog | Nothing new |
| **Your Airflow + Flink + StarRocks platform** | DAGs that submit Flink jars by class name, topics from environment variables, 56 hand-written lineage YAML files for OpenMetadata | Strongly: across-repo Kafka lineage, `python_callable` links, `lineage_yaml` | **Exporting lineage**: generate those YAML files, or OpenLineage events, from what datawright already parses, instead of writing them by hand |

## Startups, SMEs and nonprofits, tested

Public repos where a smaller organisation runs its real data stack, built and checked with `datawright build` and `coverage` (November 2026). Mattermost's and Meltano's own warehouse repos are no longer public.

| Repo | Who and stack | Before fixes | After fixes, and what they were |
|---|---|---|---|
| [duneanalytics/spellbook](https://repos.ecosyste.ms/hosts/GitHub/repositories/duneanalytics%2Fspellbook) | Dune (crypto analytics startup): five dbt projects on Trino, 8,063 SQL files | 271 datasets, 31 edges: models opening with `{{ config(...) }}` were skipped, macros parsed as queries | **10,184 datasets, 7,571 models, 7,359 edges in 8 s.** Models are read from `ref()`/`source()` even when heavy Jinja stops them parsing; macros are skipped; coverage suggests `dbt parse` for column lineage |
| datacoves/balboa | Datacoves (data platform startup) reference stack: Snowflake, dbt, Airflow, Airbyte, Fivetran, dlt | 55 DAG tasks with nothing linked | Tasks that run dbt link to the models they select, the way dbt reads selectors: names, `tag:` (tags from SQL, schema YAML and `dbt_project.yml`), `source:`, paths, `+`/`n+`/`@` and `--exclude`. Only run-state selectors (`state:`, `result:`) stay with dbt |
| wikimedia/analytics-refinery | Wikimedia (nonprofit, Hadoop scale): 335 Hive SQL files | 83 edges, between variable names | `${var}` substitution is now recognised, so jobs that share a variable name are no longer falsely linked (0 edges: the real names come from the Airflow repo, which `repos:` can add) |
| [hoangsonww/End-to-End-Data-Pipeline](https://awesome.ecosyste.ms/projects/github.com%2Fhoangsonww%2Fend-to-end-data-pipeline) | Demo: Kafka, Spark, Airflow, Great Expectations | Build output counted as unread | Binary build output ignored; coverage flags Kafka clients and Great Expectations, both on the roadmap |

## Tools to learn from

| Tool | What it is | Relationship | Idea to borrow |
|---|---|---|---|
| **Recce** | Reviews dbt changes by comparing dev and prod data: lineage diff, schema and row-count diff, profile, value and histogram diffs, a reviewer checklist ([Recce](https://github.com/DataRecce/recce)) | Coexist (dbt only) | **Data diff in `review`**: run guarded queries on the changed tables in dev and prod and report row-count, schema and key-metric differences. Today `review` reads code only |
| **SQLMesh** | Plans changes before running them; marks each change breaking (downstream affected) or non-breaking (added column) and backfills only what's affected; column lineage for impact ([SQLMesh](https://github.com/SQLMesh/sqlmesh), [explainer](https://davidsj.substack.com/p/breaking-and-non-breaking-changes)) | Learn from; a connector later | **Breaking-change labels in `review`**: from the diff and column uses, label each change breaking or not, and name exactly what to backfill |
| **datacontract-cli / ODCS** | Open Data Contract Standard (Linux Foundation): one YAML file per dataset with structure, semantics, quality and ownership; `changelog` spots breaking changes ([docs](https://docs.datacontract.com/open-data-contract-standard)) | Connect | **Contracts**: a connector reads ODCS files into cards (owner, guarantees), and `review` flags a change that breaks one |
| **Altimate Code** | MIT agent harness for dbt and SQL (OpenCode fork), 100+ deterministic tools, column lineage, FinOps ([repo](https://github.com/AltimateAI/altimate-code)) | Alternative (an agent you switch to) | **A lineage benchmark** like their 500-query one (vendor-reported); **cost analysis** as a future skill |
| **Snowflake Cortex Code, Databricks Genie Code, BigQuery data engineering agent** | Agents native to each platform, governed by its access rules ([Verdantix](https://www.verdantix.com/client-portal/blog/databricks-bets-on-ai-agents-for-autonomous-data-engineering-with-genie-code), [Pulse](https://www.pulse.bot/big-data/news/cortex-code-expands-one-governed-agent-for-your-entire-data-stack-everywhere-you-work-f2afe559-012b-4ea8-bfee-360fcdfd4c54/), [Integrate.io](https://www.integrate.io/blog/ai-agents-data-engineering-teams.md)) | Connect (their MCP servers); we cover what spans platforms | **Governance as a feature**: respect the warehouse's access rules, keep an audit log of what agents queried, human review of changes |
| **Astronomer agents** | Airflow skills and MCP server ([repo](https://github.com/astronomer/agents)) | Connect (setup installs them) | Nothing to copy |
| **OpenLineage, DataHub, OpenMetadata** | Lineage standard and catalogs | Connect (imports) | **Write back**: export datawright's parsed lineage to them |
| **Monte Carlo, Bigeye** (observability) | Monitoring, incidents, lineage-based root cause | Coexist | Tiers and SLAs (as Uber) |

## Where the kit stands

- **Strong:** context across a whole stack in one index (Airflow, Flink, Kafka, dbt, Dagster, catalogs, several repos); impact on any column in any clause; review on every merge request; debugging by walking lineage; learning a new stack through coverage and connectors; works in any agent.
- **Added from this research:** tiers; a readiness score per table; ODCS contracts; Looker dashboards in lineage (other BI tools through connectors); data diff in `review`.
- **Partial:** data diff (aggregates, not values); contracts (required columns, not types or quality rules); BI (Looker only).
- **Missing:** breaking-change labels; cost analysis; PII and access review; writing lineage back to catalogs.

## Proposed additions, in order

1. **Breaking-change labels in `review`**, from SQLMesh. Small: datawright already knows the diff and every column use.
2. **Tiers**, from Uber. A `tier` field that tightens `check` limits and marks tier-0 changes in `review`. Done.
3. **BI lineage**, from Monzo. A LookML connector first, so dashboards show up in `impact` and `review`. Done for Looker.
4. **Data diff in `review`**, from Recce. Guarded before-and-after queries on dev and prod, where a dev environment exists. Done (`--diff`).
5. **A readiness score per table**, from Airbnb. Shown on cards and ranked in `lint`. Done.
6. **Lineage export**, closing the loop for teams who maintain catalog lineage by hand: OpenLineage events or catalog YAML.
7. **Contracts** via ODCS, read by a connector and checked in `review`. Done.
8. **The lineage accuracy benchmark**, from Altimate, before claiming accuracy anywhere.
