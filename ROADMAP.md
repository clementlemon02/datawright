# Roadmap

What's open, why it matters, and what "done" means. Once the repo is on GitHub, each item becomes an issue. Ordered by priority within each section.

## Next

| Item | Why | Done when |
|---|---|---|
| Monitoring in production | `datawright check` and `on-call` are tested on the demo, not yet on a real warehouse schedule | The checks DAG running a week on a real repo, with false alerts tuned away |
| Merge request check on a real project | `datawright review` and the CI templates are tested on the demo and test repos, not yet on a GitLab project | The GitLab job posting on a real merge request, with the token set up; optional AI review (`pipeline-review`) as a second job |
| ADE-bench, with and without the kit | The headline claim for launch: agents finish real data tasks better | Success rate, tokens and time on ADE-bench, both ways, in ARCHITECTURE.md |
| Publish | | README stats reviewed for employer details, GitHub Actions running the four self-checks (tests/), a version tag, repo public on `clementlemon02/datawright` |

## Ideas from other tools and real platforms

From [docs/landscape.md](docs/landscape.md): what Uber, Airbnb, Monzo and tools like Recce, SQLMesh and ODCS do that the kit doesn't, in order.

| Item | Borrowed from | Done when |
|---|---|---|
| Breaking-change labels by column | SQLMesh | `review` labels each changed SQL file breaking, non-breaking or unchanged (from the old and new query); next: per-column labels for Python and dbt-versioned models, and what to backfill ranked by downstream tier |
| More BI tools | Monzo | Tableau (Metadata API) and Power BI (scanner API) through `bi_export.py`; Looker UI dashboards from its API; dashboard view counts so `review` ranks dashboards by use |
| Data diff by value | Recce, Datafold | `review --diff` compares rows, columns, keys and sums; next, which keys' values changed, and histograms for the columns that moved |
| Lineage export | OpenLineage, catalogs | datawright's parsed lineage written as OpenLineage events or catalog lineage YAML, replacing hand-written files |
| Contract types and quality rules | ODCS (datacontract-cli) | Contracts check required columns today; next, promised types against the code's, and quality rules turned into `data-tests` |

## Meaning (docs/semantics.md)

| Item | Why | Done when |
|---|---|---|
| Lineage accuracy beyond dbt | `benchmarks/lineage`: against dbt's manifest, precision 100% on three projects; recall 100% (jaffle-shop), 100% (balboa), 99.5% (cal-itp, 1,198 edges; was 78.3% before two fixes). Column lineage and non-dbt repos have no answer key yet | A hand-checked column-lineage key for one real project; an Airflow + SQL repo checked against its warehouse's query log |
| Loops over computed lists | The last lineage misses without a manifest (6 of 1,198 on cal-itp): `source()` names built in a Jinja loop over a list that isn't written out in the file. Literal and `set` lists and model versions are done | Read from the model's YAML or macro; coverage says when `dbt parse` would add edges |
| Small models reaching for datawright | Haiku used datawright in 9 of 12 hard-suite runs after firmer instructions (6 before) and got 8 right (7 before): within noise | An MCP-first setup measured on the hard suite with more repeats |
| First hour with real users | A fresh agent now completes setup and a first question on jaffle-shop in under 3 minutes ($0.82); nobody outside has tried it | Three teams' answers to docs/try-it.md, and their fixes |
| Meaning benchmark on a real repo | `benchmarks/meaning`: easy suite, all right either way (datawright 18% cheaper); hard suite (multi-hop chains, piecemeal codes, a stale trap column, 1,500 files): Sonnet 7/12 → 12/12, Haiku 3/12 → 7/12 with datawright. Synthetic, 12 runs a cell | A real repo with questions and answers checked by its engineers, more repeats, other agents |

## Streaming and lineage follow-ups

| Item | Why | Done when |
|---|---|---|
| Real topic names from Helm values | Topics set by env vars resolve from deploy files in `repos:`, `env:` in config.yml or connector `env` records (and coverage counts the rest, and values no code reads); Helm templates with `{{ .Values }}` aren't expanded | A Helm chart's values.yaml and templates resolve them; tested on a public chart |
| DataWorks schedules | Alibaba's scheduler keeps DAGs in the cloud, so MaxCompute jobs show no "runs in" | An export through the DataWorks API, read by a connector, like `bi_export.py` |
| Column lineage for Flink connector tables | Flink `WITH (...)` DDL doesn't parse, so those tables lose column lineage | Strip the WITH clause before parsing; unparsed count on the Flink repo drops to near 0 |
| OpenLineage job names as "runs in" | Events name the Airflow DAG and task for jobs outside the repo | Cards show them, tagged `(openlineage)` |
| Marquez lineage graph format | Only raw events are read | `/api/v1/lineage` responses import too |
| More ready-made connectors | Read now: notebooks (.ipynb), dlt, Prefect and Python Kafka clients. Coverage still flags Luigi, Beam, SQLMesh, Great Expectations, Soda and Kestra, but has no reader for them (Dagster has one) | SQLMesh and Luigi next, each tested on a public repo; Prefect schedules from prefect.yaml; Sling replication YAML and Dagster components for the gaps they leave |

## Context layer

| Item | Why | Done when |
|---|---|---|
| Ticket-driven work, end to end | First real run (Jira, read-only) found the open-ticket query, title-only tickets, work filed under a typo'd key, and non-data tickets; all now in the skill | One ticket taken from read to an approved comment and merged change |
| Incremental build | Full rebuild every time; 11 s at 1,500 models is fine, 10k+ isn't | Parses cached by file hash; rebuild after one changed file under a second |

## Later, if users ask

- HTTP transport for `datawright serve` (web chat apps); needs auth first, since `note` and `set` write to the repo
- A thin data-engineering agent on the Claude Agent SDK: enforced guardrails, automatic context, exact cost tracking
- PII classification and access review
- Semantic search beyond names and the glossary

## Done (see ARCHITECTURE.md for results)

Under git, with CI (four self-checks and a corpus of six public repos at pinned commits); `datawright init` installs and builds in one command, and setup delivers before it asks; lineage accuracy measured against dbt manifests, with refs through macro arguments and macro bodies; `unused keys` on cards. Concept exports to dbt semantic models, OpenMetadata and DataHub glossaries, and dbt entities read back as confirmed concepts. Value maps and conversions on cards, from `CASE`, `DECODE`, conditional-aggregate aliases and unit, epoch and time-zone expressions, with a check for codes labelled two ways. Concept drift in `lint` and `review` (types, respelled and unlisted names, roles in `concepts.yml`); a MyBatis mapper connector. Concepts from usage (docs/semantics.md, step 1): join pairs, pass-through renames and declared keys grouped into concepts with a home and canonical name; `joins` and `means` on cards; `datawright join` for join paths (CLI and MCP); `concepts.yml` proposals reviewed and confirmed by people. `datawright advise`: partition, clustering and distribution keys from how code filters, joins and groups each table, against layout from DDL, dbt config or connectors, in each engine's syntax (CLI and MCP). `datawright backfill`: the Airflow tasks to clear for a table and everything downstream, in waves, with append and Kafka warnings (CLI and MCP). Best-practice checks from the index in `lint` and `review`: appends that a rerun duplicates (with key tables and dbt `unique_key` understood), several writers, `SELECT *` writes, lineage cycles, schedule order, layer order and committed secrets, checked for noise on eight public repos. Object storage paths (S3, OSS, GCS, ADLS, HDFS) as datasets from Spark, pandas, Flink and SQL, with tables created at a path joined to it, buckets from environment variables joined to the one real bucket with that path, and date or run folders folded in; DAG task arguments read into the scripts they run (`sys.argv`, `argparse` and `click` with defaults, function parameters, including calls through chains of files), so a shared loader gives one lineage and one task per table; functions called with different values, loops over literal lists and DAG factories looping over a repo's JSON or YAML config read once per call, iteration or row, each with its own lineage; Iceberg `writeTo`; Doris and MaxCompute detected from a repo's dependencies and config; deploy-time variables resolved from `env:` or connectors, with coverage flagging values no code reads. Tiers (Uber) that tighten `check` limits and flag critical tables in `review`; a readiness score per table (Airbnb) on cards and ranked in `lint`, counting dbt tests; data contracts through an ODCS connector, with broken ones on cards, in `lint` and blocking `review --strict`; BI lineage through LookML, Superset and Metabase connectors (dashboards down to the warehouse column each tile shows), with `bi_export.py` fetching Superset and Metabase definitions read-only, coverage pointing to it, and dashboards reached in `review`; `review --diff`, comparing each changed table with its dev copy (rows, columns, duplicate keys, sums). Workflow skills; dbt selectors in DAG tasks (names, tags, sources, paths, graph operators, exclusions; tags from SQL, YAML, dbt_project.yml or the manifest); dialect detection that ignores unparseable statements and trusts the engine a repo declares (dbt adapters, `.hql`) unless parsing clearly disagrees; connectors and `datawright coverage` for learning any stack (ready-made: lineage YAML, Kafka Connect, dbt schema YAML, Dagster), checked on four public repos; the ticket loop (`work-ticket` → `datawright review` → drafted merge request) with CI templates for GitLab and GitHub; `datawright update`; freshness and volume checks with `datawright check` and the `on-call` skill; `datawright` with cards, column lineage, `impact`, savings tracking, notes that go stale; MCP server with a guarded `query` tool; the shop demo; Airflow DAG, task and schedule linking; OpenLineage, DataHub and OpenMetadata imports; `change-dataset`; Kafka lineage across repos through Flink SQL, Java/Scala jobs, Routine Load and Spark streaming.
