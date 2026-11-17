# Datawright

**Know what breaks before you merge.** Datawright reads your data team's code and tells your AI agent and your reviewers what it can't see: lineage, join paths and what columns mean.

<p align="center"><img src="docs/demo.svg" alt="dctx impact, join and card output on the shop example"></p>

Agents and reviewers guess which tables a change breaks, how two tables join, and what `status = 2` means. Datawright answers from your repo: no warehouse access, no model tokens, built in seconds.

- **A data impact report on every merge request:** changed tables, the DAG tasks that run them, downstream tables and dashboards, and unsafe patterns like a rerun that duplicates rows.
- **Join paths and meanings for agents:** `dctx join a b` writes the join your code already uses; cards explain codes (`2 = approved`) and flag stale key columns.
- **11 skills** that make your agent work like a senior data engineer: build, change, debug, test, review, optimize, on-call.

Reads SQL, dbt, Airflow, Spark, Flink, Java services, Looker, Superset and Metabase. Works with Claude Code, Codex, Cursor, Gemini CLI and any agent that reads [Agent Skills](https://agentskills.io).

```
| Changed table | Code | Runs in | Downstream |
| mart.revenue_daily (tier 0) | pipelines/revenue.sql | etl.build_revenue @daily | bi.revenue_dashboard, +9 more |
Dashboards reached (3) · Contract broken (1) · rerun duplicates rows: appends with nothing clearing the slice first
```

## Install

You need [uv](https://docs.astral.sh/uv/) and git.

```
/plugin marketplace add clementlemon02/datawright     # Claude Code
/plugin install data-eng@datawright
```

```bash
npx skills add clementlemon02/datawright -a codex     # Codex, Cursor, Gemini CLI, … (swap -a)
```

Then, in your data repo, tell your agent: **"set up this repo"**. Ask things like *"what breaks if I change `orders_daily`?"* or *"review my changes before I open a PR"*.

No data repo handy? Try [examples/shop](examples/shop), a small warehouse with planted bugs. Tried it? [Ten minutes and five answers](docs/try-it.md) help more than anything.

## How well does it work?

Table lineage versus dbt's own manifest on public repos: **100% precision, 96–100% recall.** On a hard synthetic benchmark, Sonnet answered 12 of 12 questions with it and 7 of 12 without. The benchmark is small and home-made: [details and limits](docs/semantics.md#results-so-far-the-join-path-benchmark).

## More

[Skills, every command, safety rules](docs/reference.md) · [Architecture](ARCHITECTURE.md) · [Roadmap](ROADMAP.md) · [Landscape](docs/landscape.md) · [Contributing](ARCHITECTURE.md#contributing) · MIT license
