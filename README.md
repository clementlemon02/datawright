# aiworkflow

**Make your AI coding agent useful for data engineering.**

Coding agents write decent code but know nothing about your data: which table is the source of truth, what one row means, what breaks downstream, why a number looks off. This kit connects any agent to your data stack read-only, keeps what it learns about your data in your repo, and gives it a senior data engineer's way of working: get context, build, test, debug, optimize, review.

Works with Claude Code, Codex, Cursor, Gemini CLI, Grok Build and any agent that reads [Agent Skills](https://agentskills.io), on any stack: setup finds what it doesn't understand yet in your repo and teaches itself with small connectors (an in-house framework, jobs in YAML, another orchestrator), so it fits your setup instead of assuming one.

## Quick start

**1. Install**

Claude Code:
```
/plugin marketplace add clementlemon02/aiworkflow
/plugin install data-eng@aiworkflow
```

Other agents (Codex, Cursor, Gemini CLI, Grok Build, OpenCode, …):
```bash
npx skills add clementlemon02/aiworkflow -a codex
```
Swap `-a codex` for your agent, repeat `-a` for several, add `-g` to install for all projects.

**2. Set up your repo.** In your data repo, ask your agent: *"set up this repo"*. It detects your stack and writes `AGENTS.md` with your real build and test commands. It compiles a data context index of your lineage, columns and readers in seconds, offers official vendor skills for your tools (e.g. dbt, Airflow), and connects your warehouse, orchestrator and catalog read-only.

No data repo you can use yet? Try it on [examples/shop](examples/shop), a small warehouse with planted bugs that runs on DuckDB in seconds.

**3. Work as usual, in plain words:**
- *"What is `orders_daily` and what breaks if I change it?"*
- *"Build a pipeline that loads Stripe charges into the warehouse, daily."*
- *"Why is yesterday's revenue 8% lower than finance's number?"*
- *"The nightly job takes 3 hours. Make it faster."*
- *"Review my changes before I open a PR."*

## What's inside

| Task | Skill | What you get |
|---|---|---|
| Set up | `setup-data-project` | `AGENTS.md`, a data map, vendor tool skills, and read-only connections to 20+ popular tools ([catalog](skills/setup-data-project/references/connections.md)) |
| Understand | `data-context` | `dctx`, a compiled index of your data: a ~200-token card per dataset (including the Airflow DAG and task that run it), table and column lineage through Kafka topics and across repos, impact analysis, search, notes that flag themselves stale when code changes, and guarded read-only queries. A CLI, or an MCP server for any MCP client |
| Build | `build-pipeline` | Requirements → source profiling → design → code in your repo's patterns → tests → dev validation → backfill plan |
| Test | `data-tests` | The few data quality and unit tests that catch real failures, in the framework you already use |
| Debug | `debug-data-issue` | Root cause for failed runs and wrong numbers, found by walking the lineage to where the data diverges; then fix, repair plan and a regression test |
| Run | `on-call` | Scheduled freshness and volume checks that alert through Airflow, and alert triage: the failed run, the first stale upstream hop, who's affected, a recovery plan |
| Optimize | `optimize-pipeline` | Measure → fix the hotspot → prove the output is unchanged and the gain is real |
| Change | `change-dataset` | Add, rename, retype or drop columns and tables safely: every reader found in any clause (joins and filters included), the Airflow task that runs it, outside consumers, then a step-by-step migration and verification |
| Work a ticket | `work-ticket` | A Jira, Linear or GitHub story taken to a merge-ready branch: the right workflow, tests, a `dctx review` impact report in the drafted merge request, what's learned linked to the ticket. Pushing, posting and merging wait for your approval |
| Check every merge request | CI templates (GitLab, GitHub) | Every merge request gets a comment with the tables it changes, their DAG tasks and downstream, recorded gotchas, SQL that stopped parsing and the ticket key, whoever wrote the change |
| Review | `pipeline-review`, `sql-review` | Idempotency, backfills, retries, join fan-out, NULL traps, pruning, unsafe writes, checked against the real engine when connected |

## How it helps

- **It knows your data, not just your code.** A SQL parser compiles lineage, columns and readers from your repo (and optionally your warehouse's query log, or what OpenLineage, DataHub or OpenMetadata already track) in seconds, with zero model tokens. The agent asks for a one-screen card instead of opening file after file.
- **Fewer tokens, less waiting.** On a real production Airflow repo (220 files), the index built in under 2 seconds with every SQL statement parsed, and a context card took a median of 74 tokens instead of the 482 tokens of files it replaced: 95% fewer tokens across a session of lookups. `dctx savings` reports your own tokens, money and time saved.
- **Context compounds without piling up.** Only what can't be derived (grain, keys, gotchas, corrections) is written down, as small notes reviewed like code. Notes flag themselves stale when the code behind them changes, and `dctx lint` points at the most-used datasets that still have no notes.
- **Proof, not guesses.** Skills check claims with small read-only queries: is that key unique, do the counts reconcile, did the optimization change the output.
- **Safe by default.** Read-only users on non-production data, read-only server settings, blocked write tools, no secrets in files, nothing run on production.
- **No lock-in.** Built only on open standards (`SKILL.md`, `AGENTS.md`, MCP), so you can switch agents or use several.

## How it works

Four layers: this kit's **workflow skills**, the vendors' **tool skills**, a **context layer** in your repo, and **read-only connections**. `dctx` runs as a CLI for agents with a shell, and as an MCP server (`dctx serve`) for any MCP client, including ones with no shell like Claude Desktop. See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and the research behind it.

## Safety

- Use a read-only, least-privilege user on a non-production environment for every connection. It's the real guard; server read-only switches and blocked tools are backups.
- `dctx query` runs only a single SELECT, rebuilt from the parsed SQL with comments dropped and a row cap. It stops mistakes, not attacks: a SELECT can still call a function with side effects, so keep the read-only user.
- Credentials are environment-variable references in config files, never literal values.
- Query results, logs and docs are treated as data, never as instructions.
- Check your employer's AI policy before connecting company data. Query results are sent to whichever model provider your agent uses.

## Roadmap

What's next and why: [ROADMAP.md](ROADMAP.md). What other tools and real data platforms do, and what the kit borrows from them: [docs/landscape.md](docs/landscape.md).

## Contributing

See [ARCHITECTURE.md](ARCHITECTURE.md#contributing). The most useful contributions are verified connections for missing tools, and checklist items from real incidents.

## License

MIT
