# Shop demo

A small online shop's warehouse to try the kit on, without touching real data. One month of orders, payments, refunds and customers, loaded from a source system into staging and marts on DuckDB, scheduled by an Airflow DAG and read by a weekly report. It has bugs in it, like a real one.

## Try it

You need [uv](https://docs.astral.sh/uv/). Copy the demo out so it's its own git repo, then build the warehouse (a few seconds):

```bash
cp -r examples/shop ~/shop-demo && cd ~/shop-demo
git init -q && git add -A && git commit -qm "shop demo"
uv run run.py
```

Open your agent in `~/shop-demo` and say *"set up this repo"*. When it asks how to query the warehouse, the read-only query command is `uv run -q sql.py {sql}`.

## Things to ask

- *"Finance says October net revenue was $284,732.08, but mart.revenue_daily adds up to $326,960. Which number is right, and why do they differ?"* There's more than one cause. The answer key is in `tests/test_demo.py` in the aiworkflow repo; don't show it to the agent.
- *"Fix revenue_daily and add a test so it can't happen again."*
- *"What breaks if I change staging.orders?"*
- *"Review models/marts/revenue_daily.sql before I merge it."*

`uv run run.py` rebuilds everything from scratch, so you can try again after the agent changes things.

## What's in it

| Path | What |
|---|---|
| `source_system/seed.sql` | The shop's production database (generated, so the numbers never change) |
| `models/staging/`, `models/marts/` | SQL models, run by `run.py` in that order |
| `dags/shop_daily.py` | The Airflow DAG that would schedule them (Airflow isn't needed to try the demo) |
| `reports/weekly_kpis.py` | A report that reads `mart.revenue_daily` |
| `sql.py` | Runs one read-only query and prints CSV |
