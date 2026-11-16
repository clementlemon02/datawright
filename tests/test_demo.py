# /// script
# requires-python = ">=3.10"
# dependencies = ["sqlglot>=30", "pyyaml>=6", "duckdb>=1.1"]
# ///
"""Self-check for examples/shop: it builds, dctx indexes all of it, and the planted bugs are there. Run: uv run tests/test_demo.py

Answer key (keep it out of the demo, where agents would read it): mart.revenue_daily overstates revenue because
1. it joins staging.payments, and every tenth order is paid in two installments, so those orders count twice;
2. refunds (staging.refunds) are never subtracted.
"""
import shutil, subprocess, sys, tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).parents[1]


def main():
    demo = Path(tempfile.mkdtemp()) / "shop"
    shutil.copytree(ROOT / "examples/shop", demo, ignore=shutil.ignore_patterns("shop.duckdb", ".data-context"))
    sh = lambda *a: subprocess.run(a, cwd=demo, check=True, capture_output=True, text=True).stdout
    sh("git", "init", "-q")
    sh("git", "add", "-A")
    sh("git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
    sh(sys.executable, "run.py")
    (demo / ".data-context").mkdir()
    shutil.copy(ROOT / "skills/data-context/scripts/dctx.py", demo / ".data-context")
    (demo / ".data-context/config.yml").write_text(f"dialect: duckdb\nquery_command: {sys.executable} sql.py {{sql}}\n")
    dctx = lambda *a: sh(sys.executable, ".data-context/dctx.py", *a)

    built = dctx("build")
    print(built)
    assert "unparsed" not in built, built
    card = dctx("card", "revenue_daily")
    for text in ("produced by models/marts/revenue_daily.sql:2", "staging.orders", "staging.payments",
                 "reports/weekly_kpis.py", "dags/shop_daily.py", "revenue ← staging.orders.amount"):
        assert text in card, f"card is missing {text!r}:\n{card}"
    assert "raw.refunds" in dctx("lineage", "raw.refunds", "--down", "9") and "mart.customer_ltv" in dctx("lineage", "raw.refunds", "--down", "9")
    assert dctx("query", "select count(*) as days from mart.revenue_daily").split() == ["days", "31"]

    con = duckdb.connect(str(demo / "shop.duckdb"), read_only=True)
    one = lambda sql: con.execute(sql).fetchone()[0]
    mart, gross, refunds = one("select sum(revenue) from mart.revenue_daily"), one("select sum(amount) from staging.orders"), one("select sum(amount) from staging.refunds")
    doubled = one("select sum(amount) from staging.orders where order_id in (select order_id from staging.payments where installment = 2)")
    assert mart == gross + doubled and refunds > 0, (mart, gross, doubled, refunds)
    print(f"mart {mart} = gross {gross} + double-counted {doubled}; true net is {gross - refunds} ({mart / (gross - refunds) - 1:.0%} over)")

    # monitoring: the data ends in October, so the daily mart is stale; a planted drop in one day's orders shows as LOW
    check = lambda *a: subprocess.run([sys.executable, ".data-context/dctx.py", "check", *a], cwd=demo, capture_output=True, text=True)
    con.close()
    dctx("set", "staging.orders", "freshness=ordered_at 3650d")
    plan = check("--all", "--plan")
    assert plan.returncode == 0 and "PLAN   mart.revenue_daily  order_date (guessed), alert after 48.5 h (from the schedule)" in plan.stdout, plan.stdout
    out = check("--all")
    assert out.returncode == 1 and "STALE  mart.revenue_daily" in out.stdout and "alert after 48.5 h" in out.stdout, out.stdout + out.stderr
    dctx("set", "mart.revenue_daily", "tier=0")  # critical: late by one run is already an incident
    assert "PLAN   mart.revenue_daily  order_date (guessed), alert after 24.2 h" in check("mart.revenue_daily", "--plan").stdout
    assert "OK     staging.orders" in out.stdout and "SKIP   mart.customer_ltv" in out.stdout, out.stdout
    with duckdb.connect(str(demo / "shop.duckdb")) as w:  # lose most of the last day's orders, as a broken load would
        w.execute("delete from staging.orders where cast(ordered_at as date) = date '2026-10-31' and order_id % 10 != 0")
    out = check("staging.orders")
    assert out.returncode == 1 and out.stdout.startswith("LOW    staging.orders"), out.stdout + out.stderr
    print(out.stdout)

    plan = dctx("backfill", "staging.orders", "--from", "2026-10-28", "--to", "2026-10-31")
    assert "airflow tasks clear shop_daily -t '^stg_orders$' -s 2026-10-28 -e 2026-10-31 -y" in plan, plan
    assert "wave 2 (after the wave before has finished)" in plan and "airflow tasks clear shop_daily -t '^(customer_ltv|revenue_daily)$'" in plan, plan

    # data diff in review: fix the double counting on a branch, build the dev copy, and compare it with prod
    model = demo / "models/marts/revenue_daily.sql"
    model.write_text(model.read_text().replace("join staging.payments p on p.order_id = o.order_id",
                                               "where o.order_id in (select order_id from staging.payments)").replace("count(*) as orders", "count(*) as orders,\n       0 as refunds"))
    with duckdb.connect(str(demo / "shop.duckdb")) as w:
        w.execute("create schema mart_dev; " + model.read_text().replace("mart.revenue_daily", "mart_dev.revenue_daily"))
    dctx("build")
    dctx("set", "mart.revenue_daily", "key=order_date")
    review = lambda *a: subprocess.run([sys.executable, ".data-context/dctx.py", "review", "--base", "HEAD", *a], cwd=demo, capture_output=True, text=True)
    assert "set `dev_name`" in review("--diff").stdout
    (demo / ".data-context/config.yml").write_text((demo / ".data-context/config.yml").read_text() + "dev_name: {schema}_dev.{table}\n")
    out = review("--diff").stdout
    print(out)
    row = next(l for l in out.splitlines() if l.startswith("| `mart.revenue_daily` → `mart_dev.revenue_daily`"))
    assert "| 31 → 31 | +refunds | 0 → 0 | revenue -" in row and "orders -" in row, row
    assert "the data diff ran read-only SELECTs" in out
    with duckdb.connect(str(demo / "shop.duckdb")) as w:  # a join that fans out: the key is no longer unique
        w.execute("insert into mart_dev.revenue_daily select * from mart_dev.revenue_daily where order_date = date '2026-10-30'")
    out = review("--diff", "--where", "order_date >= date '2026-10-25'").stdout
    assert "| 7 → 8 (+14.3%) | +refunds | 0 → 1 **new duplicates** |" in out and "where `order_date >= date '2026-10-25'`" in out, out
    print("demo self-check passed")


if __name__ == "__main__":
    main()
