#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["sqlglot>=30", "pyyaml>=6"]
# ///
# GENERATED from src/*.py by tools/build.py: edit the parts in src/, then run `python tools/build.py`.
"""datawright: compiled data context for AI agents.

Builds a local index of datasets, columns, lineage, readers and change history
from the repo's SQL, dbt manifests, PySpark calls, Airflow DAGs, Kafka/Flink/Spark streaming jobs (SQL, Java, Scala)
and optional warehouse and catalog exports,
using a SQL parser instead of a model. Then answers small, token-budgeted questions.

  build                                    compile .data-context/index.db (seconds, zero model tokens)
  init [--connectors a,b]                  first time in a repo: install datawright into .data-context/ and build (run the skill's copy)
  card <dataset> [--budget 800]            one-screen context for a dataset
  code <dataset>                           the code that produces it, for agents that can't open files
  impact <dataset[.column]>                everything a change would touch: writers, code reading it in any clause, downstream
  backfill <dataset> [--from D --to D]     the Airflow tasks to clear to refill it and everything downstream, in order; runs nothing
  advise [dataset]                         partition, cluster and distribution keys from how the code filters and joins it
  concepts [name] [--write]                what columns mean, from how code joins and renames them; --write: concepts.yml to review
  join <table> <table>                     how to join two tables, from the joins the code has written
  export <dbt|openmetadata|datahub> [--all] concepts as dbt entities or a catalog glossary, into .data-context/exports/
  lineage <dataset[.column]> [--up N] [--down N]   upstream / downstream tree (impact analysis)
  search <words...>                        find datasets, columns, notes and glossary terms
  note <dataset> "<text>" [--kind K] [--column C] [--source S]   record a gotcha or correction
  set <dataset> grain=... key=a,b owner=... description=...       record what a dataset is
  lint                                     stale or orphaned notes, undocumented hot datasets, parse failures
  savings [--days 30]                      tokens, money and time saved by answering from the index
  query "<select>" [--limit 100]           one read-only SELECT through query_command in config.yml
  check [dataset...] [--all]               freshness and volume through query_command; exits 1 on a problem (for scheduled alerts)
  update [--from PATH]                     replace this copy with the newest installed one
  coverage                                 what in this repo datawright doesn't read yet, and what to do about each gap
  review [--base B] [--strict] [--diff]    markdown impact report of this branch's changes, for merge requests and CI;
                                           --diff compares each changed table's data with its dev copy (dev_name)
  serve                                    MCP server on stdio: card, code, impact, lineage, search, note, set and query as tools

card, lineage, search, lint and savings need only the Python standard library.
"""
from __future__ import annotations

import argparse, ast, contextlib, csv, datetime, io, json, logging, os, re, shlex, sqlite3, subprocess, sys, time
from collections import Counter
from fnmatch import fnmatch
from pathlib import Path

logging.getLogger("sqlglot").setLevel(logging.CRITICAL)  # its warnings quote SQL, which can contain secrets
VERSION = "0.1.2"
SKIP = {".git", ".venv", "venv", "node_modules", "target", "dbt_packages", "dbt_modules", "__pycache__", ".data-context"}
SQL_START = re.compile(r"\s*(?:--[^\n]*\n\s*|/\*.*?\*/\s*|jinja_expr\s*)*(with|select|insert|merge|create|update)\b", re.I | re.S)  # dbt models open with {{ config(...) }}
KINDS = ("correction", "gotcha", "caveat", "decision")  # also the order notes appear in cards
RANK = {"source": 0, "warehouse": 1, "topic": 2, "jvm job": 2, "routine load": 2, "table": 2, "view": 2, "pyspark": 2, "dbt source": 3, "seed": 3, "snapshot": 3, "dbt model": 3}
DML = re.compile(r"\s*(?:--[^\n]*\n\s*)*(insert|merge|select|with|update|create\s+(?:or\s+replace\s+)?(?:table|view))\b", re.I)
DIALECTS = (None, "starrocks", "mysql", "spark", "hive", "databricks", "snowflake", "bigquery", "postgres", "redshift", "trino",
            "duckdb", "clickhouse", "tsql", "oracle")  # sqlglot names; None is generic SQL
TEST_FILE = re.compile(r"(^|/)(tests?|spec|__tests__)/|(^|/)test_[^/]*$|_test\.\w+$|Tests?\.(java|scala|kt)$")  # fixtures reuse real names
SIDE_JOB = re.compile(r"backfill|adhoc|ad_hoc|manual|migrat|scratch|tmp|temp|test|fix", re.I)  # writers that aren't the main producer
TODAY = datetime.date.today().isoformat()
ROOT = HOME = DB = Path()
LOCAL = {}  # connector-table names declared in the file being parsed: Flink jobs all call their tables src and sink

SCHEMA = """
create table datasets(name text primary key, kind text, file text, line int, description text, grain text,
  key text, owner text, verified text, rows int, uses int, schema_at text, freshness text, tier text);
create table tests(dataset text, source text);
create table contracts(dataset text, column_name text, type text, required int, source text);
create table columns(dataset text, name text, type text, description text, primary key(dataset, name));
create table edges(src text, dst text, via text, how text, primary key(src, dst));
create table col_edges(src text, dst text, primary key(src, dst));
create table readers(dataset text, file text, line int, primary key(dataset, file));
create table writers(dataset text, file text, line int, primary key(dataset, file));
create table modes(dataset text, file text, line int, mode text);
create table clauses(dataset text, column_name text, clause text, file text, line int);
create table layout(dataset text, kind text, columns text, file text, line int);
create table joins(left_dataset text, left_column text, right_dataset text, right_column text, file text, line int);
create table keys(dataset text, column_name text, source text);
create table concepts(concept text, dataset text, column_name text, role text, confirmed int, evidence int);
create table value_maps(dataset text, column_name text, value text, label text, how text, file text, line int);
create table conversions(src_dataset text, src_column text, dst_dataset text, dst_column text, kind text, detail text, file text, line int);
create table notes(dataset text, column_name text, date text, kind text, text text, source text, stale text);
create table changes(file text primary key, hash text, date text, subject text);
create table glossary(term text, meaning text, location text);
create table issues(file text, line int, error text);
create table tags(dataset text, tag text);
create table runs(file text, line_from int, line_to int, dag text, task text, schedule text, at text);
create table calls(dataset text, at text);
create table col_uses(dataset text, column_name text, file text, line int, primary key(dataset, column_name, file, line));
create table files(path text primary key, reader text);
create table meta(k text primary key, v text);
"""


def run(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        return ""


