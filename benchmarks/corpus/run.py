# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Regression corpus: public data repos at pinned commits, built with dctx and compared with expected.json.

uv run run.py [--only jaffle-shop,balboa] [--update] [--all] [--cache DIR]
Any change in what dctx finds (datasets, lineage edges, column edges, readers, unparsed statements, dialect, concepts,
best-practice findings) shows as a diff: review it, and --update when the change is intended. --all adds the two large
repos (minutes). Repos are fetched once into the cache; nothing in them is executed."""
import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
KIT = HERE.parents[1] / "skills/data-context"
REPOS = [  # (name, GitHub repo, commit, connectors, large?)
    ("jaffle-shop", "dbt-labs/jaffle-shop", "5beb145b00f5465ec759cfcdd9745e858818cf95", ["dbt_yaml"], False),
    ("balboa", "datacoves/balboa", "2148844581d51207ac269b1154627282b8ea1201", ["dbt_yaml"], False),
    ("hooli", "dagster-io/hooli-data-eng-pipelines", "a039747bed3fe573493900d41d6659d01b7e9034", ["dagster", "dbt_yaml"], False),
    ("e2e-pipeline", "hoangsonww/End-to-End-Data-Pipeline", "8b0976c74f484544648d9483a098ea4738defda3", [], False),
    ("healthcare-looker", "looker-open-source/healthcare_demo", "eeb03ac61c87bab65a9b6cfeb76915f38eebfe56", ["lookml"], False),
    ("cal-itp", "cal-itp/data-infra", "c9e3d487a750e1c647d82545f4f993734dab8879", ["dbt_yaml"], False),
    ("bigquery-etl", "mozilla/bigquery-etl", "c299f52544e54a808d102d26c19e8cb6e26820d4", [], True),
    ("spellbook", "duneanalytics/spellbook", "e5f9ff909fb2ca8d0e33f3f0b7035d4d567e7901", [], True),
]
git = lambda *a, cwd=None: subprocess.run(["git", *a], cwd=cwd, capture_output=True, text=True)


def fetch(cache, name, repo, sha):
    d = cache / name
    if git("rev-parse", "HEAD", cwd=d).stdout.strip() == sha if d.exists() else False:
        return d
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    git("init", "-q", cwd=d)
    git("remote", "add", "origin", f"https://github.com/{repo}.git", cwd=d)
    r = git("fetch", "-q", "--depth", "1", "origin", sha, cwd=d)
    if r.returncode or git("checkout", "-q", "FETCH_HEAD", cwd=d).returncode:
        sys.exit(f"could not fetch {repo}@{sha[:8]}: {r.stderr.strip()[-300:]}")
    return d


def measure(d, connectors):
    home = d / ".data-context"
    shutil.rmtree(home, ignore_errors=True)
    (home / "connectors").mkdir(parents=True)
    shutil.copy(KIT / "scripts/dctx.py", home / "dctx.py")
    for c in connectors:
        shutil.copy(KIT / f"assets/connectors/{c}.py", home / "connectors")
    t = time.time()
    r = subprocess.run(["uv", "run", "-q", ".data-context/dctx.py", "build"], cwd=d, capture_output=True, text=True)
    if r.returncode:
        return {"error": r.stderr.strip().splitlines()[-1][:200] if r.stderr.strip() else "build failed"}, 0
    con = sqlite3.connect(home / "index.db")
    one = lambda sql: con.execute(sql).fetchone()[0]
    lint = subprocess.run(["uv", "run", "-q", ".data-context/dctx.py", "lint"], cwd=d, capture_output=True, text=True).stdout
    found = next((l.split(":", 1)[1].strip() for l in lint.splitlines() if l.startswith("best practices:")), "")
    out = {"datasets": one("select count(*) from datasets"), "edges": one("select count(*) from edges"), "column_edges": one("select count(*) from col_edges"),
           "readers": one("select count(*) from readers"), "runs": one("select count(*) from runs"), "unparsed": one("select count(*) from issues"),
           "dialect": one("select v from meta where k='dialect'"), "joins": one("select count(*) from joins"),
           "concepts": one("select count(distinct concept) from concepts"), "value_maps": one("select count(distinct dataset || column_name) from value_maps"),
           "best_practices": found}
    shutil.rmtree(home, ignore_errors=True)
    return out, time.time() - t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only")
    ap.add_argument("--update", action="store_true", help="write what dctx finds now as the expected result")
    ap.add_argument("--all", action="store_true", help="include the large repos")
    ap.add_argument("--cache", default=str(Path.home() / ".cache/aiworkflow-corpus"))
    a = ap.parse_args()
    path = HERE / "expected.json"
    expected = json.loads(path.read_text()) if path.exists() else {}
    picked = [r for r in REPOS if (r[0] in a.only.split(",") if a.only else a.all or not r[4])]
    changed = 0
    for name, repo, sha, connectors, _ in picked:
        got, secs = measure(fetch(Path(a.cache), name, repo, sha), connectors)
        want = expected.get(name)
        diff = {k: ((want or {}).get(k), v) for k, v in got.items() if (want or {}).get(k) != v}
        print(f"{name:18} {secs:5.1f}s  " + ("ok" if not diff else "NEW" if want is None else "CHANGED " + ", ".join(f"{k}: {b} → {n}" for k, (b, n) in diff.items()))
              + f"  ({got.get('datasets')} datasets, {got.get('edges')} edges, {got.get('dialect')})", flush=True)
        changed += bool(diff)
        if a.update:
            expected[name] = got
    if a.update:
        path.write_text(json.dumps(expected, indent=1, sort_keys=True) + "\n")
        print(f"wrote {path}")
    elif changed:
        sys.exit(f"{changed} repos differ from expected.json: review the changes; if intended, rerun with --update")


if __name__ == "__main__":
    main()
