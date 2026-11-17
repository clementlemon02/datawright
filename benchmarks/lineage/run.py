# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Table-lineage accuracy against dbt's own answer: precision and recall of datawright's edges between a dbt project's models,
seeds, snapshots and sources, with the manifest hidden from datawright.

uv run run.py <dbt project dir> [--adapter duckdb] [--manifest target/manifest.json]
Without --manifest it runs `dbt deps` and `dbt parse` (no warehouse needed) through uvx with dbt-<adapter>.
dbt resolves every ref() and source() itself, so its parent map is independent of how datawright reads the SQL."""
import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

DCTX = Path(__file__).parents[2] / "skills/data-context/scripts/datawright.py"


def manifest_for(project, adapter):
    work = Path(tempfile.mkdtemp()) / "p"
    shutil.copytree(project, work, ignore=shutil.ignore_patterns("target", ".data-context", ".git", "dbt_packages", "logs"))
    out = {"type": adapter, **({"path": "x.duckdb"} if adapter == "duckdb" else
                               {"method": "oauth", "project": "p", "dataset": "d"} if adapter == "bigquery" else
                               {"account": "a", "user": "u", "password": "p", "database": "d", "warehouse": "w", "schema": "s"} if adapter == "snowflake" else
                               {"host": "h", "user": "u", "password": "p", "port": 5432, "dbname": "d", "schema": "s"})}  # placeholders: parse never connects
    (work / "profiles.yml").write_text(json.dumps({"bench": {"target": "dev", "outputs": {"dev": out}}}))  # --profile overrides the project's
    dbt = ["uvx", "-q", "--from", "dbt-core", "--with", f"dbt-{adapter}", "dbt"]
    for cmd in (["deps"], ["parse"]):
        r = subprocess.run([*dbt, "--no-version-check", *cmd, "--profiles-dir", ".", "--profile", "bench"], cwd=work, capture_output=True, text=True)
        if r.returncode:
            sys.exit(f"dbt {cmd[0]} failed:\n{(r.stdout + r.stderr)[-1500:]}")
    return work / "target/manifest.json"


def truth(manifest):
    m = json.loads(Path(manifest).read_text())
    project = m["metadata"].get("project_name")
    name = {}
    for uid, n in {**m["nodes"], **m["sources"]}.items():
        if n["resource_type"] in ("model", "seed", "snapshot") and n.get("package_name") == project:
            name[uid] = n["name"].lower()
        elif n["resource_type"] == "source" and n.get("package_name") == project:
            name[uid] = f"{n['source_name']}.{n['name']}".lower()
    kind = {uid: (m["nodes"].get(uid) or m["sources"][uid])["resource_type"] for uid in name}
    edges = {(name[p], name[c]) for c, ps in m["parent_map"].items() if c in name and kind[c] in ("model", "snapshot") for p in ps if p in name}
    return edges, set(name.values())


def datawright_edges(project):
    work = Path(tempfile.mkdtemp()) / "p"
    shutil.copytree(project, work, ignore=shutil.ignore_patterns("target", ".data-context", ".git", "dbt_packages", "logs"))  # no manifest: SQL only
    subprocess.run(["git", "init", "-q"], cwd=work, check=True)
    (work / ".data-context").mkdir()
    shutil.copy(DCTX, work / ".data-context/datawright.py")
    r = subprocess.run(["uv", "run", "-q", ".data-context/datawright.py", "build"], cwd=work, capture_output=True, text=True)
    if r.returncode:
        sys.exit(r.stderr[-800:])
    con = sqlite3.connect(work / ".data-context/index.db")
    return {(s.lower(), d.lower()) for s, d in con.execute("select src, dst from edges where src != dst")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("project")
    ap.add_argument("--adapter", default="duckdb")
    ap.add_argument("--manifest")
    a = ap.parse_args()
    project = Path(a.project).resolve()
    want, nodes = truth(a.manifest or manifest_for(project, a.adapter))
    short = lambda n: n if n in nodes else next((x for x in (".".join(n.split(".")[-k:]) for k in (2, 1)) if x in nodes), n)  # db.schema.model -> model
    got = {(short(s), short(d)) for s, d in datawright_edges(project)}
    got = {e for e in got if e[0] in nodes and e[1] in nodes}
    hit = got & want
    print(f"{project.name}: {len(nodes)} nodes, {len(want)} true edges · datawright found {len(got)}: precision {len(hit) / max(len(got), 1):.1%}, recall {len(hit) / max(len(want), 1):.1%}")
    for label, es in (("missed", want - got), ("extra", got - want)):
        if es:
            print(f"  {label} ({len(es)}): " + ", ".join(f"{s} → {d}" for s, d in sorted(es)[:8]) + (" …" if len(es) > 8 else ""))
    return len(hit), len(got), len(want)


if __name__ == "__main__":
    main()
