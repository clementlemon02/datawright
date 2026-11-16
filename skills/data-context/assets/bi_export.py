# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Fetch BI definitions into .data-context/imports/ for dctx: read-only, run from the repo root.

  uv run bi_export.py superset   # SUPERSET_URL, plus SUPERSET_ACCESS_TOKEN or SUPERSET_USERNAME + SUPERSET_PASSWORD
  uv run bi_export.py metabase   # METABASE_URL, plus METABASE_API_KEY (Admin > Authentication > API keys) or METABASE_SESSION

Superset: every dashboard's export ZIP (datasets, charts, dashboards; never data or database passwords).
Metabase: databases' table and field names, questions, models and dashboards. Neither fetches rows.
Use an account that can see the dashboards but not change them. Schedule it (cron, an Airflow task) to keep dctx current.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

OUT = Path(".data-context/imports")


def env(name, required=True):
    v = os.environ.get(name, "").strip()
    if required and not v:
        sys.exit(f"Set {name} in the environment (never in a file in the repo).")
    return v


def call(base, path, headers, data=None):
    req = urllib.request.Request(base.rstrip("/") + path, headers={"Accept": "application/json", **headers},
                                 data=json.dumps(data).encode() if data is not None else None)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        sys.exit(f"{path.split('?')[0]}: HTTP {e.code}")  # never the response: it can echo credentials
    return body if path.startswith("/api/v1/dashboard/export") else json.loads(body or b"null")


def superset():
    base = env("SUPERSET_URL")
    token = env("SUPERSET_ACCESS_TOKEN", required=False)
    if not token:
        login = {"username": env("SUPERSET_USERNAME"), "password": env("SUPERSET_PASSWORD"), "provider": "db", "refresh": False}
        token = call(base, "/api/v1/security/login", {}, login)["access_token"]
    auth = {"Authorization": f"Bearer {token}"}
    ids, page = [], 0
    while True:
        q = urllib.parse.quote(f"(columns:!(id),page:{page},page_size:100)")
        batch = [d["id"] for d in call(base, f"/api/v1/dashboard/?q={q}", auth).get("result") or []]
        ids += batch
        if len(batch) < 100:
            break
        page += 1
    if not ids:
        sys.exit("No dashboards visible to this account.")
    OUT.mkdir(parents=True, exist_ok=True)
    zipped = call(base, "/api/v1/dashboard/export/?q=" + urllib.parse.quote(f"!({','.join(map(str, ids))})"), auth)
    (OUT / "superset.zip").write_bytes(zipped)
    print(f"wrote {OUT / 'superset.zip'}: {len(ids)} dashboards with their charts and datasets; dctx build unpacks it")


def metabase():
    base = env("METABASE_URL")
    key, session = env("METABASE_API_KEY", required=False), env("METABASE_SESSION", required=False)
    if not (key or session):
        sys.exit("Set METABASE_API_KEY (or METABASE_SESSION) in the environment.")
    auth = {"x-api-key": key} if key else {"X-Metabase-Session": session}
    dbs = call(base, "/api/database", auth)
    dbs = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
    tables = []
    for db in dbs:
        meta = call(base, f"/api/database/{db['id']}/metadata", auth)
        tables += [{"id": t["id"], "db": db.get("name"), "schema": t.get("schema"), "name": t["name"],
                    "fields": [{"id": f["id"], "name": f["name"]} for f in t.get("fields") or []]} for t in meta.get("tables") or []]
    keep = ("id", "name", "type", "description", "archived", "database_id", "dataset_query", "collection_id")
    cards = [{k: c.get(k) for k in keep} | {"columns": [m.get("name") for m in c.get("result_metadata") or []]}
             for c in call(base, "/api/card", auth) if not c.get("archived")]
    dashboards = []
    for d in call(base, "/api/dashboard", auth):
        if d.get("archived"):
            continue
        full = call(base, f"/api/dashboard/{d['id']}", auth)
        dashcards = full.get("dashcards") or full.get("ordered_cards") or []
        dashboards.append({"id": d["id"], "name": d["name"], "description": d.get("description"),
                           "cards": sorted({dc["card_id"] for dc in dashcards if dc.get("card_id")})})
    out = OUT / "metabase"
    out.mkdir(parents=True, exist_ok=True)
    (out / "metabase.json").write_text(json.dumps({"tables": tables, "cards": cards, "dashboards": dashboards}, indent=1))
    print(f"wrote {out / 'metabase.json'}: {len(tables)} tables, {len(cards)} questions and models, {len(dashboards)} dashboards")


if __name__ == "__main__":
    {"superset": superset, "metabase": metabase}.get(sys.argv[1] if len(sys.argv) > 1 else "", lambda: sys.exit(__doc__))()
