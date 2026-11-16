# /// script
# requires-python = ">=3.10"
# dependencies = ["sqlglot>=30", "pyyaml>=6"]
# ///
"""Self-check for bi_export.py against a stand-in Superset and Metabase API, then dctx reading what it wrote."""
import io
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).parents[1]
SKILL = ROOT / "skills/data-context"
seen = []


def export_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("dashboard_export_1/datasets/wh/orders.yaml", "table_name: orders\nschema: mart\nuuid: d1\ndatabase_uuid: x\n"
                   "columns:\n- {column_name: amount}\n")
        z.writestr("dashboard_export_1/charts/Sales_1.yaml", "slice_name: Sales\nuuid: c1\ndataset_uuid: d1\nparams: {metrics: [{expressionType: SIMPLE, "
                   "aggregate: SUM, column: {column_name: amount}}]}\n")
        z.writestr("dashboard_export_1/dashboards/Ops_1.yaml", "dashboard_title: Ops\nuuid: b1\nposition: {CHART-1: {type: CHART, meta: {uuid: c1}}}\n")
    return buf.getvalue()


class API(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        seen.append(("POST", self.path))
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert body["username"] == "viewer" and body["provider"] == "db"
        self.reply({"access_token": "tok"})

    def do_GET(self):
        seen.append(("GET", self.path, self.headers.get("Authorization") or self.headers.get("x-api-key")))
        p = self.path
        if p.startswith("/api/v1/dashboard/export"):
            return self.reply(export_zip(), "application/zip")
        if p.startswith("/api/v1/dashboard/"):  # two pages: 100 dashboards, then 1
            return self.reply({"result": [{"id": i} for i in (range(1, 101) if "page%3A0" in p else [101])]})
        routes = {"/api/database": {"data": [{"id": 1, "name": "wh"}]},
                  "/api/database/1/metadata": {"tables": [{"id": 7, "schema": "mart", "name": "orders", "fields": [{"id": 70, "name": "amount"}]}]},
                  "/api/card": [{"id": 1, "name": "Order total", "type": "question", "archived": False, "result_metadata": [{"name": "sum"}],
                                 "dataset_query": {"type": "query", "query": {"source-table": 7, "aggregation": [["sum", ["field", 70, None]]]}}}],
                  "/api/dashboard": [{"id": 5, "name": "Ops"}, {"id": 6, "name": "Gone", "archived": True}],
                  "/api/dashboard/5": {"dashcards": [{"card_id": 1}, {"card_id": None}]}}
        self.reply(routes[p])


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), API)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    repo = Path(tempfile.mkdtemp()) / "repo"
    (repo / ".data-context/connectors").mkdir(parents=True)
    for c in ("superset.py", "metabase.py"):
        shutil.copy(SKILL / "assets/connectors" / c, repo / ".data-context/connectors")
    (repo / "orders.sql").write_text("create table mart.orders (amount decimal(10, 2));\n")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    run = lambda *a, **env: subprocess.run([sys.executable, *a], cwd=repo, capture_output=True, text=True, env={"PATH": "/usr/bin:/bin", **env})

    out = run(str(SKILL / "assets/bi_export.py"), "superset", SUPERSET_URL=url, SUPERSET_USERNAME="viewer", SUPERSET_PASSWORD="pw-123")
    assert out.returncode == 0 and "101 dashboards" in out.stdout and "pw-123" not in out.stdout + out.stderr, out.stdout + out.stderr
    assert ("POST", "/api/v1/security/login") in seen and any(s[0] == "GET" and "export" in s[1] and s[2] == "Bearer tok" for s in seen)
    out = run(str(SKILL / "assets/bi_export.py"), "metabase", METABASE_URL=url)
    assert out.returncode == 1 and "METABASE_API_KEY" in out.stderr, "no credentials: says which variable to set"
    out = run(str(SKILL / "assets/bi_export.py"), "metabase", METABASE_URL=url, METABASE_API_KEY="mb-key")
    assert out.returncode == 0 and "1 tables, 1 questions and models, 1 dashboards" in out.stdout, out.stdout + out.stderr
    assert all(s[2] == "mb-key" for s in seen if s[1].startswith("/api/") and not s[1].startswith("/api/v1"))

    shutil.copy(SKILL / "scripts/dctx.py", repo / ".data-context")
    (repo / "docker/superset_config.py").parent.mkdir()
    (repo / "docker/superset_config.py").write_text("ROW_LIMIT = 5000\n")
    (repo / "Dockerfile").write_text("FROM apache/superset:6.1.0\n")
    hidden = Path(tempfile.mkdtemp()) / "parked"  # coverage before any export: it says how to bring the dashboards in
    shutil.move(repo / ".data-context/imports", hidden)
    run(".data-context/dctx.py", "build")
    assert "Superset is used here, but its dashboards aren't in the index: add the superset connector (`dctx init --connectors superset`)" in run(".data-context/dctx.py", "coverage").stdout
    shutil.move(hidden, repo / ".data-context/imports")
    built = run(".data-context/dctx.py", "build")
    assert built.returncode == 0, built.stderr
    imp = run(".data-context/dctx.py", "impact", "mart.orders.amount").stdout
    assert "superset:dashboard.ops.sales" in imp and "metabase:dashboard.ops.order_total" in imp, imp
    assert "Superset is used here" not in run(".data-context/dctx.py", "coverage").stdout
    print("bi export self-check passed")


if __name__ == "__main__":
    main()
