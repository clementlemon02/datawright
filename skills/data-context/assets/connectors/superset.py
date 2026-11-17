"""datawright connector: Apache Superset (and Preset) dashboards, so they show up in `impact` and `review`.

Reads Superset's export format: the ZIP from Dashboards > Export (or `superset export-dashboards`, or the REST API)
dropped into .data-context/imports/, which datawright unpacks; or the YAML files kept in git by `preset-cli superset sync`.
Datasets become `superset:dataset.<table>`, read from their table or, for virtual datasets, from the tables their SQL
reads; charts `superset:chart.<name>` with the columns and metrics they use; dashboards `superset:dashboard.<name>`
with a column per chart. So `datawright impact mart.orders.revenue` reaches `superset:dashboard.sales.revenue_by_day`.
"""
import json
import re
from pathlib import Path

import yaml

KINDS = ("charts", "datasets", "dashboards")
COLUMN_KEYS = ("groupby", "columns", "all_columns", "x_axis", "granularity_sqla", "series", "entity", "order_by_cols",
               "metric", "metrics", "metric_2", "percent_metrics", "secondary_metric", "size", "x", "y", "timeseries_limit_metric")
_index = {}


def match(path):
    p = Path(path)
    return p.suffix in (".yaml", ".yml") and (bool(set(p.parts) & set(KINDS)) or p.name in ("dashboard.yaml", "dataset.yaml"))


def slug(s):
    return re.sub(r"\W+", "_", str(s).strip().lower()).strip("_")


def root_of(path):
    """The export's top folder: the one holding charts/, datasets/ and dashboards/."""
    p = Path(ROOT, path)
    for i, part in enumerate(p.parts):
        if part in KINDS:
            return Path(*p.parts[:i])
    return p.parent


def load(text):
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError:
        return {}
    return doc if isinstance(doc, dict) else {}


def params(chart):
    p = chart.get("params") or {}
    if isinstance(p, str):  # older exports keep params as a JSON string
        try:
            p = json.loads(p)
        except ValueError:
            p = {}
    return p if isinstance(p, dict) else {}


def dataset_name(d):
    return ".".join(x for x in (d.get("schema"), d.get("table_name")) if x).lower()


def index(root):
    """uuid -> what it is, across one export: datasets with their columns and metrics, charts with what they use."""
    if root in _index:
        return _index[root]
    docs = [load(f.read_text(errors="replace")) for f in sorted(root.rglob("*.y*ml"))]
    ix = {}
    for d in docs:
        if d.get("table_name") and d.get("uuid"):
            cols = {str(c.get("column_name")).lower() for c in d.get("columns") or [] if c.get("column_name")}
            metrics = {str(m.get("metric_name")).lower() for m in d.get("metrics") or [] if m.get("metric_name")}
            ix[d["uuid"]] = {"name": f"superset:dataset.{dataset_name(d)}", "cols": cols, "metrics": metrics}
    for d in docs:
        if d.get("slice_name") and d.get("uuid"):
            ds = ix.get(d.get("dataset_uuid"), {})
            ix[d["uuid"]] = {"name": f"superset:chart.{slug(d['slice_name'])}", "dataset": ds.get("name"),
                             "uses": uses(params(d), ds.get("cols", set()), ds.get("metrics", set()))}
    _index[root] = ix
    return ix


def uses(p, cols, metrics):
    """Dataset columns and metrics a chart's form data refers to."""
    out = set()

    def add(v):
        if isinstance(v, str):
            if v.lower() in cols | metrics or not cols:
                out.add(v.lower())
        elif isinstance(v, dict):
            if isinstance(v.get("column"), dict):
                add(v["column"].get("column_name"))
            if v.get("subject"):
                add(v["subject"])
            for w in re.findall(r"[A-Za-z_]\w*", str(v.get("sqlExpression") or "")):
                if w.lower() in cols | metrics:
                    out.add(w.lower())
        elif isinstance(v, list):
            for x in v:
                add(x)
    for k in COLUMN_KEYS:
        add(p.get(k))
    add(p.get("adhoc_filters"))
    return sorted(out)


def extract(path, text):
    d = load(text)
    if d.get("table_name") and "database_uuid" in d:
        yield from dataset(d)
    elif d.get("slice_name"):
        c = index(root_of(path)).get(d.get("uuid")) or {}
        me = f"superset:chart.{slug(d['slice_name'])}"
        yield {"dataset": me, "kind": "superset chart", "description": d.get("description") or d.get("viz_type")}
        if c.get("dataset"):
            yield {"edge": [c["dataset"], me], "kind": "superset chart", "source_kind": "superset dataset"}
            for col in c["uses"]:
                yield {"column": [me, col]}
                yield {"column_edge": [f"{c['dataset']}.{col}", f"{me}.{col}"]}
    elif d.get("dashboard_title"):
        me = f"superset:dashboard.{slug(d.get('slug') or d['dashboard_title'])}"
        yield {"dataset": me, "kind": "superset dashboard", "description": d["dashboard_title"]}
        ix = index(root_of(path))
        for v in (d.get("position") or {}).values():
            if isinstance(v, dict) and v.get("type") == "CHART" and (c := ix.get((v.get("meta") or {}).get("uuid"))):
                tile = c["name"].split(".", 1)[1]
                yield {"edge": [c["name"], me], "kind": "superset dashboard", "source_kind": "superset chart"}
                yield {"column": [me, tile]}
                for col in c["uses"]:
                    yield {"column_edge": [f"{c['name']}.{col}", f"{me}.{tile}"]}


def dataset(d):
    me = f"superset:dataset.{dataset_name(d)}"
    sql = d.get("sql")
    yield {"dataset": me, "kind": "superset dataset", "description": d.get("description")}
    if sql:  # a virtual dataset: the tables its SQL reads. ponytail: tables only, its columns don't trace further
        for t in tables(sql):
            yield {"edge": [t, me], "kind": "superset dataset"}
    else:
        yield {"edge": [dataset_name(d), me], "kind": "superset dataset"}
    cols = {str(c["column_name"]).lower(): c for c in d.get("columns") or [] if c.get("column_name")}
    for name, c in cols.items():
        yield {"column": [me, name, c.get("type"), c.get("description") or c.get("verbose_name")]}
        if c.get("expression"):  # a calculated column: from the columns its expression names
            for w in set(re.findall(r"[A-Za-z_]\w*", c["expression"].lower())) & set(cols) - {name}:
                yield {"column_edge": [f"{me}.{w}", f"{me}.{name}"]}
        elif not sql:
            yield {"column_edge": [f"{dataset_name(d)}.{name}", f"{me}.{name}"]}
    for m in d.get("metrics") or []:
        if m.get("metric_name"):
            name = str(m["metric_name"]).lower()
            yield {"column": [me, name, "metric", m.get("description") or m.get("expression")]}
            for w in set(re.findall(r"[A-Za-z_]\w*", str(m.get("expression") or "").lower())) & set(cols):
                yield {"column_edge": [f"{me}.{w}", f"{me}.{name}"]}


def tables(sql):
    """Tables a query reads, with Jinja templating removed."""
    import sqlglot
    from sqlglot import exp
    sql = re.sub(r"\{%.*?%\}|\{\{.*?\}\}", "1", sql, flags=re.S)
    for dialect in (None, "bigquery", "mysql"):
        try:
            tree = sqlglot.parse_one(sql, read=dialect)
            break
        except Exception:
            tree = None
    if tree is None:
        return []
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    return sorted({".".join(p.name for p in t.parts).lower().replace("`", "") for t in tree.find_all(exp.Table)} - ctes)
