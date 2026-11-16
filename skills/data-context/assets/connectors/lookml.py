"""dctx connector: Looker's LookML, so dashboards show up in `impact` and `review`.

Views (`sql_table_name`, or the tables a `derived_table` reads) become `looker:view.<view>`, with a
column per dimension and measure traced to the warehouse column it reads. Explores (`looker:explore.<explore>`) read
their base and joined views; LookML dashboards (`*.dashboard.lookml`) read their explores, and each tile's fields are
column edges, so `dctx impact mart.orders.revenue` reaches `looker:dashboard.<dashboard>.<tile>`.
Dashboards built in the Looker UI live in the instance, not the repo: export them through its API instead.
"""
import re
from pathlib import Path

import yaml

TOKEN = re.compile(r"(\+?\w+)\s*:\s*|\}|#[^\n]*")
REF = re.compile(r"\$\{(\w+)(?:\.(\w+))?\}(?:\.(\w+))?")


def match(path):
    return path.endswith((".lkml", ".lookml"))


def parse(text):
    """LookML as nested [key, name or value, children or None]."""
    root, i = [], 0
    stack = [root]
    while m := TOKEN.search(text, i):
        tok, i = m.group(0), m.end()
        if tok.startswith("#"):
            continue
        if tok == "}":
            if len(stack) > 1:
                stack.pop()
            continue
        key = m.group(1)
        if key.startswith("sql") or key.endswith("_sql") or key == "html":  # free text up to ";;"
            end = text.find(";;", i)
            end = len(text) if end < 0 else end
            stack[-1].append([key, text[i:end].strip(), None])
            i = end + 2
        elif b := re.match(r"(\+?\w*)\s*\{", text[i:]):
            node = [key, b.group(1), []]
            stack[-1].append(node)
            stack.append(node[2])
            i += b.end()
        else:
            v = re.match(r'"[^"]*"|\[[^\]]*\]|[^\s}\]]+', text[i:])
            stack[-1].append([key, v.group(0).strip('"') if v else "", None])
            i += v.end() if v else 0
    return root


def get(node, key):
    return next((v for k, v, c in node or () if k == key and c is None), None)


def table(name):
    return re.sub(r'[`"\[\]]', "", name).strip().lower()


def constants():
    """`@{NAME}` values from the project's manifest.lkml. ROOT is set by dctx."""
    try:
        text = next(Path(ROOT).rglob("manifest.lkml")).read_text()
    except (NameError, StopIteration, OSError):
        return {}
    return {n: get(c, "value") or "" for k, n, c in parse(text) if k == "constant" and c is not None}


def extract(path, text):
    consts = constants()
    text = re.sub(r"@\{(\w+)\}", lambda m: consts.get(m[1], m[0]), text)
    if path.endswith(".lookml"):  # LookML dashboards are YAML
        yield from dashboards(text)
        return
    for key, name, body in parse(text):
        name = name.lstrip("+")
        if key == "view" and body is not None:
            yield from view(name, body)
        elif key == "explore" and body is not None:
            base = get(body, "from") or get(body, "view_name") or name
            yield {"edge": [f"looker:view.{base}", f"looker:explore.{name}"], "kind": "looker explore", "source_kind": "looker view"}
            for k, j, jb in body:
                if k == "join" and jb is not None:
                    yield {"edge": [f"looker:view.{get(jb, 'from') or j}", f"looker:explore.{name}"], "kind": "looker explore", "source_kind": "looker view"}


def view(name, body):
    me = f"looker:view.{name}"
    src = get(body, "sql_table_name")
    derived = next((c for k, _, c in body if k == "derived_table" and c is not None), None)
    if src:
        src = table(src)
        yield {"edge": [src, me], "kind": "looker view"}
    elif derived is not None:
        sql = re.sub(r"\{%\s*condition.*?endcondition\s*%\}", "1=1", get(derived, "sql") or "", flags=re.S)  # a templated filter
        sql = re.sub(r"\{%.*?%\}|\{\{.*?\}\}", "", sql, flags=re.S)
        for v in re.findall(r"\$\{(\w+)\.SQL_TABLE_NAME\}", sql):
            yield {"edge": [f"looker:view.{v}", me], "kind": "looker view", "source_kind": "looker view"}
        for t in tables(re.sub(r"\$\{\w+\.SQL_TABLE_NAME\}", "looker_pdt", sql)):
            yield {"edge": [t, me], "kind": "looker view"}
        for k, e, _ in derived:
            if k == "explore_source":  # a native derived table queries an explore
                yield {"edge": [f"looker:explore.{e}", me], "kind": "looker view", "source_kind": "looker explore"}
    yield {"dataset": me, "kind": "looker view", "description": get(body, "label")}
    for k, field, fb in body:
        if k not in ("dimension", "measure", "dimension_group") or fb is None:
            continue
        names = [f"{field}_{t}" for t in re.findall(r"\w+", get(fb, "timeframes") or "")] if k == "dimension_group" else []
        for f in names or [field]:
            yield {"column": [me, f, get(fb, "type"), get(fb, "description") or get(fb, "label")]}
            for v, col, tcol in REF.findall(get(fb, "sql") or ""):
                if v == "TABLE" and tcol:
                    origin = f"{src or me}.{tcol}"
                elif v != "TABLE" and not tcol:
                    origin = f"looker:view.{v}.{col}" if col else f"{me}.{v}"
                else:
                    continue
                if origin.lower() != f"{me}.{f}".lower():
                    yield {"column_edge": [origin, f"{me}.{f}"]}


def tables(sql):
    """Tables a derived table's SQL reads. ponytail: tables only, so its fields don't trace to warehouse columns."""
    import sqlglot
    from sqlglot import exp
    for dialect in (None, "bigquery"):  # backticked `project.dataset.table` needs bigquery's reader
        try:
            tree = sqlglot.parse_one(sql, read=dialect)
            break
        except Exception:
            tree = None
    if tree is None:
        return []
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    names = {table(".".join(p.name for p in t.parts)) for t in tree.find_all(exp.Table)}
    return sorted(names - ctes - {"looker_pdt"})


def dashboards(text):
    for d in yaml.safe_load(text) or []:
        if not isinstance(d, dict) or not d.get("dashboard"):
            continue
        me = f"looker:dashboard.{d['dashboard']}"
        yield {"dataset": me, "kind": "looker dashboard", "description": d.get("title")}
        for el in d.get("elements") or []:
            if el.get("explore"):
                yield {"edge": [f"looker:explore.{el['explore']}", me], "kind": "looker dashboard", "source_kind": "looker explore"}
            tile = el.get("name") or el.get("title")
            for f in el.get("fields") or []:
                if tile and "." in str(f):
                    yield {"column_edge": [f"looker:view.{f}", f"{me}.{tile}"]}
