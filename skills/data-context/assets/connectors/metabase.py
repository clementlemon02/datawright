"""datawright connector: Metabase questions, models and dashboards, so they show up in `impact` and `review`.

Reads .data-context/imports/metabase/metabase.json, written by `uv run <data-context skill>/assets/bi_export.py metabase`
(read-only API calls). Questions become `metabase:question.<name>` (models `metabase:model.<name>`) reading the tables
their query builder or SQL names, and the questions they build on; dashboards `metabase:dashboard.<name>` with a column
per card, so `datawright impact mart.orders.revenue` reaches `metabase:dashboard.sales.<card>`.
ponytail: in SQL questions, any column of a table it reads that the SQL names counts as used (any clause).
"""
import json
import re


def match(path):
    return path.endswith("metabase.json")


def slug(s):
    return re.sub(r"\W+", "_", str(s).strip().lower()).strip("_")


def field_ids(node, out):
    """Field ids in a query, legacy (["field", 12, opts]) or staged (["field", {opts}, 12])."""
    if isinstance(node, list):
        if len(node) >= 2 and node[0] == "field":
            out.update(x for x in node[1:3] if isinstance(x, int))
        for x in node:
            field_ids(x, out)
    elif isinstance(node, dict):
        for x in node.values():
            field_ids(x, out)
    return out


def sources(node, out):
    """source-table values anywhere in a query: table ids, and "card__N" for questions built on questions."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k in ("source-table", "source-card"):
                out.add(f"card__{v}" if k == "source-card" else v)
            sources(v, out)
    elif isinstance(node, list):
        for x in node:
            sources(x, out)
    return out


def field_names(node, out):
    """Columns of a base question named in a query: ["field", "REVENUE", opts] or ["field", opts, "REVENUE"]."""
    if isinstance(node, list):
        if len(node) >= 2 and node[0] == "field":
            out.update(x.lower() for x in node[1:3] if isinstance(x, str))
        for x in node:
            field_names(x, out)
    elif isinstance(node, dict):
        for x in node.values():
            field_names(x, out)
    return out


def native_sql(q):
    if q.get("type") == "native":
        return (q.get("native") or {}).get("query") or ""
    return " ".join(s.get("native", "") for s in q.get("stages") or [] if s.get("lib/type") == "mbql.stage/native")


def extract(path, text):
    doc = json.loads(text)
    tables = {t["id"]: ".".join(x for x in (t.get("schema"), t["name"]) if x).lower() for t in doc.get("tables") or []}
    fields = {f["id"]: (tables[t["id"]], f["name"].lower()) for t in doc.get("tables") or [] for f in t.get("fields") or []}
    cols_of = {}
    for tid, col in fields.values():
        cols_of.setdefault(tid, set()).add(col)
    cards = [c for c in doc.get("cards") or [] if not c.get("archived")]
    taken = {}
    for c in cards:
        taken[slug(c["name"])] = taken.get(slug(c["name"]), 0) + 1
    name = {c["id"]: f"metabase:{'model' if c.get('type') == 'model' else 'question'}.{slug(c['name'])}"
            + (f"_{c['id']}" if taken[slug(c["name"])] > 1 else "") for c in cards}
    used = {}
    for c in sorted(cards, key=lambda c: c["id"]):  # a question's base questions come first, so their columns are known
        me, q = name[c["id"]], c.get("dataset_query") or {}
        kind = "metabase model" if c.get("type") == "model" else "metabase question"
        yield {"dataset": me, "kind": kind, "description": c.get("description")}
        reads, cols, parents = set(), set(), set()
        for s in sources(q, set()):
            if isinstance(s, int) and s in tables:
                reads.add(tables[s])
            elif isinstance(s, str) and s.startswith("card__") and s[6:].isdigit() and int(s[6:]) in name:
                parents.add(int(s[6:]))
        cols |= {fields[f] for f in field_ids(q, set()) if f in fields}
        sql = native_sql(q)
        if sql:  # strip Metabase's template syntax, then read it like any SQL
            parents |= {int(r) for r in re.findall(r"\{\{\s*#(\d+)", sql) if int(r) in name}
            sql = re.sub(r"\{\{\s*#\d+[^}]*\}\}", "metabase_card", re.sub(r"\[\[.*?\]\]", "", sql, flags=re.S))
            sql = re.sub(r"\{\{[^}]*\}\}", "1", sql)
            found = tables_in(sql)
            for f in found - {"metabase_card"}:  # as Metabase knows it (schema.table), when the SQL leaves the schema out
                known = [t for t in tables.values() if t == f or t.endswith("." + f) or f.endswith("." + t)]
                reads.add(known[0] if len(known) == 1 else f)
            words = set(re.findall(r"[a-z_]\w*", sql.lower()))
            cols |= {(t, w) for t in reads for w in cols_of.get(t, set()) & words}
        for t in sorted(reads):
            yield {"edge": [t, me], "kind": kind}
        for col in c.get("columns") or []:
            yield {"column": [me, str(col).lower()]}
        for t, col in sorted(cols):
            yield {"column_edge": [f"{t}.{col}", f"{me}.{col}"]}
        named = field_names(q, set())  # a base question's columns, referred to by name
        for pid in sorted(parents):
            yield {"edge": [name[pid], me], "kind": kind, "source_kind": "metabase question"}
            via = {n for n in named if n in used.get(pid, set())} if named else used.get(pid, set())  # none named: all pass through
            for col in sorted(via):
                yield {"column_edge": [f"{name[pid]}.{col}", f"{me}.{col}"]}
                cols.add((name[pid], col))
        used[c["id"]] = {col for _, col in cols}
    for d in doc.get("dashboards") or []:
        me = f"metabase:dashboard.{slug(d['name'])}"
        yield {"dataset": me, "kind": "metabase dashboard", "description": d.get("description") or d["name"]}
        for cid in d.get("cards") or []:
            if cid in name:
                tile = name[cid].split(".", 1)[1]
                yield {"edge": [name[cid], me], "kind": "metabase dashboard", "source_kind": "metabase question"}
                yield {"column": [me, tile]}
                for col in sorted(used.get(cid, ())):
                    yield {"column_edge": [f"{name[cid]}.{col}", f"{me}.{tile}"]}


def tables_in(sql):
    import sqlglot
    from sqlglot import exp
    for dialect in (None, "postgres", "mysql", "bigquery"):
        try:
            tree = sqlglot.parse_one(sql, read=dialect)
            break
        except Exception:
            tree = None
    if tree is None:
        return set()
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    return {".".join(p.name for p in t.parts).lower() for t in tree.find_all(exp.Table)} - ctes
