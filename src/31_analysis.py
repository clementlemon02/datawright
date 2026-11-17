ENGINE_LAYOUT = {  # how each engine names the keys: (partition, cluster or sort, distribution); None where it doesn't apply
    "bigquery": ("PARTITION BY DATE({p})", "CLUSTER BY {s}", None),
    "snowflake": (None, "CLUSTER BY ({s}), on large tables only", None),
    "starrocks": ("PARTITION BY RANGE({p})", "ORDER BY ({s}), the sort key", "DISTRIBUTED BY HASH({d})"),
    "doris": ("PARTITION BY RANGE({p})", "the key columns, in this order: {s}", "DISTRIBUTED BY HASH({d})"),
    "redshift": (None, "SORTKEY({s})", "DISTKEY({d})"),
    "clickhouse": ("PARTITION BY toYYYYMM({p})", "ORDER BY ({s})", None),
    "databricks": ("PARTITIONED BY ({p}), on tables over a terabyte", "CLUSTER BY ({s}), liquid clustering", None),
    "spark": ("PARTITIONED BY ({p})", "sort within partitions by {s} (Iceberg: WRITE ORDERED BY {s})", "bucket by {d} (Iceberg: bucket({d}))"),
    "hive": ("PARTITIONED BY ({p})", "SORTED BY ({s})", "CLUSTERED BY ({d}) INTO n BUCKETS"),
    "trino": ("partitioning = ARRAY['{p}']", "sorted_by = ARRAY['{s}']", "bucketed_by = ARRAY['{d}']"),
    "postgres": ("PARTITION BY RANGE ({p}), on very large tables", "an index on ({s})", None),
}


def advise(con, arg=None):
    """Partition, clustering and distribution keys from how the code actually filters and joins each table, against the
    layout its DDL declares. With no table, the tables where it would help most."""
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    dialect = setting("dialect", "") or (q("select v from meta where k='dialect'") or [[""]])[0][0]
    engine = ENGINE_LAYOUT.get(dialect.lower(), ENGINE_LAYOUT["spark"] if dialect.lower() in ("athena", "iceberg") else None)

    def facts(name):
        uses = {cl: Counter() for cl in ("filter", "join", "group")}
        for col, cl, n in q("""select column_name, clause, count(distinct file || ':' || line) from clauses
                               where dataset=? and clause in ('filter', 'join', 'group') group by 1, 2""", name):
            uses[cl][col] = n
        now = {k: c for k, c in q("select kind, columns from layout where dataset=?", name)}
        return uses, now

    def plan(name):  # (lines of advice, how much it would help)
        uses, now = facts(name)
        f, j = uses["filter"], uses["join"]
        types = dict(q("select name, type from columns where dataset=?", name))
        timeish = lambda c: bool(re.search(r"date|time|_dt$|^dt$|^ds$|_at$|^day$|^hour$|^month$", c + " " + (types.get(c) or ""), re.I))
        part = next((c for c, n in f.most_common() if timeish(c) and n >= 2), None)
        sort = [c for c, n in f.most_common() if c != part and n >= 2][:3]
        dist = next((c for c, n in j.most_common() if n >= 2), None)
        out, gain = [], 0
        have = lambda kind: set((now.get(kind) or "").split(",")) - {""}
        if part and part not in have("partition"):
            gain += f[part]
            out.append(("partition", part, f"filtered in {f[part]} queries; partitioning on it lets them skip the rest"))
        for c in have("partition"):
            if not f.get(c) and sum(f.values()) >= 3:
                gain += 1
                out.append(("partition", None, f"partitioned by {c}, which none of the {sum(f.values())} filtering queries here use: they read every partition "
                                               "(fine if it's there for retention or loading)"))
        if sort and not set(sort[:1]) & (have("cluster") | have("sort")):
            gain += f[sort[0]]
            now_ = ", ".join(sorted(have("cluster") | have("sort")))
            out.append(("cluster", ", ".join(sort), f"filtered on {', '.join(f'{c} ({f[c]})' for c in sort)}" + (f"; now keyed on {now_}, which queries filter less" if now_ else "")))
        if dist and dist not in have("distribution") and engine and engine[2]:
            gain += j[dist]
            out.append(("distribution", dist, f"joined on it in {j[dist]} queries; co-locating rows by it saves shuffling them"))
        return out, gain, uses, now

    def show(name, out, uses, now):
        print(f"layout advice for {name}" + (f" ({dialect})" if dialect else ""))
        print("  now: " + (" · ".join(f"{k} {c}" for k, c in sorted(now.items())) or "no partitioning, clustering or distribution declared in the repo's DDL"))
        for cl, label in (("filter", "filtered on"), ("join", "joined on"), ("group", "grouped by")):
            if uses[cl]:
                print(f"  {label}: " + ", ".join(f"{c} ({n})" for c, n in uses[cl].most_common(5)))
        rows = (q("select rows from datasets where name=?", name) or [[None]])[0][0]
        if rows and int(rows) < 1_000_000:
            print(f"  {int(rows):,} rows: small enough that layout hardly matters")
        for kind, cols, why in out:
            i = {"partition": 0, "cluster": 1, "distribution": 2}[kind]
            how = engine[i].format(p=cols, s=cols, d=cols) if engine and engine[i] and cols else None
            print(f"  {kind}: " + (f"{how}  ({why})" if how else why))
        if not out:
            print("  nothing to change: its layout fits how it's queried here (or the code doesn't filter or join it enough to tell)")
        print("  counts are distinct statements in the repo and imported query logs; check the warehouse's own stats before changing a big table")

    if arg:
        name = resolve(con, arg)
        if (q("select kind from datasets where name=?", name) or [[""]])[0][0] == "view":
            print(f"{name} is a view: it has no layout of its own. Advise on what it reads: {', '.join(r[0] for r in q('select src from edges where dst=?', name)[:5])}")
            return name, set()
        out, _, uses, now = plan(name)
        show(name, out, uses, now)
        return name, set()
    mine = {r[0] for r in q("select name from datasets where file is not null and kind != 'view' union select dataset from layout")}  # tables this repo defines
    ranked = sorted(((plan(r[0]), r[0]) for r in q("select distinct dataset from clauses where clause in ('filter', 'join')") if r[0] in mine),
                    key=lambda x: -x[0][1])
    ranked = [(p_, n) for p_, n in ranked if p_[1]][:15]
    print(f"tables where partitioning, clustering or distribution would help most: {len(ranked)}" + (f" ({dialect})" if dialect else ""))
    for (out, gain, _, _), n in ranked:
        print(f"  {n}: " + "; ".join(f"{k} {c}" if c else why for k, c, why in out))
    print("  details: datawright advise <table>")
    return "", set()


def concepts_cmd(con, arg=None, write=False):
    """What columns mean, compiled from how the code joins and renames them; --write merges proposals into concepts.yml."""
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    names = [r[0] for r in q("select concept from concepts group by concept order by max(confirmed) desc, max(evidence) desc, concept")]
    if arg:
        names = [n for n in names if n == arg.lower()] or sys.exit(f"No concept '{arg}'. Try: datawright concepts")
    out = []
    for n in names:
        rows = q("""select c.dataset, c.column_name, c.role, c.confirmed, c.evidence, k.type from concepts c
                    left join columns k on k.dataset = c.dataset and k.name = c.column_name where c.concept=? order by c.role != 'home', c.dataset""", n)
        home = next((r for r in rows if r["role"] == "home"), rows[0])
        types = sorted({(r["type"] or "").upper() for r in rows if r["type"]})
        by_name = Counter(r["column_name"] for r in rows)
        out.append({"name": n, "home": f"{home['dataset']}.{home['column_name']}", "confirmed": bool(home["confirmed"]),
                    "canonical": max(by_name, key=lambda c: (c != "id", by_name[c], c not in ("code", "key", "uuid", "name", "ref", "value", "type"), len(c))),
                    "type": types[0] if len(types) == 1 else None,
                    "columns": [f"{r['dataset']}.{r['column_name']}" for r in rows if r["role"] != "proposed"],
                    "proposed": [f"{r['dataset']}.{r['column_name']}" for r in rows if r["role"] == "proposed"],
                    "evidence": max(r["evidence"] for r in rows), "types": types, "names": by_name})
    if write:
        import yaml
        path = HOME / "concepts.yml"
        doc = (yaml.safe_load(path.read_text()) or {}) if path.exists() else {}
        kept = [k for k in doc.get("concepts") or [] if isinstance(k, dict) and k.get("confirmed")]
        have = {k["name"] for k in kept}
        for c in out:
            for k in kept:  # a confirmed concept keeps its words; new columns joined to it are listed for review
                if k["name"] == c["name"] and c["proposed"]:
                    k["proposed"] = sorted(set(k.get("proposed") or []) | set(c["proposed"]))
        fresh = [{"name": c["name"], "confirmed": False, "description": "", "canonical": c["canonical"], "roles": [], "type": c["type"] or ", ".join(c["types"]) or None,
                  "home": c["home"], "columns": c["columns"], "evidence": c["evidence"]} for c in out if c["name"] not in have]
        path.write_text("# What columns mean, compiled by `datawright concepts --write` from how the code joins and renames them.\n"
                        "# Review each: rename it, fix its home, add a description, drop columns that don't belong, then set confirmed: true.\n"
                        "# Confirmed concepts are never rewritten; columns newly joined to one appear under `proposed`.\n"
                        "# roles: other names that rightly hold the concept (referrer_id for a player); any other name is flagged as drift.\n"
                        + yaml.safe_dump({"concepts": kept + fresh}, sort_keys=False, allow_unicode=True, width=200))
        print(f"wrote .data-context/concepts.yml: {len(kept)} confirmed, {len(fresh)} proposed. Review it, then rebuild")
        return "", set()
    print(f"concepts: {len(out)}, from how the code joins and renames columns" + ("" if arg else " (datawright concepts <name> for one; --write to review them in concepts.yml)"))
    twins = {}  # same name, no join between them: likely one concept the code never connects (an app database and its warehouse copy)
    for c in out:
        twins.setdefault(c["canonical"], []).append(c["name"])
    for canonical, ns in sorted(twins.items()):
        if len(ns) > 1 and not arg and canonical not in ("id", "code", "key", "uuid", "name", "ref", "value", "type"):
            print(f"  maybe one concept: {', '.join(ns)} (all called {canonical}, but no join in the code links them): merge them in concepts.yml if so")
    for c in out[: None if arg else 40]:
        mixed = f" · TYPES DIFFER: {', '.join(c['types'])}" if len(c["types"]) > 1 else ""
        print(f"  {c['name']}{'' if c['confirmed'] else ' (proposed)'}: home {c['home']} · {len(c['columns'])} columns, evidence {c['evidence']}"
              f" · called {', '.join(f'{k} ({v})' for k, v in c['names'].most_common(4))}{mixed}")
        if arg:
            for col in c["columns"] + [f"{x} (proposed)" for x in c["proposed"]]:
                print(f"    {col}")
    return "", set()


def join_path(con, a, b):
    """How to join two tables: the shortest path through joins the code has written, as SQL."""
    q = lambda sql, *args: con.execute(sql, args).fetchall()
    a, b = resolve(con, a), resolve(con, b)
    graph = {}
    for r in q("""select left_dataset, left_column, right_dataset, right_column, count(distinct file || ':' || line) n from joins group by 1, 2, 3, 4"""):
        graph.setdefault(r[0], []).append((r[2], r[1], r[3], r[4], "written"))
        graph.setdefault(r[2], []).append((r[0], r[3], r[1], r[4], "written"))
    def search(graph):  # breadth first, the most-used join first at each step
        best, frontier = {a: []}, [a]
        for _ in range(4):
            nxt = []
            for n in frontier:
                for other, mine, theirs, used, why in sorted(graph.get(n, []), key=lambda e: -e[3]):
                    if other not in best:
                        best[other] = best[n] + [(n, mine, other, theirs, used, why)]
                        nxt.append(other)
            frontier = nxt
        return best
    best = search(graph)
    if b not in best:  # never joined in the code: columns that hold the same concept
        for c in q("select distinct concept from concepts where dataset in (?, ?)", a, b):
            cols = q("select dataset, column_name from concepts where concept=?", c[0])
            for x in cols:
                for y in cols:
                    if x[0] != y[0] and not any(e[0] == y[0] for e in graph.get(x[0], [])):
                        graph.setdefault(x[0], []).append((y[0], x[1], y[1], 0, f"same concept {c[0]}, not joined in the code yet"))
        best = search(graph)
    if b not in best:  # last resort: a column named like a concept's column joins that concept's home. Labelled: check it
        homes = {r[0]: (r[1], r[2]) for r in q("select concept, dataset, column_name from concepts where role='home'")}
        named = {}
        for r in q("select distinct concept, column_name from concepts"):
            if re.search(r"(_id|_key|_code|_no|uuid)$", r[1]):  # a key's name; generic words (code, name, status) say nothing
                named.setdefault(r[1], []).append(r[0])
        near = lambda t_, h: len(os.path.commonprefix([t_.split("."), h.split(".")]))  # same catalog and schema first
        for t_ in (a, b):
            for (c,) in q("select name from columns where dataset=?", t_):
                hs = [(homes[k], k) for k in named.get(c, []) if k in homes and homes[k][0] != t_]
                if hs:
                    (h, k) = max(hs, key=lambda x: (near(t_, x[0][0]), x[1]))
                    why = f"by name only: {c} is what the code calls {k} elsewhere; check it"
                    graph.setdefault(t_, []).append((h[0], c, h[1], 0, why))
                    graph.setdefault(h[0], []).append((t_, h[1], c, 0, why))
        best = search(graph)
    if b not in best:
        print(f"no join path between {a} and {b} in the code within 4 hops. Their columns: datawright card {a}, datawright card {b}")
        return a, set()
    alias = {a: "t0"}
    inferred = sum(1 for *_, used, why in best[b] if not used)
    print(f"-- {a} to {b}: {len(best[b])} join{'s' if len(best[b]) != 1 else ''}, "
          + (f"{inferred} of them inferred, not written in the code: check those" if inferred else "as the code joins them"))
    print(f"select …\nfrom {a} t0")
    typ = lambda ds, c: (q("select type from columns where dataset=? and name=?", ds, c) or [[None]])[0][0]
    for i, (x, mine, y, theirs, used, why) in enumerate(best[b], 1):
        alias[y] = f"t{i}"
        left, right, note = f"t{i}.{theirs}", f"{alias[x]}.{mine}", ""
        tx, ty = typ(x, mine), typ(y, theirs)
        if type_family(tx) and type_family(ty) and type_family(tx) != type_family(ty):  # VARCHAR playerid against BIGINT id: cast the text side
            if type_family(ty) == "text":
                left = f"cast({left} as {tx.lower()})"
            else:
                right = f"cast({right} as {ty.lower()})"
            note = f"; {x}.{mine} is {tx}, {y}.{theirs} is {ty}"
        how = f"written in {used} quer{'y' if used == 1 else 'ies'}" if used else why  # not nested in the f-string: Python 3.10 and 3.11
        print(f"join {y} t{i} on {left} = {right}  -- {how}{note}")
    return a, set()


def export(con, target, include_proposed=False, out_dir=None):
    """Concepts and value maps as files other tools load: dbt semantic-model entities, an OpenMetadata glossary CSV with a
    column-to-term list, or a DataHub business glossary. Written to .data-context/exports/; nothing is sent anywhere."""
    import csv as csv_
    import yaml
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    said = {}
    if (HOME / "concepts.yml").exists():
        doc = yaml.safe_load((HOME / "concepts.yml").read_text()) or {}
        said = {k["name"]: k for k in doc.get("concepts") or [] if isinstance(k, dict) and k.get("name")}
    concepts = []
    for (name,) in q("select distinct concept from concepts order by concept"):
        rows = q("select dataset, column_name, role, confirmed from concepts where concept=? order by role != 'home', dataset", name)
        confirmed = any(r["confirmed"] for r in rows)
        if not (confirmed or include_proposed):
            continue
        rows = [r for r in rows if include_proposed or r["role"] != "proposed"]
        home = next((r for r in rows if r["role"] == "home"), rows[0])
        k = said.get(name, {})
        names = Counter(r["column_name"] for r in rows if r["column_name"] != "id")
        canonical = k.get("canonical") or (max(names, key=lambda c: (names[c], c)) if names else home["column_name"])
        values = {}
        for v, label, how in q(f"""select value, label, how from value_maps where {' or '.join(['(dataset=? and column_name=?)'] * len(rows))}
                                   order by how = 'alias'""", *[x for r in rows for x in (r["dataset"], r["column_name"])]):
            values.setdefault(v, label if how == "label" else f"~{label}")
        types = sorted({(r[0] or "").upper() for r in q(f"""select type from columns where {' or '.join(['(dataset=? and name=?)'] * len(rows))}""",
                                                        *[x for r in rows for x in (r["dataset"], r["column_name"])]) if r[0]})
        about = k.get("description") or ""
        made = (f"Home: {home['dataset']}.{home['column_name']}. Also called {', '.join(sorted(set(names) - {canonical})) or 'nothing else'}."
                + (f" Values: {', '.join(f'{a} = {b}' for a, b in sorted(values.items())[:12])}." if values else "")
                + ("" if confirmed else " Proposed by datawright from how the code joins and renames columns; not yet reviewed."))
        concepts.append({"name": name, "canonical": canonical, "confirmed": confirmed, "description": f"{about} {made}".strip(),
                         "home": home, "rows": rows, "types": types, "synonyms": sorted(set(names) - {canonical})})
    if not concepts:
        sys.exit("No concepts to export: confirm some in .data-context/concepts.yml (datawright concepts --write), or pass --all for proposals too")
    out = Path(out_dir) if out_dir else HOME / "exports"
    out.mkdir(parents=True, exist_ok=True)
    if target == "dbt":
        models = {r[0] for r in q("select name from datasets where kind='dbt model'")}
        sms = {}
        for c in concepts:
            for r in c["rows"]:
                if r["dataset"] in models:
                    sm = sms.setdefault(r["dataset"], {"name": r["dataset"], "model": f"ref('{r['dataset']}')", "entities": []})
                    if c["name"] not in {e["name"] for e in sm["entities"]}:
                        e = {"name": c["name"], "type": "primary" if r["role"] == "home" else "foreign"}
                        if r["column_name"] != c["name"]:
                            e["expr"] = r["column_name"]
                        sm["entities"].append(e)
        if not sms:
            sys.exit("No dbt models hold these concepts: nothing to export for dbt")
        for sm in sms.values():
            if not any(e["type"] == "primary" for e in sm["entities"]):
                sm["primary_entity"] = sm["name"]  # MetricFlow needs one; this model's own grain, which datawright doesn't know
        path = out / "datawright_semantic_models.yml"
        path.write_text("# dbt semantic models from datawright concepts: one entity per concept a model holds (primary where it's the home).\n"
                        "# Copy into your dbt project's models/ folder, then `dbt parse`. Add measures and dimensions as you need them.\n"
                        "# dbt's semantic layer needs a time spine model; if the project has none: docs.getdbt.com/docs/build/metricflow-time-spine\n"
                        + yaml.safe_dump({"semantic_models": list(sms.values())}, sort_keys=False, width=200))
        print(f"wrote {path}: {len(sms)} semantic models, {sum(len(s['entities']) for s in sms.values())} entities")
    elif target == "openmetadata":
        path, cols = out / "openmetadata_glossary.csv", out / "openmetadata_column_terms.csv"
        with path.open("w", newline="") as f:
            w = csv_.writer(f)
            w.writerow(["parent", "name*", "displayName", "description*", "synonyms", "relatedTerms", "references", "tags"])
            for c in concepts:
                w.writerow(["", c["name"], c["name"].replace("_", " ").title(), c["description"], ";".join(c["synonyms"]), "", "", ""])
        with cols.open("w", newline="") as f:
            w = csv_.writer(f)
            w.writerow(["table", "column", "glossary_term"])
            for c in concepts:
                for r in c["rows"]:
                    w.writerow([r["dataset"], r["column_name"], c["name"]])
        print(f"wrote {path} ({len(concepts)} terms; import it into a glossary in OpenMetadata: Glossary > ⋮ > Import) "
              f"and {cols} (which columns to tag with each term; prefix your service name to the table names). Columns can differ by version: compare with a template exported from your glossary")
    elif target == "datahub":
        path = out / "datahub_business_glossary.yml"
        terms = [{"name": c["name"], "description": c["description"], "term_source": "INTERNAL",
                  "custom_properties": {"home": f"{c['home']['dataset']}.{c['home']['column_name']}", "canonical": c["canonical"],
                                        "type": ", ".join(c["types"]) or "unknown", "columns": ", ".join(f"{r['dataset']}.{r['column_name']}" for r in c["rows"][:40]),
                                        "status": "confirmed" if c["confirmed"] else "proposed"}} for c in concepts]
        path.write_text("# DataHub business glossary from datawright concepts. Ingest with a recipe:\n"
                        "#   source: {type: datahub-business-glossary, config: {file: <this file>, enable_auto_id: true}}\n"
                        + yaml.safe_dump({"version": "1", "source": "DataHub", "owners": {"users": [], "groups": []},  # required by the ingester
                                          "nodes": [{"name": "Data concepts", "description":
                                          "What key columns mean, compiled by datawright from how the code joins and renames them", "terms": terms}]},
                                         sort_keys=False, allow_unicode=True, width=200))
        print(f"wrote {path}: {len(terms)} terms")
    return "", set()


def impact(con, arg, budget=1500):
    """Everything a change to a dataset, or to one of its columns, would touch: the plan for changing it safely."""
    name = resolve(con, arg, quiet=True)
    ds, col = (name, None) if name else (resolve(con, arg.lower().rpartition(".")[0] or arg), arg.lower().rpartition(".")[2])
    q = lambda sql, *a: con.execute(sql, a).fetchall()

    def at(f, ln):  # file:line, with the Airflow task that runs it
        t = q("select dag, task from runs where file=? and ? between line_from and line_to and task != '' limit 1", f, ln or 1)
        return f"{f}:{ln}" + (f" ({t[0][0]}.{t[0][1]})" if t else "") if f != "query log" else "warehouse query log"
    d = con.execute("select * from datasets where name=?", (ds,)).fetchone()
    writers = [at(r["file"], r["line"]) for r in q("select file, line from writers where dataset=? order by file", ds)]
    if col:
        uses = q("select file, line from col_uses where dataset=? and column_name=? order by file, line", ds, col)
        stars = q("select file, line from col_uses where dataset=? and column_name='*' order by file, line", ds)
        down = sorted(reach(f"{ds}.{col}", lambda n: [r[0] for r in q("select dst from col_edges where src=?", n)], 9))
        out = [f"impact of changing {ds}.{col}" + ("" if (ds, col) in {(ds, r[0]) for r in q("select name from columns where dataset=?", ds)} else "  (column not in the index: check the name)")]
    else:
        uses = q("select distinct file, line from col_uses where dataset=? and column_name != '*' order by file, line", ds)
        stars = q("select distinct file, line from col_uses where dataset=? and column_name='*' order by file, line", ds)
        down = sorted(reach(ds, lambda n: [r[0] for r in q("select dst from edges where src=?", n)], 9))
        out = [f"impact of changing {ds}"]
    tags = {r[0]: r[1][7:] for r in q("select dst, via from edges where via like 'import %'")}
    weak = weak_links(con, [ds], down)[0] if not col else set()
    files = {r["file"] for r in uses + stars if r["file"] != "query log"} | {r[0] for r in q("select file from writers where dataset=?", ds)}
    sections = [out + [f"owner: {d['owner']}"] * bool(d and d["owner"]),
                capped("written by (change these first)", writers, 10, f"datawright card {ds}"),
                capped("code that reads it" + (" (any clause: select, join, where)" if col else ""), [at(r["file"], r["line"]) for r in uses], 25, f"sqlite3 .data-context/index.db \"select * from col_uses where dataset='{ds}'\""),
                capped("reads every column (select *, breaks on drops and reorders)", [at(r["file"], r["line"]) for r in stars], 10, ""),
                capped("downstream" + (" columns" if col else ""), [n + (f" ({tags[n.rsplit('.', 1)[0] if col else n]})" if (n.rsplit('.', 1)[0] if col else n) in tags else "") + (" (inferred link)" if n in weak else "") for n in down], 30, f"datawright lineage {arg} --down 9"),
                [f"notes: {n['kind']}: {n['text']}" for n in q("select kind, text from notes where dataset=? and (column_name=? or column_name='')", ds, col or "")][:5],
                [] if uses or stars or down else ["nothing in the repo or the imported lineage reads it; check the warehouse query log before dropping"]]
    emit(sections, budget)
    return ds, files


def reach(start, nbrs, depth):
    seen, frontier = set(), [start]
    for _ in range(depth):
        frontier = list(dict.fromkeys(m for n in frontier for m in nbrs(n) if m not in seen))
        seen.update(frontier)
    return seen


def weak_links(con, roots, down):
    """(nodes in `down` that are reached only through inferred links, [(src, dst, via)] of the inferred links among them)."""
    inside = set(roots) | set(down)
    rows = [r for r in con.execute("select src, dst, how, via from edges") if r[0] in inside and r[1] in down]
    incoming = {}
    for s, d, how, via in rows:
        incoming.setdefault(d, []).append(how)
    return ({n for n in down if incoming.get(n) and all(h == "inferred" for h in incoming[n])},
            sorted((s, d, via) for s, d, how, via in rows if how == "inferred"))


