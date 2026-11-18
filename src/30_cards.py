# ---------------------------------------------------------------- queries

def connect():
    if not DB.exists():
        sys.exit("No index yet. Run: datawright build")
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    if built_by(con) != VERSION:
        sys.exit(f"The index was built by datawright {built_by(con) or 'before 0.2'}; this is {VERSION}. Run: datawright build")
    return con


def built_by(con):
    return (con.execute("select v from meta where k='version'").fetchone() or [""])[0]


def outdated(con):
    """True when a commit landed after the last build, or another datawright version built it."""
    if built_by(con) != VERSION:
        return True
    built = con.execute("select v from meta where k='built_at'").fetchone()
    last = run(["git", "-C", str(ROOT), "log", "-1", "--format=%ct"]).strip()
    return bool(last and built and int(last) > int(built[0]))


def resolve(con, arg, quiet=False):
    a = arg.lower()
    if con.execute("select 1 from datasets where name=?", (a,)).fetchone():
        return a
    cands = [r[0] for r in con.execute("select name from datasets where (name like ? and name not like '%:%') or name = ? order by name", ("%." + a, "kafka:" + a))]
    if len(cands) == 1:
        return cands[0]
    if not quiet:
        sys.exit(f"Ambiguous '{arg}': {', '.join(cands[:10])}" if cands else f"No dataset '{arg}'. Try: datawright search {arg}")
    return None


def emit(sections, budget):
    """Print sections in priority order until the token budget (about 4 characters a token) runs out."""
    left = budget * 4
    for lines in sections:
        for i, line in enumerate(lines):
            if len(line) + 1 > left:
                print(f"… {len(lines) - i} more lines cut at --budget {budget}")
                return
            print(line)
            left -= len(line) + 1


def capped(label, items, limit, more):
    if not items:
        return []
    shown = items[:limit] + ([f"+{len(items) - limit} more: {more}"] if len(items) > limit else [])
    return [f"{label} ({len(items)}): " + ", ".join(shown)]


def contract_breaks(con, name):
    """Required columns a table's contract promises that its code no longer produces: [(column, contract file)]."""
    have = {r[0] for r in con.execute("select name from columns where dataset=?", (name,))}
    if not have or "*" in have:
        return []  # columns unknown (or select *): nothing to hold the contract against
    return [(r[0], r[1]) for r in con.execute("select column_name, source from contracts where dataset=? and required=1", (name,)) if r[0] not in have]


def readiness(con, d):
    """0-100: how ready a table is to be trusted and changed, from what datawright knows; and what's missing, heaviest first."""
    one = lambda sql, *a: con.execute(sql, a).fetchone() is not None
    since = (datetime.date.today() - datetime.timedelta(days=180)).isoformat()
    parts = [("grain or description", 20, bool(d["grain"] or d["description"])), ("owner", 15, bool(d["owner"])),
             ("tests", 15, one("select 1 from tests where dataset=?", d["name"])), ("key", 10, bool(d["key"])),
             ("monitoring", 10, bool(d["freshness"]) or (bool(d["file"]) and one("select 1 from runs where file=? and task != ''", d["file"]))),
             ("known producer", 10, bool(d["file"])), ("no stale notes", 10, not one("select 1 from notes where dataset=? and stale != ''", d["name"])),
             ("verified in 180 days", 10, bool(d["verified"]) and d["verified"] >= since)]
    return sum(w for _, w, ok in parts if ok), [n for n, _, ok in parts if not ok]


def card(con, arg, budget):
    name = resolve(con, arg)
    d = con.execute("select * from datasets where name=?", (name,)).fetchone()
    ch = con.execute("select * from changes where file=?", (d["file"],)).fetchone() if d["file"] else None
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    head = f"{name}  [{d['kind']}]" + (f"  produced by {d['file']}:{d['line']}" if d["file"] else "")
    head += f"  · last change {ch['date']} {ch['hash']} \"{ch['subject'][:60]}\"" if ch else ""
    head += f"  · ~{d['rows']:,} rows" if d["rows"] else ""
    head += f"  · schema in {d['schema_at']}" if d["schema_at"] else ""
    stale_index = ["(index older than the latest commit: run datawright build)"] if outdated(con) else []

    score, missing = readiness(con, d)
    about = [f"readiness {score}/100" + (f" · tier {d['tier']}" if d["tier"] else "") + (f" · missing: {', '.join(missing[:4])}" if missing else "")]
    about += [f"{k}: {d[k]}" for k in ("description", "grain", "key", "owner", "freshness") if d[k]]
    terms_ = q("select count(*) n, sum(required) req, min(source) src from contracts where dataset=?", name)[0]
    if terms_["n"]:
        about.append(f"contract ({terms_['src']}): {terms_['n']} columns, {terms_['req'] or 0} required")
        about += [f"CONTRACT BROKEN: required column {c} is no longer produced ({s})" for c, s in contract_breaks(con, name)]
    if d["verified"] and ch and ch["date"] > d["verified"]:
        about.append(f"(verified {d['verified']}, code changed {ch['date']}: recheck)")
    if not (d["grain"] or d["description"]):
        about.append("grain and meaning not recorded yet: work them out, then datawright set")
    order = "case kind " + " ".join(f"when '{k}' then {i}" for i, k in enumerate(KINDS)) + " else 9 end"
    notes = [f"  {n['date']} {n['kind']}{' [' + n['column_name'] + ']' if n['column_name'] else ''}: {n['text']}"
             + (f"  ({n['source']})" if n["source"] else "")
             + (f"  (STALE: {n['stale']})" if n["stale"] else "")
             for n in q(f"select * from notes where dataset=? order by {order}, date desc", name)]
    tag = lambda via: " (query log)" if via == "query log" else f" ({via[7:]})" if via.startswith("import ") else " (pyspark)" if via.endswith("(pyspark)") else ""
    guess = lambda r: " (inferred)" if r["how"] == "inferred" else ""  # not written plainly: substituted, bound or pattern-matched
    up = [r["src"] + tag(r["via"]) + guess(r) for r in q("select src, via, how from edges where dst=? order by src", name)]
    down = [r["dst"] + guess(r) for r in q("select dst, how from edges where src=? order by dst", name)]
    readers = q("select file, line from readers where dataset=? order by file", name)
    writers = q("select file, line from writers where dataset=? and file is not ? order by file", name, d["file"])
    down += [f"{r['file']}:{r['line']} (reads)" for r in readers]
    # files an agent would otherwise open to learn the same: the producer, downstream producers, readers, notes
    files = {d["file"]} if d["file"] else set()
    files |= {r[0] for r in q("select p.file from edges e join datasets p on p.name = e.dst where e.src=? and p.file is not null", name)}
    files |= {r["file"] for r in readers} | {w["file"] for w in writers}
    if d["schema_at"]:
        files.add(d["schema_at"].rsplit(":", 1)[0])
    if doc_path(name).exists():
        files.add(str(doc_path(name).relative_to(ROOT)))
    feeds = {}
    for r in q("select src, dst from col_edges where dst like ? order by src", name + ".%"):
        feeds.setdefault(r["dst"].rsplit(".", 1)[1], []).append(r["src"])
    cols, same, mapped = [], {}, []
    for c in q("select * from columns where dataset=? order by rowid", name):
        cols.append(f"{c['name']}{' ' + c['type'] if c['type'] else ''}")
        srcs = feeds.get(c["name"], [])
        if len(srcs) == 1 and srcs[0].rsplit(".", 1)[1] == c["name"]:  # copied 1:1, say it once per source table
            same.setdefault(srcs[0].rsplit(".", 1)[0], []).append(c["name"])
        elif srcs:
            mapped.append(f"{c['name']} ← {', '.join(srcs)}")
    sources = [f"same names from {t} ({len(cs)})" for t, cs in sorted(same.items(), key=lambda x: -len(x[1]))] + mapped
    terms = [t["term"] for t in q("select term from glossary where location=? or location like ?", name, name + ".%")]
    also = capped("also written by", [f"{w['file']}:{w['line']}" for w in writers], 5, f"sqlite3 .data-context/index.db \"select * from writers where dataset='{name}'\"")
    runs = q("select distinct dag, task, schedule, at from runs where file=? and ? between line_from and line_to order by dag, task",
             d["file"], d["line"] or 1) if d["file"] else []
    runs = [r for r in runs if r["task"]] or runs  # a known task beats "somewhere in this DAG"
    runs = own_runs(con, name, runs)
    files |= {r["at"].rsplit(":", 1)[0] for r in runs}
    sched = capped("runs in", [f"{r['dag']}{'.' + r['task'] if r['task'] else ' (task unknown)'} {r['schedule']} ({r['at']})" for r in runs], 3,
                   f"sqlite3 .data-context/index.db \"select * from runs where file='{d['file']}'\"")
    joins_ = q("""select other, mine, theirs, count(distinct file || ':' || line) n from (
                    select right_dataset other, left_column mine, right_column theirs, file, line from joins where left_dataset=?
                    union all select left_dataset, right_column, left_column, file, line from joins where right_dataset=?)
                  group by 1, 2, 3 order by n desc, other""", name, name)
    joined = [f"{j['other']} on {j['mine']} = {j['theirs']} ({j['n']})" for j in joins_]
    means = []
    for c in q("select concept, column_name, role, confirmed from concepts where dataset=? order by column_name", name):
        home = q("select dataset, column_name from concepts where concept=? and role='home'", c["concept"])
        other = sorted({r[0] for r in q("select column_name from concepts where concept=? and column_name != ?", c["concept"], c["column_name"])})
        means.append(f"{c['column_name']} → {c['concept']}" + ("" if c["confirmed"] else " (proposed)")
                     + (" · its home" if c["role"] == "home" else f" · home {home[0][0]}.{home[0][1]}" if home else "")
                     + (f" · also {' / '.join(other[:4])}" if other else ""))
    used_cols = {r[0] for r in q("select column_name from col_uses where dataset=?", name)} | {r[0].rsplit(".", 1)[1] for r in q("select src from col_edges where src like ?", name + ".%")}
    dead = []  # key-like columns no code reads or joins: a plausible join that the team doesn't use (stale, deprecated, or just unused here)
    if "*" not in used_cols and len(used_cols) >= 1 and joins_:
        dead = [r[0] for r in q("select name from columns where dataset=? order by name", name)
                if r[0] not in used_cols and (stem := re.sub(r"_?(id|ref|key|code|no|uid|uuid)$", "", r[0])) != r[0]
                and not (stem and name.rsplit(".", 1)[-1].startswith(stem))  # bet_id on bets is the table's own id, not a way to join out
                and not q("select 1 from keys where dataset=? and column_name=?", name, r[0])]
    unused = [f"{', '.join(dead[:6])}: no code here reads or joins {'it' if len(dead) == 1 else 'them'}; the code joins this table on {', '.join(dict.fromkeys(j['mine'] for j in joins_[:3]))}"] if dead else []
    values = []
    for (c,) in q("select distinct column_name from value_maps where dataset=? order by column_name", name):
        labels = {}
        for v, l, h, n in q("""select value, label, how, count(distinct file || ':' || line) from value_maps where dataset=? and column_name=?
                               group by 1, 2, 3 order by how = 'alias', 4 desc""", name, c):
            meant = re.sub(r"(_(count|cnt|amount|amt|sum|total|num|id|at|time|date|ts|value|val|usd|eur|gbp|cents|qty|pct|rate))+$", "", l) if h == "alias" else l
            labels.setdefault(v, (l if h == "label" else f"≈ {meant if len(meant) >= 3 else l}", n))
        places = q("select count(distinct file || ':' || line) from value_maps where dataset=? and column_name=?", name, c)[0][0]
        shown = sorted(labels.items(), key=lambda x: (not x[0].lstrip("-").isdigit(), int(x[0]) if x[0].lstrip("-").isdigit() else 0, x[0]))
        values.append(f"{c}: " + ", ".join(f"{v} = {l}" for v, (l, _) in shown[:8]) + (" …" if len(shown) > 8 else "")
                      + f" ({places} place{'s' if places != 1 else ''} in the code)")
    for r in q("select dst_column, src_dataset, src_column, kind, detail from conversions where dst_dataset=? group by 1, 2, 3, 4, 5", name):
        values.append(f"{r['dst_column']} ← {r['src_dataset']}.{r['src_column']} {r['detail']}")
    for r in q("select src_column, dst_dataset, dst_column, detail from conversions where src_dataset=? group by 1, 2, 3, 4", name):
        values.append(f"{r['src_column']} → {r['dst_dataset']}.{r['dst_column']} ({r['detail']})")
    emit([[head] + sched + also, stale_index, about, (["notes:"] + notes) if notes else [],
          capped("upstream", up, 8, f"datawright lineage {name} --up 3"),
          capped("downstream", down, 10, f"datawright lineage {name} --down 9"),
          capped("joins", joined, 6, f"datawright join {name} <other table>"),
          [f"unused keys: {unused[0]}"] if unused else [],
          capped("columns", cols, 30, f"sqlite3 .data-context/index.db \"select * from columns where dataset='{name}'\""),
          capped("means", means, 8, "datawright concepts"),
          capped("values", values, 8, f"sqlite3 .data-context/index.db \"select * from value_maps where dataset='{name}'\""),
          capped("column sources", sources, 12, f"datawright lineage {name}.<column> --up 3"),
          [f"glossary: {', '.join(terms)}"] if terms else []], budget)
    return name, files


def code(con, arg, limit=80):
    """The producing statement and the lines around it: what an agent with a shell would open the file for."""
    name = resolve(con, arg)
    d = con.execute("select file, line from datasets where name=?", (name,)).fetchone()
    path = (ROOT / (d["file"] or "")).resolve()
    repos = json.loads((con.execute("select v from meta where k='repos'").fetchone() or ["[]"])[0])
    if not d["file"] or not any(r.resolve() in path.parents for r, _ in roots(repos)):
        sys.exit(f"No code in this repo produces {name}: it's a source, or it comes from the warehouse")
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    start = max(0, (d["line"] or 1) - 4)  # a few lines above, for the comment that usually explains it
    shown = lines[start:start + limit]
    print(f"{d['file']}:{start + 1}-{start + len(shown)}\n" + "\n".join(shown))
    if start + limit < len(lines):
        print(f"… {len(lines) - start - limit} more lines")
    return name, set()  # a file read, not a saving


def lineage_cmd(con, arg, up, down, limit, fmt="text"):
    name = resolve(con, arg, quiet=True)
    columns = name is None  # not a dataset, so treat it as dataset.column
    if not columns:
        ups = lambda n: [r[0] for r in con.execute("select src from edges where dst=? order by src", (n,))]
        downs = lambda n: [r[0] for r in con.execute("select dst from edges where src=? order by dst", (n,))] + \
            [f"{r[0]}:{r[1]} (reads)" for r in con.execute("select file, line from readers where dataset=? order by file", (n,))]
    else:
        ds, _, col = arg.lower().rpartition(".")
        name = f"{resolve(con, ds)}.{col}" if ds else sys.exit(f"No dataset or column '{arg}'")
        ups = lambda n: [r[0] for r in con.execute("select src from col_edges where dst=? order by src", (n,))]
        downs = lambda n: [r[0] for r in con.execute("select dst from col_edges where src=? order by dst", (n,))]

    def producer(m):  # the file an agent would otherwise open to learn this hop
        if m.endswith("(reads)"):
            return m.rsplit(":", 1)[0]
        r = con.execute("select file from datasets where name=?", (m.rsplit(".", 1)[0] if columns else m,)).fetchone()
        return r[0] if r else None
    out, files = [name], {producer(name)}
    if fmt == "mermaid":  # a graph to paste into a merge request or README: dashed arrows are inferred links
        nodes, edges = walk_edges(con, name, up, down, table=not columns)
        files.update(producer(n) for n in nodes)
        print("\n".join(mermaid_graph(nodes, edges, [name])))
        return name, files - {None}

    def walk(n, nbrs, arrow, depth, seen, lvl=1):
        for m in nbrs(n):
            if len(out) >= limit:
                return
            files.add(producer(m))
            out.append(f"{'  ' * lvl}{arrow} {m}{'  (see above)' if m in seen else ''}")
            if m not in seen and lvl < depth and not m.endswith("(reads)"):
                seen.add(m)
                walk(m, nbrs, arrow, depth, seen, lvl + 1)
    if up:
        walk(name, ups, "↑", up, set())
    if down:
        walk(name, downs, "↓", down, set())
    print("\n".join(out))
    if len(out) >= limit:
        print(f"… cut at {limit} lines (--limit)")
    if down:
        print(f"downstream within {down} hops: {len(reach(name, downs, down))} {'columns' if columns else 'datasets and readers'}")
    return name, files - {None}


def own_runs(con, name, runs):
    """The runs of a table's producing code, narrowed to the tasks that produce this table when a shared script makes many."""
    mine = {r[0] for r in con.execute("select at from calls where dataset=?", (name,))}
    return [r for r in runs if r["at"] in mine] if mine else list(runs)


def backfill(con, arg, start=None, end=None):
    """The plan to refill a table and everything downstream of it for a date range: the Airflow tasks to clear, upstream
    first, in waves, and what needs care before running them. Prints commands; runs nothing."""
    name = resolve(con, arg)
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    ups, downs = {}, {}
    for s, d in q("select src, dst from edges where src != dst"):
        downs.setdefault(s, set()).add(d)
        ups.setdefault(d, set()).add(s)
    kind = dict(q("select name, kind from datasets"))
    every = {name} | reach(name, lambda n: downs.get(n, ()), 9)
    skipped = sorted(n for n in every if ":" in n or kind.get(n) == "view")  # topics, dashboards, views: nothing to refill
    tables = every - set(skipped)
    order, waves, left = {}, [], set(tables)
    while left:  # waves: a table goes once everything it reads from the set has gone
        wave = sorted(n for n in left if not (ups.get(n, set()) & left))
        if not wave:  # a cycle: put the rest together, lint reports it
            wave = sorted(left)
        waves.append(wave)
        left -= set(wave)
    s, e = start or "<start>", end or "<end>"
    keyed = {r[0] for r in q("select dataset from modes where mode='keyed' union select dataset from keys where source='ddl'")}
    care, no_task, files = [], [], set()
    print(f"backfill plan for {name} and {len(tables) - 1} downstream tables, {s} to {e}. Nothing is run: review it, then run the commands")
    for i, wave in enumerate(waves, 1):
        by_dag = {}
        print(f"wave {i}" + (" (after the wave before has finished)" if i > 1 else ""))
        for n in wave:
            d = con.execute("select * from datasets where name=?", (n,)).fetchone()
            runs = own_runs(con, n, q("select dag, task, schedule, at from runs where file=? and ? between line_from and line_to and task != ''",
                                      d["file"], d["line"] or 1)) if d and d["file"] else []
            runs = [r for r in runs if not re.search(r"backfill|adhoc|ad_hoc", r["dag"], re.I)] or runs  # the regular task, if there is one
            if d and d["file"]:
                files.add(d["file"])
            if not runs:
                no_task.append(n)
                print(f"  {n}  no Airflow task builds it" + (f": rerun {d['file']}:{d['line']} for the range by hand" if d and d["file"] else ""))
                continue
            r = runs[0]
            by_dag.setdefault(r["dag"], []).append(r["task"])
            print(f"  {n}  {r['dag']}.{r['task']}  {r['schedule']}")
            modes = {m[0] for m in q("select mode from modes where dataset=? and file=?", n, d["file"])}
            if "append" in modes and not modes & {"overwrite", "delete", "merge", "replace", "update"} and n not in keyed:
                care.append(f"{n} appends ({d['file']}:{d['line']}): delete {s} to {e} from it first, or the rerun duplicates those rows")
            if any(u.startswith("kafka:") for u in ups.get(n, ())):
                alt = sorted({f"{x['dag']}.{x['task']}" for w in q("select file, line from writers where dataset=? and file != ?", n, d["file"])
                              for x in own_runs(con, n, q("select dag, task, schedule, at from runs where file=? and ? between line_from and line_to and task != ''", w["file"], w["line"] or 1))})
                care.append(f"{n} is fed from Kafka: clearing the task restarts the job; the data comes back only if the topic still holds {s} to {e} (check retention, or reset the consumer offsets)"
                            + (f". Or reload it with {', '.join(alt[:3])}, which also writes it" if alt else ""))
        for dag, tasks in sorted(by_dag.items()):
            rx = tasks[0] if len(tasks) == 1 else "(" + "|".join(sorted(set(tasks))) + ")"
            print(f"    airflow tasks clear {dag} -t '^{rx}$' -s {s} -e {e} -y")
    if care:
        print("before running:")
        for c in care:
            print(f"  {c}")
    if skipped:
        print(f"not refilled: {len(skipped)} views, topics and dashboards, which show the new data once their sources have it")
    print("clear reruns days the DAG has already run; for days it never ran, use `airflow dags backfill` (Airflow 2) or `airflow backfill create` (Airflow 3)")
    print(f"after: datawright check {' '.join(sorted(tables)[:8])}{' …' if len(tables) > 8 else ''}")
    return name, files


