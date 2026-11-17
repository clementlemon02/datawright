def search(con, words):
    hits = {}
    def add(name, score, why):
        h = hits.setdefault(name, [0, []])
        h[0] += score
        if why not in h[1]:
            h[1].append(why)
    for w in words:
        like = f"%{w.lower()}%"
        for r in con.execute("select name from datasets where name like ?", (like,)):
            add(r[0], 3, "name")
        for r in con.execute("select name from datasets where lower(coalesce(description,'') || ' ' || coalesce(grain,'')) like ?", (like,)):
            add(r[0], 2, "description")
        for r in con.execute("select dataset, name from columns where name like ? or lower(coalesce(description,'')) like ?", (like, like)):
            add(r[0], 1, f"column {r[1]}")
        for r in con.execute("select dataset from notes where lower(text) like ?", (like,)):
            add(r[0], 2, "note")
        for r in con.execute("""select distinct d.name, r.dag, r.task from runs r join datasets d
                                on d.file = r.file and coalesce(d.line, 1) between r.line_from and r.line_to
                                where lower(r.dag) like ? or lower(r.task) like ?""", (like, like)):
            add(r[0], 2, f"dag {r[1]}.{r[2]}")
    for r in con.execute("select term, meaning, location from glossary"):
        if any(w.lower() in (r[0] + " " + r[1]).lower() for w in words):
            print(f"glossary: {r[0]} = {r[1]}" + (f"  ({r[2]})" if r[2] else ""))
    for name, (score, why) in sorted(hits.items(), key=lambda x: -x[1][0])[:15]:
        print(f"{name}  ({'; '.join(why[:3])})")
    if not hits:
        print("no matches")
    return " ".join(words), set()  # search replaces grep, which is cheap, so it claims no savings


LAYERS = [  # common warehouse layer conventions, earliest first; `layers:` in config.yml sets a team's own
    {"ods": 0, "dwd": 1, "dwm": 2, "dw": 2, "dws": 3, "dm": 4, "ads": 4, "app": 4},
    {"raw": 0, "landing": 0, "staging": 1, "stg": 1, "base": 1, "intermediate": 2, "int": 2, "core": 3, "marts": 4, "mart": 4, "reporting": 5},
    {"bronze": 0, "silver": 1, "gold": 2, "platinum": 3},
]
SECRETS = [  # (what, pattern): reported by file and line, never the value
    ("password or key in code", re.compile(r"""(?i)\b(password|passwd|pwd|secret|secret_key|api_key|apikey|access_key|auth_token|token)\b["']?\s*[:=]\s*["']([^"'\s$%{}<>]{6,})["']""")),
    ("credentials in a connection URL", re.compile(r"""\b[a-z][a-z0-9+.-]*://[^/\s:@'"]+:([^@\s'"/$%{}<>]{4,})@""")),
    ("AWS access key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("private key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")),
    ("access token", re.compile(r"\b(?:xox[bpas]-[0-9A-Za-z-]{10,}|ghp_[0-9A-Za-z]{36}|github_pat_\w{20,}|glpat-[\w-]{20})")),
]
NAME_LIKE = re.compile(r"[a-z]+(?:[_.-][a-z]+)*|[A-Z][A-Z0-9_]*|0x[0-9a-fA-F]{40}")  # a reference (secret name, env var), or a chain address
PLACEHOLDER = re.compile(r"(?i)^(password|passwd|changeme|change_me|secret|example|dummy|test\w*|none|null|xx+|\*+|your\w*|redacted|placeholder)$")


def type_family(t):
    """The kind of a column type, so BIGINT and INT64 agree and BIGINT and VARCHAR don't. None when unknown."""
    t = (t or "").lower().strip()
    for fam, rx in (("integer", r"^number\(\d+(,\s*0)?\)$|int|long|serial"), ("decimal", r"dec|numeric|number|money|float|double|real"),
                    ("timestamp", r"timestamp|datetime"), ("date", r"^date"), ("text", r"char|string|text|clob|uuid|json"),
                    ("boolean", r"bool|^bit"), ("binary", r"binary|blob|bytes")):
        if t and re.search(rx, t):
            return fam
    return None


def cron_offset(schedule):
    """(period in minutes, minutes into the period it starts) for simple daily and hourly schedules, else None."""
    s = (schedule or "").strip().lower()
    if s in ("@daily", "@midnight"):
        return 1440, 0
    if s == "@hourly":
        return 60, 0
    f = s.split()
    if len(f) == 5 and f[0].isdigit() and f[2:] == ["*", "*", "*"]:
        return (1440, int(f[1]) * 60 + int(f[0])) if f[1].isdigit() else (60, int(f[0])) if f[1] == "*" else None
    return None


def practices(con, files=None):
    """Data engineering best practices checked from the index: [(rule, "file:line", message)]. files: only those files."""
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    keep = lambda f: files is None or f in files
    out = []
    keyed = {r[0] for r in q("select dataset from modes where mode='keyed' union select dataset from keys where source='ddl'")}  # upserts
    streaming = {r[0] for r in q("select distinct dst from edges where src like 'kafka:%'")}

    def scheduled(file, line):  # the scheduled tasks that run this code
        rows = q("select dag, task, schedule, at from runs where file=? and ? between line_from and line_to and task != ''", file, line or 1)
        return [r for r in rows if (r["schedule"] or "manual").lower() not in ("manual", "none", "@once")]

    # 1. reruns that duplicate: an append with nothing clearing its slice, in a scheduled task (a retry runs it twice)
    for ds, f in q("select distinct dataset, file from modes where mode='append'"):
        modes = {r[0] for r in q("select mode from modes where dataset=? and file=?", ds, f)}
        if keep(f) and not modes & {"overwrite", "delete", "merge", "replace", "update"} and ds not in keyed | streaming \
                and not TEST_FILE.search(f) and (runs := own_runs(con, ds, scheduled(f, q("select min(line) from modes where dataset=? and file=?", ds, f)[0][0]))):
            ln = q("select min(line) from modes where dataset=? and file=? and mode='append'", ds, f)[0][0]
            out.append(("rerun duplicates rows", f"{f}:{ln}", f"{ds}: appends in {runs[0]['dag']}.{runs[0]['task']} ({runs[0]['schedule']}) "
                        "with nothing clearing the slice first; a retry or rerun adds the rows again. Overwrite the partition, delete it first, or MERGE on a key"))
    for d in q("select name, file from datasets where kind='dbt model' and file is not null"):
        if keep(d["file"]) and (ROOT / d["file"]).is_file():
            head = (ROOT / d["file"]).read_text(encoding="utf-8", errors="replace")[:3000]
            if re.search(r"materialized\s*=\s*['\"]incremental", head) and not re.search(r"unique_key|insert_overwrite|microbatch", head):
                out.append(("rerun duplicates rows", f"{d['file']}:1", f"{d['name']}: incremental model with no unique_key; rerunning a window appends it twice"))
    # 2. several scheduled DAGs write one table: they race, and nobody owns it
    for ds in [r[0] for r in q("select dataset from writers group by dataset having count(*) > 1")]:
        dags = {}
        for w in q("select file, line from writers where dataset=?", ds):
            for r in own_runs(con, ds, scheduled(w["file"], w["line"])):
                if not re.search(r"backfill|adhoc|ad_hoc|manual|init", r["dag"] + r["task"], re.I):
                    dags.setdefault(r["dag"], f"{w['file']}:{w['line']}")
        if len(dags) > 1 and any(keep(at.rsplit(":", 1)[0]) for at in dags.values()):
            out.append(("several writers", next(iter(dags.values())), f"{ds}: written on a schedule by {len(dags)} DAGs ({', '.join(sorted(dags)[:4])}); they can race and overwrite each other"))
    # 3. INSERT ... SELECT * into a stored table: an upstream column added or reordered shifts this table's columns silently
    for r in q("""select distinct w.dataset, w.file, w.line from writers w join col_uses u on u.file = w.file and u.line = w.line
                  join datasets d on d.name = w.dataset where u.column_name = '*' and d.kind not in ('view', 'dbt model')"""):
        if keep(r["file"]) and not TEST_FILE.search(r["file"]):
            out.append(("select * into a table", f"{r['file']}:{r['line']}", f"{r['dataset']}: written with SELECT *; list the columns so an upstream change can't shift them"))
    # 4. cycles in lineage (a table that feeds itself through others)
    graph = {}
    for s, d in q("select src, dst from edges where src != dst"):
        graph.setdefault(s, set()).add(d)
    index, low, stack, on, comps, counter = {}, {}, [], set(), [], [0]
    for root in list(graph):  # Tarjan's strongly connected components, iterative
        if root in index:
            continue
        work = [(root, iter(graph.get(root, ())))]
        index[root] = low[root] = counter[0]; counter[0] += 1; stack.append(root); on.add(root)
        while work:
            v, it = work[-1]
            w = next(it, None)
            if w is not None:
                if w not in index:
                    index[w] = low[w] = counter[0]; counter[0] += 1; stack.append(w); on.add(w)
                    work.append((w, iter(graph.get(w, ()))))
                elif w in on:
                    low[v] = min(low[v], index[w])
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[v])
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop(); on.discard(w); comp.append(w)
                    if w == v:
                        break
                if len(comp) > 1:
                    comps.append(sorted(comp))
    for comp in comps:
        at = q(f"select via from edges where src in ({','.join('?' * len(comp))}) and dst in ({','.join('?' * len(comp))}) limit 1", *comp, *comp)
        f = (at[0][0] if at else "").rsplit(":", 1)[0]
        if keep(f):
            out.append(("lineage cycle", at[0][0] if at else "", f"{' → '.join(comp[:5])}{' …' if len(comp) > 5 else ''} feed each other; a backfill can't order them"))
    # 5. schedules: a table read before its upstream is built, or built only by hand
    task_of, kind_of = {}, dict(q("select name, kind from datasets"))
    for d in q("select name, file, line from datasets where file is not null"):
        runs = own_runs(con, d["name"], scheduled(d["file"], d["line"]))
        task_of[d["name"]] = runs[0] if runs else None
    for s, d, via in q("select src, dst, via from edges where src != dst and via like '%:%'"):
        f, ln = via.rsplit(":", 1)
        rs = task_of.get(s)  # judged by the task that runs the reading code: a backfill reading a table is no schedule
        rd = next(iter(own_runs(con, d, scheduled(f, int(ln) if ln.isdigit() else 1))), None)
        if not rd or not keep(f):
            continue
        dag_file = rd["at"].rsplit(":", 1)[0]
        waits = (ROOT / dag_file).is_file() and re.search(r"Sensor|wait_for|ExternalTask|Dataset\(|Asset\(|TriggerDagRun", (ROOT / dag_file).read_text(errors="replace"))
        if rs and rs["dag"] != rd["dag"] and not waits:
            a, b = cron_offset(rs["schedule"]), cron_offset(rd["schedule"])
            if a and b and a[0] == b[0] and b[1] < a[1]:
                out.append(("reads before upstream is built", via, f"{d} ({rd['dag']} {rd['schedule']}) reads {s}, built later in the {'day' if a[0] == 1440 else 'hour'} "
                            f"({rs['dag']} {rs['schedule']}), with no sensor: it reads the previous run. Move it after, or wait on the upstream"))
        elif rs is None and s in task_of and not TEST_FILE.search(f) and kind_of.get(s) != "view":  # a view computes on read
            out.append(("scheduled table, unscheduled upstream", via, f"{d} ({rd['dag']} {rd['schedule']}) reads {s}, which no scheduled task builds: it goes stale unless someone runs it"))
    # 6. layers: lineage running backwards (a mart feeding staging), from `layers:` in config.yml or a convention the names follow
    names = [r[0] for r in q("select name from datasets")]
    cfg = [x.strip().strip("'\"") for x in re.findall(r"[\w-]+", setting("layers", "").strip("[]"))]
    conventions = [{x: i for i, x in enumerate(cfg)}] if cfg else LAYERS

    def layer(n, conv):  # a schema named for the layer (ods.orders), or a table prefixed with it (ods_orders)
        for part in n.lower().replace(":", ".").split("."):
            for word in (part, part.split("_")[0]):
                if word in conv:
                    return conv[word]
        return None
    used = [c for c in conventions if len({layer(n, c) for n in names} - {None}) >= 2]  # the conventions this repo's names follow
    for s, d, via in q("select src, dst, via from edges where src != dst"):
        for conv in used:
            ls, ld = layer(s, conv), layer(d, conv)
            if ls is not None and ld is not None and ls > ld and keep((via or "").rsplit(":", 1)[0]):
                out.append(("layer order", via, f"{d} reads {s}, from a later layer; data should flow {' → '.join(sorted(conv, key=conv.get)[:6])}"))
                break
    # 8. concept drift: one concept under several names or kinds of type (the dictionary rots one table at a time)
    try:
        import yaml
        doc = yaml.safe_load((HOME / "concepts.yml").read_text()) if (HOME / "concepts.yml").exists() else {}
        said = {k["name"]: k for k in (doc or {}).get("concepts") or [] if isinstance(k, dict) and k.get("confirmed") and k.get("name")}
    except Exception:
        said = {}
    squash = lambda s: re.sub(r"[^a-z0-9]", "", s.lower())
    for concept in [r[0] for r in q("select distinct concept from concepts")]:
        rows = q("""select c.dataset, c.column_name, c.role, k.type, d.file, d.line, d.schema_at from concepts c
                    left join columns k on k.dataset = c.dataset and k.name = c.column_name left join datasets d on d.name = c.dataset
                    where c.concept=?""", concept)
        k = said.get(concept, {})
        names = Counter(r["column_name"] for r in rows if r["column_name"] != "id" and r["role"] != "proposed")
        canonical = k.get("canonical") or (max(names, key=lambda c: (names[c], c.endswith("_id"), c)) if names else None)
        roles = {str(x).lower() for x in k.get("roles") or []}
        fams = Counter(f for r in rows if r["role"] != "proposed" and (f := type_family(r["type"])))
        usual = type_family(k.get("type")) or (fams.most_common(1)[0][0] if fams else None)
        usual_type = k.get("type") or next((r["type"] for r in rows if type_family(r["type"]) == usual), None)
        for r in rows:
            at = f"{r['file']}:{r['line']}" if r["file"] else r["schema_at"] or ""
            if not at or not keep(at.rsplit(":", 1)[0]) or TEST_FILE.search(at):
                continue
            where_ = f"{r['dataset']}.{r['column_name']}"
            fam = type_family(r["type"])
            if fam and usual and fam != usual and sum(fams.values()) >= 2:
                out.append(("concept type differs", at, f"{where_} is {r['type']}, but {concept} is {usual_type} in {fams[usual]} other tables: joins need a cast and lose index and partition pruning"))
            c = r["column_name"]
            if canonical and c not in (canonical, "id") and c not in roles and (squash(c) == squash(canonical) or (k and c not in {x.rsplit(".", 1)[-1] for x in k.get("columns") or []})):
                out.append(("concept name differs", at, f"{where_} holds {concept}, which the team calls {canonical}"
                            + ("" if squash(c) == squash(canonical) else f": rename it, or add {c} to its roles in concepts.yml")))
    # 9. one code, different labels: status 1 is 'active' in one report and 'enabled' in another
    for ds, col, v in q("""select dataset, column_name, value from value_maps where how='label'
                           group by 1, 2, 3 having count(distinct lower(label)) > 1"""):
        rows = q("select label, file, line from value_maps where dataset=? and column_name=? and value=? and how='label' order by file, line", ds, col, v)
        common = Counter(r["label"].lower() for r in rows).most_common(1)[0][0]
        for r in rows:
            if r["label"].lower() != common and keep(r["file"]):
                out.append(("value labels differ", f"{r['file']}:{r['line']}", f"{ds}.{col} = {v} is labelled '{r['label']}' here but '{common}' "
                            f"in {sum(x['label'].lower() == common for x in rows)} other places: reports will disagree"))
    # 7. secrets written into the code
    for (f,) in q("select path from files"):
        if not keep(f) or TEST_FILE.search(f) or Path(f).suffix.lower() in NOISE - {".sh"} or f.startswith(".data-context/"):
            continue
        try:
            lines = (ROOT / f).read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for i, line in enumerate(lines, 1):
            if re.match(r"\s*(#|//|--|;|\*|/\*)", line):  # comments hold examples, not credentials
                continue
            for what, rx in SECRETS:
                m = rx.search(line)
                val = m.group(m.lastindex) if m and m.lastindex else ""
                if m and what == SECRETS[0][0] and m.group(1).lower() not in ("password", "passwd", "pwd") \
                        and (NAME_LIKE.fullmatch(val) or not re.search(r"\d", val) or len(val) < 12):
                    continue  # token = "access_token", secret = "sftp-password": names a secret, doesn't hold one
                if m and what == SECRETS[0][0] and re.fullmatch(r"[A-Z][A-Z0-9_]*", val):
                    continue  # password = "DB_PASSWORD": an environment variable's name
                if m and not (val and PLACEHOLDER.match(val)):
                    out.append(("secret in code", f"{f}:{i}", f"{what}: move it to the platform's secret store or an environment variable, and rotate it"))
                    break
    return out


def lint(con):
    q = lambda sql: con.execute(sql).fetchall()
    stale = q("select dataset, column_name, date, kind, text, stale from notes where stale != '' order by dataset")
    print(f"stale or orphaned notes: {len(stale)}")
    for n in stale[:20]:
        print(f"  {n['dataset']}{'.' + n['column_name'] if n['column_name'] else ''}: {n['text'][:80]}  ({n['stale']})")
    hot = q("""select d.name, (select count(*) from edges e where e.src = d.name) + (select count(*) from readers r where r.dataset = d.name)
               + d.uses as score from datasets d where d.grain is null and d.description is null order by score desc limit 10""")
    hot = [h for h in hot if h["score"]]
    print(f"most-used datasets with no recorded grain or meaning: {len(hot)}")
    for h in hot:
        print(f"  {h['name']}  (used by {h['score']})")
    crit = q("""select d.*, (select count(*) from edges e where e.src = d.name) + (select count(*) from readers r where r.dataset = d.name)
                + d.uses as used from datasets d where d.file is not null order by coalesce(d.tier, '9'), used desc limit 40""")
    ranked = sorted(((readiness(con, d), d) for d in crit), key=lambda x: (x[1]["tier"] or "9", x[0][0]))
    weak = [(s, d) for s, d in ranked if s[0] < 70]
    print(f"least ready of the critical and most-used tables (below 70/100): {len(weak)}")
    for (score, missing), d in weak[:10]:
        print(f"  {d['name']}  {score}/100{' · tier ' + d['tier'] if d['tier'] else ''} · missing: {', '.join(missing[:3])}")
    breaks = [(r[0], c, s) for r in q("select distinct dataset from contracts") for c, s in contract_breaks(con, r[0])]
    print(f"broken contracts: {len(breaks)}")
    for ds, c, s in breaks[:10]:
        print(f"  {ds}: required column {c} not produced ({s})")
    unread = q("""select name, file from datasets d where file is not null and uses = 0
                  and not exists (select 1 from edges e where e.src = d.name) and not exists (select 1 from readers r where r.dataset = d.name)
                  and not exists (select 1 from col_uses u where u.dataset = d.name) order by name""")
    print(f"produced but read by nothing in the repo or imported lineage (deprecation candidates; check the query log first): {len(unread)}")
    for u in unread[:15]:
        print(f"  {u['name']}  ({u['file']})")
    found = practices(con)
    print(f"best practices: {len(found)} findings" + (f" ({', '.join(f'{r} {n}' for r, n in Counter(r for r, *_ in found).most_common())})" if found else ""))
    for rule in dict.fromkeys(r for r, *_ in found):
        hits = [(at, msg) for r, at, msg in found if r == rule]
        print(f"  {rule}:")
        for at, msg in hits[:5]:
            print(f"    {at}  {msg}")
        if len(hits) > 5:
            print(f"    … {len(hits) - 5} more")
    meta = dict(q("select k, v from meta"))
    print(f"unparsed statements: {meta.get('unparsed', 0)} of {meta.get('statements', 0)}"
          + ("  (set `dialect:` in .data-context/config.yml if most fail)" if int(meta.get("unparsed", 0) or 0) else ""))
    for r in q("select file, count(*) n, min(error) e from issues group by file order by n desc limit 5"):
        print(f"  {r['file']}: {r['n']}  e.g. {r['e'][:90]}")


