def build(dialect=None):
    import yaml
    started = time.time()
    HOME.mkdir(exist_ok=True)
    ign = HOME / ".gitignore"
    if not ign.exists():
        ign.write_text("# generated or local-only; imports can contain query text with real values\nindex.db\nindex.tmp\nimports/\nusage.jsonl\n")
    cfg = yaml.safe_load((HOME / "config.yml").read_text()) or {} if (HOME / "config.yml").exists() else {}
    dialect = dialect or cfg.get("dialect")
    auto = not dialect
    repos = [str(r) for r in cfg.get("repos") or []]
    files = repo_files(cfg.get("exclude") or [], repos) + export_files()
    projects = [str(Path(f).parent) for f in files if Path(f).name == "dbt_project.yml"]
    macros = dbt_macros(files, projects)
    versioned = set()  # dbt models that declare `versions:`: orders_v1.sql and orders_v2.sql are both the model `orders`
    for f in files:
        if f.endswith((".yml", ".yaml")) and any(Path(p) in Path(f).parents for p in projects):
            try:
                body = (ROOT / f).read_text(encoding="utf-8", errors="replace")
                if "versions:" in body:
                    versioned |= {str(m["name"]).lower() for m in (yaml.safe_load(body) or {}).get("models") or [] if isinstance(m, dict) and m.get("versions")}
            except Exception:
                pass
    ix, stmts, file_set = Index(), [], set(files)
    shared, exprs = {}, {}  # JVM constants by name, when unique across files: jobs read topics from a Config class
    for f in (f for f in files if f.endswith((".java", ".scala", ".kt"))):
        jt = (ROOT / f).read_text(encoding="utf-8", errors="replace")
        for k, v in jvm_constants(jt).items():
            shared[k] = v if k not in shared or shared[k] == v else None
        for k, v in jvm_exprs(jt).items():
            exprs[k] = v if k not in exprs or exprs[k] == v else None
    env = env_values((ROOT / f).read_text(encoding="utf-8", errors="replace") for f in files
                     if Path(f).name.startswith(".env") or Path(f).suffix in (".env", ".properties", ".yaml", ".yml"))
    # ponytail: full rebuild every time (~7 ms a file with column lineage); cache parses by file hash if repos outgrow it
    connectors, problems = load_connectors()
    for p in problems:
        ix.issue(*p)
    for f in files:
        suffix = Path(f).suffix.lower()
        mine = []
        for name, match, extract, owns in connectors:
            try:
                if match(f):
                    mine.append((name, extract, owns))
            except Exception as e:
                ix.issue(f, 0, f"connector {name}: {redact(e)}")
        builtin = suffix in (".sql", ".hql", ".py", ".ipynb", ".java", ".scala", ".kt") and not any(o for *_, o in mine)  # OWNS: the connector reads it instead
        if not builtin and not mine:
            continue
        text = (ROOT / f).read_text(encoding="utf-8", errors="replace")
        ok = []
        for name, extract, _ in mine:  # a connector's own reading of the file, alongside the built-in one unless it OWNS it
            try:
                stmts += add_records(ix, name, f, extract(f, text))
                ok.append(f"connector {name}")
            except Exception as e:
                ix.issue(f, 0, f"connector {name}: {redact(e)}")
        ix.read_by[f] = ", ".join(ok + ([suffix[1:]] if builtin else []))
        if not builtin:
            continue
        if suffix in (".java", ".scala", ".kt"):
            stmts += extract_jvm(ix, f, text, {k: v for k, v in shared.items() if v is not None}, {k: v for k, v in exprs.items() if v}, env)
        elif suffix == ".py":
            stmts += extract_python(ix, f, text, file_set)
        elif suffix == ".ipynb":
            stmts += extract_python(ix, f, notebook_code(text), file_set)
        else:
            in_project = [p for p in projects if Path(p) in Path(f).parents]
            if in_project and "macros" in Path(f).relative_to(in_project[0]).parts:
                ix.read_by[f] = "dbt macro"  # templates, not queries
                continue
            in_dbt = bool(in_project) and bool({"models", "snapshots"} & set(Path(f).parts))
            if in_dbt:  # ref() and source() name a model's inputs even when heavy Jinja keeps its SQL from parsing
                model = Path(f).stem.lower()
                ix.dataset(model, "dbt model", file=f, line=1)
                ix.writers.setdefault((model, f), 1)
                live = re.sub(r"\{#.*?#\}", lambda c: re.sub(r"[^\n]", " ", c[0]), text, flags=re.S)  # Jinja comments out, lines kept
                for block in re.finditer(r"\{\{.*?\}\}|\{%.*?%\}", live, re.S):  # anywhere in an expression or block: a macro's argument, a set
                    at = lambda m: f"{f}:{text.count(chr(10), 0, block.start() + m.start()) + 1}"
                    for m in DBT_REF.finditer(block[0]):
                        if m[1].lower() != model:
                            ix.edge(m[1].lower(), model, at(m))
                    for m in DBT_SOURCE.finditer(block[0]):
                        ix.dataset(f"{m[1]}.{m[2]}".lower())
                        ix.edge(f"{m[1]}.{m[2]}".lower(), model, at(m))
                    for m in re.finditer(r"(?<![\w.])(\w+)\s*\(", block[0]):  # a macro the model calls: what the macro reads, the model reads
                        for src in macros.get(in_project[0], {}).get(m[1], ()):
                            if src != model:
                                ix.dataset(src)
                                ix.edge(src, model, at(m), "inferred")
            if in_dbt:  # a loop over a literal list: {% for t in ["a", "b"] %} ... source('s', t) ... {% endfor %}, read once per value
                lists = {m[1]: re.findall(r"""['"]([^'"]+)['"]""", m[2]) for m in re.finditer(r"\{%-?\s*set\s+(\w+)\s*=\s*\[(.*?)\]\s*-?%\}", live, re.S)}
                for loop in re.finditer(r"\{%-?\s*for\s+(\w+)\s+in\s+(?:\[(.*?)\]|(\w+))\s*-?%\}(.*?)\{%-?\s*endfor\s*-?%\}", live, re.S):
                    values = re.findall(r"""['"]([^'"]+)['"]""", loop[2]) if loop[2] is not None else lists.get(loop[3], [])
                    where = f"{f}:{text.count(chr(10), 0, loop.start()) + 1}"
                    for v in values[:500]:
                        body = re.sub(rf"\b{loop[1]}\b", f"'{v}'", loop[4])
                        for m in DBT_REF.finditer(body):
                            if m[1].lower() != model:
                                ix.edge(m[1].lower(), model, where, "inferred")
                        for m in DBT_SOURCE.finditer(body):
                            ix.dataset(f"{m[1]}.{m[2]}".lower())
                            ix.edge(f"{m[1]}.{m[2]}".lower(), model, where, "inferred")
            stmts += extract_sql(text, f, dbt_model=Path(f).stem.lower() if in_dbt else None)
            if in_dbt:
                dbt_layout(ix, text, Path(f).stem.lower(), f)
    for base in versioned:
        for name in [n for n in ix.ds if re.fullmatch(rf"{re.escape(base)}_v\d+", n)]:
            ix.aliases[name] = base
    seen = {(s[0], s[1], s[2]) for s in stmts}
    for f, args, at in sorted(set(ix.calls), key=repr):  # each task's arguments read into the script: one lineage per call
        text = (ROOT / f).read_text(encoding="utf-8", errors="replace")
        if re.search(r"\bsys\.argv\b|\bparse(_known)?_args\(|@click\.", text):
            stmts += [s for s in extract_python(ix, f, text, file_set, argv=list(args)) if (s[0], s[1], s[2]) not in seen]
    handled = set()
    for _ in range(6):  # calls across files, a hop a round: a file read with its callers' values passes them on to the next
        fresh = [c for c in dict.fromkeys(ix.fcalls) if c not in handled]
        if not fresh:
            break
        handled.update(fresh)
        binds = {}  # file -> {function: [(args, keywords)]}: other files' calls into it
        for f, fn, vals, kws in fresh:
            binds.setdefault(f, {}).setdefault(fn, []).append((vals, kws))
        for f, bind in sorted(binds.items()):
            stmts += [s for s in extract_python(ix, f, (ROOT / f).read_text(encoding="utf-8", errors="replace"), file_set, bind=bind)
                      if (s[0], s[1], s[2]) not in seen]
    stmts += load_imports(ix)
    connector_tables(ix, stmts)
    dialect = dialect or detect_dialect(stmts, dialect_hint(files))
    parse_all(ix, stmts, dialect)
    load_dbt(ix, projects)
    dbt_tags(ix, projects, files)
    ix.settle_columns()
    curated, notes, glossary = load_curated()
    changes = git_changes(repos)

    names = set(ix.ds) | {n for n, _ in ix.cols} | set(curated) | {n for n, *_ in notes}
    late = {**ix.env, **{str(k).lower(): str(v).lower() for k, v in (cfg.get("env") or {}).items()}}  # config.yml wins over connectors
    for n in [n for n in names if "://{bucket}/" in n]:  # s3://{bucket}/x is s3://<the one bucket holding x>/x
        scheme, rest = n.split("://{bucket}/")
        if len(m := [x for x in names if x.startswith(scheme + "://") and x.split("/", 3)[3:] == [rest] and "{bucket}" not in x]) == 1:
            ix.aliases.setdefault(n, m[0])
    wanted = {v for n in names for v in re.findall(r"env:(\w+)", n)}
    unused_env = sorted(set(late) - wanted - {k.lower() for k in env})  # values for variables no code reads: a typo, or stale docs
    for n in [n for n in names if "env:" in n]:  # kafka:env:TOPIC_VAR -> kafka:<its value>, once the value is known
        if (to := re.sub(r"env:(\w+)", lambda m: late.get(m[1], m[0]), n)) != n:
            ix.aliases.setdefault(n, to)
            names.add(to)
    final = resolver(names, ix.aliases)
    split = lambda dc: (final(dc.rsplit(".", 1)[0]) + "." + dc.rsplit(".", 1)[1]) if "." in dc else dc

    datasets = {}
    for name, d in ix.ds.items():
        if final(name) not in datasets:
            datasets[final(name)] = dict(d)
            continue
        m = datasets[final(name)]
        if RANK.get(d["kind"], 0) > RANK.get(m["kind"], 0):
            m["kind"] = d["kind"]
        for k in ("file", "line", "description", "rows"):
            m[k] = m[k] or d[k]
        m["uses"] += d["uses"]
    writers = {}
    for (ds, f), ln in ix.writers.items():
        writers.setdefault(final(ds), []).append((f, ln))
    for name, ws in writers.items():  # the main producer: not a notebook, backfill/adhoc/test script if there's a choice
        if name in datasets:
            datasets[name]["file"], datasets[name]["line"] = min(ws, key=lambda w: (w[0].endswith(".ipynb"), bool(SIDE_JOB.search(w[0])), w[0]))
    for name, c in curated.items():
        d = datasets.setdefault(final(name), {"kind": "source", "file": None, "line": None, "description": None, "rows": None, "uses": 0})
        d.update({k: v for k, v in c.items()})
    for n in notes:
        ds = final(n[0])
        prod = changes.get(datasets.get(ds, {}).get("file") or "")
        n[0] = ds
        n.append("dataset not found" if ds not in datasets else f"code changed {prod[1]}" if prod and n[2] and prod[1] > n[2] else "")

    tmp = HOME / "index.tmp"
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(tmp)
    con.executescript(SCHEMA)
    for t, (f, ln) in ix.ddl.items():
        if final(t) in datasets:
            datasets[final(t)].setdefault("schema_at", f"{f}:{ln}")
    con.executemany("insert into datasets values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(n, d["kind"], d["file"], d["line"], d["description"], d.get("grain"), d.get("key"), d.get("owner"),
                      d.get("verified"), d["rows"], d["uses"], d.get("schema_at"), d.get("freshness"), d.get("tier")) for n, d in datasets.items()])
    tested = {(final(ds), src) for ds, src in ix.tests} | {(final(ds), f) for (ds, f) in ix.readers if TEST_FILE.search(f)}
    con.executemany("insert into tests values (?,?)", sorted(tested))
    con.executemany("insert into contracts values (?,?,?,?,?)", [(final(ds), c, typ, req, src) for ds, c, typ, req, src in ix.contracts])
    con.executemany("insert or ignore into columns values (?,?,?,?)", [(final(ds), c, t, desc) for (ds, c), (t, desc) in ix.cols.items()])
    con.executemany("insert or ignore into edges values (?,?,?,?)", [(final(s), final(d), v, ix.how.get((s, d), "direct")) for (s, d), v in ix.edges.items() if final(s) != final(d)])
    con.executemany("insert or ignore into col_edges values (?,?)", [(split(s), split(d)) for s, d in ix.col_edges])
    con.executemany("insert or ignore into readers values (?,?,?)", [(final(ds), f, ln) for (ds, f), ln in ix.readers.items()])
    con.executemany("insert or ignore into writers values (?,?,?)", [(final(ds), f, ln) for (ds, f), ln in ix.writers.items()])
    con.executemany("insert into notes values (?,?,?,?,?,?,?)", notes)
    used = {d["file"] for d in datasets.values() if d["file"]} | {f for _, f in ix.writers} | {f for f, _ in ix.ddl.values()}
    con.executemany("insert into changes values (?,?,?,?)", [(f, *c) for f, c in changes.items() if f in used])
    con.executemany("insert into glossary values (?,?,?)", [(t, m, split(w) if w and final(w) not in datasets else final(w)) for t, m, w in glossary])
    con.executemany("insert into issues values (?,?,?)", ix.issues)
    if ix.selectors:  # dbt run -s ... in DAG tasks, against the finished graph
        models = {n: d["file"] for n, d in datasets.items() if d["kind"] in ("dbt model", "seed", "snapshot") and d.get("file")}
        ups, downs = {}, {}
        for s, d_ in ((final(s), final(d_)) for s, d_ in ix.edges):
            downs.setdefault(s, set()).add(d_)
            ups.setdefault(d_, set()).add(s)
        tags = {}
        for n, ts in ix.tags.items():
            tags.setdefault(final(n), set()).update(ts)
        for sel, exc, dag, task, sched, at in ix.selectors:
            chosen = dbt_select(sel, models, datasets, tags, ups, downs)
            if chosen is not None:
                chosen -= dbt_select(exc, models, datasets, tags, ups, downs) or set()
                ix.runs += [(models[n], 1, 10**9, dag, task, sched, at) for n in chosen if n in models]
    con.executemany("insert into tags values (?,?)", sorted({(final(n), t) for n, ts in ix.tags.items() for t in ts}))
    for name, dag, task, sched, at in ix.dataset_runs:
        if (d := datasets.get(final(name))) and d.get("file"):
            ix.runs.append((d["file"], d["line"] or 1, d["line"] or 1, dag, task, sched, at))
    con.executemany("insert into runs values (?,?,?,?,?,?,?)", sorted(set(ix.runs)))
    owned = []  # a shared script's tables, each to the task whose arguments name it. ponytail: by name match, best match wins
    for f in {f for f, *_ in ix.calls}:
        mine = {(at, a.lower()) for g, args, at in ix.calls if g == f for a in args if a and re.fullmatch(r"\w{3,}", a)}
        for name, ws in writers.items():
            if any(w == f for w, _ in ws):
                score = Counter()
                for at, a in mine:
                    score[at] += len(a) if a in name.rsplit(".", 1)[-1] else 0
                best = max(score.values(), default=0)
                owned += [(name, at) for at, s in score.items() if best and s == best]
    con.executemany("insert into calls values (?,?)", sorted(set(owned)))
    con.executemany("insert into modes values (?,?,?,?)", sorted({(final(d), f, ln, m) for d, f, ln, m in ix.modes}))
    con.executemany("insert into layout values (?,?,?,?,?)", sorted({(final(d), k, c, f, ln) for d, k, c, f, ln in ix.layout}))
    rows = set()  # each resolved column use, with the clause it's in
    for ds, col, w, ln in ix.uses:
        if col != "*" and "jinja_expr" not in col:
            for cl in ix.clauses.get((w, ln, col, ds)) or ix.clauses.get((w, ln, col, None)) or ():
                rows.add((final(ds), col, cl, w, ln))
    con.executemany("insert into clauses values (?,?,?,?,?)", sorted(rows))
    jrows = {(final(a), ac, final(b), bc, f, ln) for a, ac, b, bc, f, ln in ix.joins if final(a) != final(b)}
    con.executemany("insert into joins values (?,?,?,?,?,?)", sorted(jrows))
    keyed = {(final(d), c, s) for d, c, s in ix.keys} | {(n, d["key"].strip().lower(), "set") for n, d in datasets.items()
                                                        if d.get("key") and "," not in d["key"]}
    con.executemany("insert into keys values (?,?,?)", sorted(keyed))
    pairs = Counter()
    for a, ac, b, bc, f, ln in jrows:
        pairs[tuple(sorted((f"{a}.{ac}", f"{b}.{bc}")))] += 1
    same = {(split(s), split(d)) for s, d in ix.identity if split(s) != split(d)}
    try:
        import yaml
        doc = yaml.safe_load((HOME / "concepts.yml").read_text()) if (HOME / "concepts.yml").exists() else {}
        confirmed = [k for k in (doc or {}).get("concepts") or [] if isinstance(k, dict) and k.get("confirmed") and k.get("name")]
    except Exception as e:
        ix.issue(".data-context/concepts.yml", 0, redact(e))
        confirmed = []
    named = {k["name"] for k in confirmed}
    for name, cols in sorted(ix.entities.items()):  # a dbt semantic layer's entities: declared by people, so confirmed
        if name not in named:
            home = next((c for c, typ in cols if typ == "primary"), cols[0][0])
            confirmed.append({"name": name, "columns": [split(c) for c, _ in cols], "home": split(home)})
    con.executemany("insert into value_maps values (?,?,?,?,?,?,?)", sorted({(final(d), c, v, l, h, f, ln) for d, c, v, l, h, f, ln in ix.value_maps}))
    con.executemany("insert into conversions values (?,?,?,?,?,?,?,?)", sorted({(final(s), sc, final(d), dc, k, x, f, ln) for s, sc, d, dc, k, x, f, ln in ix.conversions}))
    con.executemany("insert into concepts values (?,?,?,?,?,?)", concepts_of(pairs, same, {f"{d}.{c}" for d, c, _ in keyed}, confirmed))
    con.executemany("insert into files values (?,?)", [(f, ix.read_by.get(f, "")) for f in files])
    con.executemany("insert or ignore into col_uses values (?,?,?,?)", [(final(ds), c, w, ln) for ds, c, w, ln in ix.uses])
    stats = {"built_at": str(int(time.time())), "version": VERSION, "repos": json.dumps(repos), "dialect": dialect or "", "files": str(len(files)),
             "statements": str(ix.statements), "unparsed": str(len(ix.issues)), "unused_env": json.dumps(unused_env)}
    con.executemany("insert into meta values (?,?)", stats.items())
    con.commit()
    counts = [con.execute(f"select count(*) from {t}").fetchone()[0] for t in ("datasets", "edges", "col_edges", "readers", "notes")]
    con.close()
    tmp.replace(DB)
    print(f"indexed {counts[0]} datasets, {counts[1]} lineage edges, {counts[2]} column edges, {counts[3]} readers, "
          f"{counts[4]} notes from {len(files)} files in {time.time() - started:.1f}s, "
          f"dialect {dialect or 'generic'}{' (auto-detected; pin it with dialect: in .data-context/config.yml)' if auto else ''}"
          + (f"; {len(ix.issues)} of {ix.statements} statements unparsed (datawright lint)" if ix.issues else ""))
    if (newest := installed_copies()[:1]) and newest[0][0] > version_of(Path(__file__).read_text()):
        print(f"a newer datawright is installed ({'.'.join(map(str, newest[0][0]))}): run datawright update")


