FILE_READS = re.compile(r"""\b(?:s3|s3cluster|files|hdfs|oss|gcs|url)\s*\(\s*(?:["']?(?:uri|path|url)["']?\s*=\s*)?["']([^"']+)["']"""  # Doris, StarRocks, ClickHouse
                        r"""|\b(?:parquet|csv|json|orc|delta|avro|text)\s*\.\s*`([^`]+)`"""            # Spark SQL: parquet.`s3://...`
                        r"""|\bdata\s+infile\s*\(\s*["']([^"']+)["']|\binpath\s+'([^']+)'"""          # broker load, Hive LOAD DATA
                        r"""|\bfrom\s+'((?:s3|gs|oss|azure|abfss?)://[^']+)'""", re.I)                 # COPY ... FROM 's3://...'
FILE_WRITES = re.compile(r"""\bdirectory\s+'([^']+)'|\bunload\s*\(.*?\)\s*to\s+'([^']+)'|\bcopy\s+into\s+'([^']+)'""", re.I | re.S)
SQL_TARGET = re.compile(r"""\binsert\s+(?:into|overwrite)\s+(?:table\s+)?([\w.`"]+)|\binto\s+table\s+([\w.`"]+)|\bcopy\s+(?:into\s+)?([\w.`"]+)\s+from"""
                        r"""|\bcreate\s+(?:or\s+replace\s+)?(?:\w+\s+)?(?:table|view)\s+(?:if\s+not\s+exists\s+)?([\w.`"]+)""", re.I)


def storage_sql(ix, file, line, sql):
    """Files a SQL statement reads into a table or writes out, and tables stored at a path (LOCATION, external_location)."""
    clean = lambda x: x.replace("`", "").replace('"', "").lower()
    target = next((clean(next(g for g in m.groups() if g)) for m in SQL_TARGET.finditer(sql)), None)
    target = None if target in ("directory", "local") else target  # INSERT OVERWRITE DIRECTORY writes files, not a table
    at = f"{file}:{line}"
    if (loc := re.search(r"\b(?:external_)?location\s*=?\s*'([^']+)'", sql, re.I)) and (name := storage_name(loc[1])) and target \
            and re.match(r"\s*create\b", sql, re.I):
        ix.aliases.setdefault(name, target)  # the table is its files: jobs writing the path write the table
        ix.dataset(name, "files")
    for m in FILE_READS.finditer(sql):
        if name := storage_name(next(g for g in m.groups() if g)):
            ix.dataset(name, "files")
            if target:
                ix.edge(name, target, at, "inferred")
            else:
                ix.readers.setdefault((name, file), line)
    for m in FILE_WRITES.finditer(sql):
        if name := storage_name(next(g for g in m.groups() if g)):
            ix.dataset(name, "files", file=file, line=line)
            ix.writers.setdefault((name, file), line)
            for s in re.findall(r"""\b(?:from|join)\s+([\w.`"]+)""", sql, re.I):  # ponytail: names after FROM and JOIN, not a parse
                if "." in s or s.isidentifier():
                    ix.edge(clean(s), name, at, "inferred")


def literal_strings(n):
    """Values of a literal list or tuple of strings, else None."""
    if isinstance(n, (ast.List, ast.Tuple)) and n.elts and all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in n.elts):
        return [e.value for e in n.elts]
    return None


def callee(f):
    return f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""


def dbt_args(text):
    """The --select and --exclude selectors of a dbt command line."""
    picked, flag = {"select": [], "exclude": []}, None
    for tok in text.replace("'", " ").replace('"', " ").split():
        if tok.startswith("-"):
            flag = "select" if tok in ("-s", "--select", "-m", "--models") else "exclude" if tok == "--exclude" else None
        elif flag:
            picked[flag].append(tok)
    return picked["select"], picked["exclude"]


def dbt_select(tokens, models, names, tags, ups, downs):
    """What dbt would select: names (with * wildcards), tag:, source:, path: or folders, with +, n+, +n and @ around them.
    Space-separated tokens are a union, comma-joined parts an intersection. None if a method needs dbt's run state."""
    def reach(start, nbrs, depth):
        seen, frontier = set(), set(start)
        for _ in range(depth if depth is not None else 10**6):
            frontier = {m for n in frontier for m in nbrs.get(n, ())} - seen
            if not frontier:
                break
            seen |= frontier
        return seen
    out = set()
    for tok in tokens:
        part_sets = []
        for part in tok.split(","):
            m = re.fullmatch(r"(@)?(\d*\+)?(.+?)(\+\d*)?", part)
            at, up, body, down = m.groups() if m else (None, None, part, None)
            method, _, value = body.rpartition(":")
            value = value.lower()
            if method == "tag":
                base = {n for n, ts in tags.items() if value in ts}
            elif method == "source":
                base = {n for n in names if n == value or n.startswith(value + ".")}
            elif method == "path" or (not method and "/" in value):
                base = {n for n, f in models.items() if f"/{value.strip('/')}/" in f"/{f.lower()}" or f.lower().endswith(value)}
            elif not method:
                base = {n for n in names if fnmatch(n.rsplit(".", 1)[-1], value) or fnmatch(n, value)}
            else:
                return None  # state:, result:, config: and others need dbt itself (dbt ls)
            sel = set(base)
            if up:
                sel |= reach(base, ups, int(up[:-1]) if up[:-1] else None)
            if down or at:
                sel |= reach(base, downs, int(down[1:]) if down and down[1:] else None)
            if at:
                sel |= reach(sel, ups, None)
            part_sets.append(sel)
        out |= set.intersection(*part_sets) if part_sets else set()
    return out


def airflow_runs(tree, path, render, iter_values, files, selectors=None, calls=None):
    selectors = [] if selectors is None else selectors
    """Which Airflow DAG and task run which code: [(file, first line, last line, dag, task, schedule, task's file:line)].
    A task runs the files its arguments name (sql="x.sql", application="job.py", bash_command="python x.py") and the inline SQL
    in its own lines. ponytail: static only; tasks inside loops over literal lists are expanded, DAGs built from runtime config are missed."""
    envs = {}  # node -> one {loop variable: value} per iteration, for loops and comprehensions over literal string lists
    for n in ast.walk(tree):
        loops = [(n.target, n.iter, n.body)] if isinstance(n, ast.For) else \
            [(g.target, g.iter, [n]) for g in n.generators] if isinstance(n, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)) else []
        for target, it, body in loops:
            vals = iter_values(it)
            if isinstance(target, ast.Name) and vals:
                for c in (c for b in body for c in ast.walk(b)):
                    envs.setdefault(id(c), []).extend({target.id: v} for v in vals)

    def dag_info(call, default_id):
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        dag_id = render(call.args[0]) if call.args else render(kw["dag_id"]) if "dag_id" in kw else default_id
        dag_id = dag_id if dag_id and "{" not in dag_id else default_id or Path(path).stem  # dag_id=Path(__file__).stem and friends
        s = next((kw[k] for k in ("schedule", "schedule_interval", "timetable") if k in kw), None)
        return dag_id, "manual" if s is None or (isinstance(s, ast.Constant) and s.value is None) else render(s) or ast.unparse(s)[:40]
    spans, named = [], {}  # (first line, last line, dag): tasks written inside belong to it; variable -> dag
    for n in ast.walk(tree):
        if isinstance(n, ast.With):
            for item in n.items:
                if isinstance(item.context_expr, ast.Call) and callee(item.context_expr.func) == "DAG":
                    spans.append((n.lineno, n.end_lineno, dag_info(item.context_expr, None)))
                    if isinstance(item.optional_vars, ast.Name):
                        named[item.optional_vars.id] = spans[-1][2]
        elif isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and callee(n.value.func) == "DAG" and isinstance(n.targets[0], ast.Name):
            named[n.targets[0].id] = dag_info(n.value, None)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in n.decorator_list:
                if callee(dec.func if isinstance(dec, ast.Call) else dec) == "dag":
                    spans.append((n.lineno, n.end_lineno, dag_info(dec, n.name) if isinstance(dec, ast.Call) else (n.name, "manual")))
    dags = {d for *_, d in spans} | set(named.values())

    def find(ref):  # a path in a task argument, as the repo file it names
        if (near := os.path.normpath(str(Path(path).parent / ref))) in files:
            return near
        parts = os.path.normpath(ref).strip("/").split("/")
        for i in range(len(parts)):  # deployed paths (/opt/airflow/..., a template_searchpath) differ from repo paths: longest matching tail
            tail = "/".join(parts[i:])
            if hits := [f for f in files if f == tail or f.endswith("/" + tail)]:
                return hits[0] if len(hits) == 1 else None
        return None
    funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assigned = {t.id: (n.lineno, n.end_lineno) for n in tree.body if isinstance(n, ast.Assign) for t in n.targets if isinstance(t, ast.Name)}

    def used_by(name):  # line spans a python_callable runs: it, the functions it calls here, and the module constants they read
        seen, todo, spans_ = set(), [name], []
        while todo:
            f = todo.pop()
            if f in seen or f not in funcs:
                continue
            seen.add(f)
            spans_.append((funcs[f].lineno, funcs[f].end_lineno))
            for m in ast.walk(funcs[f]):
                if isinstance(m, ast.Name):
                    todo.append(m.id) if m.id in funcs else spans_.append(assigned[m.id]) if m.id in assigned else None
        return spans_
    strings = [(n.lineno, n.value) for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    jvm = [f for f in files if f.endswith((".java", ".scala", ".kt"))]

    def linked(s):  # repo files a task names: a script path, or a JVM job class (com.x.Job -> .../com/x/Job.java)
        for ref in re.findall(r"[\w./-]+\.(?:sql|hql|py|java|scala|kt)\b", s):
            if f := find(ref):
                yield f
        for cls in re.findall(r"\b[A-Za-z_]\w*(?:\.\w+){2,}\b", s):
            hits = [f for f in jvm if re.search(rf"(^|/){re.escape(cls.replace('.', '/'))}\.(java|scala|kt)$", f)]
            if len(hits) == 1:
                yield hits[0]
    out = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and any(k.arg == "task_id" for k in n.keywords):
            kw = {k.arg: k.value for k in n.keywords if k.arg}
            ids, dag_node = [(render(kw["task_id"], e), e) for e in envs.get(id(n), [None])], kw.get("dag")
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
                "task" in (callee(d.func if isinstance(d, ast.Call) else d), getattr(getattr(d.func if isinstance(d, ast.Call) else d, "value", None), "id", None))
                for d in n.decorator_list):  # TaskFlow: @task, @task.bash(...)
            kw, ids, dag_node = {}, [(n.name, None)], None
        else:
            continue
        dag = named.get(dag_node.id) if isinstance(dag_node, ast.Name) else None
        dag = dag or next((d for a, b, d in sorted(spans, key=lambda s: s[1] - s[0]) if a <= n.lineno <= b), None)
        dag = dag or (next(iter(dags)) if len(dags) == 1 else None)
        for task, env in ids:
            if not (task and dag and dag[0]):
                continue
            at = f"{path}:{n.lineno}"
            out.append((path, n.lineno, n.end_lineno, dag[0], task, dag[1], at))
            spans_ = used_by(kw["python_callable"].id) if isinstance(kw.get("python_callable"), ast.Name) else \
                [(n.lineno, n.end_lineno)] if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) else []  # TaskFlow: the function's own strings
            out += [(path, a, b, dag[0], task, dag[1], at) for a, b in spans_]
            texts = [render(v, env) or "" for v in kw.values()] + [s for ln, s in strings if any(a <= ln <= b for a, b in spans_)]
            out += [(f, 1, 10**9, dag[0], task, dag[1], at) for f in {f for s in texts for f in linked(s)} - {path}]
            if calls is not None:  # the arguments a task passes the script it runs: spark-submit job.py orders, application_args=[...]
                listed = [render(e, env) for e in kw["application_args"].elts] if isinstance(kw.get("application_args"), (ast.List, ast.Tuple)) else None
                for s in texts:
                    for m in re.finditer(r"([\w./-]+\.py)\b", s):
                        if (f := find(m[1])) and f != path:
                            rest = re.split(r"\n|&&|;|\|", s[m.end():].replace("\\\n", " "), maxsplit=1)[0]
                            try:
                                args = listed if listed is not None else shlex.split(rest)
                            except ValueError:
                                continue
                            calls.append((f, tuple(a if a and "{" not in a else None for a in args), at))
            for s in texts:  # dbt run -s <selectors>: resolved after the build, when tags and the whole graph are known
                for m in re.finditer(r"\bdbt\s+(?:run|build|test|seed|snapshot)\b(.*)", s):
                    selectors.append((*dbt_args(m[1]), dag[0], task, dag[1], at))
    if len(dags) == 1:  # SQL no task claims still belongs to the file's only DAG
        out.append((path, 1, 10**9, *next(iter(dags))[:1], "", next(iter(dags))[1], path))
    return out


def dbt_tags(ix, projects, files):
    """dbt tags without a manifest: config(tags=...) in models, tags in schema YAML (source tags reach their tables), and
    folder-level +tags in dbt_project.yml. ponytail: no vars or macros in tag values, no tags inherited from packages."""
    import yaml
    as_list = lambda v: [v] if isinstance(v, str) else [x for x in v if isinstance(x, str)] if isinstance(v, list) else []
    add = lambda n, ts: ix.tags.setdefault(n.lower(), set()).update(x.lower() for x in ts)
    for proj in projects:
        try:
            cfg = yaml.safe_load((ROOT / proj / "dbt_project.yml").read_text()) or {}
        except Exception:
            continue
        model_dirs = [str(Path(proj) / d) for d in (cfg.get("model-paths") or cfg.get("source-paths") or ["models"])]
        folders = []  # (folder under a model path, tags)

        def walk(node, prefix, inherited):
            if isinstance(node, dict):
                here = inherited + as_list(node.get("+tags", node.get("tags")))
                if here:
                    folders.append((prefix, here))
                for k, v in node.items():
                    if not k.startswith("+") and k != "tags":
                        walk(v, f"{prefix}/{k}", here)
        for tree in (cfg.get("models") or {}).values():  # the first level names the project or a package
            walk(tree, "", [])
        for f in (f for f in files if any(f.startswith(d + "/") for d in model_dirs)):
            under = "/" + f.split("/", len(Path(next(d for d in model_dirs if f.startswith(d + "/"))).parts))[-1]
            if f.endswith(".sql"):
                stem = Path(f).stem
                for prefix, ts in folders:
                    if under.startswith(prefix + "/"):
                        add(stem, ts)
                text = (ROOT / f).read_text(encoding="utf-8", errors="replace")[:5000]
                if m := re.search(r"config\((?:[^()]|\([^()]*\))*?\btags\s*=\s*(\[[^\]]*\]|['\"][^'\"]+['\"])", text):
                    add(stem, re.findall(r"['\"]([^'\"]+)['\"]", m[1]))
            elif f.endswith((".yml", ".yaml")):
                try:
                    docs = list(yaml.safe_load_all((ROOT / f).read_text(encoding="utf-8", errors="replace")))
                except Exception:
                    continue
                for doc in (d for d in docs if isinstance(d, dict)):
                    for m in doc.get("models") or []:
                        if isinstance(m, dict) and m.get("name"):
                            add(m["name"], as_list(m.get("tags")) + as_list((m.get("config") or {}).get("tags")))
                            if m.get("tests") or m.get("data_tests") or any(isinstance(c, dict) and (c.get("tests") or c.get("data_tests"))
                                                                           for c in m.get("columns") or []):
                                ix.tests.add((m["name"].lower(), f))
                            for c in (c for c in m.get("columns") or [] if isinstance(c, dict) and c.get("name")):
                                for test in (c.get("data_tests") or c.get("tests") or []):  # unique: a key; relationships: a declared join
                                    if test == "unique" or (isinstance(test, dict) and "unique" in test):
                                        ix.keys.add((m["name"].lower(), c["name"].lower(), "dbt"))
                                    rel = test.get("relationships") if isinstance(test, dict) else None
                                    rel = (rel.get("arguments") or rel) if isinstance(rel, dict) else None
                                    if rel and (to := re.search(r"ref\(\s*['\"]([^'\"]+)['\"]", str(rel.get("to", "")))) and rel.get("field"):
                                        ix.joins.add((m["name"].lower(), c["name"].lower(), to[1].lower(), str(rel["field"]).lower(), f, 0))
                    for sm in doc.get("semantic_models") or []:  # dbt semantic layer: entities name the join keys
                        model = re.search(r"ref\(\s*['\"]([^'\"]+)", str((sm or {}).get("model", "")))
                        for e in (sm or {}).get("entities") or [] if model else []:
                            if isinstance(e, dict) and e.get("name") and e.get("type") in ("primary", "foreign", "unique", "natural"):
                                col = str(e.get("expr") or e["name"]).lower()
                                if re.fullmatch(r"\w+", col):
                                    ix.entities.setdefault(e["name"].lower(), []).append((f"{model[1].lower()}.{col}", e["type"]))
                    for m in doc.get("models") or []:  # dbt 1.12+: entities on the model's columns
                        for c in (m.get("columns") or []) if isinstance(m, dict) and m.get("name") else []:
                            e = c.get("entity") if isinstance(c, dict) else None
                            if isinstance(e, dict) and e.get("type") and c.get("name"):
                                ix.entities.setdefault(str(e.get("name") or c["name"]).lower(), []).append((f"{m['name'].lower()}.{c['name'].lower()}", e["type"]))
                    for src in doc.get("sources") or []:
                        if isinstance(src, dict) and src.get("name"):
                            src_tags = as_list(src.get("tags")) + as_list((src.get("config") or {}).get("tags"))
                            for tbl in src.get("tables") or []:
                                if isinstance(tbl, dict) and tbl.get("name"):
                                    add(f"{src['name']}.{tbl['name']}", src_tags + as_list(tbl.get("tags")) + as_list((tbl.get("config") or {}).get("tags")))


def load_dbt(ix, projects):
    import json
    for proj in projects:
        man = ROOT / proj / "target" / "manifest.json"
        if not man.exists():
            continue
        m, ids = json.loads(man.read_text()), {}
        for nid, n in {**m.get("nodes", {}), **m.get("sources", {})}.items():
            rt = n.get("resource_type")
            if rt not in ("model", "seed", "snapshot", "source"):
                continue
            ident = n.get("alias") or n.get("identifier") or n.get("name")
            name = ".".join(p for p in (n.get("database"), n.get("schema"), ident) if p).lower()
            ids[nid] = name  # how raw SQL refers to it: ref('model') -> model, source('src','tbl') -> src.tbl
            ix.aliases[(f"{n.get('source_name')}.{n.get('name')}" if rt == "source" else n.get("name", "")).lower()] = name
            path = str(Path(proj) / n["original_file_path"]) if n.get("original_file_path") else None
            kind = {"model": "dbt model", "source": "dbt source"}.get(rt, rt)
            ix.dataset(name, kind, **({"file": path, "line": 1} if rt != "source" else {}), description=n.get("description"))
            ix.tags.setdefault(name, set()).update(t.lower() for t in n.get("tags") or [])
            for c, meta in (n.get("columns") or {}).items():
                ix.column(name, c, meta.get("data_type"), meta.get("description"))
        for nid, n in m.get("nodes", {}).items():
            for dep in n.get("depends_on", {}).get("nodes", []):
                if nid in ids and dep in ids:
                    ix.edge(ids[dep], ids[nid], ix.ds[ids[nid]].get("file") or "dbt", "declared")


