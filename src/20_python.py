def extract_python(ix, path, text, files=frozenset(), argv=None, bind=None):
    """SQL in Python strings (constants, f-strings, + and .format() resolved where values are known), plus Spark lineage.
    argv: the arguments a task runs this script with, read through sys.argv, argparse and click. bind: {function: [(args,
    keywords)]} other files call this file's functions with. Loops over literal lists or lists in the repo's config files,
    and functions called with different values, are read once per iteration or call, each with lineage of its own."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    values, lists, built = {}, {}, {}
    for n in ast.walk(tree):
        target = n.targets[0] if isinstance(n, ast.Assign) and len(n.targets) == 1 else getattr(n, "target", None) if isinstance(n, ast.AnnAssign) else None
        if isinstance(target, ast.Name) and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
            values.setdefault(target.id, set()).add(n.value.value)
        elif isinstance(target, ast.Name) and literal_strings(n.value):
            lists[target.id] = literal_strings(n.value)
        if isinstance(target, ast.Name) and n.value is not None:
            built.setdefault(target.id, []).append(n.value)
    consts = {k: v.pop() for k, v in values.items() if len(v) == 1}  # names with exactly one known value
    built = {k: v[0] for k, v in built.items() if len(v) == 1 and isinstance(v[0], (ast.JoinedStr, ast.BinOp, ast.Call, ast.Subscript, ast.Attribute))}  # path = f"s3://...", t = sys.argv[1]
    building = set()

    def config_file(node):  # a JSON or YAML file in the repo that an expression names
        for c in ast.walk(node):
            if isinstance(c, ast.Constant) and isinstance(c.value, str) and re.search(r"\.(ya?ml|json)$", c.value):
                near = os.path.normpath(str(Path(path).parent / c.value))
                hits = [near] if near in files else [f for f in files if f == c.value.lstrip("./") or f.endswith("/" + c.value.lstrip("./"))]
                if len(hits) == 1:
                    return hits[0]
        return None

    def load_config(f):
        import yaml
        try:
            text_ = (ROOT / f).read_text(encoding="utf-8", errors="replace")
            return json.loads(text_) if f.endswith(".json") else yaml.safe_load(text_)
        except Exception:
            return None
    loads = re.compile(r"\b(load|safe_load|full_load|read_text|open)\(")
    loaded = {n.targets[0].id: load_config(f) for n in ast.walk(tree) if isinstance(n, ast.Assign) and len(n.targets) == 1
              and isinstance(n.targets[0], ast.Name) and loads.search(ast.unparse(n.value)) and (f := config_file(n.value))}

    def data_of(node):  # an expression over a repo config file: conf, conf["tables"], conf.get("tables"), load(open("x.yaml"))["k"]
        if isinstance(node, ast.Name):
            return loaded.get(node.id)
        if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            base = data_of(node.value)
            return base.get(node.slice.value) if isinstance(base, dict) else None
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("get", "keys", "values"):
            base = data_of(node.func.value)
            if not isinstance(base, dict):
                return None
            if node.func.attr == "get":
                return base.get(node.args[0].value) if node.args and isinstance(node.args[0], ast.Constant) else None
            return list(base) if node.func.attr == "keys" else list(base.values())
        if isinstance(node, ast.Call) and loads.search(ast.unparse(node)) and (f := config_file(node)):
            return load_config(f)
        return None

    def iter_values(it):  # what a loop iterates: a literal list, a list variable, or a list (or dict's keys) from a repo config file
        if vals := (lists.get(it.id) if isinstance(it, ast.Name) else literal_strings(it)):
            return vals
        d = data_of(it)
        d = list(d) if isinstance(d, dict) else d
        return [v for v in d if isinstance(v, (str, dict))][:500] or None if isinstance(d, list) else None
    defs = {n.name: [a.arg for a in n.args.args] for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    passed = {}  # a function's parameters take the values its calls in this file pass, when every call agrees
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in defs:
            params = defs[n.func.id]
            for name, arg in list(zip(params, n.args)) + [(k.arg, k.value) for k in n.keywords if k.arg in params]:
                passed.setdefault(name, []).append(arg)

    def render(n, env=None):
        """Text of a string expression with unknown parts as {name}; None if it isn't one. env: loop variables' values."""
        if isinstance(n, ast.Constant):
            return n.value if isinstance(n.value, str) else None
        if isinstance(n, ast.Subscript) and ast.unparse(n.value) == "sys.argv" and isinstance(n.slice, ast.Constant) \
                and isinstance(n.slice.value, int) and 0 < n.slice.value <= len(argv or ()):
            return argv[n.slice.value - 1]
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id in parsed:  # args.table from argparse
            return cli.get(n.attr)
        item = n.value if isinstance(n, ast.Subscript) else n.func.value if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "get" else None
        if isinstance(item, ast.Name) and isinstance(row := (env or {}).get(item.id), dict):  # t["name"], t.get("name") over config rows
            key = n.slice if isinstance(n, ast.Subscript) else (n.args or [None])[0]
            v = row.get(key.value) if isinstance(key, ast.Constant) else None
            return str(v) if isinstance(v, (str, int)) and not isinstance(v, bool) else None
        if isinstance(n, ast.Name):
            if isinstance(v := (env or {}).get(n.id), str) or (v := consts.get(n.id)):
                return v
            if n.id in commands and n.id in cli:  # a click option or argument, as the task passed it
                return cli[n.id]
            if n.id in building:
                return None
            building.add(n.id)
            try:
                if n.id in built:  # assigned once from a string expression: render that
                    return render(built[n.id], env)
                got = {render(a, env) for a in passed.get(n.id, [])}  # a parameter every call fills with the same value
                return got.pop() if len(got) == 1 and None not in got else None
            finally:
                building.discard(n.id)
        if isinstance(n, ast.JoinedStr):
            hole = lambda e: "{" + (e.id if isinstance(e, ast.Name) else e.attr if isinstance(e, ast.Attribute) else "x") + "}"  # keeps the name: {date_str}
            return "".join(v.value if isinstance(v, ast.Constant) else render(v.value, env) or hole(v.value) for v in n.values)
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Add):
            left, right = render(n.left, env), render(n.right, env)
            return None if left is None and right is None else (left or "{x}") + (right or "{x}")
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "format":
            base = render(n.func.value, env)
            if base is None:
                return None
            args, kw = [render(a, env) for a in n.args], {k.arg: render(k.value, env) for k in n.keywords if k.arg}
            pos = iter(args)
            slot = lambda m: (next(pos, None) if m[1] == "" else args[int(m[1])] if m[1].isdigit() and int(m[1]) < len(args) else kw.get(m[1])) or "{x}"
            return re.sub(r"\{(\w*)\}", slot, base)
        return None

    parsed, cli, positional, flags, commands = set(), {}, [], {}, set()  # argparse and click: namespaces, option values
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and getattr(n.value.func, "attr", "") in ("parse_args", "parse_known_args"):
            tgt = n.targets[0].elts[0] if isinstance(n.targets[0], ast.Tuple) else n.targets[0]
            if isinstance(tgt, ast.Name):
                parsed.add(tgt.id)
        is_click = isinstance(n, ast.Call) and getattr(n.func, "attr", "") in ("option", "argument") and getattr(n.func.value, "id", "") == "click"
        if isinstance(n, ast.Call) and (getattr(n.func, "attr", "") == "add_argument" or is_click) and n.args:
            names = [a.value for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
            kw = {k.arg: k.value for k in n.keywords if k.arg}
            if not names:
                continue
            longs = [x for x in names if x.startswith("--")] or [x for x in names if x.startswith("-")] or names
            plain = [x for x in names if not x.startswith("-")]  # click: an explicit parameter name beside the flags
            dest = render(kw["dest"]) if "dest" in kw else (plain[0] if is_click and plain else longs[0].lstrip("-")).replace("-", "_")
            if is_click:
                commands.add(dest)
            if "default" in kw and render(kw["default"]):
                cli[dest] = render(kw["default"])
            if names[0].startswith("-"):
                flags.update({x: dest for x in names if x.startswith("-")})
            else:
                positional.append(dest)
    if argv is not None:  # the task's arguments, parsed the way the script declares them
        rest, i = list(argv), 0
        while i < len(rest):
            a = rest[i] or ""
            if a.startswith("-") and (a.split("=", 1)[0] in flags):
                key, _, val = a.partition("=")
                if not _:
                    i += 1
                    val = rest[i] if i < len(rest) else None
                cli[flags[key]] = val
            elif not a.startswith("-") and positional:
                cli[positional.pop(0)] = rest[i]
            i += 1

    loops = {}  # node -> [(key, {loop variable: value})], one per iteration over a literal list
    for n in ast.walk(tree):
        its = [(n.target, n.iter, n.body)] if isinstance(n, ast.For) else \
            [(g.target, g.iter, [n]) for g in n.generators] if isinstance(n, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)) else []
        for target, it, body in its:
            vals = iter_values(it)
            if isinstance(target, ast.Name) and vals:
                for c in (c for b in body for c in ast.walk(b)):
                    loops.setdefault(id(c), []).extend(((id(n), i), {target.id: v}) for i, v in enumerate(vals))
    inside, calls_of = {}, {}  # node -> its function; function -> [(key, {parameter: value})], one per call that differs
    for f in (f for f in ast.walk(tree) if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef))):
        for b in f.body:
            for c in ast.walk(b):
                inside[id(c)] = f.name
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in defs:
            params = defs[n.func.id]
            for lk, le in loops.get(id(n), [(None, None)]):
                bound = {p: v for p, a in list(zip(params, n.args)) + [(k.arg, k.value) for k in n.keywords if k.arg in params] if (v := render(a, le))}
                if bound:
                    calls_of.setdefault(n.func.id, []).append(((id(n), lk), bound))
    calls_of = {f: cs for f, cs in calls_of.items() if len({tuple(sorted(b.items())) for _, b in cs}) > 1}  # all alike: `passed` has it
    for fname, bl in (bind or {}).items():  # calls from other files, each read like a call here
        params = defs.get(fname, [])
        for i, (vals, kws) in enumerate(bl):
            if b := {**{p: v for p, v in zip(params, vals) if v}, **{k: v for k, v in kws if v and k in params}}:
                calls_of.setdefault(fname, []).append((("bind", i), b))

    def scopes(n):  # [(group, env)] a node is read under: per call of its function, per loop iteration, or once
        fs = calls_of.get(inside.get(id(n)), [(None, {})])
        ls = loops.get(id(n), [(None, {})])
        return [((fk, lk), {**fe, **le}) for fk, fe in fs for lk, le in ls]

    def module_file(module, level=0):  # the repo file an import names: pkg.jobs -> pkg/jobs.py, or a relative import
        rel = (module or "").replace(".", "/")
        base = Path(path).parent.joinpath(*[".."] * max(level - 1, 0)) if level else None
        for cand in ([os.path.normpath(str(base / f"{rel}.py")), os.path.normpath(str(base / rel / "__init__.py"))] if base else [f"{rel}.py", f"{rel}/__init__.py"]):
            hits = [f for f in files if f == cand or (not base and f.endswith("/" + cand))]
            if len(hits) == 1:
                return hits[0]
        return None
    imported, modules = {}, {}  # name -> (file, function) from `from x import f`; alias -> file from `import x as m`
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and (f := module_file(n.module, n.level)):
            imported.update({a.asname or a.name: (f, a.name) for a in n.names})
        elif isinstance(n, ast.Import):
            modules.update({a.asname or a.name: f for a in n.names if (f := module_file(a.name))})
    for n in ast.walk(tree):  # calls into another file's functions, with the values passed: that file is read once per call
        target = imported.get(n.func.id) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) else \
            (modules[n.func.value.id], n.func.attr) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) \
            and isinstance(n.func.value, ast.Name) and n.func.value.id in modules else None
        if target and target[0] != path:
            for _, e in scopes(n):
                vals, kws = tuple(render(a, e) for a in n.args), tuple((k.arg, render(k.value, e)) for k in n.keywords if k.arg)
                if any(vals) or any(v for _, v in kws):
                    ix.fcalls.append((*target, vals, kws))

    out, done, sql_reads = [], set(), set()
    for n in ast.walk(tree):  # docstrings are prose, not SQL
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.body \
                and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant):
            done.add(id(n.body[0].value))
    for n in ast.walk(tree):  # breadth-first, so the outermost string expression wins
        if id(n) in done or not isinstance(n, (ast.Constant, ast.JoinedStr, ast.BinOp, ast.Call)):
            continue
        texts = list(dict.fromkeys(x for _, e in scopes(n) if (x := render(n, e)) is not None))
        if not texts:
            continue
        done.update(id(c) for c in ast.walk(n))
        for text_ in texts:
            if len(text_) >= 20 and SQL_START.match(text_):
                made = extract_sql(text_, path, n.lineno, py=True)
                out += made
                if re.match(r"\s*(select|with)\b", text_, re.I):  # pandas read_sql: what a query reads feeds the table to_sql writes
                    for st in made:
                        sql_reads |= select_tables(st[2])
                if argv is not None or bind is not None or not isinstance(n, ast.Constant):  # a value was filled in: an f-string, an argument, a loop, a call
                    ix.inferred.update((path, s[1]) for s in made)

    # ponytail: Spark DataFrame lineage is by group: a call or loop iteration's writes take its own reads and the file's
    groups = {}  # group -> (reads, writes)
    write_lines = {}  # table -> line of the call that writes it: a task's lines then say which tables it builds
    pandas_writes = set()  # to_sql targets: fed by the file's read_sql queries too
    kafka = re.search(r"\b(kafka|confluent_kafka|aiokafka)\b", text) is not None  # clients: producers write topics, consumers read them
    consumers = ("KafkaConsumer", "AIOKafkaConsumer")
    for n, (group, env) in ((n, s) for n in ast.walk(tree) if isinstance(n, ast.Call) and (isinstance(n.func, ast.Attribute)
                            or kafka and getattr(n.func, "id", "") in consumers) for s in scopes(n)):
        render_ = lambda x: render(x, env)
        attr, args, names = (n.func.attr if isinstance(n.func, ast.Attribute) else n.func.id), n.args, []
        if attr in ("table", "saveAsTable", "insertInto", "toTable", "writeTo") and args:
            names = [render_(args[0])]
        elif kafka and attr in ("produce", "send", "send_and_wait") and (args or any(k.arg == "topic" for k in n.keywords)):
            topic = render_(args[0] if args else next(k.value for k in n.keywords if k.arg == "topic"))
            names = ["kafka:" + topic] if topic else []  # a topic named at run time is no dataset
        elif kafka and (attr in consumers or attr == "subscribe") and (args or any(k.arg == "topics" for k in n.keywords)):
            topics = [e for a in (args if attr in consumers else args[:1]) for e in (a.elts if isinstance(a, (ast.List, ast.Tuple, ast.Set)) else [a])]
            names = ["kafka:" + t for t in (render_(e) for e in topics) if t]
        elif attr in ("to_sql", "read_sql_table"):  # pandas: the table is the name, qualified by schema=
            nm = args[0] if args else next((k.value for k in n.keywords if k.arg in ("name", "table_name")), None)
            sch = next((k.value for k in n.keywords if k.arg == "schema"), None)
            if nm is not None and render_(nm):
                names = [(f"{render_(sch)}." if sch is not None and render_(sch) else "") + render_(nm)]
        elif attr in ("load", "save", "parquet", "csv", "json", "orc", "avro", "text", "delta") or attr.startswith(("read_", "to_")):
            arg = args[0] if args else next((k.value for k in n.keywords if k.arg in ("path", "paths")), None)
            names = [storage_name(render_(arg))] if arg is not None else []  # files on S3, OSS, GCS, ADLS or HDFS
        elif attr == "option" and len(args) >= 2 and render_(args[0]) == "path":
            names = [storage_name(render_(args[1]))]
        elif attr == "option" and len(args) >= 2 and render_(args[0]) in ("dbtable", "table"):
            names = [render_(args[1])]
        elif attr == "option" and len(args) >= 2 and render_(args[0]) in ("subscribe", "topic"):  # Structured Streaming on Kafka
            names = ["kafka:" + x.strip() for x in (render_(args[1]) or "").split(",") if x.strip()]
        elif attr == "jdbc" and len(args) >= 2:
            names = [render_(args[1])]
        names = [x for x in names if x and (re.fullmatch(r"[\w.:-]+", x) or STORAGE.match(x)) and not x.lower().startswith("global_temp.")]
        if not names:
            continue
        chain, cur = set(), getattr(n.func, "value", None)  # spark.read.format(...).option(...): which side of the chain are we on?
        while isinstance(cur, (ast.Call, ast.Attribute)):
            cur = cur.func if isinstance(cur, ast.Call) else cur
            if isinstance(cur, ast.Attribute):
                chain.add(cur.attr)
                cur = cur.value
        written = attr in ("saveAsTable", "insertInto", "toTable", "writeTo", "save", "to_sql") or attr.startswith("to_") or (kafka and attr in ("produce", "send", "send_and_wait")) or chain & {"write", "writeStream"}
        if attr == "to_sql" and not TEST_FILE.search(path):
            pandas_writes.update(x.lower() for x in names)
        if written and not TEST_FILE.search(path):
            for x in names:
                write_lines.setdefault(x.lower(), n.lineno)
        groups.setdefault(group, (set(), set()))[1 if written and not TEST_FILE.search(path) else 0].update(x.lower() for x in names)
        if written and not TEST_FILE.search(path):  # Spark's save mode, from the call and the line it sits on
            seg = "\n".join(text.splitlines()[n.lineno - 1:n.end_lineno + 1])
            mode = "stream" if "writeStream" in chain else "overwrite" if re.search(r"overwrite|createOrReplace|replace\(", seg, re.I) \
                else "append" if re.search(r"""mode\(\s*["']append|\.append\(\)|insertInto\(""", seg) else "create"
            ix.modes.update((x.lower(), path, n.lineno, mode) for x in names)
    file_reads = groups.get((None, None), (set(), set()))[0]
    all_reads = {r for rs, _ in groups.values() for r in rs}
    all_writes = {w for _, ws in groups.values() for w in ws}
    for (fk, lk), (rs, ws) in groups.items():
        feed = all_reads if (fk, lk) == (None, None) else rs | groups.get((fk, None), (set(), set()))[0] | file_reads
        for w in ws:
            feed = feed | sql_reads if w in pandas_writes else feed
            ix.dataset(w, "topic" if w.startswith("kafka:") else "files" if STORAGE.match(w) else "pyspark", file=path, line=write_lines.get(w, 1))
            ix.writers.setdefault((w, path), write_lines.get(w, 1))
            for r in feed - ws:
                ix.dataset(r)
                ix.edge(r, w, f"{path} (pyspark)", "inferred")
    for r in all_reads if not all_writes else ():
        ix.dataset(r)["uses"] += 1
        ix.readers.setdefault((r, path), 1)
    if re.search(r"\bdlt\b", text):
        dlt_tables(ix, tree, path, render)
    if re.search(r"\bprefect\b", text):
        ix.runs += prefect_runs(tree, path, render)
    if re.search(r"\bDAG\(|@dag\b", text):
        ix.runs += airflow_runs(tree, path, render, iter_values, files, ix.selectors, ix.calls)
    return out


