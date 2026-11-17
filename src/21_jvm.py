JVM_STR = re.compile(r'"""(.*?)"""|"((?:[^"\\\n]|\\.)*)"', re.S)
JVM_IO = [  # (regex whose group 1 is the argument list, reads?, kind)
    (r"\.setTopics\(([^)]*)\)", True, "kafka"), (r"FlinkKafkaConsumer\w*\s*(?:<[^>]*>)?\s*\(\s*([^,)]+)", True, "kafka"),
    (r'\.option\(\s*"subscribe"\s*,\s*([^)]+)\)', True, "kafka"), (r"\.table\(\s*([^)]+)\)", True, "table"),
    (r"\.setTopic\(\s*([^)]+)\)", False, "kafka"), (r"FlinkKafkaProducer\w*\s*(?:<[^>]*>)?\s*\(\s*([^,)]+)", False, "kafka"),
    (r'\.option\(\s*"topic"\s*,\s*([^)]+)\)', False, "kafka"), (r"\.(?:saveAsTable|insertInto|toTable|writeTo)\(\s*([^)]+)\)", False, "table"),
]


def jvm_constants(text):
    """String constants in a JVM source file, text blocks included: {name: value}."""
    return {m[1]: m[2] if m[2] is not None else m[3].replace("\\n", "\n").replace('\\"', '"') for m in re.finditer(
        r'\b(?:String|val|var)\s+(\w+)\s*(?::\s*String\s*)?=\s*(?:"""(.*?)"""|"((?:[^"\\\n]|\\.)*)")', text, re.S)}


def jvm_exprs(text):
    """String constants defined by an expression rather than a literal (System.getenv("X"), A + B): {name: expression}."""
    return {m[1]: m[2].strip() for m in re.finditer(r'\bString\s+(\w+)\s*=\s*([^;"][^;]*);', text)}


def env_values(texts):
    """Environment variables set in deploy files (.env, .properties, YAML incl. Kubernetes env lists): {NAME: value}."""
    out = {}
    for text in texts:
        for m in re.finditer(r"name:\s*['\"]?([A-Z][A-Z0-9_]{2,})['\"]?\s*\n\s*value:\s*['\"]?([\w.:/-]+)", text):
            out.setdefault(m[1], m[2])
        for m in re.finditer(r"^\s*(?:export\s+)?([A-Z][A-Z0-9_]{2,})\s*[=:]\s*['\"]?([\w.:/-]+)", text, re.M):
            out.setdefault(m[1], m[2])
    return out


def jvm_args(text, i):
    """Arguments of the call whose '(' is just before position i, split at top-level commas (strings and nesting respected)."""
    args, depth, start, j = [], 1, i, i
    while j < len(text) and depth:
        if m := JVM_STR.match(text, j):
            j = m.end()
            continue
        c = text[j]
        depth += (c in "([{") - (c in ")]}")
        if (c == "," and depth == 1) or depth == 0:
            args.append(text[start:j].strip())
            start = j + 1
        j += 1
    return args


def extract_jvm(ix, path, text, shared=None, exprs=None, env=None):
    """Java, Scala and Kotlin jobs: SQL in string literals and String.format(template, args) calls, with constants resolved from
    this file or any JVM file in the index (Config.TOPIC), plus Kafka and StarRocks sources and sinks from Flink and Spark builders.
    ponytail: regex, not a parser, and file-level like PySpark lineage; names read from config files or job arguments are missed."""
    consts, exprs, env = {**(shared or {}), **jvm_constants(text)}, {**(exprs or {}), **jvm_exprs(text)}, env or {}

    def value(a, depth=0):  # a string as the job would build it: literals, constants, env vars, config lookups with defaults, +
        if depth > 4:
            return None
        vals = []
        for x in (x.strip() for x in re.split(r'\+(?=(?:[^"]*"[^"]*")*[^"]*$)', a)):
            name = x.rsplit(".", 1)[-1] if re.fullmatch(r"[\w.]+", x) else None
            call = re.fullmatch(r"(?:[\w.]*\.)?(\w+)\((.*)\)", x, re.S)
            args = jvm_args(x, x.index("(") + 1) if call else []
            if m := JVM_STR.fullmatch(x):
                vals.append(m[1] if m[1] is not None else m[2])
            elif name and consts.get(name) is not None:
                vals.append(consts[name])
            elif call and re.search(r"env", call[1], re.I) and len(args) == 1 and (m := JVM_STR.fullmatch(args[0])):  # getenv("X"): set at deploy time
                vals.append(env.get(m[2] or "", f"env:{m[2]}"))
            elif call and re.search(r"get|env|param|prop|config|arg|opt", call[1], re.I) and len(args) >= 2:  # lookup("key", DEFAULT)
                vals.append(value(args[-1], depth + 1))
            elif name and exprs.get(name):  # Config.TOPIC = System.getenv("..."), String topic = lookup("key", DEFAULT)
                vals.append(value(exprs[name], depth + 1))
            elif name and (asg := re.search(rf"\b{name}\s*=\s*([^;]+);", text)) and asg[1].strip() != x:
                vals.append(value(asg[1].strip(), depth + 1))
            else:
                vals.append(None)
        return None if any(v is None for v in vals) else "".join(vals)
    pieces, out = [], []
    for m in re.finditer(r"String\.format\(", text):  # the SQL these jobs actually run: the template with its arguments filled in
        args = jvm_args(text, m.end())
        if args and (template := value(args[0])) and SQL_START.match(template):
            fill = iter([value(a) or "{x}" for a in args[1:]])
            made = extract_sql(re.sub(r"%[sd]", lambda _: next(fill, "{x}"), template), path, text.count("\n", 0, m.start()) + 1, py=True)
            out += made
            ix.inferred.update((path, s[1]) for s in made)
    for m in JVM_STR.finditer(text):
        s = m[1] if m[1] is not None else m[2].replace("\\n", "\n").replace("\\t", " ").replace('\\"', '"')
        gap = re.fullmatch(r"\s*\+\s*(?:([\w.]+)\s*\+\s*)?", text[pieces[-1][1]:m.start()]) if pieces else None
        if gap:  # "select " + TABLE + " where ..." reads as one string
            pieces[-1][1:] = [m.end(), pieces[-1][2] + (consts.get(gap[1].rsplit(".", 1)[-1], "{x}") if gap[1] else "") + s, True]  # True: built from parts
        else:
            pieces.append([m.start(), m.end(), s])
    for start, _, s, *built in pieces:
        if len(s) >= 20 and SQL_START.match(s) and "%s" not in s:  # a format template is read above, filled in
            made = extract_sql(s, path, text.count("\n", 0, start) + 1, py=True)
            out += made
            if built:  # joined with + and constants: built, not written plainly
                ix.inferred.update((path, x[1]) for x in made)

    def values(args):  # "literal", CONSTANT or Config.CONSTANT; anything computed is unknown
        for a in args.split(","):
            a = a.strip()
            v = a[1:-1] if a[:1] == '"' and a[-1:] == '"' else consts.get(a.rsplit(".", 1)[-1]) if re.fullmatch(r"[\w.]+", a) else None
            if v:
                yield v
    reads, writes = set(), {}
    is_test = bool(TEST_FILE.search(path))
    for rx, is_read, kind in JVM_IO:
        for m in re.finditer(rx, text):
            for v in values(m[1]):
                name = ("kafka:" + v if kind == "kafka" else v).lower()
                reads.add(name) if is_read or is_test else writes.setdefault(name, text.count("\n", 0, m.start()) + 1)
    for m in re.finditer(r"\.(load|save|parquet|csv|json|orc|avro|text)\(\s*([^)]+)\)|new\s+Path\(\s*([^)]+)\)", text):  # files on object storage
        for v in values(m[2] or m[3]):
            if name := storage_name(v):
                stmt = text[text.rfind(";", 0, m.start()) + 1:m.start()]  # the statement so far: is this the write side?
                if not is_test and (m[1] == "save" or re.search(r"\.write(Stream)?\b|\bwrite\(\)|Sink\b|sinkTo|forBulkFormat|forRowFormat", stmt)):
                    writes.setdefault(name, text.count("\n", 0, m.start()) + 1)
                else:
                    reads.add(name)
    db, tbl = (next(values(m[1]), None) if (m := re.search(rf'"{k}"\s*,\s*([^)]+)\)', text)) else None for k in ("database-name", "table-name"))
    if tbl:  # StarRocks sink options builder
        writes.setdefault(f"{db}.{tbl}".lower() if db else tbl.lower(), text.count("\n", 0, m.start()) + 1)
    for w, line in writes.items():
        ix.dataset(w, "topic" if w.startswith("kafka:") else "files" if STORAGE.match(w) else "jvm job", file=path, line=line)
        ix.writers.setdefault((w, path), line)
        for r in reads - set(writes):
            ix.dataset(r, "topic" if r.startswith("kafka:") else "source")
            ix.edge(r, w, path, "inferred")
    for r in reads if not writes else ():
        ix.dataset(r, "topic" if r.startswith("kafka:") else "source")["uses"] += 1
        ix.readers.setdefault((r, path), 1)
    return out


def connector_tables(ix, stmts):
    """SQL tables that stand for something else - CREATE TABLE ... WITH ('connector' = 'kafka' | 'starrocks' | 'jdbc', ...) in Flink
    and Spark SQL - become aliases of that topic or table; StarRocks Routine Load jobs become topic -> table lineage."""
    props = lambda body: {k.lower(): v for k, v in re.findall(r"['\"]([^'\"]+)['\"]\s*=\s*['\"]([^'\"]*)['\"]", body)}
    for file, line, sql, *_ in stmts:
        if m := re.match(r"\s*create\s+(?:temporary\s+)?table\s+(?:if\s+not\s+exists\s+)?([\w.`\"]+).*\bwith\s*\((.*)\)\s*$", sql, re.I | re.S):
            p, name = props(m[2]), None
            if "kafka" in p.get("connector", "") and p.get("topic"):
                topic = p["topic"].split(";")[0]
                name = "kafka:" + topic if "jinja_expr" not in topic else f"kafka:?{Path(file or 'query log').stem}"  # set at run time
            elif p.get("connector") in ("starrocks", "jdbc") and p.get("table-name"):
                name = ".".join(x for x in (p.get("database-name"), p["table-name"]) if x)
            elif p.get("connector") == "filesystem" and p.get("path"):
                name = storage_name(p["path"])
            if name and "jinja_expr" not in name:  # a placeholder filled at run time is no name: never merge jobs through it
                ix.local.setdefault(file, {})[m[1].replace("`", "").replace('"', "").lower()] = name.lower()
                ix.dataset(name.lower(), "topic" if name.startswith("kafka:") else "table")
        if file is not None:
            storage_sql(ix, file, line, sql)
        if m := re.match(r"\s*create\s+routine\s+load\s+[\w.`]+\s+on\s+([\w.`]+).*?\bfrom\s+kafka\s*\((.*?)\)\s*$", sql, re.I | re.S):
            if (topic := props(m[2]).get("kafka_topic")) and "jinja_expr" not in topic:
                tbl, src = m[1].replace("`", "").lower(), "kafka:" + topic.lower()
                ix.dataset(src, "topic")
                ix.dataset(tbl, "routine load", **({"file": file, "line": line} if file else {}))
                ix.edge(src, tbl, f"{file}:{line}" if file else "query log")
                if file:
                    ix.writers.setdefault((tbl, file), line)


