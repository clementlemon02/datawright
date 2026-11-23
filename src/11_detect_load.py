ADAPTERS = {"trino": "trino", "athena": "athena", "snowflake": "snowflake", "bigquery": "bigquery", "databricks": "databricks",
            "spark": "spark", "redshift": "redshift", "postgres": "postgres", "duckdb": "duckdb", "clickhouse": "clickhouse",
            "starrocks": "starrocks", "sqlserver": "tsql", "synapse": "tsql", "fabric": "tsql", "mysql": "mysql", "oracle": "oracle",
            "doris": "doris", "maxcompute": "hive"}  # MaxCompute SQL is Hive-like; sqlglot has no dialect of its own for it
ENGINES = [("doris", r"apache/doris|\bpydoris\b|flink-doris-connector|spark-doris-connector|\bdoris\.(fe|be)\b"),
           ("hive", r"\bpyodps\b|\bodps\.|maxcompute")]  # engines a repo's config and dependencies name


STORAGE = re.compile(r"^(s3[an]?|oss|gs|abfss?|wasbs?|hdfs|cosn|obs)://", re.I)
PARTITION = re.compile(r"=|[*?\[]|^\d+$|\.(parquet|csv|tsv|json|jsonl|orc|avro|txt|gz|snappy|zst|zip)$"  # dt=..., 2026, *.parquet
                       r"|^\{(\w+_)?(date|dt|ds|day|hour|hr|month|year|ts|time|timestamp|run|batch|uuid|partition)(_\w+)?\}$", re.I)  # {date_str}
UNKNOWN = re.compile(r"[{}]|jinja_expr")


def storage_name(p):
    """An object-store or HDFS path as a dataset name (s3://bucket/events): one name whatever partition, file or glob is
    read, so jobs that share files connect. None when it names no single dataset (a bucket alone, an unknown in the middle)."""
    p = str(p or "").strip().strip("'\"`")
    m = STORAGE.match(p)
    if not m:
        return None
    scheme = {"s3a": "s3", "s3n": "s3", "abfss": "abfs", "wasbs": "wasb"}.get(m[1].lower(), m[1].lower())
    parts = [x for x in p[m.end():].split("/") if x]
    while parts and PARTITION.search(parts[-1]):  # dt=2026-11-04, dt={ds}, 2026/11/04, *.parquet, part-0000.snappy.parquet
        parts.pop()
    if parts and UNKNOWN.search(parts[0]):  # a bucket from the environment: joined to the real one after the build
        parts[0] = "{bucket}"
    if len(parts) < 2 or any(UNKNOWN.search(x) or PARTITION.search(x) for x in parts[1:]):  # an unknown folder may be any table
        return None
    return f"{scheme}://{'/'.join(parts)}".lower()


def dialect_hint(files):
    """The engine a repo declares: dbt adapters in its dependencies or profiles (dbt-trino, type: snowflake), or mostly .hql files."""
    votes = Counter()
    for f in files:
        if re.search(r"(^|/)(requirements[^/]*\.txt|pyproject\.toml|Pipfile|setup\.py|profiles\.yml)$", f):
            text = (ROOT / f).read_text(encoding="utf-8", errors="replace")
            votes.update(ADAPTERS[a.lower()] for a in re.findall(r"(?i)\bdbt-(" + "|".join(ADAPTERS) + r")\b", text))
            if f.endswith("profiles.yml"):
                votes.update(ADAPTERS[a.lower()] for a in re.findall(r"(?im)^\s*type:\s*['\"]?(" + "|".join(ADAPTERS) + r")\b", text))
        if re.search(r"(^|/)(requirements[^/]*\.txt|pyproject\.toml|docker-compose[^/]*\.ya?ml|Dockerfile[^/]*|pom\.xml|build\.gradle[^/]*|[^/]*\.properties)$", f):
            text = (ROOT / f).read_text(encoding="utf-8", errors="replace")[:50000]
            votes.update(e for e, rx in ENGINES if re.search(rx, text, re.I))
    sql = sum(f.endswith((".sql", ".hql")) for f in files)
    if sql and sum(f.endswith(".hql") for f in files) > sql / 2:
        votes["hive"] += 10**6
    top = votes.most_common(2)
    return top[0][0] if top and (len(top) == 1 or top[0][1] > top[1][1]) else None


PREFER = {"postgres": 0, None: 1}  # dialects that parse the sample equally well: the portable one wins, not whichever is listed first


def detect_dialect(stmts, hint=None):
    """The dialect that parses the most of a sample of the repo's statements. Statements no dialect parses (heavy templating)
    say nothing about the engine, so they don't count. Dialects that parse the same SQL (Spark and BigQuery both read backtick
    names) are told apart by how many functions each doesn't recognise: the right one knows more."""
    import sqlglot
    from sqlglot import exp
    sample = [s[2] for s in stmts if len(s[2]) < 4000]
    sample = sample[::max(1, len(sample) // 120)][:120]

    def trial(dialect, q):  # (parsed?, unrecognised functions)
        try:
            tree = sqlglot.parse_one(q, read=dialect)
            return not isinstance(tree, exp.Command), sum(1 for _ in tree.find_all(exp.Anonymous)) if tree else 0
        except Exception:
            return False, 0

    cache = {}

    def score(d, _ranked, sqls, telling):
        r = cache.get(d) or [trial(d, q) for q in sqls]
        return sum(not r[i][0] for i in telling), sum(r[i][1] for i in telling)

    def rank(dialects, sqls):
        results = {d: [trial(d, q) for q in sqls] for d in dialects}
        if len(sqls) == len(sample):
            cache.update(results)
        telling = [i for i in range(len(sqls)) if any(results[d][i][0] for d in dialects)]
        return sorted(dialects, key=lambda d: (sum(not results[d][i][0] for i in telling), sum(results[d][i][1] for i in telling), PREFER.get(d, 9))), telling
    if not sample:
        return None
    candidates = DIALECTS + ((hint,) if hint and hint not in DIALECTS else ())
    shortlist, telling = rank(candidates, sample[:40])
    finalists = candidates if len(telling) < 10 else list(dict.fromkeys(shortlist[:3] + ([hint] if hint else [])))
    best, telling = rank(finalists, sample)  # heavy templating gives little signal in the first 40: then every dialect competes
    margin = max(3, len(telling) // 10)  # a sample of ~120 is noisy: overruling what a repo declares takes a clear difference
    if hint and (len(telling) < 10 or score(hint, best, sample, telling)[0] <= score(best[0], best, sample, telling)[0] + margin):
        return hint
    return best[0] if telling else hint


def redact(e):
    """An error's first line without any text it quotes: SQL and file contents can hold secrets."""
    msg = str(e).strip().splitlines()[0].split(". Line ")[0] if str(e).strip() else type(e).__name__
    msg = re.sub(r"<Token.*", "<token>", msg)  # sqlglot token reprs carry the SQL's text, string literals included
    return re.sub(r"['\"].*", "'…'", msg)[:120]  # quoted text has quotes of its own: cut at the first


def load_connectors():
    """Connectors in .data-context/connectors/*.py: each defines match(path) -> bool and extract(path, text) -> records.
    They teach datawright a stack it doesn't know (an in-house framework, a config format, another tool's exports)."""
    import importlib.util
    out, problems = [], []
    for f in sorted((HOME / "connectors").glob("*.py")):
        try:
            spec = importlib.util.spec_from_file_location(f"datawright_connector_{f.stem}", f)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod.ROOT = ROOT  # after loading, so it replaces the connector's placeholder: connectors may read other repo files
            out.append((f.stem, mod.match, mod.extract, bool(getattr(mod, "OWNS", False))))
        except Exception as e:
            problems.append((f".data-context/connectors/{f.name}", 0, f"connector didn't load: {redact(e)}"))
    return out, problems


def add_records(ix, name, path, records):
    """What a connector found in one file. Records: {"sql": text, "line": n} is parsed like any SQL; {"produces": dataset,
    "line": n, "kind": k}, {"reads": dataset, "line": n}, {"edge": [source, target]}, {"column": [dataset, column, type]},
    {"column_edge": [source.column, target.column]}, {"run": {"dag", "task", "schedule", "lines": [first, last]}},
    {"dataset": name, "description": text, "owner": team} for what a table is without saying who writes it; columns take an optional description.
    {"contract": [dataset, column, type, required]} for what a data contract promises; {"env": [NAME, value]} for a deploy-time variable;
    {"layout": [dataset, kind, columns]} for a partition, cluster, sort or distribution key declared outside DDL.
    A run may name another "file" it schedules, or a "dataset" (placed on whatever code produces it). A connector module with OWNS = True replaces the built-in reader for its files."""
    stmts = []
    for r in records or ():
        line = int(r.get("line") or 1)
        if "sql" in r:
            stmts += extract_sql(str(r["sql"]), path, line, py=True)
        elif "produces" in r:
            ds = str(r["produces"]).lower()
            ix.dataset(ds, r.get("kind") or name, file=path, line=line, description=r.get("description"))
            ix.writers.setdefault((ds, path), line)
        elif "reads" in r:
            ix.dataset(str(r["reads"]).lower())["uses"] += 1
            ix.readers.setdefault((str(r["reads"]).lower(), path), line)
        elif "edge" in r:
            s, t = (str(x).lower() for x in r["edge"])
            ix.dataset(s, r.get("source_kind") or "source")
            ix.dataset(t, r.get("kind") or name)
            ix.edge(s, t, path, "declared")
        elif "column" in r:
            ds, col, *rest = list(r["column"]) + [None, None]
            ix.column(str(ds).lower(), str(col), rest[0], rest[1])
        elif "dataset" in r:
            ix.dataset(str(r["dataset"]).lower(), r.get("kind") or "source", description=r.get("description"), owner=r.get("owner"))
        elif "layout" in r:  # [dataset, kind, columns]: a partition, cluster, sort or distribution key declared outside DDL
            ds, kind, cols = (list(r["layout"]) + [None, None])[:3]
            cols = ",".join(cols) if isinstance(cols, (list, tuple)) else cols
            if cols and kind in ("partition", "cluster", "sort", "distribution"):
                ix.layout.add((str(ds).lower(), kind, str(cols).lower(), path, line))
        elif "contract" in r:  # [dataset, column, type, required]: what the table promises its consumers
            ds, col, typ, req = (list(r["contract"]) + [None, None, None])[:4]
            ix.contracts.append((str(ds).lower(), str(col).lower(), typ, int(bool(req)), path))
        elif "env" in r:  # [NAME, value]: what a deploy-time variable is set to, so names built from it resolve
            ix.env.setdefault(str(r["env"][0]).lower(), str(r["env"][1]).lower())
        elif "column_edge" in r:
            ix.col_edge(tuple(str(x).lower() for x in r["column_edge"]), "declared")
        elif "run" in r and r["run"].get("dataset"):  # schedules a table defined elsewhere: placed on its producer after the build
            g = r["run"]
            ix.dataset_runs.append((str(g["dataset"]).lower(), g["dag"], g.get("task") or "", g.get("schedule") or "manual", f"{path}:{line}"))
        elif "run" in r:
            g = r["run"]
            a, b = (g.get("lines") or [1, 10**9])[:2]
            ix.runs.append((g.get("file") or path, a, b, g["dag"], g.get("task") or "", g.get("schedule") or "manual", f"{path}:{line}"))
    return stmts


def parse_all(ix, stmts, dialect):
    import sqlglot
    from sqlglot import exp
    global LOCAL
    for file, line, sql, dbt_model, calls in stmts:
        LOCAL = ix.local.get(file, {})
        ix.statements += 1
        if file and re.match(r"\s*create\b", sql, re.I):
            table_layout(ix, sql, file, line)
        try:
            stmt = sqlglot.parse_one(sql, read=dialect)
        except Exception as e:  # reported by `lint`. Keep the message only, never the SQL: it can hold secrets
            ix.issue(file, line, redact(e))
            continue
        if isinstance(stmt, exp.Command) and DML.match(sql):
            ix.issue(file, line, "unsupported syntax for this dialect")
        elif stmt is not None:
            add_statement(ix, stmt, dialect, file, line, dbt_model, calls)


