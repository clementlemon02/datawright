# ---------------------------------------------------------------- build

HOW_RANK = {"direct": 0, "declared": 1, "inferred": 2}  # the more certain evidence wins when an edge is found twice


class Index:
    def __init__(self):
        self.ds, self.cols, self.edges, self.col_edges = {}, {}, {}, set()
        self.readers, self.writers, self.issues, self.aliases, self.statements, self.pending = {}, {}, [], {}, 0, []
        self.ddl, self.runs, self.uses, self.pending_uses, self.local, self.read_by, self.dataset_runs = {}, [], set(), [], {}, {}, []
        self.selectors, self.tags, self.tests, self.contracts, self.env, self.calls, self.fcalls, self.modes = [], {}, set(), [], {}, [], [], set()
        self.clauses, self.layout = {}, set()  # (file, line, column, table or None) -> clauses; (table, kind, columns, file, line)
        self.joins, self.identity, self.keys = set(), set(), set()  # join pairs; pass-through renames (src, dst); declared keys
        self.value_maps, self.conversions, self.pending_maps = set(), set(), []  # what codes mean (CASE, DECODE); unit, epoch, time-zone changes
        self.how, self.inferred = {}, set()  # edge -> direct | declared | inferred; (file, line) of SQL built by substitution or binding
        self.entities = {}  # dbt semantic-model entity -> [(model.column, type)]

    def issue(self, file, line, error):
        if len(self.issues) < 500:
            self.issues.append((file or "query log", line, error))

    def settle_columns(self):
        """Attribute unresolved columns: the only source table, else the one source known to have that column."""
        for dst, sources, col in self.pending:
            owners = sources if len(sources) == 1 else [s for s in sources if (s, col) in self.cols]
            if len(owners) == 1:
                self.col_edges.add((f"{owners[0]}.{col}", dst))
        for sources, col, *rest in self.pending_maps:
            owners = [s for s in sources if (s, col) in self.cols]
            if len(owners) == 1:
                self.value_maps.add((owners[0], col, *rest))
        for sources, col, where, line in self.pending_uses:
            owners = [s for s in sources if (s, col) in self.cols]
            if len(owners) == 1:
                self.uses.add((owners[0], col, where, line))
        known = {ds for ds, _ in self.cols}  # drop output aliases taken for columns (order by total): keep known or unknowable ones
        self.uses = {u for u in self.uses if u[1] == "*" or (u[0], u[1]) in self.cols or u[0] not in known}

    def dataset(self, name, kind="source", **fields):
        d = self.ds.setdefault(name, {"kind": kind, "file": None, "line": None, "description": None, "rows": None, "uses": 0})
        if RANK.get(kind, 0) > RANK.get(d["kind"], 0):
            d["kind"] = kind
        for k, v in fields.items():
            if v not in (None, "") and d.get(k) in (None, ""):
                d[k] = v
        return d

    def column(self, ds, col, typ=None, desc=None):
        if "jinja_expr" in col.lower():  # a column list filled at run time ({insert_cols}): no column to name
            return
        c = self.cols.setdefault((ds, col.lower()), [None, None])
        c[0], c[1] = c[0] or typ, c[1] or desc

    def edge(self, src, dst, via, how="direct"):
        """how sure: direct (written plainly in code), declared (a connector or import says so), inferred (substituted, bound or pattern-matched)."""
        if src and dst and src != dst:
            self.edges.setdefault((src, dst), via)
            if HOW_RANK[how] < HOW_RANK[self.how.get((src, dst), "inferred")] or (src, dst) not in self.how:
                self.how[(src, dst)] = how


def tname(t) -> str:
    """Dataset name of a parsed table. Templated qualifiers are dropped ({db}.orders -> orders), templated names are unknown."""
    if t is None or not t.name or "jinja_expr" in t.name.lower() or (t.db or "").lower() == "global_temp":  # Spark's in-job views
        return ""
    n = ".".join(p for p in (t.catalog, t.db, t.name) if p and "jinja_expr" not in p.lower()).lower()
    return LOCAL.get(n, n)


def valid(name: str) -> bool:
    return bool(name) and "jinja_expr" not in name


def strip_jinja(sql: str, py: bool = False) -> str:
    """Make templated SQL parseable while keeping line numbers: refs/sources become names."""
    keep_lines = lambda fill: (lambda m: fill + "\n" * m.group().count("\n"))
    sql = re.sub(r"\{\{\s*ref\(\s*(?:['\"][^'\"]+['\"]\s*,\s*)?['\"]([^'\"]+)['\"][^)]*\)\s*\}\}", r"\1", sql)
    sql = re.sub(r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", r"\1.\2", sql)
    sql = re.sub(r"\{#.*?#\}|\{%.*?%\}", keep_lines(" "), sql, flags=re.S)
    sql = re.sub(r"\{\{.*?\}\}", keep_lines("jinja_expr"), sql, flags=re.S)
    sql = re.sub(r"\$\{[^}\n]*\}", "jinja_expr", sql)  # ${var}: Hive and Spark SQL substitution, shell and Airflow templates
    if py:  # str.format / f-string slots and DB-API parameters (%s, %(name)s)
        sql = re.sub(r"\{[^{}\s]*\}|%\(\w+\)[sd]|%[sd]\b", "jinja_expr", sql)
    return sql


def chunks(text: str):
    """Yield (line, statement) split on semicolons outside strings and comments (Kafka JAAS configs end in ';' inside a string)."""
    pos = 0
    for m in re.finditer(r"'(?:[^'\\]|\\.)*'|--[^\n]*|/\*.*?\*/|(;)|\Z", text, re.S):
        if m[1] is None and (m[0] or m.start() < len(text)):
            continue
        body = text[pos:m.start()]
        if start := SQL_START.match(body):
            yield text.count("\n", 0, pos + start.start(1)) + 1, body.strip()
        pos = m.end()


def extract_sql(text, file, line0=1, dbt_model=None, py=False, calls=1):
    """Statements in a piece of SQL text: [(file, line, sql, dbt_model, calls)]."""
    return [(file, line0 + line - 1, sql, dbt_model, calls) for line, sql in chunks(strip_jinja(text, py))]


