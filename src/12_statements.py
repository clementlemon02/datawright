LAYOUT = [  # (kind, pattern): how a table is laid out, from its DDL in any engine
    ("partition", r"\bPARTITION(?:ED)?\s+BY\s+((?:RANGE|LIST|HASH)?\s*\((?:[^()]|\([^()]*\))*\)|\w+\s*\([^)]*\)|\w+)"),
    ("distribution", r"\bDISTRIBUTED\s+BY\s+HASH\s*\(([^)]*)\)|\bDISTKEY\s*\(([^)]*)\)|\bCLUSTERED\s+BY\s*\(([^)]*)\)\s*(?:SORTED\s+BY\s*\([^)]*\)\s*)?INTO\b"),
    ("cluster", r"\bCLUSTER\s+BY\s+(\([^)]*\)|[\w\s,]+?)(?=\s+(?:OPTIONS|AS)\b|\s*;|\s*$|\s+[A-Z_]+\s*=)"),
    ("sort", r"\bSORTKEY\s*\(([^)]*)\)|\bORDER\s+BY\s*\(([^)]*)\)|\b(?:DUPLICATE|UNIQUE|PRIMARY|AGGREGATE)\s+KEY\s*\(([^)]*)\)|\bSORTED\s+BY\s*\(([^)]*)\)"),
]


def dbt_layout(ix, text, model, file):
    """Layout a dbt model's config declares: partition_by, cluster_by, sort, dist, distributed_by (BigQuery, Snowflake,
    Redshift, Databricks, StarRocks adapters)."""
    head = text[:4000]
    names = lambda s: [w.lower() for w in re.findall(r"['\"](\w+)['\"]", s)]
    keys = [("partition", r"partition_by\s*=\s*\{[^}]*?['\"]field['\"]\s*:\s*(['\"]\w+['\"])|partition_by\s*=\s*(\[[^\]]*\]|['\"]\w+['\"])"),
            ("cluster", r"(?:liquid_)?cluster(?:ed)?_by\s*=\s*(\[[^\]]*\]|['\"]\w+['\"])"),
            ("sort", r"\bsort\s*=\s*(\[[^\]]*\]|['\"]\w+['\"])"),
            ("distribution", r"\bdist\s*=\s*(['\"]\w+['\"])|distributed_by\s*=\s*(\[[^\]]*\]|['\"]\w+['\"])")]
    for kind, rx in keys:
        if (m := re.search(rx, head)) and (cols := names(next(g for g in m.groups() if g))):
            if not (kind == "distribution" and cols[0] in ("even", "all", "auto")):  # Redshift's styles name no key
                ix.layout.add((model, kind, ",".join(cols), file, 1))


def table_layout(ix, sql, file, line):
    """Partition, distribution, clustering and sort keys a CREATE TABLE declares, for `datawright advise`."""
    m = re.match(r"\s*create\s+(?:or\s+replace\s+)?(?:\w+\s+)*?table\s+(?:if\s+not\s+exists\s+)?([\w.`\"]+)", sql, re.I)
    if not m:
        return
    name = m[1].replace("`", "").replace('"', "").lower()
    for k in re.finditer(r"\b(?:PRIMARY|UNIQUE)\s+KEY\s*\(([^)]*)\)", sql, re.I):  # what identifies a row: the home of a concept
        cols = [c.strip(" `\"").lower() for c in k[1].split(",") if c.strip()]
        ided = [c for c in cols if re.search(r"(^id$|_id$|_key$|_code$|_no$|uuid)", c)]
        for c in ided or cols[:1]:
            ix.keys.add((name, c, "ddl"))
    ddl = re.split(r"\bAS\s+(?:SELECT|WITH|\()", sql, maxsplit=1, flags=re.I)[0]  # a CTAS's query isn't layout
    for kind, rx in LAYOUT:
        for g in re.finditer(rx, ddl, re.I | re.S):
            body = re.sub(r"'[^']*'|\"[^\"]*\"", "", next(x for x in g.groups() if x))  # date_trunc('month', date): 'month' isn't a column
            cols = []
            for part in re.split(r",(?![^()]*\))", body.strip("() ")):
                words = [w for w in re.finditer(r"[A-Za-z_]\w*", part) if not part[w.end():].lstrip().startswith("(")]
                if words and words[0][0].upper() not in ("RANGE", "LIST", "HASH", "BUCKETS"):
                    cols.append(words[0][0].lower())
            if cols:
                ix.layout.add((name, kind, ",".join(dict.fromkeys(cols)), file, line))


def add_statement(ix, stmt, dialect, file, line, dbt_model, calls):
    from sqlglot import exp
    if file and TEST_FILE.search(file):  # a test's fixtures don't produce data: every table it touches is one it depends on
        for s in {n for n in map(tname, stmt.find_all(exp.Table)) if valid(n)}:
            ix.dataset(s)
            ix.readers.setdefault((s, file), line)
        return
    target, select, kind = "", None, "table"
    if isinstance(stmt, exp.Create) and str(stmt.args.get("kind", "")).upper() in ("TABLE", "VIEW"):
        target, select = tname(stmt.this.find(exp.Table) if stmt.this else None), stmt.expression
        kind = "view" if str(stmt.args.get("kind")).upper() == "VIEW" else "table"
        if isinstance(stmt.this, exp.Schema):  # DDL column definitions
            for cd in stmt.this.expressions:
                if isinstance(cd, exp.ColumnDef) and valid(target):
                    ix.column(target, cd.name, cd.args["kind"].sql() if cd.args.get("kind") else None)
        if select is None:  # plain CREATE TABLE: where the schema is defined, not who writes the data
            if valid(target):
                ix.dataset(target, kind)
                if file:
                    ix.ddl.setdefault(target, (file, line))
                    if re.search(r"\b(PRIMARY|UNIQUE)\s+KEY\b", stmt.sql(dialect=dialect or None), re.I):  # StarRocks, Doris: INSERT upserts
                        ix.modes.add((target, file, line, "keyed"))
            return
        if file and valid(target):
            ix.modes.add((target, file, line, "replace" if stmt.args.get("replace") else "create"))
    elif isinstance(stmt, (exp.Insert, exp.Merge, exp.Update)):
        target = tname(stmt.this.find(exp.Table) if stmt.this else None)
        select = stmt.expression if isinstance(stmt, exp.Insert) else None
        if file and valid(target):  # how it writes: a rerun of an append adds the rows again
            ix.modes.add((target, file, line, "merge" if isinstance(stmt, exp.Merge) else "update" if isinstance(stmt, exp.Update)
                          else "overwrite" if stmt.args.get("overwrite") or stmt.args.get("where") else "append"))
    elif isinstance(stmt, (exp.Delete, exp.TruncateTable)):  # clears what the next insert writes: the pair is safe to rerun
        for tb in ([stmt.this] if isinstance(stmt, exp.Delete) else stmt.expressions):
            if file and valid(n := tname(tb.find(exp.Table) if tb else None)):
                ix.modes.add((n, file, line, "delete"))
        return
    elif isinstance(stmt, exp.Query):
        select, target, kind = stmt, dbt_model or "", "dbt model"
    else:
        return
    ctes = {c.alias.lower() for c in stmt.find_all(exp.CTE)}

    def cte_ref(t):  # a reference to a CTE, unless it sits in that CTE's own definition: orders as (select * from orders) reads the table
        n, p = tname(t), t.parent
        while n in ctes and p is not None:
            if isinstance(p, exp.CTE) and p.alias.lower() == n:
                return False
            p = p.parent
        return n in ctes
    sources = {n for n, t in ((tname(t), t) for t in stmt.find_all(exp.Table)) if valid(n) and n != target and not cte_ref(t)}
    via = f"{file}:{line}" if file else "query log"
    add_uses(ix, stmt, sources, file or "query log", line or 0)
    if file:
        add_meanings(ix, stmt, target, sources, file, line)
    if valid(target):
        ix.dataset(target, kind, **({"file": file, "line": line} if file else {}))
        if file:
            ix.writers.setdefault((target, file), line)
        for s in sources:
            ix.dataset(s)
            ix.edge(s, target, via, "inferred" if (file, line) in ix.inferred else "direct")
        if isinstance(select, exp.Query):
            add_columns(ix, target, select, dialect, sources)
    else:  # a read: who uses which dataset
        for s in sources:
            ix.dataset(s)["uses"] += calls
            if file:
                ix.readers.setdefault((s, file), line)


def add_uses(ix, stmt, sources, where, line):
    """Every source column a statement reads, wherever it appears (select, join, where, group by), and select * reads.
    ponytail: alias-based; an unqualified column over several tables is settled by known columns at the end of the build."""
    from sqlglot import exp
    alias = {}
    for t in stmt.find_all(exp.Table):
        if (n := tname(t)) in sources:
            alias[n], alias[t.alias_or_name.lower()] = n, n
    for star in stmt.find_all(exp.Star):
        if isinstance(star.parent, exp.Select):  # select * : every source; count(*) is not a read of columns
            ix.uses.update((s, "*", where, line) for s in sources)
        elif isinstance(star.parent, exp.Column) and star.parent.table.lower() in alias:
            ix.uses.add((alias[star.parent.table.lower()], "*", where, line))
    def owner_of(c):
        return alias.get(c.table.lower()) if c.table else (next(iter(sources)) if len(sources) == 1 else None)
    timeish = re.compile(r"(^|_)(date|dt|ds|day|hour|month|year|time|ts|timestamp)$|_at$", re.I)
    bare = lambda e: e.this if isinstance(e, (exp.Cast, exp.TryCast)) else e  # p.id = CAST(s.playerid AS BIGINT) joins the columns too
    for eq in stmt.find_all(exp.EQ):  # a.player_id = b.id in a join or a where: the two columns hold the same thing
        left, right = bare(eq.left), bare(eq.right)
        if isinstance(left, exp.Column) and isinstance(right, exp.Column) and eq.find_ancestor(exp.Join, exp.Where):
            a, b = owner_of(left), owner_of(right)
            if a and b and a != b and not (timeish.search(left.name) and timeish.search(right.name)):  # aligned dates aren't entities
                ix.joins.add((a, left.name.lower(), b, right.name.lower(), where, line))
    for c in stmt.find_all(exp.Column):
        if isinstance(c.this, exp.Star) or not c.name:
            continue
        p, clause = c.parent, "select"  # where it's used: a filter and a join key say different things about a table's layout
        while p is not None and not isinstance(p, exp.Select):
            if isinstance(p, (exp.Where, exp.Having)) or (isinstance(p, exp.Join) and c in list(p.args.get("on").find_all(exp.Column) if p.args.get("on") else [])):
                clause = "filter" if not isinstance(p, exp.Join) else "join"
                break
            if isinstance(p, (exp.Group, exp.Order)):
                clause = "group" if isinstance(p, exp.Group) else "order"
                break
            p = p.parent
        owner = alias.get(c.table.lower()) if c.table else (next(iter(sources)) if len(sources) == 1 else None)
        ix.clauses.setdefault((where, line, c.name.lower(), owner), set()).add(clause)
        if c.table:
            if c.table.lower() in alias:
                ix.uses.add((alias[c.table.lower()], c.name.lower(), where, line))
        elif len(sources) == 1:
            ix.uses.add((next(iter(sources)), c.name.lower(), where, line))
        elif sources:
            ix.pending_uses.append((tuple(sorted(sources)), c.name.lower(), where, line))


def add_meanings(ix, stmt, target, sources, file, line):
    """What values mean, from the code that decodes them: CASE status WHEN 1 THEN 'active' (or WHEN status = 1, or DECODE)
    gives status's labels; amount / 100, FROM_UNIXTIME(ms / 1000) and CONVERT_TZ give conversions into the output column."""
    from sqlglot import exp
    alias = {}
    for t in stmt.find_all(exp.Table):
        if (n := tname(t)) in sources:
            alias[n], alias[t.alias_or_name.lower()] = n, n
    owner = lambda c: alias.get(c.table.lower()) if c.table else (next(iter(sources)) if len(sources) == 1 else None)
    lit = lambda e: (e.name if isinstance(e, exp.Literal) else "true" if isinstance(e, exp.Boolean) and e.this else
                     "false" if isinstance(e, exp.Boolean) else None)
    for node in stmt.find_all(exp.Case, exp.DecodeCase):
        pairs, col = [], None
        if isinstance(node, exp.DecodeCase):  # DECODE(col, v1, r1, v2, r2, ..., default)
            args = node.expressions
            col = args[0] if args and isinstance(args[0], exp.Column) else None
            pairs = [(args[i], args[i + 1]) for i in range(1, len(args) - 1, 2)]
        elif isinstance(node.this, exp.Column):  # CASE col WHEN v THEN r
            col, pairs = node.this, [(i.this, i.args.get("true")) for i in node.args.get("ifs") or []]
        else:  # CASE WHEN col = v THEN r, WHEN col IN (v, w) THEN r: only when every branch tests the same column
            for i in node.args.get("ifs") or []:
                cond, res = i.this, i.args.get("true")
                if isinstance(cond, exp.EQ) and isinstance(cond.left, exp.Column) and lit(cond.right) is not None:
                    pairs.append((cond.right, res))
                    c = cond.left
                elif isinstance(cond, exp.In) and isinstance(cond.this, exp.Column) and all(lit(v) is not None for v in cond.expressions):
                    pairs += [(v, res) for v in cond.expressions]
                    c = cond.this
                else:
                    pairs = []
                    break
                if col is not None and c.name.lower() != col.name.lower():
                    pairs = []
                    break
                col = c
        if col is None or not pairs or any(lit(v) is None for v, _ in pairs):
            continue
        if all(isinstance(r, exp.Literal) and r.is_string for _, r in pairs):  # labels written in the code ('active'); THEN 1 counts rows
            labels, how = [(lit(v), lit(r)) for v, r in pairs], "label"
        else:  # SUM(CASE WHEN type = 1 THEN amount END) AS deposit_amount: the alias says what 1 means
            named, up = None, node.parent
            unit = lambda e: isinstance(e, (exp.Div, exp.Mul)) and isinstance(e.expression.this if isinstance(e.expression, exp.Nullif) else e.expression, exp.Literal)
            while isinstance(up, (exp.AggFunc, exp.Coalesce, exp.Cast, exp.Paren)) or unit(up):  # SUM(CASE ...) / 100 AS x, not SUM(a) - SUM(b) AS x
                up = up.parent
            named = up if isinstance(up, exp.Alias) else None
            if not named or len(pairs) != 1 or not named.alias:
                continue
            labels, how = [(lit(pairs[0][0]), named.alias.lower())], "alias"
        for v, r in labels:
            row = (col.name.lower(), v[:40], r[:60], how, file, line)
            if ds := owner(col):
                ix.value_maps.add((ds, *row))
            elif len(sources) > 1 and not col.table:  # unqualified over several tables: settled once every table's columns are known
                ix.pending_maps.append((tuple(sorted(sources)), *row))
    if not (valid(target) and isinstance(stmt, (exp.Create, exp.Insert)) and isinstance(sel := stmt.expression, exp.Select)):
        return
    for p in sel.selects:  # conversions into the output column
        out, e = p.alias_or_name.lower(), (p.this if isinstance(p, exp.Alias) else p)
        e = e.this if isinstance(e, exp.Cast) else e
        num = lambda x: x.this if isinstance(x, exp.Nullif) else x
        kind = detail = src = None
        if isinstance(e, (exp.Div, exp.Mul)) and isinstance(e.this, exp.Column) and isinstance(f := num(e.expression), exp.Literal) \
                and f.name in ("10", "100", "1000", "1000000", "1000000000", "60", "3600", "86400"):
            kind, detail, src = "unit", f"{'÷' if isinstance(e, exp.Div) else '×'} {f.name}", e.this
        elif isinstance(e, exp.UnixToTime) and (inner := e.this) is not None:
            src = inner.this if isinstance(inner, exp.Div) else inner
            kind, detail = "epoch", "epoch milliseconds to time" if isinstance(inner, exp.Div) else "epoch seconds to time"
        elif type(e).__name__ in ("ConvertTimezone", "AtTimeZone", "FromTimeZone"):
            zones = [z.name for z in e.find_all(exp.Literal) if z.is_string]
            src = next(e.find_all(exp.Column), None)
            kind, detail = "time zone", " to ".join(zones) if zones else "converted"
        if kind and isinstance(src, exp.Column) and (ds := owner(src)):
            ix.conversions.add((ds, src.name.lower(), target, out, kind, detail, file, line))


def add_columns(ix, target, select, dialect, sources):
    from sqlglot import exp
    from sqlglot.lineage import lineage
    for c in select.named_selects:
        if c != "*":
            ix.column(target, c)
    if isinstance(select, exp.Select):  # these clauses can't change where a column comes from, and their aliases trip the resolver
        select = select.copy()
        for clause in ("group", "order", "having", "qualify"):
            select.set(clause, None)
    try:
        nodes = lineage(None, select, None, dialect=dialect)
    except Exception:
        return

    def plain(node):  # a column passed through, maybe renamed: no function, cast or arithmetic on the way
        e = node.expression
        e = e.this if isinstance(e, exp.Alias) else e
        return isinstance(e, (exp.Column, exp.Table, exp.Star))

    def leaves(node, last, same=True):  # carry the last named column through `select *` hops
        name = node.name.split(".")[-1].lower()
        last = last if name == "*" else name
        same = same and plain(node)
        if not node.downstream:
            if isinstance(node.source, exp.Table):
                yield tname(node.source), last, same
            elif isinstance(node.source, exp.Placeholder):  # a column sqlglot couldn't place; literals don't get here
                yield None, last, same
        for d in node.downstream:
            yield from leaves(d, last, same)
    for col, node in nodes.items():
        for table, src_col, same in leaves(node, col.lower()):
            if valid(table):
                ix.col_edges.add((f"{table}.{src_col}", f"{target}.{col.lower()}"))
                if same:
                    ix.identity.add((f"{table}.{src_col}", f"{target}.{col.lower()}"))
            elif table is None and sources:  # unqualified column read through `select *`: settle after the whole repo is read
                ix.pending.append((f"{target}.{col.lower()}", tuple(sorted(sources)), src_col))


