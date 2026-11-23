# ---------------------------------------------------------------- review

NUMERIC = re.compile(r"int|dec|num|double|float|real|money", re.I)


def data_diff(con, tables, where=""):
    """Each changed table's prod copy against its dev copy (dev_name in config.yml, e.g. {schema}_dev.{table}): rows,
    columns, duplicate keys and the sums of numeric columns, two guarded SELECTs a side through query_command.
    Returns (markdown lines, blocking problems: keys that became duplicated). ponytail: whole-table scans unless --where."""
    import sqlglot
    template = setting("dev_name", "")
    if not template:
        return ["Data diff: set `dev_name` in .data-context/config.yml (e.g. `dev_name: {schema}_dev.{table}`) to compare each changed table with its dev copy.", ""], 0
    dialect = setting("dialect", "") or (con.execute("select v from meta where k='dialect'").fetchone() or [""])[0]
    w = f" WHERE {where}" if where else ""
    pct = lambda a, b: "" if a == b else f" ({(b - a) / a:+.1%})" if a else " (new)"
    out, blocking = ["**Data diff** (prod → dev" + (f", where `{where}`" if where else "") + "):", "",
                     "| Table | Rows | Columns | Duplicate keys | Sums that changed |", "|---|---|---|---|---|"], 0
    for ds in sorted(t for t in tables if ":" not in t):  # namespaced names (topics, dashboards) aren't tables to query
        schema, _, tbl = ds.rpartition(".")
        dev = template.format(name=ds, schema=schema.rpartition(".")[2], table=tbl)
        d = con.execute("select key from datasets where name=?", (ds,)).fetchone()
        keys = [k.strip() for k in ((d and d["key"]) or "").split(",") if k.strip()]
        types = dict(con.execute("select name, type from columns where dataset=?", (ds,)).fetchall())
        try:
            heads = {}
            for side in (ds, dev):
                got = list(csv.reader(io.StringIO(query(f"SELECT * FROM {side}{w} LIMIT 1", 1))))
                heads[side] = ([c.lower() for c in got[0]], got[1] if len(got) > 1 else []) if got and got[0] != ["(no rows)"] else ([], [])
            (pc, sample), (dc, _) = heads[ds], heads[dev]
            def number(c):
                v = sample[pc.index(c)] if c in pc and pc.index(c) < len(sample) else ""
                return bool(NUMERIC.search(types.get(c) or "")) or (not types.get(c) and re.fullmatch(r"-?\d+(\.\d+)?(e-?\d+)?", v or "") is not None)
            nums = [c for c in pc if c in dc and c not in keys and not re.search(r"(^|_)id$", c) and number(c)][:10]
            stats = {}
            for side in (ds, dev):
                dup = f", (SELECT COUNT(*) FROM (SELECT {', '.join(keys)} FROM {side}{w} GROUP BY {', '.join(keys)} HAVING COUNT(*) > 1) AS k) AS dup_" if keys else ""
                sql = f"SELECT COUNT(*) AS rows_{''.join(f', SUM(CAST({c} AS DOUBLE)) AS s{i}' for i, c in enumerate(nums))}{dup} FROM {side}{w}"
                r = next(csv.DictReader(io.StringIO(query(sqlglot.transpile(sql, read="duckdb", write=dialect or None)[0], 1))), {})
                stats[side] = {k: float(v or 0) for k, v in r.items()}
        except SystemExit as e:
            out.append(f"| `{ds}` | could not query `{dev}`: {redact(e.code)} | | | |")
            continue
        a, b = stats[ds], stats[dev]
        cols = ", ".join([f"+{c}" for c in dc if c not in pc] + [f"-{c}" for c in pc if c not in dc]) or "same"
        dups = "-" if not keys else f"{a['dup_']:.0f} → {b['dup_']:.0f}" + (" **new duplicates**" if b["dup_"] > a["dup_"] else "")
        blocking += bool(keys and b["dup_"] > a["dup_"])
        sums = ", ".join(f"{c} {pct(a[f's{i}'], b[f's{i}'])[2:-1]}" for i, c in enumerate(nums) if abs(b[f"s{i}"] - a[f"s{i}"]) > 1e-9 * max(1, abs(a[f"s{i}"])))
        out.append(f"| `{ds}` → `{dev}` | {a['rows_']:,.0f} → {b['rows_']:,.0f}{pct(a['rows_'], b['rows_'])} | {cols} | {dups} | {sums or 'none'} |")
    return out + [""], blocking


def terminal_text(out):
    """Markdown for merge request comments; on a terminal, plain text: tables become one block per row."""
    if not sys.stdout.isatty() or os.environ.get("DW_MARKDOWN"):
        return "\n".join(out)
    res, head, i = [], None, 0
    while i < len(out):
        l = out[i]
        if l == "```mermaid":  # a graph for merge request comments: skipped on a terminal
            i = out.index("```", i) + 1
            continue
        if l.startswith("|") and i + 1 < len(out) and set(out[i + 1]) <= set("|-: "):
            head, i = [c.strip() for c in l.strip("|").split("|")], i + 2
            while i < len(out) and out[i].startswith("|"):
                cells = [c.strip() for c in out[i].strip("|").split("|")]
                res.append(cells[0])
                res += [f"  {h:<{max(map(len, head[1:]))}}  {c}" for h, c in zip(head[1:], cells[1:]) if c != "-"]
                res.append("")
                i += 1
            continue
        res.append(l)
        i += 1
    t = re.sub(r"\n{3,}", "\n\n", "\n".join(res))
    t = re.sub(r"^##(.*)$", lambda m: "\033[1m" + m[1] + "\033[0m", t, flags=re.M)
    return re.sub(r"</?sub>|`|\*\*", "", t)


def signature(text, dialect):
    """What a SQL file's queries produce: [({output column: expression}, {clause: sql})], one per statement; None when one doesn't parse."""
    import sqlglot
    from sqlglot import exp
    out = []
    for _, stmt in chunks(strip_jinja(text)):
        try:
            tree = sqlglot.parse_one(stmt, read=dialect or None)
        except Exception:
            return None
        inner = tree.args.get("expression")  # CREATE TABLE AS and INSERT carry the query
        query = tree if isinstance(tree, exp.Query) else inner if isinstance(inner, exp.Query) else None
        if query is None:
            out.append(({}, {"statement": tree.sql(dialect or None).lower()}))
        elif isinstance(query, exp.Select):
            rest = query.copy()
            rest.set("expressions", [])
            out.append(({(e.alias_or_name or e.sql()).lower(): e.unalias().sql(dialect or None).lower() for e in query.expressions},
                        {k: " ".join(x.sql(dialect or None) for x in (v if isinstance(v, list) else [v])).lower()
                         for k, v in rest.args.items() if v and k != "expressions"}))
        else:  # UNION and friends: any edit counts
            out.append(({c.lower(): "" for c in query.named_selects}, {"set operation": query.sql(dialect or None).lower()}))
    return out


CLAUSES = {"from": "FROM", "from_": "FROM", "joins": "JOIN", "where": "WHERE", "group": "GROUP BY", "having": "HAVING",
           "qualify": "QUALIFY", "distinct": "DISTINCT", "limit": "LIMIT", "with": "CTE", "with_": "CTE", "order": "ORDER BY"}


def classify_change(old, new):
    """(label, removed columns, changed columns, added columns, clauses changed): breaking when rows or an existing column
    change, non-breaking when columns are only added, none when only formatting differs, unknown when SQL doesn't parse."""
    if old is None or new is None:
        return "unknown", [], [], [], []
    if len(old) != len(new):
        return "breaking", [], [], [], ["statements added or removed"]
    removed, changed, added, clauses = [], [], [], []
    for (oc, ok), (nc, nk) in zip(old, new):
        removed += [c for c in oc if c not in nc]
        changed += [c for c in oc if c in nc and oc[c] != nc[c]]
        added += [c for c in nc if c not in oc]
        clauses += [CLAUSES.get(k, k.upper()) for k in dict.fromkeys([*ok, *nk]) if ok.get(k) != nk.get(k)]
    clauses = list(dict.fromkeys(clauses))
    if removed or changed or clauses:
        return "breaking", removed, changed, added, clauses
    return ("non-breaking" if added else "none"), [], [], added, []


def review(con, base=None, strict=False, diff=False, where=""):
    """Markdown impact report for this branch's changes against base: what the changed code produces and reads, what's
    downstream, recorded gotchas, SQL that stopped parsing, and the ticket keys; with diff, the data before and after.
    Returns the number of blocking problems."""
    base = base or next((b for b in ("origin/HEAD", "origin/main", "origin/master", "main", "master")
                         if run(["git", "-C", str(ROOT), "rev-parse", "--verify", "-q", b])), None)
    if not base:
        sys.exit("No base to compare with: pass --base <branch or commit>")
    merge_base = run(["git", "-C", str(ROOT), "merge-base", base, "HEAD"]).strip() or base
    changed = sorted(set(run(["git", "-C", str(ROOT), "diff", "--name-only", merge_base]).split())
                     | set(run(["git", "-C", str(ROOT), "ls-files", "--others", "--exclude-standard"]).split()))
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    downs = lambda n: [r[0] for r in q("select dst from edges where src=?", n)]
    tags = {r[0]: r[1][7:] for r in q("select dst, via from edges where via like 'import %'")}
    # merge request pipelines check out a detached HEAD: the branch and title come from the CI's environment
    branch = (os.environ.get("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME") or os.environ.get("GITHUB_HEAD_REF")
              or run(["git", "-C", str(ROOT), "rev-parse", "--abbrev-ref", "HEAD"]).strip())
    subjects = run(["git", "-C", str(ROOT), "log", "--format=%s", f"{merge_base}..HEAD"]).splitlines()
    subjects += [t for t in (os.environ.get("CI_MERGE_REQUEST_TITLE"), os.environ.get("PR_TITLE")) if t]
    key = re.compile(r"\b[A-Z][A-Z0-9]+-\d+\b")
    keys_branch, keys_commits = set(key.findall(branch)), {k for s in subjects for k in key.findall(s)}
    out, blocking = [f"## Data impact of `{branch}` against `{base}`", ""], 0

    produced = {}  # dataset -> changed file that writes it
    for f in changed:
        for r in q("select dataset from writers where file=? union select name from datasets where file=?", f, f):
            produced.setdefault(r[0], f)
    read = sorted({r[0] for f in changed for r in q("select dataset from readers where file=? union select dataset from col_uses where file=?", f, f)}
                  - set(produced))
    weak_all = set()
    if produced:
        out += ["| Changed table | Code | Runs in | Downstream | Read by |", "|---|---|---|---|---|"]
        for ds, f in sorted(produced.items()):
            d = con.execute("select line from datasets where name=?", (ds,)).fetchone()
            run_ = own_runs(con, ds, q("select dag, task, schedule, at from runs where file=? and ? between line_from and line_to and task != ''", f, (d["line"] if d else 1) or 1))[:1]
            down = sorted(reach(ds, downs, 9))
            readers = [r[0] for r in q("select distinct file from readers where dataset=? union select distinct file from col_uses where dataset=?", ds, ds) if r[0] not in changed]
            weak, guesses = weak_links(con, [ds], down)
            weak_all.update(guesses)
            down_txt = ", ".join(n + (f" ({tags[n]})" if n in tags else "") + (" (inferred)" if n in weak else "") for n in down[:8]) + (f" +{len(down) - 8} more" if len(down) > 8 else "")
            tier = con.execute("select tier from datasets where name=?", (ds,)).fetchone()
            out.append(f"| `{ds}`{' **tier ' + tier[0] + '**' if tier and tier[0] else ''} | `{f}` | {f'{run_[0][0]}.{run_[0][1]} `{run_[0][2]}`' if run_ else '-'} | {down_txt or '-'} | "
                       f"{', '.join(f'`{r}`' for r in readers[:5]) + (f' +{len(readers) - 5}' if len(readers) > 5 else '') or '-'} |")
        out.append("")
    if produced:  # the changed tables and what they reach, as a graph (GitHub draws it; a terminal gets the table above)
        nodes = list(dict.fromkeys([*produced, *(n for ds in produced for n in reach(ds, downs, 9))]))[:30]
        if len(nodes) > len(produced):
            inside = set(nodes)
            out += mermaid_graph(nodes, [r for r in q("select src, dst, how from edges") if r[0] in inside and r[1] in inside], produced) + [""]
    if produced:  # what kind of change each changed SQL file makes: SQLMesh-style breaking / non-breaking
        dialect = setting("dialect", "") or (con.execute("select v from meta where k='dialect'").fetchone() or [""])[0]
        kinds = []
        for f, names in sorted({f: [d for d, g in produced.items() if g == f] for f in produced.values()}.items()):
            if not f.endswith((".sql", ".hql")) or not (ROOT / f).is_file():
                continue
            ds, label_names = names[0], ", ".join(f"`{n}`" for n in names)
            old = run(["git", "-C", str(ROOT), "show", f"{merge_base}:{f}"])
            if not old.strip():
                kinds.append(f"- {label_names}: new")
                continue

            def use(c, ds=ds):  # who still depends on a column that went away or changed
                files = [f"`{r[0]}:{r[1]}`" for r in q("select file, line from col_uses where dataset=? and column_name=?", ds, c) if r[0] not in changed][:3]
                feeds = [f"`{r[0]}`" + (" (inferred)" if r[1] == "inferred" else "") for r in q("select dst, how from col_edges where src=?", f"{ds}.{c}")][:3]
                return f"`{c}`" + (f" (read by {', '.join(files)})" if files else "") + (f" (feeds {', '.join(feeds)})" if feeds else "")
            label, gone, edited, added, clauses = classify_change(signature(old, dialect), signature((ROOT / f).read_text(errors="replace"), dialect))
            why = [x for x in (f"removes {', '.join(map(use, gone))}" if gone else "", f"changes {', '.join(map(use, edited))}" if edited else "",
                               f"{' and '.join(clauses)} changed, so rows may differ" if clauses else "", f"adds {', '.join(f'`{c}`' for c in added)}" if added else "") if x]
            kinds.append(f"- {label_names}: " + {"unknown": "unknown, the SQL doesn't parse", "none": "same output (formatting or comments only)"}.get(label, f"**{label}**: " + "; ".join(why)))
        if kinds:
            out += ["**Change type:**", ""] + kinds + [""]
    if weak_all:  # links built from substituted names, bound arguments or patterns, not written plainly: a person should look
        out += [f"**Verify ({len(weak_all)} inferred link{'s' * (len(weak_all) > 1)}):** these were worked out, not read plainly, so check them before relying on the list above:", ""]
        out += [f"- `{s}` → `{d}` (`{v}`)" for s, d, v in sorted(weak_all)[:8]] + ([f"- +{len(weak_all) - 8} more"] if len(weak_all) > 8 else []) + [""]
    if read:
        out += [f"Also reads: {', '.join(f'`{r}`' for r in read[:15])}", ""]
    critical = sorted({(n, r[0]) for ds in produced for n in [ds, *reach(ds, downs, 9)]
                       for r in [con.execute("select tier from datasets where name=?", (n,)).fetchone()] if r and r[0] in ("0", "1")})
    if critical:  # tier 0 and 1 tables this change reaches: the ones to double-check before merging
        out += ["**Critical tables reached:** " + ", ".join(f"`{n}` (tier {tr})" for n, tr in critical[:10]), ""]
    boards = sorted({n for ds in produced for n in reach(ds, downs, 9)} & {r[0] for r in q("select name from datasets where kind like '%dashboard%'")})
    if boards:  # BI dashboards (from a connector such as lookml) that show what this change computes
        out += [f"**Dashboards reached ({len(boards)}):** " + ", ".join(f"`{n}`" for n in boards[:15]) + (f" +{len(boards) - 15} more" if len(boards) > 15 else ""), ""]
    if diff and produced:
        lines, n = data_diff(con, produced, where)
        out, blocking = out + lines, blocking + n
    dags = q(f"""select dag, schedule, count(distinct task) n from runs where substr(at, 1, instr(at || ':', ':') - 1) in
                 ({','.join('?' * len(changed))}) group by dag, schedule order by dag""", *changed) if changed else []
    if dags:
        out += [f"DAGs changed: {', '.join(f'`{d[0]}` `{d[1]}` ({d[2]} tasks)' for d in dags[:15])}" + (f" +{len(dags) - 15} more" if len(dags) > 15 else ""), ""]
    if not produced and not read and not dags:
        out += ["No table in the index is produced or read by the changed files.", ""]
    notes = q(f"select dataset, kind, text, stale from notes where dataset in ({','.join('?' * len(produced))}) and kind in ('gotcha', 'correction', 'decision')", *produced) if produced else []
    if notes:
        out += ["**Recorded gotchas on these tables** (check the change against them):", ""]
        out += [f"- `{n['dataset']}` {n['kind']}: {n['text']}" + (f" ({n['stale']})" if n["stale"] else "") for n in notes[:10]] + [""]
    breaks = [(ds, c, s) for ds in produced for c, s in contract_breaks(con, ds)]
    if breaks:  # the change no longer produces what a contract promises consumers
        blocking += len(breaks)
        out += [f"**Contract broken ({len(breaks)}):**", ""] + [f"- `{ds}` no longer produces required column `{c}` (`{s}`)" for ds, c, s in breaks[:10]] + [""]
    found = practices(con, set(changed)) if changed else []
    if found:  # best practices in the changed files; a secret blocks the merge
        blocking += sum(r == "secret in code" for r, *_ in found)
        out += [f"**Best practices ({len(found)}):**", ""] + [f"- {r}: `{at}` {msg}" for r, at, msg in found[:15]] + [""]
    broken = q(f"select file, line, error from issues where file in ({','.join('?' * len(changed))}) order by file, line", *changed) if changed else []
    if broken:
        blocking += len(broken)
        out += [f"**SQL in changed files that doesn't parse ({len(broken)}):** lineage for it is unknown, so the table above may be incomplete.", ""]
        out += [f"- `{b['file']}:{b['line']}` {b['error']}" for b in broken[:10]] + [""]
    if not keys_branch | keys_commits:
        blocking += 1
        out.append("**No ticket key** in the branch name or commits.")
    elif keys_commits and keys_branch and not keys_commits <= keys_branch:
        out.append(f"**Ticket keys differ:** branch {', '.join(sorted(keys_branch))}, commits {', '.join(sorted(keys_commits))}. Check they're the intended tickets.")
    else:
        out.append(f"Ticket: {', '.join(sorted(keys_branch | keys_commits))} (check it's the intended ticket).")
    out += ["", f"<sub>{len(changed)} changed files. Generated by datawright {VERSION} from the repo's code; "
            + ("the data diff ran read-only SELECTs.</sub>" if diff and produced else "nothing was queried.</sub>")]
    print(terminal_text(out))
    return blocking if strict else 0


