# ---------------------------------------------------------------- warehouse queries

def query(sql, limit=100):
    """Run one read-only SELECT through the CLI in query_command, capped at `limit` rows.
    ponytail: a parser guard against mistakes, not attackers (a side-effecting function in a SELECT passes); the read-only user is the boundary."""
    import sqlglot
    from sqlglot import exp
    cmd, limit, dialect = setting("query_command", ""), max(1, min(int(limit), 1000)), setting("dialect", "")
    if "{sql}" not in cmd:
        sys.exit('No warehouse connection: set query_command in .data-context/config.yml, e.g. psql "$DATABASE_URL" --csv -c {sql}')
    if not dialect and DB.exists():
        dialect = (connect().execute("select v from meta where k='dialect'").fetchone() or [""])[0]
    try:
        stmts = [s for s in sqlglot.parse(sql, read=dialect or None) if s]
    except Exception as e:
        sys.exit(f"Rejected: not valid {dialect or 'SQL'}: {str(e).splitlines()[0][:150]}")
    writes = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Into, exp.Lock, exp.Create, exp.Drop, exp.Alter, exp.Command)
    if len(stmts) != 1 or not isinstance(stmts[0], exp.Query) or stmts[0].find(*writes):
        sys.exit("Rejected: only one read-only SELECT (or WITH … SELECT) runs here; writes, DDL and multiple statements don't.")
    q = stmts[0]
    n = q.args["limit"].expression if isinstance(q.args.get("limit"), exp.Limit) else None
    if not (n is not None and n.is_int and int(n.name) <= limit):
        q = q.limit(limit)
    text = q.sql(dialect=dialect or None, comments=False)  # run what was parsed, so no comment or second statement rides along
    argv = [os.path.expanduser(os.path.expandvars(t)).replace("{sql}", text) for t in shlex.split(cmd)]
    try:
        r = subprocess.run(argv, cwd=ROOT, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=120)
    except FileNotFoundError:
        sys.exit(f"query_command failed: {argv[0]} not found")
    except subprocess.TimeoutExpired:
        sys.exit("Query timed out after 120s: narrow it down")
    if r.returncode:
        sys.exit(f"Query failed: {(r.stderr or r.stdout).strip()[-1500:]}")  # the tail: errors and tracebacks end with the message
    out = r.stdout.strip()
    return out[:10000] + "\n… cut at 10,000 characters: select fewer columns or rows" if len(out) > 10000 else out or "(no rows)"


# ---------------------------------------------------------------- monitoring

def every_minutes(schedule):
    """Roughly how often an Airflow schedule runs, in minutes; None when manual or unknown. ponytail: cron is approximated."""
    s = (schedule or "").strip().lower()
    named = {"@hourly": 60, "@daily": 1440, "@weekly": 10080, "@monthly": 43200, "@yearly": 525600, "@annually": 525600}
    if s in named:
        return named[s]
    if m := re.fullmatch(r"timedelta\((.*)\)", s):
        units = {"weeks": 10080, "days": 1440, "hours": 60, "minutes": 1, "seconds": 1 / 60}
        return sum(float(v) * units.get(k, 0) for k, v in re.findall(r"(\w+)\s*=\s*([\d.]+)", m[1])) or None
    f = s.split()
    if len(f) != 5:
        return None
    step = lambda x: int(x.split("/")[1]) if re.fullmatch(r"[\d*-]+/\d+", x) else None
    return (1 if f[0] == "*" else step(f[0]) if step(f[0]) and f[1] == "*" else 60 if f[1] == "*" else 60 * step(f[1]) if step(f[1])
            else 1440 if f[2] == "*" and f[4] == "*" else 10080 if f[2] == "*" else 43200)


def time_column(con, name):
    """The column that best says when rows arrived: a timestamp named like loaded_at or updated_at, else any date column."""
    def score(c):  # only a time-typed or time-named column qualifies; names like event_name don't
        n, typ = c[0], (c[1] or "").lower()
        timeish = ("time" in typ or "date" in typ) and "int" not in typ, bool(re.search(r"(_at|_on|_time|_ts|_date|^date|^dt|^ds)$", n))
        return 0 if not any(timeish) else timeish[0] * 4 + timeish[1] + bool(re.search(r"load|ingest|updated|modified|insert", n)) * 2
    best = max(con.execute("select name, type from columns where dataset=?", (name,)).fetchall(), key=score, default=None)
    return best[0] if best and score(best) else None


def parse_ts(s):
    s = re.sub(r"(\.\d{6})\d+", r"\1", s.strip().replace("T", " ").replace("Z", "+00:00"))
    return datetime.datetime.fromisoformat(re.sub(r"([+-]\d\d)$", r"\1:00", s)).replace(tzinfo=None)


def check(con, names, scheduled, plan=False):
    """Freshness and volume for each dataset, through query_command. Returns the number of problems.
    Freshness: age of the newest row vs a limit (set, or twice the schedule plus 30 minutes). Volume: rows in the latest
    window vs the window before it. ponytail: compared in the warehouse session's time zone; one query per dataset."""
    import sqlglot
    dialect = setting("dialect", "") or (con.execute("select v from meta where k='dialect'").fetchone() or [""])[0]
    if not dialect and not plan:
        sys.exit("check compares against the warehouse's own clock, so it needs the engine: set dialect: in .data-context/config.yml")
    rows = con.execute("select * from datasets where file is not null and name not like 'kafka:%' order by name").fetchall()
    rows = [d for d in rows if d["name"] in names] if names else [d for d in rows if d["freshness"] or (scheduled and con.execute(
        "select 1 from runs where file=? and ? between line_from and line_to", (d["file"], d["line"] or 1)).fetchone())]
    if names and len(rows) < len(names):
        rows = [con.execute("select * from datasets where name=?", (resolve(con, n),)).fetchone() for n in names]
    if not rows:
        print("nothing to check: set a time column with `datawright set <dataset> freshness=<column> [2h]`, or use --all for every scheduled table")
    problems = 0
    for d in rows:
        spec = (d["freshness"] or "").split()
        col = spec[0] if spec else time_column(con, d["name"])
        run = next(iter(own_runs(con, d["name"], con.execute("select dag, task, schedule, at from runs where file=? and ? between line_from and line_to and task != ''",
                                                              (d["file"], d["line"] or 1)).fetchall())), None)
        every = every_minutes(run["schedule"]) if run else None
        unit = re.fullmatch(r"(\d+)([mhd])", spec[1]) if len(spec) > 1 else None
        streaming = con.execute("select 1 from edges where dst=? and src like 'kafka:%' limit 1", (d["name"],)).fetchone()
        tier = d["tier"]  # tier 0 alerts soonest: a critical table late by one run is already an incident
        slack = {"0": 1.0, "1": 1.5}.get(tier, 2.0)
        limit = (int(unit[1]) * {"m": 1, "h": 60, "d": 1440}[unit[2]] if unit else (120 if tier == "0" else 360) if streaming  # rows arrive with events
                 else slack * every + (15 if tier == "0" else 30) if every else 1560)
        where = f"  (runs in {run['dag']}.{run['task']} {run['schedule']})" if run else ""
        if not col:
            print(f"SKIP   {d['name']}  no time column: datawright set {d['name']} freshness=<column> [2h]")
            continue
        w = 1440 if streaming else max(int(every or 1440), 60)  # streaming traffic swings by hour: compare whole days
        if plan:  # what would run, without a query: review this before scheduling the checks
            print(f"PLAN   {d['name']}  {col} ({'set' if spec else 'guessed'}), alert after {duration(limit * 60)} "
                  f"({'set' if unit else 'streaming default: set one from the topic quiet hours' if streaming else 'from the schedule' if every else 'default'}), "
                  f"volume window {duration(w * 60)}{where}")
            continue
        sql = sqlglot.transpile(f"""SELECT CAST(m.latest AS VARCHAR) AS latest, CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS VARCHAR) AS now_,
            SUM(CASE WHEN CAST(t.{col} AS TIMESTAMP) > CAST(m.latest AS TIMESTAMP) - INTERVAL {w} MINUTE THEN 1 ELSE 0 END) AS recent,
            SUM(CASE WHEN CAST(t.{col} AS TIMESTAMP) <= CAST(m.latest AS TIMESTAMP) - INTERVAL {w} MINUTE THEN 1 ELSE 0 END) AS before
            FROM {d['name']} AS t CROSS JOIN (SELECT MAX({col}) AS latest FROM {d['name']}) AS m
            WHERE CAST(t.{col} AS TIMESTAMP) > CAST(m.latest AS TIMESTAMP) - INTERVAL {2 * w} MINUTE GROUP BY m.latest""",
                                read="duckdb", write=dialect or None)[0]
        try:
            r = next(csv.DictReader(io.StringIO(query(sql, 10))), None)
        except SystemExit as e:
            print(f"ERROR  {d['name']}  {str(e.code)[:200]}")
            problems += 1
            continue
        if not r or not r.get("latest"):
            print(f"EMPTY  {d['name']}  no rows{where}")
            problems += 1
            continue
        latest, now = parse_ts(r["latest"]), parse_ts(r["now_"])
        dated = len(r["latest"].strip()) <= 10  # a date column: the latest day is complete only at its end
        age = (now - latest).total_seconds() / 60 - (1440 if dated else 0)
        recent, before = int(float(r["recent"] or 0)), int(float(r["before"] or 0))
        down = len(reach(d["name"], lambda n: [x[0] for x in con.execute("select dst from edges where src=?", (n,))], 9))
        hint = f"; {down} downstream (datawright impact {d['name']})" if down else ""
        if age > limit:
            print(f"STALE  {d['name']}  newest {col} {r['latest']} is {duration(age * 60)} old; alert after {duration(limit * 60)}{where}{hint}")
            problems += 1
        elif before >= 20 and recent < before / 2:
            print(f"LOW    {d['name']}  {recent} rows in the latest {duration(w * 60)} vs {before} before ({recent / before - 1:.0%}){where}{hint}")
            problems += 1
        else:
            print(f"OK     {d['name']}  newest {col} {r['latest']}, {recent} rows in the latest {duration(w * 60)}")
    return problems


