# ---------------------------------------------------------------- coverage

NOISE = {".dll", ".exe", ".so", ".bin", ".cache", ".pyc", ".whl", ".egg", ".md", ".rst", ".txt", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".lock", ".gitignore", ".gitkeep", ".pdf", ".html",
         ".css", ".js", ".ts", ".tsx", ".csv", ".parquet", ".jar", ".class", ".xlsx", ".woff", ".ttf", ".sh", ".dockerignore", ""}
HINTS = [  # what an unread file looks like, from its first few KB, and what to do about it
    (r"<mapper\s+namespace=", "MyBatis mappers (SQL behind Java services): add the mybatis connector (`datawright init --connectors mybatis`)"),
    (r"^\s*(models|sources|seeds|snapshots)\s*:\s*$[\s\S]{0,600}^\s*-\s*name\s*:", "dbt schema files (descriptions, columns): add the dbt_yaml connector (`datawright init --connectors dbt_yaml`)"),
    (r"connector\.class", "Kafka Connect configs: add the kafka_connect connector (`datawright init --connectors kafka_connect`)"),
    (r"^\s*-?\s*(from|source|upstream)\s*:[\s\S]{0,300}^\s*(to|target|downstream)\s*:", "hand-written lineage: add the lineage_yaml connector (`datawright init --connectors lineage_yaml`)"),
    (r"\b(dag_id|schedule_interval|schedule|cron)\s*[:=]", "pipeline or DAG definitions in config: a connector can emit run records"),
    (r"(?im)^[\s\-|>:\"']*(select\s+[\s\S]{1,400}?\bfrom\s+[\w.`\"]+|insert\s+(into|overwrite)\s+\S+|merge\s+into\s+\S+|create\s+(or\s+replace\s+)?(table|view)\s+\S+)", "SQL inside config: a connector can hand it over as sql records"),
]
KNOWN_TOOLS = [  # tools with no built-in reader, by what their code or config contains
    ("Dagster", r"\bfrom dagster\b|\bimport dagster\b"), ("Luigi", r"\bimport luigi\b"),
    ("Apache Beam", r"\bimport apache_beam\b"), ("SQLMesh", r"\bfrom sqlmesh\b|MODEL\s*\("),
    ("Great Expectations", r"\bgreat_expectations\b"), ("Soda", r"\bsoda(cl)?\b.*checks"),
    ("PyODPS (MaxCompute)", r"\bfrom odps\b|\bimport odps\b|\bpyodps\b"), ("DataWorks", r"\bdataworks\b"),
    ("Databricks notebooks", r"^# Databricks notebook source"), ("Kestra", r"^\s*namespace:\s*\S+[\s\S]*\btasks:"),
]
BI_TOOLS = {  # dashboards live in the tool, not the repo: how to bring them in
    "Superset": (r"superset_config|apache[-/]superset|\bfrom superset\b|SUPERSET_URL",
                 "add the superset connector (`datawright init --connectors superset`), then `uv run <data-context skill>/assets/bi_export.py superset` (or drop a dashboard export ZIP in .data-context/imports/)"),
    "Metabase": (r"\bmetabase\b|METABASE_URL", "add the metabase connector (`datawright init --connectors metabase`), then `uv run <data-context skill>/assets/bi_export.py metabase`"),
    "Tableau": (r"\.twbx?\b|tableauserverclient|\btabcmd\b", "export workbook lineage from Tableau's Metadata API as lineage YAML (lineage_yaml connector)"),
}


def coverage(con):
    """What in this repo datawright doesn't read yet, largest gaps first, with what to do about each: the loop that teaches datawright a new stack."""
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    rows = q("select path, reader from files")
    if not rows:
        return print("No file list in the index: run datawright build (this index predates coverage)")
    read = [r for r in rows if r["reader"]]
    unread = [r["path"] for r in rows if not r["reader"] and Path(r["path"]).suffix.lower() not in NOISE]
    by = Counter(r for x in read for r in x["reader"].split(", "))
    print(f"read {len(read)} of {len(rows)} files ({', '.join(f'{k} {v}' for k, v in by.most_common())})")
    groups = {}
    for f in unread:
        try:
            head = (ROOT / f).read_text(encoding="utf-8", errors="replace")[:4000]
        except OSError:
            continue
        if re.search(r"(^|/)(\.?pre-commit|docker-compose|compose|mkdocs|\.?readthedocs|codecov|dependabot|renovate|\.?gitlab-ci|netlify|vercel)[^/]*$", f):
            continue  # tool configs: never data, whatever their keys look like
        hint = next((h for rx, h in HINTS if re.search(rx, head, re.M)), None)
        if hint:
            groups.setdefault(hint, []).append(f)
    ext = Counter(Path(f).suffix.lower() for f in unread)
    print(f"not read: {len(unread)} files ({', '.join(f'{e} {n}' for e, n in ext.most_common(6))})" if unread else "not read: nothing that looks like code or config")
    for hint, fs in sorted(groups.items(), key=lambda x: -len(x[1])):
        print(f"  {len(fs)} look like {hint}  e.g. {fs[0]}")
    tasks = q("""select dag, task, min(at) at from runs r where task != '' group by dag, task having count(*) = 1 and not exists
                 (select 1 from datasets d where d.file = r.file and d.line between r.line_from and r.line_to)""")  # only the task call itself
    if tasks:
        dag_files = Counter(t["at"].rsplit(":", 1)[0] for t in tasks)
        print(f"DAG tasks with no idea what they run: {len(tasks)} in {len(dag_files)} files, e.g. {dag_files.most_common(1)[0][0]}"
              "  (an operator datawright doesn't follow, or code in another repo: add it under repos:, or a connector can link them)")
    by_connector = "e.via in (select path from files where reader like '%connector%')"
    known = """(select name from datasets where file is not null and file not in (select path from files where reader like '%connector%')
                union select dataset from readers union select src from edges where via not in (select path from files where reader like '%connector%')
                union select dst from edges where via not in (select path from files where reader like '%connector%'))"""
    total = q(f"select count(*) from edges e where {by_connector}")[0][0]
    lonely = q(f"select count(*) from edges e where {by_connector} and e.src not in {known} and e.dst not in {known}")[0][0]
    if total:
        print(f"connector edges: {total}, {total - lonely} touching a table found in code; the rest link systems outside the code"
              + (" or use names that don't line up (DROP_PARTS, TYPE_PREFIX, RENAME in the connector)" if lonely else ""))
    how = dict(q("select how, count(*) from edges group by how"))
    if how:
        print(f"lineage links: {how.get('direct', 0)} read plainly from code, {how.get('declared', 0)} declared by a connector or import, "
              f"{how.get('inferred', 0)} inferred (substituted names, bound arguments, patterns: shown as (inferred), check before relying)")
    unset = q("select name from datasets where name like '%env:%' order by name")
    if unset:
        print(f"names set by environment variables at deploy time: {len(unset)}, e.g. {unset[0][0]}  (add the repo with the deploy config"
              " under repos:, list their values under env: in config.yml, or have a connector emit env records)")
    unused = json.loads((q("select v from meta where k='unused_env'") or [["[]"]])[0][0])
    if unused:
        print(f"values given for variables no code reads: {len(unused)}, e.g. {', '.join(unused[:3]).upper()}  (a typo in config.yml or"
              " a connector's source, or docs that no longer match the code)")
    unparsed = q("select count(*) n, count(distinct file) f from issues")[0]
    dbt_failing = q("select count(distinct i.file) from issues i join datasets d on d.file = i.file and d.kind = 'dbt model'")[0][0]
    if dbt_failing >= 10 and not q("select 1 from datasets where kind = 'dbt model' and description is not null limit 1"):
        print(f"dbt models whose SQL doesn't parse: {dbt_failing} (Jinja datawright can't expand). Their lineage still comes from ref() and source();"
              " for columns, run `dbt parse` (no warehouse needed) so target/manifest.json exists, then rebuild")
    if unparsed["n"]:
        print(f"unparsed: {unparsed['n']} statements in {unparsed['f']} files (datawright lint; wrong dialect, or syntax a connector should strip)")
    seen, bi = Counter(), set()
    for r in rows:
        if Path(r["path"]).suffix.lower() in (".py", ".yaml", ".yml", ".json", ".sql", ".ipynb", ".env") or Path(r["path"]).name.startswith(("Dockerfile", "docker-compose")):
            try:
                text = (ROOT / r["path"]).read_text(encoding="utf-8", errors="replace")[:20000]
            except OSError:
                continue
            seen.update(n for n, rx in KNOWN_TOOLS if re.search(rx, text, re.M))
            bi.update(n for n, (rx, _) in BI_TOOLS.items() if re.search(rx, text, re.M | re.I))
    covered = " ".join(r["reader"] for r in read).lower()  # a connector named after a tool reads it now
    seen = Counter({n: c for n, c in seen.items() if f"connector {n.split()[0].lower()}" not in covered})
    for n in sorted(b for b in bi if f"connector {b.lower()}" not in covered):  # dashboards reached show in impact and review
        print(f"{n} is used here, but its dashboards aren't in the index: {BI_TOOLS[n][1]}")
    if seen:
        print("tools used here that datawright has no built-in reader for: " + ", ".join(f"{n} ({c} files)" for n, c in seen.most_common()))
    if groups or tasks or seen:
        print("next: write a connector for the largest gap (data-context skill, references/connectors.md), rebuild, and run coverage again")


