def main(argv=None):
    global ROOT, HOME, DB
    here = Path(__file__).absolute().parent  # a copy in <repo>/.data-context/ serves that repo, wherever it's started from
    ROOT = here.parent if here.name == ".data-context" else Path(run(["git", "rev-parse", "--show-toplevel"]).strip() or Path.cwd())
    HOME, DB = ROOT / ".data-context", ROOT / ".data-context" / "index.db"
    p = argparse.ArgumentParser(prog="datawright", description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__.split("\n\n")[2])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build").add_argument("--dialect", help="sqlglot dialect, e.g. snowflake, bigquery, spark, postgres")
    c = sub.add_parser("card")
    c.add_argument("dataset")
    c.add_argument("--budget", type=int, default=800, help="approximate token budget")
    sub.add_parser("code").add_argument("dataset")
    sub.add_parser("impact").add_argument("target", metavar="dataset[.column]")
    sub.add_parser("advise").add_argument("target", metavar="dataset", nargs="?")
    cp = sub.add_parser("concepts")
    cp.add_argument("name", nargs="?")
    cp.add_argument("--write", action="store_true", help="merge proposals into .data-context/concepts.yml for review")
    ex = sub.add_parser("export")
    ex.add_argument("target", choices=("dbt", "openmetadata", "datahub"))
    ex.add_argument("--all", action="store_true", help="include proposed concepts, not only confirmed ones")
    ex.add_argument("--out", help="folder to write to (default .data-context/exports)")
    jp = sub.add_parser("join")
    jp.add_argument("a")
    jp.add_argument("b")
    bf = sub.add_parser("backfill")
    bf.add_argument("target", metavar="dataset")
    bf.add_argument("--from", dest="start", help="first date to refill, e.g. 2026-11-01")
    bf.add_argument("--to", dest="end", help="last date to refill")
    ln = sub.add_parser("lineage")
    ln.add_argument("target")
    ln.add_argument("--up", type=int, default=1)
    ln.add_argument("--down", type=int, default=1)
    ln.add_argument("--limit", type=int, default=60)
    sub.add_parser("search").add_argument("words", nargs="+")
    n = sub.add_parser("note")
    n.add_argument("dataset")
    n.add_argument("text")
    n.add_argument("--kind", default="gotcha", choices=KINDS)
    n.add_argument("--column")
    n.add_argument("--source")
    s = sub.add_parser("set")
    s.add_argument("dataset")
    s.add_argument("pairs", nargs="+", metavar="field=value")
    sub.add_parser("lint")
    sub.add_parser("savings").add_argument("--days", type=int, default=30)
    qp = sub.add_parser("query")
    qp.add_argument("sql")
    qp.add_argument("--limit", type=int, default=100, help="max rows, at most 1000")
    sub.add_parser("serve")
    ck = sub.add_parser("check")
    ck.add_argument("datasets", nargs="*", help="default: datasets with freshness set")
    ck.add_argument("--all", action="store_true", help="also every table an Airflow task produces")
    ck.add_argument("--plan", action="store_true", help="show the column, limit and window per table without querying")
    sub.add_parser("update").add_argument("--from", dest="source", metavar="PATH", help="a datawright.py to update from")
    it = sub.add_parser("init")
    it.add_argument("--connectors", default="", help="ready-made connectors to copy in, e.g. dbt_yaml,lookml")
    sub.add_parser("coverage")
    rv = sub.add_parser("review")
    rv.add_argument("--base", help="branch or commit to compare with (default: origin/HEAD, main or master)")
    rv.add_argument("--strict", action="store_true", help="exit 1 if changed SQL doesn't parse, a contract breaks, a key gains duplicates, a secret is committed, or no ticket key is found")
    rv.add_argument("--diff", action="store_true", help="compare each changed table's data with its dev copy (dev_name in config.yml), through query_command")
    rv.add_argument("--where", default="", help="with --diff: a filter for both sides, e.g. \"order_date >= current_date - 7\"")
    sub.add_parser("version")
    a = p.parse_args(argv)
    started = time.time()
    if a.cmd == "init":  # run from the installed skill's copy: puts datawright, its config and chosen connectors in this repo, then builds
        import shutil
        src = Path(__file__).resolve()
        HOME.mkdir(exist_ok=True)
        if src != (HOME / "datawright.py").resolve():
            shutil.copy(src, HOME / "datawright.py")
        assets = src.parent.parent / "assets"
        if not assets.is_dir():  # installed from PyPI: the assets sit next to the module
            assets = src.parent / "datawright_kit" / "assets"
        if not assets.is_dir():  # run from the repo's copy (to add a connector later): the installed skill has the assets
            assets = next((path.parent.parent / "assets" for _, path in installed_copies() if (path.parent.parent / "assets").is_dir()), assets)
        if not (HOME / "config.yml").exists() and (assets / "config.yml").exists():
            shutil.copy(assets / "config.yml", HOME / "config.yml")
        for c in filter(None, (x.strip().removesuffix(".py") for x in a.connectors.split(","))):
            if not (assets / "connectors" / f"{c}.py").exists():
                sys.exit(f"No ready-made connector '{c}'. Available: {', '.join(sorted(f.stem for f in (assets / 'connectors').glob('*.py')))}")
            (HOME / "connectors").mkdir(exist_ok=True)
            shutil.copy(assets / "connectors" / f"{c}.py", HOME / "connectors" / f"{c}.py")
        dw = HOME / "dw"  # a short command for people: .data-context/dw card orders (agents keep using the full path)
        dw.write_text('#!/bin/sh\nexec uv run -q "$(dirname "$0")/datawright.py" "$@"\n')
        dw.chmod(0o755)
        print(f"datawright {VERSION} installed in .data-context/ (commit it; the index and imports are gitignored). From now on: uv run .data-context/datawright.py <command>, or .data-context/dw <command>")
        build(None)
        print("next: `coverage` shows what it doesn't read yet; `card <table>` and `lint` show what it found")
    elif a.cmd == "build":
        build(a.dialect)
        track("build", None, started=started)
    elif a.cmd == "card":
        sys.stdout.write(answer("card", card, a.dataset, a.budget))
    elif a.cmd == "impact":
        sys.stdout.write(answer("impact", impact, a.target))
    elif a.cmd == "concepts":
        sys.stdout.write(answer("concepts", concepts_cmd, a.name, a.write))
    elif a.cmd == "export":
        export(connect(), a.target, a.all, a.out)
    elif a.cmd == "join":
        sys.stdout.write(answer("join", join_path, a.a, a.b))
    elif a.cmd == "advise":
        sys.stdout.write(answer("advise", advise, a.target))
    elif a.cmd == "backfill":
        sys.stdout.write(answer("backfill", backfill, a.target, a.start, a.end))
    elif a.cmd == "code":
        sys.stdout.write(answer("code", code, a.dataset))
    elif a.cmd == "lineage":
        sys.stdout.write(answer("lineage", lineage_cmd, a.target, a.up, a.down, a.limit))
    elif a.cmd == "search":
        sys.stdout.write(answer("search", search, a.words))
    elif a.cmd == "query":
        print(query(a.sql, a.limit))
    elif a.cmd == "serve":
        serve()
    elif a.cmd == "check":
        if check(connect(), a.datasets, a.all, a.plan):
            sys.exit(1)  # a failed task alerts through the scheduler, like any other
    elif a.cmd == "update":
        update(a.source)
    elif a.cmd == "coverage":
        coverage(connect())
    elif a.cmd == "review":
        if review(connect(), a.base, a.strict, a.diff, a.where):
            sys.exit(1)
    elif a.cmd == "lint":
        lint(connect())
    elif a.cmd == "savings":
        savings(a.days)
    elif a.cmd == "note":
        track("note", note(a.dataset, a.text, a.kind, a.column, a.source))
    elif a.cmd == "set":
        track("set", set_fields(a.dataset, a.pairs))
    else:
        print(VERSION)


if __name__ == "__main__":
    main()
