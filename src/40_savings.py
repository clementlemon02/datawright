# ---------------------------------------------------------------- savings

def setting(key, default):
    """Read a top-level scalar from config.yml without needing PyYAML."""
    p = HOME / "config.yml"
    m = re.search(rf"^{key}:[ \t]*([^#\n]*)", p.read_text(), re.M) if p.exists() else None
    v = m.group(1).strip() if m else ""
    if len(v) > 1 and v[0] in "'\"" and v[-1] == v[0]:
        v = v[1:-1]
    return v or default


def track(cmd, target, text="", files=(), started=None):
    """Append one line to .data-context/usage.jsonl (local, gitignored). A file counts at most once an hour."""
    if str(setting("track", "true")).lower() == "false":
        return
    log, recent = HOME / "usage.jsonl", set()
    if files and log.exists():
        for line in log.read_text().splitlines()[-1000:]:
            e = json.loads(line)
            if e["ts"] > time.time() - 3600:
                recent.update(e.get("paths", []))
    fresh = sorted(f for f in set(files) - recent if f and (ROOT / f).is_file())
    entry = {"ts": int(time.time()), "cmd": cmd, "target": target, "out": len(text) // 4,
             "baseline": sum((ROOT / f).stat().st_size for f in fresh) // 4, "paths": fresh,
             "ms": int((time.time() - (started or time.time())) * 1000)}
    HOME.mkdir(exist_ok=True)
    with open(log, "a") as fh:
        fh.write(json.dumps(entry) + "\n")


def answer(cmd, fn, *args):
    """Run a lookup, log it for `savings` and return its text."""
    started, buf = time.time(), io.StringIO()
    with contextlib.redirect_stdout(buf):
        name, files = fn(connect(), *args)
    track(cmd, name, buf.getvalue(), files, started)
    return buf.getvalue()


def duration(secs):
    return f"{secs / 3600:.1f} h" if secs >= 3600 else f"{secs / 60:.0f} min" if secs >= 60 else f"{secs:.0f} s"


def savings(days):
    log = HOME / "usage.jsonl"
    es = [e for e in map(json.loads, log.read_text().splitlines() if log.exists() else []) if e["ts"] >= time.time() - days * 86400]
    if not es:
        return print("No usage recorded yet. Every card, lineage and search answer is logged here.")
    qs = [e for e in es if e["cmd"] in ("card", "lineage", "search", "impact")]
    price, per_file = float(setting("price_per_mtok", 3)), float(setting("seconds_per_file", 4))
    read = sum(e["out"] for e in qs)
    saved = sum(max(e["baseline"] - e["out"], 0) for e in qs)
    avoided = sum(len(e["paths"]) for e in qs)
    secs = avoided * per_file  # a lookup replaces the search an agent would otherwise run, so the file reads are the saving
    builds = [e for e in es if e["cmd"] == "build"]
    print(f"datawright savings, last {days} days")
    print(f"  questions answered: {len(qs)} ({', '.join(f'{k} {v}' for k, v in Counter(e['cmd'] for e in qs).most_common())})"
          + (f" · index builds: {len(builds)}, avg {sum(e['ms'] for e in builds) / len(builds) / 1000:.1f}s, 0 model tokens" if builds else ""))
    print(f"  tokens: {read:,} read instead of ~{read + saved:,}, so ~{saved:,} saved ({saved / max(read + saved, 1):.0%})")
    print(f"  money: ~${saved * price / 1e6:,.2f} saved at ${price:g} per million input tokens")
    print(f"  time: ~{duration(secs)} saved: {avoided:,} file reads avoided by {len(qs)} lookups, at {per_file:g}s per read")
    print(f"  knowledge captured: {sum(e['cmd'] == 'note' for e in es)} notes, {sum(e['cmd'] == 'set' for e in es)} datasets described")
    top = Counter(e["target"] for e in qs if e["cmd"] != "search").most_common(5)
    if top:
        print("  most asked about: " + ", ".join(f"{t} ({n})" for t, n in top))
    if DB.exists():
        con = sqlite3.connect(DB)
        blank = [t for t, n in top if n >= 2 and con.execute(
            "select 1 from datasets where name=? and grain is null and description is null", (t,)).fetchone()]
        stale = con.execute("select count(*) from notes where stale != ''").fetchone()[0]
        if blank:
            print(f"  next: record grain and meaning for {', '.join(blank)} (datawright set): asked about often, still blank")
        if stale:
            print(f"  next: recheck {stale} stale notes (datawright lint)")
    print("  Conservative: each file counts once an hour; grep calls, warehouse queries and context re-sent on later turns aren't counted.\n"
          "  Change the assumptions with price_per_mtok and seconds_per_file in .data-context/config.yml.")


