# ---------------------------------------------------------------- writes

def doc_path(name):
    return HOME / "notes" / (re.sub(r"[/\\]", "_", name) + ".yml")  # names can come from a model or be paths (delta.`/mnt/x`): never a dir


def load_doc(name):
    import yaml
    p = doc_path(name)
    return (yaml.safe_load(p.read_text()) or {}) if p.exists() else {"dataset": name}


def save_doc(name, doc):
    import yaml
    doc_path(name).parent.mkdir(parents=True, exist_ok=True)
    doc_path(name).write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100))


def canonical(arg):
    if DB.exists():
        name = resolve(sqlite3.connect(DB), arg, quiet=True)
        if name:
            return name, True
    return arg.lower(), False


def note(arg, text, kind, column, source):
    name, known = canonical(arg)
    entry = {"date": TODAY, "kind": kind, **({"column": column.lower()} if column else {}), "text": text, **({"source": source} if source else {})}
    doc = load_doc(name)
    doc.setdefault("notes", []).append(entry)
    save_doc(name, doc)
    if known:
        with sqlite3.connect(DB) as con:
            con.execute("insert into notes values (?,?,?,?,?,?,'')", (name, (column or "").lower(), TODAY, kind, text, source or ""))
    print(f"noted on {name}" + ("" if known else " (not in the index yet)") + f": {doc_path(name).relative_to(ROOT)}")
    return name


def set_fields(arg, pairs):
    name, known = canonical(arg)
    doc, fields = load_doc(name), {}
    for p in pairs:
        k, _, v = p.partition("=")
        k = "description" if k == "meaning" else k  # the word agents reach for
        if k not in ("description", "grain", "key", "owner", "freshness", "tier") or not v:
            sys.exit(f"Expected field=value with field in description, grain, key, owner, freshness, tier; got '{p}'")
        if k == "tier" and v not in ("0", "1", "2", "3"):
            sys.exit("tier is 0 (most critical: revenue, regulatory, customer-facing) to 3 (scratch)")
        fields[k] = [x.strip() for x in v.split(",")] if k == "key" else v
    doc.update(fields, verified=TODAY)
    doc = {"dataset": doc.pop("dataset", name), **doc}
    save_doc(name, doc)
    if known:
        with sqlite3.connect(DB) as con:
            for k, v in fields.items():
                con.execute(f"update datasets set {k}=?, verified=? where name=?", (", ".join(v) if isinstance(v, list) else v, TODAY, name))
    print(f"recorded {', '.join(fields)} for {name}: {doc_path(name).relative_to(ROOT)}")
    return name


