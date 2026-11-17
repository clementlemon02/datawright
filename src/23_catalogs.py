def read_rows(path: Path):
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = "\t" if path.suffix == ".tsv" or sample.count("\t") > sample.count(",") else ","
        for row in csv.DictReader(f, delimiter=delim):
            yield {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}


def pick(row, *keys):
    return next((row[k] for k in keys if row.get(k)), "")


def json_docs(path):
    """Records in a .json or .jsonl export: a list, {"events"/"data": [...]}, one object, or one object per line."""
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        doc = json.loads(text)
    except ValueError:
        doc = [json.loads(line) for line in text.splitlines() if line.strip().startswith("{")]
    doc = doc.get("events") or doc.get("data") or doc if isinstance(doc, dict) else doc
    return [d for d in (doc if isinstance(doc, list) else [doc]) if isinstance(d, dict)]


def load_catalog(ix, path):
    """Lineage and schemas exported from OpenLineage (Marquez, Airflow, Spark), DataHub or OpenMetadata, told apart by shape."""
    dh_ds = lambda urn: (re.search(r"urn:li:dataset:\(urn:li:dataPlatform:[^,]+,([^,]+),", urn or "") or [None, ""])[1].lower()
    dh_col = lambda urn: (lambda m: (dh_ds(m[1]), re.sub(r"\[[^\]]*\]\.", "", m[2]).lower()) if m else ("", ""))(
        re.search(r"urn:li:schemaField:\((urn:li:dataset:\(.*\)),(.+)\)$", urn or ""))
    om = lambda fqn: fqn.replace('"', "").split(".", 1)[-1].lower() if fqn else ""  # drop the service name
    for d in json_docs(path):
        if "inputs" in d or "outputs" in d:  # OpenLineage run event
            ins = [i["name"].lower() for i in d.get("inputs") or [] if i.get("name")]
            for o in d.get("outputs") or []:
                out = (o.get("name") or "").lower()
                if not out:
                    continue
                ix.dataset(out, "warehouse")
                for i in ins:
                    ix.dataset(i)
                    ix.edge(i, out, "import openlineage", "declared")
                facets = o.get("facets") or {}
                for f in (facets.get("schema") or {}).get("fields") or []:
                    ix.column(out, f["name"], f.get("type"), f.get("description"))
                for col, f in ((facets.get("columnLineage") or {}).get("fields") or {}).items():
                    for src in f.get("inputFields") or []:
                        ix.col_edges.add((f"{src['name'].lower()}.{src['field'].lower()}", f"{out}.{col.lower()}"))
        elif "entityUrn" in d or "aspectName" in d:  # DataHub metadata change proposal
            a = d.get("aspect") or {}
            a = a.get("json") or json.loads(a.get("value") or "{}")
            dst = dh_ds(d.get("entityUrn"))
            if d.get("aspectName") == "upstreamLineage" and dst:
                ix.dataset(dst, "warehouse")
                for u in a.get("upstreams") or []:
                    ix.dataset(dh_ds(u.get("dataset")))
                    ix.edge(dh_ds(u.get("dataset")), dst, "import datahub", "declared")
                for g in a.get("fineGrainedLineages") or []:
                    for s in map(dh_col, g.get("upstreams") or []):
                        for t in map(dh_col, g.get("downstreams") or []):
                            if s[0] and t[0]:
                                ix.col_edges.add((f"{s[0]}.{s[1]}", f"{t[0]}.{t[1]}"))
            elif d.get("aspectName") == "schemaMetadata" and dst:
                for f in a.get("fields") or []:
                    ix.column(dst, re.sub(r"\[[^\]]*\]\.", "", f["fieldPath"]), f.get("nativeDataType"), f.get("description"))
        elif "upstreamEdges" in d or "downstreamEdges" in d:  # OpenMetadata lineage graph
            ids = {n["id"]: n.get("fullyQualifiedName") for n in [d.get("entity") or {}, *(d.get("nodes") or [])] if n.get("id")}
            name = lambda e: om(e.get("fullyQualifiedName") or ids.get(e.get("id"))) if isinstance(e, dict) else om(ids.get(e))
            for e in (d.get("upstreamEdges") or []) + (d.get("downstreamEdges") or []):
                s, t = name(e.get("fromEntity")), name(e.get("toEntity"))
                if s and t:
                    ix.dataset(s)
                    ix.dataset(t, "warehouse")
                    ix.edge(s, t, "import openmetadata", "declared")
                for c in ((e.get("lineageDetails") or {}).get("columnsLineage")) or []:
                    for f in c.get("fromColumns") or []:
                        if c.get("toColumn"):
                            ix.col_edges.add((om(f), om(c["toColumn"])))
        elif "fullyQualifiedName" in d and "columns" in d:  # OpenMetadata table
            t = om(d["fullyQualifiedName"])
            ix.dataset(t, "warehouse", description=d.get("description"))
            for c in d["columns"]:
                ix.column(t, c["name"], c.get("dataTypeDisplay") or c.get("dataType"), c.get("description"))


def load_imports(ix):
    """Read warehouse and catalog exports; returns query-log statements for parsing."""
    csv.field_size_limit(2**31 - 1)  # query texts can be long
    stmts = []
    for path in sorted((HOME / "imports").glob("*.json*")):
        load_catalog(ix, path)
    for path in sorted((HOME / "imports").glob("*.[ct]sv")):
        kind = path.stem.lower()
        for r in read_rows(path):
            table = lambda: ".".join(p for p in (pick(r, "table_catalog", "catalog", "database"), pick(r, "table_schema", "schema"), pick(r, "table_name", "table")) if p).lower()
            if kind.startswith("columns") and table() and pick(r, "column_name", "column", "name"):
                ix.dataset(table(), "warehouse")
                ix.column(table(), pick(r, "column_name", "column", "name"), pick(r, "data_type", "type") or None, pick(r, "comment", "description") or None)
            elif kind.startswith("tables") and table():
                n = pick(r, "row_count", "rows")
                ix.dataset(table(), "warehouse", rows=int(float(n)) if re.fullmatch(r"[\d.]+", n) else None, description=pick(r, "comment", "description"))
            elif kind.startswith("edges"):
                s, t = pick(r, "source", "source_table_full_name", "src").lower(), pick(r, "target", "target_table_full_name", "dst").lower()
                if s and t:
                    ix.dataset(s)
                    ix.dataset(t, "warehouse")
                    ix.edge(s, t, "import warehouse lineage", "declared")
                    sc, tc = pick(r, "source_column", "source_column_name"), pick(r, "target_column", "target_column_name")
                    if sc and tc:
                        ix.col_edges.add((f"{s}.{sc.lower()}", f"{t}.{tc.lower()}"))
            elif kind.startswith("queries"):
                n = pick(r, "calls", "count", "executions")
                stmts += extract_sql(pick(r, "query_text", "query", "statement_text", "sql"), None, calls=int(n) if n.isdigit() else 1)
    return stmts


def load_curated():
    import yaml
    curated, notes, glossary = {}, [], []
    for p in sorted((HOME / "notes").glob("*.y*ml")):
        doc = yaml.safe_load(p.read_text()) or {}
        name = str(doc.get("dataset") or p.stem).lower()
        curated[name] = {k: ", ".join(map(str, v)) if isinstance(v, list) else str(v)
                         for k, v in doc.items() if k in ("description", "grain", "key", "owner", "verified", "freshness", "tier") and v not in (None, "")}
        for n in doc.get("notes") or []:
            notes.append([name, str(n.get("column") or "").lower(), str(n.get("date") or ""), str(n.get("kind") or "note"),
                          str(n.get("text") or ""), str(n.get("source") or "")])
    gp = HOME / "glossary.yml"
    for g in (yaml.safe_load(gp.read_text()) or []) if gp.exists() else []:
        glossary.append([str(g.get("term", "")), str(g.get("meaning", "")), str(g.get("where", "")).lower()])
    return curated, notes, glossary


def roots(repos):
    """(folder, path prefix) for this repo and each extra repo in config.yml, so a DAG repo and its job repo index together."""
    return [(ROOT, "")] + [(r, os.path.relpath(r, ROOT.resolve()) + "/") for r in ((ROOT / x).resolve() for x in repos) if r.is_dir()]


def repo_files(exclude, repos=()):
    files = []
    for root, prefix in roots(repos):
        out = run(["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard"])
        if out:  # a sparse checkout lists files that aren't on disk
            names = [f for f in out.splitlines() if (root / f).is_file()]
        else:
            names = []
            for d, dirs, fs in os.walk(root):
                dirs[:] = [x for x in dirs if x not in SKIP]
                names += [str(Path(d, f).relative_to(root)) for f in fs]
        # hidden folders hold tooling (.claude/skills, .github, .venv), not pipelines
        files += [prefix + f for f in names if not SKIP.intersection(Path(f).parts) and not any(p.startswith(".") for p in Path(f).parts[:-1])
                  and not any(fnmatch(prefix + f, x) for x in exclude)]
    return files


def export_files():
    """Files in .data-context/imports/ subfolders, for connectors: BI exports (a Superset ZIP is unpacked next to itself)."""
    import zipfile
    imports = HOME / "imports"
    for z in imports.glob("*.zip"):
        dest = imports / z.stem
        if not dest.exists() or dest.stat().st_mtime < z.stat().st_mtime:
            import shutil
            shutil.rmtree(dest, ignore_errors=True)  # a fresh export replaces the old one: deleted dashboards go too
            with zipfile.ZipFile(z) as zf:
                for m in zf.infolist():  # never outside dest, whatever the archive says
                    if not m.is_dir() and (target := (dest / m.filename).resolve()).is_relative_to(dest.resolve()):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(zf.read(m))
            dest.touch()
    return [str(f.relative_to(ROOT)) for f in sorted(imports.glob("*/**/*")) if f.is_file()]


