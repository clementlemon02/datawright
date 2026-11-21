# ---------------------------------------------------------------- Python pipelines: notebooks, dlt, Prefect

def notebook_code(text):
    """A Jupyter notebook's code cells as one Python file: `%sql` and `%%sql` magics become SQL strings, other magics and shell
    lines are commented out. ponytail: lines count within the joined code cells, not the .ipynb file."""
    try:
        cells = json.loads(text).get("cells", [])
    except ValueError:
        return ""
    out = []
    for c in cells:
        if c.get("cell_type") != "code":
            continue
        src = c.get("source", "")
        lines = (src if isinstance(src, str) else "".join(src)).splitlines()
        if lines and re.match(r"\s*%%sql\b", lines[0]):
            lines = ['_ = """' + re.sub(r"\s*%%sql\b[^\n]*", "", lines[0])] + [l.replace('"""', "'''") for l in lines[1:]] + ['"""']
        else:
            lines = ['_ = """' + m[1].replace('"""', "'''") + '"""' if (m := re.match(r"\s*%sql\s+(.*)", l)) else
                     "#" + l if re.match(r"\s*[%!]", l) else l for l in lines]
        out += lines + [""]
    return "\n".join(out)


def pipeline_decorators(fn):
    """[(decorator name, its Call or None)] of a function: @flow, @flow(name="x"), @prefect.task, @dlt.resource(...)."""
    out = []
    for d in fn.decorator_list:
        call = d if isinstance(d, ast.Call) else None
        target = call.func if call else d
        out.append((target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", ""), call))
    return out


def dlt_tables(ix, tree, path, render):
    """dlt writes {dataset_name}.{table}: the table is a resource's name (or table_name=) and pipeline.run(..., table_name=).
    ponytail: one dataset per file; its sources are external APIs, so no upstream links."""
    ds = None
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "pipeline" and getattr(n.func.value, "id", "") == "dlt":
            ds = next((render(k.value) for k in n.keywords if k.arg == "dataset_name"), None) or ds
    if not ds:
        return
    tables = {}  # table -> line
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for name, call in pipeline_decorators(n):
                if name in ("resource", "transformer"):
                    kw = {k.arg: render(k.value) for k in (call.keywords if call else []) if k.arg}
                    tables.setdefault(kw.get("table_name") or kw.get("name") or n.name, n.lineno)
        elif isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "run":
            t = next((render(k.value) for k in n.keywords if k.arg == "table_name"), None)
            if t:
                tables.setdefault(t, n.lineno)
    for t, line in tables.items():
        name = f"{ds}.{t}".lower()
        if re.fullmatch(r"[\w.]+", name):
            ix.dataset(name, "table", file=path, line=line)
            ix.writers.setdefault((name, path), line)


def prefect_runs(tree, path, render):
    """[(file, first line, last line, flow, task, schedule, file:line)]: each @task function runs inside the file's @flow, on the
    schedule its `.serve(cron=...)` sets. A flow without tasks runs its own lines. ponytail: schedules from prefect.yaml aren't read."""
    flows, tasks, cron = [], [], "manual"
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for name, call in pipeline_decorators(n):
                kw = {k.arg: render(k.value) for k in (call.keywords if call else []) if k.arg}
                if name == "flow":
                    flows.append((kw.get("name") or n.name, n))
                elif name == "task":
                    tasks.append((kw.get("name") or n.name, n))
        elif isinstance(n, ast.Call) and getattr(n.func, "attr", "") in ("serve", "deploy"):
            c = next((k.value for k in n.keywords if k.arg in ("cron", "interval")), None)
            if c is not None:
                cron = render(c) or ast.unparse(c)[:40]
    if not flows:
        return []
    flow = flows[0][0]
    rows = [(path, t.lineno, t.end_lineno, flow, name, cron, f"{path}:{t.lineno}") for name, t in tasks]
    return rows + ([(path, f.lineno, f.end_lineno, name, name, cron, f"{path}:{f.lineno}") for name, f in flows] if not tasks else [])
