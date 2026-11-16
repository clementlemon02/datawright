"""dctx connector: Dagster assets, their dependencies and the schedules that materialize them.

Reads @asset / @dg.asset functions (key from name, key or key_prefix), their upstream from deps=[...], ins={...: AssetIn(...)}
and function parameters, AssetSpec(key, deps=...) declarations, and define_asset_job + ScheduleDefinition schedules.
Asset keys become dataset names joined with dots (["ANALYTICS", "orders"] -> analytics.orders), which line up with
warehouse tables named schema.table. dbt models under @dbt_assets need nothing extra: dctx reads their SQL. Static only:
assets built in loops or factories from runtime config are missed. Copy to .data-context/connectors/.
"""
import ast

ROOT = None
SKIP_PARAMS = {"context", "config", "self"}
_repo_jobs = None  # job variable -> (name, selected keys), across the repo: schedules often live apart from their jobs


def match(path):
    return path.endswith(".py")


def extract(path, text):
    if "dagster" not in text:
        return
    tree = ast.parse(text)
    keys = {}  # asset function name -> its key, for deps that name a function
    assets = []
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            dec = next((d for d in n.decorator_list if callee(d) in ("asset", "multi_asset")), None)
            if dec is not None:
                kw = {k.arg: k.value for k in dec.keywords if k.arg} if isinstance(dec, ast.Call) else {}
                key = key_of(kw.get("key")) or ".".join(strings(kw.get("key_prefix")) + [lit(kw.get("name")) or n.name])
                keys[n.name] = key
                assets.append((n, kw, key))
    for n, kw, key in assets:
        yield {"produces": key, "line": n.lineno, "kind": "dagster asset"}
        ups = [dep(d, keys) for d in (kw.get("deps").elts if isinstance(kw.get("deps"), (ast.List, ast.Tuple)) else [])]
        ins = kw.get("ins")
        named = {}
        if isinstance(ins, ast.Dict):
            for k, v in zip(ins.keys, ins.values):
                if isinstance(k, ast.Constant) and isinstance(v, ast.Call):
                    a = {x.arg: x.value for x in v.keywords if x.arg}
                    named[k.value] = key_of(a.get("key")) or ".".join(strings(a.get("key_prefix")) + [k.value])
        params = [a.arg for a in n.args.args if a.arg not in SKIP_PARAMS]
        ups += [named.get(p) or keys.get(p, p) for p in params]  # a parameter is an upstream asset unless it's a resource or config
        for u in filter(None, ups):
            yield {"edge": [u, key], "kind": "dagster asset"}
    for n in ast.walk(tree):  # AssetSpec("key", deps=[...]): assets defined by other systems, with their lineage
        if isinstance(n, ast.Call) and callee(n) == "AssetSpec":
            kw = {k.arg: k.value for k in n.keywords if k.arg}
            key = key_of(n.args[0] if n.args else kw.get("key"))
            if key:
                yield {"dataset": key, "kind": "dagster asset", "description": lit(kw.get("description"))}
                for d in kw.get("deps").elts if isinstance(kw.get("deps"), (ast.List, ast.Tuple)) else []:
                    if u := dep(d, keys):
                        yield {"edge": [u, key], "kind": "dagster asset"}
    jobs = file_jobs(tree, keys)
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and callee(n) == "ScheduleDefinition":
            kw = {k.arg: k.value for k in n.keywords if k.arg}
            name = kw["job"].id if isinstance(kw.get("job"), ast.Name) else None
            job = jobs.get(name) or repo_jobs().get(name) if name else None
            if job and lit(kw.get("cron_schedule")):
                for key in job[1]:
                    yield {"run": {"dag": job[0], "task": key, "schedule": lit(kw["cron_schedule"]), "dataset": key}, "line": n.lineno}


def file_jobs(tree, keys):
    """define_asset_job assignments in one module: variable -> (job name, selected asset keys)."""
    jobs = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and callee(n.value) == "define_asset_job":
            kw = {k.arg: k.value for k in n.value.keywords if k.arg}
            name = lit(n.value.args[0]) if n.value.args else lit(kw.get("name"))
            sel = kw.get("selection")
            picked = [dep(x, keys) for x in sel.elts] if isinstance(sel, (ast.List, ast.Tuple)) else [dep(sel, keys)] if sel is not None else []
            for t in n.targets:
                if isinstance(t, ast.Name):
                    jobs[t.id] = (name or t.id, [p for p in picked if p])
    return jobs


def repo_jobs():
    global _repo_jobs
    if _repo_jobs is None:
        _repo_jobs = {}
        for f in ROOT.rglob("*.py") if ROOT else []:
            if "/." not in str(f) and "define_asset_job" in (text := f.read_text(errors="replace")):
                try:
                    _repo_jobs.update(file_jobs(ast.parse(text), {}))
                except SyntaxError:
                    pass
    return _repo_jobs


def callee(n):
    f = n.func if isinstance(n, ast.Call) else n
    return f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""


def lit(n):
    return n.value if isinstance(n, ast.Constant) and isinstance(n.value, str) else None


def strings(n):
    return [lit(n)] if lit(n) else [x.value for x in n.elts if isinstance(x, ast.Constant)] if isinstance(n, (ast.List, ast.Tuple)) else []


def key_of(n):
    """AssetKey(["a", "b"]), AssetKey("a"), ["a", "b"] or "a/b" as a dotted name."""
    if isinstance(n, ast.Call) and callee(n) == "AssetKey" and n.args:
        n = n.args[0]
    parts = strings(n) if isinstance(n, (ast.List, ast.Tuple)) else (lit(n) or "").split("/") if lit(n) else []
    return ".".join(parts).lower() or None


def dep(n, keys):
    """A deps or selection entry: a key, an asset function's name, or another asset's function."""
    if isinstance(n, ast.Name):
        return keys.get(n.id, n.id)
    return key_of(n)
