def concepts_of(joins, identity, keys, confirmed):
    """Columns that hold the same thing, from how code uses them: joined to each other, or passed through under another name.
    joins: {(left "ds.col", right "ds.col"): statements}; identity: {(src, dst)}; keys: {"ds.col"}; confirmed: [{name, columns, home}]
    from concepts.yml. Returns [(concept, dataset, column, role, confirmed, evidence)], role home, alias or proposed."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    degree, evidence = Counter(), Counter()
    for (a, b), n in joins.items():
        parent[find(a)] = find(b)
        degree[a] += n
        degree[b] += n
    grouped, more = set(parent), True
    while more:  # renames extend what joins established (player_id passed on as uid); a column copied through layers alone isn't an entity
        more = False
        for a, b in identity:
            if (a in grouped) != (b in grouped) or (a in grouped and find(a) != find(b)):
                parent[find(a)] = find(b)
                more = more or not {a, b} <= grouped
                grouped |= {a, b}
    identity = {(a, b) for a, b in identity if a in grouped}
    groups = {}
    for x in list(parent):
        groups.setdefault(find(x), set()).add(x)
    for (a, b), n in joins.items():
        evidence[find(a)] += n
    for a, b in identity:
        evidence[find(a)] += 1
    col = lambda x: x.rsplit(".", 1)[1]
    owned = {c: k for k in confirmed for c in k.get("columns") or []}
    rows, taken = [], {k["name"] for k in confirmed}
    for k in confirmed:  # a person's word wins: kept as written
        for c in k.get("columns") or []:
            if "." in c:
                rows.append((k["name"], *c.rsplit(".", 1), "home" if c == k.get("home") else "alias", 1, 0))
    for root, members in sorted(groups.items(), key=lambda g: -evidence[g[0]]):
        if len(members) < 2:
            continue
        mine = next((owned[m] for m in members if m in owned), None)
        if mine:  # columns newly joined to a confirmed concept: proposed for it
            rows += [(mine["name"], *m.rsplit(".", 1), "proposed", 0, evidence[root]) for m in sorted(members - set(owned))]
            continue
        names = Counter(col(m) for m in sorted(members) if col(m) != "id")
        generic = {"code", "id", "key", "uuid", "name", "ref", "value", "type"}
        canonical = max(names, key=lambda c: (names[c], c not in generic, c.endswith("_id"), len(c), c)) if names else None  # player_id over playerid
        name = re.sub(r"(?:_id|_uid|_uuid|_key|_code|_no)$|(?<=\w{3})(?<!u)id$", "", canonical or "") or canonical
        named_after = lambda m: bool(name) and name in m.rsplit(".", 1)[0].rsplit(".", 1)[-1]  # customers holds customer
        home = max(members, key=lambda m: (m in keys, named_after(m), col(m) == "id", degree[m], m))
        if (canonical and name == canonical) or (names and len(names) > 1 and max(names.values()) == 1):  # user_ref says little: its home's name
            table = home.rsplit(".", 1)[0].rsplit(".", 1)[-1]
            name = re.sub(r"(_accounts?|_master|_dim|_table|s)$", "", re.sub(r"^(dim|tbl|t)_", "", table)) or table
        canonical = canonical or col(home)
        if not name or name == "id":
            name = home.rsplit(".", 1)[0].rsplit(".", 1)[-1]
        base, i = name, 2
        while name in taken:
            name, i = f"{base}_{i}", i + 1
        taken.add(name)
        rows += [(name, *m.rsplit(".", 1), "home" if m == home else "alias", 0, evidence[root]) for m in sorted(members)]
    return rows


DBT_REF = re.compile(r"(?<![\w.])ref\(\s*(?:['\"][^'\"]+['\"]\s*,\s*)?['\"]([^'\"]+)['\"]")
DBT_SOURCE = re.compile(r"(?<![\w.])source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)")


def dbt_macros(files, projects):
    """What each dbt macro reads, following the macros it calls: {project: {macro: {dataset names}}}. A model that calls
    the macro reads them too, as dbt counts it. ponytail: refs built from the macro's arguments (ref(model_name)) aren't known."""
    out = {}
    for proj in projects:
        body = {}
        for f in files:
            if Path(proj) in Path(f).parents and "macros" in Path(f).relative_to(proj).parts and f.endswith(".sql"):
                text = re.sub(r"\{#.*?#\}", "", (ROOT / f).read_text(encoding="utf-8", errors="replace"), flags=re.S)
                for m in re.finditer(r"\{%-?\s*macro\s+(\w+)\s*\((.*?)\{%-?\s*endmacro", text, re.S):
                    body[m[1]] = m[2]
        reads = {n: {r.lower() for r in DBT_REF.findall(b)} | {f"{a}.{c}".lower() for a, c in DBT_SOURCE.findall(b)} for n, b in body.items()}
        calls = {n: {c for c in re.findall(r"(?<![\w.])(\w+)\s*\(", b) if c in body and c != n} for n, b in body.items()}
        for _ in range(4):  # macros calling macros
            for n in body:
                for c in calls[n]:
                    reads[n] |= reads[c]
        out[proj] = {n: r for n, r in reads.items() if r}
    return out


def git_changes(repos=()):
    latest = {}
    for root, prefix in roots(repos):
        cur = None
        for line in run(["git", "-C", str(root), "log", "-n", "5000", "--date=short", "--format=@@%h|%ad|%s", "--name-only"]).splitlines():
            if line.startswith("@@"):
                cur = line[2:].split("|", 2)
            elif line.strip() and cur and prefix + line not in latest:
                latest[prefix + line] = cur
        for line in run(["git", "-C", str(root), "status", "--porcelain"]).splitlines():
            latest[prefix + line[3:].strip().strip('"')] = ["working", TODAY, "uncommitted changes"]
    return latest


def resolver(names, explicit):
    """Map partly qualified names (orders, stg.orders) onto the unique fully qualified one."""
    alias, suffix = dict(explicit), {}
    for n in names:
        parts = [] if ":" in n else n.split(".")  # namespaced names (kafka:a.b.c, looker:view.orders) are not table b.c
        for k in range(1, len(parts)):
            suffix.setdefault(".".join(parts[-k:]), set()).add(n)

    def final(n):
        for _ in range(5):
            if alias.get(n, n) == n:
                break
            n = alias[n]
        return n
    for n in sorted(names, key=lambda x: -x.count(".")):  # longest first, so raw.orders resolves before orders
        if n not in alias and len(cands := {final(c) for c in suffix.get(n, set()) - {n}}) == 1:
            alias[n] = cands.pop()
    return final


