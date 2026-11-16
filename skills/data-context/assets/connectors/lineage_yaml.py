"""dctx connector: lineage written by hand in YAML or JSON, the files teams keep for OpenMetadata, DataHub or their own docs.

Finds every list of edges, at any depth, written as from/to, source/target or upstream/downstream. Each side is a name
or an object with fqn, name or id (and optionally type, which becomes the dataset kind). Example:

    edges:
      - from: {type: table, fqn: warehouse.ods.orders}
        to: {type: apiEndpoint, fqn: api.orders.list}

Copy to .data-context/connectors/ and adjust the settings below.
"""
import json
import re

FILES = r"lineage|\.lineage\.|edges"   # which .yaml/.yml/.json paths hold lineage (a regex on the path)
DROP_PARTS = 0                          # leading name parts to drop, e.g. 1 for OpenMetadata's service prefix
TYPE_PREFIX = {"topic": "kafka:"}       # name prefixes by type, so these line up with what dctx finds in code
RENAME = []                             # (regex, replacement) pairs for your naming, e.g. [(r"\$\{env\}_?", "")]
PAIRS = (("from", "to"), ("source", "target"), ("upstream", "downstream"))


def match(path):
    return path.endswith((".yaml", ".yml", ".json")) and re.search(FILES, path, re.I) is not None


def extract(path, text):
    if path.endswith(".json"):
        doc = json.loads(text)
    else:
        import yaml
        doc = list(yaml.safe_load_all(text))  # files may hold several documents
    for item in walk(doc):
        for a, b in PAIRS:
            if a in item and b in item and (src := name(item[a])) and (dst := name(item[b])):
                kind = item[b].get("type") if isinstance(item[b], dict) else None
                yield {"edge": [src, dst], "kind": kind or "lineage yaml"}
                break


def walk(x):
    if isinstance(x, dict):
        yield x
        for v in x.values():
            yield from walk(v)
    elif isinstance(x, list):
        for v in x:
            yield from walk(v)


def name(side):
    n = side if isinstance(side, str) else next((side.get(k) for k in ("fqn", "fullyQualifiedName", "name", "id") if side.get(k)), None) \
        if isinstance(side, dict) else None
    if not n:
        return None
    n = ".".join(str(n).split(".")[DROP_PARTS:])
    for rx, to in RENAME:
        n = re.sub(rx, to, n)
    kind = side.get("type") if isinstance(side, dict) else None
    return TYPE_PREFIX.get(kind, "") + n
