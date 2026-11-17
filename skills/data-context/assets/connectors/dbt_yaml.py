"""datawright connector: dbt schema files (models/**/*.yml, version 2), for repos that don't commit a compiled manifest.

Reads model, source, seed and snapshot descriptions and their columns (name, data_type, description). Lineage still comes
from the models' SQL, which datawright reads with ref() and source() resolved; with target/manifest.json present, datawright reads
that instead and this adds little. Copy to .data-context/connectors/.
"""
import re


def match(path):
    return path.endswith((".yml", ".yaml")) and not re.search(r"(^|/)(dbt_project|profiles|packages|dependencies|selectors)\.ya?ml$", path)


def extract(path, text):
    import yaml
    for doc in yaml.safe_load_all(text):  # files may hold several documents
        if isinstance(doc, dict):
            yield from document(doc)


def document(doc):
    for kind in ("models", "seeds", "snapshots"):
        for m in doc.get(kind) or []:
            if isinstance(m, dict) and m.get("name"):
                yield from table(m["name"], m, kind[:-1])
    for src in doc.get("sources") or []:  # source('src', 'tbl') reads as src.tbl
        for tbl in (src.get("tables") or []) if isinstance(src, dict) else []:
            if isinstance(tbl, dict) and tbl.get("name"):
                yield from table(f"{src['name']}.{tbl['name']}", tbl, "dbt source")


def table(name, spec, kind):
    yield {"dataset": name, "description": spec.get("description"), "kind": kind}
    for c in spec.get("columns") or []:
        if isinstance(c, dict) and c.get("name"):
            yield {"column": [name, c["name"], c.get("data_type"), c.get("description")]}
