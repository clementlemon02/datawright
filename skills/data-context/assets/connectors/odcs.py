"""datawright connector: data contracts in the Open Data Contract Standard (ODCS v3, as written by datacontract-cli), plus the older
Data Contract Specification. Each contract's tables get the contracted columns, types and required flags, a description and an
owner; `datawright review` then flags a change that stops producing a required column, and cards show the contract.
Copy to .data-context/connectors/ and adjust FILES to where your contracts live.
"""
import re

FILES = r"contract|odcs"   # a regex on the path of .yaml/.yml files to read


def match(path):
    return path.endswith((".yaml", ".yml")) and re.search(FILES, path, re.I) is not None


def extract(path, text):
    import yaml
    for doc in yaml.safe_load_all(text):
        if isinstance(doc, dict) and (doc.get("kind") == "DataContract" or "dataContractSpecification" in doc):
            yield from contract(doc)


def contract(doc):
    owner = text_of(doc.get("owner")) or text_of((doc.get("info") or {}).get("owner")) or team(doc.get("team"))
    prefix = qualifier(doc.get("servers"))
    tables = doc.get("schema") if isinstance(doc.get("schema"), list) else [  # ODCS: schema is a list; the older spec keys models by name
        {"name": k, **v} for k, v in (doc.get("models") or {}).items() if isinstance(v, dict)]
    for tbl in tables:
        if not isinstance(tbl, dict) or not (tbl.get("physicalName") or tbl.get("name")):
            continue
        name = ".".join(filter(None, [prefix, tbl.get("physicalName") or tbl.get("name")]))
        yield {"dataset": name, "description": text_of(tbl.get("description")) or text_of(doc.get("description")), "owner": owner}
        props = tbl.get("properties") or [{"name": k, **v} for k, v in (tbl.get("fields") or {}).items() if isinstance(v, dict)]
        for c in props:
            if isinstance(c, dict) and c.get("name"):
                typ = c.get("physicalType") or c.get("logicalType") or c.get("type")
                required = c.get("required") or c.get("primaryKey") or c.get("primary")
                yield {"contract": [name, c["name"], typ, required]}


def text_of(v):  # ODCS v3 descriptions are objects ({purpose, usage, limitations})
    return v if isinstance(v, str) else (v.get("purpose") or v.get("usage")) if isinstance(v, dict) else None


def team(t):
    return t.get("name") if isinstance(t, dict) else None


def qualifier(servers):
    """The schema (or dataset) of the only server, so contract names line up with the warehouse."""
    items = servers if isinstance(servers, list) else list(servers.values()) if isinstance(servers, dict) else []
    if len(items) == 1 and isinstance(items[0], dict):
        return items[0].get("schema") or items[0].get("dataset")
    return None
