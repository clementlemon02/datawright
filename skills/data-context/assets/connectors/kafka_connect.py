"""datawright connector: Kafka Connect connector configs (JSON, as posted to the Connect REST API, or .properties).

Source connectors (Debezium CDC: table.include.list with topic.prefix or database.server.name) give table -> topic lineage;
sink connectors (topics or topics.regex, with table.name.format or a database/table option) give topic -> table lineage.
Topics are named kafka:<topic>, like the topics datawright finds in Flink and Spark jobs, so both sides connect.
Copy to .data-context/connectors/ and adjust FILES to where your configs live.
"""
import json
import re

FILES = r"connect|connector|debezium|sink|source"   # a regex on the path of .json / .properties files to read


def match(path):
    return path.endswith((".json", ".properties")) and re.search(FILES, path, re.I) is not None


def extract(path, text):
    cfg = json.loads(text) if path.endswith(".json") else dict(
        (k.strip(), v.strip()) for k, _, v in (ln.partition("=") for ln in text.splitlines() if "=" in ln and not ln.lstrip().startswith("#")))
    cfg = cfg.get("config", cfg)  # the REST API wraps it: {"name": ..., "config": {...}}
    cls = str(cfg.get("connector.class", ""))
    if not cls:
        return
    prefix = cfg.get("topic.prefix") or cfg.get("database.server.name")
    if prefix and cfg.get("table.include.list"):  # Debezium: one topic per captured table, <prefix>.<db>.<table>
        for table in filter(None, (t.strip() for t in cfg["table.include.list"].split(","))):
            yield {"edge": [table, f"kafka:{prefix}.{table}"], "kind": "topic"}
    topics = [t.strip() for t in str(cfg.get("topics", "")).split(",") if t.strip()]
    target = cfg.get("table.name.format") or cfg.get("table") or cfg.get("collection")
    db = cfg.get("database") or cfg.get("database-name")
    for topic in topics:  # sinks: each topic lands in the table its format names (${topic} by default)
        table = (target or "${topic}").replace("${topic}", topic)
        yield {"edge": [f"kafka:{topic}", f"{db}.{table}" if db and "." not in table else table], "kind": "kafka connect sink"}
