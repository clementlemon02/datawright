"""dctx connector: MyBatis mapper XML, the SQL behind Java services (<select>, <insert>, <update>, <delete>).

Dynamic SQL is rendered to plain SQL: every <if> is included, <where>/<set>/<trim> add their keyword and drop the leading
AND/OR or trailing comma, <foreach> keeps one item, <include> pulls in its <sql> fragment, #{param} becomes a value and
${param} an unknown. A statement with <choose> is rendered once per branch, so every table any branch reads is seen.
"""
import re
import xml.etree.ElementTree as ET

FILES = r"mapper"  # which .xml paths hold mappers (a regex on the path)
STATEMENTS = ("select", "insert", "update", "delete")


def match(path):
    return path.endswith(".xml") and re.search(FILES, path, re.I) is not None


def extract(path, text):
    try:
        root = ET.fromstring(re.sub(r"<!DOCTYPE[^>]*>", "", text))
    except ET.ParseError:
        return
    if root.tag != "mapper":
        return
    fragments = {e.get("id"): e for e in root.iter("sql") if e.get("id")}
    for el in root:
        if el.tag not in STATEMENTS:
            continue
        line = text.count("\n", 0, max(text.find(f'id="{el.get("id")}"'), 0)) + 1
        branches = max([len([c for c in ch if c.tag in ("when", "otherwise")]) for ch in el.iter("choose")] or [1])
        seen = set()
        for pick in range(min(branches, 12)):
            sql = re.sub(r"--[^\n]*", "", render(el, fragments, pick))  # before joining lines, or a comment eats the rest
            sql = re.sub(r"\s+", " ", sql).strip()
            sql = re.sub(r"#\{[^}]*\}", "1", sql)
            sql = re.sub(r"\$\{[^}]*\}", "jinja_expr", sql)
            if sql and sql not in seen:
                seen.add(sql)
                yield {"sql": sql, "line": line}


def render(el, fragments, pick, depth=0):
    """The SQL text an element stands for, with one branch of each <choose> (the pick-th, or its last)."""
    if depth > 20:
        return ""
    parts = [el.text or ""]
    for ch in el:
        inner = lambda e=ch: render(e, fragments, pick, depth + 1)
        if ch.tag == "include":
            ref = (ch.get("refid") or "").rsplit(".", 1)[-1]
            parts.append(render(fragments[ref], fragments, pick, depth + 1) if ref in fragments else "")
        elif ch.tag == "where":
            body = re.sub(r"^\s*(AND|OR)\b", "", inner(), flags=re.I)
            parts.append(f" WHERE {body} " if body.strip() else " ")
        elif ch.tag == "set":
            parts.append(" SET " + re.sub(r",\s*$", "", inner().rstrip()) + " ")
        elif ch.tag == "trim":
            body = inner()
            for o in (ch.get("prefixOverrides") or "").split("|"):
                if o.strip() and body.lstrip().upper().startswith(o.strip().upper()):
                    body = body.lstrip()[len(o.strip()):]
            for o in (ch.get("suffixOverrides") or "").split("|"):
                if o.strip() and body.rstrip().upper().endswith(o.strip().upper()):
                    body = body.rstrip()[: -len(o.strip())]
            parts.append(f" {ch.get('prefix') or ''} {body} {ch.get('suffix') or ''} ")
        elif ch.tag == "foreach":
            parts.append(f" {ch.get('open') or ''}{inner()}{ch.get('close') or ''} ")
        elif ch.tag == "choose":
            options = [c for c in ch if c.tag in ("when", "otherwise")]
            if options:
                parts.append(" " + render(options[min(pick, len(options) - 1)], fragments, pick, depth + 1) + " ")
        elif ch.tag in ("if", "when", "otherwise"):
            parts.append(" " + inner() + " ")
        elif ch.tag != "bind":
            parts.append(inner())
        parts.append(ch.tail or "")
    return "".join(parts)
