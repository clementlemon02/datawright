# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.1"]
# ///
"""Do agents write the right SQL on a warehouse whose keys and codes drifted, with and without datawright?

uv run run.py [--model sonnet] [--reps 1] [--tasks 1,2] [--jobs 4]
Each run copies a fresh repo (make_repo.py), asks Claude Code (headless, project settings only, no user plugins) one
question, and runs the answer.sql it writes against the seeded warehouse; rows are compared with the reference answer.
Conditions: baseline (the repo as is) and datawright (the data-context skill installed and its index built, as setup leaves it).
Results go to results/<timestamp>.jsonl and a summary is printed. Costs real API money: about $0.3 to $1 a run."""
import argparse
import concurrent.futures
import datetime
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
KIT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import make_hard  # noqa: E402
import make_repo  # noqa: E402

TASKS = [  # (question, reference SQL): every one needs a renamed key, a coded value, or both
    ("What is the total approved deposit amount in US dollars for each player? Return name, deposit_usd.",
     "select p.name, sum(r.amount_cents) / 100 from fin.requests r join crm.player_accounts p on p.id = r.user_ref "
     "where r.type = 1 and r.status = 2 group by p.name"),
    ("How many sessions did each agent's players have? Return agent_name, sessions.",
     "select a.agent_name, count(*) from app.sessions s join crm.player_accounts p on p.id = cast(s.playerid as bigint) "
     "join crm.agents a on a.code = p.agent_code group by a.agent_name"),
    ("What is the total stake in US dollars for each game? Return title, stake_usd.",
     "select g.title, sum(b.stake_cents) / 100 from game.bets b join game.games g on g.code = b.game_code group by g.title"),
    ("How many withdrawal requests were rejected, per player country? Return country, rejected_withdrawals.",
     "select p.country, count(*) from fin.requests r join crm.player_accounts p on p.id = r.user_ref "
     "where r.type = 2 and r.status = 3 group by p.country"),
    ("How many distinct players both had at least one session and placed at least one bet? Return players.",
     "select count(*) from (select cast(playerid as bigint) from app.sessions intersect select member_ref from game.bets)"),
]
PROMPT = ("You're working in a data engineering repo for an online gaming platform. The warehouse is DuckDB: run read-only SQL "
          'with `python3 sql.py "<sql>"` (prints CSV). Answer the question below with one DuckDB SQL query. Save that query, and '
          "nothing else, to answer.sql in the repo root, returning exactly the columns asked for, in that order.\n\nQuestion: {q}")
NO_DATA = ("You're working in a data engineering repo for an online gaming platform, without access to the warehouse (DuckDB): "
           "work from the repo's code. Answer the question below with one DuckDB SQL query. Save that query, and nothing else, "
           "to answer.sql in the repo root, returning exactly the columns asked for, in that order.\n\nQuestion: {q}")


def setup(work, condition, n_noise=0, data=True, suite="basic"):
    (make_hard if suite == "hard" else make_repo).build(work, n_noise)
    subprocess.run(["git", "init", "-q"], cwd=work, check=True)
    if condition == "datawright":  # what setup-data-project leaves: the skill, AGENTS.md's data-context section, a built index
        shutil.copytree(KIT / "skills/data-context", work / ".claude/skills/data-context")
        (work / ".data-context").mkdir()
        shutil.copy(KIT / "skills/data-context/scripts/datawright.py", work / ".data-context/datawright.py")
        (work / ".data-context/config.yml").write_text("dialect: duckdb\n" + ("query_command: python3 sql.py {sql}\n" if data else ""))
        template = (KIT / "skills/setup-data-project/assets/AGENTS.md").read_text()
        section = template[template.index("## Data context"):].split("\n## ")[0]
        (work / "CLAUDE.md").write_text(re_strip(section) + "\n")
        subprocess.run(["uv", "run", "-q", ".data-context/datawright.py", "build"], cwd=work, check=True, capture_output=True)


def re_strip(text):  # template placeholders (<Only if …>) out
    import re
    return re.sub(r"\s*<Only[^>]*>", "", text)


def rows(con, sql):
    norm = lambda v: round(float(v), 2) if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v).strip().lower()
    return sorted(tuple(norm(v) for v in r) for r in con.execute(sql).fetchall())


def one(task, condition, rep, model, n_noise=0, data=True, suite="basic"):
    q, ref = (make_hard.TASKS if suite == "hard" else TASKS)[task - 1]
    work = Path(tempfile.mkdtemp(prefix=f"meaning-{condition}-{task}-")) / "repo"
    work.mkdir()
    setup(work, condition, n_noise, data, suite)
    started = time.time()
    cmd = ["claude", "-p", (PROMPT if data else NO_DATA).format(q=q), "--model", model, "--output-format", "stream-json", "--verbose", "--max-turns", "30",
           "--setting-sources", "project", "--strict-mcp-config",  # no user plugins, hooks or MCP servers
           "--allowedTools", "Read", "Grep", "Glob", "Write", "Edit", "Skill",  # nothing else: unlisted tools are denied headless
           "Bash(uv run .data-context/datawright.py:*)", "Bash(uv run -q .data-context/datawright.py:*)"] + (["Bash(python3 sql.py:*)"] if data else [])
    try:
        out = subprocess.run(cmd, cwd=work, capture_output=True, text=True, timeout=900).stdout
    except subprocess.TimeoutExpired:
        out = ""
    events = [json.loads(l) for l in out.splitlines() if l.startswith("{")]
    result = next((e for e in events if e.get("type") == "result"), {})
    calls = [b for e in events if e.get("type") == "assistant" for b in e["message"].get("content", []) if b.get("type") == "tool_use"]
    datawright_calls = sum("datawright.py" in json.dumps(b.get("input")) for b in calls)
    answer = (work / "answer.sql").read_text() if (work / "answer.sql").exists() else ""
    con = duckdb.connect(str(work / "warehouse.duckdb"), read_only=True)
    try:
        got, error = rows(con, answer), None
    except Exception as e:
        got, error = None, str(e).splitlines()[0][:160] if answer else "no answer.sql"
    correct = got is not None and got == rows(con, ref)
    con.close()
    return {"task": task, "condition": condition, "rep": rep, "model": model, "noise": n_noise, "data": data, "suite": suite, "correct": correct, "error": error,
            "cost": result.get("total_cost_usd"), "turns": result.get("num_turns"), "seconds": round(time.time() - started),
            "tool_calls": len(calls), "datawright_calls": datawright_calls, "answer": answer.strip()[:2000], "workdir": str(work)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="sonnet")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--suite", default="basic", choices=("basic", "hard"), help="hard: multi-hop chains, piecemeal codes, a stale trap column")
    ap.add_argument("--tasks", help="task numbers, e.g. 1,3 (default: all in the suite)")
    ap.add_argument("--conditions", default="baseline,datawright")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--noise", type=int, default=0, help="extra pipeline files, with decoys: a big repo")
    ap.add_argument("--no-data", action="store_true", help="agents can't query the warehouse: they work from the code")
    a = ap.parse_args()
    n_tasks = len(make_hard.TASKS if a.suite == "hard" else TASKS)
    picked = list(map(int, a.tasks.split(","))) if a.tasks else list(range(1, n_tasks + 1))
    runs = [(t, c, r) for r in range(1, a.reps + 1) for t in picked for c in a.conditions.split(",")]
    out = HERE / "results" / f"{datetime.datetime.now():%Y%m%d-%H%M%S}-{a.suite}-{a.model}.jsonl"
    out.parent.mkdir(exist_ok=True)
    results = []
    with concurrent.futures.ThreadPoolExecutor(a.jobs) as pool:
        for r in pool.map(lambda x: one(*x, a.model, a.noise, not a.no_data, a.suite), runs):
            results.append(r)
            with out.open("a") as f:
                f.write(json.dumps(r) + "\n")
            print(f"task {r['task']} {r['condition']:8} rep {r['rep']}: {'RIGHT' if r['correct'] else 'wrong'}  ${r['cost'] or 0:.2f}  "
                  f"{r['turns']} turns  {r['seconds']}s  datawright calls {r['datawright_calls']}" + (f"  ({r['error']})" if r["error"] else ""), flush=True)
    print(f"\n{'condition':10} {'right':>7} {'cost':>8} {'turns':>6} {'seconds':>8}")
    for c in a.conditions.split(","):
        rs = [r for r in results if r["condition"] == c]
        avg = lambda k: sum(r[k] or 0 for r in rs) / len(rs)
        print(f"{c:10} {sum(r['correct'] for r in rs):>3}/{len(rs):<3} ${avg('cost'):>7.2f} {avg('turns'):>6.1f} {avg('seconds'):>8.0f}")
    print(f"results: {out}")


if __name__ == "__main__":
    main()
