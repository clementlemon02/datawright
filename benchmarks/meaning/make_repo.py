# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.1"]
# ///
"""A small gaming-platform warehouse with the drift real teams have: one player key under four names (id, user_ref,
playerid as text, member_ref), agents keyed by code, and request types and statuses explained only by the report SQL.
uv run make_repo.py <dir> builds the repo, its SQL and warehouse.duckdb (seeded, so answers are stable)."""
import random
import sys
from pathlib import Path

import duckdb

FILES = {
    "ddl/warehouse.sql": """create table crm.player_accounts (id bigint, name varchar, country varchar, agent_code varchar, ref bigint);
create table crm.agents (id int, code varchar, agent_name varchar);
create table fin.requests (req_id bigint, user_ref bigint, type int, status int, amount_cents bigint, created_ms bigint);
create table app.sessions (session_id bigint, playerid varchar, started_at timestamp);
create table game.bets (bet_id bigint, member_ref bigint, game_code varchar, stake_cents bigint);
create table game.games (id int, code varchar, title varchar);
""",
    "reports/finance_daily.sql": """-- Finance dashboard: approved money in and out per country, in dollars.
insert overwrite table rep.finance_daily
select p.country,
       sum(case when r.type = 1 then r.amount_cents end) / 100 as deposit_usd,
       sum(case when r.type = 2 then r.amount_cents end) / 100 as withdrawal_usd
from fin.requests r
join crm.player_accounts p on p.id = r.user_ref
where r.status = 2
group by p.country;
""",
    "reports/request_status.sql": """-- Support view of every payment request.
insert overwrite table rep.request_status
select r.req_id,
       case r.status when 1 then 'pending' when 2 then 'approved' when 3 then 'rejected' end as status_label,
       from_unixtime(r.created_ms / 1000) as created_at
from fin.requests r;
""",
    "pipelines/sessions_by_country.sql": """insert overwrite table rep.sessions_by_country
select p.country, count(*) as sessions
from app.sessions s
join crm.player_accounts p on p.id = cast(s.playerid as bigint)
group by p.country;
""",
    "pipelines/agent_players.sql": """insert overwrite table rep.agent_players
select a.agent_name, count(*) as players
from crm.player_accounts p
join crm.agents a on a.code = p.agent_code
group by a.agent_name;
""",
    "pipelines/bets_by_player.sql": """insert overwrite table rep.bets_by_player
select p.name, sum(b.stake_cents) / 100 as stake_usd
from game.bets b
join crm.player_accounts p on p.id = b.member_ref
group by p.name;
""",
    "pipelines/bets_by_game.sql": """insert overwrite table rep.bets_by_game
select g.title, count(*) as bets
from game.bets b
join game.games g on g.code = b.game_code
group by g.title;
""",
    "reports/referrals.sql": """-- Who referred whom.
select p.name, r.name as referred_by
from crm.player_accounts p
join crm.player_accounts r on r.id = p.ref;
""",
    "sql.py": '''"""Run one read-only query on warehouse.duckdb and print CSV: python3 sql.py "select ..." """
import csv, sys
import duckdb
cur = duckdb.connect("warehouse.duckdb", read_only=True).execute(sys.argv[1])
out = csv.writer(sys.stdout)
out.writerow([d[0] for d in cur.description])
out.writerows(cur.fetchmany(200))
''',
    "README.md": "# Gaming platform warehouse\n\nSQL for the platform's reports and pipelines. The warehouse is `warehouse.duckdb`.\n",
}


DECOYS = {  # the same names meaning other things, as in any real warehouse
    "ddl/support.sql": """create table ops.tickets (ticket_id bigint, user_ref bigint, status int, type int);
create table crm.staff_users (id bigint, name varchar);
create table fin.requests_v1 (req_id bigint, user_ref bigint, type int, status int, amount double);
""",
    "support/tickets.sql": """-- Support queue: user_ref here is the staff member handling the ticket.
insert overwrite table rep.ticket_queue
select t.ticket_id, s.name as handler,
       case t.status when 1 then 'open' when 2 then 'closed' when 3 then 'escalated' end as status_label,
       case t.type when 1 then 'deposit issue' when 2 then 'login issue' end as topic
from ops.tickets t
join crm.staff_users s on s.id = t.user_ref;
""",
    "legacy/finance_v1.sql": """-- Old finance report on the v1 requests table (amounts in dollars there).
insert overwrite table rep.finance_v1
select case r.type when 1 then 'withdrawal' when 2 then 'deposit' end as kind,
       case r.status when 1 then 'approved' when 2 then 'pending' when 3 then 'rejected' end as state,
       sum(r.amount) as amount
from fin.requests_v1 r
join crm.staff_users s on s.id = r.user_ref
group by 1, 2;
""",
}


def noise(n, rnd):
    """n pipeline files like a big repo's: their own tables, keys named user_ref, member_ref, playerid for other entities,
    and their own type and status codes."""
    labels = [["new", "active", "closed"], ["draft", "live", "archived"], ["ok", "warn", "fail"], ["queued", "sent", "bounced"]]
    out = {}
    for i in range(n):
        dom = rnd.choice(["mkt", "ops", "risk", "cs", "aff"])
        key = rnd.choice(["user_ref", "member_ref", "playerid", "owner_id", "account_ref"])
        ent = rnd.choice(["staff_users", "partners", "devices", "campaigns", "merchants"])
        ls = rnd.choice(labels)
        out[f"pipelines/{dom}/{dom}_job_{i:03d}.sql"] = (
            f"insert overwrite table {dom}.metric_{i:03d}\n"
            f"select x.id, e.name, case x.status when 1 then '{ls[0]}' when 2 then '{ls[1]}' when 3 then '{ls[2]}' end as status_label,\n"
            f"       sum(case when x.type = {rnd.randint(1, 3)} then x.amount end) as {rnd.choice(['deposit', 'refund', 'bonus', 'fee'])}_amount\n"
            f"from {dom}.events_{i % 40:02d} x\njoin {dom}.{ent} e on e.id = x.{key}\ngroup by 1, 2, 3;\n")
    return out


def build(root, n_noise=0):
    root = Path(root)
    rnd_files = random.Random(11)
    files = {**FILES, **(DECOYS if n_noise else {}), **noise(n_noise, rnd_files)}
    for path, text in files.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(text)
    rnd = random.Random(7)
    con = duckdb.connect(str(root / "warehouse.duckdb"))
    for schema in ("crm", "fin", "app", "game"):
        con.execute(f"create schema if not exists {schema}")
    con.execute(FILES["ddl/warehouse.sql"])
    agents = [(1, "AG-N", "North Desk"), (2, "AG-S", "South Desk"), (3, "AG-E", "East Desk")]
    con.executemany("insert into crm.agents values (?, ?, ?)", agents)
    players = [(1000 + i, f"Player {i:02d}", rnd.choice(["PH", "SG", "MY", "TH"]), rnd.choice(agents)[1],
                (1000 + rnd.randint(1, 40)) if rnd.random() < 0.6 else None) for i in range(1, 41)]
    con.executemany("insert into crm.player_accounts values (?, ?, ?, ?, ?)", players)
    ids = [p[0] for p in players]
    con.executemany("insert into fin.requests values (?, ?, ?, ?, ?, ?)",
                    [(i, rnd.choice(ids), rnd.choice([1, 1, 2]), rnd.choice([1, 2, 2, 3]), rnd.randint(10, 5000) * 100, 1_760_000_000_000 + i * 60_000)
                     for i in range(1, 301)])
    con.executemany("insert into app.sessions values (?, ?, ?)",
                    [(i, str(rnd.choice(ids[:30])), f"2026-10-{rnd.randint(1, 28):02d} 10:00:00") for i in range(1, 501)])
    games = [(1, "SLOT-7", "Lucky Sevens"), (2, "PKR-2", "Poker Night"), (3, "RLT-3", "Roulette Royale")]
    con.executemany("insert into game.games values (?, ?, ?)", games)
    con.executemany("insert into game.bets values (?, ?, ?, ?)",
                    [(i, rnd.choice(ids[10:]), rnd.choice(games)[1], rnd.randint(1, 500) * 100) for i in range(1, 801)])
    if n_noise:  # the decoy tables exist, so a wrong join returns rows instead of an error
        con.execute("create schema if not exists ops")
        con.execute(DECOYS["ddl/support.sql"])
        con.executemany("insert into crm.staff_users values (?, ?)", [(1000 + i, f"Staff {i:02d}") for i in range(1, 41)])
        con.executemany("insert into ops.tickets values (?, ?, ?, ?)", [(i, 1000 + rnd.randint(1, 40), rnd.randint(1, 3), rnd.randint(1, 2)) for i in range(1, 201)])
        con.executemany("insert into fin.requests_v1 values (?, ?, ?, ?, ?)",
                        [(i, 1000 + rnd.randint(1, 40), rnd.choice([1, 2]), rnd.choice([1, 2, 3]), rnd.randint(10, 5000)) for i in range(1, 201)])
    con.close()


if __name__ == "__main__":
    build(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 0)
