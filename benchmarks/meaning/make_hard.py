# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.1"]
# ///
"""The harder suite: no single file answers a question. Join chains of three and four hops with a renamed key at each
hop, every hop written in a different file; request codes decoded only piecemeal across reports; and one trap column
(app.sessions.player_id, still populated but stale: the team's code goes through devices.owner_ref).
"Right" is how the team's own code computes it. uv run make_hard.py <dir> [noise files]"""
import random
import sys
from pathlib import Path

import duckdb

import make_repo

DDL = """create table crm.player_accounts (id bigint, name varchar, country varchar, agent_code varchar, ref bigint);
create table crm.agents (id int, code varchar, agent_name varchar, region_no int);
create table crm.regions (no int, region_name varchar);
create table fin.requests (req_id bigint, user_ref bigint, type int, status int, amount_cents bigint);
create table app.devices (device_uid varchar, owner_ref bigint, platform varchar);
create table app.sessions (session_id bigint, device_uid varchar, player_id bigint, started_at timestamp);
create table game.bets (bet_id bigint, member_ref bigint, round_ref bigint, stake bigint);
create table game.rounds (id bigint, tbl int, started_ms bigint);
create table game.tables (table_no int, g_code varchar, limit_tier int);
create table game.games (id int, code varchar, title varchar, vendor int);
create table game.vendors (vid int, vendor_name varchar);
"""
FILES = {
    "ddl/warehouse.sql": DDL,
    "pipelines/rounds/bets_per_round.sql": "insert overwrite table rep.bets_per_round\nselect r.id, count(*) as bets\nfrom game.bets b\njoin game.rounds r on r.id = b.round_ref\ngroup by r.id;\n",
    "pipelines/tables/rounds_per_table.sql": "insert overwrite table rep.rounds_per_table\nselect t.table_no, count(*) as rounds\nfrom game.rounds r\njoin game.tables t on t.table_no = r.tbl\ngroup by t.table_no;\n",
    "pipelines/catalog/tables_per_game.sql": "insert overwrite table rep.tables_per_game\nselect g.title, count(*) as tables\nfrom game.tables t\njoin game.games g on g.code = t.g_code\ngroup by g.title;\n",
    "pipelines/catalog/games_per_vendor.sql": "insert overwrite table rep.games_per_vendor\nselect v.vendor_name, count(*) as games\nfrom game.games g\njoin game.vendors v on v.vid = g.vendor\ngroup by v.vendor_name;\n",
    "pipelines/players/stake_by_player.sql": "insert overwrite table rep.stake_by_player\nselect p.name, sum(b.stake) / 100 as stake_usd\nfrom game.bets b\njoin crm.player_accounts p on p.id = b.member_ref\ngroup by p.name;\n",
    "pipelines/devices/sessions_per_platform.sql": "-- sessions.player_id is stale since the device migration: always go through the device's owner\ninsert overwrite table rep.sessions_per_platform\nselect d.platform, count(*) as sessions\nfrom app.sessions s\njoin app.devices d on d.device_uid = s.device_uid\ngroup by d.platform;\n",
    "pipelines/devices/devices_per_country.sql": "insert overwrite table rep.devices_per_country\nselect p.country, count(*) as devices\nfrom app.devices d\njoin crm.player_accounts p on p.id = d.owner_ref\ngroup by p.country;\n",
    "pipelines/agents/players_per_agent.sql": "insert overwrite table rep.players_per_agent\nselect a.agent_name, count(*) as players\nfrom crm.player_accounts p\njoin crm.agents a on a.code = p.agent_code\ngroup by a.agent_name;\n",
    "pipelines/agents/agents_per_region.sql": "insert overwrite table rep.agents_per_region\nselect rg.region_name, count(*) as agents\nfrom crm.agents a\njoin crm.regions rg on rg.no = a.region_no\ngroup by rg.region_name;\n",
    "reports/money_in.sql": "insert overwrite table rep.money_in\nselect p.country, sum(case when r.type = 1 then r.amount_cents end) / 100 as deposit_usd\nfrom fin.requests r\njoin crm.player_accounts p on p.id = r.user_ref\ngroup by p.country;\n",
    "reports/money_out.sql": "insert overwrite table rep.money_out\nselect r.user_ref, sum(case when r.type = 2 then r.amount_cents end) / 100 as withdrawal_usd\nfrom fin.requests r\ngroup by r.user_ref;\n",
    "reports/ops/approvals.sql": "insert overwrite table rep.approvals\nselect r.user_ref, count(case when r.status = 2 then 1 end) as approved_requests\nfrom fin.requests r\ngroup by r.user_ref;\n",
    "reports/risk/rejections.sql": "insert overwrite table rep.rejections\nselect r.user_ref, sum(case when r.status = 3 then 1 else 0 end) as rejected_count\nfrom fin.requests r\ngroup by r.user_ref;\n",
    "reports/ops/backlog.sql": "insert overwrite table rep.backlog\nselect count(case when r.status = 1 then 1 end) as pending_requests\nfrom fin.requests r;\n",
    "sql.py": make_repo.FILES["sql.py"],
    "README.md": make_repo.FILES["README.md"],
}
TASKS = [
    ("What is the total stake in US dollars for each game vendor? Return vendor_name, stake_usd.",
     "select v.vendor_name, sum(b.stake) / 100 from game.bets b join game.rounds r on r.id = b.round_ref join game.tables t on t.table_no = r.tbl "
     "join game.games g on g.code = t.g_code join game.vendors v on v.vid = g.vendor group by v.vendor_name"),
    ("How many sessions were there per region (the region of the player's agent)? Return region_name, sessions.",
     "select rg.region_name, count(*) from app.sessions s join app.devices d on d.device_uid = s.device_uid join crm.player_accounts p on p.id = d.owner_ref "
     "join crm.agents a on a.code = p.agent_code join crm.regions rg on rg.no = a.region_no group by rg.region_name"),
    ("How many withdrawal requests were rejected, per region of the player's agent? Return region_name, rejected_withdrawals.",
     "select rg.region_name, count(*) from fin.requests r join crm.player_accounts p on p.id = r.user_ref join crm.agents a on a.code = p.agent_code "
     "join crm.regions rg on rg.no = a.region_no where r.type = 2 and r.status = 3 group by rg.region_name"),
    ("What is the total approved deposit amount in US dollars for each agent? Return agent_name, deposit_usd.",
     "select a.agent_name, sum(r.amount_cents) / 100 from fin.requests r join crm.player_accounts p on p.id = r.user_ref join crm.agents a on a.code = p.agent_code "
     "where r.type = 1 and r.status = 2 group by a.agent_name"),
    ("How many distinct players placed at least one bet on a game from the vendor 'Aurora Studios' and also have at least one approved deposit? Return players.",
     "select count(distinct b.member_ref) from game.bets b join game.rounds r on r.id = b.round_ref join game.tables t on t.table_no = r.tbl "
     "join game.games g on g.code = t.g_code join game.vendors v on v.vid = g.vendor where v.vendor_name = 'Aurora Studios' "
     "and b.member_ref in (select user_ref from fin.requests where type = 1 and status = 2)"),
    ("How many sessions were there per device platform, counting only players who currently have at least one pending request? Return platform, sessions.",
     "select d.platform, count(*) from app.sessions s join app.devices d on d.device_uid = s.device_uid "
     "where d.owner_ref in (select user_ref from fin.requests where status = 1) group by d.platform"),
]


def build(root, n_noise=0):
    root = Path(root)
    files = {**FILES, **(make_repo.DECOYS if n_noise else {}), **make_repo.noise(n_noise, random.Random(11))}
    if n_noise:
        files["ddl/warehouse.sql"] = DDL
    for path, text in files.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(text)
    rnd = random.Random(23)
    con = duckdb.connect(str(root / "warehouse.duckdb"))
    for schema in ("crm", "fin", "app", "game", "ops"):
        con.execute(f"create schema if not exists {schema}")
    con.execute(DDL)
    con.executemany("insert into crm.regions values (?, ?)", [(10, "North"), (20, "South"), (30, "Islands")])
    agents = [(i, f"AG-{i:02d}", f"Desk {i:02d}", rnd.choice([10, 20, 30])) for i in range(1, 9)]
    con.executemany("insert into crm.agents values (?, ?, ?, ?)", agents)
    ids = [5000 + i for i in range(1, 81)]
    con.executemany("insert into crm.player_accounts values (?, ?, ?, ?, ?)",
                    [(i, f"Player {i}", rnd.choice(["PH", "SG", "MY", "TH"]), rnd.choice(agents)[1], rnd.choice(ids) if rnd.random() < 0.5 else None) for i in ids])
    con.executemany("insert into fin.requests values (?, ?, ?, ?, ?)",
                    [(i, rnd.choice(ids), rnd.choice([1, 1, 2]), rnd.choice([1, 2, 2, 3]), rnd.randint(10, 5000) * 100) for i in range(1, 601)])
    devices = [(f"dev-{i:04d}", rnd.choice(ids), rnd.choice(["ios", "android", "web"])) for i in range(1, 121)]
    con.executemany("insert into app.devices values (?, ?, ?)", devices)
    con.executemany("insert into app.sessions values (?, ?, ?, ?)",  # player_id: stale, a plausible but different player
                    [(i, rnd.choice(devices)[0], rnd.choice(ids), f"2026-10-{rnd.randint(1, 28):02d} 10:00:00") for i in range(1, 1201)])
    con.executemany("insert into game.vendors values (?, ?)", [(1, "Aurora Studios"), (2, "Blue Reef"), (3, "Cobalt Games")])
    games = [(i, f"G-{i:03d}", f"Game {i:02d}", rnd.choice([1, 2, 3])) for i in range(1, 13)]
    con.executemany("insert into game.games values (?, ?, ?, ?)", games)
    tables = [(100 + i, rnd.choice(games)[1], rnd.randint(1, 3)) for i in range(1, 25)]
    con.executemany("insert into game.tables values (?, ?, ?)", tables)
    rounds = [(9000 + i, rnd.choice(tables)[0], 1_760_000_000_000 + i * 30_000) for i in range(1, 401)]
    con.executemany("insert into game.rounds values (?, ?, ?)", rounds)
    con.executemany("insert into game.bets values (?, ?, ?, ?)",
                    [(i, rnd.choice(ids[20:]), rnd.choice(rounds)[0], rnd.randint(1, 500) * 100) for i in range(1, 2001)])
    if n_noise:
        con.execute(make_repo.DECOYS["ddl/support.sql"])
        con.executemany("insert into crm.staff_users values (?, ?)", [(i, f"Staff {i}") for i in ids[:40]])
        con.executemany("insert into ops.tickets values (?, ?, ?, ?)", [(i, rnd.choice(ids[:40]), rnd.randint(1, 3), rnd.randint(1, 2)) for i in range(1, 201)])
        con.executemany("insert into fin.requests_v1 values (?, ?, ?, ?, ?)",
                        [(i, rnd.choice(ids[:40]), rnd.choice([1, 2]), rnd.choice([1, 2, 3]), rnd.randint(10, 5000)) for i in range(1, 201)])
    con.close()


if __name__ == "__main__":
    build(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 0)
