# Meaning from usage: a strategy for data dictionaries

Researched November 2026. The problem: teams build tables with different names, types and encodings for the same thing (`player_id`, `uid`, `user_id`; `BIGINT` here, `VARCHAR` there; status `1/0` here, `'active'` there). Humans and AI agents can't tell what joins to what, or what a value means.

## What the research says

- **Most AI failures on real databases are about meaning, not SQL.** On a large multi-database benchmark, schema linking (picking the right tables and columns) caused over 60% of text-to-SQL failures ([LinkAlign, EMNLP 2025](https://arxiv.org/pdf/2503.18596)). On BIRD, giving GPT-4 a short note of external knowledge per question raised accuracy from 34.9% to 54.9% ([BIRD](https://arxiv.org/pdf/2305.03111.pdf), via a [review](https://liner.com/review/can-llm-already-serve-as-database-interface-big-bench-for)). Context beats a better model.
- **Matching columns by name and values works, but needs the data.** Schema matchers combine small models for candidates with LLM reranking ([Magneto, PVLDB 2025](https://arxiv.org/html/2412.08194v2)). Semantic type detection reads column values ([Sherlock](https://ar5iv.labs.arxiv.org/html/1905.10688), [Sato](https://arxiv.org/pdf/1911.06311)). Join discovery hashes values to find overlap ([Aurum](https://www.csail.mit.edu/research/aurum-large-scale-data-discovery)). All of these read data, which is costly at scale and often not allowed.
- **Usage is the best evidence, and it is under-used.** The newest join-inference work starts from value overlap, then prunes thousands of coincidental matches (6,185 to 21 on a production database) with an LLM judge, and refines with the database's query history ([Tursio, 2026](https://arxiv.org/pdf/2603.04176)). Query history is the refinement there; it can be the starting point.
- **Industry converges on a graph of entities, managed as code.** dbt's MetricFlow makes join keys first-class "entities" (primary, foreign, unique, natural) and derives join paths from them ([docs](https://docs.getdbt.com/docs/build/join-logic)). Ontology-based data access maps a domain model onto SQL so queries run against concepts, not tables ([Ontop](https://www.inf.unibz.it/~calvanese/papers/calv-etal-SWJ-2017.pdf)). Uber's Databook and Airbnb's Dataportal model metadata as graphs, and both found moving from manual curation to automation the hard part ([Databook](https://atlan.com/uber-databook-metadata-catalog/), [Dataportal](https://medium.com/airbnb-engineering/852d76c51770)). Kimball's conformed dimensions are the same idea from 1990s modelling: one definition, reused by every fact table ([conformed dimensions](https://coginiti.co/glossary/conformed-dimensions/)).
- **Standards are arriving.** Open Semantic Interchange (Snowflake, dbt Labs, Salesforce and others, September 2025) aims at one vendor-neutral, likely YAML, spec for semantic metadata ([announcement](https://finance.yahoo.com/news/snowflake-salesforce-dbt-labs-more-130100051.html)); its spec details weren't published at the time of writing.
- **LLM-written descriptions are good enough to propose, not to trust blindly.** On ~500,000 columns, 87 to 88% of generated descriptions were accepted by data stewards as is or with minor edits ([Singh et al., 2025](https://arxiv.org/abs/2503.09003)).
- **The pain is organisational.** 51% of data engineers cite lack of ownership as a modelling pain ([2026 survey](https://joereis.github.io/practical_data_data_eng_survey)); trust rose from 66% to 83% as a priority in a year ([dbt Labs, 2026](https://www.getdbt.com/blog/new-dbt-labs-report-finds-ai-driven-acceleration-is-outpacing-trust-and-governance)). A dictionary nobody owns goes stale; one reviewed like code doesn't.

## The idea: meaning is in how data is used

Every team already wrote its data dictionary, scattered through its code:

| What the code does | What it tells us |
|---|---|
| `JOIN p ON w.player_id = p.id` | `w.player_id` and `p.id` are the same concept, and `p` is where it's defined |
| `SELECT player_id AS uid` (lineage with no transformation) | `uid` downstream is that concept under another name |
| `amount / 100 AS amount_usd`, `CAST(ts AS DATE)`, `CONVERT_TZ(ts, 'UTC', 'Asia/Manila')` | two representations of one concept, and the conversion between them (cents to dollars, UTC to local) |
| `CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'closed' END` | the value dictionary for `status`: written once by whoever understood it, then forgotten |
| `WHERE is_deleted = 0` on every read | a rule every consumer must apply |

On one real platform (an Airflow repo, a Flink jar and a Java service), a plain text count found 552 joins between differently named columns across 95 name pairs (`id = player_id`, `entity_id = id`, `player_id = playerid`) and 1,289 `CASE … WHEN` expressions. The dictionary is there; it's just spread over a thousand places.

datawright already parses all of this, across every repo and engine, without touching data. The strategy: **compile the dictionary from usage, have humans confirm it once, then enforce it where code changes.** It's the same move that made lineage work: deterministic extraction first, judgment only where needed.

### 1. Concepts, from usage (no data read)

Group columns into concepts with union-find over evidence, each edge weighted and kept as provenance:

- join equalities in code and imported query logs (strongest: someone ran it and relied on it);
- identity lineage (a column passed through or renamed);
- declared keys: dbt `unique` + `relationships` tests, MetricFlow entities, contracts, primary keys in DDL;
- name variants (`userId`, `user_id`, `uid`), as a weak tie-breaker only.

A concept's home is the table where it's a key (`crm.players.id`); every other column is a reference to it. That gives each concept a name, a type, a home and its aliases, with confidence from the evidence count. Value overlap (Aurum, Tursio) is an optional check through the guarded `query_command`, not the source.

### 2. Conversions and value dictionaries, from transformations

Where lineage transforms a concept, record the conversion: unit (`/100`, `*1000`), type (`CAST`), time zone (`CONVERT_TZ`, `AT TIME ZONE`), encoding (`CASE ... WHEN 1 THEN 'active'`). `CASE` mappings become value dictionaries attached to the column they decode. This is where most "what does 3 mean" questions are already answered.

### 3. A dictionary file, reviewed like code

`datawright concepts` writes proposals to `.data-context/concepts.yml`: concept, canonical name and type, home, aliases, conversions, value dictionary, and the evidence for each. Humans (or an agent, with a human approving the merge request) rename, merge, split and describe. Confirmed entries are never overwritten; new evidence becomes new proposals. Ownership is the file's history.

### 4. Everywhere it's needed

- **Cards** say what each column is: `player_id → concept player (home crm.players.id; also uid, user_id)`, with its value dictionary.
- **Relationships**: `datawright erd <table>` and card lines like `joins crm.players on player_id = id (12 queries)`, so an agent picks the right join path the first time.
- **Lint**: one concept with several types (forces casts, breaks pruning), several names, or conversions applied inconsistently (one consumer divides by 100, another doesn't).
- **Review**: a branch adding `userid VARCHAR` is told the team's name for it is `player_id BIGINT`; a new `CASE` on a known coded column is checked against the dictionary.
- **Agents**: the MCP server answers "what is X" and "how do I join A to B" from concepts, which is the schema-linking step research says fails most.

### 5. Connect, don't absorb

Export concepts as dbt semantic-model entities, an OpenMetadata or DataHub glossary, and Open Semantic Interchange once its spec is published. Import them back from the same places, so a team that already curates a glossary starts with it.

## Why this is different

| Approach | Needs data | Needs curation up front | Stays current |
|---|---|---|---|
| Catalog glossaries (manual) | No | Heavy | Rarely |
| Value-based matching (Aurum, Sherlock, Magneto) | Yes | No | On re-profiling |
| Semantic layers (MetricFlow, OSI) | No | Heavy: every entity written by hand | When maintained |
| **Usage-compiled concepts (this)** | **No** (optional sample check) | **Confirm proposals only** | **Every build, and checked in every merge request** |

## How we'll know it works

- **Join-path accuracy for agents**: questions that need a join across renamed keys, on the demo, a production platform and public repos, answered with and without concepts. The BIRD and LinkAlign results say this is where the gain is.
- **Concept precision**: hand-check proposals on three repos; aim for the steward acceptance rate the metadata research reports (~85%) before trusting them in review.
- **Drift over time**: the number of names and types per concept, tracked per release on a real repo.

## Results so far: the join-path benchmark

`benchmarks/meaning/` builds a small gaming warehouse with real drift (the player key is `id`, `user_ref`, `playerid` as text and `member_ref`; agents are keyed by `code`; request `type` and `status` are explained only by report SQL; a decoy `ref` column) and asks five questions, each needing a renamed key, a coded value or both. An agent (Claude Code, Sonnet, headless, project settings only, a fixed tool allowlist) writes `answer.sql`, which is run against seeded data and compared with the reference answer. Two conditions: the repo as is, and the repo with the data-context skill installed and its index built, as setup leaves it. 50 runs, $9 in total (November 2026):

| Setting | Right, baseline / datawright | Cost per question | Turns | Seconds |
|---|---|---|---|---|
| Small repo (8 SQL files), warehouse access | 5/5 / 5/5 | $0.22 / $0.15 | 11.0 / 9.0 | 26 / 24 |
| Big repo (211 files: decoy `user_ref` and `status` meanings, a legacy table with reversed codes), warehouse access | 10/10 / 10/10 | $0.19 / $0.16 | 12.6 / 9.6 | 36 / 27 |
| Big repo, no warehouse access (code only) | 10/10 / 10/10 | $0.20 / $0.17 | 9.9 / 9.9 | 27 / 31 |

**What it shows.** A strong model got every answer right without datawright, even through 200 decoy files and without data to check against: the meaning was in the code, and it found it. datawright made the same answers 18% cheaper and took 14% fewer turns overall. So on this benchmark the gain is efficiency, not correctness; the accuracy claim isn't shown yet.

**What it changed anyway.** Building the benchmark found four bugs that would have hurt agents on any repo, all fixed: joins through `CAST` weren't recorded; `datawright join` didn't write the cast a text key needs; a code decoded inside `SUM(CASE …) / 100` wasn't read; and concepts whose every table names them differently got a poor name. It also found that setup's AGENTS.md never told agents about `datawright join` or the card's `means` and `values`; it does now.

### The harder suite

`--suite hard` (`make_hard.py`): no single file answers a question. Join chains of three and four hops with a renamed key at every hop, each hop written in a different file; request codes decoded only piecemeal (`status = 2` as `approved_requests` in one report, `status = 3` as `rejected_count` in another); one trap column, `app.sessions.player_id`, still populated but stale, where the team's code goes through `devices.owner_ref`; 1,500 noise files with decoy meanings and a legacy table with reversed codes. Six questions, two repeats, warehouse access, 48 runs, $8 (November 2026):

| Model | Right, baseline | Right, with datawright | Cost per question | Turns |
|---|---|---|---|---|
| Sonnet | 7/12 | **12/12** | $0.26 / $0.22 | 15.5 / 13.4 |
| Haiku | 3/12 | **7/12** | $0.13 / $0.09 | 18.4 / 13.3 |

Every wrong answer was one of two things: joining on the stale `sessions.player_id` (a plausible name, rows come back, the numbers are wrong), or a wrong `type` or `status` code, once taken from the legacy file whose codes are reversed. Having the data to query didn't save the baseline: wrong joins return plausible rows.

Read it with these limits in mind:
- **Small and home-made.** 12 runs a cell, one synthetic repo, written by the people who built datawright with its strengths in mind. It shows the mechanism, not a general accuracy number.
- **Haiku used datawright in only 6 of its 12 datawright runs**, despite the repo's instructions. When it did, it was right 5 times of 6; when it didn't, 2 of 6. Getting weaker models to reach for the tool is half the problem.
- **After two changes, Haiku reached for datawright in 9 of 12 runs (from 6) and got 8 of 12 right (from 7).** The changes: firmer wording in setup's AGENTS.md, and an `unused keys` line on cards for key-like columns no code reads or joins. More use; the accuracy change is within noise at 12 runs.
- **datawright avoided the stale column only through `datawright join`** when this was first run; the `unused keys` card line now names it.

**Next.** A real repo with questions and answers checked by its engineers; more repeats; other agents (Codex, Cursor); and the two product gaps above.

## Build order

1. Join pairs and identity lineage into concepts; relationships on cards; `concepts.yml` proposals. **Done**: `datawright concepts`, `datawright join`, `joins` and `means` on cards, MCP `join`. On a production platform's repos (Airflow, a Flink jar, a Java service) it found 15 concepts, kept a referrer column in the player concept because the code joins it to the players table, and flagged the app-database and warehouse copies of one entity as likely one concept.
2. Review and lint checks for new drift (names, types). **Done**: kinds of type compared within each concept, respelled names flagged, and once a concept is confirmed any name not in its `roles` too; shown in `lint` and on the merge request. A MyBatis connector came with it, since the drift on the test platform lived in Java services' mapper XML.
3. Conversions and `CASE` value dictionaries. **Done**: labels from `CASE`, searched `CASE` and `DECODE`; from conditional aggregates' aliases when no label is written (`SUM(CASE WHEN type = 1 THEN amount END) AS deposit_amount` gives `type 1 ≈ deposit`), which is how most of the test platform's `CASE` expressions encode meaning; unit, epoch and time-zone conversions; and a check for one code labelled two ways. On the test platform it named a four-value payout type from one `CASE`, a three-level hierarchy code from aliases, and a request type from seven conditional aggregates.
4. Exports (dbt entities, OpenMetadata glossary), then OSI. **Done**: `datawright export dbt | openmetadata | datahub`; the dbt file passes `dbt parse` (1.12) and the DataHub file passes DataHub's own glossary ingestion. A dbt project's entities, in either the `semantic_models:` or the newer per-column form, are read back as confirmed concepts (jaffle-shop's six arrive that way). Open Semantic Interchange waits for its spec.
5. Optional value-overlap check through `query_command` for low-confidence concepts.
