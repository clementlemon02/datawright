# Benchmarks

| Benchmark | Question | Run |
|---|---|---|
| [corpus](corpus/run.py) | Does datawright still find the same things in public repos at pinned commits? Any difference fails CI until reviewed and `--update`d | `uv run benchmarks/corpus/run.py` |
| [lineage](lineage/run.py) | How much of a dbt project's true lineage (dbt's own manifest) does datawright find from SQL alone? | `uv run benchmarks/lineage/run.py <dbt project> --adapter bigquery` |
| [meaning](meaning/README.md) | Do agents write the right SQL on a warehouse whose keys and codes drifted, with and without datawright? | `uv run --with duckdb benchmarks/meaning/run.py --suite hard` |

## Lineage accuracy (November 2026)

Table lineage between a dbt project's models, seeds and sources, datawright reading SQL only (no manifest), against the manifest from `dbt parse`:

| Project | True edges | Precision | Recall | What's missed |
|---|---|---|---|---|
| shop demo (hand-checked, table and column edges) | 8 + 21 | 100% | 100% | |
| dbt-labs/jaffle-shop | 17 | 100% | 100% | |
| datacoves/balboa | 39 | 100% | 97.4% | One versioned model (`_v1`, `_v2` files of one model) |
| cal-itp/data-infra (865 nodes) | 1,198 | 100% | 95.7% | Sources named inside a Jinja loop: only running Jinja resolves them |

The first measurement on cal-itp was 78.3% recall. Two fixes took it to 95.7%: `ref()` and `source()` passed as a macro's argument, and refs inside the macros a model calls. No edge datawright reports was wrong in any project. When a project has a manifest (`dbt parse`), datawright reads it and the loop-built sources come with it.

Not yet measured: column lineage on a real project (dbt has no column-level answer key), and non-dbt SQL repos.
