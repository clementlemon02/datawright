# Meaning benchmark

Do agents write the right SQL on a warehouse whose keys and codes drifted, with and without datawright? Results and what they mean: [docs/semantics.md](../../docs/semantics.md#results-so-far-the-join-path-benchmark).

```bash
uv run --with duckdb run.py --suite hard --model sonnet --reps 2 --noise 1500   # multi-hop chains, piecemeal codes, a trap column
uv run --with duckdb run.py --reps 2 --noise 200            # big repo with decoys
uv run --with duckdb run.py --reps 2 --noise 200 --no-data  # agents can't query the warehouse
```

Each run costs about $0.15 to $0.30 on Sonnet. Agents run headless with project settings only and a fixed tool allowlist (read files, write `answer.sql`, `python3 sql.py`, `datawright.py`).
