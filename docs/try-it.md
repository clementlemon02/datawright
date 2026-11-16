# Try it in ten minutes, and tell us what happened

We've tested the kit on public repos and one production platform, all by its authors. What we don't know is how it goes for you. Ten minutes on a real repo and five answers would help more than anything else.

## Try it

1. Install the skills (see the [README](../README.md#quick-start)).
2. In a data repo, ask your agent: **"Set up this repo for data engineering work. Then tell me what `<a table you know well>` is and what breaks if I change `<one of its columns>`."**
3. Check the answer against what you know.

No agent to hand? The index alone takes a minute, from the repo root:

```bash
uv run <path to>/skills/data-context/scripts/dctx.py init
```

```bash
uv run .data-context/dctx.py coverage
```

```bash
uv run .data-context/dctx.py lint
```

Nothing leaves your machine: dctx reads code, not data, and makes no network calls.

## Tell us

Open an issue with the "First hour" template, or just answer these:

1. **Stack**: warehouse, orchestrator, transformation tool, roughly how many SQL or pipeline files.
2. **Did setup finish without your help?** If not, where did it stop?
3. **Was the answer about your table right?** Anything missing or wrong (a downstream it didn't know, a writer it missed)?
4. **`coverage`**: what did it say it doesn't read in your repo?
5. **Would you keep it?** What would make you?

Wrong lineage is the most useful thing you can report: the table, what dctx said, and what's true.
