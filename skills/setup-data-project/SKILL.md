---
name: setup-data-project
description: Set up any data engineering repo for AI coding agents (Claude Code, Codex, Cursor, Gemini CLI, Grok Build or others), whatever the stack. Writes AGENTS.md with the repo's real commands, builds a compiled data context index (lineage, schema, readers) with zero model tokens, installs vendor skills for detected tools, and connects warehouses, orchestrators and catalogs read-only. Use when the user says "set up this repo", "connect my warehouse/orchestrator/catalog", "add an MCP server", or opens a data repo with no AGENTS.md or CLAUDE.md. Rerun when the stack changes.
license: MIT
---

# Set up a data project

Goal: any agent working in this repo knows how to build and test it, has a growing map of the data it touches, and can look at (never change) the systems behind it. Detect the stack and the agents; don't assume them. Safe to rerun: update what exists instead of overwriting it.

**Deliver first, ask last.** Steps 1 to 3 need nothing from the user: do them in one go (detect, write `AGENTS.md`, build the data context), and answer whatever else they asked using the new index. Only then ask your questions, together, at the end: corrections to what you detected, other agents they use, and whether to connect a warehouse. Don't stop to ask before the index exists.

## 1. Detect the stack and the agents

Read the repo:
- **Dependencies**: `requirements*.txt`, `pyproject.toml`, `package.json`, `pom.xml`, `build.gradle`, `go.mod`
- **Config and infra**: `docker-compose*.yml`, `Dockerfile`, Terraform / Helm, `.env.example`, `profiles.yml`, connection configs
- **Build and CI**: `Makefile`, `justfile`, `.github/workflows`, `Jenkinsfile`, `.gitlab-ci.yml`
- **Code**: imports, connection strings, table and topic names, to find what it reads and writes
- **Tickets**: keys in `git log` and branch names (`DATA-123`), to find the tracker (Jira, Linear, GitHub Issues) and the team's branch and commit conventions
- **Agents already configured**: `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, `.mcp.json`, `.codex/`, `.cursor/`, `.gemini/`, `.grok/`

List the data systems, orchestration and transformation tools, and compute engines you found, and keep the list for your closing summary, where the user can correct it and name other agents they use. Carry on.

## 2. Write AGENTS.md

`AGENTS.md` is the shared instructions file most agents read. Write it once and point the others at it.

- If one exists, propose additions instead of overwriting it.
- Otherwise fill [assets/AGENTS.md](assets/AGENTS.md). Only list commands you found in the repo (Makefile targets, CI steps, README); never invent one. Keep it under ~60 lines and leave out sections you can't fill.
- Write the pointer for the agent you are running as now; add the others when the user names them.
- For agents that read a different file, create it as a one-line pointer, not a copy: Claude Code reads `CLAUDE.md` and Gemini CLI reads `GEMINI.md`; write `@AGENTS.md` in each. For other agents, check their docs.

## 3. Build the data context

Follow "First time in a repo" in the `data-context` skill: it puts `datawright` in `.data-context/`, sets the SQL dialect and builds the index of datasets, lineage, columns and readers from the code, in seconds and without model tokens. Then run `datawright coverage` and work through its gaps with connectors (the skill's `references/connectors.md`), largest first, until what's left doesn't matter: in-house frameworks, jobs defined in config, other orchestrators, hand-written lineage. That's how the kit learns this team's stack instead of assuming one. Show the user the build summary, what coverage found and which connectors you wrote, and one `card` for a central dataset.

## 4. Add vendor tool skills

If a detected tool has official vendor skills (see the vendor skills section of [references/connections.md](references/connections.md)), offer to install them for each of the user's agents. They teach the tool's mechanics; this kit handles the workflow.

## 5. Connect data systems, read-only

Connect only the systems the user's tasks need. Ask which, and which **non-production** environment to use. For each:

1. **Pick the connection.** Look the tool up in [references/connections.md](references/connections.md). If it's missing, search for "<tool> official MCP server" and prefer the vendor's own server, then one the vendor's docs list, then a widely used community one. Re-read the server's current README for exact commands and env vars, since they change. If the tool's CLI is already installed and logged in with a read-only user, the agent can use that instead. If nothing exists, say so and skip; don't write a server.
2. **Make it read-only**: a least-privilege read-only user first, then the server's read-only setting, then block any remaining write tools.
3. **Write the config for each agent the user named**, in that agent's format (check its MCP docs):

   **Claude Code**: `.mcp.json`, with write tools denied in `.claude/settings.json`
   ```json
   { "mcpServers": { "<tool>": { "command": "…", "args": ["…"], "env": { "VAR": "${VAR}" } } } }
   ```
   ```json
   { "permissions": { "deny": ["mcp__<tool>__<write_tool>"] } }
   ```

   **Codex**: `.codex/config.toml` (loaded only in trusted projects)
   ```toml
   [mcp_servers.<tool>]
   command = "…"
   args = ["…"]
   disabled_tools = ["<write_tool>"]
   ```

   **Cursor, Gemini CLI, Grok Build, others**: the same server and blocked tools, in the format their docs give.

   Remote (HTTP) servers use a `url` instead of `command`, and usually OAuth.

Credentials are always environment-variable references, never literal values: these files get committed.

**datawright as a server, optional.** If an agent has no shell, or the user wants agents to query only through a guard that runs single SELECTs with a row cap, register datawright too (command `uv`, args `["run", ".data-context/datawright.py", "serve"]`, no env vars; see the `data-context` skill) and set `query_command` in `.data-context/config.yml` to the read-only CLI connection. For agents with a shell, also deny the raw database CLI (Claude Code: `Bash(psql:*)` in `permissions.deny`).

## 6. Check every merge request

Offer the data impact check for the repo's CI: [assets/ci/gitlab-ci.yml](assets/ci/gitlab-ci.yml) for GitLab, [assets/ci/github-actions.yml](assets/ci/github-actions.yml) for GitHub. On every merge request it builds the index and posts `datawright review`: the tables the change touches, their DAG tasks and downstream, recorded gotchas, SQL that no longer parses, the ticket key. `.data-context/` must be committed. Posting on GitLab needs a project access token in a CI variable; the template says how. Commit `.data-context/` and the CI file only with the user's approval.

## 7. Hand over

Tell the user, as a short list:
- what you wrote, by file and by agent
- which env vars to export (shell profile, or a git-ignored `.envrc` / `.env`)
- to restart each agent and check its MCP tool list shows the new servers
- to check their employer's AI policy before connecting company data, since query results go to the model provider
- next step: ask for the context or impact of the table they're working on (`datawright card`, `datawright lineage`), and `datawright savings` later to see what it saved
