# Code Review Graph SOP

This project uses `code-review-graph` as the first-line exploration and code
review tool.

## Where it is configured

Shared repo-side MCP config exists in:

- `.mcp.json`
- `.cursor/mcp.json`
- `.opencode.json`
- `.kiro/settings/mcp.json`

Local client config also exists in:

- `~/.codex/config.toml`
- `~/.gemini/antigravity/mcp_config.json`

## What block 5 standardized

On 2026-04-18:

- the graph was rebuilt for the current `Sound_of_Culture` workspace
- Kiro MCP config was created in-repo
- local Antigravity MCP config was manually realigned to this workspace after
  the installer left an older repo path in place
- repo MCP configs for graph access were confirmed for Codex, Cursor, and
  OpenCode

## Current coverage limits

The graph currently indexes:

- `python`
- `bash`

It does **not** provide structural coverage for:

- Stata `.do` files
- LaTeX reports
- CSV / DTA / data artifacts
- Overleaf assets

For those materials, use direct file inspection after checking whether the
graph can at least help you find adjacent Python or shell orchestration.

## Known repository-noise issue

This repository includes tracked archival and replication snapshot material,
including `.coldstart_*` and packaged replication copies. As a result:

- graph search can surface duplicate communities and duplicate functions
- older replication-package copies can appear before the active working file

Therefore:

- prefer active paths under `execution/step*` and `execution/phase_*`
- use path awareness when interpreting graph search results
- only work inside archival or replication-package paths when the task
  explicitly requires it

## Default workflow

1. start with `get_minimal_context`
2. use graph search / relationship tools before ripgrep
3. if the task concerns Stata, LaTeX, or data outputs, treat graph results as
   partial context only
4. after meaningful code changes, rebuild or update the graph if needed and
   restart clients if they are not seeing fresh results

## Rebuild command

```bash
code-review-graph build --repo /Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture
```

## Operational rule

Use the graph aggressively for Python and shell code. Do not over-trust it for
the rest of the project.
