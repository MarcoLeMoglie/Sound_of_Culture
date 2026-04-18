# Review Graph System Inventory

## Status

- block 5 completed on 2026-04-18
- graph rebuilt for the active `Sound_of_Culture` workspace
- current graph stats after rebuild:
  - files: 80
  - nodes: 1034
  - edges: 14614
  - indexed languages: `python`, `bash`

## Configured clients

- Codex: `~/.codex/config.toml`
- Cursor: `.cursor/mcp.json`
- OpenCode: `.opencode.json`
- Claude Code style MCP file: `.mcp.json`
- Kiro: `.kiro/settings/mcp.json`
- Antigravity: `~/.gemini/antigravity/mcp_config.json`

## Important limitation

The graph does not currently give structural coverage for:

- Stata `.do`
- LaTeX
- Overleaf content
- data artifacts

## Important noise source

Tracked archival and replication copies inside the repo, especially
`.coldstart_*` and packaged replication snapshots, can appear in graph search
results and community summaries.

## Operational interpretation rule

- use graph-first for active Python and shell code
- do not assume graph-first is enough for Stata, LaTeX, or reports
- when in doubt, inspect the active path directly
