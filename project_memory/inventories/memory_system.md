# Memory System Inventory

## Repo-side memory

Guaranteed shared written memory currently lives in:

- `project_memory/status/current_status.md`
- `project_memory/decisions/decision_log.md`
- `project_memory/handoffs/next_agent.md`
- `project_memory/inventories/report_inventory.md`

## Supermemory MCP configuration

Configured locations:

- repo `.mcp.json`
- repo `.cursor/mcp.json`
- repo `.opencode.json`
- local Codex config: `~/.codex/config.toml`

Server configuration:

- URL: `https://mcp.supermemory.ai/mcp`
- project scope header: `x-sm-project = sound-of-culture`

## Validation status

- configured: yes
- authenticated in client: not yet validated
- safe to rely on as sole memory layer: not yet

## Operational rule

Until authentication and real usage are confirmed, always keep the repo-side
memory files up to date even if Supermemory is enabled in the client.
