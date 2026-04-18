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
- authenticated in local Codex config via bearer token env var: yes
- tracked repo configs store no credentials: yes
- direct HTTP verification against MCP server: yes
- MCP tool visibly available in the current live assistant session: yes
- `whoAmI` test: passed
- `memory(save)` test: passed
- `recall` test: passed
- safe to rely on as sole memory layer across all clients: not yet

## Operational rule

Until the MCP tool is confirmed as available and working in all relevant
clients, always keep the repo-side memory files up to date even if Supermemory
is enabled in the client.

## Secret-handling rule

- never store the Supermemory API key in tracked repo files
- keep authentication in local-only client configuration
- for local Codex, use `bearer_token_env_var = "SUPERMEMORY_API_KEY"` in
  `~/.codex/config.toml`
