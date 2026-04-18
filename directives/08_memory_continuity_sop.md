# Memory And Continuity SOP

## Purpose

This project is multi-agent and multi-conversation. Memory continuity is not a
nice-to-have. It is a hard requirement.

## Current memory system

`supermemory` MCP is configured for this project with:

- server URL: `https://mcp.supermemory.ai/mcp`
- project scope: `sound-of-culture`

Until client authentication is completed and validated in the relevant tools,
the active guaranteed memory layer remains the repo-side structure under
`project_memory/`.

## Minimum required updates

After each meaningful work unit, update as needed:

- `project_memory/status/current_status.md`
- `project_memory/decisions/decision_log.md`
- `project_memory/handoffs/next_agent.md`
- `project_memory/inventories/report_inventory.md`

## What to record

- current branch and current block
- what changed
- what remains open
- what report was updated
- what result is final, provisional, or rejected
- the exact next recommended step

## Handoff rule

Write handoffs so that another agent can continue without rereading the entire
conversation history.

## Cross-user sharing note

Inference:

To guarantee that different human users and their clients see the exact same
shared Supermemory workspace, they will likely need to authenticate against the
same Supermemory account or use the same shared API key plus the same
`x-sm-project` value. The `x-sm-project` header alone is used here to scope the
project, but authentication identity may still matter.
