# Memory And Continuity SOP

## Purpose

This project is multi-agent and multi-conversation. Memory continuity is not a
nice-to-have. It is a hard requirement.

## Current memory system

Until `supermemory` is installed and validated, the active memory layer is the
repo-side structure under `project_memory/`.

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
