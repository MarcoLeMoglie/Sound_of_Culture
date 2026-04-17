# Decision Log

## 2026-04-18

### Decision

Use a phase-based canonical architecture while temporarily preserving the legacy
`execution/step*` layout.

### Why

Legacy paths are still hardcoded in scripts, imports, and documentation. A
full path migration before documenting the new architecture would create
avoidable breakage.

### Consequence

The current branch uses a transitional model:

- phase-based structure for organization and reporting
- legacy execution paths for code stability

## 2026-04-18

### Decision

Make the rewritten directives and Overleaf-report SOP the canonical instruction
layer for the project.

### Why

The old directives were tied to an earlier project framing and did not encode
the new four-phase research agenda, the dual Phase-1 report requirement, or the
mandatory kept-vs-discarded reporting rule.

### Consequence

Agents should now start from:

- `AGENTS.md`
- `directives/README.md`
- `directives/00_project_charter.md`
- `directives/07_reporting_overleaf_sop.md`
- `directives/08_memory_continuity_sop.md`
