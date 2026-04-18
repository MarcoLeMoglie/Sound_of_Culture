<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.

### Current Review-Graph Limits

- the current graph covers `python` and `bash`
- Stata, LaTeX, CSV, and report assets are not structurally indexed by the
  graph and still require direct file inspection
- the repository contains tracked archival / replication snapshot material,
  including `.coldstart_*` and packaged replication copies, so graph search may
  return duplicate legacy paths
- unless the task is explicitly about archival material, prefer active working
  paths under `execution/step*`, `execution/phase_*`, `directives/`, and
  `project_memory/`
- if graph results look stale or inconsistent, rebuild with
  `code-review-graph build --repo /Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture`
  and restart the client

## Sound of Culture: Project Instructions

### Project North Star

This project studies how music can be used as a measure of culture in the
United States.

The project now has four canonical phases:

1. **Phase 1: Dataset construction**
   - build the `country-only` and `adjacent-only` artist universes
   - scrape and process Ultimate Guitar material using the country artist list
   - build the final song-level dataset
   - recover missing metadata in all final datasets
2. **Phase 2: Exploratory analysis**
   - audit the constructed dataset
   - identify construction problems
   - describe temporal and spatial patterns
3. **Phase 3: Validation**
   - validate the music-based culture measure with external culture measures
     across US states
4. **Phase 4: Causal applications**
   - estimate how social shocks affect culture
   - first application: war deaths from 1946 onward
   - second application: the China shock in US manufacturing

### Canonical Deliverables

The canonical narrative outputs are the Overleaf reports stored in:

- `/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Applicazioni/Overleaf/Sound of culture`

The canonical phase layout for reports is:

- `phase_01_dataset_construction/01_country_only_and_adjacent_only/`
- `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/`
- `phase_02_exploratory_analysis/`
- `phase_03_validation/`
- `phase_04_causal_shocks/01_war_deaths/`
- `phase_04_causal_shocks/02_china_shock/`

The repo-side mirrors for organization live in:

- `reports/`
- `project_memory/`
- `workspace_maps/`

### Current Architecture Rule

The project is in a transition state:

- the **canonical conceptual structure** is phase-based
- many **working scripts still live in legacy `execution/step*` folders**
- every new modification must explicitly remember this coexistence rule rather
  than assuming the repository already has a single clean architecture

Do **not** rename or move legacy execution folders unless the current task
explicitly includes path migration and you have checked downstream references.

### Shared-Agent Continuity

This workspace must be usable by:

- Codex in this thread
- Antigravity reading the same project state
- a coauthor's Codex reading the same project state

Therefore continuity is mandatory.

`supermemory` is operational for local Codex in the shared project scope
`sound-of-culture`, but repo-side memory files under `project_memory/` remain
mandatory as the written backup layer and as the cross-client fallback for any
tool that is not yet authenticated locally.

### Mandatory Memory Discipline

After every meaningful work unit, update the relevant files in `project_memory/`:

- `project_memory/status/`
- `project_memory/decisions/`
- `project_memory/handoffs/`
- `project_memory/inventories/`

At minimum, record:

- what changed
- what remains unresolved
- what the next agent should do next
- whether a result was kept, rejected, or left provisional

### Mandatory Reporting Discipline

Every substantial new attempt must be reflected in the relevant phase report.

Each report must say, in plain English:

- what was done
- why it was done
- how it was done
- what code was used
- what output was generated
- whether the attempt is part of the final retained workflow
- if not retained, why it was dropped

This rule applies even to failed or discarded approaches if they meaningfully
informed the project.

### Stata Documentation Standard

Any analysis implemented in Stata must be documented at a high level of detail.

Each serious Stata workflow should leave:

- a commented `.do` file
- a report section that explains the logic block by block
- code snippets for important output-generating steps
- paths to generated tables and figures
- explicit sample restrictions and variable construction notes

Do not leave Stata analyses as unexplained `.do` files.

### Branching And Safety

- Never delete historical material from the current branch unless it is already
  preserved on a dedicated archival branch.
- Before destructive reorganization, create a snapshot branch and push it.
- Keep archival branches immutable after creation.
- Use a dedicated working branch for each major restructuring block.

### Data Safety

- Do not delete downloaded data or generated artifacts unless they are already
  safely preserved on an archival branch or in another explicit backup location.
- Prefer additive reorganization over destructive cleanup until the final
  cutover block.

### Where To Look First

- `directives/README.md`
- `directives/00_project_charter.md`
- `directives/07_reporting_overleaf_sop.md`
- `directives/08_memory_continuity_sop.md`
- `directives/10_code_review_graph_sop.md`
- `workspace_maps/restructure_block2_map_2026-04-18.md`

### Current Status Note

As of 2026-04-18:

- block 1 archive branch was created and pushed
- block 2 phase-based workspace skeleton was created and pushed
- block 3 directives and SOP rewrite was completed
- block 4 `supermemory` setup was completed and validated in-session
- block 5 review-graph setup was standardized across Codex, Cursor, OpenCode,
  Kiro, and local Antigravity configuration
- the review graph was rebuilt on 2026-04-18 and currently indexes `python`
  and `bash` only
