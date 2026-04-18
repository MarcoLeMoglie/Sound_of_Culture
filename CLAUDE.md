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

### Key Rules For Claude

`AGENTS.md` is the canonical instruction file for this project.

The essential operating rules are:

- work against the new four-phase project design
- treat Overleaf reports as canonical narrative deliverables
- preserve legacy `execution/step*` paths unless migration is explicit
- update `project_memory/` after meaningful work
- document every serious attempt, including rejected ones
- keep Stata workflows fully explained
- preserve archival branches before destructive cleanup
- remember that `supermemory` is configured, but client authentication still
  needs to be completed and validated

Read next:

- `directives/README.md`
- `directives/00_project_charter.md`
- `directives/07_reporting_overleaf_sop.md`
- `directives/08_memory_continuity_sop.md`
