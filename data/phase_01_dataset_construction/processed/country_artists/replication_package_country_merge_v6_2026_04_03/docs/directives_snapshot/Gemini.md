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

### Key Rules For Gemini

Follow `AGENTS.md` as the canonical instruction file for this project.

Use this summary when you need a quick project picture:

1. The project studies music as a measure of culture in the US.
2. The canonical structure is phase-based:
   - phase 1 dataset construction
   - phase 2 exploratory analysis
   - phase 3 validation
   - phase 4 causal shocks
3. The canonical reports live in the Overleaf project:
   - `/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Applicazioni/Overleaf/Sound of culture`
4. Legacy execution code still lives in `execution/step*` and should not be
   renamed casually.
5. Every meaningful attempt must be written into the relevant report, including
   whether the attempt is kept or discarded and why.
6. Every substantial work unit must update repo-side shared memory under
   `project_memory/`.
7. Stata work must always be documented in detail, with commented code and
   report snippets.
8. `supermemory` is configured for the project, but client authentication still
   needs to be completed and validated.

Read next:

- `directives/README.md`
- `directives/00_project_charter.md`
- `directives/07_reporting_overleaf_sop.md`
- `directives/08_memory_continuity_sop.md`
