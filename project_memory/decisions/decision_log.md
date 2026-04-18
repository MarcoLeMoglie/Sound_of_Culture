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

## 2026-04-18

### Decision

Treat `code-review-graph` as the primary exploration tool for Python and shell
code, but not as the sole source of truth for the full project.

### Why

The rebuilt graph is valuable and now standardized across clients, but it only
indexes `python` and `bash` in this workspace and it also sees tracked
archival / replication snapshot paths that can generate duplicate results.

### Consequence

Agents should:

- use graph tools first for Python and shell exploration
- fall back to direct reads for Stata, LaTeX, and data artifacts
- interpret graph results with path awareness, especially around `.coldstart_*`
  and replication-package copies

## 2026-04-18

### Decision

Make the repository root `README.md` and the phase-based Overleaf entry points
the canonical onboarding layer for collaborators.

### Why

The project had instructions and inventories scattered across multiple files,
but no single onboarding document at the repository root and no complete set of
phase report entry points in Overleaf for the missing phases.

### Consequence

Collaborators should now start from:

- `README.md`
- `AGENTS.md`
- `project_memory/status/current_status.md`
- `workspace_maps/restructure_block_history_2026-04-18.md`

And phase report work should now target the phase-based Overleaf entry points
created during block 6.

## 2026-04-18

### Decision

Use a safe pre-cutover inventory before any destructive cleanup of legacy
execution paths.

### Why

The restructured branch is now the GitHub default branch, but the repository
still contains many active references to legacy `execution/step*` paths and to
replication-package copies that intentionally preserve historical layouts.

### Consequence

The next cleanup block should migrate active references first and should avoid
rewriting archival or packaged replication material unless explicitly required.

## 2026-04-18

### Decision

Introduce phase-based wrapper entrypoints instead of moving or deleting legacy
implementation files immediately.

### Why

The project still contains active code, Stata scripts, and replication wrappers
that depend on legacy `execution/step*` layouts. Wrapper entrypoints reduce
user-facing legacy references while keeping the implementation stable.

### Consequence

The canonical execution surface now starts in:

- `execution/phase_01_dataset_construction/`
- `execution/phase_02_exploratory_analysis/`

But the underlying implementation and archival material remain in place until a
later migration block safely retires them.

## 2026-04-18

### Decision

Move operational launch instructions and project-root replication launchers to
phase-based entrypoints before attempting any deeper import-level migration.

### Why

After block 8, the biggest remaining user-facing legacy exposure was in the
instructions and orchestrators people actually use to run the project:
replication READMEs, Antigravity notes, and Stata launchers. Migrating those
first reduces practical confusion without disturbing audited package layouts.

### Consequence

The canonical entrypoints for project-root reruns now live in:

- `execution/phase_01_dataset_construction/*.py`
- `execution/phase_01_dataset_construction/do/*.do`

Historical `step*` names remain inside bundled replication packages and legacy
implementation modules until a later migration block decides whether deeper
package-level rerooting is worth the breakage risk.

## 2026-04-18

### Decision

Start the deep migration by turning selected Phase 1 wrappers into importable
bridge modules and rerooting active internal imports to those bridge modules.

### Why

Option 1 was chosen for the restructuring program: the goal is no longer only
to hide legacy paths at the launcher surface, but to progressively converge the
active codebase toward a single phase-based architecture. To do that safely,
the first step is to make canonical phase modules importable while keeping
legacy files as the underlying implementation.

### Consequence

The repository still follows a coexistence rule for now, and every new
modification must explicitly remember that rule rather than pretending the
architecture is already fully clean. At the same time, new active imports
should prefer `execution.phase_01_dataset_construction` whenever a bridge
module exists there.

## 2026-04-18

### Decision

Validate the restructuring with safe end-to-end runtime checks before pushing
deeper into additional import migrations.

### Why

After block 10, the project had moved from surface wrappers to real bridge
modules. The next risk was no longer syntax but runtime behavior. Safe local
workflow checks provide better evidence that the new phase-based surface is not
just importable, but usable.

### Consequence

Runtime validation should now accompany further restructuring whenever the
changes touch active execution paths. In this first pass, the validated paths
were:

- `execution/phase_01_dataset_construction/build_billboard_country_supplemental_targets.py`
- `execution/phase_01_dataset_construction/run_country_songs_replication.py`

## 2026-04-18

### Decision

Use repo-relative links, not local absolute filesystem links, in the main
GitHub-facing `README.md`.

### Why

The default branch README is consumed on GitHub first. Absolute local links of
the form `/Users/...` work in the desktop app but break on GitHub, which makes
the repository landing page confusing for collaborators.

### Consequence

The main `README.md` should keep GitHub-facing references repo-relative.

## 2026-04-18

### Decision

Treat the restructuring program as operationally complete once every active
Phase 1 Python entrypoint and every active Phase 2 exploratory `.do` launcher
has a canonical phase-based bridge.

### Why

At this point the remaining legacy `execution/step*` directories are serving
mainly as the implementation backend and as compatibility roots for historical
replication packages. Pushing further just to erase those names would be a
deeper refactor, not a prerequisite for productive work on the new project.

### Consequence

Agents should now:

- launch active work from `execution/phase_01_dataset_construction/` and
  `execution/phase_02_exploratory_analysis/`
- treat additional legacy cleanup as optional refactoring, not as an immediate
  blocker
- keep the coexistence rule in mind whenever they touch legacy implementation
  files that still sit behind the phase-based bridge layer

## 2026-04-18

### Decision

Make English the mandatory language for all canonical phase reports in
Overleaf.

### Why

The reports are the shared research-facing deliverables for collaborators and
must be readable in a consistent language across agents, conversations, and
coauthors.

### Consequence

All retained phase-report text should be written in English, even if working
notes or chat discussions happen in Italian.

## 2026-04-18

### Decision

Do not perform a destructive cleanup of tracked legacy `execution/step*` files
yet; treat destructive cutover as a separate migration program.

### Why

The phase-based surface is now complete operationally, but it still delegates
to legacy runtime backends. Deleting those files now would break active
wrappers, replication packaging, and exploratory Stata launchers.

### Consequence

The project now has a formal destructive-cutover plan in
`workspace_maps/destructive_cutover_plan_2026-04-18.md`.

Until that plan's native-backend migration and validation stages are complete:

- do not delete tracked legacy execution code
- limit cleanup to transient residue
- treat any future deletion pass as high-risk work requiring explicit runtime
  validation
Machine-specific local paths can still appear as plain text when they are
needed to describe external assets such as the Overleaf folder.

## 2026-04-18

### Decision

Prioritize helper-module bridging and helper-import cleanup before attempting
another large live rebuild path.

### Why

After blocks 11 and 12, the phase-based surface had already been validated on
the replication wrappers. The next highest-value bottleneck was the helper
layer: local imports like `scraper_client` and residual unbridged Phase 1
modules still made the code harder to reason about and less consistent with the
new architecture.

### Consequence

The next migration pass should favor:

- bridge modules for high-value helper scripts
- rerooting active helper imports to those bridges
- local cached runtime validation where possible

before spending effort on more expensive live rebuild workflows.

## 2026-04-18

### Decision

Complete wrapper coverage across the full active Python surface of
`step1_download` and `step2_digitalize` once the bridge pattern was already
working.

### Why

After the previous helper-migration passes, most of the risk had shifted from
architecture design to consistency. Leaving a long tail of legacy-only helper
files would have kept Phase 1 conceptually split even if the high-value paths
were already bridged.

### Consequence

The canonical Phase 1 directory now provides wrappers across the full active
Python surface of the legacy `step1_download` and `step2_digitalize`
subtrees. Future restructuring can move on to deeper artist-side helpers or
stop here and reuse the wrapper layer while focusing on substantive research.
