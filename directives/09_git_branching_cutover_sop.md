# Git Branching And Cutover SOP

## Archival rule

Before destructive cleanup or reorganization:

1. create an archival branch
2. commit the full current state
3. push the archival branch

## Working-branch rule

Use a dedicated working branch for each major restructuring block.

## Current restructuring logic

- block 1: archival freeze
- block 2: phase-based workspace skeleton
- block 3: directives and SOP rewrite
- later blocks: memory plugin, graph standardization, report system, final cutover

## Deletion rule

Do not remove legacy code or historical artifacts from the current branch until:

- they are preserved on an archival branch
- the replacement structure is ready
- downstream references have been checked
