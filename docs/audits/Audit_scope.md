# AI Auditor Onboarding Guide

## 1. Initial Setup
1. Read `docs/AGENTS.md` to understand repository requirements and local bead workflow conventions.
2. Review `docs/pureflow_proposal.md` for the project direction and intended runtime model.
3. Review the relevant epic plan in `docs/epics/` before grading proposed-versus-implemented scope.

## 2. Repository Analysis

### 2.1 Structure Audit
- Confirm the workspace layout is coherent for a multi-crate Rust project:
  - repo-root `Cargo.toml`
  - repo-root `README.md`
  - repo-root `LICENSE`
  - member crates under `crates/*/src/`
  - project docs under `docs/`
- Check for:
  - missing or stale documentation
  - inconsistent crate or directory naming
  - repo metadata that conflicts with the workspace manifest

### 2.2 Proposal Review
- Analyze `docs/pureflow_proposal.md`:
  - clarity of objectives
  - technical feasibility
  - alignment with current code structure
  - missing requirements or unresolved design decisions
- Use the active epic docs to distinguish:
  - intentional scaffold gaps
  - genuine drift from the planned bead sequence

### 2.3 Code Quality Assessment
- Review all relevant code in `crates/`:
  - implementation of current-bead objectives
  - missing documentation on public items
  - unused or dead code
  - weak error handling
  - correctness bugs
  - inefficient algorithms where they materially matter
  - security issues if the code crosses trust boundaries
- Check for:
  - unit test coverage
  - property-based tests where invariants are non-trivial
  - lint and formatting health when practical

## 3. Assessment Output
- Write the audit as a dated file under `docs/audits/`, using the pattern `Audit_YYYY_MM_DD.md` or the repo's current dated naming convention.
- Include:

### 3.1 Summary
- overall repository health score
- key findings

### 3.2 Detailed Findings
- proposal or planning issues
- code issues with file references
- testing gaps
- documentation drift

### 3.3 Recommendations
- concrete remediation items that could become future beads
- clear separation between:
  - immediate follow-up work
  - deferred future-epic work

## 4. Beads Integration
- Record remediation suggestions in the audit document itself unless the human explicitly asks for direct Beads creation.
- When proposing follow-up work, reference the Beads issue tracker in `.beads/issues.jsonl` and existing bead IDs where relevant.
- Do not assume a separate `task_database.md` exists in this repo.
