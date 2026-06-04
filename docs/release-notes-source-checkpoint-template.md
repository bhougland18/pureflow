# Source Checkpoint Release Notes Template

Use this template for a reviewed repository handoff such as the current
`0.1.0`-style source checkpoint. Replace bracketed placeholders before tagging,
archiving, or sending a handoff.

## Release

- Version: `[workspace version, for example 0.1.0]`
- Date: `[YYYY-MM-DD]`
- Commit or JJ change: `[revision]`
- Release type: source checkpoint / internal demo handoff

## Summary

`[One or two paragraphs describing why this checkpoint exists and what audience
should use it.]`

## Capabilities

Summarize the user-facing surfaces present in this checkpoint:

- Validated canonical JSON workflow documents.
- Workflow topology inspection and text explanation through `pureflow inspect`
  and `pureflow explain`.
- Native workflow execution with bounded ports and JSONL metadata.
- Stable `pureflow run --json` summary fields for machine-facing callers.
- Native node executor registry and reusable test helpers.
- Wasmtime Component Model batch node support through manifest-loaded WASM
  components.
- Host-side validation of WASM outputs before downstream graph delivery.
- Fuel limits and cancellation-aware interruption for guest invocation.
- Runnable examples documented in [examples-catalog.md](examples-catalog.md)
  and [workflow-run-guide.md](workflow-run-guide.md).

## Validation Run

Record the validation environment:

- Runner or machine: `[local machine, CI run URL, or runner identifier]`
- Toolchain path: `[Nix devshell, CI workflow, or other exact environment]`
- Date/time: `[timestamp and timezone]`

Record the full gate from [validation-matrix.md](validation-matrix.md):

| Check | Command | Result | Notes |
| --- | --- | --- | --- |
| Format | `cargo fmt --all --check` | `[pass/fail/skipped]` | `[notes]` |
| Workspace compile | `cargo check --workspace --all-targets` | `[pass/fail/skipped]` | `[notes]` |
| Workspace tests | `cargo test --workspace` | `[pass/fail/skipped]` | `[notes]` |
| Strict Clippy | `cargo clippy --workspace --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone` | `[pass/fail/skipped]` | `[notes]` |
| Metadata bench compile | `cargo bench -p pureflow-core --bench metadata_overhead --no-run` | `[pass/fail/skipped]` | `[notes]` |
| Backpressure bench compile | `cargo bench -p pureflow-engine --bench backpressure_capacity --no-run` | `[pass/fail/skipped]` | `[notes]` |
| Dylint | `cargo-dylint-nightly --all` | `[pass/fail/skipped]` | `[notes]` |
| Diff whitespace | `git diff --check` | `[pass/fail/skipped]` | `[notes]` |

If validation used CI, link the workflow run and note whether Dylint was skipped
because local lint packages were unavailable on the runner.

## Artifact Policy

This checkpoint is source-only unless explicitly changed in this section:

- Crates.io publication: none; workspace crates inherit `publish = false`.
- Official binaries: none by default.
- Source archive: `[tag/archive path/checksum, if produced]`
- Package metadata audit:
  [package-metadata-audit-2026-05-09.md](package-metadata-audit-2026-05-09.md)

If CLI binaries are produced, record the target platform, toolchain, exact build
command, artifact filename, and checksum here before handoff.

## Known Deferred Work

Record open work that is intentionally non-blocking for this checkpoint:

- Deferred data-tier and analytics work:
  - `cdt-pyg`: Epic 13: Deferred Data Tier and Analytics
  - `cdt-pyg.1`: arrow-schema-compatibility-plan
  - `cdt-pyg.2`: datafusion-node-crate-spike
  - `cdt-pyg.3`: arrow-copy-latency-benchmarks
- Additional deferred Beads: `[list IDs, titles, and why they do not block]`

## Beads Snapshot

Record the release task state:

```bash
br ready --json
br stats --json
```

- Ready release-blocking work: `[none or list]`
- Open non-blocking work: `[summary]`
- Closed release-prep Beads: `[list IDs]`

## Handoff Notes

- Important docs changed: `[list]`
- Important examples to demo: `[list]`
- Environment assumptions: `[Nix/devshell, WASM target, local lints, etc.]`
- Follow-up owner: `[person/team]`
