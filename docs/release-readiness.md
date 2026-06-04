# Release Readiness Checklist

This checklist is for deciding whether the current Pureflow checkout is ready to
tag, package, demo, or hand off as a release candidate. The workspace is still
early-stage and currently has `publish = false`, so "release" means a reviewed
repository state rather than a crates.io publication.

## Versioning And Package Metadata

- [ ] Confirm the intended workspace version in `Cargo.toml`.
  - Current version: `0.1.0`
- [ ] Confirm every crate inherits `version.workspace = true`.
- [ ] Confirm every crate inherits `license.workspace = true`.
  - Current license: `MIT`
- [ ] Confirm every crate inherits `publish.workspace = true`.
  - Current publish policy: `false`
- [ ] Confirm `LICENSE` exists and matches the workspace license field.
- [ ] Decide whether the release is a source-only checkpoint, binary artifact,
  or internal demo handoff.
  - Current artifact intent: source-only repository checkpoint for internal
    demo handoff. No crates.io publication or official binary artifacts are
    produced by default.
- [ ] If producing binaries, document the target platform and exact build
  command used.
- [ ] Review the latest package metadata audit:
  [package-metadata-audit-2026-05-09.md](package-metadata-audit-2026-05-09.md).

## Validation Gate

- [ ] Run the full gate from [validation-matrix.md](validation-matrix.md):

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone
cargo bench -p pureflow-core --bench metadata_overhead --no-run
cargo bench -p pureflow-engine --bench backpressure_capacity --no-run
cargo-dylint-nightly --all
git diff --check
```

- [ ] Record any warnings or skipped checks in the release notes.
- [ ] Capture validation results in
  [release-notes-source-checkpoint-template.md](release-notes-source-checkpoint-template.md).
- [ ] If Dylint prints local-lint package discovery warnings, confirm the actual
  Rust lint pass still completes.
- [ ] Confirm generated outputs under `target/` and `target/criterion/` are not
  staged.

## CLI Behavior

- [ ] `validate` accepts the native example workflow:

```bash
cargo run -p pureflow-cli -- validate examples/native-linear-etl.workflow.json
```

- [ ] `inspect` emits JSON for the native example workflow:

```bash
cargo run -p pureflow-cli -- inspect examples/native-linear-etl.workflow.json
```

- [ ] `explain` emits topology and metadata policy text:

```bash
cargo run -p pureflow-cli -- explain examples/native-linear-etl.workflow.json
```

- [ ] `run` writes metadata JSONL and reports 24 records for the native example:

```bash
cargo run -p pureflow-cli -- run examples/native-linear-etl.workflow.json /tmp/pureflow-native-linear-etl.metadata.jsonl
```

- [ ] `run --json` prints a completed summary with `error: null`:

```bash
cargo run -p pureflow-cli -- run --json examples/native-linear-etl.workflow.json /tmp/pureflow-native-linear-etl.metadata.jsonl
```

- [ ] `run --wasm-components` is smoke-tested with a manifest-backed component
  workflow when releasing WASM-facing behavior.
- [ ] No-progress watchdog policy remains library-only for this release. The
  CLI rejects cycles under the default acyclic run policy and does not expose a
  watchdog flag until feedback-loop execution policy is also exposed.
- [ ] `PUREFLOW_TRACE` or `RUST_LOG` tracing remains opt-in and writes to stderr,
  not stable metadata JSONL.

## Examples

- [ ] Native linear ETL example output still matches
  [examples-catalog.md](examples-catalog.md).
- [ ] Engine feedback-loop example succeeds:

```bash
cargo run -p pureflow-engine --example feedback_loop
```

- [ ] WASM mixed pipeline example succeeds through the Nix devshell:

```bash
env -u RUSTFLAGS nix develop . --command cargo run -p pureflow-wasm --example mixed_pipeline
```

- [ ] If the ambient shell lacks `wasm32-wasip2`, confirm the docs still direct
  users to the Nix devshell path.

## Metadata And Summary Contracts

- [ ] Metadata JSONL still matches [metadata-json.md](metadata-json.md).
- [ ] `run --json` summary fields remain stable:
  - `status`
  - `error`
  - `workflow`
  - `metadata`
  - `summary`
- [ ] New metadata record fields are additive, or the schema docs are updated in
  the same change.
- [ ] Reproducibility-sensitive fields remain out of metadata JSONL:
  timestamps, process ids, hostnames, thread ids, random addresses, and raw
  payload bytes.

## Benchmarks

- [ ] Metadata benchmark compiles:

```bash
cargo bench -p pureflow-core --bench metadata_overhead --no-run
```

- [ ] Backpressure benchmark compiles:

```bash
cargo bench -p pureflow-engine --bench backpressure_capacity --no-run
```

- [ ] If performance claims are made, run measurement benchmarks and summarize
  Criterion output using [benchmark-operations.md](benchmark-operations.md).
- [ ] Do not commit `target/criterion/`.

## Documentation

- [ ] README describes current capabilities and crate layout.
- [ ] Workflow command guidance is current:
  [workflow-run-guide.md](workflow-run-guide.md).
- [ ] Runnable examples and expected output are current:
  [examples-catalog.md](examples-catalog.md).
- [ ] Validation expectations are current:
  [validation-matrix.md](validation-matrix.md).
- [ ] Latest handoff is current:
  [handoff_2026-05-06.md](handoff_2026-05-06.md).

## Known Deferred Work

These Beads are intentionally deferred and should not block release readiness
unless the release goal explicitly includes analytics/data-tier work:

- `cdt-pyg`: Epic 13: Deferred Data Tier and Analytics
- `cdt-pyg.2`: datafusion-node-crate-spike
- `cdt-pyg.3`: arrow-copy-latency-benchmarks

Deferred scope:

- Arrow schema compatibility expectations are documented in
  [arrow-schema-compatibility-plan-2026-05-11.md](arrow-schema-compatibility-plan-2026-05-11.md).
- Optional DataFusion node crate exploration should not introduce DataFusion
  into core runtime dependencies.
- Arrow copy and latency benchmarks wait until real Arrow workloads exist.

## Beads And Handoff

- [ ] No Beads are unexpectedly `in_progress`.
- [ ] Release-blocking open Beads are either closed or explicitly documented as
  non-blocking.
- [ ] Create release notes from
  [release-notes-source-checkpoint-template.md](release-notes-source-checkpoint-template.md)
  before tagging, archiving, or sending a release handoff.
- [ ] Run `br ready --json` and record remaining ready work.
- [ ] Run `br stats --json` and record open, closed, blocked, and deferred
  counts.
- [ ] Run `br sync --flush-only` after closing release-related Beads.
- [ ] Update or create a handoff if the release candidate changes task state or
  important commands.
