# Validation Matrix

This project has both a documented validation matrix and a checked-in CI
workflow at `.github/workflows/ci.yml`. Run commands from the repository root.

Use the Nix devshell for the full gate so the expected Rust toolchain, Dylint
driver, WASM target, and project wrappers are available:

```bash
nix develop .
```

## Full Gate

Run this set before calling a code bead complete:

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

The CI workflow runs the same required format, compile, test, Clippy, benchmark
compile, and diff-whitespace checks through the Nix devshell.

Dylint remains part of the full local gate. The CI workflow includes the Dylint
step, but skips it with a notice when the local lint packages referenced by
`workspace.metadata.dylint` are not present on the runner.

Equivalent Dylint invocation from outside an entered shell:

```bash
nix develop . --command cargo-dylint-nightly --all
```

## Matrix

| Check | Command | Purpose | Required For |
| --- | --- | --- | --- |
| Format | `cargo fmt --all --check` | Verifies Rust formatting without editing files. | Any Rust code change |
| Workspace compile | `cargo check --workspace --all-targets` | Compiles all crates, tests, examples, benches, and binaries. | Any Rust code change |
| Workspace tests | `cargo test --workspace` | Runs unit, doc, and integration-style tests in workspace crates. | Behavioral changes |
| Strict Clippy | `cargo clippy --workspace --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone` | Enforces the strict lint profile used during development. | Any Rust code change |
| Dylint | `cargo-dylint-nightly --all` | Runs local project lints from `workspace.metadata.dylint`. | Any Rust code change |
| Metadata bench compile | `cargo bench -p pureflow-core --bench metadata_overhead --no-run` | Ensures the metadata Criterion benchmark builds. | Metadata/core changes |
| Backpressure bench compile | `cargo bench -p pureflow-engine --bench backpressure_capacity --no-run` | Ensures the engine Criterion benchmark builds. | Engine/runtime changes |
| Diff whitespace | `git diff --check` | Catches trailing whitespace and conflict markers. | Any change |

## Targeted Checks

Use targeted checks while iterating, then run the full relevant gate before
closing the bead.

CLI changes:

```bash
cargo check -p pureflow-cli --all-targets
cargo test -p pureflow-cli
cargo clippy -p pureflow-cli --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone
```

Engine changes:

```bash
cargo check -p pureflow-engine --all-targets
cargo test -p pureflow-engine
cargo clippy -p pureflow-engine --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone
cargo bench -p pureflow-engine --bench backpressure_capacity --no-run
```

WASM changes:

```bash
cargo check -p pureflow-wasm --all-targets
cargo test -p pureflow-wasm
cargo clippy -p pureflow-wasm --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone
```

Metadata/core changes:

```bash
cargo check -p pureflow-core --all-targets
cargo test -p pureflow-core
cargo clippy -p pureflow-core --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone
cargo bench -p pureflow-core --bench metadata_overhead --no-run
```

Documentation-only changes:

```bash
git diff --check
```

If the documentation change updates commands or expected output, run the
documented command being changed.

## CI Workflow

The checked-in GitHub Actions workflow is:

- `.github/workflows/ci.yml`

It runs on every push and pull request. The workflow installs Nix, enters the
project devshell for each Rust validation command, and keeps the same strict
Clippy profile documented above.

Benchmark jobs are compile checks only. They do not commit Criterion output.

## Justfile Shortcuts

The `justfile` provides convenience wrappers:

```bash
just fmt
just check
just test
just dylint-all
```

These shortcuts are useful while iterating, but the full gate and CI workflow
above are the source of truth for release hygiene. In particular, `just check`
currently runs `cargo check --workspace`, while the full gate uses
`cargo check --workspace --all-targets`.

## Benchmark Compile Checks

Benchmark compile checks are part of the matrix because benches exercise code
paths that normal tests may not instantiate:

```bash
cargo bench -p pureflow-core --bench metadata_overhead --no-run
cargo bench -p pureflow-engine --bench backpressure_capacity --no-run
```

Use measurement runs only when evaluating performance:

```bash
cargo bench -p pureflow-core --bench metadata_overhead
cargo bench -p pureflow-engine --bench backpressure_capacity
```

See [benchmark-operations.md](benchmark-operations.md) for benchmark meaning and
comparison guidance.

## Known Environment Requirements

- `cargo-dylint-nightly` is expected to come from the devshell.
- WASM examples that build `wasm32-wasip2` guests should run through the Nix
  devshell.
- Dylint may print warnings about missing local lint package directories before
  running the configured lints; treat emitted Rust warnings as actionable.
- Generated benchmark output under `target/criterion/` is not committed.
