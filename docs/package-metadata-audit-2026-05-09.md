# Package Metadata Audit 2026-05-09

This audit records the package metadata state for the current release-readiness
work. The project is still treated as a reviewed source checkpoint and internal
demo handoff, not a crates.io publication.

## Scope

Workspace crates audited:

- `crates/pureflow-cli`
- `crates/pureflow-core`
- `crates/pureflow-contract`
- `crates/pureflow-engine`
- `crates/pureflow-introspection`
- `crates/pureflow-runtime`
- `crates/pureflow-test-kit`
- `crates/pureflow-types`
- `crates/pureflow-wasm`
- `crates/pureflow-workflow`
- `crates/pureflow-workflow-format`

The WASM uppercase guest fixture at
`crates/pureflow-wasm/fixtures/uppercase-guest` is intentionally excluded from
the root workspace. It is a standalone fixture workspace used to build a
Component Model guest. Its package metadata is explicit rather than inherited:
`version = "0.1.0"`, `edition = "2024"`, `license = "MIT"`, and
`publish = false`.

## Findings

All root workspace crates inherit the shared package metadata from
`Cargo.toml`:

- `version.workspace = true`
- `edition.workspace = true`
- `license.workspace = true`
- `publish.workspace = true`

The inherited workspace package values are:

- `version = "0.1.0"`
- `edition = "2024"`
- `license = "MIT"`
- `publish = false`

The repository `LICENSE` file is the MIT license and matches the workspace
`license = "MIT"` field.

## Artifact Intent

The current release artifact intent is source-only: a reviewed repository
checkpoint suitable for internal demo handoff. No crate should be published to
crates.io from this state, and no official binary artifacts are produced by
default.

If a release later includes CLI binaries, the release notes should record the
target platform, toolchain, and exact build command before the artifact is
published or handed off.

## Recorded Gaps

Because every package currently has `publish = false`, public registry metadata
such as per-crate `description`, `repository`, `homepage`, `documentation`, and
`readme` fields is intentionally deferred. Add that metadata before enabling
crates.io publication for any crate.
