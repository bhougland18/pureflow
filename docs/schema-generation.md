# Schema Generation

The CLI exposes machine-readable JSON Schemas for workflow authoring tools,
AI-assisted editors, and manifest generators.

Generate the workflow document schema:

```bash
cargo run -p pureflow-cli -- schema workflow
```

Generate the WASM component manifest schema:

```bash
cargo run -p pureflow-cli -- schema wasm-manifest
```

Redirect output when a tool expects a schema file:

```bash
cargo run -p pureflow-cli -- schema workflow > /tmp/pureflow-workflow.schema.json
cargo run -p pureflow-cli -- schema wasm-manifest > /tmp/pureflow-wasm-manifest.schema.json
```

Schema generation lives in the CLI tooling layer so workflow parsing crates do
not need schema-generation dependencies. The schemas mirror the current Serde
boundary types and are intended for authoring feedback, editor completion, and
early validation. `pureflow validate` and `pureflow validate-manifest` remain the
authoritative validators because they also enforce semantic rules such as
identifier validity, graph connectivity, duplicate manifest nodes, readable
component paths, and workflow-node membership.

## Workflow Schema

`pureflow schema workflow` emits a JSON Schema for workflow documents accepted by
`pureflow validate`, `pureflow inspect`, `pureflow explain`, and `pureflow run`.

The schema represents these current expectations:

- `pureflow_version` is required and must be `"1"`.
- Unknown top-level, node, edge, and endpoint fields are rejected with
  `additionalProperties: false`.
- Workflow, node, and port identifiers are non-empty strings without
  whitespace and at most 256 bytes.
- Edge `capacity` is optional, but when present it must be an integer greater
  than or equal to `1`.

The same schema shape applies to JSON, TOML, and YAML workflow documents after
format decoding. Format-specific syntax is still handled by the parser selected
from the input file extension.

## WASM Manifest Schema

`pureflow schema wasm-manifest` emits a JSON Schema for the manifest passed to
`pureflow validate-manifest` and `pureflow run --wasm-components`.

The schema represents these current expectations:

- The root object requires `components`.
- Unknown root and component-entry fields are rejected with
  `additionalProperties: false`.
- Every component entry requires `node` and `component`.
- `node` follows the same identifier shape as workflow node IDs.
- `component` is a non-empty path string. Relative paths resolve from the
  manifest directory.
- `fuel` is optional and, when present, is an integer greater than or equal to
  `0`.

Manifest schema validation does not prove that component files exist or that
manifest nodes are present in a workflow. Run `validate-manifest`, with
`--workflow` when available, for those checks.

