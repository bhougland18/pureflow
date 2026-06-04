# WASM Uppercase Smoke Path

This walkthrough exercises the full CLI WASM component path:

1. Build the uppercase guest fixture
2. Validate the component manifest
3. Validate and explain the workflow
4. Run the workflow with the WASM component
5. Inspect the metadata output
6. Clean generated artifacts

All commands run from the repo root inside the Nix dev shell:

```bash
nix develop .
```

## Workflow Topology

```
source (native) ──out→in──▶ wasm-upper (WASM) ──out→in──▶ sink (native)
```

`source` and `sink` are handled by the generic native CLI executor. `wasm-upper`
runs through `WasmtimeBatchComponent` backed by the compiled uppercase guest
fixture. Packets flow across real bounded Pureflow port channels on every edge.

The workflow document is `examples/wasm-uppercase.workflow.json`. The component
manifest is `examples/wasm-uppercase.components.json`.

## Step 1 — Build the Uppercase Guest

The `wasm32-wasip2` component must be compiled before the CLI can load it. From
inside the dev shell:

```bash
cargo build \
  --manifest-path crates/pureflow-wasm/fixtures/uppercase-guest/Cargo.toml \
  --target wasm32-wasip2 \
  --release
```

The compiled component is written to:

```text
crates/pureflow-wasm/fixtures/uppercase-guest/target/wasm32-wasip2/release/pureflow_wasm_uppercase_guest_fixture.wasm
```

This path is what `examples/wasm-uppercase.components.json` references (resolved
relative to the manifest file, so `../crates/pureflow-wasm/...`).

## Step 2 — Validate the Component Manifest

Use `validate-manifest` with the workflow to verify the manifest is well-formed
and that every declared component node exists in the workflow:

```bash
cargo run -p pureflow-cli -- validate-manifest \
  --workflow examples/wasm-uppercase.workflow.json \
  examples/wasm-uppercase.components.json
```

Expected output:

```text
valid manifest `examples/wasm-uppercase.components.json`
components: 1
workflow: `wasm-uppercase`
```

`validate-manifest` checks: JSON is well-formed, no unknown fields, all node IDs
are valid Pureflow identifiers, no duplicate node entries, all declared component
paths are readable on disk, and (with `--workflow`) every manifest node exists
in the workflow graph. It does not load or execute the WASM component.

If the component file is missing (step 1 not run), the command fails with:

```text
error: invalid WASM component manifest: component path `...uppercase_guest_fixture.wasm` for node `wasm-upper` is not readable
```

## Step 3 — Validate the Workflow

```bash
cargo run -p pureflow-cli -- validate examples/wasm-uppercase.workflow.json
```

Expected output:

```text
valid workflow `wasm-uppercase`
nodes: 3
edges: 2
```

## Step 4 — Explain the Workflow (optional)

```bash
cargo run -p pureflow-cli -- explain examples/wasm-uppercase.workflow.json
```

Expected output:

```text
workflow `wasm-uppercase`
status: valid
nodes: 3
edges: 2
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
node order:
  - source inputs=0 outputs=1
  - wasm-upper inputs=1 outputs=1
  - sink inputs=1 outputs=0
edges:
  - source.out -> wasm-upper.in capacity=8
  - wasm-upper.out -> sink.in capacity=8
```

## Step 5 — Run the Workflow

```bash
cargo run -p pureflow-cli -- run \
  --wasm-components examples/wasm-uppercase.components.json \
  examples/wasm-uppercase.workflow.json \
  /tmp/wasm-uppercase.metadata.jsonl
```

Expected text summary:

```text
ran workflow `wasm-uppercase`
nodes: 3
edges: 2
metadata: /tmp/wasm-uppercase.metadata.jsonl
records: 22
```

For a machine-facing JSON summary instead:

```bash
cargo run -p pureflow-cli -- run \
  --json \
  --wasm-components examples/wasm-uppercase.components.json \
  examples/wasm-uppercase.workflow.json \
  /tmp/wasm-uppercase.metadata.jsonl
```

The JSON output includes `status`, `error`, `workflow`, `metadata`, and `summary`
fields. A successful run has `"status": "completed"` and `"error": null`.

## Step 6 — Inspect the Metadata

Count metadata records by type:

```bash
grep -o '"record_type":"[^"]*"' /tmp/wasm-uppercase.metadata.jsonl | sort | uniq -c
```

Expected (counts may vary by run):

```text
  6 "record_type":"lifecycle"
  4 "record_type":"message"
 12 "record_type":"queue_pressure"
```

Check the lifecycle records to confirm the wasm-upper node completed:

```bash
grep '"record_type":"lifecycle"' /tmp/wasm-uppercase.metadata.jsonl | python3 -m json.tool --no-ensure-ascii | grep -E '"node_id"|"kind"'
```

The WASM node executes through the same lifecycle boundary as native nodes:
`node_started` followed by `node_completed` (or `node_failed` on error).

## Step 7 — Clean Generated Artifacts

The metadata JSONL file is a generated artifact; remove it when done:

```bash
rm /tmp/wasm-uppercase.metadata.jsonl
```

The compiled WASM component is in `target/` (excluded by `.gitignore`). To clean it:

```bash
cargo clean \
  --manifest-path crates/pureflow-wasm/fixtures/uppercase-guest/Cargo.toml \
  --target wasm32-wasip2
```

Or clean the entire fixture build:

```bash
cargo clean \
  --manifest-path crates/pureflow-wasm/fixtures/uppercase-guest/Cargo.toml
```

Do not commit `target/` directories or `.wasm` files. The
`uppercase-guest/.gitignore` excludes both.

## What This Validates

Running this smoke path exercises the real CLI component-loading path:

- `pureflow validate-manifest` parses the manifest, validates node IDs, and
  checks that component paths are readable before any execution
- `pureflow run --wasm-components` calls `WasmtimeBatchComponent::from_component_bytes_with_limits`,
  applies the configured fuel limit, and runs guest invocation through `BatchNodeExecutor`
- WASM outputs pass through the host `PortsOut` validation boundary before
  entering downstream graph edges
- Metadata records are emitted for the WASM node lifecycle identically to
  native nodes

See `crates/pureflow-wasm/fixtures/uppercase-guest/README.md` for the authoring
guide and WIT contract reference.
