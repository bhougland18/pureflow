# Workflow Authoring Examples

This pack gives small workflow shapes to copy while authoring generated or
hand-written Pureflow workflow files. The examples are intentionally compact so
the topology is easy to inspect before adding real node implementations.

Run commands from the repository root.

## Native Fanout

Files:

- `examples/authoring/native-fanout.workflow.json`

Shape:

```text
source.rows -> branch.rows
branch.clean -> sink.clean
branch.audit -> audit-log.audit
```

Useful commands:

```bash
cargo run -p pureflow-cli -- validate examples/authoring/native-fanout.workflow.json
cargo run -p pureflow-cli -- inspect examples/authoring/native-fanout.workflow.json
cargo run -p pureflow-cli -- explain examples/authoring/native-fanout.workflow.json
cargo run -p pureflow-cli -- run examples/authoring/native-fanout.workflow.json /tmp/pureflow-authoring-native-fanout.metadata.jsonl
```

Expected `validate` output:

```text
valid workflow `authoring-native-fanout`
nodes: 4
edges: 3
```

Expected `explain` notes:

- node order includes `source`, `branch`, `sink`, and `audit-log`
- edges include capacities `4`, `2`, and `1`
- execution remains `native-registry`

Expected run summary:

```text
ran workflow `authoring-native-fanout`
nodes: 4
edges: 3
metadata: /tmp/pureflow-authoring-native-fanout.metadata.jsonl
records: 35
```

## Native Join

Files:

- `examples/authoring/native-join.workflow.yaml`

Shape:

```text
left-source.left -> join.left
right-source.right -> join.right
join.joined -> sink.joined
```

Useful commands:

```bash
cargo run -p pureflow-cli -- validate examples/authoring/native-join.workflow.yaml
cargo run -p pureflow-cli -- inspect examples/authoring/native-join.workflow.yaml
cargo run -p pureflow-cli -- explain examples/authoring/native-join.workflow.yaml
cargo run -p pureflow-cli -- run examples/authoring/native-join.workflow.yaml /tmp/pureflow-authoring-native-join.metadata.jsonl
```

Expected `validate` output:

```text
valid workflow `authoring-native-join`
nodes: 4
edges: 3
```

Expected `inspect` notes:

- JSON output contains four nodes and three edges
- the `join` node reports two input ports, `left` and `right`
- passive native contracts report `execution_mode: "native"` and receive/emit
  port capabilities

Expected run summary:

```text
ran workflow `authoring-native-join`
nodes: 4
edges: 3
metadata: /tmp/pureflow-authoring-native-join.metadata.jsonl
records: 35
```

## WASM Uppercase

Files:

- `examples/authoring/wasm-uppercase.workflow.json`
- `examples/authoring/wasm-uppercase.components.json`

Build the guest fixture before validating or running the manifest:

```bash
cargo build \
  --manifest-path crates/pureflow-wasm/fixtures/uppercase-guest/Cargo.toml \
  --target wasm32-wasip2 \
  --release
```

Useful commands:

```bash
cargo run -p pureflow-cli -- validate examples/authoring/wasm-uppercase.workflow.json
cargo run -p pureflow-cli -- validate-manifest \
  --workflow examples/authoring/wasm-uppercase.workflow.json \
  examples/authoring/wasm-uppercase.components.json
cargo run -p pureflow-cli -- explain examples/authoring/wasm-uppercase.workflow.json
cargo run -p pureflow-cli -- run \
  --wasm-components examples/authoring/wasm-uppercase.components.json \
  examples/authoring/wasm-uppercase.workflow.json \
  /tmp/pureflow-authoring-wasm-uppercase.metadata.jsonl
```

Expected `validate` output:

```text
valid workflow `authoring-wasm-uppercase`
nodes: 3
edges: 2
```

Expected `validate-manifest` output after the component exists:

```text
valid manifest `examples/authoring/wasm-uppercase.components.json`
components: 1
workflow: `authoring-wasm-uppercase`
```

Expected run summary:

```text
ran workflow `authoring-wasm-uppercase`
nodes: 3
edges: 2
metadata: /tmp/pureflow-authoring-wasm-uppercase.metadata.jsonl
records: 19
```

The WASM manifest path is resolved relative to
`examples/authoring/wasm-uppercase.components.json`, so it points two
directories up to the fixture build output under `crates/pureflow-wasm`.

## Schema Commands

Generate schemas while building tooling around these files:

```bash
cargo run -p pureflow-cli -- schema workflow
cargo run -p pureflow-cli -- schema wasm-manifest
```

Use `pureflow validate` and `pureflow validate-manifest --workflow` as the final
authoritative checks after schema-assisted editing.
