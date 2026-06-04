# Native Linear ETL Example

This example is a runnable three-node native workflow:

- `source.rows` emits one deterministic packet.
- `transform.rows` drains its input and emits one deterministic packet on `transform.cleaned`.
- `sink.cleaned` drains the transformed packet and completes.

The current CLI native executor is intentionally small. It proves the real
registry, bounded port, lifecycle metadata, and message metadata path; it does
not load user-defined ETL code yet.

Validate, inspect, and explain the topology:

```bash
cargo run -p pureflow-cli -- validate examples/native-linear-etl.workflow.json
cargo run -p pureflow-cli -- inspect examples/native-linear-etl.workflow.json
cargo run -p pureflow-cli -- explain examples/native-linear-etl.workflow.json
```

Run it and write metadata JSONL:

```bash
cargo run -p pureflow-cli -- run examples/native-linear-etl.workflow.json /tmp/pureflow-native-linear-etl.metadata.jsonl
```

Expected run summary:

```text
ran workflow `native-linear-etl`
nodes: 3
edges: 2
metadata: /tmp/pureflow-native-linear-etl.metadata.jsonl
records: 24
```

The metadata file contains 24 records:

- six lifecycle records, one `started` and one `completed` record for each node
- four message boundary records
- 14 queue-pressure records describing reserve, send, receive, and upstream-closure observations

- source output enqueued on `source.rows`
- transform input dequeued from `source.rows`
- transform output enqueued on `transform.cleaned`
- sink input dequeued from `transform.cleaned`

See `../docs/metadata-json.md` for the stable metadata JSONL and
`pureflow run --json` summary shapes.
