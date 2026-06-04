# Fan-Out/Fan-In Workload

This workload exercises a concrete bounded fan-out/fan-in graph outside unit
tests. It sends three byte rows through one splitter output that fans out to two
native enrichment nodes, then fans both branches back into one collector input.

Run commands from the repository root.

## Topology

Workflow file:

- `examples/workloads/fanout-fanin.workflow.json`

Runnable native executor example:

- `crates/pureflow-engine/examples/fanout_fanin.rs`

Shape:

```text
source.rows -> splitter.rows
splitter.row -> left-enrich.row
splitter.row -> right-enrich.row
left-enrich.enriched -> collector.enriched
right-enrich.enriched -> collector.enriched
```

Every edge uses `capacity = 1` so the run exercises bounded delivery,
fan-out reservation across two downstream edges, and fan-in collection from two
upstream senders.

## Validate And Inspect

```bash
cargo run -p pureflow-cli -- validate examples/workloads/fanout-fanin.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/fanout-fanin.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/fanout-fanin.workflow.json
```

Expected `validate` output:

```text
valid workflow `fanout-fanin-workload`
nodes: 5
edges: 5
```

Expected `explain` highlights:

```text
workflow `fanout-fanin-workload`
status: valid
nodes: 5
edges: 5
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
  - splitter.row -> left-enrich.row capacity=1
  - splitter.row -> right-enrich.row capacity=1
  - left-enrich.enriched -> collector.enriched capacity=1
  - right-enrich.enriched -> collector.enriched capacity=1
```

`inspect` should show `execution_mode: "native"` for every node, one output
port on `splitter`, and one input port on `collector` fed by two upstream edges.

## Run

```bash
cargo run -p pureflow-engine --example fanout_fanin
```

Expected output:

```text
fanout/fanin workflow `fanout-fanin-workload` completed
source rows: 3
collector rows: 6
collector payloads: left:alpha, left:beta, left:gamma, right:alpha, right:beta, right:gamma
scheduled nodes: 5
completed nodes: 5
metadata records: 127
metadata lifecycle records: 10
metadata message records: 27
metadata queue_pressure records: 90
```

The exact metadata counts are useful as a regression signal for the current
runtime. If metadata emission changes intentionally, update this page and the
example output together.

## Metadata Shape

The example writes metadata through `JsonlMetadataSink<Vec<u8>>` and checks
that the run emitted all three expected record families:

- `lifecycle`: five started and five completed node records
- `message`: enqueue/dequeue observations for source, split, branch, and
  collector traffic
- `queue_pressure`: capacity and closure observations on every bounded edge

The important workload pressure is not the row content; it is the combination
of one output port delivering to two downstream queues, one input port draining
from two upstream queues, and capacity-one edges making queue-pressure metadata
visible.

