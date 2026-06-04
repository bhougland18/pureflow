# Examples Catalog

This catalog lists runnable examples, the command to run each one, the expected
observable output, and the product surface the example exercises.

Run commands from the repository root.

## Authoring Examples Pack

Files:

- `examples/authoring/README.md`
- `examples/authoring/native-fanout.workflow.json`
- `examples/authoring/native-join.workflow.yaml`
- `examples/authoring/wasm-uppercase.workflow.json`
- `examples/authoring/wasm-uppercase.components.json`

The authoring pack provides compact workflow shapes for generated or
hand-written workflow documents:

- native fanout: one source branches to a primary sink and audit sink
- native join: two sources feed a join-style node before a sink
- WASM uppercase: native source and sink with one manifest-loaded WASM node

See [../examples/authoring/README.md](../examples/authoring/README.md) for
validate, inspect, explain, native run, and WASM run snippets with expected
output notes.

## Fan-Out/Fan-In Workload

Files:

- `examples/workloads/fanout-fanin.workflow.json`
- `examples/workloads/fanout-fanin.md`
- `crates/pureflow-engine/examples/fanout_fanin.rs`

Commands:

```bash
cargo run -p pureflow-cli -- validate examples/workloads/fanout-fanin.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/fanout-fanin.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/fanout-fanin.workflow.json
cargo run -p pureflow-engine --example fanout_fanin
```

Expected workload output:

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

Surfaces exercised:

- one output port fan-out to two bounded downstream queues
- one collector input port fan-in from two upstream senders
- capacity-one edge pressure
- native `NodeExecutor` implementations using `PortsIn`/`PortsOut`
- in-memory JSONL metadata shape for lifecycle, message, and queue-pressure
  records

## Stream Join/Window Workload

Files:

- `examples/workloads/stream-join-window.workflow.json`
- `examples/workloads/stream-join-window.md`
- `crates/pureflow-engine/examples/stream_join_window.rs`

Commands:

```bash
cargo run -p pureflow-cli -- validate examples/workloads/stream-join-window.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/stream-join-window.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/stream-join-window.workflow.json
cargo run -p pureflow-engine --example stream_join_window
```

Expected workload output:

```text
stream join/window workflow `stream-join-window-workload` completed
event packets: 5
profile packets: 4
joined rows: 3
joined payloads: joined:w1:alpha:click:gold, joined:w1:beta:open:silver, joined:w2:alpha:checkout:platinum
recv_any order: events:event:w1:alpha:click, profiles:profile:w1:alpha:gold, events:event:w1:beta:open, profiles:profile:w1:beta:silver, events:event:w2:alpha:checkout, profiles:profile:w2:alpha:platinum, events:event:w1:gamma:orphan-event, profiles:profile:w3:delta:orphan-profile, events:event:w4:epsilon:late-orphan, closed
unmatched events: event:w1:gamma:orphan-event, event:w4:epsilon:late-orphan
unmatched profiles: profile:w3:delta:orphan-profile
scheduled nodes: 4
completed nodes: 4
metadata records: 122
metadata lifecycle records: 8
metadata message records: 24
metadata queue_pressure records: 90
```

Surfaces exercised:

- `PortsIn::recv_any` on a two-input stateful native node
- uneven event/profile input rates
- bounded capacity-one queues
- deterministic windowed join output and unmatched window remainders
- metadata useful for diagnosing receive ordering and closure behavior

## Replay/Branch Evaluation Workload

Files:

- `examples/workloads/replay-branch-eval.workflow.json`
- `examples/workloads/replay-branch-eval.md`
- `crates/pureflow-engine/examples/replay_branch_eval.rs`

Commands:

```bash
cargo run -p pureflow-cli -- validate examples/workloads/replay-branch-eval.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/replay-branch-eval.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/replay-branch-eval.workflow.json
cargo run -p pureflow-engine --example replay_branch_eval
```

Expected workload output:

```text
replay/branch eval workflow `replay-branch-eval-workload` completed
source inputs: 3
branch-a outputs: 3
branch-b outputs: 3
  row[0]: tag:alpha | rev:ahpla
  row[1]: tag:beta | rev:ateb
  row[2]: tag:gamma | rev:ammag
scheduled nodes: 4
completed nodes: 4
metadata records: 88
metadata lifecycle records: 8
metadata message records: 21
metadata queue_pressure records: 59
```

Surfaces exercised:

- fan-out from one source output port to two independent downstream edges
- two distinct evaluator input ports drained sequentially (not fan-in)
- deterministic per-branch output comparison
- branch count verification as a regression gate
- metadata attributable to each branch independently

## AI-Call Orchestration Mock Workload

Files:

- `examples/workloads/ai-call-orchestration.workflow.json`
- `examples/workloads/ai-call-orchestration.md`
- `crates/pureflow-engine/examples/ai_call_orchestration.rs`

Commands:

```bash
cargo run -p pureflow-cli -- validate examples/workloads/ai-call-orchestration.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/ai-call-orchestration.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/ai-call-orchestration.workflow.json
cargo run -p pureflow-engine --example ai_call_orchestration
```

Expected workload output:

```text
ai orchestration workflow `ai-call-orchestration-workload` completed
prompt: prompt:what is the weather in sf?
tool call: tool_call:get_weather:SF
tool result: tool_result:get_weather:72F:sunny
final response: response:The weather in SF is 72F and sunny.
scheduled nodes: 5
completed nodes: 5
metadata records: 46
metadata lifecycle records: 10
metadata message records: 8
metadata queue_pressure records: 28
```

Surfaces exercised:

- prompt → LLM → tool-call → tool-result → response single-turn pipeline
- deterministic native mocks for LLM and tool decisions (no network calls)
- colon-delimited typed packet protocol
- five-node linear topology
- capability gap analysis: multi-turn feedback loops and external-effect
  enforcement documented in `ai-call-orchestration.md`

## Watcher Cancellation Workload

Files:

- `examples/workloads/watcher-cancellation.workflow.json`
- `examples/workloads/watcher-cancellation.md`
- `crates/pureflow-engine/examples/watcher_cancellation.rs`

Commands:

```bash
cargo run -p pureflow-cli -- validate examples/workloads/watcher-cancellation.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/watcher-cancellation.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/watcher-cancellation.workflow.json
cargo run -p pureflow-engine --example watcher_cancellation
```

Expected workload output:

```text
watcher cancellation workflow `watcher-cancellation-workload` cancelled as expected
source changes: 4
controller drained changes: 4
watcher observed changes: 4
watcher control messages: shutdown:source-closed:changes=4
watcher recv_any order: changes:change:config.toml, changes:change:routes.yaml, changes:change:secrets.env, changes:change:templates/email.txt, control:shutdown:source-closed:changes=4
terminal state: cancelled
scheduled nodes: 3
completed nodes: 2
cancelled nodes: 1
failed nodes: 0
metadata records: 70
metadata lifecycle records: 6
metadata node_cancelled records: 1
metadata error records: 2
metadata message records: 14
metadata queue_pressure records: 48
```

Surfaces exercised:

- watcher-style native node with a data input and a control input
- cancellation as expected terminal state, not demo failure
- `node_cancelled` lifecycle metadata
- `CDT-CANCEL-001` error metadata
- bounded fan-out from source to watcher and shutdown controller

## Native Linear ETL Workflow

Files:

- `examples/native-linear-etl.workflow.json`
- `examples/native-linear-etl.md`

Commands:

```bash
cargo run -p pureflow-cli -- validate examples/native-linear-etl.workflow.json
cargo run -p pureflow-cli -- inspect examples/native-linear-etl.workflow.json
cargo run -p pureflow-cli -- explain examples/native-linear-etl.workflow.json
cargo run -p pureflow-cli -- run examples/native-linear-etl.workflow.json /tmp/pureflow-native-linear-etl.metadata.jsonl
cargo run -p pureflow-cli -- run --json examples/native-linear-etl.workflow.json /tmp/pureflow-native-linear-etl.metadata.jsonl
```

Expected `validate` output:

```text
valid workflow `native-linear-etl`
nodes: 3
edges: 2
```

Expected `explain` highlights:

```text
workflow `native-linear-etl`
status: valid
nodes: 3
edges: 2
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
  - source.rows -> transform.rows capacity=2
  - transform.cleaned -> sink.cleaned capacity=2
```

Expected text `run` output:

```text
ran workflow `native-linear-etl`
nodes: 3
edges: 2
metadata: /tmp/pureflow-native-linear-etl.metadata.jsonl
records: 24
```

Expected `run --json` summary fields:

```json
{
  "status": "completed",
  "error": null,
  "metadata": {
    "record_count": 24
  },
  "summary": {
    "terminal_state": "completed",
    "scheduled_node_count": 3,
    "completed_node_count": 3,
    "error_count": 0
  }
}
```

Metadata output:

- writes 24 JSONL records to the requested metadata path
- includes lifecycle, message-boundary, and queue-pressure records
- uses stable execution id `cli-run-1`

Surfaces exercised:

- canonical workflow JSON parsing
- workflow validation
- CLI `validate`, `inspect`, `explain`, `run`, and `run --json`
- native executor registry
- bounded graph ports and output validation
- metadata JSONL writer
- run summary JSON

## Engine Feedback Loop Example

File:

- `crates/pureflow-engine/examples/feedback_loop.rs`

Command:

```bash
cargo run -p pureflow-engine --example feedback_loop
```

Expected output:

```text
counter received seed
driver received ack
workflow feedback-loop completed with 2 scheduled nodes and 0 errors
```

Surfaces exercised:

- `WorkflowGraph::with_cycles_allowed`
- explicit `WorkflowRunPolicy::feedback_loops`
- `StaticNodeExecutorRegistry`
- bounded cyclic graph wiring
- async `PortsIn`/`PortsOut` send and receive
- `WorkflowRunSummary` success reporting

## WASM Mixed Pipeline Example

Files:

- `crates/pureflow-wasm/examples/mixed_pipeline.rs`
- `crates/pureflow-wasm/examples/README.md`
- `crates/pureflow-wasm/fixtures/uppercase-guest/`

Command:

```bash
env -u RUSTFLAGS nix develop . --command cargo run -p pureflow-wasm --example mixed_pipeline
```

Expected output:

```text
# no stdout on success
```

The process exits successfully after asserting that the native sink received
`HELLO FROM WASM`.

Important environment note:

- The example builds the uppercase guest fixture for `wasm32-wasip2` during the
  run.
- The ambient shell may fail with `can't find crate for core` if the
  `wasm32-wasip2` target is not installed.
- Use the Nix devshell command above so the Rust target and WASM tools are
  available.

Surfaces exercised:

- real `wasm32-wasip2` guest fixture build
- `WasmtimeBatchComponent`
- `BatchNodeExecutor<WasmtimeBatchComponent>`
- mixed native and WASM executors in one `StaticNodeExecutorRegistry`
- bounded native source -> WASM transform -> native sink graph
- host-owned output validation before WASM packets enter downstream edges

## CLI WASM Component Manifest Smoke Path

The CLI can load WASM component nodes from a manifest. This path is not yet a
checked-in standalone workflow example, but it is the product surface used by
`pureflow run --wasm-components`.

Manifest shape:

```json
{
  "components": [
    {
      "node": "wasm-upper",
      "component": "components/uppercase.wasm",
      "fuel": 100000000
    }
  ]
}
```

Command shape:

```bash
cargo run -p pureflow-cli -- run \
  --wasm-components wasm-components.json \
  workflow.json \
  /tmp/pureflow.metadata.jsonl
```

Expected output shape:

```text
ran workflow `<workflow-id>`
nodes: <node-count>
edges: <edge-count>
metadata: /tmp/pureflow.metadata.jsonl
records: <record-count>
```

Surfaces exercised:

- CLI WASM component manifest parsing
- component path resolution relative to manifest location
- per-component Wasmtime fuel limit selection
- mixed native/WASM executor registry construction
- CLI metadata JSONL and run summary surfaces

## Related Docs

- [workflow-run-guide.md](workflow-run-guide.md)
- [metadata-json.md](metadata-json.md)
- [../examples/authoring/README.md](../examples/authoring/README.md)
- [../examples/workloads/fanout-fanin.md](../examples/workloads/fanout-fanin.md)
- [../examples/workloads/stream-join-window.md](../examples/workloads/stream-join-window.md)
- [../examples/workloads/replay-branch-eval.md](../examples/workloads/replay-branch-eval.md)
- [../examples/workloads/ai-call-orchestration.md](../examples/workloads/ai-call-orchestration.md)
- [../examples/workloads/watcher-cancellation.md](../examples/workloads/watcher-cancellation.md)
- [../examples/native-linear-etl.md](../examples/native-linear-etl.md)
- [../crates/pureflow-wasm/examples/README.md](../crates/pureflow-wasm/examples/README.md)
