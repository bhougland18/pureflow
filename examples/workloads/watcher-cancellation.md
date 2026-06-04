# Watcher Cancellation Workload

This workload models a long-running watcher whose normal terminal path is
cancellation. A change source fans out file-change byte messages to both the
watcher and a shutdown controller. The controller drains the same change stream
and sends one control message after source closure. The watcher receives changes
and control with `recv_any`; when it sees the control message, it returns
`PureflowError::cancelled`.

Run commands from the repository root.

## Topology

Workflow file:

- `examples/workloads/watcher-cancellation.workflow.json`

Runnable native executor example:

- `crates/pureflow-engine/examples/watcher_cancellation.rs`

Shape:

```text
change-source.changes -> watcher.changes
change-source.changes -> shutdown-controller.changes
shutdown-controller.control -> watcher.control
```

Every edge uses `capacity = 1`. The watcher is expected to observe all four
change messages before the shutdown control packet. Cancellation is the desired
result because the watcher has been asked to stop cleanly.

## Validate And Inspect

```bash
cargo run -p pureflow-cli -- validate examples/workloads/watcher-cancellation.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/watcher-cancellation.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/watcher-cancellation.workflow.json
```

Expected `validate` output:

```text
valid workflow `watcher-cancellation-workload`
nodes: 3
edges: 3
```

Expected `explain` highlights:

```text
workflow `watcher-cancellation-workload`
status: valid
nodes: 3
edges: 3
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
  - change-source.changes -> watcher.changes capacity=1
  - change-source.changes -> shutdown-controller.changes capacity=1
  - shutdown-controller.control -> watcher.control capacity=1
```

## Run

```bash
cargo run -p pureflow-engine --example watcher_cancellation
```

Expected output:

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

The process exits successfully. The example treats cancellation as a clean
watcher shutdown, not a failed demo run. The workflow summary is cancelled
because the watcher returns `CDT-CANCEL-001`, while the source and controller
complete normally.

## Metadata Shape

The important metadata signal is the `node_cancelled` lifecycle record for the
watcher. The run also records error metadata with `CDT-CANCEL-001` so callers can
distinguish an expected cancellation from a completed run.

Expected record families:

- `lifecycle`: started/completed for source and controller, started/cancelled
  for watcher
- `error`: watcher cancellation plus workflow cancellation summary
- `message`: change and control packet boundary observations
- `queue_pressure`: capacity and closure observations on every bounded edge

Use this workload when evaluating watcher-style tasks, file monitors, control
ports, or other nodes where shutdown-by-cancellation is a normal operational
path.
