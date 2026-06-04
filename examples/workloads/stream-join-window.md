# Stream Join/Window Workload

This workload exercises `PortsIn::recv_any` with uneven byte-message input
rates. Event packets and profile packets arrive on separate bounded inputs; the
join node buffers by `(window, account)` until both sides of a key have arrived.

Run commands from the repository root.

## Topology

Workflow file:

- `examples/workloads/stream-join-window.workflow.json`

Runnable native executor example:

- `crates/pureflow-engine/examples/stream_join_window.rs`

Shape:

```text
event-source.events -> join-window.events
profile-source.profiles -> join-window.profiles
join-window.joined -> sink.joined
```

Every edge uses `capacity = 1`. The event source sends five packets, while the
profile source sends four packets. The join-window node uses `recv_any` to
consume whichever input is ready, records the observed receive order, emits
three joined rows, then records unmatched window state after both inputs close.

## Validate And Inspect

```bash
cargo run -p pureflow-cli -- validate examples/workloads/stream-join-window.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/stream-join-window.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/stream-join-window.workflow.json
```

Expected `validate` output:

```text
valid workflow `stream-join-window-workload`
nodes: 4
edges: 3
```

Expected `explain` highlights:

```text
workflow `stream-join-window-workload`
status: valid
nodes: 4
edges: 3
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
  - event-source.events -> join-window.events capacity=1
  - profile-source.profiles -> join-window.profiles capacity=1
  - join-window.joined -> sink.joined capacity=1
```

`inspect` should show `join-window` with two input ports, one output port,
native execution mode, and receive/emit port capabilities.

## Run

```bash
cargo run -p pureflow-engine --example stream_join_window
```

Expected output:

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

The receive order is part of the example output because it is the easiest way
to diagnose ordering and closure behavior while `recv_any` is still a small
runtime primitive.

## Metadata Shape

The example writes metadata through `JsonlMetadataSink<Vec<u8>>` and checks
that all expected record families are present:

- `lifecycle`: four started and four completed node records
- `message`: enqueue/dequeue observations for event, profile, and joined
  traffic
- `queue_pressure`: receive-attempt, receive-ready, receive-closed, reserve,
  and send observations across the capacity-one queues

The important workload pressure is a stateful node that receives from two
inputs without choosing a fixed blocking order. It must keep enough buffered
window state to emit matches when either side arrives first, and it must
distinguish closure from an empty momentary queue.

