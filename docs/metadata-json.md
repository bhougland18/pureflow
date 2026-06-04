# Metadata JSONL and Run Summary JSON

Pureflow emits machine-facing runtime facts in two related JSON surfaces:

- Metadata JSONL: one JSON object per runtime observation.
- CLI run summary JSON: one JSON document describing a completed `pureflow run --json` invocation.

These shapes are intended for automation. Consumers should read fields by name
and tolerate additive fields. Required identifiers use their validated string
forms. Optional fields are represented as `null` when absent.

## Stability Notes

Metadata and run summary JSON intentionally omit wall-clock timestamps,
monotonic durations, process ids, thread ids, hostnames, random run-local
addresses, and raw payload bytes. These facts either make repeated runs
non-reproducible or belong in a tracing/logging surface rather than the stable
metadata stream.

JSON object key order is stable in the current writer for reproducible tests,
but consumers must not rely on object key order. Record ordering follows runtime
observation order and can change when future runtimes add concurrency.

## Metadata JSONL

The CLI writes metadata JSONL with:

```bash
cargo run -p pureflow-cli -- run examples/native-linear-etl.workflow.json /tmp/pureflow.metadata.jsonl
```

Each line has a `record_type` discriminator.

### Execution Context

Execution context records identify the workflow, node, execution attempt, and
cancellation state visible at a node boundary.

```json
{
  "record_type": "execution_context",
  "context": {
    "workflow_id": "flow",
    "node_id": "source",
    "execution": {
      "execution_id": "run-1",
      "attempt": 1
    },
    "cancellation": {
      "state": "active"
    }
  }
}
```

Cancellation uses `"state": "requested"` with a `reason` field when cancellation
has been requested.

### Lifecycle

Lifecycle records describe runtime state transitions for a node.

```json
{
  "record_type": "lifecycle",
  "kind": "node_started",
  "context": {
    "workflow_id": "flow",
    "node_id": "source",
    "execution": {
      "execution_id": "run-1",
      "attempt": 1
    },
    "cancellation": {
      "state": "active"
    }
  }
}
```

Current lifecycle `kind` values are:

- `node_scheduled`
- `node_started`
- `node_completed`
- `node_failed`
- `node_cancelled`

### Message Boundary

Message records describe packet movement at Pureflow-owned port boundaries. They
carry message metadata, not payload bytes.

```json
{
  "record_type": "message",
  "kind": "enqueued",
  "message": {
    "message_id": "cli-source-out-0",
    "workflow_id": "flow",
    "execution": {
      "execution_id": "cli-run-1",
      "attempt": 1
    },
    "route": {
      "source": {
        "node_id": "source",
        "port_id": "out"
      },
      "target": {
        "node_id": "sink",
        "port_id": "in"
      }
    }
  }
}
```

Current message `kind` values are:

- `enqueued`
- `dequeued`
- `dropped`

### Queue Pressure

Queue pressure records describe bounded edge observations at port operations.
They are control-plane diagnostics for capacity, queue depth, and closure.

```json
{
  "record_type": "queue_pressure",
  "kind": "reserve_ready",
  "direction": "output",
  "port_id": "out",
  "context": {
    "workflow_id": "flow",
    "node_id": "source",
    "execution": {
      "execution_id": "cli-run-1",
      "attempt": 1
    },
    "cancellation": {
      "state": "active"
    }
  },
  "connected_edge_count": 1,
  "capacity": 8,
  "queued_count": null
}
```

Current queue pressure `kind` values are:

- `receive_attempted`
- `receive_ready`
- `receive_empty`
- `receive_closed`
- `reserve_attempted`
- `reserve_ready`
- `reserve_full`
- `send_committed`
- `send_dropped`

The `direction` field is `input` or `output`. `capacity` is `null` when there is
no connected bounded edge. `queued_count` is present only when the runtime can
observe queued input packets.

### Error

Error records describe node-level or workflow-level failures using the stable
Pureflow error taxonomy.

```json
{
  "record_type": "error",
  "kind": "workflow_failed",
  "workflow_id": "flow",
  "node_id": null,
  "execution": {
    "execution_id": "cli-run-1",
    "attempt": 1
  },
  "error": {
    "code": "CDT-EXEC-001",
    "message": "CDT-EXEC-001: node execution failed: first failed",
    "visibility": "internal",
    "retry_disposition": "unknown"
  },
  "diagnostic": null
}
```

Current error `kind` values are:

- `node_failed`
- `workflow_failed`

For node failures, `node_id` contains the node identifier. For workflow failures,
`node_id` is `null`.

Deadlock watchdog failures attach a structured diagnostic:

```json
{
  "type": "workflow_deadlock",
  "scheduled_node_count": 2,
  "pending_node_count": 2,
  "completed_node_count": 0,
  "failed_node_count": 0,
  "cancelled_node_count": 0,
  "bounded_edge_count": 2,
  "no_progress_timeout_ms": 1,
  "cycle_policy": "allow_feedback_loops",
  "feedback_loop_startup": "start_all_nodes",
  "feedback_loop_termination": "all_nodes_complete"
}
```

### External Effect

External effect records describe node-observed tool, service, database, or API
effects. They are explicit metadata facts emitted by node/runtime integration
code; Pureflow does not infer them from ordinary message traffic.

```json
{
  "record_type": "external_effect",
  "kind": "external_effect_completed",
  "context": {
    "workflow_id": "flow",
    "node_id": "tool-executor",
    "execution": {
      "execution_id": "cli-run-1",
      "attempt": 1
    },
    "cancellation": {
      "state": "active"
    }
  },
  "effect": "external_effect",
  "operation": "tool_call",
  "target": "get_weather",
  "response_status": "ok"
}
```

Current external effect `kind` values are:

- `external_effect_requested`
- `external_effect_completed`
- `external_effect_failed`

The `effect` field is the declared `EffectCapability` label, such as
`external_effect` or `network_outbound`. `operation` should name the operation
family, and `target` should identify the external target at a stable,
non-secret level. `response_status` is `null` when no stable status is known.
Do not record credentials, request bodies, response bodies, wall-clock
timestamps, or durations in this metadata family.

## CLI Run Summary JSON

The CLI emits run summary JSON with:

```bash
cargo run -p pureflow-cli -- run --json examples/native-linear-etl.workflow.json /tmp/pureflow.metadata.jsonl
```

The command still writes metadata JSONL to the requested path. The JSON summary
is printed to stdout.

```json
{
  "status": "completed",
  "error": null,
  "workflow": {
    "id": "native-linear-etl",
    "node_count": 3,
    "edge_count": 2
  },
  "metadata": {
    "path": "/tmp/pureflow.metadata.jsonl",
    "record_count": 24
  },
  "summary": {
    "terminal_state": "completed",
    "scheduled_node_count": 3,
    "completed_node_count": 3,
    "failed_node_count": 0,
    "cancelled_node_count": 0,
    "pending_node_count": 0,
    "observed_message_count": 0,
    "error_count": 0,
    "first_error": null,
    "deadlock_diagnostic": null
  }
}
```

`status` and `summary.terminal_state` currently use the same values:

- `completed`
- `failed`
- `cancelled`

When the workflow fails, `error` and `summary.first_error` use the same stable
error object shape as metadata error records. When the watchdog detects no
progress, `summary.deadlock_diagnostic` uses the same deadlock diagnostic fields
shown above with an additional `workflow_id` field.

`summary.observed_message_count` is currently reserved and remains `0` until
runner-level message accounting is attached.

## Tracing Is Separate

The CLI can opt into runtime tracing with `CONDUIT_TRACE` or `RUST_LOG`, but
tracing output is not part of the stable metadata JSONL or run summary JSON
contract. Metadata remains reproducible by default; tracing is for interactive
diagnostics.
