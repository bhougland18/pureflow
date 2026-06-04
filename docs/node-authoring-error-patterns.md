# Node Authoring Error Patterns

This guide captures recommended error patterns for native and WASM node
authors. The goal is consistent behavior across node implementations and stable
diagnostics in metadata JSONL and CLI `run --json` summaries.

## Error Taxonomy

Pureflow exposes one shared runtime-facing error type: `PureflowError`. Node
authors most often use:

| Condition | Native return | Stable code | Visibility | Retry |
| --- | --- | --- | --- | --- |
| malformed node input during execution | `PureflowError::execution(...)` | `CDT-EXEC-001` | `internal` | `unknown` |
| runtime cancellation observed | propagate cancellation from port operations or return `PureflowError::cancelled(...)` | `CDT-CANCEL-001` | `user` | `safe` |
| metadata sink failure | returned by runtime metadata path | `CDT-META-001` | `internal` | `unknown` |
| lifecycle observer failure | returned by runtime observer path | `CDT-LIFE-001` | `internal` | `unknown` |
| invalid static identifiers/capabilities | validation layer, not normal node code | `CDT-VAL-*` | `user` | `never` |

Node contract `RetryDisposition` is an author declaration. Runtime error
`retry_disposition` is derived from the emitted `PureflowError`. Keep both in
sync conceptually: a contract that declares `RetryDisposition::Never` should
not intentionally surface transient errors as if retrying were safe.

## Native Node Pattern

Native nodes should preserve cancellation and port errors by using the Pureflow
port APIs directly and propagating `?`:

```rust
use pureflow_core::{PureflowError, NodeExecutor, PacketPayload, PortsIn, PortsOut, Result};
use pureflow_core::context::NodeContext;
use pureflow_test_kit::port_id;
use futures::future::BoxFuture;

struct UppercaseNative;

impl NodeExecutor for UppercaseNative {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(
        &self,
        ctx: NodeContext,
        mut inputs: PortsIn,
        outputs: PortsOut,
    ) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();

            while let Some(packet) = inputs.recv(&port_id("in"), &cancellation).await? {
                let Some(bytes) = packet.payload().as_bytes() else {
                    return Err(PureflowError::execution(
                        "uppercase node expected bytes payload on input `in`",
                    ));
                };

                let uppercased: Vec<u8> = bytes.iter().map(u8::to_ascii_uppercase).collect();
                let output = packet.map_payload(|_payload| PacketPayload::from(uppercased));
                outputs.send(&port_id("out"), output, &cancellation).await?;
            }

            Ok(())
        })
    }
}
```

The important parts are:

- take `ctx.cancellation_token()` once and pass it to every async receive/send
- use `?` on `inputs.recv` and `outputs.send` so cancellation remains
  `CDT-CANCEL-001` instead of being hidden as execution failure
- return `PureflowError::execution(...)` for malformed runtime payloads or node
  logic failures
- include node-local context in the message: port id, expected payload kind, or
  rejected field name

Do not catch cancellation and reword it as a generic execution error. Operators
need cancellation to remain visible as `visibility: "user"` and
`retry_disposition: "safe"`.

## WASM Guest Pattern

WASM guests do not construct `PureflowError` directly. They return the WIT
`batch-error` variant from `pureflow:batch@0.1.0`.

Use:

- `batch-error::unsupported-payload(string)` when the guest receives a payload
  variant it does not support
- `batch-error::guest-failure(string)` for internal guest failures

The uppercase fixture uses this pattern:

```rust
let Payload::Bytes(bytes) = packet.payload else {
    return Err(BatchError::UnsupportedPayload(
        String::from("uppercase guest accepts only bytes payloads"),
    ));
};
```

The host maps guest errors to `PureflowError::execution`, so they appear as:

- `code: "CDT-EXEC-001"`
- `visibility: "internal"`
- `retry_disposition: "unknown"`

WASM cancellation is host-managed. If cancellation is already requested or is
observed while Wasmtime is running the component, the adapter returns
`CDT-CANCEL-001`. Guest code should not invent its own cancellation protocol
inside payloads unless that protocol is part of the node's business contract.

## Malformed Payloads

Malformed payloads are runtime execution failures because the workflow and
contract layers have already accepted the graph. Handle them consistently:

| Case | Native behavior | WASM behavior |
| --- | --- | --- |
| unsupported payload variant | `PureflowError::execution("node expected bytes payload on input `in`")` | `BatchError::UnsupportedPayload("node accepts only bytes payloads")` |
| invalid bytes encoding | `PureflowError::execution("node expected UTF-8 bytes on input `in`")` | `BatchError::GuestFailure("node expected UTF-8 bytes")` |
| missing required control field | `PureflowError::execution("control payload missing field `name`")` | `BatchError::GuestFailure("control payload missing field `name`")` |
| output packet cannot be sent | propagate `outputs.send(...).await?` | return output batches; host `PortsOut` validation reports the send failure |

Prefer short, stable messages. They become part of metadata and CLI summaries.

## Metadata JSONL Shape

When a node fails, metadata includes a node-level error record:

```json
{
  "record_type": "error",
  "kind": "node_failed",
  "workflow_id": "wasm-uppercase",
  "node_id": "wasm-upper",
  "execution": {
    "execution_id": "cli-run-1",
    "attempt": 1
  },
  "error": {
    "code": "CDT-EXEC-001",
    "message": "CDT-EXEC-001: node execution failed: uppercase guest accepts only bytes payloads",
    "visibility": "internal",
    "retry_disposition": "unknown"
  },
  "diagnostic": null
}
```

The workflow supervisor also records the first terminal workflow error:

```json
{
  "record_type": "error",
  "kind": "workflow_failed",
  "workflow_id": "wasm-uppercase",
  "node_id": null,
  "execution": {
    "execution_id": "cli-run-1",
    "attempt": 1
  },
  "error": {
    "code": "CDT-EXEC-001",
    "message": "CDT-EXEC-001: node execution failed: uppercase guest accepts only bytes payloads",
    "visibility": "internal",
    "retry_disposition": "unknown"
  },
  "diagnostic": null
}
```

For cancellation, the same records use `CDT-CANCEL-001`, `visibility: "user"`,
and `retry_disposition: "safe"`.

## `run --json` Shape

The CLI summary repeats the first workflow error in both top-level `error` and
`summary.first_error`:

```json
{
  "status": "failed",
  "error": {
    "code": "CDT-EXEC-001",
    "message": "CDT-EXEC-001: node execution failed: uppercase guest accepts only bytes payloads",
    "visibility": "internal",
    "retry_disposition": "unknown"
  },
  "summary": {
    "terminal_state": "failed",
    "failed_node_count": 1,
    "cancelled_node_count": 0,
    "error_count": 1,
    "first_error": {
      "code": "CDT-EXEC-001",
      "message": "CDT-EXEC-001: node execution failed: uppercase guest accepts only bytes payloads",
      "visibility": "internal",
      "retry_disposition": "unknown"
    }
  }
}
```

If a sibling node observes cancellation after the first failure, the summary may
include both failed and cancelled node counts. The top-level `error` still
reports the first workflow error.

## Author Checklist

- Use contracts to declare retry expectations before implementation.
- Preserve cancellation by using `ctx.cancellation_token()` with every async
  port operation.
- Treat malformed runtime payloads as execution failures with concise messages.
- Use WASM `unsupported-payload` for unsupported WIT payload variants.
- Avoid raw substrate errors in public node APIs; let Pureflow adapters map
  port, task, and Wasmtime failures into `PureflowError`.
- Verify failure behavior with metadata JSONL and `run --json`, not just text
  output.

## Related Docs

- [`contract-capability-authoring.md`](contract-capability-authoring.md)
- [`metadata-json.md`](metadata-json.md)
- [`../examples/wasm-uppercase.md`](../examples/wasm-uppercase.md)
- [`../crates/pureflow-wasm/fixtures/uppercase-guest/README.md`](../crates/pureflow-wasm/fixtures/uppercase-guest/README.md)
