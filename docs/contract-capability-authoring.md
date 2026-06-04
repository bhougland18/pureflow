# Contract And Capability Authoring

This guide explains how to map a workflow node to Pureflow contracts and
capability descriptors. Use it when adding native node helpers, WASM component
nodes, or inspection metadata that should line up with the workflow topology.

Contracts answer what a node declares about its interface and execution shape.
Capabilities answer what runtime actions the node may perform. Validation keeps
both aligned with the workflow graph.

## Authoring Model

For each workflow node, author three matching pieces:

1. Workflow topology: node id plus input and output port ids.
2. `NodeContract`: port contracts, schema refs, execution mode, determinism,
   and retry declaration.
3. `NodeCapabilities`: receive/emit claims for the same ports plus any host
   effects the node may request.

The validator intentionally checks these together:

- every workflow node needs one contract and one capability descriptor
- every contract port must exist on the workflow node with the same direction
- every capability port must exist on the workflow node with the matching
  receive/emit direction
- schemas on connected edge endpoints must match when both sides declare schema
  refs
- strict execution boundaries reject host effects that Pureflow cannot enforce

`SchemaRef` is opaque. Today, compatibility is exact equality. Prefer stable,
versioned names such as `schema://text-bytes/v1` or
`schema://customer-event/v2` rather than informal labels.

## Native Versus WASM Enforcement

Native node capabilities are advisory metadata. A native executor is host code,
so `EffectCapability` entries describe expected behavior for review,
inspection, and future policy, but they are not a sandbox.

WASM contracts are strict. A `NodeContract` with `ExecutionMode::Wasm` is
validated as an enforceable boundary. Current WASM guest components import no
host capabilities, so declaring effects such as filesystem, network, process,
environment, or clock access is rejected. Pure computation with receive/emit
port capabilities is the supported shape.

Use this rule of thumb:

| Execution mode | Effects today | Meaning |
| --- | --- | --- |
| `ExecutionMode::Native` | allowed | advisory declaration for host code |
| `ExecutionMode::Wasm` | rejected | strict boundary has no host-effect imports |
| `ExecutionMode::Process` | rejected | reserved for a future process adapter |

Common effect capabilities include filesystem read/write, outbound network,
process spawn, environment read/write, clock access, and the generic
`ExternalEffect` marker for tool, service, database, or API effects that are not
captured precisely by a lower-level capability. Use the most specific
capability that communicates the node's behavior; for AI tool orchestration,
`ExternalEffect` is the stable declaration for "this node may perform an
external tool call."

## Field Guidance

`PortContract`:

- use `PortDirection::Input` for workflow input ports
- use `PortDirection::Output` for workflow output ports
- attach the same `SchemaRef` to both ends of an edge when the packet format is
  known
- use `None` only when the schema is intentionally unknown

`ExecutionMode`:

- `Native` for host Rust executors
- `Wasm` for `WasmtimeBatchComponent` nodes
- `Process` is future-facing and currently has no effect enforcement adapter

`Determinism`:

- `Deterministic` when identical inputs and execution metadata should produce
  identical outputs
- `NonDeterministic` when behavior depends on external state or time
- `Unknown` when the author has not made a claim

`RetryDisposition`:

- `Never` for validation failures, malformed input, or non-idempotent side
  effects
- `Safe` when retrying the node should not duplicate external effects
- `Unknown` when retry safety has not been established

`NodeCapabilities`:

- declare `PortCapabilityDirection::Receive` for each workflow input port
- declare `PortCapabilityDirection::Emit` for each workflow output port
- use `NodeCapabilities::native_passive` when a node only receives/emits packets
  and has no external host effects
- use `NodeCapabilities::new` when a native node needs advisory effect metadata

## Complete WASM Uppercase Example

This example matches the runnable workflow in
[`examples/wasm-uppercase.workflow.json`](../examples/wasm-uppercase.workflow.json):

```text
source (native) -> wasm-upper (WASM) -> sink (native)
```

All packets use the same byte-payload schema:

```rust
use pureflow_contract::{
    Determinism, ExecutionMode, NodeContract, PortContract, SchemaRef,
    validate_workflow_contracts,
};
use pureflow_core::{
    RetryDisposition,
    capability::{NodeCapabilities, PortCapability, PortCapabilityDirection},
};
use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, node_id, port_id};
use pureflow_workflow::PortDirection;

let packet_schema = SchemaRef::new("schema://uppercase-bytes/v1")?;

let workflow = WorkflowBuilder::new("wasm-uppercase")
    .node(NodeBuilder::new("source").output("out").build())
    .node(NodeBuilder::new("wasm-upper").input("in").output("out").build())
    .node(NodeBuilder::new("sink").input("in").build())
    .edge("source", "out", "wasm-upper", "in")
    .edge("wasm-upper", "out", "sink", "in")
    .build();

let contracts = vec![
    NodeContract::new(
        node_id("source"),
        [PortContract::new(
            port_id("out"),
            PortDirection::Output,
            Some(packet_schema.clone()),
        )],
        ExecutionMode::Native,
        Determinism::Deterministic,
        RetryDisposition::Safe,
    )?,
    NodeContract::new(
        node_id("wasm-upper"),
        [
            PortContract::new(
                port_id("in"),
                PortDirection::Input,
                Some(packet_schema.clone()),
            ),
            PortContract::new(
                port_id("out"),
                PortDirection::Output,
                Some(packet_schema.clone()),
            ),
        ],
        ExecutionMode::Wasm,
        Determinism::Deterministic,
        RetryDisposition::Never,
    )?,
    NodeContract::new(
        node_id("sink"),
        [PortContract::new(
            port_id("in"),
            PortDirection::Input,
            Some(packet_schema),
        )],
        ExecutionMode::Native,
        Determinism::Deterministic,
        RetryDisposition::Safe,
    )?,
];

let capabilities = vec![
    NodeCapabilities::native_passive(
        node_id("source"),
        [PortCapability::new(port_id("out"), PortCapabilityDirection::Emit)],
    )?,
    NodeCapabilities::native_passive(
        node_id("wasm-upper"),
        [
            PortCapability::new(port_id("in"), PortCapabilityDirection::Receive),
            PortCapability::new(port_id("out"), PortCapabilityDirection::Emit),
        ],
    )?,
    NodeCapabilities::native_passive(
        node_id("sink"),
        [PortCapability::new(port_id("in"), PortCapabilityDirection::Receive)],
    )?,
];

validate_workflow_contracts(&workflow, &contracts, &capabilities)?;
```

The `wasm-upper` node is pure: it receives bytes, emits bytes, and declares no
effects. That is why the same passive capability helper is valid for both
native nodes and the WASM node. If `wasm-upper` declared
`EffectCapability::FileSystemRead`, validation would reject it because the
current WASM boundary cannot enforce that host effect.

## Native Node With Advisory Effects

For native host code, effect capabilities are still useful documentation. A
native source that reads from a file might declare:

```rust
use pureflow_core::capability::{
    EffectCapability, NodeCapabilities, PortCapability, PortCapabilityDirection,
};
use pureflow_test_kit::{node_id, port_id};

let source_capabilities = NodeCapabilities::new(
    node_id("source"),
    [PortCapability::new(port_id("out"), PortCapabilityDirection::Emit)],
    [EffectCapability::FileSystemRead],
)?;
```

This descriptor says reviewers and inspection tools should expect filesystem
reads from the native source. It does not sandbox that source. Keep native
effect declarations conservative and specific.

A native AI tool executor that calls a tool service can declare:

```rust
use pureflow_core::capability::{
    EffectCapability, NodeCapabilities, PortCapability, PortCapabilityDirection,
};
use pureflow_test_kit::{node_id, port_id};

let tool_capabilities = NodeCapabilities::new(
    node_id("tool-executor"),
    [
        PortCapability::new(port_id("call"), PortCapabilityDirection::Receive),
        PortCapability::new(port_id("result"), PortCapabilityDirection::Emit),
    ],
    [EffectCapability::ExternalEffect],
)?;
```

When the node performs a tool call, node or host integration code can emit
`ExternalEffectMetadataRecord` values so metadata consumers can distinguish
effect observations from message movement and queue pressure.

## Common Validation Failures

| Failure | Cause | Fix |
| --- | --- | --- |
| `EmptySchemaRef` | schema string is empty or whitespace | use a stable non-empty schema ref |
| `DuplicatePortContract` | same port appears twice in one contract | declare each workflow port once |
| `UnknownWorkflowPort` | contract references a port absent from the workflow node | match the workflow topology |
| `PortDirectionMismatch` | contract says input but workflow says output, or the reverse | use the workflow port direction |
| `CapabilityDirectionMismatch` | capability says receive for an output port, or emit for an input port | map inputs to receive and outputs to emit |
| `SchemaMismatch` | connected ports declare different schema refs | use the same schema ref on both edge endpoints |
| `UnenforceableEffectCapability` | WASM or process contract declares host effects | remove effects or use native execution |

## Where This Fits

- Use [`workflow-run-guide.md`](workflow-run-guide.md) for CLI execution and
  metadata interpretation.
- Use [`node-authoring-error-patterns.md`](node-authoring-error-patterns.md)
  for native and WASM error, cancellation, and retry guidance.
- Use [`../examples/wasm-uppercase.md`](../examples/wasm-uppercase.md) for the
  full template-to-run WASM smoke path.
- Use
  [`../crates/pureflow-wasm/fixtures/uppercase-guest/README.md`](../crates/pureflow-wasm/fixtures/uppercase-guest/README.md)
  when authoring a new WASM guest component.
