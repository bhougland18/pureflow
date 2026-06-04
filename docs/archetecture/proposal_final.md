# Pureflow FBP Engine Final Strategy Proposal

Date: 2026-04-26

## 1. Final Recommendation

Pureflow should continue with the original architecture direction: a
metadata-first, AI-inspectable Flow-Based Programming runtime built on
`asupersync`, with Pureflow-owned workflow, port, node-contract, metadata,
capability, and introspection APIs.

If you are evaluating reuse of this library in `highland-labs`, see
[`highland-labs-integration-proposal.md`](highland-labs-integration-proposal.md)
for the migration strategy and boundary mapping.

The highest-priority correction is execution semantics. The repository has
good boundaries, but `pureflow-engine::run_workflow` is still a sequential
scaffold. It wires bounded channels, then awaits each node to completion before
starting the next node. That is not yet FBP.

The next implementation wave should make Pureflow a true long-lived process
graph:

1. Add async, cancel-safe port APIs.
2. Add a supervised concurrent workflow runner.
3. Make edge capacity explicit.
4. Add cycle policy and topological diagnostics.
5. Add node contracts and external workflow definitions.
6. Strengthen metadata, introspection, and CLI surfaces.
7. Add WASM as a host-owned batch adapter only after the native FBP core works.

`docs/archetecture/additional_considerations.md` is currently empty. The
actionable new considerations came from
`docs/archetecture/strategy/proposal_request.md` and the additional ideas in
`docs/archetecture/proposal_2.md`.

## 2. Useful Additions Adopted From Proposal 2

The second proposal adds several practical ideas that should be incorporated:

| Addition | Decision |
| --- | --- |
| Optimize around concrete workflow shapes | Adopt. These shapes make runtime tradeoffs testable. |
| Async `recv_any` | Adopt. Needed for joins, watchers, control inputs, and multi-input nodes without busy polling. |
| Edge capacity in `EdgeDefinition` | Adopt. The current capacity of `1` is useful for backpressure tests but too restrictive as a default. |
| Topological ordering and explicit cycle policy | Adopt with nuance. FBP can support cycles, but cycles should be rejected by default until explicitly enabled. |
| Identifier length cap | Adopt. It closes a low-cost robustness gap. |
| Separate `pureflow-workflow-format` crate | Adopt. Keeps in-memory graph types free from parser dependencies. |
| Feature hygiene | Adopt. `serde`, `yaml`, `toml`, `arrow`, `wasm`, and `tracing` should be explicit features. |
| Introspection as pure data over workflow + contracts | Adopt. AI tooling should inspect before execution. |
| Benchmark and concurrency test beads | Adopt. Backpressure and metadata overhead need measured guardrails. |

Proposal 2 also contains crate versions that are now stale for several
dependencies. This final proposal keeps the newer checked versions from
`proposal_1` where appropriate and adds newer checked versions for the extra
crates proposal 2 introduced.

## 3. Current Baseline

The current codebase is boundary-complete and execution-light:

| Layer | Current state | Gap |
| --- | --- | --- |
| `pureflow-types` | Validated ID newtypes for workflow, execution, message, node, and port | No length cap |
| `pureflow-workflow` | Deterministic static graph validation for nodes, ports, and edges | No serde, no capacity, no cycle policy, no implementation refs |
| `pureflow-core` | `NodeExecutor`, context, cancellation, message metadata, capabilities, lifecycle, metadata sink, error taxonomy, bounded ports | Ports are only non-blocking; metadata is not emitted at message boundaries |
| `pureflow-runtime` | `AsupersyncRuntime`, one-node execution, lifecycle/metadata observers, cancellation bridge, deterministic test runtime | No workflow supervisor |
| `pureflow-engine` | Wires edges as bounded channels and invokes nodes sequentially | Not a long-lived FBP process graph |
| `pureflow-cli` | Temporary empty-workflow print scaffold | No validate/run/inspect commands |

The important architectural boundary is already correct: `asupersync` is the
runtime substrate, not the public programming model. That boundary must remain
intact.

## 4. Workflow Shapes To Optimize For

These shapes should drive tests, examples, and performance checks:

| Shape | Why it matters |
| --- | --- |
| Linear ETL | Basic MVP: source -> transform -> sink |
| Fan-out/fan-in | Tests reserve/commit fan-out, bounded queues, and merge behavior |
| AI-call orchestration | Mixes streaming text, structured tool calls, and external effects |
| Stream join/window | Requires `recv_any`, stateful nodes, and uneven input rates |
| Replay/branch evaluation | Uses metadata lineage to compare variants |
| Long-running watcher | Requires cancellation as the primary termination path |
| Feedback loop | Requires explicit cycle opt-in and deadlock diagnostics |

The engine should not optimize for DAG-only completion. It should optimize for
bounded, observable, long-lived message flow.

## 5. Target Architecture

```text
CLI / API / AI Tools
        |
        v
Workflow Format Loader
  - JSON canonical
  - TOML human-authored
  - YAML optional and gated
        |
        v
Validation Pipeline
  - workflow structure
  - edge capacity
  - cycle policy
  - node contract lookup
  - schema compatibility
  - capability policy
        |
        v
Pureflow Engine
  - graph wiring
  - workflow supervisor
  - shared cancellation
  - node task lifecycle
  - failure policy
  - metadata fan-in
        |
        v
Pureflow Runtime Adapter
  - asupersync runtime
  - bounded channel substrate
  - deterministic test runtime
        |
        +--------------------+
        |                    |
        v                    v
Native Node Executor     WASM Batch Executor
trusted/advisory         sandboxed/enforced
```

Runtime supervision:

```text
WorkflowSupervisor
        |
        +--> node task A -- bounded edge --> node task B
        |                                      |
        +--> node task C -- bounded edge ------+
        |
        +--> shared CancellationHandle
        |
        +--> MetadataSink + LifecycleHook
```

Metadata flow:

```text
Execution context ----+
Lifecycle events -----+--> MetadataSink --> memory / JSONL / trace bridge / AI view
Message metadata -----+
Queue pressure -------+
Validation facts -----+
```

## 6. Refined Implementation Strategy

### 6.1 Add Async, Cancel-Safe Ports First

Long-lived nodes cannot be implemented cleanly with only `try_recv` and
`try_send`. Add async APIs to `PortsIn` and `PortsOut` before replacing the
engine runner.

Recommended shape:

```rust
impl PortsIn {
    pub async fn recv(
        &mut self,
        port_id: &PortId,
        cancellation: &CancellationToken,
    ) -> Result<Option<PortPacket>>;

    pub async fn recv_any(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<(PortId, PortPacket)>>;
}

impl PortsOut {
    pub async fn reserve(
        &self,
        port_id: &PortId,
        cancellation: &CancellationToken,
    ) -> Result<PortSendPermit<'_>>;

    pub async fn send(
        &self,
        port_id: &PortId,
        packet: PortPacket,
        cancellation: &CancellationToken,
    ) -> Result<()>;
}
```

Semantics:

- `Ok(Some(packet))` means a packet was received.
- `Ok(None)` means upstream is closed and drained.
- cancellation returns a cancellation error, not `None`.
- reserve/commit remains all-or-nothing across fan-out edges.
- `try_*` methods remain for deterministic tests and polling-style nodes.
- no public method exposes raw `asupersync` channel types.

### 6.2 Replace Sequential Execution With A Workflow Supervisor

Replace the primary `pureflow-engine::run_workflow` semantics with a supervised
concurrent runner:

```rust
pub struct WorkflowRuntime {
    runtime: AsupersyncRuntime,
    policy: WorkflowRunPolicy,
}

pub async fn run_workflow_with_observers<E, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &E,
    hook: &H,
    metadata_sink: &M,
) -> Result<WorkflowRunSummary>
where
    E: NodeExecutorRegistry + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + ?Sized;
```

Supervisor rules:

- wire all edges before starting node tasks
- attach one workflow-level cancellation token to every `NodeContext`
- spawn one task per node
- emit `NodeScheduled`, `NodeStarted`, and terminal lifecycle events
- drop output handles when a node terminates so downstream receives closure
- fail fast for the MVP: first node failure cancels the workflow
- return a summary with terminal state, first error, cancelled nodes, and basic
  counts

The current sequential runner can remain temporarily as a test helper, but it
should not remain the main engine API.

### 6.3 Add Edge Capacity

Move edge capacity into workflow structure:

```rust
pub struct EdgeDefinition {
    source: EdgeEndpoint,
    target: EdgeEndpoint,
    capacity: EdgeCapacity,
}

pub enum EdgeCapacity {
    Default,
    Bounded(NonZeroUsize),
}
```

Initial policy:

- `Default` resolves to `64` for ordinary runs.
- Tests that prove backpressure explicitly use `Bounded(1)`.
- unbounded channels are not offered.
- CLI/introspection prints capacity per edge.

### 6.4 Add Topological Diagnostics And Cycle Policy

FBP can support cycles, but cycles change startup, termination, and deadlock
behavior. Add cycle detection now, reject cycles by default, and provide an
explicit opt-in later.

Recommended changes:

- add `WorkflowGraph::topological_order()`
- add `WorkflowValidationError::CycleDetected`
- use hand-rolled Kahn's algorithm initially; avoid `petgraph` until more graph
  algorithms are needed
- add `WorkflowGraph::with_cycles_allowed(...)` or a graph policy object when
  feedback loops are intentionally supported

### 6.5 Add Node Contracts Before Schema Execution

Keep `pureflow-workflow` focused on topology. Add contract types in a separate
crate, likely `pureflow-contract`:

- `NodeContractId`
- `NodeContract`
- `PortContract`
- `SchemaRef`
- `ExecutionMode`: `Native`, `Wasm`, later `Process`
- declared effect capabilities
- determinism and retry metadata

Validation should prove:

- every workflow node resolves to a known contract
- every workflow port matches a contract port
- edge source schema is compatible with edge target schema
- capability declarations match workflow topology
- WASM/process nodes have enforceable capability policies

This is the right place to connect AI-generated workflows to safe validation.

### 6.6 Add External Workflow Definitions In A Format Crate

Create `crates/pureflow-workflow-format` instead of putting parsers directly in
`pureflow-workflow`.

Format strategy:

- JSON is canonical and always supported by the format crate.
- TOML is supported for human-authored workflows.
- YAML is optional and feature-gated.
- every file includes `pureflow_version = "1"` or equivalent.
- missing/unknown versions produce typed errors.
- raw serde structs parse into validated domain types.

This keeps the core graph crate small and makes parser dependencies optional.

### 6.7 Add Introspection As Pure Data

Add introspection types as pure projections over validated workflow and
contract data:

```rust
pub struct WorkflowIntrospection {
    pub workflow: WorkflowId,
    pub nodes: Vec<NodeIntrospection>,
    pub edges: Vec<EdgeIntrospection>,
}
```

No runtime should be required to inspect:

- nodes
- ports
- edge capacities
- schemas
- capabilities
- enforcement level
- determinism
- declared execution mode

Render JSON behind a `serde` feature for CLI and AI consumers.

### 6.8 Strengthen Metadata At Runtime Boundaries

Extend `MetadataRecord` while preserving source-specific metadata:

- workflow started/completed/failed/cancelled
- node scheduled/started/completed/failed/cancelled
- message enqueued/dequeued/dropped
- edge capacity and queue pressure snapshots
- validation facts
- node contract summaries

Add these sink implementations:

- `InMemoryMetadataSink` for tests and CLI summaries
- `JsonlMetadataSink` for reproducible run logs
- `TieredMetadataSink` to avoid recording large payload bytes
- optional tracing bridge behind a feature

Metadata must remain typed runtime facts. It should not become generic logging.

### 6.9 Tier Payloads Without Pulling Arrow Into The MVP

The current `PortPacket = MessageEnvelope<Vec<u8>>` is enough for the first
channel-backed scaffold, but the next durable shape should avoid excessive
copies.

Recommended intermediate payload:

```rust
pub enum PacketPayload {
    Control(serde_json::Value),
    Bytes(bytes::Bytes),
    Structured(Arc<dyn DataPacket>),
    #[cfg(feature = "arrow")]
    Arrow(arrow::record_batch::RecordBatch),
}
```

Do not add Arrow/DataFusion to core until the byte-message runtime and WASM
boundary are stable.

### 6.10 Define The WASM Boundary Before Picking Runtime Details

The original proposal is right that host-owned channels plus batch-oriented
WASM is the safe MVP model.

Add a runtime-neutral batch trait first:

```rust
pub trait BatchExecutor {
    fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs>;
}
```

Then implement `pureflow-wasm` with Wasmtime later:

- host reads batches from `PortsIn`
- host invokes the WASM component
- host validates output envelopes
- host sends through normal `PortsOut`
- capabilities are strict for WASM, advisory for native nodes

Do not give WASM direct channel access in the MVP.

### 6.11 Add Feature Hygiene

Recommended features:

| Crate | Feature | Adds |
| --- | --- | --- |
| `pureflow-core` | `serde` | serialization for public data/metadata/introspection |
| `pureflow-core` | `arrow` | Arrow payload variant |
| `pureflow-workflow` | `serde` | serialization for graph domain types |
| `pureflow-workflow-format` | `json` | JSON parser |
| `pureflow-workflow-format` | `toml` | TOML parser |
| `pureflow-workflow-format` | `yaml` | YAML parser |
| `pureflow-runtime` | `tracing` | tracing bridge at lifecycle/metadata seams |
| `pureflow-wasm` | `wasi` | WASI support only when needed |

## 7. Library Recommendations

Versions below were checked on 2026-04-26. Re-check before pinning anything
that changes quickly, especially Wasmtime, DataFusion, Arrow, and JSON Schema.

| Area | Recommendation | Version | Timing |
| --- | --- | ---: | --- |
| Runtime substrate | `asupersync` | `0.2.9` | Keep; already in workspace |
| Cheap byte payloads | `bytes` | `1.11.1` | Near-term |
| Serialization | `serde` | `1.0.228` | External definitions/contracts |
| JSON | `serde_json` | `1.0.149` | Canonical workflow format |
| TOML | `toml` | `0.9.8` | Human-authored workflows |
| YAML | `serde_yml` | `0.0.12` | Optional feature; avoid `serde_yaml` as default because it is deprecated |
| JSON Schema generation | `schemars` | `1.2.1` | AI/tooling schemas |
| JSON Schema validation | `jsonschema` | `0.46.0` | Optional for untrusted AI-generated workflow validation |
| CLI | `clap` | `4.6.0` | Real subcommands |
| CLI completions | `clap_complete` | `4.6.2` | Shell completions |
| Errors | `thiserror` | `2.0.18` | Internal typed errors |
| CLI/application errors | `anyhow` | `1.0.102` | CLI glue only |
| IDs | `uuid` | `1.23.0` | Prefer feature `v7` for sortable generated IDs |
| Optional sortable IDs | `ulid` | `1.2.1` | Alternative to UUIDv7 if desired |
| Time | `time` | `0.3.47` | Metadata timestamps |
| Tracing facade | `tracing` | `0.1.44` | Optional diagnostics bridge |
| Tracing setup | `tracing-subscriber` | `0.3.23` | CLI/runtime setup |
| Non-blocking logs | `tracing-appender` | `0.2.4` | Optional CLI/app file logs |
| Metrics facade | `metrics` | `0.24.3` | Optional counters/histograms |
| Prometheus exporter | `metrics-exporter-prometheus` | `0.18.1` | Optional operations surface |
| WASM runtime | `wasmtime` | `43.0.0` | WASM MVP |
| WASI support | `wasmtime-wasi` | `43.0.0` | Only if guest nodes need WASI |
| WIT bindings | `wit-bindgen` | `0.56.0` | Component model bindings |
| Capability filesystem | `cap-std` | `4.0.2` | Optional native/process capability adapters |
| Columnar data | `arrow` | `58.1.0` | Future high-performance tier |
| Query engine | `datafusion` | `53.0.0` | Future node implementation, not core |
| Property tests | `proptest` | `1.6.0` | Already in workspace |
| Benchmarks | `criterion` | `0.8.2` | Backpressure and metadata overhead |
| Concurrency model tests | `loom` | `0.7.2` | Supervisor/port concurrency tests where practical |
| Snapshot tests | `insta` | `1.47.2` | Introspection JSON diagnostics |

Crates intentionally not recommended:

- `tokio`: avoid fragmenting the runtime substrate.
- `async-std`: same issue as Tokio, with weaker ecosystem momentum.
- `flume` or `crossbeam-channel`: avoid multiple channel semantics.
- `dashmap`: no current shared-state hotspot justifies it.

Fork posture:

- do not fork preemptively
- keep `asupersync` behind `pureflow-runtime` so a fork remains cheap if needed
- keep YAML behind `pureflow-workflow-format` so parser choice is swappable
- keep Wasmtime behind `pureflow-wasm` so runtime churn does not leak into core

## 8. Roadmap

### Phase 1: True FBP Core

Deliver:

- async `recv`, `recv_any`, `reserve`, and `send`
- concurrent workflow supervisor
- fail-fast shared cancellation
- edge capacity support
- topological diagnostics and default cycle rejection
- identifier length cap
- deterministic tests for backpressure, cancellation, failure, fan-out, fan-in,
  and closure propagation

Exit criteria:

- source and sink run concurrently over a bounded edge
- blocked send unblocks when downstream receives
- failure cancels sibling tasks deterministically
- no public API exposes `asupersync` internals

### Phase 2: Contracts, Formats, And Inspection

Deliver:

- `pureflow-contract`
- `pureflow-workflow-format`
- JSON/TOML loaders
- optional YAML loader
- versioned workflow files
- introspection JSON
- CLI `validate` and `inspect`

Exit criteria:

- AI-generated workflows can be validated before execution
- invalid topology, unknown contracts, schema mismatch, and capability mismatch
  return stable diagnostics

### Phase 3: Metadata Productization

Deliver:

- richer metadata vocabulary
- message metadata at port boundaries
- in-memory and JSONL sinks
- tiered metadata policy
- optional tracing bridge
- CLI `run` and `explain`

Exit criteria:

- workflow execution can be diagnosed from workflow file plus JSONL metadata
- message lineage, lifecycle, queue pressure, and errors are structured

### Phase 4: WASM Vertical Slice

Deliver:

- `BatchExecutor`
- `pureflow-wasm`
- Wasmtime host adapter
- one sample WASM node
- capability enforcement at the WASM boundary

Exit criteria:

- one native node and one WASM node run in the same bounded graph
- WASM outputs are validated before entering the graph
- denied capabilities fail with stable errors

### Phase 5: High-Performance Data Tier

Deliver only after real workload pressure:

- Arrow payload feature
- schema compatibility for Arrow batches
- optional DataFusion node crate
- benchmarks proving copy and latency behavior

Exit criteria:

- byte payload APIs remain simple
- Arrow/DataFusion stay out of core runtime dependencies

## 9. Risk Register

| Risk | Severity | Mitigation |
| --- | --- | --- |
| Sequential runner masks FBP bugs | High | Prioritize async ports and concurrent supervisor before WASM/DataFusion |
| Runtime substrate leaks into public API | High | Keep `asupersync` behind runtime/port adapters; add public API checks |
| Deadlock in cycles | High | Reject cycles by default; require explicit opt-in and add watchdog diagnostics later |
| Fan-out partial delivery | High | Preserve reserve/commit across all downstream senders |
| Metadata overhead breaks zero-cost goal | Medium | Add `NoopMetadataSink` benchmarks and `TieredMetadataSink` |
| YAML parser maintenance risk | Medium | JSON canonical; YAML feature-gated and isolated |
| Wasmtime release churn | Medium | Hide behind `pureflow-wasm` and `BatchExecutor` |
| Native capabilities mistaken for sandboxing | Medium | Mark native enforcement as advisory in contracts and introspection |
| AI workflows bypass validation | High | CLI/API must validate before run; no best-effort execution |
| Arrow/DataFusion complexity lands too early | Medium | Defer until byte-message runtime and WASM boundary are stable |

## 10. Final Decision

Adopt a merged strategy:

- use the original proposal as the vision
- use `proposal_1` as the contract-first, current-version, metadata-first base
- adopt `proposal_2`'s concrete runtime refinements: workload shapes,
  `recv_any`, edge capacity, cycle policy, feature hygiene, workflow-format
  isolation, identifier caps, and performance/concurrency testing

The near-term goal is not WASM, Arrow, or a large CLI. The near-term goal is a
small runtime that proves Pureflow is actually FBP: long-lived concurrent nodes,
bounded backpressure, clean cancellation, structured metadata, and no runtime
substrate leakage.

## 11. Additional Beads

These beads extend the earlier `proposal_1` bead list with the useful additions
from `proposal_2`.

1. `ports-async-recv-send`: add async `recv`, `reserve`, and `send` methods on
   `PortsIn`/`PortsOut` with cancellation mapping.
2. `ports-recv-any`: add `recv_any` for multi-input nodes and stream joins.
3. `engine-concurrent-supervisor`: replace primary sequential workflow runner
   with supervised concurrent node execution.
4. `engine-fail-fast-cancellation`: attach one workflow cancellation handle to
   all nodes and cancel siblings on first failure.
5. `workflow-edge-capacity`: add `EdgeCapacity` to `EdgeDefinition` and wire it
   into channel construction.
6. `workflow-topology-diagnostics`: add topological ordering and
   `CycleDetected` diagnostics.
7. `workflow-cycle-policy`: reject cycles by default and add an explicit
   cycles-allowed construction path.
8. `types-identifier-length-cap`: add a maximum identifier length and typed
   `TooLong` error.
9. `contracts-core`: add `pureflow-contract` with node contracts, port
   contracts, schema refs, execution modes, and contract validation.
10. `workflow-format-crate`: add `pureflow-workflow-format` with versioned raw
    workflow definitions.
11. `workflow-format-json`: add JSON parser and round-trip tests.
12. `workflow-format-toml`: add TOML parser behind a feature.
13. `workflow-format-yaml`: add optional YAML parser behind a feature.
14. `introspection-core`: add pure workflow/contract introspection data types.
15. `introspection-json-snapshots`: add JSON rendering and snapshot tests.
16. `metadata-message-boundary`: emit message metadata from port send/receive
    boundaries.
17. `metadata-jsonl-sink`: add reproducible JSONL metadata sink.
18. `metadata-tiered-sink`: add policy adapter that avoids recording large
    payload bytes.
19. `payload-bytes-tier`: introduce `PacketPayload::Bytes(bytes::Bytes)`.
20. `payload-control-tier`: introduce `PacketPayload::Control(serde_json::Value)`.
21. `payload-arrow-feature`: add Arrow payload support behind an `arrow`
    feature, deferred until Phase 5.
22. `runtime-tracing-feature`: add optional tracing bridge for lifecycle and
    cancellation events.
23. `cli-use-pureflow-runtime`: replace temporary CLI `futures::executor`
    execution with `AsupersyncRuntime`.
24. `cli-validate-inspect`: add `validate` and `inspect` commands.
25. `cli-run-explain`: add `run` and `explain` commands once metadata sinks
    exist.
26. `bench-backpressure-capacity`: add Criterion benchmarks for capacity `1`,
    default capacity, and fan-out/fan-in.
27. `bench-metadata-overhead`: benchmark `NoopMetadataSink` vs default/tiered
    sinks.
28. `api-substrate-leak-check`: add a check that public Pureflow APIs do not
    expose `asupersync` types.
29. `wasm-batch-trait`: add runtime-neutral `BatchExecutor`,
    `BatchInputs`, and `BatchOutputs`.
30. `wasm-wasmtime-adapter`: add `pureflow-wasm` with Wasmtime host-owned batch
    execution.
31. `wasm-capability-enforcement`: enforce declared capabilities for WASM
    nodes.
32. `wasm-native-mixed-example`: add a native + WASM linear pipeline example
    matching the MVP slice.

## References

- Original architecture proposal: `docs/archetecture/pureflow_proposal.md`
- Strategy request: `docs/archetecture/strategy/proposal_request.md`
- First strategy proposal: `docs/archetecture/proposal_1.md`
- Second strategy proposal: `docs/archetecture/proposal_2.md`
- Current engine scaffold: `crates/pureflow-engine/src/lib.rs`
- Current runtime boundary: `crates/pureflow-runtime/src/lib.rs`
- Current port adapters: `crates/pureflow-core/src/ports.rs`
- Current workflow validation: `crates/pureflow-workflow/src/lib.rs`
- `bytes`: https://docs.rs/crate/bytes/latest
- `serde`: https://docs.rs/crate/serde/latest
- `serde_json`: https://docs.rs/crate/serde_json/latest
- `toml`: https://docs.rs/crate/toml/0.9.8
- `serde_yml`: https://docs.rs/crate/serde_yml/latest
- `schemars`: https://docs.rs/crate/schemars/latest
- `jsonschema`: https://docs.rs/crate/jsonschema/latest
- `wasmtime`: https://docs.rs/crate/wasmtime/latest
- `wasmtime-wasi`: https://docs.rs/crate/wasmtime-wasi/latest
- `wit-bindgen`: https://docs.rs/crate/wit-bindgen/latest
- `arrow`: https://docs.rs/crate/arrow/latest
- `datafusion`: https://docs.rs/crate/datafusion/latest
- `criterion`: https://docs.rs/crate/criterion/latest
- `loom`: https://docs.rs/crate/loom/latest
- `insta`: https://docs.rs/crate/insta/latest
- `cap-std`: https://docs.rs/crate/cap-std/latest
