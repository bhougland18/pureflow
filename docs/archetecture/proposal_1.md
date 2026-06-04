# Pureflow FBP Engine Strategy Proposal 1

Date: 2026-04-26

## 1. Executive Position

Pureflow should continue with the original proposal's direction: a metadata-first,
AI-inspectable Flow-Based Programming runtime built on `asupersync`, with
Pureflow-owned graph, port, capability, metadata, and node-contract APIs. The
important correction is sequencing. The repository currently contains a strong
foundation, but not yet a full FBP runtime:

- `pureflow-workflow` validates static nodes, ports, and edges.
- `pureflow-core` owns node context, cancellation, message metadata, lifecycle,
  capability descriptors, errors, and bounded port handles.
- `pureflow-runtime` wraps `asupersync` for one node execution boundary.
- `pureflow-engine` wires bounded edge channels, then invokes nodes sequentially.
- `PortsIn` and `PortsOut` expose non-blocking `try_recv`, `try_reserve`, and
  `try_send`, but they do not yet expose the async waiting surface needed for
  long-lived streaming nodes.

The next strategic move should be to turn the engine from a sequential scaffold
into a supervised concurrent graph runner while preserving the current public
boundary: `asupersync` remains the runtime substrate, not the user-facing FBP
model.

`docs/archetecture/additional_considerations.md` is currently empty. The
actionable new considerations reviewed for this proposal are from
`docs/archetecture/strategy/proposal_request.md`.

## 2. Design Principles To Keep

The original proposal is technically sound in these areas and should remain the
architecture baseline:

- Nodes are long-lived processes, not one-shot DAG tasks.
- Edges are bounded channels and backpressure is a correctness feature.
- The runtime owns channels; node implementations receive Pureflow port handles.
- Workflow structure, runtime behavior, metadata, capabilities, and extensions
  remain separate concerns.
- WASM nodes should initially operate on host-managed message batches rather
  than receiving direct channel access.
- Routine observability is runtime metadata, not a node `EffectCapability`.
- Native nodes are trusted; WASM and future process nodes are enforceable
  isolation boundaries.

The current code already reflects several of these decisions. The strategy is to
extend those seams instead of replacing them.

## 3. Current Gap Analysis

| Area | Current state | Needed next |
| --- | --- | --- |
| Workflow graph | Validates node/port/edge structure deterministically | Add external serde models, per-edge config, node implementation references, and semantic validation outside the topology crate |
| Engine execution | Runs nodes one after another | Spawn all eligible nodes under one supervised task tree |
| Ports | Bounded channel handles with non-blocking try operations | Add cancel-safe async receive/reserve/send APIs and explicit close/drain semantics |
| Backpressure | Bounded channels exist | Prove upstream pressure propagation with concurrent tests and occupancy metadata |
| Cancellation | Pureflow-owned cancellation tokens are visible in node contexts | Attach one workflow-level cancellation handle to all node contexts and propagate failure policy deterministically |
| Lifecycle | Node start/complete/fail events exist | Emit scheduled/cancelled events, workflow-level events, and message observations |
| Metadata | Sink boundary exists | Record execution context, lifecycle, message send/receive, queue pressure, and validation facts without collapsing source-specific metadata |
| Capabilities | Capability descriptors and workflow cross-validation exist | Add node-contract registry and enforcement adapters for WASM |
| WASM | Not implemented | Add a `pureflow-wasm` crate using a batch-oriented host adapter |
| CLI | Temporary scaffold | Add `validate`, `run`, `inspect`, and `explain` commands once external definitions exist |

## 4. Proposed Target Architecture

```text
CLI / API / AI Tools
        |
        v
Workflow Definition Loader
  - JSON / TOML first
  - YAML optional and explicitly gated
        |
        v
Validation Pipeline
  - structural graph validation
  - node contract lookup
  - schema compatibility
  - capability policy
  - edge capacity policy
        |
        v
Pureflow Engine
  - graph wiring
  - workflow supervisor
  - node task lifecycle
  - failure / cancellation policy
  - metadata fan-in
        |
        v
Pureflow Runtime Adapter
  - asupersync task tree
  - bounded channels
  - cancellation bridge
  - deterministic test runtime
        |
        +--------------------+
        |                    |
        v                    v
Native Node Executor     WASM Batch Executor
trusted/advisory         sandboxed/enforced
```

Runtime flow:

```text
               workflow cancellation handle
                         |
                         v
                  WorkflowSupervisor
                         |
     +-------------------+-------------------+
     |                   |                   |
 node task A         node task B         node task C
     |                   |                   |
     +---- bounded edge -+---- bounded edge -+
          channels with reserve/commit send
```

Metadata flow:

```text
NodeContext metadata ----+
Lifecycle events --------+--> MetadataSink --> storage / trace / CLI / AI view
Message metadata --------+
Queue pressure facts ----+
Validation facts --------+
```

## 5. Required Code Modifications

### 5.1 Promote The Engine To A Concurrent Graph Runner

Add a workflow-level runtime entry point that owns graph execution policy:

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

Concrete changes:

- Replace the sequential loop in `pureflow-engine::run_workflow` with graph
  wiring plus one supervised task per node.
- Keep the current sequential function only as a test helper or remove it once
  the concurrent runner is available; do not let it remain the primary engine
  API because it encodes the wrong FBP semantics.
- Introduce a workflow cancellation handle shared by every `NodeContext`.
- Add a failure policy enum: `FailFast`, `CancelDownstream`, and
  `IsolateNode`. Start with `FailFast` only.
- Record `NodeScheduled`, `NodeStarted`, terminal events, and workflow terminal
  summary.

### 5.2 Add Async, Cancel-Safe Port APIs

The current non-blocking port methods are useful for tests and probes, but FBP
nodes need to wait for input and output capacity.

Add methods shaped like:

```rust
impl PortsIn {
    pub async fn recv(
        &mut self,
        port_id: &PortId,
        cancellation: &CancellationToken,
    ) -> Result<Option<PortPacket>>;
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

Concrete changes:

- Keep reserve/commit semantics for fan-out so a packet is either delivered to
  every connected downstream edge or to none.
- Map runtime channel errors into Pureflow errors at the port boundary.
- Define input closure semantics: `Ok(None)` means all upstream senders are
  closed and drained; cancellation remains an error.
- Add queue occupancy/available-capacity observations behind a metadata hook,
  not as public `asupersync` channel access.
- Preserve `try_*` methods for deterministic unit tests and non-blocking
  polling nodes.

### 5.3 Add Node Contracts Before Schema Execution

The workflow crate should continue to own only static topology. Node
implementation details belong in a separate contract layer.

Add a new crate or module, preferably `pureflow-contract`, with:

- `NodeContractId` or `ComponentId`
- `NodeContract`
- `PortContract`
- `SchemaRef`
- `ExecutionMode` with `Native`, `Wasm`, and later `Process`
- declared capabilities
- determinism and retry metadata

Then validate:

- every workflow node resolves to a known contract
- workflow ports match contract ports
- edge source schema is compatible with edge target schema
- node capabilities align with topology and selected execution mode
- WASM nodes have enforceable capabilities before execution

This keeps `pureflow-workflow` small and prevents it from becoming a security,
typing, or scheduling crate.

### 5.4 Add External Workflow Definitions

Add serde-backed raw definition types that parse into validated domain types:

```text
crates/pureflow-workflow
  RawWorkflowDefinition
  RawNodeDefinition
  RawEdgeDefinition
  WorkflowDefinition::try_from_raw(...)
```

Recommended initial file formats:

- JSON as the canonical interchange format.
- TOML for human-authored local workflows.
- YAML only behind a feature flag or separate importer because the dominant
  `serde_yaml` crate is deprecated.

Each edge should accept optional capacity:

```toml
[[edges]]
from = { node = "source", port = "out" }
to = { node = "sink", port = "in" }
capacity = 32
```

Default capacity should be greater than the current capacity of one for normal
pipelines. Use capacity `1` in tests that intentionally prove backpressure.

### 5.5 Build A WASM Batch Adapter

Add `crates/pureflow-wasm` after the native concurrent runtime passes tests.

MVP shape:

- Host owns all `PortsIn` and `PortsOut`.
- Host reads up to `batch_size` messages from declared inputs.
- Host calls a WASM component with message metadata and payload bytes.
- WASM returns zero or more output envelopes.
- Host validates output ports and pushes messages through normal Pureflow ports.

Do not expose direct channel operations inside WASM in the MVP. That would mix
sandboxing, scheduling, and backpressure before the host contract is proven.

### 5.6 Strengthen Metadata Without Turning It Into Logging

Extend `MetadataRecord` with runtime facts that are useful for AI inspection:

- workflow started/completed/failed/cancelled
- node scheduled/started/completed/failed/cancelled
- message enqueued/dequeued/dropped
- edge capacity and pressure snapshots
- validation result facts
- node contract and capability summaries

Keep the sink as the collection boundary. Storage, fan-out, sampling, tracing,
and graph/RDF projection should remain policy choices behind sink
implementations.

### 5.7 CLI Roadmap

Once definitions and validation exist, replace the temporary CLI with:

- `pureflow validate <workflow-file>`
- `pureflow inspect <workflow-file>`
- `pureflow run <workflow-file> --execution-id <id>`
- `pureflow explain <workflow-file>` for AI- and human-readable validation and
  runtime summaries

The CLI should depend on public Pureflow APIs only. It should not reach into
`asupersync` directly.

## 6. Recommended Rust Crates

Version numbers below were checked on 2026-04-26. Re-check before pinning,
especially for Wasmtime and DataFusion because they release frequently.

| Area | Recommendation | Version | Use |
| --- | --- | ---: | --- |
| Runtime substrate | `asupersync` | `0.2.9` | Keep as the task tree, cancellation, and bounded channel substrate already in the workspace |
| Serialization | `serde` | `1.0.228` | Derive definitions, contracts, metadata export |
| JSON | `serde_json` | `1.0.149` | Canonical workflow interchange and AI tooling |
| TOML | `toml` | `0.9.8` | Human-authored local workflow definitions |
| YAML | avoid default `serde_yaml`; optional `serde_yml` evaluation | `serde_yaml 0.9.34+deprecated`, `serde_yml 0.0.12` | Do not make YAML a core dependency until maintenance risk is accepted |
| JSON Schema | `schemars` | `1.2.1` | Emit schemas for workflow files, node contracts, and AI validation |
| CLI | `clap` | `4.6.0` | CLI parsing once scaffold CLI becomes real |
| CLI completions | `clap_complete` | `4.6.2` | Shell completion generation |
| Errors | `thiserror` | `2.0.18` | Derive internal error enums; keep public error codes explicit |
| CLI/application errors | `anyhow` | `1.0.102` | CLI glue only, not core public APIs |
| IDs | `uuid` | `1.23.0` | Execution/message IDs; prefer feature `v7` for sortable generated IDs |
| Time | `time` | `0.3.47` | Timestamps in metadata records |
| Tracing facade | `tracing` | `0.1.44` | Internal structured diagnostics, separate from metadata |
| Tracing setup | `tracing-subscriber` | `0.3.23` | CLI/runtime subscriber setup |
| Non-blocking logs | `tracing-appender` | `0.2.4` | Optional file logging for CLI/app surfaces |
| Metrics facade | `metrics` | `0.24.3` | Runtime counters and histograms if Prometheus is needed |
| Prometheus exporter | `metrics-exporter-prometheus` | `0.18.1` | Optional, not core runtime |
| WASM runtime | `wasmtime` | `43.0.0` | WASM/component execution boundary |
| WIT bindings | `wit-bindgen` | `0.56.0` | Component model bindings for guest nodes |
| Columnar data | `arrow` | `58.1.0` | Future high-performance packet payload tier |
| Query engine | `datafusion` | `53.0.0` | Future analytical/dataflow nodes, not MVP runtime |
| Property tests | `proptest` | `1.6.0` | Already in workspace; keep for graph and port invariants |

### Fork Or Upstream Improvement Candidates

Do not fork crates preemptively. Wrap first, fork only when the wrapper cannot
preserve Pureflow semantics.

- `asupersync`: request or contribute task-tree introspection, deterministic
  scheduler hooks, channel occupancy observation, and lifecycle callbacks if
  these cannot be cleanly implemented through adapters.
- `serde_yaml`: avoid as a default dependency. If YAML must become first-class,
  either isolate it behind a feature flag or maintain a narrow fork/importer
  with only the subset Pureflow supports.
- `wasmtime`: do not fork. Isolate behind `pureflow-wasm` because release cadence
  and component APIs can move quickly.
- `datafusion`: do not fork for MVP. If future nodes require custom streaming
  operators, implement them as extension nodes before considering engine-level
  modifications.

## 7. Implementation Roadmap

### Phase 1: Honest Concurrent FBP Core

Goal: prove that the runtime is actually flow-based.

Deliverables:

- concurrent workflow supervisor
- async cancel-safe port receive/reserve/send
- fail-fast workflow cancellation
- deterministic tests for backpressure, cancellation, node failure, fan-out,
  fan-in, and cyclic graph startup
- lifecycle and metadata observations for every node boundary

Exit criteria:

- a source and sink can run concurrently with a bounded edge of capacity `1`
- a blocked send unblocks when downstream receives
- node failure cancels siblings and descendants deterministically
- no public API exposes `asupersync` channel or task context types

### Phase 2: Workflow Definitions And Contracts

Goal: make workflows external, inspectable, and semantically validated.

Deliverables:

- serde raw workflow definitions
- JSON and TOML loaders
- generated JSON Schema for workflow files
- node contract model and registry
- schema/capability/contract validation pipeline
- CLI `validate` and `inspect`

Exit criteria:

- invalid topology, unknown contracts, bad port directions, schema mismatch,
  and capability mismatch produce stable diagnostics
- AI tools can inspect workflow and node contract structure without running it

### Phase 3: Runtime Metadata Productization

Goal: make execution explainable without coupling to any one storage backend.

Deliverables:

- richer `MetadataRecord` vocabulary
- in-memory sink for tests and CLI summaries
- JSONL sink for reproducible run logs
- optional tracing bridge
- CLI `run` and `explain`

Exit criteria:

- a run can be replayed diagnostically from workflow definition plus metadata
  log
- lifecycle, message lineage, and errors are visible in a structured output

### Phase 4: WASM MVP

Goal: prove the extension boundary without giving WASM direct channel access.

Deliverables:

- `pureflow-wasm` crate
- Wasmtime host adapter
- WIT contract for batch input and output
- one sample WASM node
- capability enforcement for filesystem, network, clock, environment, and fuel
  or epoch interruption

Exit criteria:

- one native node and one WASM node run in the same bounded flow graph
- WASM output is validated against declared ports and schemas
- denied capabilities fail before or during execution with stable errors

### Phase 5: High-Performance Data Tier

Goal: add Arrow/DataFusion only after the byte-message runtime is proven.

Deliverables:

- typed payload enum or packet abstraction that supports bytes and Arrow batches
- schema compatibility for Arrow payloads
- optional DataFusion-backed node type for analytical transforms

Exit criteria:

- byte payload APIs remain simple
- Arrow payloads avoid unnecessary copies at graph edges where possible
- analytical nodes do not force DataFusion into the core runtime

## 8. Risk Mitigation

| Risk | Mitigation |
| --- | --- |
| Sequential runner hides FBP bugs | Prioritize concurrent supervisor before WASM or schema work |
| Runtime substrate leaks into public API | Keep all `asupersync` types behind `pureflow-runtime` and port adapters |
| Deadlocks in bounded cyclic graphs | Add deterministic tests, explicit startup policy, and cancellation-on-deadlock diagnostics |
| Fan-out produces partial messages | Preserve reserve/commit across every downstream edge before sending |
| Native capabilities create false security | Document native capabilities as advisory and enforce only in WASM/process modes |
| Metadata becomes logging | Keep metadata records typed and source-specific; tracing remains an optional sink/bridge |
| YAML maintenance risk | Make JSON/TOML first-class; gate YAML |
| Wasmtime release churn | Hide behind `pureflow-wasm`; pin versions and update intentionally |
| DataFusion/Arrow complexity overwhelms MVP | Defer until the byte-message runtime and WASM boundary are stable |
| AI-generated workflows bypass policy | Require validation pipeline before execution; provide structured diagnostics rather than best-effort execution |

## 9. Near-Term Bead Breakdown

1. `engine-concurrent-supervisor`: add workflow-level cancellation handle,
   spawn one runtime-managed task per node, and emit lifecycle events.
2. `ports-async-cancel-safe`: add async receive/reserve/send methods with
   cancellation and deterministic tests.
3. `engine-backpressure-tests`: prove bounded propagation with capacity `1`
   and blocked-send scenarios.
4. `workflow-raw-definitions`: add serde raw models and JSON/TOML parsing.
5. `contracts-core`: add node contract types, registry trait, and validation.
6. `cli-validate-inspect`: replace temporary CLI behavior with validation and
   inspection.
7. `metadata-run-log`: add in-memory and JSONL metadata sinks.
8. `wasm-batch-spike`: add a minimal Wasmtime batch executor behind a new crate.

## 10. Recommended Decision

Adopt the proposal with one explicit priority: do not start with WASM or
DataFusion. Build the concurrent FBP core first. Without concurrent long-lived
nodes, async bounded ports, and workflow-level cancellation, the system remains
a sequential workflow scaffold with FBP-shaped types.

The current repository has the right seams. The next work should make those
seams executable under true flow-based semantics.

## References

- Original architecture proposal: `docs/archetecture/pureflow_proposal.md`
- Strategy request: `docs/archetecture/strategy/proposal_request.md`
- Current engine scaffold: `crates/pureflow-engine/src/lib.rs`
- Current runtime boundary: `crates/pureflow-runtime/src/lib.rs`
- Current port adapters: `crates/pureflow-core/src/ports.rs`
- Current workflow validation: `crates/pureflow-workflow/src/lib.rs`
- `serde`: https://docs.rs/crate/serde/latest
- `serde_json`: https://docs.rs/crate/serde_json/latest
- `toml`: https://docs.rs/crate/toml/0.9.8
- `serde_yaml`: https://docs.rs/crate/serde_yaml/latest
- `serde_yml`: https://docs.rs/crate/serde_yml/latest
- `schemars`: https://docs.rs/crate/schemars/latest
- `clap`: https://docs.rs/crate/clap/4.6.0
- `clap_complete`: https://docs.rs/crate/clap_complete/latest
- `thiserror`: https://docs.rs/crate/thiserror/latest
- `anyhow`: https://docs.rs/crate/anyhow/latest
- `uuid`: https://docs.rs/crate/uuid/latest
- `time`: https://docs.rs/crate/time/latest
- `tracing`: https://docs.rs/crate/tracing/latest
- `tracing-subscriber`: https://docs.rs/crate/tracing-subscriber/latest
- `tracing-appender`: https://docs.rs/crate/tracing-appender/latest
- `metrics`: https://docs.rs/metrics
- `metrics-exporter-prometheus`: https://docs.rs/crate/metrics-exporter-prometheus/latest
- `wasmtime`: https://docs.rs/crate/wasmtime/latest
- `wit-bindgen`: https://docs.rs/crate/wit-bindgen/latest
- `arrow`: https://docs.rs/crate/arrow/latest
- `datafusion`: https://docs.rs/crate/datafusion/latest
