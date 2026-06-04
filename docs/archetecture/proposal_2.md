# Pureflow Architecture Proposal v2

Companion to `pureflow_proposal.md` (v4) and the strategy brief in
`strategy/proposal_request.md`. The original proposal sets the vision; this
document grades it against what actually exists in `crates/` today and proposes
the next wave of work.

The format is deliberately blind: no organizational or model identifiers; all
recommendations are justified by code references and crate evidence.

---

## 1. What Already Exists (Honest Baseline)

A walk through `crates/` shows the scaffold is further along than the v4
proposal lets on. The runtime substrate is real, the boundary types are
disciplined, and the error / capability / metadata seams are in place. What is
*missing* is the execution model itself.

| Layer | Crate | Status |
| --- | --- | --- |
| Identifier primitives | `pureflow-types` | Validated newtypes (`WorkflowId`, `NodeId`, `PortId`, `ExecutionId`, `MessageId`); whitespace/control rejection; property tests. **No length cap.** |
| Static graph | `pureflow-workflow` | `WorkflowDefinition`/`WorkflowGraph` with deterministic dup-node, dup-port, unknown-endpoint, and direction-mismatch validation. **No cycle detection. No serde.** |
| Boundary types | `pureflow-core` | `NodeExecutor` trait (async via GAT `RunFuture<'a>`), `NodeContext`, `CancellationHandle`/`Token`, `ExecutionMetadata`, `MessageEnvelope`, `NodeCapabilities`, `LifecycleHook`, `MetadataSink`, `PureflowError` taxonomy with stable codes. |
| Port surface | `pureflow-core::ports` | `PortsIn`/`PortsOut` over `asupersync::channel::mpsc` with reserve/commit permits, fan-out via cloned `Sender`s, `try_recv`/`try_send` only. **No async `recv().await`/`send().await` surface.** |
| Runtime wrapper | `pureflow-runtime` | `AsupersyncRuntime` wraps `RuntimeBuilder`; `block_on` one-node execution; cancellation handle bridge; lifecycle + metadata observer dispatch; deterministic `current_thread` test ctor. |
| Orchestration | `pureflow-engine` | `run_workflow` iterates nodes **sequentially in declaration order**, wires edges as bounded channels with capacity 1. Node ordering is not topological. |
| Test kit | `pureflow-test-kit` | Builders, `RecordingExecutor`, `FailingExecutor`, identifier strategy. |
| CLI | `pureflow-cli` | `PrintExecutor` over an empty workflow. |

The scaffold is **lint-clean, formatted, dylint-clean, and tested**
(see `docs/handoff_2026-04-26.md`). The error model and lifecycle hook gaps the
last audit flagged are now closed.

The single largest divergence from v4 is in `pureflow-engine`:

> `run_workflow` (engine/lib.rs:22) walks `workflow.nodes()` once, sequentially,
> awaiting each node to completion before starting the next. That is a
> *DAG-style one-shot scheduler*, not a *long-lived FBP process graph*.

Everything else builds toward the FBP model; `run_workflow` is the tip that
needs to be replaced before any of the proposal's interesting semantics
(streaming, backpressure propagation, demand-driven execution) become
observable.

---

## 2. Common Workflow Shapes to Optimize For

Before recommending architecture, name the workloads. The v4 proposal is
abstract here. Concrete shapes drive concrete decisions:

1. **Linear ETL pipeline** — ingest → transform → sink. The MVP slice in v4 §7.
2. **Fan-out / fan-in** — one source feeding N parallel transforms whose
   outputs merge into one sink (per-tenant processing, A/B model evaluation).
3. **AI-call orchestration** — node calls an LLM, emits structured tool calls,
   downstream nodes execute and return; mixes streaming text with control
   messages.
4. **Stream join / window** — two sources at different rates joined by key,
   bounded by time or message count.
5. **Replay / branch evaluation** — run the same workflow with a swapped node
   to compare lineage and outputs (the v4 metadata-first ambition pays off
   here).
6. **Long-running watchers** — a source node that never terminates (file
   watcher, queue consumer); other nodes are bounded by cancellation only.

These six shapes drive the rest of the document. They are not a roadmap of
features; they are the loads the engine has to survive.

---

## 3. Architecture Optimization

### 3.1 Replace `run_workflow` with a Long-Lived Process Scheduler

The single highest-leverage change. Today every node runs once. FBP wants every
node to run *as a process* until it self-terminates, all upstreams disconnect,
or the workflow is cancelled.

Proposed shape (lives in `pureflow-engine`):

```text
WorkflowSupervisor
  ├── for each NodeDefinition: spawn one supervised task on AsupersyncRuntime
  │     ├── task body = NodeExecutor::run(ctx, ports_in, ports_out)
  │     └── on terminal: emit Lifecycle event, drop output senders
  ├── shared CancellationHandle attached to every NodeContext
  └── join all tasks; aggregate first error or cancellation
```

`asupersync::runtime::Runtime::block_on` already exists and a single
`block_on(join_all(spawned_tasks))` is enough for v1; per-task supervision
beyond that can land later. The important invariants:

- Drop semantics close edges. When a node returns, its `PortsOut` drops, which
  drops the contained `mpsc::Sender`s. Downstream `try_recv` then yields
  `Disconnected`, which is already mapped to `PortRecvError::Disconnected` in
  `crates/pureflow-core/src/ports.rs:155-170`.
- Cancellation is shared. `CancellationHandle::token` is already cloneable and
  mutex-backed; attach it to every `NodeContext` at spawn time.
- The supervisor owns wiring. `build_port_wiring` in
  `crates/pureflow-engine/src/lib.rs:51-72` already builds the right map; reuse
  it. The bug is not in wiring, only in execution shape.

### 3.2 Add an Async Receive/Send Surface to Ports

`PortsIn::try_recv` and `PortsOut::try_send` are non-blocking. Long-lived
nodes need to *await* a packet without spinning. Add:

```rust
impl PortsIn {
    pub async fn recv(&mut self, port_id: &PortId) -> Result<PortPacket, PortRecvError>;
    pub async fn recv_any(&mut self) -> Result<(PortId, PortPacket), PortRecvError>;
}
impl PortsOut {
    pub async fn send(&self, port_id: &PortId, packet: PortPacket) -> Result<(), PortSendError>;
    pub async fn reserve(&self, port_id: &PortId) -> Result<PortSendPermit<'_>, PortSendError>;
}
```

Implementation backs onto `asupersync::channel::mpsc::Receiver::recv` and
`Sender::reserve` (both `async`). The Pureflow-owned wrapper preserves the
boundary contract documented in `crates/pureflow-core/src/ports.rs:1-27`.

`recv_any` is the FBP "pick from any input" primitive. Implement by polling
each receiver's `recv` future inside `futures::future::select_all`. This is
the primitive that makes shape 4 (stream join) and shape 6 (watcher with
control input) expressible.

### 3.3 Topological Ordering and Cycle Policy

`WorkflowGraph::validate` (`crates/pureflow-workflow/src/lib.rs:294`) is
structural only. Add a topological sort (`petgraph` or hand-rolled Kahn's
algorithm — the graphs are small) used for two purposes:

1. **Initial scheduling order** — start sources first so downstream nodes have
   data to wait on. Not strictly required but reduces wakeup churn.
2. **Cycle detection** — FBP allows cycles, but most real workloads don't want
   them. Default to *reject* with an opt-in `WorkflowGraph::with_cycles_allowed`
   builder. Cycles change deadlock analysis; making them explicit is cheap
   honesty.

### 3.4 Backpressure as Configured Capacity, Not a Default of 1

`DEFAULT_EDGE_CAPACITY = NonZeroUsize::MIN` (`engine/lib.rs:15`) means every
edge currently has a one-message buffer. That works for the scaffold; it will
be measurably wrong under any real load.

Add per-edge capacity to `EdgeDefinition`:

```rust
pub struct EdgeDefinition {
    source: EdgeEndpoint,
    target: EdgeEndpoint,
    capacity: EdgeCapacity,
}
pub enum EdgeCapacity {
    Default,                  // engine-chosen, e.g. 64
    Bounded(NonZeroUsize),    // explicit
}
```

`Default` lets the engine pick (start with 64; revisit after benchmarks).
Explicit values document the contract for known-bursty edges. `Unbounded` is
deliberately not offered; the v4 proposal's §2.7 commitment to bounded
channels is correct and worth defending.

### 3.5 Make the Metadata Sink Useful

`MetadataSink::record` is currently called only for lifecycle events
(`runtime/lib.rs:240-274`). Two cheap additions land most of v4 §2.4:

1. **Emit `MetadataRecord::Message` from the port surface** when a packet is
   committed via `PortSendPermit::send`. The sink can sample, drop, or fully
   record at its own discretion. This gives lineage capture without making
   nodes responsible for it.
2. **Wrap the sink with a tiered policy adapter** (`TieredMetadataSink`):
   always record control-tier messages; sample structured-tier; never record
   Arrow-tier payload bytes (only metadata). This addresses the
   metadata-first-vs-zero-cost tension flagged in
   `docs/audits/Audit_4_23.md` §2.6.

The trait signature does not need to change.

### 3.6 Tier the Data Model Without Premature Arrow

v4 §4.3 proposes three tiers (Control / Structured / High-perf). The current
`PortPacket = MessageEnvelope<Vec<u8>>` is tier 1 only. Concretize the tier
boundary with one type:

```rust
pub enum PacketPayload {
    Control(serde_json::Value),
    Bytes(Bytes),                       // bytes::Bytes for cheap clone/slice
    Structured(Arc<dyn DataPacket>),    // schema-bearing record
    Arrow(arrow::record_batch::RecordBatch),
}
pub type PortPacket = MessageEnvelope<PacketPayload>;
```

`Arrow` lives behind a feature flag (`arrow`) so users who do not need it do
not pay the dependency cost. `Bytes` is universally cheap. The `Structured`
variant is the open extension point; do not commit to a schema system yet.

### 3.7 External Workflow Definitions

v4 §2.9 names YAML/JSON/TOML. Concretely:

- `serde` derives on `WorkflowDefinition`, `NodeDefinition`, `EdgeDefinition`,
  `EdgeCapacity`, `NodeCapabilities`, `EffectCapability` — gated behind
  `serde` feature on `pureflow-workflow` and `pureflow-core`.
- A new `pureflow-workflow-format` crate owns parsing (separate so the
  in-memory crates do not pull `serde_json`/`serde_yml`/`toml` by default).
- Format versioning via a top-level `pureflow_version: "1"` field, rejected
  with a typed error if missing or unknown. Cheap, prevents the worst kind of
  silent breakage.

### 3.8 AI-Inspectable Introspection Surface

v4 §2.3 / §4.6. The introspection data model is a function over already
existing types: `(WorkflowDefinition, [NodeCapabilities]) ->
WorkflowIntrospection`. No runtime needed.

```rust
pub struct WorkflowIntrospection {
    pub workflow: WorkflowId,
    pub nodes: Vec<NodeIntrospection>,
    pub edges: Vec<EdgeIntrospection>,
}
pub struct NodeIntrospection {
    pub id: NodeId,
    pub inputs: Vec<PortIntrospection>,
    pub outputs: Vec<PortIntrospection>,
    pub effects: Vec<EffectCapability>,
    pub deterministic: Option<bool>,
}
```

Render to JSON via `serde` for AI consumers. Schema descriptions hook in once
the structured-tier `DataPacket` lands (§3.6). Keep this in `pureflow-core`
behind the `serde` feature so it does not become a runtime dependency.

### 3.9 WASM Boundary (Defer the Engine, Define the Edge)

v4 §2.5 / §5 commits to "host owns channels, WASM operates on batches". That
is the right model. **Do not pick a WASM engine yet.** What to do now:

- Define `WasmNode` purely as a trait with a batch-in / batch-out shape inside
  `pureflow-core`:
  ```rust
  pub trait BatchExecutor {
      fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs>;
  }
  ```
- Provide a `WasmModule` adapter type that holds an opaque `Box<dyn BatchExecutor>`.
- Pick the engine (`wasmtime` recommended below) only when an MVP node needs
  to ship.

This keeps the boundary stable while letting the engine choice be a
quarterly-scale decision rather than a today decision.

### 3.10 Crate-Level Feature Hygiene

The boundary already wins by being narrow. Reinforce it with explicit
features so consumers compile only what they use:

| Crate | Feature | Adds |
| --- | --- | --- |
| `pureflow-core` | `serde` | `Serialize`/`Deserialize` derives on public types |
| `pureflow-core` | `arrow` | `PacketPayload::Arrow` variant |
| `pureflow-workflow` | `serde` | derives on graph types |
| `pureflow-workflow-format` (new) | `yaml`, `json`, `toml` | one parser per feature |
| `pureflow-runtime` | `tracing` | `tracing` events at lifecycle/metadata seams |

`asupersync` already depends on `tracing`; the `tracing` feature here only
controls Pureflow's emission, not its presence in the dep tree.

---

## 4. Specific Code Modifications (in priority order)

The numbering matches the order I would actually land beads.

### 4.1 (Bead candidate) — Async port surface

**Files:** `crates/pureflow-core/src/ports.rs`, `crates/pureflow-core/src/lib.rs`.

Add `recv` / `recv_any` / `send` / `reserve` async methods that delegate to
`asupersync::channel::mpsc::Receiver::recv` and `Sender::reserve`. Preserve
`try_recv` / `try_send` for non-blocking call sites. Map errors via the
existing `PortRecvError` / `PortSendError` taxonomy. Add tests in the same
shape as the existing reserve/commit tests at `ports.rs:643-723`.

### 4.2 — Long-lived process scheduler in `pureflow-engine`

**Files:** `crates/pureflow-engine/src/lib.rs`.

Replace `run_workflow`'s sequential loop with a supervisor that spawns each
node on `AsupersyncRuntime`. Wire shared cancellation via a single
`CancellationHandle`. Drop senders/receivers explicitly when each task ends so
disconnect signals propagate. Aggregate the first error (preserving the
existing executor-failure-takes-precedence rule from `runtime/lib.rs:255-258`).

The existing `build_port_wiring` is reusable as-is.

### 4.3 — Edge capacity in `EdgeDefinition`

**Files:** `crates/pureflow-workflow/src/lib.rs`,
`crates/pureflow-engine/src/lib.rs`, `crates/pureflow-test-kit/src/lib.rs`.

Add `EdgeCapacity` enum, default `64`. Threaded through
`WorkflowBuilder::edge` with a new `edge_with_capacity` variant. Existing
tests stay green because `Default` resolves to a non-zero capacity.

### 4.4 — Topological order + cycle detection

**Files:** `crates/pureflow-workflow/src/lib.rs`.

Add `WorkflowGraph::topological_order` returning `Result<Vec<NodeId>,
WorkflowValidationError::CycleDetected>`. Default `WorkflowGraph::new` calls
it and rejects cycles. New constructor `WorkflowGraph::with_cycles_allowed`
skips the check for users who genuinely want feedback loops. Add
`CycleDetected { cycle: Vec<NodeId> }` variant to
`WorkflowValidationError`.

Cheapest implementation: hand-rolled Kahn's algorithm; the graphs are small
enough that pulling `petgraph` for one function is overkill until other graph
algorithms are needed.

### 4.5 — Identifier length cap

**Files:** `crates/pureflow-types/src/lib.rs`.

Audit recommendation AB-10 from `docs/audits/Audit_4_23.md`. Add
`MAX_IDENTIFIER_LEN = 256` (rationale: well above any human-typed identifier,
well below DoS vector size). New error variant
`IdentifierError::TooLong { kind, limit }`. Property test: any identifier
within the cap continues to validate.

### 4.6 — Metadata sink emits `Message` records

**Files:** `crates/pureflow-core/src/ports.rs`,
`crates/pureflow-runtime/src/lib.rs`.

Plumb the metadata sink reference into `PortsOut` (likely as a clonable
`Arc<dyn MetadataSink>`). On `PortSendPermit::send`, emit
`MetadataRecord::Message(envelope.metadata().clone())`. Existing
`NoopMetadataSink` keeps current call sites working.

### 4.7 — Tiered payload type

**Files:** `crates/pureflow-core/src/message.rs`,
`crates/pureflow-core/src/ports.rs`.

Introduce `PacketPayload` enum (Control / Bytes / Structured / Arrow). Make
`PortPacket = MessageEnvelope<PacketPayload>`. Gate `Arrow` behind a feature.
Existing `Vec<u8>` call sites convert via `PacketPayload::Bytes(Bytes::from(vec))`.

### 4.8 — `serde` features and `pureflow-workflow-format` crate

**Files:** new `crates/pureflow-workflow-format/`, plus feature gates on
`pureflow-core` and `pureflow-workflow`.

Adds JSON parsing first (smallest surface), then YAML and TOML behind their
own features. Top-level version field, typed error on missing/unknown.

### 4.9 — Introspection rendering

**Files:** new module `crates/pureflow-core/src/introspection.rs`.

Pure functions over already-validated types. `serde::Serialize` impls behind
the `serde` feature. CLI subcommand to dump JSON.

### 4.10 — Replace `block_on(futures::executor)` in CLI with `AsupersyncRuntime`

**Files:** `crates/pureflow-cli/src/main.rs`.

Currently `cli/main.rs:38` uses `futures::executor::block_on`. The runtime
crate exists for exactly this; switching closes the consistency gap before any
real CLI commands land.

---

## 5. Library Recommendations

Versions are pinned to current stable releases as of Q2 2026. All
recommendations are *additive* — none require giving up `asupersync` as the
core substrate.

### 5.1 Required next (within 1–2 epics)

| Crate | Version | Purpose | Notes |
| --- | --- | --- | --- |
| `bytes` | `1.10` | Cheap clone-able byte slices for `PacketPayload::Bytes` | Already widely available; transitive elsewhere likely. |
| `serde` | `1.0` | Workflow format derives | Feature-gated. |
| `serde_json` | `1.0` | First parser for workflow format | Stable, tiny surface. |
| `petgraph` | `0.6` | Cycle detection + future graph analysis | Optional; hand-rolled Kahn first. |
| `tracing` | `0.1` | Lifecycle / metadata tracing | Already a transitive dep via `asupersync`. |
| `tracing-subscriber` | `0.3` | CLI log formatting | Already a transitive dep via `asupersync`. |

### 5.2 Required when external workflows ship (Epic 3+)

| Crate | Version | Purpose | Notes |
| --- | --- | --- | --- |
| `serde_yml` | `0.0.12` | YAML parsing | **`serde_yaml` is archived as of 2024.** `serde_yml` is the maintained fork. If the fork's stewardship feels insufficient, vendor a minimal YAML subset using `yaml-rust2`. |
| `toml` | `0.8` | TOML parsing | Stable. |
| `schemars` | `0.8` | Auto-derive JSON schemas for AI introspection | Plays cleanly with `serde`. |
| `jsonschema` | `0.28` | Validate inbound workflow JSON against schema | Optional; only if AI-generated workflows are accepted untrusted. |

### 5.3 Required when WASM lands (Epic 4+)

| Crate | Version | Purpose | Notes |
| --- | --- | --- | --- |
| `wasmtime` | `27` | WASM execution engine | Mature, Bytecode Alliance, fits the host-owned-channel model in v4 §5. |
| `wasmtime-wasi` | `27` | WASI bindings if guests need filesystem/network gated by capabilities | Same major version as `wasmtime`. |
| `wat` | `1.x` (via wasmtime) | Test-fixture WASM authoring | Dev-only. |

`wasmer` is the alternative; either works. `wasmtime` wins on
batched-call ergonomics and on the Component Model story (relevant if guest
nodes ever need typed interfaces).

### 5.4 Required when high-performance data tier lands

| Crate | Version | Purpose | Notes |
| --- | --- | --- | --- |
| `arrow` | `54` | Columnar `RecordBatch` | Feature-gated `arrow`. |
| `arrow-flight` | `54` | Optional cross-process / cross-machine transport | Defer until distributed runtime is on the table. |
| `datafusion` | `44` | SQL / DataFrame nodes | Optional integration crate, not a core dep. |

### 5.5 Useful and low-risk

| Crate | Version | Purpose |
| --- | --- | --- |
| `ulid` | `1.1` | Time-sortable execution / message IDs (better than UUIDv4 for log replay) |
| `clap` | `4.5` | CLI argument parsing once subcommands appear |
| `criterion` | `0.5` | Benchmark suite (Cargo.toml flag for §3.4 capacity decisions) |
| `loom` | `0.7` | Concurrency permutation tests for the supervisor in §4.2 |
| `insta` | `1.43` | Snapshot tests for introspection JSON output |
| `cap-std` | `3.4` | Capability-based filesystem when `EffectCapability::FileSystem*` is enforced for native nodes |

### 5.6 Forks worth tracking (do not adopt yet)

- **`asupersync`.** Single-vendor, recently published (`0.2.9`). Watch
  upstream cadence; if a quarter passes without releases, fork into
  `crates/pureflow-asupersync/` so Pureflow can patch independently. The
  boundary documented in `crates/pureflow-runtime/src/lib.rs:11-19` is
  precisely the seam that makes a fork cheap.
- **`serde_yml`.** Already a fork. If its release pace stalls, the workflow
  format crate is small enough to swap to a hand-rolled parser using
  `yaml-rust2`.
- **`rdf-datafusion`.** Mentioned in `docs/audits/Audit_4_23.md` §2.6 as the
  eventual RDF integration path. Track maturity; `oxigraph` is the
  conservative fallback. Neither becomes a Pureflow dep — both are *node*
  implementations consumers ship separately.

### 5.7 Crates intentionally NOT recommended

- **`tokio`.** The runtime substrate decision has been made
  (`asupersync`). Adding tokio fragments the executor model and forces every
  port type to choose sides.
- **`async-std`.** Same reason; also less actively maintained than tokio.
- **`flume` / `crossbeam-channel`.** `asupersync::channel::mpsc` already
  serves the port surface; a second channel implementation creates two error
  vocabularies for one job.
- **`dashmap`.** No current site needs concurrent hash maps. Revisit if a
  shared-state node becomes a thing.

---

## 6. Architecture Diagrams

### 6.1 Runtime layering (target)

```text
+-------------------------------------------------------------+
|                       pureflow-cli                           |
|         (subcommands, format parsing, introspection)        |
+----------------------------+--------------------------------+
                             |
+----------------------------v--------------------------------+
|                     pureflow-engine                          |
|   WorkflowSupervisor:                                       |
|     - spawn one task per NodeDefinition                     |
|     - bounded edge channels (capacity per EdgeDefinition)   |
|     - shared CancellationHandle                             |
|     - aggregate terminal results                            |
+----------------------------+--------------------------------+
                             |
+-----------------+----------v----------+----------------------+
| pureflow-runtime | pureflow-core         | pureflow-workflow    |
|  AsupersyncRT   |  NodeExecutor        |  WorkflowDefinition |
|  lifecycle disp.|  PortsIn/PortsOut    |  topo + cycle check |
|  metadata disp. |  Cancellation*       |  serde derives      |
|  cancel bridge  |  Capabilities        |  pureflow-workflow-  |
|                 |  MetadataRecord      |    format (parsing) |
|                 |  Introspection       |                     |
+--------+--------+--------+-------------+----------+----------+
         |                 |                        |
         |                 |                        |
+--------v---------+   +---v-------------+   +------v---------+
|   asupersync     |   | pureflow-types   |   | optional crates|
|  Runtime, mpsc,  |   | NewType IDs +   |   | arrow, wasm-   |
|  Cx, JoinError   |   | length cap      |   | time, etc.     |
+------------------+   +-----------------+   +----------------+
```

### 6.2 Long-lived node lifecycle (target)

```text
spawn ─► NodeStarted ─► loop {                              ▲
            ▲           PortsIn::recv_any().await           │
            │             │                                 │
            │             ├─ packet ─► node logic           │
            │             │            │                    │
   cancellation observed  │            └─► PortsOut::send   │
            │             │                                 │
            │             └─ Disconnected/Cancelled ─► break│
            │           }                                   │
            └─────────────────────► NodeCompleted / Failed ─┘
                                    drop(PortsOut) → close downstream
```

### 6.3 Common workflow shapes mapped to runtime primitives

```text
Linear ETL              :  Source ─► Transform ─► Sink
Fan-out / fan-in        :  Source ─► [Worker × N] ─► Merger
AI orchestration        :  Prompt ─► LLM ─► (text | tools)
                                            │
                                            └─► Tool ─► back-edge
Stream join (windowed)  :  A ─► \
                                 ─► Joiner (recv_any + window state)
                            B ─► /
Replay / branch eval    :  Source ─► [variant_a, variant_b] ─► Compare
Long-running watcher    :  Watcher (never returns) ─► Pipeline ─► Sink
                              ▲
                              └─ cancellation only
```

All six shapes fit on the supervisor model in §3.1 *if* §3.2 (async port
surface) lands first. Without `recv_any`, shape 4 (stream join) and shape 6
(watcher with control input) cannot be expressed without busy-polling.

---

## 7. Implementation Roadmap

Sequenced as four epics. Each epic is sized to be ~5–10 beads in the existing
`cdt-*` style; rough effort given as S/M/L. The order minimizes churn — each
epic depends only on what comes before it.

### Epic A — Long-Lived Execution (foundation finish line)

| Order | Item | Size | Notes |
| --- | --- | --- | --- |
| A1 | Async port `recv` / `send` / `reserve` | M | Unblocks every other item below. |
| A2 | `recv_any` primitive | S | Builds on A1 with `select_all`. |
| A3 | `WorkflowSupervisor` spawn-and-join | L | Core bead; LOC small but invariants matter. |
| A4 | `EdgeCapacity` + non-1 default | S | Format-bearing change; do before serde. |
| A5 | Topo order + cycle detection | M | Add `CycleDetected` variant. |
| A6 | Identifier length cap (`AB-10`) | S | Closes outstanding audit bead. |
| A7 | Replace `futures::executor::block_on` in CLI | S | Cleanup. |

Exit criteria: a 4-node fan-out workflow with bounded edge capacities runs to
completion under `AsupersyncRuntime` and surfaces lifecycle + per-message
metadata.

### Epic B — Metadata-First Surface

| Order | Item | Size | Notes |
| --- | --- | --- | --- |
| B1 | Plumb metadata sink into `PortsOut` | M | §3.5(1). |
| B2 | `TieredMetadataSink` adapter | S | §3.5(2). |
| B3 | `tracing` feature on `pureflow-runtime` | S | Lifecycle + cancellation events. |
| B4 | `PacketPayload` tiered enum (no Arrow yet) | M | §3.6 minus the Arrow variant. |
| B5 | Introspection types + JSON renderer | M | §3.8. |
| B6 | CLI subcommand `pureflow inspect <workflow>` | S | Smoke test for B5. |

Exit criteria: an AI consumer can fetch a workflow's structural and
capability surface as JSON; per-message lineage is emitted at the port boundary.

### Epic C — External Workflow Format

| Order | Item | Size | Notes |
| --- | --- | --- | --- |
| C1 | `serde` feature on `pureflow-core`, `pureflow-workflow` | M | Derives only; no parsing. |
| C2 | `pureflow-workflow-format` crate, JSON parser | M | Top-level `pureflow_version` field. |
| C3 | YAML parser via `serde_yml` | S | Behind `yaml` feature. |
| C4 | TOML parser | S | Behind `toml` feature. |
| C5 | CLI subcommand `pureflow run <file.{json,yaml,toml}>` | S | Closes the loop end-to-end. |

Exit criteria: a hand-written YAML workflow parses, validates, executes, and
emits metadata. AI-generated workflows can be validated before run.

### Epic D — WASM Vertical Slice

| Order | Item | Size | Notes |
| --- | --- | --- | --- |
| D1 | `BatchExecutor` trait + `WasmModule` adapter | M | §3.9. |
| D2 | `wasmtime` integration crate `pureflow-wasm` | L | Host-owned channels; batch in/out. |
| D3 | One example WASM node + integration test | M | Mirrors v4 §7 MVP slice. |
| D4 | `EffectCapability` enforcement at WASM boundary | M | Strict mode; native stays advisory. |

Exit criteria: v4 §7's "2–4 nodes in a linear pipeline, one native + one WASM"
runs and passes its assertions.

### Optional Epic E — Arrow / High-Performance Tier

Pull only when there is a real workload. The trait surface from B4 makes this
additive.

---

## 8. Risk Register and Mitigations

Risks listed by severity / likelihood, with concrete mitigations rather than
generic warnings.

### 8.1 `asupersync` stalls or breaks compatibility

- **Severity:** High. Pureflow's runtime substrate is a single-vendor crate at
  `0.2.9`.
- **Likelihood:** Moderate; experimental research crates churn.
- **Mitigation:** The boundary in `crates/pureflow-core/src/error.rs:404-442`
  and `crates/pureflow-runtime/src/lib.rs:11-19` is already kept narrow.
  *Verify the boundary is leak-free* by adding a `cargo deny` rule (or a test
  that greps `pub` symbols) ensuring no `asupersync` types are reachable from
  `pureflow-core`'s public surface. If upstream stalls, fork into
  `crates/pureflow-asupersync/` and patch via `[patch.crates-io]`.

### 8.2 `serde_yaml` archived; YAML support is on a fork

- **Severity:** Medium.
- **Mitigation:** Keep YAML support behind a feature flag from day one. JSON
  is the always-supported format. If `serde_yml` stalls, swap to a hand-rolled
  parser over `yaml-rust2` — the format crate is small.

### 8.3 Long-lived supervisor deadlocks under cycles

- **Severity:** High; data corruption risk if errors silently hang.
- **Likelihood:** Moderate once cycles are intentionally allowed.
- **Mitigation:** Default cycle detection (§3.3) makes the opt-in explicit.
  Add a per-workflow watchdog timer in the supervisor (configurable, default
  off). For the supervisor itself: `loom`-based concurrency tests on the
  spawn/join path land alongside §4.2.

### 8.4 Backpressure surprises with default capacity

- **Severity:** Medium.
- **Mitigation:** §3.4's explicit `EdgeCapacity` enum forces the choice into
  the workflow definition. CLI introspection (B6) prints capacity per edge so
  surprises are visible before runtime. Benchmark suite (§5.5) measures
  throughput vs latency for a few representative shapes.

### 8.5 Metadata-first cost regression

- **Severity:** Medium; v4 §2.10 promises zero-cost.
- **Mitigation:** `TieredMetadataSink` (B2) keeps Arrow-tier payload bytes
  out of the metadata path entirely. `criterion` benches (§5.5) include a
  baseline run with `NoopMetadataSink` and a representative run with the
  default sampling adapter; regressions in the gap fail CI.

### 8.6 Capability layer remains advisory for native nodes

- **Severity:** Low (acknowledged in `docs/audits/Audit_4_23.md` §2.6); but
  worth stating so the boundary is not later mistaken for a sandbox.
- **Mitigation:** Documented at the trait, repeated in introspection JSON
  output ("enforcement: advisory" for native, "strict" for WASM). Future
  process-backed nodes can promote enforcement.

### 8.7 WASM engine choice locks in too early

- **Severity:** Medium.
- **Mitigation:** §3.9 keeps `BatchExecutor` engine-free. The
  `wasmtime`-backed implementation lives in its own crate. Swapping to
  `wasmer` or to a sandboxed process is a re-implementation of one trait, not
  a runtime overhaul.

### 8.8 Schema drift between proposal v4 and v2 (this doc)

- **Severity:** Low, but it has happened before (see `Audit_4_23.md` §2.3
  finding 2 on `NodeExecutor` shape vs §4.2).
- **Mitigation:** Add a "Status: Implementation in flight" header to each v4
  section that this proposal supersedes, with a back-link. When an Epic
  closes, mark the corresponding v4 section as "Realized in
  `crates/...`". Avoid silent drift.

---

## 9. Out of Scope (Explicit)

- Distributed execution. Same as v4 §8.
- An in-tree RDF/SPARQL engine. RDF is a *node*, not a runtime feature.
- A workflow UI. CLI + introspection JSON is the surface for the foreseeable
  future.
- Multi-tenant scheduling, quotas, or fair queuing. The runtime is
  single-process; quotas live above it if and when they are needed.

---

## 10. Summary

The scaffold is not "early"; it is *boundary-complete and execution-light*. The
shortest path to a runtime that earns the v4 vision is:

1. Make ports awaitable.
2. Replace the sequential walker with a long-lived supervisor.
3. Make backpressure capacity a workflow-level decision.
4. Wire metadata through ports.
5. Land an external format and an introspection surface.
6. Add the WASM boundary as an additive trait, not a rewrite.

Done in that order, the architecture stays internally consistent, no library
choice is locked in before its time, and `asupersync` remains a substrate that
Pureflow can fork without touching node-facing APIs.
