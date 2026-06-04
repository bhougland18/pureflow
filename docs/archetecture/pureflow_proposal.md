# Pureflow Workflow Engine — Architecture & Requirements Proposal (v4)

If you are looking for the reuse path into `highland-labs`, see
[`highland-labs-integration-proposal.md`](highland-labs-integration-proposal.md).

## 1. Vision

Pureflow is a **Flow-Based Programming (FBP) execution engine** designed for:

* AI-first inspection and collaboration
* high-concurrency, structured execution
* streaming, backpressure-aware computation
* extensibility via WASM-based nodes
* clear architectural boundaries (Polylith-aligned)
* metadata-rich, provenance-aware computation

> Pureflow is not a DAG scheduler.
> It is a **runtime for correct, observable, AI-native computation**.

---

## 2. Core Design Principles

### 2.1 Structured Concurrency (Asupersync-Based)

Built on `asupersync`:

Source: <https://github.com/Dicklesworthstone/asupersync>

* workflows form a **task tree**
* child tasks are bound to parent lifetimes
* cancellation propagates deterministically

Defined behavior:

* **Cancellation:** parent cancels → all descendants terminate
* **Panic handling:** configurable (fail-fast vs isolate)
* **No orphan tasks:** enforced by runtime

---

### 2.2 Flow-Based Programming (FBP) Core Model

* nodes are **long-lived processes**
* edges are **bounded channels**
* execution is **streaming and demand-driven**

This replaces DAG execution with:

> **Reactive flow graphs with backpressure**

Runtime boundary:

* Pureflow owns workflow topology, node contracts, port abstractions, metadata,
  capability descriptors, and introspection
* `asupersync` provides task execution, cancellation, and async primitives under
  those Pureflow-owned abstractions
* public node APIs should not expose raw `asupersync` task context or channel
  types without an explicit design decision

---

### 2.3 AI-First System Design

AI interacts with the system via:

* introspection APIs
* workflow definitions
* validation pipelines

Capabilities:

* inspect node contracts
* validate data compatibility
* propose workflow changes
* debug failures

---

### 2.4 Metadata-First Architecture

All execution produces metadata:

* lineage (input → output)
* execution trace
* timing + failures
* node contracts

Metadata remains split by source:

* execution context metadata identifies a node boundary
* message metadata travels with payloads
* lifecycle metadata records runtime transitions

A metadata sink/collector API is the collection boundary. It does not replace
the source-specific metadata types, and it does not make logging/tracing a node
capability by default.

Designed for:

* reproducibility
* debugging
* AI reasoning
* future graph/RDF integration

---

### 2.5 WASM as Extension Boundary

Execution modes:

* Native (Rust)
* WASM (sandboxed)
* Process (future)

MVP WASM model:

* host owns channels
* WASM operates on **message batches**
* no direct channel access inside WASM

---

### 2.6 Capability Model (Explicit)

#### Enforcement Levels

| Node Type | Enforcement             |
| --------- | ----------------------- |
| WASM      | strict sandbox          |
| Native    | trusted (advisory only) |
| Process   | OS-level (future)       |

Capabilities include:

* filesystem
* network
* secrets
* CPU/memory limits
* execution time
* determinism flags

Routine runtime observability is not an `EffectCapability`. Logging, tracing,
and metadata collection stay in the runtime/metadata layers unless a node asks
the host to write to an external sink that should be permissioned explicitly.

---

### 2.7 Backpressure as First-Class Concern

System guarantees:

* bounded channels (no unbounded queues)
* upstream pressure propagation
* demand-driven execution

Future:

* credit-based flow control

---

### 2.8 Polylith Alignment

* components map to nodes and subsystems
* enables independent testing and evolution

> Polylith defines structure
> Pureflow defines execution

---

### 2.9 External Workflow Definitions

Workflows defined in:

* YAML / JSON / TOML

Enables:

* versioning
* AI generation
* reproducibility

---

### 2.10 Zero-Cost Abstractions

* Rust-native implementation
* minimal runtime overhead
* explicit control of performance-critical paths

---

## 3. High-Level Architecture

```text
UI / CLI / API
        |
        v
Workflow Engine
--------------------------
Scheduler
Flow Executor (FBP)
Backpressure Engine
Metadata Engine
Capability Enforcement
--------------------------
        |
   +----+----+
   |         |
Native     WASM
Nodes      Nodes
   |         |
   +----+----+
        |
    Data Layer
```

---

## 4. Core Components

### 4.1 Workflow Engine

Responsibilities:

* parse workflow definitions
* build flow graph
* manage lifecycle
* enforce backpressure
* coordinate nodes

---

### 4.2 Node Execution Model (Corrected)

```rust
trait NodeExecutor {
    async fn run(
        &self,
        ctx: NodeContext,
        inputs: PortsIn,
        outputs: PortsOut,
    ) -> Result<()>;
}
```

Key properties:

* current scaffold note: `PortsIn` / `PortsOut` are placeholder handles that
  expose declared port identities until channel-backed runtime wiring lands
* nodes do not create channels
* engine wires all connections
* nodes operate continuously

---

### 4.3 Data Model

#### Tier 1: Control Data

```rust
enum ControlValue {
    Json(serde_json::Value),
    Bytes(Vec<u8>),
}
```

---

#### Tier 2: Structured Data

```rust
trait DataPacket {
    fn schema(&self) -> Schema;
}
```

---

#### Tier 3: High-Performance

* Apache Arrow
* zero-copy buffers

---

### 4.4 Backpressure Engine

* bounded channels
* push/pull hybrid execution
* upstream propagation

---

### 4.5 Metadata Engine

Tracks:

* execution graph
* lineage
* timing
* failures
* schemas

Initial implementation shape:

* keep context, message, and lifecycle metadata as distinct source types
* expose a metadata sink for runtime collection
* defer storage, fan-out, buffering, and graph/RDF projection policy

---

### 4.6 Introspection API

```json
{
  "node": "transform",
  "input_schema": "...",
  "output_schema": "...",
  "capabilities": ["network"],
  "deterministic": false
}
```

---

## 5. WASM Execution Model (MVP)

* host reads from channel
* passes data to WASM
* receives output
* pushes downstream

No streaming inside WASM (initially)

---

## 6. AI Interaction Model

AI workflow lifecycle:

1. Inspect (introspection API)
2. Propose (workflow diff)
3. Validate (schema + capability checks)
4. Execute (gated via policy / AI guard)

---

## 7. MVP Vertical Slice (Expanded)

The MVP must validate the core thesis.

Includes:

* 2–4 nodes in a linear pipeline
* bounded channels only
* one native node + one WASM node
* message-based WASM bridge
* metadata capture
* CLI execution

Explicit goals:

* prove structured concurrency works
* prove backpressure propagation
* prove WASM integration

---

## 8. Non-Goals (Initial)

* distributed execution
* full RDF engine
* UI builder
* enterprise orchestration

---

## 9. Future Directions

* streaming WASM execution
* Arrow/DataFusion integration
* distributed runtime
* extension marketplace
* AI-assisted optimization

---

## 10. Summary

Pureflow is:

> a **flow-based, metadata-first, AI-inspectable execution engine**
> built in Rust with structured concurrency and pluggable capabilities

It prioritizes:

* correctness
* composability
* observability
* extensibility

---

## Final Note

Pureflow is not:

* a DAG runner
* a scheduler
* a pipeline tool

It is:

> **a runtime for building correct, inspectable, AI-native systems using Flow-Based Programming principles**
