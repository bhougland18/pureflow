# Response: Metadata Boundary Questions

Reviewing `crates/pureflow-core/src/metadata.rs` and `ports.rs` against the
current leanings in the question. The existing implementation already lines
up well with the right answers — these responses mostly confirm direction
and call out one place where the current draft should hold the line.

## Q1. Distinguish send vs receive explicitly, or rely on route metadata?

**Recommendation: keep the explicit boundary kind. Match your current
leaning.**

The current `MessageBoundaryKind { Enqueued, Dequeued, Dropped }` is the
right shape. Route metadata answers "where the message is going," not "which
side of the seam the observation came from." A single-record-with-route model
forces consumers (JSONL replay, AI tooling, snapshot tests) to infer the
boundary from sink scope or surrounding context, which is fragile and
schema-unfriendly.

Concretely:

- The same envelope appears on both sides of an edge. With a typed
  `MessageBoundaryKind`, "find every drop" or "every enqueue/dequeue pair for
  message X" is a direct filter. Without it, those queries depend on knowing
  which sink emitted which record.
- `Dropped` is only meaningful at the output side. A merged record would
  collapse three distinct events (sent / received / sent-but-discarded) into
  one and lose information that is cheap to keep.
- The cost is exactly one enum byte per record. Worth it.

Future-extensibility note: when queue-pressure observations land
(`metadata-tiered-sink` and friends), resist the temptation to fold
`BlockedByCapacity` or `EdgeClosed` into `MessageBoundaryKind`. Those are
*not* per-message events — they belong on the lifecycle / port-state side
(see Q4). Keep `MessageBoundaryRecord` for events that have a real
`MessageMetadata` attached.

## Q2. Sink ownership: port handles vs. thread-local seam

**Recommendation: keep sinks owned by `PortsIn`/`PortsOut`. Reject the
thread-local approach.**

The current implementation already does this (the `Option<Arc<dyn
MetadataSink + Send + Sync>>` field on each ports struct, attached via
`with_metadata_sink`). That is the right design for several concrete
reasons:

- **It matches the rest of the executor surface.** `CancellationToken` is
  passed explicitly through `NodeContext`, not via thread-local. Adding a
  thread-local seam *only for metadata* would be the lone exception, and
  exceptions like that are how ambient state metastasizes.
- **`asupersync` can move tasks between threads.** A `tokio::task_local!`
  analogue exists, but per-task locals require pinning a future to the local
  for the duration of execution. That introduces a runtime-substrate
  primitive in the public seam — exactly what `api-substrate-leak-check`
  (`cdt-8ar.3`) is meant to prevent.
- **Per-node sinks become possible later.** If you ever want a
  `TieredMetadataSink` whose policy varies by node identity (e.g., suppress
  payload-size records on a chatty source node), explicit ownership on the
  port handles supports that trivially. A thread-local sink per workflow
  does not.
- **Cost is negligible.** The sink is held behind `Arc`, cloned only into
  permits and into the `recv_any` poll closure. `Arc` clone is a single
  atomic increment.

Hold the line on `NodeExecutor` not changing. The current shape achieves
that — `NodeExecutor::run` still takes plain `PortsIn`/`PortsOut`; whether a
sink is attached is invisible to the node. Good.

## Q3. Record payload size now, or defer to the tiered payload bead?

**Recommendation: defer. Match your current leaning.**

The current envelope is `MessageEnvelope<Vec<u8>>`. A naive `payload.len()`
on a `MessageBoundaryRecord` would lock the metadata schema into "one
`usize` byte length," which is the wrong shape as soon as the tiered payload
bead lands:

- `Control(serde_json::Value)` has no obvious "byte size" — it has a JSON
  shape and a serialized length, which are different numbers.
- `Bytes(bytes::Bytes)` has a clean `len()`.
- `Structured(Arc<dyn DataPacket>)` has whatever introspection the trait
  offers, not a single integer.
- `Arrow(RecordBatch)` has rows, columns, and an aggregate buffer size — no
  single number summarizes it well.

The right move is to wait until `cdt-rpk.4` (and the `payload-control-tier`
follow-on) introduce `PacketPayload`, then add a typed `PayloadShape` that
covers all variants:

```rust
pub enum PayloadShape {
    Bytes { len: usize },
    Control { serialized_len: usize },
    Structured { kind: StructuredKind /* or similar */ },
    Arrow { rows: usize, columns: usize, buffer_bytes: usize },
}
```

That `PayloadShape` is what `MessageBoundaryRecord` should carry — added in
the same bead that introduces `PacketPayload`, not earlier. The
`metadata-tiered-sink` policy then makes its decisions in shape-aware terms
("strip Bytes payload size when len > N", "always record Control shape")
rather than guessing from a raw `usize`.

Until then, `MessageBoundaryRecord` should stay payload-size-free. Your
current draft is already correct here.

## Q4. Receive-side metadata: deliveries only, or also drained/disconnected?

**Recommendation: receive-side message records fire only on delivered
packets. Route drained/disconnected through the lifecycle layer instead.**

The current code already does the right thing — `try_recv` and `recv` only
emit `Dequeued` when a packet is actually returned. Hold this line.

A drained or disconnected port has no `MessageMetadata` to attach. Forcing
`MessageBoundaryRecord` to carry an `Option<MessageMetadata>` (or a
synthetic placeholder) would dilute the type and contradict the
`metadata-collection-boundary` fragment header in `metadata.rs`, which
explicitly frames message records as "describe[ing] send, receive, and drop
observations at the port seam." Drained-and-empty is a *port state*
transition, not a message observation.

The right home for those events is one of:

- **`LifecycleEvent`** — extend it with port-level transitions
  (`PortDrained { port_id }`, `EdgeClosed { source, target }`). These pair
  naturally with the existing `NodeStarted`/`NodeCompleted` events and are
  what `metadata-jsonl-sink` consumers will want adjacent to node lifecycle.
- **A new `PortLifecycleRecord`** as a sibling variant of
  `MetadataRecord::Lifecycle` and `MetadataRecord::Message`, if the
  lifecycle vocabulary feels too node-shaped to absorb port events.

Either is fine; the important boundary is that `MessageBoundaryRecord`
stays about messages. I'd lean toward extending `LifecycleEvent` first —
fewer top-level variants, and the JSONL consumer story is simpler — and
splitting it out only if `LifecycleEvent` starts feeling overloaded.

Worth tracking as a follow-up bead (e.g., `metadata-port-lifecycle`)
attached to the broader metadata-productization phase rather than the
current message-boundary bead.

## Summary

| Q | Recommendation | Match current leaning? |
| --- | --- | --- |
| 1 | Keep explicit `MessageBoundaryKind`. | Yes |
| 2 | Keep sink ownership on port handles. | Yes |
| 3 | Defer payload size until typed `PayloadShape` lands with `PacketPayload`. | Yes |
| 4 | Deliveries only for message records. Drain/disconnect → lifecycle layer. | Implicit; make it explicit and track follow-up bead. |

The current direction is sound. The only addition is to write down — in the
metadata bead's closing notes — that drained/disconnected observations are
intentionally deferred to the lifecycle layer rather than treated as a gap
in `MessageBoundaryRecord`.
