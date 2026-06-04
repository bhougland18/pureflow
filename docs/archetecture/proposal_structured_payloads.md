# Pureflow Structured Payloads Proposal

Date: 2026-05-03
Status: Draft for review
Extends: `docs/archetecture/proposal_final.md` §6.9 (payload tiering)

## 1. Summary

Add a `Structured(Arc<dyn DataPacket>)` variant to `PacketPayload` and a
`DataPacket` trait in `pureflow-core`. Provide an optional, feature-gated
`PostcardPacket<T>` impl behind `serde-postcard`. Pureflow core remains
format-agnostic; postcard becomes the obvious zero-boilerplate path for
consumers that opt in. Arrow, JSON, and bytes paths are unchanged.

This addresses two real consumer needs:

- **Intra-process typed payloads without serialization tax.** A producer
  node that yields a typed value should be able to share an `Arc`-cloned
  value with a consumer node in the same process, without encoding to
  bytes on every edge hop.
- **Two-format consumers.** Downstream apps (zeroflat is the immediate
  case) will use both postcard (forms, CRDT ops, UI state) and Arrow
  (sensor data, batch analytics) as primary in-flight payload shapes.
  Both should be first-class without coupling Pureflow core to either.

## 2. Current state

`crates/pureflow-core/src/message.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(not(feature = "arrow"), derive(Eq))]
pub enum PacketPayload {
    Bytes(Bytes),
    Control(Value),
    #[cfg(feature = "arrow")]
    Arrow(arrow_array::RecordBatch),
}
```

Three variants today. The `Structured` variant proposed in
`proposal_final.md` §6.9 is not yet implemented. There is no
`DataPacket` trait.

`asupersync` is the runtime substrate; the public `PacketPayload` does
not leak it. That boundary stays intact under this proposal.

## 3. Proposed shape

### 3.1 Trait in `pureflow-core` (no new deps)

```rust
use std::any::Any;
use bytes::Bytes;

pub trait DataPacket: Any + Send + Sync + 'static {
    /// Serialize the payload to bytes for boundary crossings
    /// (WASM, persistence, network, replay).
    fn serialize(&self) -> Result<Bytes, SerializeError>;

    /// Stable identifier for the carried type. Used for contract
    /// validation, introspection, and cross-process reconstruction.
    fn schema_id(&self) -> SchemaId;

    /// Downcast support for typed consumers.
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaId(pub &'static str);

#[derive(Debug, thiserror::Error)]
pub enum SerializeError { /* ... */ }
```

`Any + 'static` lets typed consumers downcast without Pureflow owning a
type registry.

### 3.2 Variant addition

```rust
pub enum PacketPayload {
    Bytes(Bytes),
    Control(Value),
    Structured(Arc<dyn DataPacket>),       // NEW
    #[cfg(feature = "arrow")]
    Arrow(arrow_array::RecordBatch),
}

impl PacketPayload {
    pub fn structured<P: DataPacket>(packet: Arc<P>) -> Self {
        Self::Structured(packet)
    }

    pub fn as_structured(&self) -> Option<&Arc<dyn DataPacket>> {
        match self {
            Self::Structured(p) => Some(p),
            _ => None,
        }
    }

    pub fn downcast<T: 'static>(&self) -> Option<&T> {
        self.as_structured()?.as_any().downcast_ref::<T>()
    }
}
```

`Eq` derive on `PacketPayload` becomes conditional on `Structured`
absence already-conditional on `Arrow`. Trait-object equality is not
provided (intentional — equality on opaque structured payloads is
ill-defined).

### 3.3 Postcard impl behind `serde-postcard` feature

```rust
// pureflow-core, only compiled with feature = "serde-postcard"

pub struct PostcardPacket<T> {
    inner: Arc<T>,
    schema_id: SchemaId,
}

impl<T> PostcardPacket<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn wrap(value: T, schema_id: SchemaId) -> Arc<Self> {
        Arc::new(Self { inner: Arc::new(value), schema_id })
    }

    pub fn from_arc(inner: Arc<T>, schema_id: SchemaId) -> Arc<Self> {
        Arc::new(Self { inner, schema_id })
    }

    pub fn value(&self) -> &T { &self.inner }
}

impl<T> DataPacket for PostcardPacket<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn serialize(&self) -> Result<Bytes, SerializeError> {
        let bytes = postcard::to_allocvec(&*self.inner)
            .map_err(SerializeError::Postcard)?;
        Ok(Bytes::from(bytes))
    }
    fn schema_id(&self) -> SchemaId { self.schema_id }
    fn as_any(&self) -> &dyn Any { self }
}
```

Consumer-side typed access:

```rust
let form = packet.payload()
    .downcast::<PostcardPacket<MyForm>>()
    .map(|p| p.value());
```

### 3.4 What does NOT change

- `Bytes`, `Control`, and `Arrow` variants stay as they are.
- `MessageEnvelope`, `PortPacket`, port APIs unchanged.
- Workflow file format stays JSON canonical / TOML / YAML — postcard is
  for in-flight payloads, not definitions.
- Metadata sinks stay JSONL — observability data benefits from
  grep-ability, not binary compaction.
- WASM batch boundary stays bytes-in/bytes-out — see §5.

## 4. Why postcard belongs as a feature, not core

Symmetric with the existing `arrow` feature:

| Concern | Stays in core | Behind feature |
|---|---|---|
| Substrate-cleanliness | `DataPacket` trait, `Structured` variant | postcard impl, schema id type, postcard error variant |
| Mandatory deps | none (uses `bytes`, `std::any`) | `postcard`, `serde` (already in workspace) |
| Consumer choice | implement `DataPacket` themselves | one-liner `PostcardPacket::wrap(...)` |
| Coupling | none — Pureflow knows nothing about format | format-aware only when feature is on |

A consumer using fbs, CBOR, MessagePack, capnproto, or a hand-rolled
codec implements `DataPacket` directly. Consumers using postcard get the
provided impl. Consumers using only `Bytes` pay nothing.

## 5. Boundary semantics

Behavior at boundaries that cannot pass `Arc<dyn DataPacket>`:

| Boundary | Strategy |
|---|---|
| WASM batch executor | Host calls `payload.serialize()` before invoking guest; guest output returns as `Bytes` (host can re-wrap if it knows the schema). MVP: bytes-out, no auto-reconstruction. |
| Persistence (JSONL replay, future) | `serialize()` to bytes plus `schema_id` recorded as metadata. Reconstruction is a per-consumer concern; Pureflow does not own a global registry. |
| Future network distribution | Same as persistence — `serialize()` + `schema_id` on the wire, peer reconstructs if it knows the type. |

Pureflow does not own a global type registry; the consumer that knows
the workflow knows the schemas. This keeps the runtime free of
schema-versioning policy.

## 6. Contract validation hook (Phase 2, not this proposal)

`SchemaId` is the seed for a future port-contract feature: when a
workflow declares `port_in_schema = SchemaId("zeroflat::Form/v1")` and
`port_out_schema = SchemaId("zeroflat::Form/v1")`, validation can
reject mismatched edges at workflow-load time without running the
graph. This belongs to the `pureflow-contract` crate work tracked in
proposal_final.md §6.5, not to this bead set. Mentioned only so that
`SchemaId` is shaped right today for that future use.

## 7. Library recommendations

Re-check before pinning. Versions current at 2026-05-03.

| Crate | Version | Where | Why |
|---|---|---|---|
| `postcard` | `1.1.3` | `pureflow-core` dev/feature-gated | Compact serde codec, no_std-friendly, stable since 1.0 |
| `serde` | `1.0.228` (workspace) | already present | Required by `postcard` impl |
| `thiserror` | `2.0.18` | `pureflow-core` (already used) | `SerializeError` typed variants |

No new mandatory deps. `postcard` activates only with `serde-postcard`.

## 8. Roadmap fit

This is an additive change. It does not block any existing Phase 1–4
work in `proposal_final.md` §8 and does not require any current bead to
be reopened.

Suggested sequencing within Phase 2 (Contracts/Formats/Inspection):

- After `contracts-core` lands the `SchemaRef` type — to ensure
  `SchemaId` shape harmonizes with port contract schemas.
- Before `wasm-batch-trait` is fully wired with structured input — so
  the WASM host already knows how to serialize `Structured` payloads.

## 9. Candidate beads

Concrete bead breakdown for the user to file via `bd`/`br`. Sized for
one JJ change each.

1. `core-data-packet-trait`: add `DataPacket`, `SchemaId`,
   `SerializeError` to `pureflow-core` (no postcard dep). Pure trait +
   types + unit tests for downcast and equality semantics.
2. `core-structured-variant`: add `PacketPayload::Structured(Arc<dyn
   DataPacket>)`, helper constructors (`structured`, `as_structured`,
   `downcast`), and update `as_bytes`/`as_control`/`as_arrow` exhaustive
   matches. Update existing tests; add structured-roundtrip tests.
3. `core-serde-postcard-feature`: add `serde-postcard` feature plus
   `PostcardPacket<T>` impl, with tests covering wrap → downcast,
   wrap → serialize → deserialize round-trip, and `schema_id`
   stability.
4. `engine-structured-passthrough-tests`: prove `Structured` payloads
   pass cleanly through bounded edges, fan-out, fan-in, and
   cancellation paths. No engine code changes expected — this is a
   regression suite.
5. `wasm-structured-serialization` *(deferred until after
   `wasm-batch-trait` lands)*: WASM host calls `serialize()` on
   `Structured` payload before invoking guest; guest output returns as
   `PacketPayload::Bytes`.

Beads 1–4 are independent of the WASM Phase. Bead 5 lands when WASM
work resumes.

## 10. Risk register

| Risk | Severity | Mitigation |
|---|---|---|
| `Arc<dyn DataPacket>` indirection cost | Low | One vtable hop per access; negligible vs. the avoided per-edge encode. Benchmark with a Criterion case alongside `bench-metadata-overhead`. |
| Trait-object equality footgun | Low | `Eq` not implemented for the `Structured` variant; document explicitly. |
| Schema-id collision across consumers | Medium | `SchemaId(&'static str)` uses string identity; consumers should namespace (e.g., `"zeroflat::Form/v1"`). Document the convention; defer enforcement to the future contracts work. |
| Feature-flag combinatorics | Low | `serde-postcard` adds one feature; matrix already covers `arrow` and `tracing`. CI matrix grows by one column. |
| Postcard schema fragility (field order) | Medium (consumer-side) | This is a consumer-discipline issue, not Pureflow's problem. Document that `PostcardPacket<T>` users must manage `T`'s schema evolution themselves (versioned envelope pattern). |
| Async runtime substrate leakage | None | `DataPacket` and `PostcardPacket<T>` reference no runtime types. |

## 11. Open questions

- **`SchemaId` shape:** `&'static str` keeps it Copy and zero-alloc, but
  forces compile-time literals. Alternative: `Arc<str>` for runtime-built
  ids (e.g., from workflow files). Recommendation: start with
  `&'static str`, widen later if needed.
- **`DataPacket: Clone`?** Trait objects can't require `Clone` directly.
  `Arc<dyn DataPacket>` clones the pointer cheaply, which is the
  intended sharing model. Not adding `Clone` to the trait.
- **`PartialEq` on the variant:** as drafted, `PacketPayload` loses `Eq`
  unconditionally because `Arc<dyn DataPacket>` cannot derive `Eq`.
  Acceptable because `Arrow` already triggered the same conditional.
  Worth confirming no downstream code relies on `Eq`.
- **Cross-process reconstruction:** out of scope for this proposal. When
  the time comes, a separate `DataPacketRegistry` trait can map
  `SchemaId` → constructor without changing the core trait.

## 12. References

- `docs/archetecture/proposal_final.md` §6.9 — payload tiering
- `crates/pureflow-core/src/message.rs` — current `PacketPayload`
- `crates/pureflow-core/src/batch.rs` — current `BatchInputs`/`BatchOutputs`
- `docs/questions/2026-04-28_wasm_batch_trait.md` — WASM boundary open
  follow-ups (relevant to bead 5)
- `postcard`: https://docs.rs/postcard/latest
- `bytes`: https://docs.rs/bytes/latest
