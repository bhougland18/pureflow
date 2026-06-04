# Arrow Schema Compatibility Plan - 2026-05-11

This note resolves `cdt-pyg.1`: define how Pureflow should treat Arrow schema
compatibility before any DataFusion or broader data-tier work resumes.

## Current State

`pureflow-core` already exposes `PacketPayload::Arrow(RecordBatch)` behind the
optional `arrow` feature. Default builds remain Arrow-free.

`pureflow-contract` already has `SchemaRef`, an opaque non-empty string attached
to contract ports. Edge schema compatibility is exact equality when both
endpoints declare a schema. No schema registry, Arrow parser, or structural
comparison exists today.

Epic 17 workload pressure did not justify DataFusion or an Arrow-first runtime.
All completed workloads use small UTF-8 byte payloads or control messages.

## Decision

Keep Arrow compatibility as a contract-level string identity check for now.
Do not add structural Arrow schema comparison, a schema registry, or DataFusion
dependency in this bead.

Recommended Arrow schema refs:

```text
schema://arrow/<domain>/<name>/v<major>
schema://arrow/<domain>/<name>/v<major>#<producer-or-fixture-note>
```

Examples:

```text
schema://arrow/sensors/reading/v1
schema://arrow/analytics/customer-event/v2
schema://arrow/benchmarks/int64-single-column/v1
```

Compatibility means the two connected ports declare the same `SchemaRef`
string. Authors own the meaning behind that string: field names, field order,
types, nullability, metadata, dictionary encoding, timezone handling, and schema
evolution policy.

## Compatibility Rules

1. If neither endpoint declares a `SchemaRef`, Pureflow performs no schema
   compatibility check.
2. If one endpoint declares a `SchemaRef` and the other does not, current
   validation accepts the edge. Authors should avoid this for Arrow edges unless
   the missing side is intentionally generic.
3. If both endpoints declare `SchemaRef` values, validation requires exact
   string equality.
4. `PacketPayload::Arrow` does not automatically prove that a runtime
   `RecordBatch` matches the contract schema. Runtime payload validation is a
   future feature, not part of this plan.
5. Arrow schema references are versioned by the author. A breaking Arrow schema
   change should use a new major ref such as `v2`.

## Metadata Implications

Do not add Arrow schema details to stable message metadata yet.

Current `message` metadata intentionally records routing and execution metadata,
not payload bodies or payload shape. That remains correct for Arrow. Recording a
full Arrow schema, field list, or buffer detail in every message record would
make metadata noisy and couple JSONL stability to Arrow internals.

A future payload-shape metadata bead may add sampled data-tier facts such as:

```rust
PayloadShape::Arrow {
    schema_ref: Option<SchemaRef>,
    row_count: usize,
    column_count: usize,
    buffer_bytes: Option<usize>,
}
```

That should be recorded through `TieredMetadataSink::record_with_tier` as data
or high-cost data, not as default control-plane metadata.

## Capability Implications

Arrow is a payload representation, not an external effect. It does not require a
new `EffectCapability`.

DataFusion execution also should not be represented as an effect by itself. A
future DataFusion node may need effect capabilities only for external actions it
performs, such as reading files, opening network connections, or querying an
external database. In-memory query execution over received Arrow batches is
ordinary node computation.

This means Arrow schema planning should not block the separate external-effect
capability and metadata work surfaced by the AI orchestration workload.

## Boundary Semantics

Native nodes may exchange `PacketPayload::Arrow` when the `arrow` feature is
enabled. The feature flag keeps default builds and non-Arrow users free of Arrow
dependencies.

WASM nodes currently accept bytes/control payloads through the WIT boundary.
Arrow batches are not coerced into bytes automatically. A future WASM Arrow
bridge must define explicit serialization and validation behavior before Arrow
payloads cross into a guest.

CLI workflow files should continue to refer to schemas through `SchemaRef`
strings. They should not embed Arrow schema JSON unless a future schema-registry
feature is explicitly designed.

## Non-Goals

- No DataFusion node crate.
- No Arrow schema registry.
- No structural Arrow schema equivalence.
- No runtime `RecordBatch` versus `SchemaRef` validator.
- No metadata field list or buffer dump.
- No change to `PacketPayload` or port APIs.

## Future Beads

Keep these deferred until a concrete Arrow workload exists:

- `cdt-pyg.2`: DataFusion node crate spike.
- `cdt-pyg.3`: Arrow copy and latency benchmarks.

Reasonable future beads after a real workload appears:

- Add an Arrow fixture workflow that passes a small `RecordBatch` through native
  nodes under `--features arrow`.
- Add contract tests documenting exact `SchemaRef` equality for Arrow refs.
- Add sampled `PayloadShape::Arrow` metadata, gated by tiered metadata policy.
- Add runtime `RecordBatch` schema validation if a workload needs host-enforced
  Arrow compatibility rather than author-owned schema refs.

## Summary

The data-tier compatibility rule is deliberately simple: Arrow payloads stay
feature-gated, schemas are named with stable `SchemaRef` strings, and connected
ports are compatible when those strings match. This keeps Arrow available for
native high-throughput experiments without pulling DataFusion or structural
schema policy into the core runtime.
