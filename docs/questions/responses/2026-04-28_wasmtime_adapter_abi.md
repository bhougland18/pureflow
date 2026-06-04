# Response: Wasmtime Adapter ABI Questions

Reviewing the question against `proposal_final.md` §6.10, `proposal_1.md`
§5.5 and §6 (recommended deps), and the current shapes of `BatchExecutor`,
`PacketPayload`, and `MessageMetadata` in `pureflow-core`.

## Recommendation

**Adopt the Component Model / WIT convention for the first Wasmtime
adapter. Reject the host-function pull/push convention. Treat the
core-Wasm export convention as a fallback only if Component Model tooling
proves unworkable on the `cdt-mcu.2` timebox.**

Rank: WIT (preferred) > core export (acceptable spike) > host pull/push
(rejected).

## Why Component Model / WIT

- **The proposal already commits to it.** `proposal_1.md` §6 lists
  `wit-bindgen 0.56.0` as the WIT bindings dep "for guest nodes," and
  `proposal_1.md` §5.5 (which `proposal_final.md` §6.10 inherits in its
  short form) describes the host calling "a WASM component with message
  metadata and payload bytes." "Component" is a load-bearing word here:
  Wasmtime's `Component` API and core `Module` API are different code
  paths. Picking the WIT route is the documented direction.
- **It is the wrapper boundary `pureflow-wasm` already promises.**
  `proposal_final.md` §6.10 says to hide Wasmtime release churn behind
  `pureflow-wasm` and `BatchExecutor`. The WIT package *is* the part of
  that wrapper that guest authors and AI tooling actually see — versioning
  it (e.g., `pureflow:batch@0.1.0`) gives us the explicit ABI handle we
  need to evolve payload tiers (`cdt-rpk.4`, `cdt-rpk.5`, future Arrow)
  without rewriting every guest. A core-Wasm export convention pushes
  that evolution onto manual length-prefix / version-byte juggling that
  we will end up re-deriving the bad parts of WIT for.
- **It lines up with the Phase 4 exit criteria.** §8 Phase 4 lists
  "capability enforcement at the WASM boundary" and "denied capabilities
  fail with stable errors." Component Model + (eventually) WASI 0.2 is the
  natural shape for capability-gated *imports*: when a guest later needs
  filesystem or network access, those become WIT import functions the
  host declines to wire when the contract does not declare the matching
  `EffectCapability`. With a core-Wasm export convention, capability
  gating turns into ad hoc import-table editing.
- **It matches the AI-inspection story in §6.7.** A versioned WIT package
  is itself an introspectable artifact: callers can answer "what shape
  does this WASM node expect?" by reading the WIT, not by reverse-
  engineering envelope bytes. Core exports do not give you that.
- **Cancellation and validation answers do not change.** Component Model
  still uses the same Wasmtime `Engine` epoch interruption mechanism, so
  the synchronous-trait + epoch-bump plan from
  `2026-04-28_wasm_batch_trait.md` carries over unchanged. Engine-side
  output port validation (also from that response) likewise does not move
  into the adapter — the WIT contract enforces *shape* well-formedness;
  the engine still owns *port allowlist* enforcement.

## Why not host-function pull/push

`proposal_1.md` §5.5 is explicit: *"Do not expose direct channel
operations inside WASM in the MVP. That would mix sandboxing, scheduling,
and backpressure before the host contract is proven."* Pull/push imports
are the channel shape in disguise — the guest decides batch boundaries,
read pacing, and emit ordering instead of receiving a host-shaped batch.
That is exactly the inversion §6.10 is set up to prevent. Reject this
option; it is the only one that fights the host-owned-channel invariant.

## Why core-Wasm export is only a fallback

A `pureflow_invoke(input_ptr, input_len) -> output_handle` ABI plus
`alloc` / `dealloc` exports is genuinely the smallest path to a working
demo, and it has the benefit of not depending on `wit-bindgen` tooling.
But once you write the second guest in a non-Rust language, or once you
want to evolve the envelope to add Arrow without breaking older guests,
you start re-implementing version negotiation, list/option encodings,
and import gating by hand. Use core export only if the `cdt-mcu.2`
implementer hits a concrete blocker on `wit-bindgen 0.56.0` or Wasmtime
43.x Component support — and if that happens, treat it as a deliberate
spike with a follow-up bead to migrate to WIT before sample guests
proliferate.

## Concrete WIT-shape sketch

The MVP package only needs to encode what `BatchInputs` / `BatchOutputs`
already carry. A first cut:

```wit
package pureflow:batch@0.1.0;

interface batch {
    record endpoint {
        node-id: string,
        port-id: string,
    }

    record route {
        source: option<endpoint>,
        target: endpoint,
    }

    record execution {
        execution-id: string,
        attempt: u32,
    }

    record message-metadata {
        message-id: string,
        workflow-id: string,
        execution: execution,
        route: route,
    }

    variant payload {
        bytes(list<u8>),
        control(string),         // JSON-encoded for now
    }

    record packet {
        metadata: message-metadata,
        payload: payload,
    }

    record port-batch {
        port-id: string,
        packets: list<packet>,
    }

    variant batch-error {
        guest-failure(string),
        unsupported-payload(string),
    }

    invoke: func(inputs: list<port-batch>) -> result<list<port-batch>, batch-error>;
}

world pureflow-node {
    export batch;
}
```

Notes on the sketch:

- `list<port-batch>` mirrors the `BTreeMap<PortId, Vec<PortPacket>>` in
  `pureflow-core/src/batch.rs:11` and `:61` while avoiding the question of
  Component Model map representation. Order is preserved. The host can
  reject duplicate `port-id` entries on the way back into `BatchOutputs`.
- `payload` matches today's `PacketPayload::Bytes` and
  `PacketPayload::Control` (`pureflow-core/src/message.rs:11`). When Arrow
  lands, it becomes a third variant gated by a feature in `pureflow-wasm`,
  and the WIT package version bumps to `0.2.0`. Older guests still link.
- `control(string)` carries JSON as text for the MVP. That avoids
  introducing a Component Model representation for arbitrary
  `serde_json::Value` before we need one.
- The world exports `batch` and **imports nothing** in the MVP. Capability
  enforcement is "no host imports linked," so a denied capability is a
  link-time failure with a stable error. When `EffectCapability::Clock`
  or `NetworkOutbound` are first needed, they get added as imports in the
  same world and the host wires them only when the node's
  `NodeCapabilities` declares the matching effect.
- Errors crossing the boundary are a `result<_, batch-error>`. The
  Wasmtime adapter maps `batch-error::guest-failure` to
  `PureflowError::Execution` (visible) and any host-side decode failure to
  `PureflowError::Execution` with an internal-visibility code, so the
  failure shape is uniform in `metadata::MetadataRecord`.

## Concrete next steps for `cdt-mcu.2`

- Add the WIT package above under `crates/pureflow-wasm/wit/` and pin
  `wit-bindgen` and `wasmtime` versions in that crate's `Cargo.toml`. Do
  not re-export them from `pureflow-core`.
- Build guests as `wasm32-wasip2` components (not core modules). Load via
  `wasmtime::component::Component::new` + a `Linker` that imports
  *nothing* in the MVP world.
- Implement a `WasmtimeBatchExecutor: BatchExecutor` that:
  1. Maps `BatchInputs` → `list<port-batch>` once per call.
  2. Calls the typed `invoke` binding generated from the WIT.
  3. Maps the returned `list<port-batch>` → `BatchOutputs` (still no port
     allowlist check — that stays in the engine per the prior response).
  4. Bumps the engine's epoch on cancellation (the synchronous-trait +
     cancel-hook plan from `2026-04-28_wasm_batch_trait.md`).
- Pin a single Component Model preview level (currently preview2 / WASI
  0.2). Document the choice on the WIT file's package version line so
  future ABI bumps are visible to reviewers.
- Add one example guest under `crates/pureflow-wasm/examples/echo/` that
  implements the `batch` export by mirroring inputs to outputs. That
  satisfies the §8 Phase 4 "one sample WASM node" deliverable and gives
  `wasm-native-mixed-example` (`cdt-32` in the bead numbering) something
  concrete to compose with.
- Track the "core-export fallback" as a documented rejected alternative
  in the bead description, not as a feature flag. Keeping a second ABI
  alive in parallel is the worst of both worlds.

## Summary

- Pick Component Model / WIT for the first Wasmtime adapter. It is the
  shape the proposals already point at, the natural place to evolve
  payload tiers, and the cleanest seam for capability-gated imports.
- Reject the host-function pull/push convention outright — it
  reintroduces channel-shape into the guest in violation of §6.10.
- Treat core-Wasm export as a documented fallback for unforeseen tooling
  blockers, not a parallel design.
- Keep the prior decisions intact: synchronous `BatchExecutor` with
  epoch-based cancellation, engine-side output port validation, and
  permissive empty batches. The ABI choice does not change those.
