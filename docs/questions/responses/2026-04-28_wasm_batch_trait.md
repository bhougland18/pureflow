# Response: WASM Batch Trait Questions

Reviewing `crates/pureflow-core/src/batch.rs` against `proposal_final.md`
sections 6.10, 8 (Phase 4), and the §9 risk register.

## Q1. Empty input batches: accept everywhere, or per-adapter minimum policy?

**Recommendation: keep the trait permissive — empty `BatchInputs` is legal.
Defer any minimum-input policy until a concrete adapter forces the question.**

Reasoning grounded in the current code:

- `BatchInputs` is a `BTreeMap<PortId, Vec<PortPacket>>` and the only readers
  in the public surface are `packets(port_id) -> &[PortPacket]` and
  `packets_by_port()` (`pureflow-core/src/batch.rs:42`, `:48`). The data shape
  already permits "this port has zero packets" and "no ports at all" without
  a separate signal — adding a minimum policy now means inventing a new
  failure mode the type does not currently have.
- The host-owned channel model in §6.10 places batching responsibility on the
  engine: "host reads batches from `PortsIn`". Whether a batch is empty is a
  *scheduling* question (did the host accumulate any packets in this window?),
  not an *adapter* question. Pushing the policy into `BatchExecutor` would
  invert that responsibility.
- A per-adapter minimum is the kind of pluggable policy the proposal warns
  against landing before there is a real forcing function — the Wasmtime
  adapter is still `cdt-mcu.2` and unbuilt, so we have no concrete WASM node
  whose semantics demand a non-zero floor. The risk register in §9 lists
  "Wasmtime release churn" as the relevant medium risk; the mitigation is to
  hide engine-specific policy *behind* `pureflow-wasm`, not inside the trait.
- The current `EchoBatchExecutor` test
  (`pureflow-core/src/batch.rs:185`) already implicitly relies on permissive
  input handling: it iterates `packets(&port_id("in"))`, which returns `&[]`
  when absent. Tightening to "must be non-empty" would either break that
  pattern or force every adapter to add a zero-input branch.

Concrete next step inside `cdt-mcu.1` follow-up:

- Add a doc line at the top of `BatchExecutor` (`batch.rs:110`) stating:
  *"Adapters MUST accept empty `BatchInputs` and return an empty
  `BatchOutputs` unless the adapter has a documented reason to fail. Batch
  shaping is a host concern."* That converts an implicit invariant into a
  contract reviewers can hold the Wasmtime adapter to.
- If a future WASM module needs a minimum (e.g., a fixed-shape Arrow batch
  node), express it as a per-node *contract* claim, not a `BatchExecutor`
  trait method — that keeps the security/validation surface in one place
  (per the §6.7 introspection rule).

## Q2. Output validation: in the adapter or in the engine?

**Recommendation: validate in the engine, not in `BatchExecutor`. The trait
should remain unaware of workflow topology.**

Reasoning:

- §6.10 spells out the host-side pipeline literally: *"host invokes the WASM
  component / host validates output envelopes / host sends through normal
  `PortsOut`."* The adapter's job in that sequence is "produce a
  `BatchOutputs`"; envelope validation is the next step, owned by the host
  engine that already holds the contract and the `PortsOut` handles.
- `BatchExecutor` today has no access to contracts: it sees `BatchInputs` and
  returns `BatchOutputs` (`batch.rs:110-117`). Giving it validation
  responsibility would mean either passing a `&NodeCapabilities` /
  `&NodeDefinition` into `invoke`, or duplicating port allowlists inside
  every adapter. Both options widen the trait and break the "runtime-neutral"
  property §6.10 calls out as the whole point of staging the trait first.
- The §9 risk register lists "AI workflows bypass validation" as High and
  "Native capabilities mistaken for sandboxing" as Medium. Both mitigations
  require validation to live at a uniform, inspectable boundary. If the
  Wasmtime adapter is the validator, native batch executors and any future
  process adapter would each have their own validator with potentially
  drifting semantics; the engine boundary is the only place that gives one
  enforcement point for all execution modes.
- Engine-side validation also lines up with the existing
  `validate_workflow_contracts` work in `pureflow-core::capability` — that
  module is already where "permitted port use" is decided. Output envelope
  checking belongs next to it, not inside `pureflow-wasm`.

Concrete refinements:

- When the engine consumes a `BatchOutputs`, it should reject any port id
  not present in the node's declared `Emit` `PortCapability` set, mapping
  the failure to `PureflowError::Validation` (so it surfaces with the existing
  `User`-visibility error code rather than as an opaque adapter error).
- The Wasmtime adapter should still *trap* obvious shape errors from the
  guest (e.g., malformed envelope bytes that fail to deserialize into a
  `PortPacket` at all) before returning — that is "did the guest produce a
  well-formed value", which is engine-internal and different from "is this
  port allowed".
- Capture this split as a doc fragment on `BatchExecutor` so the Wasmtime
  bead does not accidentally re-implement port validation inside the
  adapter.

## Q3. Synchronous trait for the MVP, or async cancellation seam?

**Recommendation: keep `BatchExecutor::invoke` synchronous for the Wasmtime
MVP. Implement cancellation via Wasmtime epoch interruption inside the
adapter, and let the engine wrap the blocking call on a blocking task.**

Reasoning:

- The current trait is `fn invoke(&self, inputs) -> Result<BatchOutputs>`
  (`batch.rs:110-117`). The justification in the question — "lets the later
  Wasmtime adapter own engine-specific invocation, limits, and cancellation
  behavior" — is exactly the §6.10 framing. Going async at the trait level
  pulls a runtime substrate concern (which executor / which Future shape /
  whether `Send` is required) into the runtime-neutral surface, which §10
  explicitly lists as a thing to keep behind the runtime/port adapters.
- Wasmtime supports prompt, *synchronous* cancellation through epoch
  interruption: install an epoch on the engine, bump it from another thread
  (or from the engine's cancellation handler), and the in-flight guest call
  traps. That mechanism does not require the trait to be async. The
  `CancellationHandle` plumbed through `cdt-rtb.7` already gives us a place
  to wire that bump on cancellation, sitting next to the existing
  `Cancellation` error variant in `pureflow-core::error`.
- Async-at-the-trait costs more than it buys here. Making `invoke` return a
  `Future` would either force `BoxFuture<'_, Result<BatchOutputs>>` (an
  allocation per call) or introduce a GAT (`type InvokeFuture<'a>`),
  mirroring the complexity of `NodeExecutor::RunFuture` (`lib.rs:39-52`). For
  a batch boundary that wraps a guest call which is itself blocking from
  Rust's perspective, this is async-by-syntax, not async-by-behavior.
- The engine already has the right tool: a blocking-task seam via
  `asupersync` plus the cancellation handle. The Wasmtime adapter only needs
  to publish a synchronous cancellation hook that the engine triggers when a
  cancel arrives. That keeps the §9 "Sequential runner masks FBP bugs"
  mitigation honest — cancellation is observable promptly without
  re-shaping the trait.
- If a later adapter genuinely needs async-native invocation (e.g., a WASI
  HTTP host call where the guest yields back to async Rust), introduce a
  parallel trait such as `AsyncBatchExecutor` at that point. Two narrow
  traits beat one wide trait that forces today's MVP to allocate a Future
  for a synchronous call.

Concrete refinements:

- Have `cdt-mcu.2` expose a small adapter-side `CancelHook` (e.g.,
  `fn cancel(&self)` on the `WasmModule` or a sibling type) that the engine
  invokes when its `CancellationToken` flips. The adapter's implementation
  bumps the Wasmtime epoch.
- Document on `BatchExecutor` that "invocation may block; the engine is
  responsible for running it on a blocking-friendly task and for invoking
  the adapter's documented cancellation hook on cancel." That captures the
  contract without expanding the trait.

## Summary

- Empty input batches: legal at the trait level. Adapter-side minimums are a
  contract concern, not a `BatchExecutor` concern. Document the rule on the
  trait.
- Output validation: belongs in the engine, against declared port
  capabilities. The adapter only checks envelope well-formedness, not port
  allowlists. Keep `BatchExecutor` topology-blind.
- Sync vs. async: stay sync for the Wasmtime MVP. Wire cancellation through
  epoch interruption plus a small adapter-side cancel hook driven by the
  existing `CancellationHandle`. Add an `AsyncBatchExecutor` only when a
  concrete async-native adapter forces it.

All three answers preserve the §6.10 invariant that the batch trait is the
runtime-neutral seam and that engine-specific concerns live in
`pureflow-wasm`.
