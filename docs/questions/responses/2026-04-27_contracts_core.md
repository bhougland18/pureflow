# Response: Contracts Core Questions

Reviewing `crates/pureflow-contract/src/lib.rs` against `proposal_final.md`
sections 6.5 and 9.

## Q1. Schema compatibility: exact-equality vs. policy object

**Recommendation: keep exact equality. Defer the policy object.**

Reasoning grounded in current code:

- `SchemaRef` is an opaque `String` today (`pureflow-contract/src/lib.rs:30`).
  There is nothing structural to negotiate over. A "policy object" right now
  would be a wrapper around `==` with no actual decision logic to dispatch on,
  which is exactly the kind of premature abstraction `proposal_final.md` warns
  against ("don't add Arrow/DataFusion to core until the byte-message runtime
  and WASM boundary are stable" — same principle).
- The current rule in `validate_edge_schema_compatibility` is already the right
  minimal policy: when both endpoints declare a schema, require exact equality;
  when either side omits a declaration, skip the check. That is a coherent,
  documentable rule.
- A policy object becomes meaningful when one of two things lands:
  1. A schema registry that resolves `SchemaRef` to a structured schema shape
     (so "compatibility" can mean width/nullability/version checks), or
  2. The tiered payload work (`cdt-rpk.4` and follow-ons), where cross-tier
     rules — e.g., "Bytes is compatible with Structured only when an explicit
     adapter is declared" — actually need to be expressed.
- Workflow-format parsing does not by itself motivate a policy object. JSON or
  TOML files will produce the same `SchemaRef` strings the in-memory builder
  produces; equality continues to be the natural check.

Concrete next step inside the contracts bead:

- Add a brief doc comment at the head of `validate_edge_schema_compatibility`
  that names the rule explicitly: *"Edge schema check is exact equality of
  declared `SchemaRef` values when both sides declare one. Pluggable
  compatibility is deferred until `SchemaRef` carries structured shape
  information."*
- Track the pluggable-policy decision under a future bead (e.g.
  `contracts-schema-policy`) gated on either a schema registry or a tiered
  payload landing, not on workflow-format parsing.

This closes the question without creating an open-ended TODO.

## Q2. Capability descriptor: required everywhere or optional for native?

**Recommendation: keep capability descriptors required for every node. Make
"native, no effects" cheap to author.**

Reasoning:

- The risk register in `proposal_final.md` §9 lists "AI workflows bypass
  validation" as High severity with the mitigation "CLI/API must validate
  before run; no best-effort execution". Allowing missing capability metadata
  on native nodes opens precisely that gap: at validation time you cannot tell
  the difference between *"this node legitimately requests no host effects"*
  and *"this contract was never given a capability declaration."* The first is
  meaningful and inspectable; the second is silently unauthored.
- Native enforcement is advisory today, but the descriptor is still the only
  place that says "this node will touch the network / clock / filesystem."
  Stripping it on native nodes would create a two-tier inspection surface
  (introspection JSON would have to special-case missing descriptors), which
  contradicts §6.7's "introspection as pure data over workflow + contracts."
- The cost of requiring it everywhere is low. Looking at
  `pureflow_core::capability::NodeCapabilities`, a "no effects" descriptor is
  just `NodeCapabilities::new(node_id, port_capabilities, [])`. The redundancy
  with workflow port topology is intentional and called out in
  `capability-port-claims` — so making port-capability authoring ergonomic is
  a separate concern.
- Required-everywhere is also the right shape for the AI inspection story: a
  caller can answer "which nodes can spawn processes?" by scanning every
  contract uniformly, rather than having to compose `contract.is_some() &&
  contract.effects.contains(...)`.

Concrete refinements that keep the requirement comfortable:

- Add a `NodeCapabilities::native_passive(node_id, port_capabilities)` (or
  similarly named) constructor that fills in `[]` effects. This makes the
  trivial native case a one-liner, removing the only real ergonomic argument
  for relaxing the requirement.
- Consider adding a `validate_workflow_contracts` test that proves a workflow
  with a node missing only its capability descriptor fails with
  `MissingCapabilityDescriptor` and not a less specific error — the current
  test suite covers schema mismatch but not this path explicitly.

## Summary

- Schema compatibility: stay with exact equality. Document the rule in
  `validate_edge_schema_compatibility`. Track a future policy bead, gated on
  either a schema registry or tiered payloads — not on format parsing.
- Capability descriptors: stay required for every node. Add a "no effects"
  convenience constructor to make native-only workflows trivial to author.

Both decisions favor closing the open follow-ups now rather than leaving them
open against future work that does not yet have a forcing function.
