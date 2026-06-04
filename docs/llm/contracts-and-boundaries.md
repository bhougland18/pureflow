# Contracts and Boundaries

## Public Contracts

The main public contract surfaces are:

- Workflow contract: topology, ports, and edge capacities.
- Node contract: input/output expectations and execution constraints.
- Capability surface: what a node is allowed to do in a run environment.
- Runtime trait surface: how executors integrate with orchestration.
- Metadata/error surface: stable machine-facing event and terminal summary shape.

The important rule is that each surface stays narrow and explicit.
Workflow structure is not the same thing as node behavior, and execution
adapters should not redefine the workflow model.

## Invariants

Non-negotiable invariants:

- Structural graph validity is enforced before execution.
- Contract/capability mismatch blocks startup.
- Port schema incompatibility is detected before runtime scheduling.
- Metadata and error record families stay stable for downstream automation.
- Runtime adapters do not redefine core workflow semantics.
- Public APIs should not leak substrate-specific types.

Practical reading:

- If a feature changes topology rules, it belongs in workflow validation.
- If a feature changes what a node may do, it belongs in contract/capability policy.
- If a feature changes how work runs, it belongs in runtime/engine layers.
- If a feature changes the event stream, it must preserve stable record families.

## Compatibility Notes

Compatibility expectations are additive whenever possible:

- Workflow format support should be versioned explicitly when breaking changes are unavoidable.
- Contract and capability fields should evolve with defaults or additive variants.
- Metadata record families and discriminators should remain stable.
- Run summary fields should remain predictable for CI and scripting.
- New execution backends should fit behind existing Pureflow-owned contracts instead of broadening the public runtime model.
