# Epic 1: Pureflow Foundation

Goal: establish the smallest honest base for Pureflow so later work has stable types, validation rules, runtime boundaries, and documentation for the decisions that matter.

This epic is intentionally split on design boundaries, not on file count. A bead should map to a reviewable JJ change and a coherent decision surface.

## Bead Tree

### 1. Workspace Skeleton

Scope:
- Create the Rust workspace layout.
- Wire the existing crates into a coherent workspace.
- Add the shared developer entry points and any minimal repo plumbing needed for iteration.

Why this is one bead:
- It is a pure scaffolding step with no real product semantics yet.
- It establishes the repository shape everything else depends on.

Acceptance:
- Workspace builds.
- Crates resolve cleanly.
- No behavior beyond minimal smoke-level code.

Verso:
- Add a short architecture note explaining the crate boundaries and why they exist.

Tests and gates:
- `cargo check --all-targets`
- `cargo fmt --check`
- `cargo clippy --all-targets -- -W clippy::pedantic -W clippy::nursery`
- `cargo dylint --all`

### 2. Identity Primitives

Scope:
- `WorkflowId`
- `NodeId`
- `PortId`
- Any associated parsing, formatting, or generation rules

Why this is a bead:
- These are foundational Rust types with distinct invariants.
- The shape of these IDs affects validation, serialization, and every downstream API.

Acceptance:
- Identity types are distinct and minimal.
- Invariants are explicit and tested.
- The types are easy to use without collapsing into plain strings.

Verso:
- Document why each type exists, what invariant it protects, and what bug it prevents.

Tests and gates:
- Unit tests for equality, formatting, parsing, and invalid inputs.
- Property tests if there is normalization or generation logic.
- Standard lint and Dylint gates.

### 3. Workflow Model

Scope:
- Workflow definition types.
- Node and edge model.
- Port topology and graph-level structure.

Why this is a bead:
- This is the first semantic model of the system.
- It determines what the engine can represent, not just how it executes.

Acceptance:
- The model can represent a valid workflow.
- Invalid graphs are rejected for structural reasons.
- The model stays separate from runtime behavior.

Verso:
- Explain why the graph is shaped this way and which constraints are structural versus semantic.

Tests and gates:
- Validation tests for valid and invalid graphs.
- Property tests if invariants can be expressed generatively.
- Standard lint and Dylint gates.

### 4. Execution Context and Message Envelope

Scope:
- Runtime-facing context type.
- Message or payload envelope type.
- Execution metadata that must travel with work.
- Cancellation and lifecycle hooks at the type boundary.

Why this is a bead:
- This is where the runtime contract becomes concrete.
- The choice of fields here will shape `asupersync` usage and future node APIs.

Acceptance:
- The boundary types define how runtime code communicates.
- The types are stable enough to support future engine work.
- The design does not hard-code an implementation too early.

Verso:
- Explain the runtime boundary and why the envelope/context contains the fields it does.

Tests and gates:
- Serialization or round-trip tests if these types cross process or storage boundaries.
- Invariant tests for lifecycle or metadata rules.
- Standard lint and Dylint gates.

### 5. Capability and Boundary Types

Scope:
- Types that express what a node may receive, emit, or do.
- Capability descriptors or permission-like boundaries.
- Any type that constrains runtime behavior rather than workflow shape.

Why this is a bead:
- These types encode a design decision about isolation and runtime safety.
- They are likely to be revisited later, so the rationale should stay close to the code.

Acceptance:
- Capability rules are explicit and separate from workflow structure.
- Invalid combinations are rejected.
- The boundary is understandable from the types alone.

Verso:
- Document the security/runtime tradeoffs and why the boundary exists.

Tests and gates:
- Validation tests for allowed and disallowed combinations.
- Standard lint and Dylint gates.

### 6. Error Model

Scope:
- Shared error enum(s).
- Error codes.
- User-facing versus internal classification.
- Recovery or retry categories if needed.

Why this is a bead:
- Error shape is part of the public contract.
- It affects CLI behavior, future API behavior, and test expectations.

Acceptance:
- Errors are structured and stable enough for downstream use.
- Error categories are explicit.
- Error mapping is deterministic.

Verso:
- Explain the error taxonomy and why it is shaped that way.

Tests and gates:
- Tests for code mapping, formatting, and classification.
- Standard lint and Dylint gates.

### 7. Test Kit

Scope:
- Builders and fixtures.
- Fake nodes or test doubles.
- Property-test helpers and reusable invariants.

Why this is a bead:
- Later beads need a low-friction way to test model and runtime behavior.
- The helpers themselves may encode non-obvious testing strategy.

Acceptance:
- Other beads can use the helpers without repetition.
- The helpers reduce, not increase, test friction.

Verso:
- Only if the helpers encode a non-obvious strategy.

Tests and gates:
- Tests for helper logic if it is non-trivial.
- Standard lint and Dylint gates.

### 8. Documentation Pass

Scope:
- Add Verso annotations to the decision-heavy modules above.
- Make sure the narrative matches the code.
- Capture the rationale for the foundational decisions while they are still fresh.

Why this is a bead:
- The literate layer is part of the design boundary, not an afterthought.

Acceptance:
- The “why” is close to the code for all foundational decisions.
- The docs do not drift away from the implementation.

Verso:
- This bead is the main Verso payoff.

Tests and gates:
- No new behavior.
- Re-run the normal lint and Dylint gates to ensure the doc work did not introduce code drift.

## Ordering

Recommended order:
1. Workspace Skeleton
2. Identity Primitives
3. Workflow Model
4. Execution Context and Message Envelope
5. Capability and Boundary Types
6. Error Model
7. Test Kit
8. Documentation Pass

## Split Rules

- Split a bead when a type introduces a new invariant, runtime boundary, or serialized contract.
- Keep trivial wrappers and obvious helpers inside the bead that owns the surrounding decision.
- Use Verso where the future reader would reasonably ask, "why is it shaped like this?"
- If a bead becomes too broad to review in one JJ change, split it at the nearest stable boundary and preserve the dependency order.
