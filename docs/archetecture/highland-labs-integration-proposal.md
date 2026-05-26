# Pureflow Reuse Proposal for `highland-labs`

Date: 2026-05-19

## 1. Purpose

This proposal describes how the `pureflow` Rust library should be used as a
foundation inside the separate `highland-labs` repository.

The goal is to reuse the parts of `pureflow` that represent durable workflow
infrastructure while preserving the `cargo-polylith` structure that
`highland-labs` is already reserving for its Rust workspace.

The proposal is not to copy the entire `pureflow` repository wholesale.
Instead, `pureflow` should be treated as a source of reusable ownership
boundaries:

- workflow model
- validation and contract types
- runtime and execution boundaries
- WASM adapter logic
- introspection surfaces

## 2. Background

`pureflow` is currently organized as a multi-crate Cargo workspace. Its current
shape is already close to a Polylith-style decomposition:

- `pureflow-types`
- `pureflow-workflow`
- `pureflow-workflow-format`
- `pureflow-core`
- `pureflow-contract`
- `pureflow-introspection`
- `pureflow-runtime`
- `pureflow-engine`
- `pureflow-wasm`
- `pureflow-cli`

`highland-labs` already has a reserved Rust workspace at `rust/` with
`components/`, `bases/`, and `native/` intended as the primary boundaries.

That means the migration question is not whether to modularize, but how to map
the existing `pureflow` boundaries into the destination repo without dragging
along unnecessary coupling.

## 3. Recommendation

Use `pureflow` in `highland-labs` as a reusable Rust foundation, but migrate it
in phases.

Recommended order:

1. establish the Polylith workspace shape in `highland-labs/rust`
2. move the core domain model first
3. move the execution/runtime layer next
4. keep app wiring and CLI behavior thin and separate
5. only then add product-specific adapters in the destination repo

The reusable pieces should live in `components/`. Thin composition should live
in `bases/`. Entry points should live in `native/` or product-specific app
wiring.

## 4. Boundary Mapping

The following `pureflow` crates are strong candidates to become Polylith
components in `highland-labs`:

- `pureflow-types`
- `pureflow-workflow`
- `pureflow-workflow-format`
- `pureflow-core`
- `pureflow-contract`
- `pureflow-introspection`
- `pureflow-runtime`
- `pureflow-wasm`

`pureflow-engine` is also a candidate, but it may need to be split further if
the orchestration boundary and reusable execution logic want different owners.

`pureflow-cli` should not be moved as a reusable component. It is better
represented as a thin native entrypoint or product-specific command layer.

## 5. Migration Strategy

### Phase 1: Prove the workspace shape

Create the Rust workspace skeleton in `highland-labs/rust` and confirm the
tooling can build the empty structure.

This phase should validate:

- `cargo-polylith` workspace layout
- crate naming conventions
- dependency placement rules
- thin base composition

### Phase 2: Port the core model

Move the strongest model boundaries first:

- workflow identifiers and types
- workflow graph representation
- external workflow format parsing
- node and contract types

This gives `highland-labs` a reusable semantic core before runtime complexity is
introduced.

### Phase 3: Port runtime and execution boundaries

After the model layer is stable, migrate the runtime/execution layer:

- bounded port handling
- node execution traits
- metadata emission boundaries
- workflow orchestration
- WASM adapter support

### Phase 4: Add product entrypoints

Once reusable components are stable, add product-specific runners, CLI tools,
or app wiring on top of them.

## 6. Integration Modes

There are three practical ways to bring `pureflow` into `highland-labs`.

### Option A: Temporary path dependency

Use the existing `pureflow` repo as a local dependency while the first Polylith
scaffold is assembled.

Best for:

- fast validation
- early integration experiments
- avoiding premature copying

Risk:

- if kept too long, it can delay proper ownership migration

### Option B: Direct crate transfer

Copy only the crates that survive review into `highland-labs` and convert them
into Polylith components.

Best for:

- clean long-term ownership
- repo-local dependency management
- a standards-first codebase

Risk:

- requires more up-front boundary decisions

### Option C: Adapter layer first

Expose a narrow adapter in `highland-labs` that calls into `pureflow`, then
replace the adapter with owned components once the boundary is stable.

Best for:

- minimizing disruption
- staged migration of a product team

Risk:

- can become a permanent wrapper if not time-boxed

## 7. Design Rules

The migration should follow these rules:

- do not centralize all dependencies in one workspace manifest
- do not treat `pureflow-cli` as the core reusable library
- do not move prototype shortcuts into the destination repo without review
- do not let a thin base absorb reusable business logic
- do not copy the entire repo before the boundary review is complete
- keep tests near the component that owns the logic

## 8. Suggested First Slice

The safest first slice is:

1. `pureflow-types`
2. `pureflow-workflow`
3. `pureflow-workflow-format`

That slice provides the smallest stable core needed to validate that
`highland-labs` can own the workflow model before runtime behavior is migrated.

If the destination repo needs execution sooner, the next slice should be:

1. `pureflow-core`
2. `pureflow-contract`
3. `pureflow-runtime`

## 9. Expected Outcome

If this proposal is followed, `highland-labs` should end up with:

- a Polylith-aligned Rust workspace
- reusable workflow and runtime components derived from `pureflow`
- thin composition crates instead of one large shared crate
- product wiring that stays separate from reusable business logic

That preserves the architectural value of `pureflow` while letting the new repo
own its own standards, dependency boundaries, and release shape.
