# Pureflow Audit — 2026-04-23

Auditor: Claude (Opus 4.7)
Scope: `docs/audits/Audit_scope.md`
Repo state: `HEAD = 3aa8277` (cdt-dmh.4 — execution context and message envelope);
working tree contains an in-progress `crates/pureflow-core/src/capability.rs` and matching
`lib.rs` edit corresponding to bead `cdt-dmh.5` (capability and boundary types).

---

## 1. Summary

### 1.1 Overall health

**Health score for the code that exists: 8 / 10.**

This is a nightly audit — graded only against what is implemented, not
against the full proposal. The project is an early workspace scaffold that
tracks the stated *Foundation* epic (`docs/epics/epic-1-foundation.md`)
closely. What exists is disciplined, well-tested, lint-clean, and internally
consistent. Beads `cdt-dmh.1` through `.4` are complete; `.5` (capability and
boundary types) is in progress.

Context for future audits: the proposal references an experimental async
runtime called `asupersync` (https://github.com/Dicklesworthstone/asupersync).
It is not yet a Cargo dependency and no code integrates with it. That is
expected at this bead — flagging it only so the first bead that wires it in
gets extra audit attention.

### 1.2 Key findings

- **Build, test, lint health is clean.**
  - `cargo check --workspace --all-targets`: clean.
  - `cargo fmt --all --check`: clean.
  - `cargo clippy --workspace --all-targets -- -W clippy::pedantic -W clippy::nursery`: clean (no warnings on a fresh build).
  - `cargo test --workspace`: 23 tests pass, 0 fail (core 8, engine 1, types 6, workflow 8, cli/runtime 0).
- **Structural invariants are encoded in the type system** — identifiers, port
  directions, capability descriptors, and workflow graphs all reject malformed
  inputs at construction.
- **`unsafe_code = "forbid"` and `missing_docs = "warn"`** at the workspace
  level are being honored.
- **Two standard project files required by the audit scope are missing**:
  `README.md` and `LICENSE` do not exist at the repo root, even though the
  workspace manifest declares `license = "MIT"`.
- **Error model is a `String`** (`pub type Result<T> = std::result::Result<T, String>;`).
  This is acknowledged in code as a scaffold and is slated for bead `cdt-dmh.6`
  — flagging explicitly so it does not calcify.
- **`NodeExecutor` signature is placeholder-shaped**: the proposal (§4.2)
  specifies `async fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Result<()>;`
  but the scaffold's trait is synchronous and has no port surface. Expected at
  this bead; noting so the divergence is explicit and does not quietly
  propagate into downstream beads.
- **`LifecycleHook` is defined but never invoked** — `pureflow-runtime::run_node`
  just trampolines to `node.run(ctx)`, so the observer seam exists only at the
  type level.
- **Capability and workflow models are parallel and unconnected** — there is no
  check that a `NodeCapabilities` port claim matches a `NodeDefinition` port
  declaration. (Reasonable for bead .5 in isolation; should be cross-validated
  before the runtime grows.)

---

## 2. Detailed Findings

### 2.1 Repository structure

**Layout conforms to a standard multi-crate Rust workspace**, with one
exception: the audit scope lists a `src/` directory at the root, which is not
applicable — this is a workspace, not a single-crate repo. The equivalent
expectation is satisfied by `crates/*/src/`.

Workspace members (all present and building):
- `crates/pureflow-types` — identifier primitives.
- `crates/pureflow-core` — `NodeExecutor` trait, `NodeContext`, `MessageEnvelope`, capability descriptors, lifecycle events.
- `crates/pureflow-workflow` — static workflow graph validation.
- `crates/pureflow-runtime` — currently one public function.
- `crates/pureflow-engine` — sequential node dispatch.
- `crates/pureflow-cli` — scaffold entry point.

Naming is consistent (`pureflow-*`, lowercase, hyphenated). Directory layout
inside each crate follows idiomatic Rust.

**Missing files (audit scope items 2.1):**
- `README.md` at repo root — absent.
- `LICENSE` at repo root — absent despite `license = "MIT"` in `Cargo.toml`.
- The audit scope also references a `src/` directory; in a workspace the check
  should be restated as "each member crate has a `src/`", which is satisfied.

### 2.2 Proposal alignment (audit scope 2.2)

**Proposal source: `docs/pureflow_proposal.md` (the audit scope doc references
`Documents/proposal.md`; the scope's paths are stale).**

Per the nightly-audit framing, this section does **not** penalize the code
for unbuilt features. Its purpose is to record which proposal sections are
represented in the current code so future audits can observe the direction of
travel, and to surface proposal-level gaps that will need concretization
*before* the corresponding bead starts.

| Proposal item | Status in code |
| --- | --- |
| §2.1 Structured concurrency via `asupersync` | Not yet integrated. `asupersync` lives at https://github.com/Dicklesworthstone/asupersync; first bead to introduce it should be flagged for extra audit attention. |
| §2.2 Nodes as long-lived processes with bounded channels | Not yet. `run_workflow` iterates nodes once, sequentially — correct for scaffold. |
| §2.3 AI-first introspection API | Not yet. Node metadata is partially representable via `NodeCapabilities` + `NodeDefinition` but not surfaced. |
| §2.4 Metadata engine | Partial. `MessageMetadata` exists at the envelope level; no lineage, trace, or timing capture. |
| §2.5 WASM as extension boundary | Not yet. |
| §2.6 Capability model | Bead .5 in progress. Covers port direction and an opinionated `EffectCapability` enum; no enforcement wiring. |
| §2.7 Backpressure | Not yet. |
| §2.8 Polylith alignment | Reflected in crate split. |
| §2.9 External workflow definitions (YAML/JSON/TOML) | Not yet. No serde derives on workflow types. |
| §2.10 Zero-cost abstractions | Not yet measurable. |
| §4.2 `NodeExecutor` trait shape | Placeholder: sync + portless. |

**Proposal-level gaps worth resolving *before* their corresponding bead
starts** (cheap to fix on paper, expensive to fix in code):

- **No explicit error taxonomy.** The proposal never specifies how errors
  are classified (user-facing vs internal, retryable vs fatal). Bead .6 picks
  this up in the epic, but the proposal should seed the taxonomy.
- **`EffectCapability` vs proposal §2.6 list drift.** The proposal lists
  "CPU/memory limits", "execution time", and "determinism flags"; the
  scaffold enum has none of those and adds process/env/clock. Either the
  proposal or the enum should move before bead .5 closes.
- **§2.9 is silent on schema versioning, migration, or identifier
  uniqueness scope** — real interoperability questions that will bite once
  external workflow parsing starts.
- **MVP §7** calls for "2–4 nodes in a linear pipeline" and "message-based
  WASM bridge" without naming specific nodes or payload types. Concretizing
  this is the smallest surface that unblocks end-to-end bead work.
- **§5 WASM host model** does not specify batch sizing, error crossing,
  or cancellation reaching a running guest. These are load-bearing details
  for the MVP slice — worth answering on paper before the WASM bead starts.

### 2.3 Code quality (audit scope 2.3)

**General observations:**
- Doc comments exist on every public item; `missing_docs = "warn"` is
  respected (no warnings fired during check).
- Errors are defined with explicit enums (`IdentifierError`,
  `WorkflowValidationError`, `CapabilityValidationError`) and implement
  `Display` + `std::error::Error`. Good baseline — except the trait-facing
  result type below.
- No `unwrap()` / `expect()` outside tests (where they are used with explicit
  messages — acceptable).
- Validation is implemented as `reject_*` helpers using `BTreeSet` for
  deduplication. This keeps errors deterministic; reasonable at current scale.
  If graphs grow large, these may want `HashSet`.
- Tests: inline `#[cfg(test)]` modules cover happy path, each error branch, and
  a round-trip case where relevant. **Property-based tests are absent** —
  `docs/AGENTS.md` policy (lines 173-174) requires them "for Rust code with
  non-trivial invariants". Identifier validation, workflow graph validation,
  and capability validation all qualify.

**Specific issues (file:line):**

1. **`crates/pureflow-core/src/lib.rs:11`** — `pub type Result<T> = std::result::Result<T, String>;`.
   Stringly-typed errors at the trait boundary defeat the typed-error discipline
   the rest of the codebase maintains. The doc comment calls out that this is
   scaffolded; remove before any executor trait stabilizes.

2. **`crates/pureflow-core/src/lib.rs:14-22`** — `NodeExecutor` signature differs
   from proposal §4.2 (sync vs async, no `PortsIn` / `PortsOut`). This is the
   load-bearing trait; letting the scaffold signature propagate into downstream
   beads will be costly to undo. Either update the proposal or stage the
   scaffold with `#[deprecated]` / `pub(crate)` to prevent callers outside
   `pureflow-engine` from depending on it.

3. **`crates/pureflow-runtime/src/lib.rs:10-12`** — `run_node` is a one-line
   trampoline that adds no supervision, cancellation, timing, or lifecycle
   dispatch. It is effectively dead weight at the moment. Either (a) begin
   using it to dispatch `LifecycleHook::observe` for `NodeStarted` /
   `NodeCompleted` / `NodeFailed`, or (b) inline it into `pureflow-engine`
   until there is real runtime behavior to put here.

4. **`crates/pureflow-core/src/lifecycle.rs:48-55`** — `LifecycleHook` is
   declared but never registered, stored, or called anywhere in the workspace.
   Dead code at the type level. Same remediation as (3): wire a default no-op
   hook through `run_node` so the seam is real rather than aspirational.

5. **`crates/pureflow-engine/Cargo.toml:11`** — `pureflow-types.workspace = true`
   is in `[dependencies]`, but `pureflow_types` is only referenced inside the
   `#[cfg(test)]` module of `crates/pureflow-engine/src/lib.rs` (lines 32-62).
   This should move to `[dev-dependencies]` to keep the release dep graph honest.

6. **`crates/pureflow-engine/src/lib.rs:20`** — `run_workflow` iterates nodes in
   declaration order. That is fine for the scaffold but should not be mistaken
   for FBP execution. When bead .n wires asupersync, this function's name
   should be re-examined.

7. **`crates/pureflow-core/src/capability.rs` vs `crates/pureflow-workflow/src/lib.rs`**
   — `NodeCapabilities::ports` and `NodeDefinition::input_ports`/`output_ports`
   are two parallel representations of a node's port surface. Nothing
   cross-validates that a node's capability claims match its declared ports,
   or that port directions agree. Add this check before bead .5 closes, or the
   capability layer is cosmetic.

8. **`crates/pureflow-core/src/context.rs:132-136`** — `with_cancellation` is a
   by-value builder that returns a new context. There is no mechanism for an
   external actor (e.g., runtime supervisor) to cancel a running node. This is
   fine as a placeholder but note that the real mechanism will need interior
   mutability or a channel signal — the current shape will not survive
   asupersync integration unchanged.

9. **`crates/pureflow-cli/src/main.rs:16-20`** — `PrintExecutor` uses `println!`.
   Proposal §2.4 wants metadata capture + introspection. Swap in `tracing`
   once the logging layer exists; not urgent for a scaffold.

10. **Identifiers can exceed any bound.** `pureflow_types::validate_identifier`
    rejects empty/whitespace/control characters but imposes no length cap.
    Decide whether there should be one before IDs are serialized into external
    workflow definitions (§2.9) — otherwise pathological values become a
    denial-of-service vector.

11. **`EffectCapability` enum is opinionated and asymmetric.** `FileSystemRead`
    / `FileSystemWrite` are split but `NetworkOutbound` is not split from
    inbound; `EnvironmentRead` / `EnvironmentWrite` are split but `Clock`
    is not split by wall-clock vs monotonic. These asymmetries will need a
    rationale in Verso docs (bead 8) or a normalization pass.

**No identified security vulnerabilities.** The code does not perform I/O,
network access, process spawning, or deserialization of untrusted input. The
validation paths are straightforward and do not use regex or external parsers.

**No identified inefficient algorithms.** All validation is O(n) with small
constant factors; `BTreeSet`/`BTreeMap` are used over `HashSet`/`HashMap` for
deterministic iteration order, which is the right tradeoff given the small
expected input sizes.

### 2.4 Documentation

- `docs/pureflow_proposal.md` is thorough for a vision doc; see §2.2 above for
  concretization gaps.
- `docs/epics/epic-1-foundation.md` is well-shaped and consistent with the
  current bead state.
- `docs/AGENTS.md` intentionally mixes Pureflow-specific policy with
  `beads_rust` onboarding content (confirmed with author: the `br` content
  is there so agents working on Pureflow also pick up the beads ticket
  database conventions between sessions). No finding — noting for future
  auditors so the `br` sections are not flagged again.
- `docs/HANDOFF.md` is current as of bead .2 closure; it should be updated
  after bead .4 and .5 land.
- `docs/audits/Audit_scope.md` references file paths that do not exist
  (`Documents/AGENTS.md`, `Documents/proposal.md`, `Documents/assessment.md`,
  `task_database.md`). Refresh to match actual repo layout (`docs/...`, beads
  task tracking).
- No per-crate `README.md` files; given the Polylith split and the `//!`
  module docs, this is acceptable at current size.

### 2.5 Testing coverage

| Crate | Tests | Covers | Gaps |
| --- | --- | --- | --- |
| `pureflow-types` | 6 | empty, whitespace, control-char, display/parse round-trip, per-kind | No property tests; no length / Unicode edge cases. |
| `pureflow-workflow` | 8 | duplicate nodes/ports, unknown node, unknown port (both endpoints, both directions), happy path | No property tests; no cycle detection (intentionally out of scope, but worth a note). |
| `pureflow-core` | 8 | capability validation (4), context cancellation (2), lifecycle event (1), message envelope (1) | No property tests; no tests for `NodeExecutor` trait object dispatch. |
| `pureflow-engine` | 1 | recording executor visits each node with correct ctx | Only happy path; no executor-error path; uses `RefCell` — fine for single-threaded scaffold, will not survive `Send + Sync` requirements. |
| `pureflow-runtime` | 0 | — | No tests at all (admittedly one line of code). |
| `pureflow-cli` | 0 | — | No integration test that exercises `main` end-to-end. |

**Recommendation**: add a `tests/` integration folder once bead .7 (Test Kit)
lands; introduce `proptest` or `quickcheck` for identifier and graph
invariants per AGENTS.md policy.

### 2.6 Proposal feasibility assessment

Added after a standalone discussion with the author. Records the
vision-level read of `docs/pureflow_proposal.md` so future audits can observe
whether the direction of travel has changed.

**Scope context** (confirmed with author): Pureflow is a personal project
for the author's own use. Not being productized, no plans to open-source at
this time. RDF (proposal §2.4) is planned as a node — likely
`oxigraph` or `rdf-datafusion` once the latter matures — not a core
architectural feature. These two points remove the two largest risks a
proposal of this shape would otherwise carry (productization pressure and
an in-tree semantic-web engine).

**Overall read: the proposal is feasible.** The foundational primitives are
mature individually:

- **FBP execution model** — J. Paul Morrison's original work plus modern
  implementations (NoFlo, various Rust experiments) establish the pattern.
- **Bounded channels + backpressure** — proven across Go, Rust (Tokio /
  flume), and Kotlin. Not research.
- **Structured concurrency with parent-child task trees** — Swift, Trio,
  Kotlin coroutines. Well understood.
- **Host-owned-channel WASM sandboxing** — the standard way production
  WASM integrations work today; batch-in/batch-out defers the hard streaming
  problem sensibly.
- **Capability model with strict-for-WASM / advisory-for-native** —
  pragmatic. "Advisory-only" for native nodes is honest about what is
  enforceable.

**Where the real engineering work lives** — integration of those
primitives, not any one of them. Two specific tensions the proposal does not
yet address:

1. **Zero-cost (§2.10) vs metadata-first (§2.4).** Capturing
   lineage/timing/schema on every hop costs real cycles. Resolvable with
   tier-based capture (control messages always tracked, Arrow tier sampled
   per-batch) but it is a design decision the proposal should eventually
   name.
2. **Cancellation semantics in a streaming FBP graph.** When an upstream
   node is cancelled mid-emit and a downstream consumer's channel is
   partially drained, who propagates what, in what order, with what
   guarantees? This is where `asupersync`'s specific behavior will shape
   the design most. Worth a Verso note in the bead that wires asupersync
   into `pureflow-runtime`.

**Remaining risks (with productization and RDF removed):**

- **Scope discipline on the MVP slice (§7).** The proposal describes a
  narrow 2–4 node MVP; everything else is optional until that slice runs.
  Scope drift is the single largest personal-project risk for a runtime of
  this ambition.
- **Native capability enforcement is advisory.** The capability layer is a
  real security boundary only for WASM / Process nodes. This is fine
  provided the author does not later mistake native node capabilities for a
  sandbox.
- **`asupersync` single-point-of-failure.** Setting newness aside per the
  author's direction: Pureflow inherits asupersync's stability envelope.
  Accepted as a research-vehicle tradeoff; worth re-evaluating if Pureflow
  graduates to routine personal-production use.

**Architectural bets I would keep front and centre.** The proposal's
strongest ideas are the ones that make Pureflow *AI-inspectable* rather than
just *AI-integrated*: FBP over DAG, metadata-first capture, and capabilities
encoded in the type system. These three together are the proposal's
differentiator and should drive prioritization.

**Net:** this is a fundamentally sound direction for a personal experimental
runtime. The foundation bead sequence currently in flight is appropriate for
the vision. No proposal-level course correction is indicated by this audit.

---

## 3. Recommendations

Ordered by leverage — each one is concrete enough to become a bead.

1. **Add `README.md` and `LICENSE` at the repo root.** The workspace declares
   MIT but no LICENSE file exists. Low effort, unblocks the audit scope 2.1
   checklist, and fixes a real licensing claim gap.

2. **Refresh `docs/audits/Audit_scope.md`** to reference `docs/` (not
   `Documents/`), `pureflow_proposal.md` (not `proposal.md`), and the beads
   task system (not `task_database.md`). Took 10 minutes to reconcile during
   this audit; cheaper to fix once.

3. **Stage the error model bead (`cdt-dmh.6`) next.** Every downstream trait
   signature is currently returning `Result<T, String>`. Every bead that lands
   before the error model becomes a later migration. Doing .6 before .5
   closes, or immediately after, avoids that churn.

4. **Decide the `NodeExecutor` shape before the runtime grows.** Either:
   - update `docs/pureflow_proposal.md` §4.2 to match the current sync
     trait (if that is the intended scaffold); **or**
   - replace the scaffold trait with `async fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Result<()>`
     behind placeholder `PortsIn`/`PortsOut` types.
   Leaving these two in disagreement guarantees rework.

5. **Wire `LifecycleHook` through `pureflow-runtime::run_node`**, even as a
   no-op default. This makes the observer seam load-bearing rather than
   theoretical and gives bead .7 (Test Kit) something to instrument.

6. **Cross-validate `NodeCapabilities` against `NodeDefinition`** as part of
   closing bead .5. The capability layer is cosmetic without this check.

7. **Move `pureflow-types` from `[dependencies]` to `[dev-dependencies]` in
   `crates/pureflow-engine/Cargo.toml`.** One-line fix.

8. **Add property tests** for `pureflow-types::validate_identifier` and
   `pureflow-workflow::WorkflowGraph::validate`. AGENTS.md already requires
   them; do it before bead .7 so the test-kit bead has a concrete customer.

9. **Link `asupersync` in the proposal.** It is a real experimental runtime
   (https://github.com/Dicklesworthstone/asupersync) but currently referenced
   by name only. Adding the link in `docs/pureflow_proposal.md` §2.1 saves
   every future reader the lookup.

10. **Cap identifier lengths.** Before the first external workflow format
    parses user input (§2.9), decide on a maximum length and reject
    overlong IDs in `validate_identifier`.

11. **Consider re-exporting identity types from `pureflow-core`.** Downstream
    callers currently import from both `pureflow-core` and `pureflow-types`.
    A `pub use pureflow_types::{WorkflowId, NodeId, PortId, MessageId, ExecutionId};`
    in `pureflow-core` would simplify consumer code.

---

## 4. Outstanding Questions for the Human

Resolved in this audit cycle (recorded for future auditors):
- `asupersync` is a real experimental async runtime at
  https://github.com/Dicklesworthstone/asupersync. Not yet a dep; first bead
  to integrate it warrants extra audit attention.
- Nightly audit scope is "grade what exists, not what is planned".
- Findings go into this markdown under §5 "Suggested Audit Beads"; the user
  will triage into beads.
- `docs/AGENTS.md` intentionally mixes Pureflow-specific policy with
  `beads_rust` onboarding — this is by design (agent memory continuity) and
  should not be flagged in future audits.

Still open:

1. **Does bead `cdt-dmh.5` (capability) intend to cross-validate against the
   workflow graph before closing**, or is that deferred to a later bead?
   Finding 2.3(7) and audit bead **AB-4** below depend on the answer.

---

## 5. Suggested Audit Beads

One bead per remediation, in rough priority order. Dependencies noted where a
later bead assumes an earlier one landed. Each is scoped to be reviewable as
one JJ change.

### AB-1 — Add repo-root `README.md` and `LICENSE`

**Why:** Audit scope 2.1 requires both. Workspace declares `license = "MIT"`
but there is no `LICENSE` file on disk.
**Acceptance:**
- `README.md` describes what Pureflow is, how to build, how to test, and
  points at `docs/pureflow_proposal.md` and `docs/epics/epic-1-foundation.md`.
- `LICENSE` contains the MIT text matching the manifest declaration.

### AB-2 — Refresh `docs/audits/Audit_scope.md`

**Why:** Paths in the scope doc (`Documents/AGENTS.md`, `Documents/proposal.md`,
`Documents/assessment.md`, `task_database.md`) do not match the repo.
**Acceptance:**
- References point at `docs/AGENTS.md`, `docs/pureflow_proposal.md`, and the
  `docs/audits/Audit_*.md` output pattern.
- Any beads/task-database references mention `beads` (`cdt-*`) explicitly.

### AB-3 — Move `pureflow-types` to `[dev-dependencies]` in `pureflow-engine`

**Why:** `pureflow-engine` only uses `pureflow-types` inside
`#[cfg(test)]` code (lib.rs:32-62), but declares it as a runtime dependency.
**Acceptance:**
- `crates/pureflow-engine/Cargo.toml` lists `pureflow-types.workspace = true`
  only under `[dev-dependencies]`.
- `cargo check --workspace --all-targets` and `cargo test --workspace` stay
  clean.

### AB-4 — Cross-validate `NodeCapabilities` against `WorkflowDefinition`

**Why:** The capability descriptors in `pureflow-core` and the port
declarations in `pureflow-workflow` are two parallel representations of a
node's port surface with no cross-check. Without this, the capability layer
is cosmetic.
**Blocked by:** bead `cdt-dmh.5` closing.
**Acceptance:**
- A new validation entry point accepts a `WorkflowDefinition` plus a
  collection of `NodeCapabilities` and rejects any capability claim whose
  port does not exist on the declared node, or whose direction disagrees
  with the declared port direction.
- Unit tests cover mismatched node, mismatched port, mismatched direction,
  and happy path.

### AB-5 — Wire `LifecycleHook` through `pureflow-runtime::run_node`

**Why:** `LifecycleHook` is defined but never invoked. The observer seam
exists only at the type level.
**Acceptance:**
- `run_node` dispatches `NodeStarted` before the inner call and
  `NodeCompleted` / `NodeFailed` based on the result, to any hooks passed in.
- A no-op default is available so the CLI and engine do not need to supply a
  hook yet.
- Unit tests verify hook ordering and that failures still propagate the
  error.

### AB-6 — Property tests for identifier and workflow validation

**Why:** `docs/AGENTS.md` requires property tests for code with non-trivial
invariants; none exist yet. Identifier validation and workflow graph
validation are the obvious first customers.
**Acceptance:**
- `proptest` or `quickcheck` is a workspace `[dev-dependencies]` entry.
- Property tests cover: any non-empty string free of whitespace and control
  chars is a valid identifier; any graph built from valid, unique node+port
  names with edges that respect port direction validates.
- Shrinking produces readable counterexamples for seeded failures.

### AB-7 — Replace `pureflow-core::Result<T, String>` with a structured error enum

**Why:** Stringly-typed errors at the trait boundary defeat the typed-error
discipline used everywhere else. This is the epic's planned bead `cdt-dmh.6`;
sequencing it next avoids migrating every new trait signature later.
**Acceptance:**
- `pureflow-core` defines an error enum with explicit variants (at minimum:
  validation, execution, cancellation) implementing `Display` + `Error`.
- `NodeExecutor::run`, `LifecycleHook::observe`, and
  `pureflow-runtime::run_node` return the new type.
- CLI `main` converts via `?` without `.map_err(|e| e.to_string())`.

### AB-8 — Link `asupersync` in the proposal

**Why:** The runtime is referenced by name only. A single line in
`docs/pureflow_proposal.md` §2.1 saves every future contributor the lookup.
**Acceptance:**
- Proposal §2.1 includes the repository URL.
- Optional: a `docs/decisions/` ADR explaining why `asupersync` was chosen
  over Tokio.

### AB-9 — Decide and encode `NodeExecutor` async signature

**Why:** The scaffold's sync, portless signature will diverge further from
proposal §4.2 with every new consumer.
**Blocked by:** AB-7 (error model).
**Acceptance:**
- Either the proposal is updated to match the scaffold signature, or the
  trait gains `async fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Result<()>`
  with placeholder `PortsIn` / `PortsOut` types.
- Existing tests continue to pass under the new signature.

### AB-10 — Cap identifier lengths in `pureflow-types`

**Why:** `validate_identifier` has no length bound. Before external workflow
parsing (§2.9), pathological IDs become a DoS surface.
**Acceptance:**
- A named constant (e.g., `MAX_IDENTIFIER_LEN`) is introduced with a
  rationale in its doc comment.
- `validate_identifier` rejects values longer than the cap, with a new
  `IdentifierError::TooLong { kind, limit }` variant.
- Unit and property tests cover the boundary.

### AB-11 — Reconcile `EffectCapability` with proposal §2.6

**Why:** The proposal lists CPU/memory/time/determinism capabilities that the
enum does not carry, and the enum lists process/env/clock capabilities the
proposal does not mention.
**Acceptance:**
- Either the proposal or the enum is updated so both agree.
- A short Verso note on `EffectCapability` explains the final taxonomy.

### AB-12 — Re-export identity types from `pureflow-core`

**Why:** Downstream callers currently `use pureflow_core::...` and
`use pureflow_types::...` for related symbols. A re-export simplifies every
call site.
**Acceptance:**
- `pureflow-core` has `pub use pureflow_types::{WorkflowId, NodeId, PortId, MessageId, ExecutionId, IdentifierError};`.
- At least one downstream crate drops its direct `pureflow-types` import
  (picked to demonstrate the ergonomic improvement).

### AB-13 — Update `docs/HANDOFF.md` after each closed bead

**Why:** Handoff doc still reflects bead `.2` closure; beads `.3` and `.4`
have landed since.
**Acceptance:**
- Handoff lists all currently closed beads.
- A convention is captured (e.g., "update HANDOFF.md in the same JJ change
  that closes a bead") in `docs/AGENTS.md`.

---

## 6. Appendix — Commands run

```bash
cargo check --workspace --all-targets       # clean
cargo fmt --all --check                     # clean
cargo clippy --workspace --all-targets \
    -- -W clippy::pedantic -W clippy::nursery  # clean on fresh build
cargo test --workspace                      # 23 passed, 0 failed
```
