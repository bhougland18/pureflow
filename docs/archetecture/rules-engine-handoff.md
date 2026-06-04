# Rules Engine — Session Handoff

Date: 2026-06-03

This document captures the decisions made during the design session that produced
`rules-engine-proposal.md`. Read it before touching the proposal or filing beads.

---

## What Exists

- `rules-engine-proposal.md` — the full proposal, updated in this session.
  It supersedes any earlier draft. Read it in full before making changes.
- `proposal_final.md` — the core Pureflow architecture this proposal extends.
  The rules engine slots into the Phase 2/3/4 timeline defined there.

---

## Tech Stack Context

### Pureflow runs on asupersync, not tokio

Repo: https://github.com/Dicklesworthstone/asupersync

Key implications already baked into the proposal:
- All async signatures take `cx: &Cx` (asupersync capability token).
- The evaluator returns `Outcome<RuleDecision, RuleError>`, not `Result`.
  `Outcome` is asupersync's four-valued type (Ok/Err/Cancelled/Panicked).
  **Do not name anything `EvalOutcome` — that collides with asupersync's type.**
- `RuleNode` implements asupersync's multi-phase cancel protocol (drain →
  finalize → quiescence). `RuleEvalRecord` emission happens in finalize, so
  it is never silently dropped on cancellation.
- Port sends use reserve/commit, not a plain `send()`.
- `Budget` (asupersync resource quota) is threaded through `EvalContext` so
  the evaluator can exit early on pathological nested conditions.
- Tests must run under asupersync's deterministic lab runtime.

### Rule distribution uses GuardianDB + Iroh — app-layer only

Repo: https://github.com/wmaslonek/guardian-db

GuardianDB is a local-first P2P database built on iroh (QUIC) and iroh-docs
(Willow protocol CRDT). The use case: push updated rule sets to deployed apps
without resubmitting to app stores or asking IT to reinstall software.

**Critical decision**: `pureflow-rules` and `pureflow-workflow-format` have no
dependency on GuardianDB or Iroh. The boundary is the `RuleSetSource` trait
(§4.3 of the proposal). The application embedding Pureflow provides a
`GuardianDbSource` implementation. The `RuleNode` receives an `Arc<RuleSet>`
and knows nothing about how it arrived.

GuardianDB uses Last-Write-Wins (LWW) conflict resolution. If the app-layer
adapter uses one key per rule (e.g. `rule:filter.high-value`) rather than one
blob per rule set, concurrent edits to different rules won't clobber each other.
That is the adapter author's problem, not Pureflow's.

---

## Key Design Decisions Made This Session

### 1. Conditions reach Pureflow context, not just payload

The condition language was extended beyond payload fields to cover:
- **Tags**: `TagEq`, `TagExists`, `TagAbsent` — tags applied by upstream `Tag` actions
- **Provenance**: `SourceNode`, `ArrivedOnPort`, `HopCountGt/Lte`
- **Execution context**: `WorkflowIs`, `ExecutionMetadataEq`

This was the user's explicit request: "I want it to be able to harness all the
good metadata information that pureflow carries along with it node by node."

`EvalContext` carries `PacketMetadata`, `WorkflowContext`, `NodeContext`, and
`Budget` alongside the payload. The evaluator operates over all of them.

### 2. `ConditionTrace` per condition in `RuleEvalRecord`

Every condition evaluated during a rule check is traced: which condition, what
it saw, whether it matched, and which surface it drew from (payload vs. tag vs.
provenance vs. execution context). This feeds `pureflow-cli explain` and
AI-assisted rule tuning.

### 3. `RuleSetSource` is the sync boundary

`pureflow-workflow-format` resolves `rule_set_ref` URIs through a
`RuleSetSource` trait rather than hardcoded filesystem reads. Two impls ship
with Pureflow (`LocalFsSource`, `EmbeddedSource`). Everything else is app-layer.

### 4. Contract validation covers provenance conditions

`SourceNode(node_id)` and `ArrivedOnPort(port_id)` conditions are validated at
load time against the workflow graph. Invalid references produce typed
diagnostics before any execution begins.

---

## What Is NOT In Scope For Pureflow

- Rule sync, distribution, or delivery (GuardianDB adapter is app-layer).
- Hot-swapping rule sets in a running workflow (not the use case; rules are
  resolved at load time via `RuleSetSource`).
- General policy engine, RBAC, obligation frameworks.
- Scripting or arbitrary code in conditions (`Eval`/`Script` are permanently
  rejected by name in the proposal).
- Arrow/DataFusion streaming queries (deferred to core roadmap Phase 5).

---

## Open Questions (Not Yet Resolved)

1. **`rule_set_ref` URI scheme registration**: How does the app register a
   custom `RuleSetSource` for a custom URI scheme (e.g. `guardiandb://...`)?
   The proposal implies injection at workflow load time but doesn't specify
   the registry mechanism. This needs a design decision before Phase R2 beads
   are filed.

2. **`AllMatches` strategy with terminal actions**: If strategy is `AllMatches`
   and two rules both match with `Route` to different ports, what happens? The
   proposal doesn't define the collision behavior. Options: error at validation
   time, first terminal action wins, or require `AllMatches` to only contain
   `Tag` actions. Needs resolution before `rules-evaluator` bead is started.

3. **`ConditionTrace` volume under `AllMatches`**: With all rules evaluated,
   `conditions_evaluated` could be long for large rule sets. Should
   `TieredMetadataSink` suppress trace detail below a threshold? Coordinate
   with the metadata productization work in core Phase 3.

4. **asupersync version pin**: The proposal references asupersync but doesn't
   pin a version. Before writing any code in `pureflow-rules`, confirm the
   version currently used by the rest of the Pureflow crate graph.

---

## Beads To File (From §11 of Proposal)

13 beads total. In dependency order:

```
rules-types-core          (no deps — start here)
rules-eval-metadata       (no deps — parallel with above)
    └── rules-evaluator   (depends on both above)
        └── rules-native-node
rules-set-source          (no deps — start parallel with types)
    └── rules-format-loading
rules-contract-validation (depends on rules-types-core)
rules-introspection       (depends on rules-native-node, rules-eval-metadata)
rules-cli-explain         (depends on rules-introspection)
rules-metadata-jsonl      (depends on rules-native-node, rules-eval-metadata)
rules-wasm-wit            (depends on rules-types-core)
    └── rules-wasm-host
rules-example-routing     (depends on everything above)
```

File them before starting implementation. Do not use TodoWrite or markdown TODO
lists — use `bd create` per the CLAUDE.md rules.

---

## Files To Read Before Touching Anything

```
docs/archetecture/rules-engine-proposal.md   ← primary artifact
docs/archetecture/proposal_final.md          ← core roadmap context
crates/pureflow-core/src/                    ← PacketPayload, PacketMetadata
crates/pureflow-contract/                    ← SchemaRef, contract validation
crates/pureflow-introspection/               ← existing projection patterns
crates/pureflow-workflow-format/             ← where RuleSetSource trait goes
```
