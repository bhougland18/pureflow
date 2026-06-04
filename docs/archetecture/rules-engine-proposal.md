# Pureflow Rules Engine Proposal

Date: 2026-06-03

## 1. Summary

This proposal introduces a rules engine layer for Pureflow — a declarative,
inspectable mechanism for evaluating conditions against packet data and
producing routing decisions, validation outcomes, or side effects within a
running workflow graph.

The rules engine is not a replacement for node contracts or the validation
pipeline. It is a first-class node type and a companion introspection surface
that allows workflow authors to express conditional logic declaratively rather
than by embedding decision code in native executors. Rules are data. They can
be serialized, inspected, version-controlled, and verified before execution —
the same standard Pureflow already applies to workflow topology.

Because Pureflow is built on asupersync rather than tokio, this proposal
adopts asupersync's structural correctness guarantees throughout: `Cx`
capability tokens thread through all async paths, `Outcome<T, E>` is the
return type for evaluation, and `RuleNode` participates in the multi-phase
cancellation protocol. These are not conventions — they are enforced by the
type system.

---

## 2. Motivation

### 2.1 The Gap Today

Pureflow currently handles two kinds of correctness:

- **Structural correctness** — validated at load time by `pureflow-workflow`
  and `pureflow-contract`. Is the graph wired correctly? Do port schemas match?
  Are capabilities declared?
- **Execution correctness** — guaranteed at runtime by the supervisor and
  bounded channels. Do nodes process without deadlock? Does backpressure
  propagate?

What it does not yet handle is **semantic correctness at packet time**: is the
data in this packet allowed to proceed? Which downstream branch should receive
it? Does this record meet the quality threshold for the next stage?

Today, those decisions live inside native node executors — opaque Rust code
that the graph cannot inspect, the CLI cannot explain, and a rules audit cannot
verify without reading source. For AI-generated or AI-supervised workflows,
that is a blind spot.

### 2.2 What A Rules Engine Adds

A rules engine moves conditional packet-routing logic from opaque executor code
into declared, named, serializable rule sets that:

- Are validated at load time alongside the rest of the workflow.
- Are visible to `pureflow-cli inspect` and `pureflow-cli explain`.
- Are included in metadata JSONL as structured decision records.
- Can be evaluated by WASM guest components or native executors sharing the
  same rule representation.
- Allow AI tools to reason about *why* a packet took a given path.
- Can condition on both packet payload fields and Pureflow's own packet
  metadata — tags, provenance, hop count, and execution context.

### 2.3 Target Use Cases

| Use case | Example |
|---|---|
| Content-based routing | Route financial records by account type to different downstream processors |
| Quality gate | Drop or dead-letter packets that fail schema or domain constraints |
| Feature flag routing | Direct traffic to experimental versus stable downstream nodes |
| Threshold-triggered alerts | Emit to an alert port when a metric value crosses a declared bound |
| Policy enforcement | Reject records that would violate a declared data-access policy |
| Workflow branching | Replace hard-coded `if`/`else` in executors with named, auditable branches |
| Provenance-aware routing | Route differently based on which upstream node produced the packet |
| Tag-conditional routing | Apply different logic to packets tagged `high-priority` by upstream nodes |

---

## 3. Core Concepts

### 3.1 Rule

A rule is a named, serializable predicate over a packet and its Pureflow
context. It has:

```
Rule {
    id: RuleId,          // scoped identifier, e.g. "filter.high-value"
    condition: Condition, // the predicate expression
    action: RuleAction,  // what to do when the condition is true
    priority: u32,       // evaluation order within a rule set
    description: String, // human/AI-readable explanation
}
```

Rules are not Turing-complete programs. They operate over a restricted
expression language (see §3.3) and produce only declared actions (see §3.4).
This keeps them inspectable and auditable.

### 3.2 Rule Set

A rule set is an ordered collection of rules with a declared evaluation
strategy:

```
RuleSet {
    id: RuleSetId,
    strategy: EvaluationStrategy,
    rules: Vec<Rule>,
    default_action: RuleAction,
    trace_conditions: bool,  // default false; set true for audit/debug nodes
}
```

Evaluation strategies:

| Strategy | Meaning |
|---|---|
| `FirstMatch` | Evaluate in priority order; stop at first matching rule |
| `AllMatches` | Evaluate all rules; collect all `Tag` applications, then apply `default_action` |
| `Score` | Evaluate all rules; select action from highest-score match |

The `default_action` applies when no rule matches.

**`AllMatches` constraint**: Rules in an `AllMatches` rule set may only use
`Tag` actions. `Route`, `Halt`, `DeadLetter`, and `Drop` are rejected at
validation time with a typed diagnostic. This prevents ambiguous multi-terminal
outcomes. To apply tags then route, use an `AllMatches` node upstream of a
`FirstMatch` node.

**`trace_conditions`**: When `false` (default), `RuleEvalRecord` omits the
per-condition trace — zero allocation overhead for high-throughput nodes. Set
`true` on audit or debug nodes to capture the full `ConditionTrace` log for
every evaluation.

### 3.3 Condition Expression Language

Conditions must be inspectable and serializable. The expression language is
intentionally narrow — it is not a general scripting language.

Conditions operate over two surfaces: the packet **payload** and Pureflow's
**packet context** (metadata, provenance, execution state).

```
Condition ::=
    // --- Payload conditions ---
    | FieldEq(path: FieldPath, value: ScalarValue)
    | FieldNeq(path: FieldPath, value: ScalarValue)
    | FieldGt(path: FieldPath, value: NumericValue)
    | FieldLt(path: FieldPath, value: NumericValue)
    | FieldGte(path: FieldPath, value: NumericValue)
    | FieldLte(path: FieldPath, value: NumericValue)
    | FieldIn(path: FieldPath, values: Vec<ScalarValue>)
    | FieldExists(path: FieldPath)
    | FieldAbsent(path: FieldPath)
    | FieldMatches(path: FieldPath, pattern: GlobPattern)

    // --- Tag conditions (tags applied by upstream Tag actions) ---
    | TagEq(key: String, value: ScalarValue)
    | TagExists(key: String)
    | TagAbsent(key: String)

    // --- Provenance conditions (where did this packet come from?) ---
    | SourceNode(node_id: NodeId)
    | ArrivedOnPort(port_id: PortId)
    | HopCountGt(n: u32)
    | HopCountLte(n: u32)

    // --- Execution context conditions ---
    | WorkflowIs(workflow_id: WorkflowId)
    | ExecutionMetadataEq(key: String, value: ScalarValue)

    // --- Logical combinators ---
    | And(conditions: Vec<Condition>)
    | Or(conditions: Vec<Condition>)
    | Not(condition: Box<Condition>)
    | Always
    | Never
```

`FieldPath` is a dot-separated key sequence into the packet payload, e.g.
`"account.type"` or `"metrics.latency_ms"`.

Payload conditions operate over the `PacketPayload::Control` (JSON) and
`PacketPayload::Structured` variants. `PacketPayload::Bytes` must be decoded
before routing through a rule node; the rule executor rejects raw bytes without
a declared schema.

Provenance and tag conditions operate over `EvalContext` (see §4.1), which
carries the full Pureflow packet context alongside the payload.

There is no `Eval`, `Script`, or `Code` variant. Arbitrary code in a condition
breaks inspectability and the audit guarantee.

### 3.4 Rule Actions

A rule action is one of:

```
RuleAction ::=
    | Route(port: PortId)          // send packet to the named output port
    | Drop                         // discard the packet
    | DeadLetter(reason: String)   // route to the configured dead-letter port
    | Tag(key: String, value: ScalarValue)  // annotate packet metadata, then continue
    | Halt(error: String)          // fail the node with a structured error
```

`Tag` is a non-terminal action — it annotates the packet and continues
evaluation. All others are terminal.

All terminal routing actions (`Route`, `DeadLetter`) use asupersync's
two-phase reserve/commit channel API, making them cancel-safe by construction.

### 3.5 Component Authoring Pattern

`RuleNode` is stateless — each evaluation is independent, and multiple instances
may share a single `Arc<RuleSet>` with no contention. This makes fan-out
parallelism over a shared rule set zero-cost: fan N packets to N `RuleNode`
instances, each holding the same `Arc<RuleSet>`, and evaluation scales linearly
with no lock overhead.

In a polylith component model, the recommended pattern is:

```
components/payment-validator/
    src/lib.rs                       ← transformation and execution logic
    rules/default.rules.json         ← default routing rules for this domain
    contract.json                    ← port schemas
```

The transformation node and its rule set are **co-located** in the same component
package but remain **separate artifacts**. The workflow assembles them as two
distinct nodes:

```
payment-validator ──► payment-router(rule_set_ref: "…/rules/default.rules.json")
```

This gives:
- **Domain cohesion**: rules and transformation evolve together in one package.
- **Reusability**: the transformation node carries no routing knowledge and can
  be assembled with any rule set.
- **Overridability**: a different deployment substitutes a different
  `rule_set_ref` without touching the component.
- **Progressive filtering**: chain small focused rule nodes (coarse rules first,
  fine rules deeper) rather than one large rule set, keeping each node fast and
  its logic readable.

---

## 4. Crate Design

### 4.1 New Crate: `pureflow-rules`

A new crate sits between `pureflow-contract` and `pureflow-engine`:

```
crates/pureflow-rules/
    src/
        lib.rs
        condition.rs   -- Condition types and evaluator
        action.rs      -- RuleAction types
        rule.rs        -- Rule and RuleSet types
        context.rs     -- EvalContext
        eval.rs        -- RuleSetEvaluator
        error.rs       -- typed rule errors
    tests/
        eval_tests.rs
        condition_tests.rs
```

Dependencies:

- `pureflow-types` — `NodeId`, `PortId`, `WorkflowId`
- `pureflow-core` — `PacketPayload`, `PortPacket`, `PacketMetadata`
- `asupersync` — `Cx`, `Outcome`, `Budget`
- `serde` (feature-gated) — rule set serialization

`pureflow-rules` does not depend on `pureflow-engine`. The evaluator is a
pure function over rule sets, packets, and their Pureflow context.

### 4.1.1 Naming: `RuleDecision` vs `Outcome`

Asupersync's core return type is `Outcome<T, E>` (Ok / Err / Cancelled /
Panicked). To avoid collision, the result of rule evaluation is named
`RuleDecision`, not `EvalOutcome`:

```rust
pub struct RuleDecision {
    pub action: RuleAction,
    pub matched_rule: Option<RuleId>,
    pub tags_applied: Vec<(String, ScalarValue)>,
    pub eval_metadata: RuleEvalRecord,
}
```

### 4.1.2 `EvalContext`

`EvalContext` carries the full Pureflow runtime picture alongside the packet
payload. This is what enables provenance and tag conditions:

```rust
pub struct EvalContext<'a> {
    pub payload: &'a PacketPayload,
    pub packet_metadata: &'a PacketMetadata,   // tags, timestamps
    pub workflow_context: &'a WorkflowContext, // workflow_id, execution_id
    pub node_context: &'a NodeContext,         // source node, arrival port, hop count
    pub budget: &'a Budget,                   // remaining poll quota / deadline
}
```

`Budget` is asupersync's resource constraint type. The evaluator checks the
budget during condition evaluation and returns `Outcome::Cancelled` if the
quota is exhausted — protecting against pathological deeply-nested conditions.

### 4.1.3 `RuleSetEvaluator`

```rust
pub struct RuleSetEvaluator;

impl RuleSetEvaluator {
    pub fn evaluate(
        &self,
        cx: &Cx,
        rule_set: &RuleSet,
        packet: &PortPacket,
        context: &EvalContext,
    ) -> Outcome<RuleDecision, RuleError>;
}
```

`cx` threads the asupersync capability token through evaluation. The evaluator
is otherwise a pure function — no I/O, no channel access, no side effects.

### 4.2 Native Rule Node

A built-in native node type, `RuleNode`, wraps a `RuleSetEvaluator`:

```rust
pub struct RuleNode {
    rule_set: Arc<RuleSet>,
    evaluator: RuleSetEvaluator,
}
```

`RuleNode` is registered in the native executor registry under a stable
built-in contract ID, e.g. `"pureflow.rules.v1"`.

`RuleNode` is stateless. Multiple instances may share a single `Arc<RuleSet>`
with no contention. Fan-out to N parallel `RuleNode` instances is zero-cost
from a rule-evaluation standpoint; throughput scales with the fan count, bounded
only by channel backpressure.

The node:

1. Receives one packet from its single input port `"in"`.
2. Builds `EvalContext` from the packet and current workflow/node context.
3. Calls `RuleSetEvaluator::evaluate(cx, rule_set, packet, &context)`.
4. Executes the `RuleDecision` action using asupersync reserve/commit sends:
   - `Route(port)` → `ports_out.reserve(cx, &port).await?.commit(packet)`
   - `DeadLetter(reason)` → `dead_letter.reserve(cx).await?.commit((packet, reason))`
   - `Drop` → packet is discarded
   - `Halt(error)` → node fails with a structured `NodeError`
5. Emits a `RuleEvalRecord` to the metadata sink.

**Cancellation protocol**: `RuleNode` implements asupersync's multi-phase
cancel contract. On cancellation request, the node enters drain phase —
it finishes evaluating and routing the current packet, then emits the
`RuleEvalRecord` for that packet before entering finalize. The metadata
record for the last packet is never silently lost. The Region does not reach
quiescence until both the routing action and the record emission are complete.

### 4.3 Rule Set Loading and `RuleSetSource`

Rule sets can be embedded inline in the workflow document or loaded from an
external source. The format crate resolves `rule_set_ref` URIs through a
pluggable `RuleSetSource` trait rather than hardcoded filesystem reads:

```rust
pub trait RuleSetSource: Send + Sync {
    fn load(
        &self,
        cx: &Cx,
        ref_uri: &str,
    ) -> impl Future<Output = Outcome<RuleSet, RuleSourceError>>;
}
```

Two implementations ship with Pureflow:

- `LocalFsSource` — resolves paths relative to the workflow file (current behavior)
- `EmbeddedSource` — deserializes inline JSON from the workflow document

Applications register additional implementations at `WorkflowLoader`
construction time via a `SourceRegistry`:

```rust
pub struct SourceRegistry {
    sources: Vec<(String, Arc<dyn RuleSetSource>)>, // (URI scheme, impl)
}
```

The registry dispatches on URI scheme prefix: plain paths (no scheme) use
`LocalFsSource`; `"embedded://"` uses `EmbeddedSource`; any other scheme
requires the application to register a matching `RuleSetSource` impl.
Applications that don't need remote rule delivery use only the built-in
sources and never interact with the registry. There is no global singleton
and no assumed sync technology.

This is explicitly an application-layer concern — `pureflow-rules` and
`pureflow-workflow-format` have no dependency on any sync, distribution,
or storage layer. The `RuleNode` receives a resolved `Arc<RuleSet>` and
knows nothing about where it came from.

**Inline (workflow JSON):**

```json
{
  "node": "route-by-type",
  "contract": "pureflow.rules.v1",
  "rule_set": {
    "id": "account-router",
    "strategy": "FirstMatch",
    "rules": [
      {
        "id": "high-value",
        "priority": 10,
        "condition": { "FieldGte": { "path": "amount", "value": 10000 } },
        "action": { "Route": "high-value-out" },
        "description": "Route large transactions to the compliance node"
      },
      {
        "id": "priority-tagged",
        "priority": 15,
        "condition": { "TagEq": { "key": "priority", "value": "high" } },
        "action": { "Route": "fast-path-out" },
        "description": "Route packets tagged high-priority by upstream nodes"
      },
      {
        "id": "standard",
        "priority": 20,
        "condition": "Always",
        "action": { "Route": "standard-out" }
      }
    ],
    "default_action": "Drop"
  }
}
```

**Side-car file (resolved via `RuleSetSource`):**

```json
{
  "node": "route-by-type",
  "contract": "pureflow.rules.v1",
  "rule_set_ref": "rulesets/account-router.rules.json"
}
```

The `rule_set_ref` URI scheme determines which `RuleSetSource` implementation
is used. Plain paths use `LocalFsSource`. Custom schemes (e.g.
`guardiandb://...`) are resolved by application-provided sources registered
at workflow load time.

### 4.4 Contract And Introspection

`pureflow-contract` gains a `RuleSetContractRef` variant on `SchemaRef` so the
validation pipeline can verify:

- Every output port referenced in a `Route` action exists in the workflow.
- No `Route` action targets a port absent from the node's output ports.
- `DeadLetter` is only used when a `dead_letter` output port is declared.
- `SourceNode(node_id)` references a node that exists in the graph and has a
  reachable path to this `RuleNode`.
- `ArrivedOnPort(port_id)` references a valid input port on this node.

`pureflow-introspection` gains a `RuleSetIntrospection` projection that exposes:

- The full rule set definition.
- Which ports each rule can target.
- Which conditions reference payload fields vs. Pureflow context.
- The default action.
- Unreachable rules (lower-priority rules whose conditions are subsumed by
  higher-priority rules — computed at validation time for `Always`/`Never` and
  exact `FieldEq` subsumption; extended conservatively).

This makes `pureflow-cli explain` able to print: "node `route-by-type` routes
to `fast-path-out` for packets tagged `priority=high`, to `high-value-out` for
amounts ≥ 10000, otherwise to `standard-out`."

---

## 5. Metadata Integration

Every `RuleNode` execution emits a `MetadataRecord::RuleEval` into the sink.
Emission occurs in the asupersync **finalize phase** of the cancellation
protocol — it is guaranteed, not best-effort. A cancelled `RuleNode` still
emits the record for the packet it was processing when cancellation arrived.

```rust
pub struct RuleEvalRecord {
    // Workflow and execution identity
    pub execution_id: ExecutionId,
    pub workflow_id: WorkflowId,
    pub node_id: NodeId,
    pub rule_set_id: RuleSetId,
    pub strategy: EvaluationStrategy,

    // Decision
    pub matched_rule: Option<RuleId>,
    pub action_taken: RuleAction,
    pub rules_evaluated: u32,
    pub tags_applied: Vec<(String, ScalarValue)>,

    // Provenance
    pub source_node: NodeId,
    pub arrived_on_port: PortId,
    pub hop_count: u32,
    pub tags_present_at_eval: Vec<(String, ScalarValue)>,

    // Condition audit trail
    pub conditions_evaluated: Vec<ConditionTrace>,

    pub timestamp: Timestamp,
}

pub struct ConditionTrace {
    pub rule_id: RuleId,
    pub condition: Condition,
    pub result: bool,
    pub surface: ConditionSurface, // Payload | Tag | Provenance | ExecutionContext
}
```

`ConditionTrace` records what each condition saw when it evaluated — not just
which rule fired, but why. This is sufficient for:

- Auditing why a packet took a given path.
- Counting rule hit rates across a run.
- Detecting dead rules (rules that never fire over a workload).
- Detecting which conditions fired on payload vs. on Pureflow metadata.
- AI-assisted rule tuning: "which rule fired most often? which never fired?
  which conditions always evaluated against stale tags?"

---

## 6. WASM Integration

Rule sets can also be evaluated inside a WASM component, but the component must
accept a `RuleSet` as a WIT input type and return a `RuleDecision`. The host
does not give the guest direct channel access.

The host:

1. Reads a packet from `PortsIn`.
2. Serializes the packet, the rule set, and the evaluation context as WIT types.
3. Calls the WASM component.
4. Validates the returned `RuleDecision`.
5. Executes the action through the normal `PortsOut` API using asupersync
   reserve/commit sends.

This allows rule evaluation logic to live in a WASM component (e.g., a Python
or JS guest compiled to WASM) while keeping channel access host-owned and
auditable. The condition expression language is the WIT type, not a host
string-eval backdoor.

---

## 7. Implementation Phases

### Phase R1: Core Rule Types And Evaluator

Deliver:

- `pureflow-rules` crate with `Condition`, `RuleAction`, `Rule`, `RuleSet`,
  `EvalContext`, `RuleDecision`
- `RuleSetEvaluator::evaluate(cx, rule_set, packet, context)` as a pure
  function returning `Outcome<RuleDecision, RuleError>`
- `RuleEvalRecord` and `ConditionTrace` types
- `Budget` integration: evaluator checks quota during condition traversal
- Unit tests for every condition variant and strategy
- Property tests for `And`/`Or`/`Not` nesting

Exit criteria:

- All evaluation paths are covered by tests.
- Tests run under asupersync's deterministic lab runtime with virtual time.
- No dependency on `pureflow-engine` or runtime crates.
- `serde` serialization round-trips cleanly under the feature flag.

### Phase R2: Native Rule Node And Registry Integration

Deliver:

- `RuleNode` native executor with asupersync drain/finalize cancel protocol
- Reserve/commit sends for `Route` and `DeadLetter` actions
- Built-in contract `"pureflow.rules.v1"`
- Registry registration in `pureflow-engine`
- `RuleSetSource` trait in `pureflow-workflow-format`
- `LocalFsSource` and `EmbeddedSource` implementations
- Inline and side-car rule set loading
- Contract validation: output ports, dead-letter port, `SourceNode`,
  `ArrivedOnPort` references

Exit criteria:

- A workflow with a `RuleNode` validates cleanly end-to-end.
- Invalid port references in `Route` actions produce typed diagnostics.
- Invalid `SourceNode` references produce typed diagnostics at validation time.
- `pureflow-cli validate` and `pureflow-cli inspect` reflect rule node topology.

### Phase R3: Metadata And Introspection

Deliver:

- `MetadataRecord::RuleEval` emitted from `RuleNode` in finalize phase
- `ConditionTrace` per-condition audit records
- `RuleSetIntrospection` in `pureflow-introspection`
- Unreachable-rule detection at validation time
- `pureflow-cli explain` prints rule routing descriptions including which
  conditions are payload-based vs. Pureflow-context-based

Exit criteria:

- A full run of a rule-routing workflow produces `RuleEval` records in the
  JSONL sink.
- A cancelled `RuleNode` mid-packet still emits the record for that packet.
- `pureflow-cli explain` describes which rule routes to which port for a sample
  workflow.
- Unreachable rule warnings appear in `pureflow-cli validate` output.

### Phase R4: WASM Rule Evaluation

Deliver:

- WIT types for `RuleSet`, `EvalContext`, and `RuleDecision`
- Host-owned WASM rule evaluation path in `pureflow-wasm`
- A sample WASM component that implements rule evaluation for a custom
  condition type
- Capability enforcement: WASM rule components have the same channel-access
  restrictions as other batch nodes

Exit criteria:

- A WASM rule node and a native rule node produce equivalent outcomes for the
  same rule set.
- WASM capability violations fail with stable typed errors.

---

## 8. What This Is Not

**Not a general policy engine.** Pureflow's rules engine evaluates conditions
against packet payloads and Pureflow context and produces routing actions. It
does not model hierarchical policies, role-based access control, obligation
frameworks, or cross-workflow state. Those are out of scope.

**Not a script runner.** There is no `Eval`, `Script`, `LuaExpr`, or `JSExpr`
condition variant. Arbitrary code in a condition breaks the audit guarantee and
the inspectability invariant.

**Not a replacement for native executors.** Complex transformation, stateful
aggregation, and domain logic still belong in native nodes. The rules engine is
for *routing decisions*, not *computation*.

**Not a streaming query engine.** Arrow and DataFusion remain deferred to Phase 5
of the core roadmap. The rules engine operates over `PacketPayload::Control` and
`PacketPayload::Structured` variants.

**Not a rule distribution or sync layer.** How rule sets reach a deployed
application — whether from disk, a remote database, a P2P sync layer, or any
other mechanism — is an application-level concern. `pureflow-rules` and
`pureflow-workflow-format` define the `RuleSetSource` trait boundary and ship
two built-in implementations (`LocalFsSource`, `EmbeddedSource`). All other
delivery mechanisms are provided by the application via the `SourceRegistry`,
with no coupling to any specific sync technology.

---

## 9. Risk Register

| Risk | Severity | Mitigation |
|---|---|---|
| Condition language grows into a scripting language over time | High | Enumerate all allowed variants; any new variant requires a proposal; `Eval`/`Script` are permanently rejected by name |
| Rule sets become a maintenance burden when embedded in workflow JSON | Medium | Side-car `rule_set_ref` files keep rule sets versionable independently of topology |
| Unreachable-rule detection produces false positives for `Or` conditions | Medium | Restrict unreachable analysis to `Always`/`Never` and exact `FieldEq` subsumption at first; extend conservatively |
| WASM rule evaluation leaks channel access | High | WIT boundary is the only call path; the host owns all channel reads/writes |
| `RuleEval` metadata record volume overwhelms JSONL sink | Medium | `trace_conditions: bool` on `RuleSet` controls per-condition trace verbosity; default false = zero trace allocation; flip true only on audit nodes |
| `AllMatches` rules with conflicting terminal actions produce ambiguous routing | High | Resolved at validation time: `AllMatches` rules may only use `Tag` actions; `Route`/`Halt`/`DeadLetter`/`Drop` rejected with typed diagnostic |
| Rule semantics differ between native and WASM evaluators | Medium | Canonical evaluator in `pureflow-rules` is the reference implementation; WASM path calls it through WIT serialization |
| Naming collision between `RuleDecision` and asupersync `Outcome<T,E>` | High | Resolved by naming: the rule result type is `RuleDecision`; `Outcome` is exclusively the asupersync return envelope |
| Pathological nested `And`/`Or` conditions spin the evaluator | Medium | `Budget` quota check in evaluator loop; returns `Outcome::Cancelled` when exhausted |
| Cancellation loses the metadata record for the packet in flight | High | Resolved structurally: `RuleEvalRecord` emission is in asupersync finalize phase; Region does not quiesce until emission completes |
| `SourceNode` / `ArrivedOnPort` conditions reference non-existent graph elements | Medium | Contract validation at load time; invalid references produce typed diagnostics before any execution |

---

## 10. Relationship To Existing Proposals

This proposal extends the architecture described in `proposal_final.md`:

- Phase R1 can begin after Phase 2 of the core roadmap (contracts and formats
  are stable enough to represent a built-in rule contract).
- Phase R3 depends on the metadata productization work from Phase 3 of the
  core roadmap (`JsonlMetadataSink`, `TieredMetadataSink`).
- Phase R4 depends on the WASM vertical slice from Phase 4 of the core
  roadmap (`BatchExecutor`, `pureflow-wasm`).

The rules engine does not change any existing crate boundary. It adds
`pureflow-rules` as a peer to `pureflow-contract`, and `RuleNode` as a
built-in registration in the existing native executor registry.

**Asupersync dependency**: `pureflow-rules` takes a direct dependency on
`asupersync` for `Cx`, `Outcome`, and `Budget`. This is consistent with the
rest of the Pureflow crate graph. There is no tokio dependency in this crate.

---

## 11. Proposed Beads

1. `rules-types-core`: add `Condition`, `RuleAction`, `Rule`, `RuleSet`,
   `EvaluationStrategy`, `RuleDecision`, `EvalContext` to new `pureflow-rules`
   crate with serde feature.
2. `rules-evaluator`: implement `RuleSetEvaluator::evaluate` with `Cx` and
   `Budget` integration, all condition variants, strategies, and tag
   application; return `Outcome<RuleDecision, RuleError>`.
3. `rules-eval-metadata`: add `RuleEvalRecord` and `ConditionTrace` to
   `pureflow-core` metadata types.
4. `rules-native-node`: implement `RuleNode` with asupersync drain/finalize
   cancel protocol and reserve/commit port sends; register built-in contract
   `"pureflow.rules.v1"`.
5. `rules-set-source`: add `RuleSetSource` trait to `pureflow-workflow-format`;
   implement `LocalFsSource` and `EmbeddedSource`.
6. `rules-format-loading`: add inline and side-car rule set loading to
   `pureflow-workflow-format` using `RuleSetSource`.
7. `rules-contract-validation`: add port-reference validation for `Route` and
   `DeadLetter` actions, and graph-reference validation for `SourceNode` and
   `ArrivedOnPort` conditions, in the contract validation pipeline.
8. `rules-introspection`: add `RuleSetIntrospection` projection and
   unreachable-rule detection; expose payload vs. context condition surface.
9. `rules-cli-explain`: extend `pureflow-cli explain` to describe rule routing
   in plain text, distinguishing payload-based from context-based conditions.
10. `rules-metadata-jsonl`: emit `RuleEval` records (with `ConditionTrace`) from
    `RuleNode` to `JsonlMetadataSink` in asupersync finalize phase.
11. `rules-wasm-wit`: define WIT types for `RuleSet`, `EvalContext`, and
    `RuleDecision`.
12. `rules-wasm-host`: implement WASM rule evaluation path in `pureflow-wasm`.
13. `rules-example-routing`: add a runnable workflow example that uses a rule
    node for content-based routing, demonstrating both payload and tag
    conditions.

---

## References

- Final architecture strategy: `docs/archetecture/proposal_final.md`
- Crate boundaries and current state: `README.md`
- Contract types: `crates/pureflow-contract/`
- Metadata types: `crates/pureflow-core/src/` (metadata, error taxonomy)
- Introspection projections: `crates/pureflow-introspection/`
- WASM batch adapter: `crates/pureflow-wasm/`
- Workflow format loading: `crates/pureflow-workflow-format/`
- Epic 1 bead tree: `docs/epics/epic-1-foundation.md`
- Asupersync runtime: https://github.com/Dicklesworthstone/asupersync
