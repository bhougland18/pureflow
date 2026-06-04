# Content-Based Routing Workload

This workload demonstrates the Pureflow rules engine: a declarative, inspectable
`RuleNode` routing a payment stream by content. It shows the two ways a rule set
reaches a node (inline and side-car), parallel routers sharing one rule set, and
an audit router with per-condition tracing.

Run commands from the repository root.

## The routing policy

The payment router evaluates rules in priority order (`FirstMatch`):

| rule              | condition                    | surface  | action                   |
|-------------------|------------------------------|----------|--------------------------|
| `high-value`      | `amount >= 10000`            | payload  | Route `high-value-out`   |
| `priority-tagged` | `tag priority == "high"`     | tag      | Route `fast-path-out`    |
| `standard`        | `Always`                     | constant | Route `standard-out`     |

When no rule matches the `default_action` (`Drop`) applies. The `priority-tagged`
rule draws from the **tag** surface; tags are applied by upstream `Tag` actions,
so in the payload-only stream below that lane stays empty at runtime while the
rule remains visible to `pureflow explain`.

## Topology

Files:

- Workflow document: `examples/workloads/rules-content-routing.workflow.json`
- Side-car rule set: `examples/workloads/rules-content-routing.fast-path.rules.json`
- Runnable example: `crates/pureflow-engine/examples/rules_content_routing.rs`

The workflow document carries the rule set two ways:

- `router-inline` embeds the policy directly as an inline `rule_set`.
- `router-sidecar` references it by URI with `rule_set_ref`, resolved at load
  time through a `SourceRegistry` rooted at the workflow file's directory.

Both are equivalent once loaded; the side-car form is what lets a rule set be
delivered out-of-band (e.g. fetched and cached by the application) without
editing or re-shipping the workflow. The `RuleNode` never learns where its rule
set came from.

## Validate and explain

```bash
cargo run -p pureflow-cli -- validate examples/workloads/rules-content-routing.workflow.json
cargo run -p pureflow-cli -- explain  examples/workloads/rules-content-routing.workflow.json
```

`validate` ignores the rule sets and checks the graph:

```text
valid workflow `rules-content-routing`
nodes: 6
edges: 8
```

`explain` resolves both the inline and side-car rule sets and renders the routing
in plain text, labelling each rule's condition surface and flagging unreachable
rules:

```text
rule routing:
  node router-inline: rule set `payment-router` strategy first-match
    rule high-value (priority 10) — if amount >= 10000 [payload] → Route(high-value-out)
    rule priority-tagged (priority 20) — if tag priority="high" [tag] → Route(fast-path-out)
    rule standard (priority 30) — if always [constant] → Route(standard-out)
    default → Drop
  node router-sidecar: rule set `payment-router-audit` strategy first-match
    ...
```

## Run

```bash
cargo run -p pureflow-engine --example rules_content_routing
```

Expected output:

```text
content-routing workflow `rules-content-routing` completed
payments processed: 4
parallel routers sharing one Arc<RuleSet>: router-a, router-b
audit router with trace_conditions=true: audit
high-value lane packets: 6
standard lane packets:   6
fast-path lane packets:  0 (tag surface, unused by this payload-only stream)
rule_eval metadata records: 12
rule_eval records carrying a condition trace (audit): 4
scheduled nodes: 7
completed nodes: 7
```

The example broadcasts four payments to three rule nodes:

- `router-a` and `router-b` are two `RuleNode` instances that share the **same**
  `Arc<RuleSet>`. The routing policy is stateless and immutable, so it is cloned
  by reference and replicated for throughput — no rule data is duplicated.
- `audit` is a third `RuleNode` whose rule set has `trace_conditions = true`. Its
  `rule_eval` metadata records carry a populated `conditions_evaluated` trace for
  compliance review, while the hot-path routers (`trace_conditions = false`) emit
  the same routing records with an empty trace and zero per-condition allocation.

Each router evaluates every payment, so four payments across three routers
produce twelve `rule_eval` records; only the audit router's four records carry a
condition trace. Two payments clear the `high-value` threshold and three routers
each route them, so the high-value and standard lanes collect six packets each.

## Metadata shape

The run writes one `JsonlMetadataSink<Vec<u8>>` shared by the engine and the rule
nodes, so a single JSONL stream interleaves:

- `lifecycle` / `message` / `queue_pressure` — emitted by the engine.
- `rule_eval` — emitted by each `RuleNode` in the finalize phase, *before* the
  routed packet is sent, so a cancelled send still leaves an audit record. Each
  record names the rule set, the matched rule, the action taken, the packet's
  provenance (source node, arrival port, hop count), and — when tracing is on —
  the per-condition `conditions_evaluated` trace.

## Polylith co-location pattern

A `RuleNode` is a separate, stateless component from the transformation/execution
nodes it routes between. In a polylith, that means the **rules node and the
transformation node are co-located components in the same brick, not bundled into
one artifact**:

```text
brick: payments
  ├── component: payment-transform   (validates/normalises the payment)
  └── component: payment-router      (RuleNode: routes by content)
```

Keeping them as distinct components — each with its own contract and ports — buys
the properties the rules engine is designed for:

- **Independent inspectability.** `pureflow explain` describes routing without
  touching transformation logic; the rule set is auditable on its own.
- **Independent throughput.** Stateless routers replicate freely (the shared
  `Arc<RuleSet>` above) without cloning rules or coupling to transform state.
- **Independent delivery.** A rule set can be updated via `rule_set_ref` (fetched
  and cached by the application) without rebuilding the transformation component
  or re-shipping the workflow.
- **Scoped policy.** Each area/type/tag can own a router with rules specific to
  it, rather than one monolithic rule node for the whole graph.

The router knows nothing about the application's rule-delivery mechanism; the
boundary is the `rule_set` / `rule_set_ref` field in the workflow document and
the `RuleSetSource` trait behind it.
