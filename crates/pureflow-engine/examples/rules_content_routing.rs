//! Runnable content-based routing workload driven by `RuleNode`s.
//!
//! A payment stream is broadcast to three rule nodes that share a single
//! declarative rule set:
//!
//! - `router-a` and `router-b` are two parallel `RuleNode` instances that share
//!   the *same* `Arc<RuleSet>` — the stateless, inspectable routing policy can be
//!   replicated for throughput without copying the rules.
//! - `audit` is a third `RuleNode` whose rule set has `trace_conditions = true`,
//!   so its `RuleEval` metadata records carry a per-condition trace for
//!   compliance review, while the hot-path routers stay allocation-free.
//!
//! The routing policy is the payment example from the rules-engine proposal:
//!
//! | rule             | condition                       | action                 |
//! |------------------|---------------------------------|------------------------|
//! | `high-value`     | `amount >= 10000` (payload)     | Route `high-value-out` |
//! | `priority-tagged`| `tag priority == "high"`        | Route `fast-path-out`  |
//! | `standard`       | `Always`                        | Route `standard-out`   |
//!
//! Conditions are evaluated in priority order (`FirstMatch`). The `priority-tagged`
//! rule draws from the *tag* surface — tags are applied by upstream `Tag` actions,
//! so in this payload-only stream that lane stays empty at runtime while the rule
//! remains visible to `pureflow explain`. The `high-value` and `standard` lanes are
//! exercised live.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{Arc, Mutex},
};

use futures::{executor::block_on, future::BoxFuture};
use pureflow_core::{
    JsonlMetadataSink, NodeExecutor, PacketPayload, PortPacket, PortRecvError, PortsIn, PortsOut,
    PureflowError,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::{
    StaticNodeExecutorRegistry, WorkflowRunSummary, WorkflowTerminalState,
    run_workflow_with_registry_and_metadata_sink_summary,
};
use pureflow_rules::{
    Condition, EvaluationStrategy, FieldPath, Rule, RuleAction, RuleNode, RuleSet, ScalarValue,
};
use pureflow_test_kit::{NodeBuilder, WorkflowBuilder};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId};
use pureflow_workflow::WorkflowDefinition;

/// Payments fed into the workflow: two clear the `high-value` threshold, two do not.
const PAYMENTS: &[(&str, i64)] = &[("p-1", 50_000), ("p-2", 250), ("p-3", 10_000), ("p-4", 999)];

/// Build the shared payment routing policy. `trace` toggles per-condition tracing.
fn payment_rule_set(trace: bool) -> RuleSet {
    RuleSet::new(
        "payment-router",
        EvaluationStrategy::FirstMatch,
        vec![
            Rule::new(
                "high-value",
                Condition::FieldGte {
                    path: FieldPath::new("amount").expect("field path"),
                    value: ScalarValue::Integer(10_000),
                },
                RuleAction::Route(port_id("high-value-out").expect("port")),
                10,
                "Route payments of $10,000 or more to the high-value lane",
            )
            .expect("rule"),
            Rule::new(
                "priority-tagged",
                Condition::TagEq {
                    key: "priority".to_owned(),
                    value: ScalarValue::String("high".to_owned()),
                },
                RuleAction::Route(port_id("fast-path-out").expect("port")),
                20,
                "Route packets tagged priority=high to the fast path",
            )
            .expect("rule"),
            Rule::new(
                "standard",
                Condition::Always,
                RuleAction::Route(port_id("standard-out").expect("port")),
                30,
                "Everything else takes the standard lane",
            )
            .expect("rule"),
        ],
        RuleAction::Drop,
        trace,
    )
    .expect("rule set")
}

#[derive(Clone)]
enum ContentRoutingExecutor {
    /// Emits the payment stream onto `payments`.
    Source,
    /// A rule node (primary replica or audit) — delegates to the shared `RuleNode`.
    Router(Arc<RuleNode>),
    /// Collects everything that reaches one routing lane.
    Sink {
        received: Arc<Mutex<Vec<PacketPayload>>>,
    },
}

impl NodeExecutor for ContentRoutingExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        match self {
            Self::Source => Box::pin(async move {
                let cancellation = ctx.cancellation_token();
                for (index, (id, amount)) in PAYMENTS.iter().enumerate() {
                    let packet = payment_packet(&ctx, index, id, *amount)?;
                    outputs
                        .send(&port_id("payments")?, packet, &cancellation)
                        .await
                        .map_err(|e| PureflowError::execution(e.to_string()))?;
                }
                Ok(())
            }),
            // The RuleNode is a self-contained executor; just forward to it.
            Self::Router(node) => node.run(ctx, inputs, outputs),
            Self::Sink { received } => {
                let received = received.clone();
                Box::pin(async move {
                    let cancellation = ctx.cancellation_token();
                    let in_port = port_id("in")?;
                    let mut inputs = inputs;
                    loop {
                        match inputs.recv(&in_port, &cancellation).await {
                            Ok(Some(packet)) => {
                                received
                                    .lock()
                                    .expect("sink lock should not be poisoned")
                                    .push(packet.into_payload());
                            }
                            Ok(None) | Err(PortRecvError::Disconnected { .. }) => break,
                            Err(e) => return Err(PureflowError::execution(e.to_string())),
                        }
                    }
                    Ok(())
                })
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // Shared metadata sink: lifecycle/message/queue-pressure records come from
    // the engine, RuleEval records come from each RuleNode — all into one JSONL
    // stream. Each RuleNode is given a clone so it can emit RuleEvalRecords.
    let metadata_sink: Arc<JsonlMetadataSink<Vec<u8>>> =
        Arc::new(JsonlMetadataSink::new(Vec::new()));

    // One shared routing policy, replicated across two parallel routers.
    let shared_rules: Arc<RuleSet> = Arc::new(payment_rule_set(false));
    // A separate audit policy with per-condition tracing enabled.
    let audit_rules: Arc<RuleSet> = Arc::new(payment_rule_set(true));

    let router_a: Arc<RuleNode> =
        Arc::new(RuleNode::new(shared_rules.clone()).with_metadata_sink(metadata_sink.clone()));
    let router_b: Arc<RuleNode> =
        Arc::new(RuleNode::new(shared_rules.clone()).with_metadata_sink(metadata_sink.clone()));
    let audit: Arc<RuleNode> =
        Arc::new(RuleNode::new(audit_rules).with_metadata_sink(metadata_sink.clone()));

    let high: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));
    let standard: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));
    let fast: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));

    let registry: StaticNodeExecutorRegistry<ContentRoutingExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("source")?, ContentRoutingExecutor::Source),
            (
                node_id("router-a")?,
                ContentRoutingExecutor::Router(router_a),
            ),
            (
                node_id("router-b")?,
                ContentRoutingExecutor::Router(router_b),
            ),
            (node_id("audit")?, ContentRoutingExecutor::Router(audit)),
            (
                node_id("high-sink")?,
                ContentRoutingExecutor::Sink {
                    received: high.clone(),
                },
            ),
            (
                node_id("standard-sink")?,
                ContentRoutingExecutor::Sink {
                    received: standard.clone(),
                },
            ),
            (
                node_id("fast-sink")?,
                ContentRoutingExecutor::Sink {
                    received: fast.clone(),
                },
            ),
        ]));

    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("rules-content-routing-example")?);

    let summary: WorkflowRunSummary =
        block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow,
            &execution,
            &registry,
            metadata_sink.clone(),
        ))?;
    metadata_sink.flush()?;

    let high_count = high.lock().expect("high lock").len();
    let standard_count = standard.lock().expect("standard lock").len();
    let fast_count = fast.lock().expect("fast lock").len();

    // Release the registry (and with it each RuleNode's sink clone) so the
    // metadata sink is uniquely owned and can be unwrapped for inspection.
    drop(registry);
    let metadata_jsonl: String = metadata_jsonl_from_sink(metadata_sink)?;
    let rule_eval_records: usize = metadata_jsonl
        .lines()
        .filter(|line| line.contains("\"record_type\":\"rule_eval\""))
        .count();
    // A populated condition trace serializes as `"conditions_evaluated":[{...}]`;
    // with tracing off the array is empty. Only the audit router traces.
    let traced_records: usize = metadata_jsonl
        .lines()
        .filter(|line| {
            line.contains("\"record_type\":\"rule_eval\"")
                && line.contains("\"conditions_evaluated\":[{")
        })
        .count();

    // Three routers each evaluate every payment.
    let expected_rule_evals = PAYMENTS.len() * 3;
    if rule_eval_records != expected_rule_evals {
        return Err(PureflowError::metadata(format!(
            "expected {expected_rule_evals} rule_eval records, found {rule_eval_records}"
        ))
        .into());
    }
    // Two payments clear the threshold; three routers each route them.
    if high_count != 2 * 3 || standard_count != 2 * 3 || fast_count != 0 {
        return Err(PureflowError::execution(format!(
            "unexpected lane counts: high={high_count} standard={standard_count} fast={fast_count}"
        ))
        .into());
    }
    // Only the audit router traces; it evaluates every payment.
    if traced_records != PAYMENTS.len() {
        return Err(PureflowError::metadata(format!(
            "expected {} traced rule_eval records from the audit router, found {traced_records}",
            PAYMENTS.len()
        ))
        .into());
    }

    println!("content-routing workflow `{}` completed", workflow.id());
    println!("payments processed: {}", PAYMENTS.len());
    println!("parallel routers sharing one Arc<RuleSet>: router-a, router-b");
    println!("audit router with trace_conditions=true: audit");
    println!("high-value lane packets: {high_count}");
    println!("standard lane packets:   {standard_count}");
    println!(
        "fast-path lane packets:  {fast_count} (tag surface, unused by this payload-only stream)"
    );
    println!("rule_eval metadata records: {rule_eval_records}");
    println!("rule_eval records carrying a condition trace (audit): {traced_records}");
    println!("scheduled nodes: {}", summary.scheduled_node_count());
    println!("completed nodes: {}", summary.completed_node_count());

    if summary.terminal_state() != WorkflowTerminalState::Completed {
        return Err(PureflowError::execution(
            "workflow did not reach the Completed terminal state",
        )
        .into());
    }
    summary.into_result()?;
    Ok(())
}

/// Source broadcasts to three routers; each router fans out to the three lanes.
fn workflow() -> WorkflowDefinition {
    let router = |name: &str| {
        NodeBuilder::new(name)
            .input("in")
            .output("high-value-out")
            .output("fast-path-out")
            .output("standard-out")
            .build()
    };

    WorkflowBuilder::new("rules-content-routing")
        .node(NodeBuilder::new("source").output("payments").build())
        .node(router("router-a"))
        .node(router("router-b"))
        .node(router("audit"))
        .node(NodeBuilder::new("high-sink").input("in").build())
        .node(NodeBuilder::new("standard-sink").input("in").build())
        .node(NodeBuilder::new("fast-sink").input("in").build())
        // Broadcast the payment stream to every router.
        .edge("source", "payments", "router-a", "in")
        .edge("source", "payments", "router-b", "in")
        .edge("source", "payments", "audit", "in")
        // Each router's lanes fan in to the shared sinks.
        .edge("router-a", "high-value-out", "high-sink", "in")
        .edge("router-a", "standard-out", "standard-sink", "in")
        .edge("router-a", "fast-path-out", "fast-sink", "in")
        .edge("router-b", "high-value-out", "high-sink", "in")
        .edge("router-b", "standard-out", "standard-sink", "in")
        .edge("router-b", "fast-path-out", "fast-sink", "in")
        .edge("audit", "high-value-out", "high-sink", "in")
        .edge("audit", "standard-out", "standard-sink", "in")
        .edge("audit", "fast-path-out", "fast-sink", "in")
        .build()
}

fn payment_packet(
    ctx: &NodeContext,
    index: usize,
    id: &str,
    amount: i64,
) -> pureflow_core::Result<PortPacket> {
    let endpoint = MessageEndpoint::new(ctx.node_id().clone(), port_id("payments")?);
    let route = MessageRoute::new(Some(endpoint.clone()), endpoint);
    let metadata = MessageMetadata::new(
        MessageId::new(format!("payment-{index}"))?,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );
    let payload = PacketPayload::control(serde_json::json!({ "id": id, "amount": amount }));
    Ok(PortPacket::new(metadata, payload))
}

fn metadata_jsonl_from_sink(
    metadata_sink: Arc<JsonlMetadataSink<Vec<u8>>>,
) -> pureflow_core::Result<String> {
    let sink: JsonlMetadataSink<Vec<u8>> = match Arc::try_unwrap(metadata_sink) {
        Ok(sink) => sink,
        Err(_arc) => {
            return Err(PureflowError::metadata(
                "metadata sink still had multiple references after run",
            ));
        }
    };
    let bytes: Vec<u8> = sink.into_inner()?;
    String::from_utf8(bytes).map_err(|source| {
        PureflowError::metadata(format!("metadata JSONL was not UTF-8: {source}"))
    })
}

fn node_id(value: &str) -> Result<NodeId, pureflow_types::IdentifierError> {
    NodeId::new(value)
}

fn port_id(value: &str) -> Result<PortId, pureflow_types::IdentifierError> {
    PortId::new(value)
}
