//! Native RuleNode executor — first-class Pureflow node for declarative routing.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use pureflow_core::{
    CancellationToken, ConditionSurfaceRecord, ConditionTrace, MetadataRecord, MetadataSink,
    NodeExecutor, PacketPayload, PortPacket, PortSendError, PortsIn, PortsOut, PureflowError,
    Result, RuleEvalAction, RuleEvalRecord, RuleEvalStrategy,
    context::NodeContext,
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
    ports::PortRecvError,
};
use pureflow_types::{MessageId, PortId};

use crate::{
    action::RuleAction,
    condition::{ConditionSurface, EvalContext, ScalarValue},
    eval::RuleSetEvaluator,
    rule::{EvaluationStrategy, RuleDecision, RuleSet},
};

/// Built-in contract identifier for the native rule node.
///
/// Workflow authors reference this ID to declare a rule-routing node without
/// writing any Rust code:
///
/// ```json
/// { "node": "route-by-value", "contract": "pureflow.rules.v1" }
/// ```
pub const CONTRACT_ID: &str = "pureflow.rules.v1";

/// Built-in port names.
const IN_PORT: &str = "in";

/// Native executor that evaluates a [`RuleSet`] against each incoming packet
/// and routes it to the appropriate output port.
///
/// `RuleNode` is stateless. Multiple instances may share a single
/// `Arc<RuleSet>` with no contention — fan-out to N parallel `RuleNode`
/// instances over the same rule set is zero-cost from an evaluation standpoint.
///
/// ## Cancel semantics
///
/// When cancellation is requested:
/// - If the node is waiting for a packet, the wait returns immediately and the
///   node returns `PureflowError::cancelled`.
/// - If the node is mid-packet (evaluating or routing), it finishes that
///   packet's action before checking for cancellation on the next receive.
///
/// This ensures the packet in flight is always fully processed — the "drain"
/// guarantee described in the proposal.
pub struct RuleNode {
    rule_set: Arc<RuleSet>,
    evaluator: RuleSetEvaluator,
    /// Optional metadata sink for emitting `RuleEvalRecord` entries.
    ///
    /// Pass the same `Arc<M>` used with the workflow runner so that
    /// `RuleEval` records appear in the same JSONL stream as lifecycle and
    /// message boundary records.
    metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
    /// Monotonic counter used to generate unique message ids within one run.
    packet_counter: AtomicU64,
}

impl RuleNode {
    /// Create a rule node that evaluates `rule_set` for every incoming packet.
    ///
    /// No metadata records are emitted without a sink. Use
    /// [`with_metadata_sink`](Self::with_metadata_sink) to enable
    /// `RuleEvalRecord` emission.
    #[must_use]
    pub fn new(rule_set: Arc<RuleSet>) -> Self {
        Self {
            rule_set,
            evaluator: RuleSetEvaluator,
            metadata_sink: None,
            packet_counter: AtomicU64::new(0),
        }
    }

    /// Attach a metadata sink so the node emits [`RuleEvalRecord`] entries.
    ///
    /// Pass the same `Arc<M>` you will pass to the workflow runner. Records
    /// are emitted after evaluation and before the routing send, so they are
    /// always written even if the send is later cancelled.
    #[must_use]
    pub fn with_metadata_sink<M>(mut self, sink: Arc<M>) -> Self
    where
        M: MetadataSink + Send + Sync + 'static,
    {
        self.metadata_sink = Some(sink);
        self
    }

    /// The rule set this node evaluates.
    #[must_use]
    pub fn rule_set(&self) -> &RuleSet {
        &self.rule_set
    }
}

impl std::fmt::Debug for RuleNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleNode")
            .field("rule_set_id", &self.rule_set.id)
            .field("strategy", &self.rule_set.strategy)
            .field("rules", &self.rule_set.rules.len())
            .field("has_metadata_sink", &self.metadata_sink.is_some())
            .finish()
    }
}

impl NodeExecutor for RuleNode {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            let in_port = PortId::new(IN_PORT)
                .map_err(|e| PureflowError::execution(format!("invalid port id: {e}")))?;

            loop {
                // Receive one packet. Cancel on cancellation; stop on upstream close.
                let packet: PortPacket = match inputs.recv(&in_port, &cancellation).await {
                    Ok(Some(p)) => p,
                    // Upstream closed cleanly — normal end of stream.
                    Ok(None) => break,
                    // Upstream closed (all senders dropped) — end of stream.
                    Err(PortRecvError::Disconnected { .. }) => break,
                    // Port not declared — configuration error, fail loudly.
                    Err(PortRecvError::UnknownPort { port_id }) => {
                        return Err(PureflowError::execution(format!(
                            "rule node: input port `{port_id}` not declared"
                        )));
                    }
                    // Cancellation observed while waiting — drain phase is
                    // complete (no packet in flight), return cancelled.
                    Err(PortRecvError::Cancelled { .. }) => {
                        return Err(PureflowError::cancelled(
                            "rule node cancelled while waiting for input",
                        ));
                    }
                };

                // --- Drain phase begins: finish this packet regardless of cancellation ---

                // Build the evaluation context from packet routing metadata.
                let source_node = packet.metadata().route().source().map(|ep| ep.node_id());
                let arrival_port = Some(packet.metadata().route().target().port_id());
                let tags: BTreeMap<String, ScalarValue> = BTreeMap::new();
                let exec_meta: BTreeMap<String, ScalarValue> = BTreeMap::new();

                let eval_ctx = EvalContext {
                    tags: &tags,
                    source_node,
                    arrival_port,
                    hop_count: 0,
                    workflow_id: ctx.workflow_id(),
                    execution_metadata: &exec_meta,
                };

                // Evaluate the rule set against this packet.
                let decision = self
                    .evaluator
                    .evaluate(&self.rule_set, &packet, &eval_ctx)
                    .map_err(|e| PureflowError::execution(format!("rule evaluation error: {e}")))?;

                // Emit the RuleEvalRecord BEFORE executing the send.
                // This guarantees the record is written even if the send is
                // later cancelled — the finalize guarantee described in the proposal.
                if let Some(sink) = &self.metadata_sink {
                    let record =
                        build_rule_eval_record(&ctx, &self.rule_set, &packet, &decision, &tags);
                    let _ = sink.record(&MetadataRecord::RuleEval(record));
                }

                // Execute the routing decision.
                let seq = self.packet_counter.fetch_add(1, Ordering::Relaxed);
                execute_action(&decision.action, &ctx, seq, packet, &outputs, &cancellation)
                    .await?;

                // --- Drain phase complete ---

                // Check for cancellation between packets (after finishing the current one).
                if cancellation.is_cancelled() {
                    return Err(PureflowError::cancelled(
                        "rule node cancelled after processing packet",
                    ));
                }
            }

            Ok(())
        })
    }
}

/// Build a `RuleEvalRecord` from a completed evaluation, ready for the metadata sink.
fn build_rule_eval_record(
    ctx: &NodeContext,
    rule_set: &RuleSet,
    packet: &PortPacket,
    decision: &RuleDecision,
    tags_at_eval: &BTreeMap<String, ScalarValue>,
) -> RuleEvalRecord {
    let source_node = packet
        .metadata()
        .route()
        .source()
        .map(|ep| ep.node_id().clone());
    let arrival_port = Some(packet.metadata().route().target().port_id().clone());

    RuleEvalRecord {
        node_id: ctx.node_id().clone(),
        workflow_id: ctx.workflow_id().clone(),
        execution: ctx.execution().clone(),
        rule_set_id: rule_set.id.clone(),
        strategy: strategy_to_eval(rule_set.strategy),
        matched_rule: decision.matched_rule.clone(),
        action_taken: action_to_eval(&decision.action),
        rules_evaluated: rule_set.rules.len() as u32,
        tags_applied: decision
            .tags_applied
            .iter()
            .map(|(k, v)| (k.clone(), scalar_to_json(v)))
            .collect(),
        source_node,
        arrival_port,
        hop_count: 0,
        tags_present_at_eval: tags_at_eval
            .iter()
            .map(|(k, v)| (k.clone(), scalar_to_json(v)))
            .collect(),
        conditions_evaluated: decision.conditions_evaluated.clone(),
    }
}

fn strategy_to_eval(s: EvaluationStrategy) -> RuleEvalStrategy {
    match s {
        EvaluationStrategy::FirstMatch => RuleEvalStrategy::FirstMatch,
        EvaluationStrategy::AllMatches => RuleEvalStrategy::AllMatches,
        EvaluationStrategy::Score => RuleEvalStrategy::Score,
    }
}

fn action_to_eval(action: &RuleAction) -> RuleEvalAction {
    match action {
        RuleAction::Route(port) => RuleEvalAction::Route(port.clone()),
        RuleAction::Drop => RuleEvalAction::Drop,
        RuleAction::DeadLetter(reason) => RuleEvalAction::DeadLetter(reason.clone()),
        RuleAction::Tag { key, value } => RuleEvalAction::Tag {
            key: key.clone(),
            value: scalar_to_json(value).to_string(),
        },
        RuleAction::Halt(msg) => RuleEvalAction::Halt(msg.clone()),
    }
}

fn scalar_to_json(v: &ScalarValue) -> serde_json::Value {
    match v {
        ScalarValue::String(s) => serde_json::Value::String(s.clone()),
        ScalarValue::Integer(i) => serde_json::json!(*i),
        ScalarValue::Float(f) => serde_json::json!(*f),
        ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
        ScalarValue::Null => serde_json::Value::Null,
    }
}

/// Execute one routing action, consuming the packet.
async fn execute_action(
    action: &RuleAction,
    ctx: &NodeContext,
    seq: u64,
    packet: PortPacket,
    outputs: &PortsOut,
    cancellation: &CancellationToken,
) -> Result<()> {
    match action {
        RuleAction::Route(port_id) => {
            let forwarded = forward_packet(ctx, port_id, seq, packet)?;
            outputs
                .send(port_id, forwarded, cancellation)
                .await
                .map_err(|e: PortSendError| PureflowError::execution(e.to_string()))?;
        }
        RuleAction::Drop => {
            // Packet is consumed and discarded — no send needed.
        }
        RuleAction::DeadLetter(reason) => {
            let dead_port = PortId::new("dead_letter")
                .map_err(|e| PureflowError::execution(format!("invalid port id: {e}")))?;
            let forwarded = forward_packet(ctx, &dead_port, seq, packet)?;
            outputs
                .send(&dead_port, forwarded, cancellation)
                .await
                .map_err(|e: PortSendError| {
                    PureflowError::execution(format!("dead-letter send failed ({reason}): {e}"))
                })?;
        }
        RuleAction::Tag { .. } => {
            // Tag is a non-terminal action — the evaluator accumulates tags
            // and produces a terminal action. This arm is not reached as the
            // terminal of a FirstMatch/Score evaluation.
        }
        RuleAction::Halt(message) => {
            return Err(PureflowError::execution(format!("rule halted: {message}")));
        }
    }
    Ok(())
}

/// Forward an incoming packet to a new output port, updating its routing metadata.
fn forward_packet(
    ctx: &NodeContext,
    output_port: &PortId,
    seq: u64,
    incoming: PortPacket,
) -> Result<PortPacket> {
    let source = MessageEndpoint::new(ctx.node_id().clone(), output_port.clone());
    let target = MessageEndpoint::new(ctx.node_id().clone(), output_port.clone());
    let route = MessageRoute::new(Some(source), target);
    let message_id = MessageId::new(format!("{}-rule-{}-{}", ctx.node_id(), output_port, seq))
        .map_err(|e| PureflowError::execution(format!("rule node message id error: {e}")))?;
    let metadata = MessageMetadata::new(
        message_id,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );
    Ok(PortPacket::new(metadata, incoming.into_payload()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use pureflow_core::{
        CancellationHandle, JsonlMetadataSink, PacketPayload,
        context::ExecutionMetadata,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
        ports::PortRecvError,
    };
    use pureflow_engine::{
        StaticNodeExecutorRegistry, WorkflowRunSummary, WorkflowTerminalState,
        run_workflow_with_registry_and_metadata_sink_summary,
    };
    use pureflow_test_kit::{NodeBuilder, WorkflowBuilder};
    use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
    use pureflow_workflow::WorkflowDefinition;
    use serde_json::json;
    use std::collections::BTreeMap;

    use crate::{
        action::RuleAction,
        condition::{Condition, FieldPath, ScalarValue},
        rule::{EvaluationStrategy, Rule, RuleSet},
    };

    fn node_id(s: &str) -> NodeId {
        NodeId::new(s).unwrap()
    }
    fn port_id(s: &str) -> PortId {
        PortId::new(s).unwrap()
    }
    fn workflow_id(s: &str) -> WorkflowId {
        WorkflowId::new(s).unwrap()
    }
    fn field(s: &str) -> FieldPath {
        FieldPath::new(s).unwrap()
    }

    fn rule_packet(wf: &WorkflowId, payload: PacketPayload) -> PortPacket {
        let src = MessageEndpoint::new(node_id("source"), port_id("out"));
        let tgt = MessageEndpoint::new(node_id("router"), port_id("in"));
        let route = MessageRoute::new(Some(src), tgt);
        let meta = MessageMetadata::new(
            MessageId::new("m1").unwrap(),
            wf.clone(),
            ExecutionMetadata::first_attempt(ExecutionId::new("run-1").unwrap()),
            route,
        );
        PortPacket::new(meta, payload)
    }

    fn routing_workflow() -> WorkflowDefinition {
        WorkflowBuilder::new("rule-routing")
            .node(NodeBuilder::new("source").output("out").build())
            .node(
                NodeBuilder::new("router")
                    .input("in")
                    .output("high-out")
                    .output("std-out")
                    .build(),
            )
            .node(NodeBuilder::new("high-sink").input("in").build())
            .node(NodeBuilder::new("std-sink").input("in").build())
            .edge("source", "out", "router", "in")
            .edge("router", "high-out", "high-sink", "in")
            .edge("router", "std-out", "std-sink", "in")
            .build()
    }

    #[derive(Debug, Clone)]
    enum TestExecutor {
        Source {
            payload: serde_json::Value,
        },
        Router(Arc<RuleNode>),
        Sink {
            received: Arc<std::sync::Mutex<Vec<PacketPayload>>>,
        },
    }

    impl NodeExecutor for TestExecutor {
        type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

        fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
            match self {
                Self::Source { payload } => {
                    let payload = payload.clone();
                    Box::pin(async move {
                        let cancellation = ctx.cancellation_token();
                        let src = MessageEndpoint::new(ctx.node_id().clone(), port_id("out"));
                        let tgt = MessageEndpoint::new(ctx.node_id().clone(), port_id("out"));
                        let route = MessageRoute::new(Some(src), tgt);
                        let meta = MessageMetadata::new(
                            MessageId::new("src-1").unwrap(),
                            ctx.workflow_id().clone(),
                            ctx.execution().clone(),
                            route,
                        );
                        let packet = PortPacket::new(meta, PacketPayload::control(payload));
                        outputs
                            .send(&port_id("out"), packet, &cancellation)
                            .await
                            .map_err(|e| PureflowError::execution(e.to_string()))
                    })
                }
                Self::Router(node) => node.run(ctx, inputs, outputs),
                Self::Sink { received } => {
                    let received = received.clone();
                    Box::pin(async move {
                        let cancellation = ctx.cancellation_token();
                        let in_port = port_id("in");
                        let mut inputs = inputs;
                        loop {
                            match inputs.recv(&in_port, &cancellation).await {
                                Ok(Some(pkt)) => {
                                    received.lock().unwrap().push(pkt.into_payload());
                                }
                                // No upstream connection or upstream closed.
                                Ok(None) | Err(PortRecvError::Disconnected { .. }) => break,
                                Err(e) => {
                                    return Err(PureflowError::execution(e.to_string()));
                                }
                            }
                        }
                        Ok(())
                    })
                }
            }
        }
    }

    #[test]
    fn rule_node_routes_high_value_to_correct_port() {
        let rules = vec![
            Rule::new(
                "high",
                Condition::FieldGte {
                    path: field("amount"),
                    value: ScalarValue::Integer(10000),
                },
                RuleAction::Route(port_id("high-out")),
                10,
                "high value",
            )
            .unwrap(),
            Rule::new(
                "std",
                Condition::Always,
                RuleAction::Route(port_id("std-out")),
                20,
                "standard",
            )
            .unwrap(),
        ];
        let rule_set = Arc::new(
            RuleSet::new(
                "router",
                EvaluationStrategy::FirstMatch,
                rules,
                RuleAction::Drop,
                false,
            )
            .unwrap(),
        );
        let router = Arc::new(RuleNode::new(rule_set));
        let high_received: Arc<std::sync::Mutex<Vec<PacketPayload>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let std_received: Arc<std::sync::Mutex<Vec<PacketPayload>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let registry = StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("source"),
                TestExecutor::Source {
                    payload: json!({"amount": 50000}),
                },
            ),
            (node_id("router"), TestExecutor::Router(router)),
            (
                node_id("high-sink"),
                TestExecutor::Sink {
                    received: high_received.clone(),
                },
            ),
            (
                node_id("std-sink"),
                TestExecutor::Sink {
                    received: std_received.clone(),
                },
            ),
        ]));
        let workflow = routing_workflow();
        let execution = ExecutionMetadata::first_attempt(ExecutionId::new("run-1").unwrap());
        let metadata_sink = Arc::new(JsonlMetadataSink::new(Vec::new()));

        let summary: WorkflowRunSummary =
            block_on(run_workflow_with_registry_and_metadata_sink_summary(
                &workflow,
                &execution,
                &registry,
                metadata_sink,
            ))
            .expect("workflow should complete");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(summary.failed_node_count(), 0);

        let high = high_received.lock().unwrap();
        let std = std_received.lock().unwrap();
        assert_eq!(high.len(), 1, "high-value packet should reach high-sink");
        assert_eq!(std.len(), 0, "high-value packet should not reach std-sink");
    }

    #[test]
    fn rule_node_drops_packet_silently() {
        let rules =
            vec![Rule::new("drop-all", Condition::Always, RuleAction::Drop, 10, "").unwrap()];
        let rule_set = Arc::new(
            RuleSet::new(
                "dropper",
                EvaluationStrategy::FirstMatch,
                rules,
                RuleAction::Drop,
                false,
            )
            .unwrap(),
        );
        let router = Arc::new(RuleNode::new(rule_set));
        let high_received: Arc<std::sync::Mutex<Vec<PacketPayload>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let std_received: Arc<std::sync::Mutex<Vec<PacketPayload>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let registry = StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("source"),
                TestExecutor::Source {
                    payload: json!({"amount": 100}),
                },
            ),
            (node_id("router"), TestExecutor::Router(router)),
            (
                node_id("high-sink"),
                TestExecutor::Sink {
                    received: high_received.clone(),
                },
            ),
            (
                node_id("std-sink"),
                TestExecutor::Sink {
                    received: std_received.clone(),
                },
            ),
        ]));
        let workflow = routing_workflow();
        let execution = ExecutionMetadata::first_attempt(ExecutionId::new("run-2").unwrap());
        let metadata_sink = Arc::new(JsonlMetadataSink::new(Vec::new()));

        let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow,
            &execution,
            &registry,
            metadata_sink,
        ))
        .expect("workflow should complete");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(high_received.lock().unwrap().len(), 0);
        assert_eq!(std_received.lock().unwrap().len(), 0);
    }

    #[test]
    fn rule_node_contract_id_is_stable() {
        assert_eq!(CONTRACT_ID, "pureflow.rules.v1");
    }

    #[test]
    fn rule_node_emits_rule_eval_record_to_metadata_sink() {
        let rules = vec![
            Rule::new(
                "high",
                Condition::FieldGte {
                    path: field("amount"),
                    value: ScalarValue::Integer(10000),
                },
                RuleAction::Route(port_id("high-out")),
                10,
                "high value",
            )
            .unwrap(),
            Rule::new(
                "std",
                Condition::Always,
                RuleAction::Route(port_id("std-out")),
                20,
                "standard",
            )
            .unwrap(),
        ];
        let rule_set = Arc::new(
            RuleSet::new(
                "payment-router",
                EvaluationStrategy::FirstMatch,
                rules,
                RuleAction::Drop,
                false,
            )
            .unwrap(),
        );

        let metadata_sink = Arc::new(JsonlMetadataSink::new(Vec::new()));
        let router = Arc::new(RuleNode::new(rule_set).with_metadata_sink(metadata_sink.clone()));

        let high_received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let std_received = Arc::new(std::sync::Mutex::new(Vec::new()));

        let registry = StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("source"),
                TestExecutor::Source {
                    payload: json!({"amount": 50000}),
                },
            ),
            (node_id("router"), TestExecutor::Router(router)),
            (
                node_id("high-sink"),
                TestExecutor::Sink {
                    received: high_received.clone(),
                },
            ),
            (
                node_id("std-sink"),
                TestExecutor::Sink {
                    received: std_received.clone(),
                },
            ),
        ]));
        let workflow = routing_workflow();
        let execution = ExecutionMetadata::first_attempt(ExecutionId::new("run-emit-1").unwrap());

        let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow,
            &execution,
            &registry,
            metadata_sink.clone(),
        ))
        .expect("workflow should complete");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);

        // Drop the registry (and the router it contains) so the Arc has exactly one owner.
        drop(registry);

        // Read the JSONL output and check for a rule_eval record.
        let bytes = Arc::try_unwrap(metadata_sink)
            .unwrap()
            .into_inner()
            .unwrap();
        let jsonl = String::from_utf8(bytes).unwrap();
        let rule_eval_lines: Vec<&str> = jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"rule_eval\""))
            .collect();

        assert_eq!(
            rule_eval_lines.len(),
            1,
            "exactly one RuleEval record expected"
        );

        let record: serde_json::Value = serde_json::from_str(rule_eval_lines[0]).unwrap();
        assert_eq!(record["rule_set_id"], "payment-router");
        assert_eq!(record["strategy"], "first_match");
        assert_eq!(record["matched_rule"], "high");
        assert_eq!(record["action_taken"]["kind"], "route");
        assert_eq!(record["action_taken"]["port"], "high-out");
        assert_eq!(record["rules_evaluated"], 2);
    }

    #[test]
    fn rule_node_without_sink_emits_no_records() {
        // No sink attached — workflow still completes, no metadata records from rules.
        let rules = vec![
            Rule::new(
                "r",
                Condition::Always,
                RuleAction::Route(port_id("std-out")),
                10,
                "",
            )
            .unwrap(),
        ];
        let rule_set = Arc::new(
            RuleSet::new(
                "rs",
                EvaluationStrategy::FirstMatch,
                rules,
                RuleAction::Drop,
                false,
            )
            .unwrap(),
        );
        let router = Arc::new(RuleNode::new(rule_set));
        let high_received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let std_received = Arc::new(std::sync::Mutex::new(Vec::new()));

        let registry = StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("source"),
                TestExecutor::Source { payload: json!({}) },
            ),
            (node_id("router"), TestExecutor::Router(router)),
            (
                node_id("high-sink"),
                TestExecutor::Sink {
                    received: high_received.clone(),
                },
            ),
            (
                node_id("std-sink"),
                TestExecutor::Sink {
                    received: std_received.clone(),
                },
            ),
        ]));
        let workflow = routing_workflow();
        let execution = ExecutionMetadata::first_attempt(ExecutionId::new("run-no-sink").unwrap());
        let metadata_sink = Arc::new(JsonlMetadataSink::new(Vec::new()));

        let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow,
            &execution,
            &registry,
            metadata_sink.clone(),
        ))
        .expect("workflow should complete");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        drop(registry);
        let bytes = Arc::try_unwrap(metadata_sink)
            .unwrap()
            .into_inner()
            .unwrap();
        let jsonl = String::from_utf8(bytes).unwrap();
        let rule_eval_count = jsonl
            .lines()
            .filter(|l| l.contains("\"record_type\":\"rule_eval\""))
            .count();
        assert_eq!(
            rule_eval_count, 0,
            "no RuleEval records expected without sink"
        );
    }
}
