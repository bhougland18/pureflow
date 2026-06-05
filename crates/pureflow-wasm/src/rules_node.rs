//! WASM-backed `RuleNode` executor.
//!
//! [`WasmRuleNode`] is the WebAssembly counterpart to
//! `pureflow_rules::RuleNode`: it receives packets on its input port, evaluates
//! a [`RuleSet`] against each one *inside a sandboxed WASM guest* via
//! [`WasmtimeRuleComponent`], and routes the packet according to the returned
//! decision. The host owns all packet movement — every routed packet leaves
//! through `PortsOut::send`, whose reserve/commit pair provides the
//! backpressure-aware send the rules engine relies on.
//!
//! Because the guest evaluates the same declarative rule set with the same
//! semantics as the native evaluator, a `WasmRuleNode` and a native `RuleNode`
//! produce equivalent routing outcomes for the same rule set.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use pureflow_core::{
    CancellationToken, MetadataRecord, MetadataSink, NodeExecutor, PortPacket, PortSendError,
    PortsIn, PortsOut, PureflowError, Result, RuleEvalAction, RuleEvalRecord, RuleEvalStrategy,
    context::NodeContext,
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
    ports::PortRecvError,
};
use pureflow_rules::{
    RuleAction, RuleSet, ScalarValue,
    rule::{EvaluationStrategy, RuleDecision},
};
use pureflow_types::{MessageId, PortId};

use crate::rules::{HostEvalContext, WasmtimeRuleComponent};

/// Built-in port name for the rule node's single input.
const IN_PORT: &str = "in";

/// A first-class Pureflow node that evaluates a [`RuleSet`] inside a WASM guest
/// and routes each incoming packet according to the returned decision.
///
/// Construct it from an `Arc<RuleSet>` (the inspectable rule data) and an
/// `Arc<WasmtimeRuleComponent>` (the compiled guest that evaluates it). Sharing
/// both behind `Arc` makes fan-out to N parallel nodes cheap.
///
/// ## Cancel semantics
///
/// Cancellation is observed between packets and while waiting for input, and is
/// propagated into the guest call so a long evaluation is interrupted. A packet
/// already received is fully processed before the node returns — the same drain
/// guarantee as the native node.
pub struct WasmRuleNode {
    rule_set: Arc<RuleSet>,
    component: Arc<WasmtimeRuleComponent>,
    metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
    packet_counter: AtomicU64,
}

impl WasmRuleNode {
    /// Create a WASM rule node that evaluates `rule_set` via `component`.
    #[must_use]
    pub fn new(rule_set: Arc<RuleSet>, component: Arc<WasmtimeRuleComponent>) -> Self {
        Self {
            rule_set,
            component,
            metadata_sink: None,
            packet_counter: AtomicU64::new(0),
        }
    }

    /// Attach a metadata sink so the node emits [`RuleEvalRecord`] entries.
    ///
    /// Records are emitted after evaluation and before the routing send, so the
    /// record is always written even if the send is later cancelled.
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

impl std::fmt::Debug for WasmRuleNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmRuleNode")
            .field("rule_set_id", &self.rule_set.id)
            .field("strategy", &self.rule_set.strategy)
            .field("rules", &self.rule_set.rules.len())
            .field("has_metadata_sink", &self.metadata_sink.is_some())
            .finish()
    }
}

impl NodeExecutor for WasmRuleNode {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            let in_port = PortId::new(IN_PORT)
                .map_err(|e| PureflowError::execution(format!("invalid port id: {e}")))?;

            loop {
                let packet: PortPacket = match inputs.recv(&in_port, &cancellation).await {
                    Ok(Some(p)) => p,
                    Ok(None) => break,
                    Err(PortRecvError::Disconnected { .. }) => break,
                    Err(PortRecvError::UnknownPort { port_id }) => {
                        return Err(PureflowError::execution(format!(
                            "wasm rule node: input port `{port_id}` not declared"
                        )));
                    }
                    Err(PortRecvError::Cancelled { .. }) => {
                        return Err(PureflowError::cancelled(
                            "wasm rule node cancelled while waiting for input",
                        ));
                    }
                };

                // --- Drain phase: finish this packet regardless of cancellation ---

                let host_ctx = build_host_context(&ctx, &packet);
                let decision: RuleDecision = self
                    .component
                    .evaluate_with_cancellation(&self.rule_set, &host_ctx, &cancellation)
                    .map_err(|e| {
                        PureflowError::execution(format!("wasm rule evaluation error: {e}"))
                    })?;

                // Emit the RuleEvalRecord BEFORE the send so the record survives
                // a later cancelled send (finalize guarantee).
                if let Some(sink) = &self.metadata_sink {
                    let record = build_rule_eval_record(&ctx, &self.rule_set, &packet, &decision);
                    let _ = sink.record(&MetadataRecord::RuleEval(record));
                }

                let seq = self.packet_counter.fetch_add(1, Ordering::Relaxed);
                execute_action(&decision.action, &ctx, seq, packet, &outputs, &cancellation)
                    .await?;

                // --- Drain phase complete ---

                if cancellation.is_cancelled() {
                    return Err(PureflowError::cancelled(
                        "wasm rule node cancelled after processing packet",
                    ));
                }
            }

            Ok(())
        })
    }
}

/// Build an owned evaluation context from packet routing metadata.
///
/// Mirrors the native node: tags and execution metadata start empty (they are
/// populated by upstream `Tag` actions and the runner respectively), and the
/// provenance surfaces come from the packet's route.
fn build_host_context(ctx: &NodeContext, packet: &PortPacket) -> HostEvalContext {
    let source_node = packet
        .metadata()
        .route()
        .source()
        .map(|ep: &MessageEndpoint| ep.node_id().to_string());
    let arrival_port = Some(packet.metadata().route().target().port_id().to_string());

    HostEvalContext {
        payload: packet.payload().clone(),
        tags: BTreeMap::new(),
        source_node,
        arrival_port,
        hop_count: 0,
        workflow_id: ctx.workflow_id().to_string(),
        execution_metadata: BTreeMap::new(),
    }
}

/// Execute one routing action, consuming the packet. Mirrors the native node.
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
            // Packet consumed and discarded — no send needed.
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
            // Non-terminal: the evaluator folds tags into the decision and never
            // returns Tag as a terminal action.
        }
        RuleAction::Halt(message) => {
            return Err(PureflowError::execution(format!("rule halted: {message}")));
        }
    }
    Ok(())
}

/// Forward an incoming packet to a new output port, updating routing metadata.
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

fn build_rule_eval_record(
    ctx: &NodeContext,
    rule_set: &RuleSet,
    packet: &PortPacket,
    decision: &RuleDecision,
) -> RuleEvalRecord {
    let source_node = packet
        .metadata()
        .route()
        .source()
        .map(|ep: &MessageEndpoint| ep.node_id().clone());
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
            .map(|(k, v): &(String, ScalarValue)| (k.clone(), scalar_to_json(v)))
            .collect(),
        source_node,
        arrival_port,
        hop_count: 0,
        tags_present_at_eval: Vec::new(),
        conditions_evaluated: decision.conditions_evaluated.clone(),
    }
}

fn strategy_to_eval(strategy: EvaluationStrategy) -> RuleEvalStrategy {
    match strategy {
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
        RuleAction::Halt(message) => RuleEvalAction::Halt(message.clone()),
    }
}

fn scalar_to_json(value: &ScalarValue) -> serde_json::Value {
    match value {
        ScalarValue::String(s) => serde_json::Value::String(s.clone()),
        ScalarValue::Integer(i) => serde_json::json!(*i),
        ScalarValue::Float(f) => serde_json::json!(*f),
        ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
        ScalarValue::Null => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env,
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        process::{Command, Output},
        sync::Mutex,
    };

    use futures::executor::block_on;
    use pureflow_core::{
        JsonlMetadataSink, PacketPayload,
        context::ExecutionMetadata,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use pureflow_engine::{
        StaticNodeExecutorRegistry, WorkflowTerminalState,
        run_workflow_with_registry_and_metadata_sink_summary,
    };
    use pureflow_rules::{
        Condition, RuleAction, RuleSet,
        condition::{FieldPath, ScalarValue},
        rule::{EvaluationStrategy, Rule},
    };
    use pureflow_test_kit::{NodeBuilder, WorkflowBuilder};
    use pureflow_types::{ExecutionId, MessageId, NodeId, PortId};
    use pureflow_workflow::WorkflowDefinition;
    use serde_json::json;

    use crate::rules::WasmtimeRuleComponent;

    const RULES_FIXTURE_MANIFEST: &str = "fixtures/rules-guest/Cargo.toml";
    const RULES_FIXTURE_ARTIFACT: &str =
        "wasm32-wasip2/release/pureflow_wasm_rules_guest_fixture.wasm";

    fn node_id(s: &str) -> NodeId {
        NodeId::new(s).expect("node id")
    }
    fn port_id(s: &str) -> PortId {
        PortId::new(s).expect("port id")
    }
    fn field(s: &str) -> FieldPath {
        FieldPath::new(s).expect("field path")
    }

    fn routing_workflow() -> WorkflowDefinition {
        WorkflowBuilder::new("wasm-rule-routing")
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

    enum TestExecutor {
        Source {
            payload: serde_json::Value,
        },
        Router(Arc<WasmRuleNode>),
        Sink {
            received: Arc<Mutex<Vec<PacketPayload>>>,
        },
    }

    impl NodeExecutor for TestExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

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
                            MessageId::new("src-1").expect("message id"),
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
                                    received.lock().expect("lock").push(pkt.into_payload());
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

    fn build_rules_guest_fixture() -> PathBuf {
        let crate_dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let manifest_path: PathBuf = crate_dir.join(RULES_FIXTURE_MANIFEST);
        let target_dir: PathBuf = env::temp_dir().join(format!(
            "pureflow-wasm-rules-node-fixture-{}",
            std::process::id()
        ));
        let artifact_path: PathBuf = target_dir.join(RULES_FIXTURE_ARTIFACT);
        let cargo: OsString = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
        let output: Output = Command::new(cargo)
            .args([
                "build",
                "--manifest-path",
                manifest_path.to_str().expect("manifest path utf-8"),
                "--target",
                "wasm32-wasip2",
                "--release",
                "--target-dir",
                target_dir.to_str().expect("target dir utf-8"),
            ])
            .env_remove("RUSTFLAGS")
            .output()
            .expect("fixture build runs");
        assert!(
            output.status.success(),
            "rules fixture build failed\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );
        artifact_path
    }

    fn wasm32_wasip2_target_available() -> bool {
        let rustc: OsString = env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc"));
        let Ok(output) = Command::new(rustc)
            .args(["--print", "target-libdir", "--target", "wasm32-wasip2"])
            .env_remove("RUSTFLAGS")
            .output()
        else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        let libdir: PathBuf = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        fs::read_dir(libdir).is_ok_and(|entries| {
            entries.filter_map(std::result::Result::ok).any(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with("libcore-")
                        && Path::new(name)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("rlib"))
                })
            })
        })
    }

    fn router_rule_set() -> Arc<RuleSet> {
        let rules = vec![
            Rule::new(
                "high",
                Condition::FieldGte {
                    path: field("amount"),
                    value: ScalarValue::Integer(10_000),
                },
                RuleAction::Route(port_id("high-out")),
                10,
                "high value",
            )
            .expect("rule"),
            Rule::new(
                "std",
                Condition::Always,
                RuleAction::Route(port_id("std-out")),
                20,
                "standard",
            )
            .expect("rule"),
        ];
        Arc::new(
            RuleSet::new(
                "payment-router",
                EvaluationStrategy::FirstMatch,
                rules,
                RuleAction::Drop,
                false,
            )
            .expect("rule set"),
        )
    }

    /// Run the routing workflow with a `WasmRuleNode` router and return the
    /// (high-sink, std-sink) packet counts.
    fn run_routing(
        component: Arc<WasmtimeRuleComponent>,
        rule_set: Arc<RuleSet>,
        payload: serde_json::Value,
        run_id: &str,
    ) -> (usize, usize) {
        let router = Arc::new(WasmRuleNode::new(rule_set, component));
        let high: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));
        let std: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));

        let registry = StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("source"), TestExecutor::Source { payload }),
            (node_id("router"), TestExecutor::Router(router)),
            (
                node_id("high-sink"),
                TestExecutor::Sink {
                    received: high.clone(),
                },
            ),
            (
                node_id("std-sink"),
                TestExecutor::Sink {
                    received: std.clone(),
                },
            ),
        ]));
        let workflow = routing_workflow();
        let execution =
            ExecutionMetadata::first_attempt(ExecutionId::new(run_id).expect("execution id"));
        let sink = Arc::new(JsonlMetadataSink::new(Vec::new()));
        let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow, &execution, &registry, sink,
        ))
        .expect("workflow completes");
        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);

        let high_count = high.lock().expect("lock").len();
        let std_count = std.lock().expect("lock").len();
        (high_count, std_count)
    }

    #[test]
    fn wasm_rule_node_routes_through_reserve_commit_sends() {
        if !wasm32_wasip2_target_available() {
            eprintln!("skipping WASM rule node test; no wasm32-wasip2 target");
            return;
        }
        let bytes = fs::read(build_rules_guest_fixture()).expect("fixture readable");
        let component =
            Arc::new(WasmtimeRuleComponent::from_component_bytes(bytes).expect("fixture compiles"));

        // High-value packet routes to high-out, matching a native RuleNode.
        assert_eq!(
            run_routing(
                component.clone(),
                router_rule_set(),
                json!({"amount": 50_000}),
                "run-high",
            ),
            (1, 0),
        );

        // Below threshold falls through to the std-out default path.
        assert_eq!(
            run_routing(
                component,
                router_rule_set(),
                json!({"amount": 100}),
                "run-std",
            ),
            (0, 1),
        );
    }

    #[test]
    fn wasm_rule_node_emits_rule_eval_record() {
        if !wasm32_wasip2_target_available() {
            eprintln!("skipping WASM rule node metadata test; no wasm32-wasip2 target");
            return;
        }
        let bytes = fs::read(build_rules_guest_fixture()).expect("fixture readable");
        let component =
            Arc::new(WasmtimeRuleComponent::from_component_bytes(bytes).expect("fixture compiles"));

        let metadata_sink = Arc::new(JsonlMetadataSink::new(Vec::new()));
        let router = Arc::new(
            WasmRuleNode::new(router_rule_set(), component)
                .with_metadata_sink(metadata_sink.clone()),
        );
        let high: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));
        let std: Arc<Mutex<Vec<PacketPayload>>> = Arc::new(Mutex::new(Vec::new()));
        let registry = StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("source"),
                TestExecutor::Source {
                    payload: json!({"amount": 50_000}),
                },
            ),
            (node_id("router"), TestExecutor::Router(router)),
            (
                node_id("high-sink"),
                TestExecutor::Sink {
                    received: high.clone(),
                },
            ),
            (
                node_id("std-sink"),
                TestExecutor::Sink {
                    received: std.clone(),
                },
            ),
        ]));
        let workflow = routing_workflow();
        let execution =
            ExecutionMetadata::first_attempt(ExecutionId::new("run-emit").expect("execution id"));
        let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow,
            &execution,
            &registry,
            metadata_sink.clone(),
        ))
        .expect("workflow completes");
        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        drop(registry);

        let bytes = Arc::try_unwrap(metadata_sink)
            .expect("sole owner")
            .into_inner()
            .expect("lock");
        let jsonl = String::from_utf8(bytes).expect("utf-8");
        let rule_eval_lines: Vec<&str> = jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"rule_eval\""))
            .collect();
        assert_eq!(
            rule_eval_lines.len(),
            1,
            "exactly one RuleEval record expected"
        );
        let record: serde_json::Value =
            serde_json::from_str(rule_eval_lines[0]).expect("record is json");
        assert_eq!(record["rule_set_id"], "payment-router");
        assert_eq!(record["matched_rule"], "high");
        assert_eq!(record["action_taken"]["kind"], "route");
        assert_eq!(record["action_taken"]["port"], "high-out");
    }
}
