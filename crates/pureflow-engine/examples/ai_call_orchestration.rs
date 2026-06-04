//! Runnable AI-call orchestration mock workload.
//!
//! Models one prompt → LLM → tool-call → tool-result → final-response turn as
//! a deterministic linear graph with no real network calls. Packets carry
//! structured byte messages (colon-delimited type:field fields) that stand in
//! for a real LLM protocol.
//!
//! The topology is intentionally acyclic (one turn). The doc section at the
//! bottom of `ai-call-orchestration.md` records what a multi-turn feedback-loop
//! execution policy and an external-effect capability would need to add.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{Arc, Mutex},
};

use pureflow_core::{
    PureflowError, JsonlMetadataSink, NodeExecutor, PacketPayload, PortPacket, PortsIn, PortsOut,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::{
    StaticNodeExecutorRegistry, run_workflow_with_registry_and_metadata_sink_summary,
};
use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, drain_port};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId};
use pureflow_workflow::WorkflowDefinition;
use futures::{executor::block_on, future::BoxFuture};

// Deterministic scenario: a single prompt that triggers one tool call.
const PROMPT: &str = "prompt:what is the weather in sf?";

// Deterministic LLM mock response: decides to call the weather tool.
const TOOL_CALL: &str = "tool_call:get_weather:SF";

// Deterministic tool mock response.
const TOOL_RESULT: &str = "tool_result:get_weather:72F:sunny";

// Deterministic final response assembled from tool result.
const FINAL_RESPONSE: &str = "response:The weather in SF is 72F and sunny.";

#[derive(Debug, Clone)]
enum AiOrchExecutor {
    Prompter,
    LlmMock,
    ToolExecutor,
    Finalizer,
    Collector { received: Arc<Mutex<Vec<String>>> },
}

impl NodeExecutor for AiOrchExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self {
                Self::Prompter => {
                    outputs
                        .send(
                            &port_id("prompt")?,
                            packet(&ctx, "prompt", PROMPT.as_bytes(), 0)?,
                            &cancellation,
                        )
                        .await?;
                }
                Self::LlmMock => {
                    let packets =
                        drain_port(&mut inputs, &port_id("prompt")?, &cancellation).await?;
                    for (index, received) in packets.into_iter().enumerate() {
                        let prompt = packet_payload_string(received)?;
                        // Mock: any prompt triggers a tool call to get_weather.
                        let tool_call = mock_llm_decision(&prompt)?;
                        outputs
                            .send(
                                &port_id("tool-call")?,
                                packet(&ctx, "tool-call", tool_call.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::ToolExecutor => {
                    let packets = drain_port(&mut inputs, &port_id("call")?, &cancellation).await?;
                    for (index, received) in packets.into_iter().enumerate() {
                        let call = packet_payload_string(received)?;
                        let result = mock_tool_execute(&call)?;
                        outputs
                            .send(
                                &port_id("result")?,
                                packet(&ctx, "result", result.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::Finalizer => {
                    let packets =
                        drain_port(&mut inputs, &port_id("context")?, &cancellation).await?;
                    for (index, received) in packets.into_iter().enumerate() {
                        let context = packet_payload_string(received)?;
                        let response = mock_llm_finalize(&context)?;
                        outputs
                            .send(
                                &port_id("response")?,
                                packet(&ctx, "response", response.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::Collector { received } => {
                    let packets =
                        drain_port(&mut inputs, &port_id("response")?, &cancellation).await?;
                    let rows: Vec<String> = packets
                        .into_iter()
                        .map(packet_payload_string)
                        .collect::<pureflow_core::Result<_>>()?;
                    received
                        .lock()
                        .expect("collector received lock should not be poisoned")
                        .extend(rows);
                }
            }

            Ok(())
        })
    }
}

fn mock_llm_decision(prompt: &str) -> pureflow_core::Result<String> {
    if !prompt.starts_with("prompt:") {
        return Err(PureflowError::execution(format!(
            "llm-mock expected prompt: prefix, got `{prompt}`"
        )));
    }
    Ok(TOOL_CALL.to_owned())
}

fn mock_tool_execute(call: &str) -> pureflow_core::Result<String> {
    if !call.starts_with("tool_call:") {
        return Err(PureflowError::execution(format!(
            "tool-executor expected tool_call: prefix, got `{call}`"
        )));
    }
    Ok(TOOL_RESULT.to_owned())
}

fn mock_llm_finalize(context: &str) -> pureflow_core::Result<String> {
    if !context.starts_with("tool_result:") {
        return Err(PureflowError::execution(format!(
            "finalizer expected tool_result: prefix, got `{context}`"
        )));
    }
    Ok(FINAL_RESPONSE.to_owned())
}

fn main() -> Result<(), Box<dyn Error>> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("ai-call-orchestration-example")?);
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let registry: StaticNodeExecutorRegistry<AiOrchExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("prompter")?, AiOrchExecutor::Prompter),
            (node_id("llm-mock")?, AiOrchExecutor::LlmMock),
            (node_id("tool-executor")?, AiOrchExecutor::ToolExecutor),
            (node_id("finalizer")?, AiOrchExecutor::Finalizer),
            (
                node_id("collector")?,
                AiOrchExecutor::Collector {
                    received: received.clone(),
                },
            ),
        ]));
    let metadata_sink: Arc<JsonlMetadataSink<Vec<u8>>> =
        Arc::new(JsonlMetadataSink::new(Vec::new()));

    let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
        &workflow,
        &execution,
        &registry,
        metadata_sink.clone(),
    ))?;
    metadata_sink.flush()?;

    let responses: Vec<String> = received
        .lock()
        .expect("collector received lock should not be poisoned")
        .clone();
    let metadata_jsonl: String = metadata_jsonl_from_sink(metadata_sink)?;
    let metadata_counts: MetadataCounts = count_metadata_records(&metadata_jsonl);

    assert_expected_output(&responses, metadata_counts)?;

    println!("ai orchestration workflow `{}` completed", workflow.id());
    println!("prompt: {PROMPT}");
    println!("tool call: {TOOL_CALL}");
    println!("tool result: {TOOL_RESULT}");
    println!("final response: {}", responses[0]);
    println!("scheduled nodes: {}", summary.scheduled_node_count());
    println!("completed nodes: {}", summary.completed_node_count());
    println!("metadata records: {}", metadata_counts.total);
    println!("metadata lifecycle records: {}", metadata_counts.lifecycle);
    println!("metadata message records: {}", metadata_counts.message);
    println!(
        "metadata queue_pressure records: {}",
        metadata_counts.queue_pressure
    );

    summary.into_result()?;
    Ok(())
}

fn assert_expected_output(
    responses: &[String],
    metadata_counts: MetadataCounts,
) -> pureflow_core::Result<()> {
    if responses != [FINAL_RESPONSE] {
        return Err(PureflowError::execution(format!(
            "collector did not receive expected final response: got {responses:?}"
        )));
    }
    if metadata_counts.lifecycle == 0
        || metadata_counts.message == 0
        || metadata_counts.queue_pressure == 0
    {
        return Err(PureflowError::metadata(format!(
            "metadata shape was incomplete: {metadata_counts:?}"
        )));
    }
    Ok(())
}

fn workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("ai-call-orchestration-workload")
        .node(NodeBuilder::new("prompter").output("prompt").build())
        .node(
            NodeBuilder::new("llm-mock")
                .input("prompt")
                .output("tool-call")
                .build(),
        )
        .node(
            NodeBuilder::new("tool-executor")
                .input("call")
                .output("result")
                .build(),
        )
        .node(
            NodeBuilder::new("finalizer")
                .input("context")
                .output("response")
                .build(),
        )
        .node(NodeBuilder::new("collector").input("response").build())
        .edge_with_capacity("prompter", "prompt", "llm-mock", "prompt", capacity(4))
        .edge_with_capacity(
            "llm-mock",
            "tool-call",
            "tool-executor",
            "call",
            capacity(4),
        )
        .edge_with_capacity(
            "tool-executor",
            "result",
            "finalizer",
            "context",
            capacity(4),
        )
        .edge_with_capacity(
            "finalizer",
            "response",
            "collector",
            "response",
            capacity(4),
        )
        .build()
}

fn packet(
    ctx: &NodeContext,
    output_port: &str,
    payload: &[u8],
    index: usize,
) -> pureflow_core::Result<PortPacket> {
    let source = MessageEndpoint::new(ctx.node_id().clone(), port_id(output_port)?);
    let target = MessageEndpoint::new(ctx.node_id().clone(), port_id(output_port)?);
    let route = MessageRoute::new(Some(source), target);
    let message_id = MessageId::new(format!("{}-{output_port}-{index}", ctx.node_id()))?;
    let metadata = MessageMetadata::new(
        message_id,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );
    Ok(PortPacket::new(
        metadata,
        PacketPayload::from(payload.to_vec()),
    ))
}

fn packet_payload_string(pkt: PortPacket) -> pureflow_core::Result<String> {
    let bytes = pkt
        .into_payload()
        .as_bytes()
        .map(|b| b.to_vec())
        .ok_or_else(|| {
            PureflowError::execution("ai-call-orchestration workload expected byte payload")
        })?;
    String::from_utf8(bytes)
        .map_err(|e| PureflowError::execution(format!("payload was not UTF-8: {e}")))
}

fn metadata_jsonl_from_sink(
    metadata_sink: Arc<JsonlMetadataSink<Vec<u8>>>,
) -> pureflow_core::Result<String> {
    let sink = match Arc::try_unwrap(metadata_sink) {
        Ok(sink) => sink,
        Err(_arc) => {
            return Err(PureflowError::metadata(
                "metadata sink still had multiple references after run",
            ));
        }
    };
    let bytes = sink.into_inner()?;
    String::from_utf8(bytes)
        .map_err(|e| PureflowError::metadata(format!("metadata JSONL was not UTF-8: {e}")))
}

#[derive(Debug, Clone, Copy)]
struct MetadataCounts {
    total: usize,
    lifecycle: usize,
    message: usize,
    queue_pressure: usize,
}

fn count_metadata_records(metadata_jsonl: &str) -> MetadataCounts {
    MetadataCounts {
        total: metadata_jsonl.lines().count(),
        lifecycle: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"lifecycle\""))
            .count(),
        message: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"message\""))
            .count(),
        queue_pressure: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"queue_pressure\""))
            .count(),
    }
}

const fn capacity(value: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(value).expect("example capacity must be non-zero")
}

fn node_id(value: &str) -> Result<NodeId, pureflow_types::IdentifierError> {
    NodeId::new(value)
}

fn port_id(value: &str) -> Result<PortId, pureflow_types::IdentifierError> {
    PortId::new(value)
}
