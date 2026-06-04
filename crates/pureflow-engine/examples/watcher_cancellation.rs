//! Runnable watcher workload where cancellation is the expected terminal path.

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
    StaticNodeExecutorRegistry, WorkflowTerminalState,
    run_workflow_with_registry_and_metadata_sink_summary,
};
use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, drain_port};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId};
use pureflow_workflow::WorkflowDefinition;
use futures::{executor::block_on, future::BoxFuture};

const CHANGE_PAYLOADS: &[&str] = &[
    "change:config.toml",
    "change:routes.yaml",
    "change:secrets.env",
    "change:templates/email.txt",
];
const SHUTDOWN_PAYLOAD: &str = "shutdown:source-closed";
const WATCHER_CANCEL_REASON: &str = "watcher received planned shutdown";

#[derive(Debug, Default, Clone)]
struct WatcherDiagnostics {
    observed_changes: Vec<String>,
    control_messages: Vec<String>,
    recv_any_order: Vec<String>,
}

#[derive(Debug, Clone)]
enum WatcherExecutor {
    ChangeSource,
    ShutdownController {
        drained_changes: Arc<Mutex<Vec<String>>>,
    },
    Watcher {
        diagnostics: Arc<Mutex<WatcherDiagnostics>>,
    },
}

impl NodeExecutor for WatcherExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self {
                Self::ChangeSource => {
                    for (index, payload) in CHANGE_PAYLOADS.iter().enumerate() {
                        outputs
                            .send(
                                &port_id("changes")?,
                                packet(&ctx, "changes", payload.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                    Ok(())
                }
                Self::ShutdownController { drained_changes } => {
                    let packets: Vec<PortPacket> =
                        drain_port(&mut inputs, &port_id("changes")?, &cancellation).await?;
                    let changes: Vec<String> = packets
                        .into_iter()
                        .map(packet_payload_string)
                        .collect::<pureflow_core::Result<Vec<String>>>()?;
                    let change_count: usize = changes.len();
                    *drained_changes
                        .lock()
                        .expect("controller changes lock should not be poisoned") = changes;
                    let shutdown_payload: String =
                        format!("{SHUTDOWN_PAYLOAD}:changes={change_count}");
                    outputs
                        .send(
                            &port_id("control")?,
                            packet(&ctx, "control", shutdown_payload.as_bytes(), 0)?,
                            &cancellation,
                        )
                        .await?;
                    Ok(())
                }
                Self::Watcher { diagnostics } => {
                    while let Some((input_port, received_packet)) =
                        inputs.recv_any(&cancellation).await?
                    {
                        let payload: String = packet_payload_string(received_packet)?;
                        let mut cancellation_requested: bool = false;
                        let mut unexpected_input: Option<String> = None;
                        {
                            let mut diagnostics = diagnostics
                                .lock()
                                .expect("watcher diagnostics lock should not be poisoned");
                            diagnostics
                                .recv_any_order
                                .push(format!("{input_port}:{payload}"));
                            match input_port.as_str() {
                                "changes" => diagnostics.observed_changes.push(payload),
                                "control" => {
                                    diagnostics.control_messages.push(payload);
                                    cancellation_requested = true;
                                }
                                other => unexpected_input = Some(other.to_owned()),
                            }
                            drop(diagnostics);
                        }
                        if let Some(other) = unexpected_input {
                            return Err(PureflowError::execution(format!(
                                "watcher received packet on unexpected input `{other}`"
                            )));
                        }
                        if cancellation_requested {
                            return Err(PureflowError::cancelled(WATCHER_CANCEL_REASON));
                        }
                    }

                    Err(PureflowError::execution(
                        "watcher input closed before shutdown control message",
                    ))
                }
            }
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("watcher-cancellation-example")?);
    let diagnostics: Arc<Mutex<WatcherDiagnostics>> =
        Arc::new(Mutex::new(WatcherDiagnostics::default()));
    let drained_changes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let registry: StaticNodeExecutorRegistry<WatcherExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("change-source")?, WatcherExecutor::ChangeSource),
            (
                node_id("shutdown-controller")?,
                WatcherExecutor::ShutdownController {
                    drained_changes: drained_changes.clone(),
                },
            ),
            (
                node_id("watcher")?,
                WatcherExecutor::Watcher {
                    diagnostics: diagnostics.clone(),
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

    let diagnostics: WatcherDiagnostics = diagnostics
        .lock()
        .expect("watcher diagnostics lock should not be poisoned")
        .clone();
    let drained_changes: Vec<String> = drained_changes
        .lock()
        .expect("controller changes lock should not be poisoned")
        .clone();
    let metadata_jsonl: String = metadata_jsonl_from_sink(metadata_sink)?;
    let metadata_counts: MetadataCounts = count_metadata_records(&metadata_jsonl);

    assert_expected_output(&summary, &diagnostics, &drained_changes, metadata_counts)?;

    println!(
        "watcher cancellation workflow `{}` cancelled as expected",
        workflow.id()
    );
    println!("source changes: {}", CHANGE_PAYLOADS.len());
    println!("controller drained changes: {}", drained_changes.len());
    println!(
        "watcher observed changes: {}",
        diagnostics.observed_changes.len()
    );
    println!(
        "watcher control messages: {}",
        diagnostics.control_messages.join(", ")
    );
    println!(
        "watcher recv_any order: {}",
        diagnostics.recv_any_order.join(", ")
    );
    println!("terminal state: cancelled");
    println!("scheduled nodes: {}", summary.scheduled_node_count());
    println!("completed nodes: {}", summary.completed_node_count());
    println!("cancelled nodes: {}", summary.cancelled_node_count());
    println!("failed nodes: {}", summary.failed_node_count());
    println!("metadata records: {}", metadata_counts.total);
    println!("metadata lifecycle records: {}", metadata_counts.lifecycle);
    println!(
        "metadata node_cancelled records: {}",
        metadata_counts.node_cancelled
    );
    println!("metadata error records: {}", metadata_counts.error);
    println!("metadata message records: {}", metadata_counts.message);
    println!(
        "metadata queue_pressure records: {}",
        metadata_counts.queue_pressure
    );

    Ok(())
}

fn workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("watcher-cancellation-workload")
        .node(NodeBuilder::new("change-source").output("changes").build())
        .node(
            NodeBuilder::new("shutdown-controller")
                .input("changes")
                .output("control")
                .build(),
        )
        .node(
            NodeBuilder::new("watcher")
                .input("changes")
                .input("control")
                .build(),
        )
        .edge_with_capacity(
            "change-source",
            "changes",
            "watcher",
            "changes",
            capacity(1),
        )
        .edge_with_capacity(
            "change-source",
            "changes",
            "shutdown-controller",
            "changes",
            capacity(1),
        )
        .edge_with_capacity(
            "shutdown-controller",
            "control",
            "watcher",
            "control",
            capacity(1),
        )
        .build()
}

fn assert_expected_output(
    summary: &pureflow_engine::WorkflowRunSummary,
    diagnostics: &WatcherDiagnostics,
    drained_changes: &[String],
    metadata_counts: MetadataCounts,
) -> pureflow_core::Result<()> {
    let expected_changes: Vec<String> = CHANGE_PAYLOADS.iter().map(ToString::to_string).collect();
    if drained_changes != expected_changes {
        return Err(PureflowError::execution(format!(
            "shutdown controller did not drain all changes before shutdown: {drained_changes:?}"
        )));
    }
    if diagnostics.observed_changes != expected_changes {
        return Err(PureflowError::execution(format!(
            "watcher did not observe all changes before shutdown: {:?}",
            diagnostics.observed_changes
        )));
    }
    let expected_control: Vec<String> = vec![format!(
        "{SHUTDOWN_PAYLOAD}:changes={}",
        CHANGE_PAYLOADS.len()
    )];
    if diagnostics.control_messages != expected_control {
        return Err(PureflowError::execution(format!(
            "watcher did not receive expected control message: {:?}",
            diagnostics.control_messages
        )));
    }
    if summary.terminal_state() != WorkflowTerminalState::Cancelled
        || summary.cancelled_node_count() != 1
        || summary.failed_node_count() != 0
    {
        return Err(PureflowError::execution(format!(
            "summary was not the expected cancellation outcome: terminal={:?}, cancelled={}, failed={}",
            summary.terminal_state(),
            summary.cancelled_node_count(),
            summary.failed_node_count()
        )));
    }
    if metadata_counts.node_cancelled != 1 || metadata_counts.error == 0 {
        return Err(PureflowError::metadata(format!(
            "metadata did not include expected cancellation/error records: {metadata_counts:?}"
        )));
    }

    Ok(())
}

fn packet(
    ctx: &NodeContext,
    output_port: &str,
    payload: &[u8],
    index: usize,
) -> pureflow_core::Result<PortPacket> {
    let source: MessageEndpoint =
        MessageEndpoint::new(ctx.node_id().clone(), port_id(output_port)?);
    let target: MessageEndpoint =
        MessageEndpoint::new(ctx.node_id().clone(), port_id(output_port)?);
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let message_id: MessageId = MessageId::new(format!("{}-{output_port}-{index}", ctx.node_id()))?;
    let metadata: MessageMetadata = MessageMetadata::new(
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

fn packet_payload_string(packet: PortPacket) -> pureflow_core::Result<String> {
    let bytes: Vec<u8> = packet
        .into_payload()
        .as_bytes()
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| PureflowError::execution("watcher workload expected byte payload"))?;
    String::from_utf8(bytes)
        .map_err(|source| PureflowError::execution(format!("payload was not UTF-8: {source}")))
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
    String::from_utf8(bytes)
        .map_err(|source| PureflowError::metadata(format!("metadata JSONL was not UTF-8: {source}")))
}

#[derive(Debug, Clone, Copy)]
struct MetadataCounts {
    total: usize,
    lifecycle: usize,
    node_cancelled: usize,
    error: usize,
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
        node_cancelled: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"kind\":\"node_cancelled\""))
            .count(),
        error: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"error\""))
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
