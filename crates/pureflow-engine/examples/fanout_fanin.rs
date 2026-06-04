//! Runnable fan-out/fan-in workload with bounded delivery metadata.

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

const ROWS: &[&str] = &["alpha", "beta", "gamma"];

#[derive(Debug, Clone)]
enum FanoutFaninExecutor {
    Source,
    Splitter,
    Enrich { prefix: &'static str },
    Collector { received: Arc<Mutex<Vec<String>>> },
}

impl NodeExecutor for FanoutFaninExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self {
                Self::Source => {
                    for (index, row) in ROWS.iter().enumerate() {
                        outputs
                            .send(
                                &port_id("rows")?,
                                packet(&ctx, "rows", row.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::Splitter => {
                    let packets: Vec<PortPacket> =
                        drain_port(&mut inputs, &port_id("rows")?, &cancellation).await?;
                    for (index, received_packet) in packets.into_iter().enumerate() {
                        let bytes: Vec<u8> = packet_payload_bytes(received_packet)?;
                        outputs
                            .send(
                                &port_id("row")?,
                                packet(&ctx, "row", &bytes, index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::Enrich { prefix } => {
                    let packets: Vec<PortPacket> =
                        drain_port(&mut inputs, &port_id("row")?, &cancellation).await?;
                    for (index, received_packet) in packets.into_iter().enumerate() {
                        let row: String = packet_payload_string(received_packet)?;
                        let enriched: String = format!("{prefix}:{row}");
                        outputs
                            .send(
                                &port_id("enriched")?,
                                packet(&ctx, "enriched", enriched.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::Collector { received } => {
                    let packets: Vec<PortPacket> =
                        drain_port(&mut inputs, &port_id("enriched")?, &cancellation).await?;
                    let mut rows: Vec<String> = packets
                        .into_iter()
                        .map(packet_payload_string)
                        .collect::<pureflow_core::Result<Vec<String>>>()?;
                    rows.sort();
                    received
                        .lock()
                        .expect("collector rows lock should not be poisoned")
                        .extend(rows);
                }
            }

            Ok(())
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("fanout-fanin-example")?);
    let collected: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let registry: StaticNodeExecutorRegistry<FanoutFaninExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("source")?, FanoutFaninExecutor::Source),
            (node_id("splitter")?, FanoutFaninExecutor::Splitter),
            (
                node_id("left-enrich")?,
                FanoutFaninExecutor::Enrich { prefix: "left" },
            ),
            (
                node_id("right-enrich")?,
                FanoutFaninExecutor::Enrich { prefix: "right" },
            ),
            (
                node_id("collector")?,
                FanoutFaninExecutor::Collector {
                    received: collected.clone(),
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

    let collected_rows: Vec<String> = collected
        .lock()
        .expect("collector rows lock should not be poisoned")
        .clone();
    let metadata_jsonl: String = metadata_jsonl_from_sink(metadata_sink)?;
    let metadata_counts: MetadataCounts = count_metadata_records(&metadata_jsonl);
    let expected_rows: Vec<String> = expected_collected_rows();
    if collected_rows != expected_rows {
        return Err(PureflowError::execution(format!(
            "collector rows did not match expected fanout/fanin output: got {collected_rows:?}"
        ))
        .into());
    }
    if metadata_counts.lifecycle == 0
        || metadata_counts.message == 0
        || metadata_counts.queue_pressure == 0
    {
        return Err(PureflowError::metadata(format!(
            "metadata shape was incomplete: {metadata_counts:?}"
        ))
        .into());
    }

    println!("fanout/fanin workflow `{}` completed", workflow.id());
    println!("source rows: {}", ROWS.len());
    println!("collector rows: {}", collected_rows.len());
    println!("collector payloads: {}", collected_rows.join(", "));
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

fn workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("fanout-fanin-workload")
        .node(NodeBuilder::new("source").output("rows").build())
        .node(
            NodeBuilder::new("splitter")
                .input("rows")
                .output("row")
                .build(),
        )
        .node(
            NodeBuilder::new("left-enrich")
                .input("row")
                .output("enriched")
                .build(),
        )
        .node(
            NodeBuilder::new("right-enrich")
                .input("row")
                .output("enriched")
                .build(),
        )
        .node(NodeBuilder::new("collector").input("enriched").build())
        .edge_with_capacity("source", "rows", "splitter", "rows", capacity(1))
        .edge_with_capacity("splitter", "row", "left-enrich", "row", capacity(1))
        .edge_with_capacity("splitter", "row", "right-enrich", "row", capacity(1))
        .edge_with_capacity(
            "left-enrich",
            "enriched",
            "collector",
            "enriched",
            capacity(1),
        )
        .edge_with_capacity(
            "right-enrich",
            "enriched",
            "collector",
            "enriched",
            capacity(1),
        )
        .build()
}

fn expected_collected_rows() -> Vec<String> {
    let mut rows: Vec<String> = ROWS
        .iter()
        .flat_map(|row| [format!("left:{row}"), format!("right:{row}")])
        .collect();
    rows.sort();
    rows
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

fn packet_payload_bytes(packet: PortPacket) -> pureflow_core::Result<Vec<u8>> {
    packet
        .into_payload()
        .as_bytes()
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| PureflowError::execution("fanout/fanin workload expected byte payload"))
}

fn packet_payload_string(packet: PortPacket) -> pureflow_core::Result<String> {
    let bytes: Vec<u8> = packet_payload_bytes(packet)?;
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
