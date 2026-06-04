//! Runnable replay/branch evaluation workload with parallel branch comparison.
//!
//! A shared source fans out to two independent processing branches. An evaluator
//! drains each branch's output port separately and compares per-branch results
//! to verify deterministic packet counts and payload correspondence.

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

const INPUTS: &[&str] = &["alpha", "beta", "gamma"];

#[derive(Debug, Clone)]
struct BranchResult {
    a: Vec<String>,
    b: Vec<String>,
}

#[derive(Debug, Clone)]
enum ReplayBranchExecutor {
    Source,
    BranchA,
    BranchB,
    Evaluator {
        result: Arc<Mutex<Option<BranchResult>>>,
    },
}

impl NodeExecutor for ReplayBranchExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self {
                Self::Source => {
                    for (index, row) in INPUTS.iter().enumerate() {
                        outputs
                            .send(
                                &port_id("out")?,
                                packet(&ctx, "out", row.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::BranchA => {
                    let packets = drain_port(&mut inputs, &port_id("in")?, &cancellation).await?;
                    for (index, received) in packets.into_iter().enumerate() {
                        let row = packet_payload_string(received)?;
                        let transformed = format!("tag:{row}");
                        outputs
                            .send(
                                &port_id("out")?,
                                packet(&ctx, "out", transformed.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::BranchB => {
                    let packets = drain_port(&mut inputs, &port_id("in")?, &cancellation).await?;
                    for (index, received) in packets.into_iter().enumerate() {
                        let row = packet_payload_string(received)?;
                        let reversed: String = row.chars().rev().collect();
                        let transformed = format!("rev:{reversed}");
                        outputs
                            .send(
                                &port_id("out")?,
                                packet(&ctx, "out", transformed.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::Evaluator { result } => {
                    let a_packets = drain_port(&mut inputs, &port_id("a")?, &cancellation).await?;
                    let b_packets = drain_port(&mut inputs, &port_id("b")?, &cancellation).await?;
                    let a: Vec<String> = a_packets
                        .into_iter()
                        .map(packet_payload_string)
                        .collect::<pureflow_core::Result<_>>()?;
                    let b: Vec<String> = b_packets
                        .into_iter()
                        .map(packet_payload_string)
                        .collect::<pureflow_core::Result<_>>()?;
                    *result
                        .lock()
                        .expect("evaluator result lock should not be poisoned") =
                        Some(BranchResult { a, b });
                }
            }

            Ok(())
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("replay-branch-eval-example")?);
    let result: Arc<Mutex<Option<BranchResult>>> = Arc::new(Mutex::new(None));
    let registry: StaticNodeExecutorRegistry<ReplayBranchExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("source")?, ReplayBranchExecutor::Source),
            (node_id("branch-a")?, ReplayBranchExecutor::BranchA),
            (node_id("branch-b")?, ReplayBranchExecutor::BranchB),
            (
                node_id("evaluator")?,
                ReplayBranchExecutor::Evaluator {
                    result: result.clone(),
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

    let branch_result: BranchResult = result
        .lock()
        .expect("evaluator result lock should not be poisoned")
        .take()
        .ok_or_else(|| PureflowError::execution("evaluator did not record a result"))?;
    let metadata_jsonl: String = metadata_jsonl_from_sink(metadata_sink)?;
    let metadata_counts: MetadataCounts = count_metadata_records(&metadata_jsonl);

    assert_expected_output(&branch_result, metadata_counts)?;

    println!("replay/branch eval workflow `{}` completed", workflow.id());
    println!("source inputs: {}", INPUTS.len());
    println!("branch-a outputs: {}", branch_result.a.len());
    println!("branch-b outputs: {}", branch_result.b.len());
    for (i, (a, b)) in branch_result
        .a
        .iter()
        .zip(branch_result.b.iter())
        .enumerate()
    {
        println!("  row[{i}]: {a} | {b}");
    }
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
    result: &BranchResult,
    metadata_counts: MetadataCounts,
) -> pureflow_core::Result<()> {
    let expected_a: Vec<String> = INPUTS.iter().map(|r| format!("tag:{r}")).collect();
    let expected_b: Vec<String> = INPUTS
        .iter()
        .map(|r| {
            let reversed: String = r.chars().rev().collect();
            format!("rev:{reversed}")
        })
        .collect();

    if result.a != expected_a {
        return Err(PureflowError::execution(format!(
            "branch-a outputs did not match expected: got {:?}",
            result.a
        )));
    }
    if result.b != expected_b {
        return Err(PureflowError::execution(format!(
            "branch-b outputs did not match expected: got {:?}",
            result.b
        )));
    }
    if result.a.len() != result.b.len() {
        return Err(PureflowError::execution(format!(
            "branch output counts diverged: a={} b={}",
            result.a.len(),
            result.b.len()
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
    WorkflowBuilder::new("replay-branch-eval-workload")
        .node(NodeBuilder::new("source").output("out").build())
        .node(
            NodeBuilder::new("branch-a")
                .input("in")
                .output("out")
                .build(),
        )
        .node(
            NodeBuilder::new("branch-b")
                .input("in")
                .output("out")
                .build(),
        )
        .node(NodeBuilder::new("evaluator").input("a").input("b").build())
        .edge_with_capacity("source", "out", "branch-a", "in", capacity(4))
        .edge_with_capacity("source", "out", "branch-b", "in", capacity(4))
        .edge_with_capacity("branch-a", "out", "evaluator", "a", capacity(4))
        .edge_with_capacity("branch-b", "out", "evaluator", "b", capacity(4))
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
            PureflowError::execution("replay-branch-eval workload expected byte payload")
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
