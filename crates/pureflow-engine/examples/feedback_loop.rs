//! Runnable feedback-loop workflow using an explicit cycle policy.

use std::collections::BTreeMap;
use std::error::Error;

use pureflow_core::{
    PureflowError, NodeExecutor, PacketPayload, PortPacket, PortsIn, PortsOut,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::{
    FeedbackLoopRunPolicy, StaticNodeExecutorRegistry, WorkflowRunPolicy,
    run_workflow_with_registry_policy_summary,
};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use pureflow_workflow::{
    EdgeDefinition, EdgeEndpoint, NodeDefinition, WorkflowDefinition, WorkflowGraph,
};
use futures::executor::block_on;
use futures::future::BoxFuture;

#[derive(Debug, Clone, Copy)]
enum LoopRole {
    Driver,
    Counter,
}

#[derive(Debug, Clone, Copy)]
struct LoopExecutor {
    role: LoopRole,
}

impl LoopExecutor {
    const fn driver() -> Self {
        Self {
            role: LoopRole::Driver,
        }
    }

    const fn counter() -> Self {
        Self {
            role: LoopRole::Counter,
        }
    }
}

impl NodeExecutor for LoopExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self.role {
                LoopRole::Driver => {
                    let output_port: PortId = port_id("out")?;
                    outputs
                        .send(
                            &output_port,
                            packet(b"seed", "driver", "counter")?,
                            &cancellation,
                        )
                        .await?;
                    let input_port: PortId = port_id("in")?;
                    let received_packet: PortPacket = inputs
                        .recv(&input_port, &cancellation)
                        .await?
                        .ok_or_else(|| PureflowError::execution("counter did not reply"))?;
                    let payload: Vec<u8> = packet_payload_bytes(received_packet)?;
                    println!("driver received {}", String::from_utf8_lossy(&payload));
                }
                LoopRole::Counter => {
                    let input_port: PortId = port_id("in")?;
                    let received_packet: PortPacket = inputs
                        .recv(&input_port, &cancellation)
                        .await?
                        .ok_or_else(|| PureflowError::execution("driver did not seed the loop"))?;
                    let payload: Vec<u8> = packet_payload_bytes(received_packet)?;
                    println!("counter received {}", String::from_utf8_lossy(&payload));
                    let output_port: PortId = port_id("out")?;
                    outputs
                        .send(
                            &output_port,
                            packet(b"ack", "counter", "driver")?,
                            &cancellation,
                        )
                        .await?;
                }
            }

            Ok(())
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let workflow: WorkflowDefinition = feedback_loop_workflow()?;
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("feedback-loop-example")?);
    let registry: StaticNodeExecutorRegistry<LoopExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (node_id("driver")?, LoopExecutor::driver()),
            (node_id("counter")?, LoopExecutor::counter()),
        ]));
    let policy: WorkflowRunPolicy =
        WorkflowRunPolicy::feedback_loops(FeedbackLoopRunPolicy::start_all_nodes_until_complete());

    let summary = block_on(run_workflow_with_registry_policy_summary(
        &workflow, &execution, &registry, policy,
    ))?;

    println!(
        "workflow {} completed with {} scheduled nodes and {} errors",
        workflow.id(),
        summary.scheduled_node_count(),
        summary.error_count()
    );
    summary.into_result()?;
    Ok(())
}

fn feedback_loop_workflow() -> Result<WorkflowDefinition, Box<dyn Error>> {
    let driver: NodeDefinition =
        NodeDefinition::new(node_id("driver")?, [port_id("in")?], [port_id("out")?])?;
    let counter: NodeDefinition =
        NodeDefinition::new(node_id("counter")?, [port_id("in")?], [port_id("out")?])?;
    let graph: WorkflowGraph = WorkflowGraph::with_cycles_allowed(
        [driver, counter],
        [
            EdgeDefinition::new(
                EdgeEndpoint::new(node_id("driver")?, port_id("out")?),
                EdgeEndpoint::new(node_id("counter")?, port_id("in")?),
            ),
            EdgeDefinition::new(
                EdgeEndpoint::new(node_id("counter")?, port_id("out")?),
                EdgeEndpoint::new(node_id("driver")?, port_id("in")?),
            ),
        ],
    )?;

    Ok(WorkflowDefinition::new(
        workflow_id("feedback-loop")?,
        graph,
    ))
}

fn packet(value: &[u8], source_node: &str, target_node: &str) -> pureflow_core::Result<PortPacket> {
    let source: MessageEndpoint = MessageEndpoint::new(node_id(source_node)?, port_id("out")?);
    let target: MessageEndpoint = MessageEndpoint::new(node_id(target_node)?, port_id("in")?);
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("feedback-loop-example")?);
    let metadata: MessageMetadata = MessageMetadata::new(
        MessageId::new(format!("{source_node}-to-{target_node}"))?,
        workflow_id("feedback-loop")?,
        execution,
        route,
    );

    Ok(PortPacket::new(
        metadata,
        PacketPayload::from(value.to_vec()),
    ))
}

fn packet_payload_bytes(packet: PortPacket) -> pureflow_core::Result<Vec<u8>> {
    packet
        .into_payload()
        .as_bytes()
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| PureflowError::execution("feedback loop example expected byte payload"))
}

fn node_id(value: &str) -> Result<NodeId, pureflow_types::IdentifierError> {
    NodeId::new(value)
}

fn port_id(value: &str) -> Result<PortId, pureflow_types::IdentifierError> {
    PortId::new(value)
}

fn workflow_id(value: &str) -> Result<WorkflowId, pureflow_types::IdentifierError> {
    WorkflowId::new(value)
}
