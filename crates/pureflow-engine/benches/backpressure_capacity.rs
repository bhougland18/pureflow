//! Criterion benchmarks for workflow backpressure capacity behavior.

use std::hint::black_box;

use pureflow_core::{
    PureflowError, NodeExecutor, PacketPayload, PortPacket, PortsIn, PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::run_workflow;
use pureflow_test_kit::{
    NodeBuilder, WorkflowBuilder, execution_metadata, node_id, port_id, workflow_id,
};
use pureflow_types::{ExecutionId, MessageId};
use pureflow_workflow::WorkflowDefinition;
use criterion::{
    BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main, measurement::WallTime,
};
use futures::{executor::block_on, future::BoxFuture};

const MESSAGE_COUNT: usize = 32;

#[derive(Debug)]
struct BackpressureBenchExecutor {
    message_count: usize,
}

impl BackpressureBenchExecutor {
    const fn new(message_count: usize) -> Self {
        Self { message_count }
    }
}

impl NodeExecutor for BackpressureBenchExecutor {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        let message_count: usize = self.message_count;
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();

            match ctx.node_id().as_str() {
                "source" => {
                    for sequence in 0..message_count {
                        outputs
                            .send(
                                &port_id("out"),
                                packet("source", "sink", sequence),
                                &cancellation,
                            )
                            .await?;
                    }
                }
                "sink" | "left-sink" | "right-sink" => {
                    recv_exact(&mut inputs, message_count, &cancellation).await?;
                }
                "left-source" | "right-source" => {
                    let source_node: String = ctx.node_id().to_string();
                    for sequence in 0..message_count {
                        outputs
                            .send(
                                &port_id("out"),
                                packet(&source_node, "collector", sequence),
                                &cancellation,
                            )
                            .await?;
                    }
                }
                "collector" => {
                    recv_exact(&mut inputs, message_count.saturating_mul(2), &cancellation).await?;
                }
                _ => {}
            }

            Ok(())
        })
    }
}

async fn recv_exact(
    inputs: &mut PortsIn,
    packet_count: usize,
    cancellation: &pureflow_core::CancellationToken,
) -> Result<()> {
    for _packet_index in 0..packet_count {
        let packet: Option<PortPacket> = inputs.recv(&port_id("in"), cancellation).await?;
        if let Some(packet) = packet {
            black_box(packet);
        } else {
            return Err(PureflowError::execution(
                "benchmark input closed before all packets were received",
            ));
        }
    }

    Ok(())
}

fn packet(source_node: &str, target_node: &str, sequence: usize) -> PortPacket {
    let source: MessageEndpoint = MessageEndpoint::new(node_id(source_node), port_id("out"));
    let target: MessageEndpoint = MessageEndpoint::new(node_id(target_node), port_id("in"));
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(
        ExecutionId::new("bench-run").expect("benchmark execution id should be valid"),
    );
    let metadata: MessageMetadata = MessageMetadata::new(
        MessageId::new(format!("{source_node}-{sequence}"))
            .expect("benchmark message id should be valid"),
        workflow_id("bench-flow"),
        execution,
        route,
    );

    let payload_byte: u8 =
        u8::try_from(sequence).expect("benchmark message count should fit in one byte");

    PortPacket::new(metadata, PacketPayload::from(vec![payload_byte]))
}

fn linear_capacity_one_workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("bench-flow")
        .node(NodeBuilder::new("source").output("out").build())
        .node(NodeBuilder::new("sink").input("in").build())
        .edge_with_capacity("source", "out", "sink", "in", std::num::NonZeroUsize::MIN)
        .build()
}

fn linear_default_capacity_workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("bench-flow")
        .node(NodeBuilder::new("source").output("out").build())
        .node(NodeBuilder::new("sink").input("in").build())
        .edge("source", "out", "sink", "in")
        .build()
}

fn fan_out_workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("bench-flow")
        .node(NodeBuilder::new("source").output("out").build())
        .node(NodeBuilder::new("left-sink").input("in").build())
        .node(NodeBuilder::new("right-sink").input("in").build())
        .edge("source", "out", "left-sink", "in")
        .edge("source", "out", "right-sink", "in")
        .build()
}

fn fan_in_workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("bench-flow")
        .node(NodeBuilder::new("left-source").output("out").build())
        .node(NodeBuilder::new("right-source").output("out").build())
        .node(NodeBuilder::new("collector").input("in").build())
        .edge("left-source", "out", "collector", "in")
        .edge("right-source", "out", "collector", "in")
        .build()
}

fn bench_workflow(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    workflow: &WorkflowDefinition,
    delivered_messages: u64,
) {
    let execution: ExecutionMetadata = execution_metadata("bench-run");
    let executor: BackpressureBenchExecutor = BackpressureBenchExecutor::new(MESSAGE_COUNT);
    group.throughput(Throughput::Elements(delivered_messages));
    group.bench_function(name, |b| {
        b.iter(|| {
            block_on(run_workflow(
                black_box(workflow),
                black_box(&execution),
                black_box(&executor),
            ))
            .expect("benchmark workflow should run");
        });
    });
}

fn backpressure_capacity(c: &mut Criterion) {
    let linear_capacity_one: WorkflowDefinition = linear_capacity_one_workflow();
    let linear_default_capacity: WorkflowDefinition = linear_default_capacity_workflow();
    let fan_out: WorkflowDefinition = fan_out_workflow();
    let fan_in: WorkflowDefinition = fan_in_workflow();

    let mut group: BenchmarkGroup<'_, WallTime> = c.benchmark_group("engine_backpressure_capacity");
    bench_workflow(
        &mut group,
        "linear_capacity_1",
        &linear_capacity_one,
        MESSAGE_COUNT as u64,
    );
    bench_workflow(
        &mut group,
        "linear_default_capacity",
        &linear_default_capacity,
        MESSAGE_COUNT as u64,
    );
    bench_workflow(
        &mut group,
        "fan_out_default_capacity",
        &fan_out,
        MESSAGE_COUNT.saturating_mul(2) as u64,
    );
    bench_workflow(
        &mut group,
        "fan_in_default_capacity",
        &fan_in,
        MESSAGE_COUNT.saturating_mul(2) as u64,
    );
    group.finish();
}

criterion_group!(benches, backpressure_capacity);
criterion_main!(benches);
