//! Reusable builders, test doubles, and property strategies for Pureflow tests.

use std::{
    collections::BTreeMap,
    future::{Future, Ready, ready},
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex},
};

use pureflow_core::{
    CancellationToken, PureflowError, NodeExecutor, PacketPayload, PortPacket, PortRecvError,
    PortsIn, PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use pureflow_workflow::{EdgeDefinition, EdgeEndpoint, NodeDefinition, WorkflowDefinition};
use proptest::{prelude::*, sample::select};

const IDENTIFIER_ALPHABET: [char; 66] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
    'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '-', '_', '.', '/',
];

/// Create a workflow identifier for tests.
///
/// # Panics
///
/// Panics if `value` is not a valid Pureflow workflow identifier.
#[must_use]
pub fn workflow_id(value: &str) -> WorkflowId {
    WorkflowId::new(value).expect("test workflow id must be valid")
}

/// Create a node identifier for tests.
///
/// # Panics
///
/// Panics if `value` is not a valid Pureflow node identifier.
#[must_use]
pub fn node_id(value: &str) -> NodeId {
    NodeId::new(value).expect("test node id must be valid")
}

/// Create a port identifier for tests.
///
/// # Panics
///
/// Panics if `value` is not a valid Pureflow port identifier.
#[must_use]
pub fn port_id(value: &str) -> PortId {
    PortId::new(value).expect("test port id must be valid")
}

/// Create execution metadata for the first attempt of a test run.
///
/// # Panics
///
/// Panics if `value` is not a valid Pureflow execution identifier.
#[must_use]
pub fn execution_metadata(value: &str) -> ExecutionMetadata {
    ExecutionMetadata::first_attempt(
        ExecutionId::new(value).expect("test execution id must be valid"),
    )
}

/// Builder for validated workflow node definitions.
#[derive(Debug, Clone)]
pub struct NodeBuilder {
    id: NodeId,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
}

impl NodeBuilder {
    /// Start a node builder for one node identifier.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self {
            id: node_id(id),
            input_ports: Vec::new(),
            output_ports: Vec::new(),
        }
    }

    /// Add an input port to the node.
    #[must_use]
    pub fn input(mut self, id: &str) -> Self {
        self.input_ports.push(port_id(id));
        self
    }

    /// Add an output port to the node.
    #[must_use]
    pub fn output(mut self, id: &str) -> Self {
        self.output_ports.push(port_id(id));
        self
    }

    /// Build a validated node definition.
    ///
    /// # Panics
    ///
    /// Panics if the configured ports violate node-definition invariants.
    #[must_use]
    pub fn build(self) -> NodeDefinition {
        NodeDefinition::new(self.id, self.input_ports, self.output_ports)
            .expect("test node definition must be valid")
    }
}

/// Builder for validated workflow definitions.
#[derive(Debug, Clone)]
pub struct WorkflowBuilder {
    id: WorkflowId,
    nodes: Vec<NodeDefinition>,
    edges: Vec<EdgeDefinition>,
}

impl WorkflowBuilder {
    /// Start a workflow builder for one workflow identifier.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self {
            id: workflow_id(id),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Add a node definition to the workflow.
    #[must_use]
    pub fn node(mut self, node: NodeDefinition) -> Self {
        self.nodes.push(node);
        self
    }

    /// Add a validated edge between two node ports.
    #[must_use]
    pub fn edge(
        mut self,
        source_node: &str,
        source_port: &str,
        target_node: &str,
        target_port: &str,
    ) -> Self {
        self.edges.push(EdgeDefinition::new(
            EdgeEndpoint::new(node_id(source_node), port_id(source_port)),
            EdgeEndpoint::new(node_id(target_node), port_id(target_port)),
        ));
        self
    }

    /// Add a validated edge between two node ports with an explicit capacity.
    #[must_use]
    pub fn edge_with_capacity(
        mut self,
        source_node: &str,
        source_port: &str,
        target_node: &str,
        target_port: &str,
        capacity: NonZeroUsize,
    ) -> Self {
        self.edges.push(EdgeDefinition::with_capacity(
            EdgeEndpoint::new(node_id(source_node), port_id(source_port)),
            EdgeEndpoint::new(node_id(target_node), port_id(target_port)),
            capacity,
        ));
        self
    }

    /// Build a validated workflow definition.
    ///
    /// # Panics
    ///
    /// Panics if the configured workflow graph violates structural invariants.
    #[must_use]
    pub fn build(self) -> WorkflowDefinition {
        WorkflowDefinition::from_parts(self.id, self.nodes, self.edges)
            .expect("test workflow definition must be valid")
    }
}

/// Executor test double that records the visited node order.
#[derive(Default)]
pub struct RecordingExecutor {
    contexts: Mutex<Vec<NodeContext>>,
    inputs: Mutex<Vec<PortsIn>>,
    outputs: Mutex<Vec<PortsOut>>,
}

impl RecordingExecutor {
    /// Return the visited node contexts in call order.
    ///
    /// # Panics
    ///
    /// Panics if the internal recording lock has been poisoned by an earlier
    /// failing test thread.
    #[must_use]
    pub fn visited_contexts(&self) -> Vec<NodeContext> {
        self.contexts
            .lock()
            .expect("recording executor contexts lock should not be poisoned")
            .clone()
    }

    /// Return the visited node identifiers in call order.
    #[must_use]
    pub fn visited_nodes(&self) -> Vec<NodeId> {
        self.visited_contexts()
            .into_iter()
            .map(|ctx: NodeContext| ctx.node_id().clone())
            .collect()
    }

    /// Return the visited node names in call order.
    #[must_use]
    pub fn visited_node_names(&self) -> Vec<String> {
        self.visited_nodes()
            .into_iter()
            .map(|node: NodeId| node.to_string())
            .collect()
    }

    /// Return the visited input-port names in call order.
    ///
    /// # Panics
    ///
    /// Panics if the internal recording lock has been poisoned by an earlier
    /// failing test thread.
    #[must_use]
    pub fn visited_input_port_names(&self) -> Vec<Vec<String>> {
        self.inputs
            .lock()
            .expect("recording executor inputs lock should not be poisoned")
            .iter()
            .map(|ports: &PortsIn| ports.port_ids().iter().map(ToString::to_string).collect())
            .collect()
    }

    /// Return the visited output-port names in call order.
    ///
    /// # Panics
    ///
    /// Panics if the internal recording lock has been poisoned by an earlier
    /// failing test thread.
    #[must_use]
    pub fn visited_output_port_names(&self) -> Vec<Vec<String>> {
        self.outputs
            .lock()
            .expect("recording executor outputs lock should not be poisoned")
            .iter()
            .map(|ports: &PortsOut| ports.port_ids().iter().map(ToString::to_string).collect())
            .collect()
    }
}

impl NodeExecutor for RecordingExecutor {
    type RunFuture<'a> = Ready<Result<()>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        self.contexts
            .lock()
            .expect("recording executor contexts lock should not be poisoned")
            .push(ctx);
        self.inputs
            .lock()
            .expect("recording executor inputs lock should not be poisoned")
            .push(inputs);
        self.outputs
            .lock()
            .expect("recording executor outputs lock should not be poisoned")
            .push(outputs);
        ready(Ok(()))
    }
}

/// Executor test double that always returns the configured failure.
#[derive(Debug, Clone)]
pub struct FailingExecutor {
    error: PureflowError,
}

impl FailingExecutor {
    /// Create a failing executor with an explicit Pureflow error.
    #[must_use]
    pub const fn new(error: PureflowError) -> Self {
        Self { error }
    }

    /// Create a failing executor that reports an execution failure.
    #[must_use]
    pub fn execution(message: impl Into<String>) -> Self {
        Self::new(PureflowError::execution(message))
    }
}

impl NodeExecutor for FailingExecutor {
    type RunFuture<'a> = Ready<Result<()>>;

    fn run(&self, _ctx: NodeContext, _inputs: PortsIn, _outputs: PortsOut) -> Self::RunFuture<'_> {
        ready(Err(self.error.clone()))
    }
}

/// Strategy for identifiers that satisfy Pureflow's current validation rules.
pub fn valid_identifier_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(select(&IDENTIFIER_ALPHABET), 1..16)
        .prop_map(|chars: Vec<char>| chars.into_iter().collect())
}

/// Drain all packets from one input port until it closes or disconnects.
///
/// Returns the collected packets in receive order. Returns an error if the
/// cancellation token fires or an unexpected receive error occurs.
///
/// # Errors
///
/// Returns the underlying `PureflowError` for cancellation or unexpected port errors.
pub async fn drain_port(
    inputs: &mut PortsIn,
    port: &PortId,
    cancellation: &CancellationToken,
) -> Result<Vec<PortPacket>> {
    let mut packets: Vec<PortPacket> = Vec::new();
    loop {
        match inputs.recv(port, cancellation).await {
            Ok(Some(packet)) => packets.push(packet),
            Ok(None) | Err(PortRecvError::Disconnected { .. }) => return Ok(packets),
            Err(err) => return Err(err.into()),
        }
    }
}

/// Build a minimal `PortPacket` from raw bytes suitable for unit tests.
///
/// Uses canonical test identifiers: workflow `test-flow`, route `source.out →
/// sink.in`, execution `test-run`, message `test-msg-1`. Callers that need
/// specific metadata should construct `PortPacket` directly.
///
/// # Panics
///
/// Panics if any of the hard-coded canonical test identifiers are invalid.
#[must_use]
pub fn test_packet(payload: impl Into<Vec<u8>>) -> PortPacket {
    let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
    let target: MessageEndpoint = MessageEndpoint::new(node_id("sink"), port_id("in"));
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(
        ExecutionId::new("test-run").expect("test execution id must be valid"),
    );
    let metadata: MessageMetadata = MessageMetadata::new(
        MessageId::new("test-msg-1").expect("test message id must be valid"),
        workflow_id("test-flow"),
        execution,
        route,
    );
    PortPacket::new(metadata, PacketPayload::from(payload.into()))
}

/// Node executor test double that drains all input ports and records every
/// received packet in receive order.
///
/// Clones of `SinkExecutor` share the same underlying packet store, so you
/// can insert the executor into a registry and inspect it after the run
/// completes without moving the original.
#[derive(Debug, Default, Clone)]
pub struct SinkExecutor {
    received: Arc<Mutex<Vec<PortPacket>>>,
}

impl SinkExecutor {
    /// Create a new sink executor with an empty packet store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of packets received across all ports.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn packet_count(&self) -> usize {
        self.received
            .lock()
            .expect("sink executor lock must not be poisoned")
            .len()
    }

    /// Drain and return all received packets, clearing the internal store.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn drain_received(&self) -> Vec<PortPacket> {
        self.received
            .lock()
            .expect("sink executor lock must not be poisoned")
            .drain(..)
            .collect()
    }
}

impl NodeExecutor for SinkExecutor {
    type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn run(
        &self,
        ctx: NodeContext,
        mut inputs: PortsIn,
        _outputs: PortsOut,
    ) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation: CancellationToken = ctx.cancellation_token();
            let port_ids: Vec<PortId> = inputs.port_ids().to_vec();
            for port in &port_ids {
                let mut packets: Vec<PortPacket> =
                    drain_port(&mut inputs, port, &cancellation).await?;
                self.received
                    .lock()
                    .expect("sink executor lock must not be poisoned")
                    .append(&mut packets);
            }
            Ok(())
        })
    }
}

/// Node executor test double that sends pre-configured byte payloads to output
/// ports and drains all input ports without recording them.
///
/// Configure payloads per output port with [`SourceExecutor::with_port_payloads`]
/// before inserting the executor into a registry. If a port has no configured
/// payloads, nothing is sent on that port.
#[derive(Debug, Clone, Default)]
pub struct SourceExecutor {
    payloads_by_port: BTreeMap<PortId, Vec<Vec<u8>>>,
}

impl SourceExecutor {
    /// Create a new source executor with no configured payloads.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a list of byte payloads to send on one output port.
    ///
    /// # Panics
    ///
    /// Panics if `port` is not a valid Pureflow port identifier.
    #[must_use]
    pub fn with_port_payloads(mut self, port: &str, payloads: Vec<Vec<u8>>) -> Self {
        self.payloads_by_port.insert(port_id(port), payloads);
        self
    }
}

impl NodeExecutor for SourceExecutor {
    type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        let payloads_by_port: BTreeMap<PortId, Vec<Vec<u8>>> = self.payloads_by_port.clone();
        Box::pin(async move {
            let cancellation: CancellationToken = ctx.cancellation_token();

            for input_port in inputs.port_ids().to_vec() {
                drain_port(&mut inputs, &input_port, &cancellation).await?;
            }

            for (out_port, payloads) in &payloads_by_port {
                for (index, payload) in payloads.iter().enumerate() {
                    let source: MessageEndpoint =
                        MessageEndpoint::new(ctx.node_id().clone(), out_port.clone());
                    let target: MessageEndpoint =
                        MessageEndpoint::new(ctx.node_id().clone(), out_port.clone());
                    let route: MessageRoute = MessageRoute::new(Some(source), target);
                    let message_id_str: String =
                        format!("test-{}-{out_port}-{index}", ctx.node_id());
                    let message_id: MessageId = MessageId::new(message_id_str).map_err(
                        |source: pureflow_types::IdentifierError| {
                            PureflowError::execution(format!(
                                "failed to build test message id: {source}"
                            ))
                        },
                    )?;
                    let metadata: MessageMetadata = MessageMetadata::new(
                        message_id,
                        ctx.workflow_id().clone(),
                        ctx.execution().clone(),
                        route,
                    );
                    let packet: PortPacket =
                        PortPacket::new(metadata, PacketPayload::from(payload.clone()));
                    outputs.send(out_port, packet, &cancellation).await?;
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_core::{ErrorCode, InputPortHandle, OutputPortHandle, bounded_edge_channel};
    use pureflow_runtime::AsupersyncRuntime;
    use std::num::NonZeroUsize;

    use pureflow_core::PortsOut;

    fn make_channel(src_port: &str, dst_port: &str) -> (OutputPortHandle, InputPortHandle) {
        bounded_edge_channel(
            port_id(src_port),
            port_id(dst_port),
            NonZeroUsize::new(8).expect("capacity must be nonzero"),
        )
    }

    #[test]
    fn workflow_builder_builds_valid_linear_workflow() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").output("out").build())
            .node(NodeBuilder::new("second").input("in").build())
            .edge("first", "out", "second", "in")
            .build();

        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(workflow.edges().len(), 1);
    }

    #[test]
    fn failing_executor_returns_configured_error() {
        let executor: FailingExecutor = FailingExecutor::execution("boom");
        let ctx: NodeContext = NodeContext::new(
            workflow_id("flow"),
            node_id("node"),
            execution_metadata("run-1"),
        );
        let err: PureflowError = executor
            .run(ctx, PortsIn::default(), PortsOut::default())
            .into_inner()
            .expect_err("executor must fail");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
    }

    #[test]
    fn test_packet_builds_valid_port_packet() {
        let packet: PortPacket = test_packet(b"hello".to_vec());
        let payload_bytes: Vec<u8> = packet
            .into_payload()
            .as_bytes()
            .expect("test_packet should carry bytes payload")
            .to_vec();

        assert_eq!(payload_bytes, b"hello");
    }

    #[test]
    fn drain_port_collects_all_packets_until_closed() {
        let (output_handle, input_handle) = make_channel("out", "in");
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input_handle]);
        let mut source_outputs: PortsOut =
            PortsOut::from_handles([port_id("out")], [output_handle]);
        let ctx: NodeContext = NodeContext::new(
            workflow_id("flow"),
            node_id("source"),
            execution_metadata("run-1"),
        );
        let cancellation: CancellationToken = ctx.cancellation_token();

        let rt: AsupersyncRuntime = AsupersyncRuntime::new().expect("runtime must start");
        rt.block_on(async {
            source_outputs
                .send(&port_id("out"), test_packet(b"a"), &cancellation)
                .await
                .expect("send must succeed");
            source_outputs
                .send(&port_id("out"), test_packet(b"b"), &cancellation)
                .await
                .expect("send must succeed");
            drop(source_outputs);

            let packets: Vec<PortPacket> = drain_port(&mut inputs, &port_id("in"), &cancellation)
                .await
                .expect("drain must succeed");

            assert_eq!(packets.len(), 2);
        });
    }

    #[test]
    fn sink_executor_records_all_packets() {
        let (output_handle, input_handle) = make_channel("out", "in");
        let inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input_handle]);
        let mut source_outputs: PortsOut =
            PortsOut::from_handles([port_id("out")], [output_handle]);
        let ctx: NodeContext = NodeContext::new(
            workflow_id("flow"),
            node_id("sink"),
            execution_metadata("run-1"),
        );
        let cancellation: CancellationToken = ctx.cancellation_token();

        let executor: SinkExecutor = SinkExecutor::new();
        let executor_ref: SinkExecutor = executor.clone();

        let rt: AsupersyncRuntime = AsupersyncRuntime::new().expect("runtime must start");
        rt.block_on(async {
            source_outputs
                .send(&port_id("out"), test_packet(b"one"), &cancellation)
                .await
                .expect("send must succeed");
            source_outputs
                .send(&port_id("out"), test_packet(b"two"), &cancellation)
                .await
                .expect("send must succeed");
            drop(source_outputs);

            executor_ref
                .run(ctx, inputs, PortsOut::default())
                .await
                .expect("sink executor must succeed");

            assert_eq!(executor.packet_count(), 2);
        });
    }

    #[test]
    fn source_executor_sends_configured_payloads() {
        let (output_handle, input_handle) = make_channel("out", "in");
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output_handle]);
        let ctx: NodeContext = NodeContext::new(
            workflow_id("flow"),
            node_id("source"),
            execution_metadata("run-1"),
        );
        let cancellation: CancellationToken = ctx.cancellation_token();

        let executor: SourceExecutor =
            SourceExecutor::new().with_port_payloads("out", vec![b"x".to_vec(), b"y".to_vec()]);

        let mut sink_inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input_handle]);

        let rt: AsupersyncRuntime = AsupersyncRuntime::new().expect("runtime must start");
        rt.block_on(async {
            executor
                .run(ctx, PortsIn::default(), outputs)
                .await
                .expect("source executor must succeed");

            let packets: Vec<PortPacket> =
                drain_port(&mut sink_inputs, &port_id("in"), &cancellation)
                    .await
                    .expect("drain must succeed");

            assert_eq!(packets.len(), 2);
            let payloads: Vec<Vec<u8>> = packets
                .into_iter()
                .map(|p: PortPacket| {
                    p.into_payload()
                        .as_bytes()
                        .expect("payload must be bytes")
                        .to_vec()
                })
                .collect();
            assert_eq!(payloads[0], b"x");
            assert_eq!(payloads[1], b"y");
        });
    }
}
