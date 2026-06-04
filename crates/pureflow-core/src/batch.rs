//! Runtime-neutral batch execution boundary for host-owned adapters.

use std::collections::BTreeMap;

use pureflow_types::PortId;

use crate::{PortPacket, Result};

/// Input packets grouped by declared input port.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(not(feature = "arrow"), derive(Eq))]
pub struct BatchInputs {
    packets_by_port: BTreeMap<PortId, Vec<PortPacket>>,
}

impl BatchInputs {
    /// Create an empty input batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            packets_by_port: BTreeMap::new(),
        }
    }

    /// Create an input batch from port-grouped packets.
    #[must_use]
    pub fn from_packets(packets_by_port: impl Into<BTreeMap<PortId, Vec<PortPacket>>>) -> Self {
        Self {
            packets_by_port: packets_by_port.into(),
        }
    }

    /// Add one packet to the batch for a port.
    pub fn push(&mut self, port_id: PortId, packet: PortPacket) {
        self.packets_by_port
            .entry(port_id)
            .or_default()
            .push(packet);
    }

    /// Borrow all packets for one port.
    #[must_use]
    pub fn packets(&self, port_id: &PortId) -> &[PortPacket] {
        self.packets_by_port.get(port_id).map_or(&[], Vec::as_slice)
    }

    /// Borrow the full port-to-packets map.
    #[must_use]
    pub const fn packets_by_port(&self) -> &BTreeMap<PortId, Vec<PortPacket>> {
        &self.packets_by_port
    }

    /// Consume the batch into the full port-to-packets map.
    #[must_use]
    pub fn into_packets_by_port(self) -> BTreeMap<PortId, Vec<PortPacket>> {
        self.packets_by_port
    }
}

/// Output packets grouped by declared output port.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(not(feature = "arrow"), derive(Eq))]
pub struct BatchOutputs {
    packets_by_port: BTreeMap<PortId, Vec<PortPacket>>,
}

impl BatchOutputs {
    /// Create an empty output batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            packets_by_port: BTreeMap::new(),
        }
    }

    /// Create an output batch from port-grouped packets.
    #[must_use]
    pub fn from_packets(packets_by_port: impl Into<BTreeMap<PortId, Vec<PortPacket>>>) -> Self {
        Self {
            packets_by_port: packets_by_port.into(),
        }
    }

    /// Add one packet to the batch for a port.
    pub fn push(&mut self, port_id: PortId, packet: PortPacket) {
        self.packets_by_port
            .entry(port_id)
            .or_default()
            .push(packet);
    }

    /// Borrow all packets for one port.
    #[must_use]
    pub fn packets(&self, port_id: &PortId) -> &[PortPacket] {
        self.packets_by_port.get(port_id).map_or(&[], Vec::as_slice)
    }

    /// Borrow the full port-to-packets map.
    #[must_use]
    pub const fn packets_by_port(&self) -> &BTreeMap<PortId, Vec<PortPacket>> {
        &self.packets_by_port
    }

    /// Consume the batch into the full port-to-packets map.
    #[must_use]
    pub fn into_packets_by_port(self) -> BTreeMap<PortId, Vec<PortPacket>> {
        self.packets_by_port
    }
}

/// Runtime-neutral batch executor for future WASM and process adapters.
///
/// Adapters must accept empty [`BatchInputs`] and return an empty
/// [`BatchOutputs`] unless the adapter has a documented reason to fail. Batch
/// shaping is a host concern.
///
/// This trait remains topology-blind: validating output ports against workflow
/// contracts and capabilities belongs to the engine or host adapter that owns
/// those declarations, not to the batch executor.
///
/// Invocation may block. The engine is responsible for running adapters on an
/// appropriate task and for triggering any adapter-specific cancellation hook.
pub trait BatchExecutor: Send + Sync {
    /// Invoke the adapter with host-owned input packets.
    ///
    /// # Errors
    ///
    /// Returns an error if the adapter cannot complete the batch invocation.
    fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs>;
}

/// Opaque batch-executor adapter for WASM-like node implementations.
pub struct WasmModule {
    executor: Box<dyn BatchExecutor>,
}

impl WasmModule {
    /// Create a module wrapper around a batch executor.
    #[must_use]
    pub const fn new(executor: Box<dyn BatchExecutor>) -> Self {
        Self { executor }
    }

    /// Invoke the wrapped batch executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the wrapped executor fails.
    pub fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs> {
        self.executor.invoke(inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        PacketPayload,
        context::ExecutionMetadata,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use pureflow_types::{ExecutionId, MessageId, NodeId, WorkflowId};

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn message_id(value: &str) -> MessageId {
        MessageId::new(value).expect("valid message id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn packet(value: &'static [u8]) -> PortPacket {
        let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id("wasm"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route);

        PortPacket::new(metadata, PacketPayload::from(value))
    }

    struct EchoBatchExecutor;

    impl BatchExecutor for EchoBatchExecutor {
        fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs> {
            let mut outputs: BatchOutputs = BatchOutputs::new();
            for packet in inputs.packets(&port_id("in")) {
                outputs.push(port_id("out"), packet.clone());
            }
            Ok(outputs)
        }
    }

    #[test]
    fn batch_inputs_preserve_port_order_and_packet_order() {
        let mut inputs: BatchInputs = BatchInputs::new();
        inputs.push(port_id("right"), packet(b"second"));
        inputs.push(port_id("left"), packet(b"first"));
        inputs.push(port_id("right"), packet(b"third"));

        assert_eq!(
            inputs
                .packets_by_port()
                .keys()
                .map(PortId::as_str)
                .collect::<Vec<_>>(),
            vec!["left", "right"]
        );
        assert_eq!(inputs.packets(&port_id("right")).len(), 2);
    }

    #[test]
    fn wasm_module_invokes_opaque_batch_executor() {
        let module: WasmModule = WasmModule::new(Box::new(EchoBatchExecutor));
        let mut inputs: BatchInputs = BatchInputs::new();
        inputs.push(port_id("in"), packet(b"payload"));

        let outputs: BatchOutputs = module.invoke(inputs).expect("batch should run");

        assert_eq!(outputs.packets(&port_id("out")).len(), 1);
        assert_eq!(
            outputs.packets(&port_id("out"))[0]
                .payload()
                .as_bytes()
                .expect("payload should contain bytes")
                .as_ref(),
            b"payload"
        );
    }

    #[test]
    fn batch_executor_accepts_empty_inputs() {
        let module: WasmModule = WasmModule::new(Box::new(EchoBatchExecutor));

        let outputs: BatchOutputs = module.invoke(BatchInputs::new()).expect("batch should run");

        assert!(outputs.packets_by_port().is_empty());
    }
}
