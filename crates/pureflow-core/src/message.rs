//! Message envelope and routing metadata types.

use bytes::Bytes;
use pureflow_types::{MessageId, NodeId, PortId, WorkflowId};
use serde_json::Value;

use crate::context::ExecutionMetadata;

/// Packet payload carried over runtime ports.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(not(feature = "arrow"), derive(Eq))]
pub enum PacketPayload {
    /// Ordinary byte payload.
    Bytes(Bytes),
    /// Control-plane payload for orchestration messages.
    Control(Value),
    /// Apache Arrow record batch payload.
    #[cfg(feature = "arrow")]
    Arrow(arrow_array::RecordBatch),
}

impl PacketPayload {
    /// Create a byte payload.
    #[must_use]
    pub fn bytes(value: impl Into<Bytes>) -> Self {
        Self::Bytes(value.into())
    }

    /// Create a control-plane payload.
    #[must_use]
    pub fn control(value: impl Into<Value>) -> Self {
        Self::Control(value.into())
    }

    /// Borrow the payload as bytes when this is a byte payload.
    #[must_use]
    pub const fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            Self::Control(_) => None,
            #[cfg(feature = "arrow")]
            Self::Arrow(_) => None,
        }
    }

    /// Borrow the payload as control data when this is a control payload.
    #[must_use]
    pub const fn as_control(&self) -> Option<&Value> {
        match self {
            Self::Bytes(_) => None,
            Self::Control(value) => Some(value),
            #[cfg(feature = "arrow")]
            Self::Arrow(_) => None,
        }
    }

    /// Borrow the payload as an Arrow record batch when this is an Arrow payload.
    #[cfg(feature = "arrow")]
    #[must_use]
    pub const fn as_arrow(&self) -> Option<&arrow_array::RecordBatch> {
        match self {
            Self::Bytes(_) | Self::Control(_) => None,
            Self::Arrow(batch) => Some(batch),
        }
    }
}

impl From<Bytes> for PacketPayload {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl From<Vec<u8>> for PacketPayload {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(Bytes::from(value))
    }
}

impl From<&'static [u8]> for PacketPayload {
    fn from(value: &'static [u8]) -> Self {
        Self::Bytes(Bytes::from_static(value))
    }
}

impl From<Value> for PacketPayload {
    fn from(value: Value) -> Self {
        Self::Control(value)
    }
}

#[cfg(feature = "arrow")]
impl From<arrow_array::RecordBatch> for PacketPayload {
    fn from(value: arrow_array::RecordBatch) -> Self {
        Self::Arrow(value)
    }
}

/// Node/port endpoint for a message envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEndpoint {
    node_id: NodeId,
    port_id: PortId,
}

impl MessageEndpoint {
    /// Create a message endpoint.
    #[must_use]
    pub const fn new(node_id: NodeId, port_id: PortId) -> Self {
        Self { node_id, port_id }
    }

    /// Node referenced by this endpoint.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port referenced by this endpoint.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }
}

/// Static routing metadata carried alongside a message payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRoute {
    source: Option<MessageEndpoint>,
    target: MessageEndpoint,
}

impl MessageRoute {
    /// Create routing metadata from an optional source to a required target.
    #[must_use]
    pub const fn new(source: Option<MessageEndpoint>, target: MessageEndpoint) -> Self {
        Self { source, target }
    }

    /// Upstream source endpoint, absent for externally injected messages.
    #[must_use]
    pub const fn source(&self) -> Option<&MessageEndpoint> {
        self.source.as_ref()
    }

    /// Downstream target endpoint.
    #[must_use]
    pub const fn target(&self) -> &MessageEndpoint {
        &self.target
    }
}

/// Metadata attached to every message envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMetadata {
    message_id: MessageId,
    workflow_id: WorkflowId,
    execution: ExecutionMetadata,
    route: MessageRoute,
}

impl MessageMetadata {
    /// Create metadata for one message envelope.
    #[must_use]
    pub const fn new(
        message_id: MessageId,
        workflow_id: WorkflowId,
        execution: ExecutionMetadata,
        route: MessageRoute,
    ) -> Self {
        Self {
            message_id,
            workflow_id,
            execution,
            route,
        }
    }

    /// Identifier for this message.
    #[must_use]
    pub const fn message_id(&self) -> &MessageId {
        &self.message_id
    }

    /// Workflow associated with this message.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Execution metadata associated with this message.
    #[must_use]
    pub const fn execution(&self) -> &ExecutionMetadata {
        &self.execution
    }

    /// Static route for this message.
    #[must_use]
    pub const fn route(&self) -> &MessageRoute {
        &self.route
    }
}

/// Runtime message envelope that keeps payloads separate from routing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEnvelope<P> {
    metadata: MessageMetadata,
    payload: P,
}

impl<P> MessageEnvelope<P> {
    /// Create a message envelope.
    #[must_use]
    pub const fn new(metadata: MessageMetadata, payload: P) -> Self {
        Self { metadata, payload }
    }

    /// Metadata that travels with the payload.
    #[must_use]
    pub const fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    /// Borrow the payload.
    #[must_use]
    pub const fn payload(&self) -> &P {
        &self.payload
    }

    /// Consume the envelope and return the payload.
    #[must_use]
    pub fn into_payload(self) -> P {
        self.payload
    }

    /// Transform the payload while preserving metadata.
    #[must_use]
    pub fn map_payload<Q>(self, f: impl FnOnce(P) -> Q) -> MessageEnvelope<Q> {
        MessageEnvelope {
            metadata: self.metadata,
            payload: f(self.payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_types::ExecutionId;
    use serde_json::json;

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

    fn execution() -> ExecutionMetadata {
        ExecutionMetadata::first_attempt(execution_id("run-1"))
    }

    #[test]
    fn message_envelope_keeps_payload_separate_from_metadata() {
        let target: MessageEndpoint = MessageEndpoint::new(node_id("consumer"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(None, target);
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution(), route);
        let envelope: MessageEnvelope<&str> = MessageEnvelope::new(metadata, "payload");
        let mapped: MessageEnvelope<usize> = envelope.map_payload(str::len);

        assert_eq!(mapped.payload(), &7);
        assert_eq!(mapped.metadata().message_id().as_str(), "msg-1");
        assert_eq!(
            mapped.metadata().route().target().node_id().as_str(),
            "consumer"
        );
    }

    #[test]
    fn packet_payload_bytes_clone_and_slice_without_copying_user_data() {
        let payload: PacketPayload = PacketPayload::bytes(Bytes::from_static(b"abcdef"));
        let cloned: PacketPayload = payload.clone();
        let sliced: Bytes = cloned
            .as_bytes()
            .expect("payload should contain bytes")
            .slice(1..4);

        assert_eq!(
            payload
                .as_bytes()
                .expect("payload should contain bytes")
                .as_ref(),
            b"abcdef"
        );
        assert!(payload.as_control().is_none());
        assert_eq!(sliced.as_ref(), b"bcd");
    }

    #[test]
    fn packet_payload_control_carries_structured_values() {
        let payload: PacketPayload = PacketPayload::control(json!({
            "command": "flush",
            "priority": 3,
        }));
        let control: &Value = payload
            .as_control()
            .expect("payload should contain control data");

        assert_eq!(control["command"], "flush");
        assert_eq!(control["priority"], 3);
        assert!(payload.as_bytes().is_none());
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn packet_payload_arrow_carries_record_batches() {
        use std::sync::Arc;

        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch: RecordBatch =
            RecordBatch::try_new(schema, vec![values]).expect("record batch should be valid");
        let payload: PacketPayload = PacketPayload::from(batch.clone());

        assert_eq!(payload.as_arrow(), Some(&batch));
        assert!(payload.as_bytes().is_none());
        assert!(payload.as_control().is_none());
    }
}
