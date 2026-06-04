//! Pureflow-owned port handles for the executor boundary.
//!
//! ## Fragment: executor-port-staging
//!
//! The executor contract is explicit about engine-owned inputs and outputs.
//! These handles now preserve declared port identity and can carry bounded
//! edge channels, but the node-facing surface remains Pureflow-owned. Runtime
//! code may use `asupersync` internally without making node implementations
//! depend on raw runtime channels or task context.
//!
//! ## Fragment: port-adapter-boundary
//!
//! The handles expose both non-blocking probes and async operations while
//! callers still depend on Pureflow port semantics rather than on the concrete
//! async runtime. In practice that means `asupersync` concepts such as task
//! context, send permits, and channel errors belong behind `PortsIn` and
//! `PortsOut`, with explicit Pureflow error and cancellation mapping at the
//! boundary.
//!
//! ## Fragment: output-reserve-commit
//!
//! Output sends use a two-phase reserve/commit shape even before the fully
//! async `Cx`-based API lands. Reserving capacity produces a Pureflow-owned
//! permit; committing enqueues the packet; dropping or aborting the permit
//! releases capacity without creating a ghost message. This mirrors the
//! `asupersync` channel contract while keeping runtime details hidden.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

use asupersync::{Cx, channel::mpsc, runtime::yield_now};
use pureflow_types::PortId;

use crate::message::{MessageEnvelope, PacketPayload};
use crate::{
    context::{CancellationToken, NodeContext},
    metadata::{
        MessageBoundaryKind, MessageBoundaryRecord, MetadataRecord, MetadataSink,
        QueuePortDirection, QueuePressureBoundaryKind, QueuePressureRecord,
    },
};

/// Default packet payload for the first channel-backed port surface.
pub type PortPacket = MessageEnvelope<PacketPayload>;

/// Error returned when an output port cannot accept a packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortSendError {
    /// The node does not declare the requested output port.
    UnknownPort {
        /// Port requested by the caller.
        port_id: PortId,
    },
    /// A downstream input has disconnected.
    Disconnected {
        /// Output port being sent through.
        port_id: PortId,
    },
    /// At least one bounded downstream edge is full.
    Full {
        /// Output port being sent through.
        port_id: PortId,
    },
    /// The send was cancelled before it could complete.
    Cancelled {
        /// Output port being sent through.
        port_id: PortId,
    },
    /// A validation policy rejected the packet before it entered the graph.
    Rejected {
        /// Output port being sent through.
        port_id: PortId,
        /// Human-readable rejection reason.
        reason: String,
    },
}

impl fmt::Display for PortSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPort { port_id } => {
                write!(f, "output port `{port_id}` is not declared")
            }
            Self::Disconnected { port_id } => {
                write!(f, "output port `{port_id}` is disconnected")
            }
            Self::Full { port_id } => write!(f, "output port `{port_id}` is full"),
            Self::Cancelled { port_id } => {
                write!(f, "output port `{port_id}` send cancelled")
            }
            Self::Rejected { port_id, reason } => {
                write!(f, "output port `{port_id}` rejected packet: {reason}")
            }
        }
    }
}

impl Error for PortSendError {}

/// Validator invoked before output packets enter downstream graph edges.
pub trait OutputPacketValidator: Send + Sync {
    /// Validate one output packet for the requested output port.
    ///
    /// # Errors
    ///
    /// Returns an error when the packet must not be sent.
    fn validate(&self, port_id: &PortId, packet: &PortPacket) -> Result<(), PortSendError>;
}

/// Error returned when an input port cannot provide a packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortRecvError {
    /// The node does not declare the requested input port.
    UnknownPort {
        /// Port requested by the caller.
        port_id: PortId,
    },
    /// All upstream senders for this input have disconnected.
    Disconnected {
        /// Input port being received from.
        port_id: PortId,
    },
    /// The receive was cancelled before it could complete.
    Cancelled {
        /// Input port being received from.
        port_id: PortId,
    },
}

impl fmt::Display for PortRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPort { port_id } => {
                write!(f, "input port `{port_id}` is not declared")
            }
            Self::Disconnected { port_id } => {
                write!(f, "input port `{port_id}` is disconnected")
            }
            Self::Cancelled { port_id } => {
                write!(f, "input port `{port_id}` receive cancelled")
            }
        }
    }
}

impl Error for PortRecvError {}

/// Channel-backed input endpoint for one declared input port.
#[derive(Debug)]
pub struct InputPortHandle {
    port_id: PortId,
    receivers: Vec<mpsc::Receiver<PortPacket>>,
}

enum InputPollResult {
    Packet(Box<PortPacket>),
    Pending,
    Closed,
    Cancelled,
}

impl InputPortHandle {
    /// Create an input handle with no connected upstream edge.
    #[must_use]
    pub const fn disconnected(port_id: PortId) -> Self {
        Self {
            port_id,
            receivers: Vec::new(),
        }
    }

    fn connected(port_id: PortId, receiver: mpsc::Receiver<PortPacket>) -> Self {
        Self {
            port_id,
            receivers: vec![receiver],
        }
    }

    fn append(&mut self, mut other: Self) {
        self.receivers.append(&mut other.receivers);
    }

    /// Declared input port identifier.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Number of upstream bounded edges connected to this input port.
    #[must_use]
    pub const fn connected_edge_count(&self) -> usize {
        self.receivers.len()
    }

    /// Capacity of the first connected upstream edge, if one exists.
    #[must_use]
    pub fn capacity(&self) -> Option<usize> {
        self.receivers.first().map(mpsc::Receiver::capacity)
    }

    fn total_capacity(&self) -> Option<usize> {
        if self.receivers.is_empty() {
            None
        } else {
            Some(self.receivers.iter().map(mpsc::Receiver::capacity).sum())
        }
    }

    fn queued_count(&self) -> usize {
        self.receivers.iter().map(mpsc::Receiver::len).sum()
    }

    fn try_recv(&mut self) -> Result<Option<PortPacket>, PortRecvError> {
        let mut disconnected_count: usize = 0;

        for receiver in &mut self.receivers {
            match receiver.try_recv() {
                Ok(packet) => return Ok(Some(packet)),
                Err(mpsc::RecvError::Empty) => {}
                Err(mpsc::RecvError::Disconnected) => {
                    disconnected_count += 1;
                }
                Err(mpsc::RecvError::Cancelled) => {
                    return Err(PortRecvError::Cancelled {
                        port_id: self.port_id.clone(),
                    });
                }
            }
        }

        if !self.receivers.is_empty() && disconnected_count == self.receivers.len() {
            return Err(PortRecvError::Disconnected {
                port_id: self.port_id.clone(),
            });
        }

        Ok(None)
    }

    async fn recv(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<PortPacket>, PortRecvError> {
        if self.receivers.is_empty() {
            return Ok(None);
        }

        let runtime_cx: Cx = Cx::current().unwrap_or_else(Cx::for_testing);
        std::future::poll_fn(|task_cx: &mut Context<'_>| {
            if cancellation.is_cancelled() {
                return Poll::Ready(Err(PortRecvError::Cancelled {
                    port_id: self.port_id.clone(),
                }));
            }

            let mut disconnected_count: usize = 0;
            for receiver in &mut self.receivers {
                match receiver.poll_recv(&runtime_cx, task_cx) {
                    Poll::Ready(Ok(packet)) => return Poll::Ready(Ok(Some(packet))),
                    Poll::Ready(Err(mpsc::RecvError::Disconnected)) => {
                        disconnected_count += 1;
                    }
                    Poll::Ready(Err(mpsc::RecvError::Cancelled)) => {
                        return Poll::Ready(Err(PortRecvError::Cancelled {
                            port_id: self.port_id.clone(),
                        }));
                    }
                    Poll::Ready(Err(mpsc::RecvError::Empty)) | Poll::Pending => {}
                }
            }

            if disconnected_count == self.receivers.len() {
                return Poll::Ready(Err(PortRecvError::Disconnected {
                    port_id: self.port_id.clone(),
                }));
            }

            Poll::Pending
        })
        .await
    }

    fn poll_any(&mut self, runtime_cx: &Cx, task_cx: &mut Context<'_>) -> InputPollResult {
        if self.receivers.is_empty() {
            return InputPollResult::Closed;
        }

        let mut disconnected_count: usize = 0;
        for receiver in &mut self.receivers {
            let poll_result: Poll<Result<PortPacket, mpsc::RecvError>> =
                receiver.poll_recv(runtime_cx, task_cx);
            match poll_result {
                Poll::Ready(Ok(packet)) => return InputPollResult::Packet(Box::new(packet)),
                Poll::Ready(Err(mpsc::RecvError::Disconnected)) => {
                    disconnected_count += 1;
                }
                Poll::Ready(Err(mpsc::RecvError::Cancelled)) => return InputPollResult::Cancelled,
                Poll::Ready(Err(mpsc::RecvError::Empty)) | Poll::Pending => {}
            }
        }

        if disconnected_count == self.receivers.len() {
            return InputPollResult::Closed;
        }

        InputPollResult::Pending
    }
}

/// Channel-backed output endpoint for one declared output port.
#[derive(Debug, Clone)]
pub struct OutputPortHandle {
    port_id: PortId,
    senders: Vec<mpsc::Sender<PortPacket>>,
}

impl OutputPortHandle {
    /// Create an output handle with no connected downstream edge.
    #[must_use]
    pub const fn disconnected(port_id: PortId) -> Self {
        Self {
            port_id,
            senders: Vec::new(),
        }
    }

    fn connected(port_id: PortId, sender: mpsc::Sender<PortPacket>) -> Self {
        Self {
            port_id,
            senders: vec![sender],
        }
    }

    fn append(&mut self, mut other: Self) {
        self.senders.append(&mut other.senders);
    }

    /// Declared output port identifier.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Number of downstream bounded edges connected to this output port.
    #[must_use]
    pub const fn connected_edge_count(&self) -> usize {
        self.senders.len()
    }

    /// Capacity of the first connected downstream edge, if one exists.
    #[must_use]
    pub fn capacity(&self) -> Option<usize> {
        self.senders.first().map(mpsc::Sender::capacity)
    }

    fn total_capacity(&self) -> Option<usize> {
        if self.senders.is_empty() {
            None
        } else {
            Some(self.senders.iter().map(mpsc::Sender::capacity).sum())
        }
    }

    fn try_reserve(
        &self,
        metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
        validator: Option<Arc<dyn OutputPacketValidator>>,
        context: Option<&NodeContext>,
    ) -> Result<OutputPortSendPermit<'_>, PortSendError> {
        record_output_queue_pressure(
            metadata_sink.as_ref(),
            context,
            self,
            QueuePressureBoundaryKind::ReserveAttempted,
        );
        let split_senders: Option<(&mpsc::Sender<PortPacket>, &[mpsc::Sender<PortPacket>])> =
            self.senders.split_last();
        let Some((last_sender, leading_senders)): Option<(
            &mpsc::Sender<PortPacket>,
            &[mpsc::Sender<PortPacket>],
        )> = split_senders
        else {
            record_output_queue_pressure(
                metadata_sink.as_ref(),
                context,
                self,
                QueuePressureBoundaryKind::ReserveReady,
            );
            return Ok(OutputPortSendPermit {
                port_id: self.port_id.clone(),
                permits: Vec::new(),
                metadata_sink,
                validator,
                context: context.cloned(),
                connected_edge_count: self.connected_edge_count(),
                capacity: self.total_capacity(),
            });
        };

        let mut permits: Vec<mpsc::SendPermit<'_, PortPacket>> =
            Vec::with_capacity(self.senders.len());

        for sender in leading_senders {
            match sender.try_reserve() {
                Ok(permit) => permits.push(permit),
                Err(err) => {
                    if matches!(err, mpsc::SendError::Full(())) {
                        record_output_queue_pressure(
                            metadata_sink.as_ref(),
                            context,
                            self,
                            QueuePressureBoundaryKind::ReserveFull,
                        );
                    }
                    return Err(self.map_send_error(err));
                }
            }
        }

        match last_sender.try_reserve() {
            Ok(permit) => {
                permits.push(permit);
                record_output_queue_pressure(
                    metadata_sink.as_ref(),
                    context,
                    self,
                    QueuePressureBoundaryKind::ReserveReady,
                );
                Ok(OutputPortSendPermit {
                    port_id: self.port_id.clone(),
                    permits,
                    metadata_sink,
                    validator,
                    context: context.cloned(),
                    connected_edge_count: self.connected_edge_count(),
                    capacity: self.total_capacity(),
                })
            }
            Err(err) => {
                if matches!(err, mpsc::SendError::Full(())) {
                    record_output_queue_pressure(
                        metadata_sink.as_ref(),
                        context,
                        self,
                        QueuePressureBoundaryKind::ReserveFull,
                    );
                }
                Err(self.map_send_error(err))
            }
        }
    }

    fn map_send_error(&self, err: mpsc::SendError<()>) -> PortSendError {
        match err {
            mpsc::SendError::Disconnected(()) => PortSendError::Disconnected {
                port_id: self.port_id.clone(),
            },
            mpsc::SendError::Cancelled(()) => PortSendError::Cancelled {
                port_id: self.port_id.clone(),
            },
            mpsc::SendError::Full(()) => PortSendError::Full {
                port_id: self.port_id.clone(),
            },
        }
    }

    async fn reserve(
        &self,
        cancellation: &CancellationToken,
        metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
        validator: Option<Arc<dyn OutputPacketValidator>>,
        context: Option<&NodeContext>,
    ) -> Result<OutputPortSendPermit<'_>, PortSendError> {
        loop {
            if cancellation.is_cancelled() {
                return Err(PortSendError::Cancelled {
                    port_id: self.port_id.clone(),
                });
            }

            match self.try_reserve(metadata_sink.clone(), validator.clone(), context) {
                Ok(permit) => return Ok(permit),
                Err(PortSendError::Full { .. }) => yield_now().await,
                Err(err) => return Err(err),
            }
        }
    }
}

/// Reserved output capacity for one declared output port.
#[must_use = "PortSendPermit must be committed with send() or explicitly aborted"]
pub struct PortSendPermit<'a> {
    inner: OutputPortSendPermit<'a>,
}

impl PortSendPermit<'_> {
    /// Commit the reserved capacity and enqueue the packet.
    ///
    /// # Errors
    ///
    /// Returns an error if output validation rejects the packet.
    pub fn send(self, packet: PortPacket) -> Result<(), PortSendError> {
        self.inner.send(packet)
    }

    /// Release the reserved capacity without enqueueing a packet.
    pub fn abort(self) {
        self.inner.abort();
    }
}

struct OutputPortSendPermit<'a> {
    port_id: PortId,
    permits: Vec<mpsc::SendPermit<'a, PortPacket>>,
    metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
    validator: Option<Arc<dyn OutputPacketValidator>>,
    context: Option<NodeContext>,
    connected_edge_count: usize,
    capacity: Option<usize>,
}

impl OutputPortSendPermit<'_> {
    fn send(mut self, packet: PortPacket) -> Result<(), PortSendError> {
        if let Some(validator) = self.validator.as_ref()
            && let Err(err) = validator.validate(&self.port_id, &packet)
        {
            self.abort();
            return Err(err);
        }

        let boundary_kind: MessageBoundaryKind = if self.permits.is_empty() {
            MessageBoundaryKind::Dropped
        } else {
            MessageBoundaryKind::Enqueued
        };
        if let Some(metadata_sink) = self.metadata_sink.as_ref() {
            let queue_kind: QueuePressureBoundaryKind = if self.permits.is_empty() {
                QueuePressureBoundaryKind::SendDropped
            } else {
                QueuePressureBoundaryKind::SendCommitted
            };
            let record: MetadataRecord = MetadataRecord::QueuePressure(QueuePressureRecord::new(
                self.context.clone(),
                QueuePortDirection::Output,
                self.port_id.clone(),
                queue_kind,
                self.connected_edge_count,
                self.capacity,
                None,
            ));
            let _ = metadata_sink.record(&record);
            let record: MetadataRecord = MetadataRecord::Message(MessageBoundaryRecord::new(
                boundary_kind,
                packet.metadata().clone(),
            ));
            let _ = metadata_sink.record(&record);
        }

        let last_permit: Option<mpsc::SendPermit<'_, PortPacket>> = self.permits.pop();
        let last_permit: mpsc::SendPermit<'_, PortPacket> = match last_permit {
            Some(permit) => permit,
            None => return Ok(()),
        };
        let leading_permits: Vec<mpsc::SendPermit<'_, PortPacket>> = self.permits;

        for permit in leading_permits {
            permit.send(packet.clone());
        }
        last_permit.send(packet);
        Ok(())
    }

    fn abort(self) {
        for permit in self.permits {
            permit.abort();
        }
    }
}

/// Create one bounded edge channel between an output port and an input port.
#[must_use]
pub fn bounded_edge_channel(
    output_port_id: PortId,
    input_port_id: PortId,
    capacity: NonZeroUsize,
) -> (OutputPortHandle, InputPortHandle) {
    let (sender, receiver): (mpsc::Sender<PortPacket>, mpsc::Receiver<PortPacket>) =
        mpsc::channel(capacity.get());
    (
        OutputPortHandle::connected(output_port_id, sender),
        InputPortHandle::connected(input_port_id, receiver),
    )
}

/// Declared input ports available to a node execution boundary.
#[derive(Default)]
pub struct PortsIn {
    port_ids: Vec<PortId>,
    ports: Vec<InputPortHandle>,
    metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
    context: Option<NodeContext>,
}

impl PortsIn {
    /// Create input handles with declared port identifiers and no channels.
    #[must_use]
    pub fn new(port_ids: impl Into<Vec<PortId>>) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        Self::from_handles(port_ids, Vec::new())
    }

    /// Create input handles from declared ports and connected channel handles.
    #[must_use]
    pub fn from_handles(
        port_ids: impl Into<Vec<PortId>>,
        handles: impl Into<Vec<InputPortHandle>>,
    ) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        let mut by_port: BTreeMap<PortId, InputPortHandle> = BTreeMap::new();

        for handle in handles.into() {
            let port_id: PortId = handle.port_id().clone();
            if let Some(existing) = by_port.get_mut(&port_id) {
                existing.append(handle);
            } else {
                by_port.insert(port_id, handle);
            }
        }

        let mut ports: Vec<InputPortHandle> = Vec::with_capacity(port_ids.len());
        for port_id in &port_ids {
            let handle: InputPortHandle = by_port
                .remove(port_id)
                .unwrap_or_else(|| InputPortHandle::disconnected(port_id.clone()));
            ports.push(handle);
        }

        Self {
            port_ids,
            ports,
            metadata_sink: None,
            context: None,
        }
    }

    /// Attach a metadata sink for receive-side observations.
    #[must_use]
    pub fn with_metadata_sink(
        mut self,
        metadata_sink: Arc<dyn MetadataSink + Send + Sync>,
    ) -> Self {
        self.metadata_sink = Some(metadata_sink);
        self
    }

    /// Attach node context for receive-side queue observations.
    #[must_use]
    pub fn with_node_context(mut self, context: NodeContext) -> Self {
        self.context = Some(context);
        self
    }

    /// Declared input port identifiers for this node.
    #[must_use]
    pub fn port_ids(&self) -> &[PortId] {
        &self.port_ids
    }

    /// Return whether this node currently has no declared inputs.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.port_ids.is_empty()
    }

    /// Number of connected upstream edges for a declared input port.
    #[must_use]
    pub fn connected_edge_count(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&InputPortHandle| port.port_id() == port_id)
            .map(InputPortHandle::connected_edge_count)
    }

    /// Capacity of the first connected upstream edge for a declared input port.
    #[must_use]
    pub fn capacity(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&InputPortHandle| port.port_id() == port_id)
            .and_then(InputPortHandle::capacity)
    }

    /// Try to receive one packet from a declared input port without blocking.
    ///
    /// Returns `Ok(None)` when the port is declared but no packet is currently
    /// queued. Use [`Self::recv`] to wait asynchronously.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared or all upstream senders have
    /// disconnected.
    pub fn try_recv(&mut self, port_id: &PortId) -> Result<Option<PortPacket>, PortRecvError> {
        let port: &mut InputPortHandle = self
            .ports
            .iter_mut()
            .find(|port: &&mut InputPortHandle| port.port_id() == port_id)
            .ok_or_else(|| PortRecvError::UnknownPort {
                port_id: port_id.clone(),
            })?;
        Self::record_input_queue_pressure(
            self.metadata_sink.as_ref(),
            self.context.as_ref(),
            port,
            QueuePressureBoundaryKind::ReceiveAttempted,
        );
        let packet: Option<PortPacket> = match port.try_recv() {
            Ok(packet) => packet,
            Err(PortRecvError::Disconnected { .. }) => {
                Self::record_input_queue_pressure(
                    self.metadata_sink.as_ref(),
                    self.context.as_ref(),
                    port,
                    QueuePressureBoundaryKind::ReceiveClosed,
                );
                return Err(PortRecvError::Disconnected {
                    port_id: port_id.clone(),
                });
            }
            Err(err) => return Err(err),
        };
        match packet.as_ref() {
            Some(packet) => {
                Self::record_input_queue_pressure(
                    self.metadata_sink.as_ref(),
                    self.context.as_ref(),
                    port,
                    QueuePressureBoundaryKind::ReceiveReady,
                );
                Self::record_message_boundary(
                    self.metadata_sink.as_ref(),
                    MessageBoundaryKind::Dequeued,
                    packet.metadata(),
                );
            }
            None => Self::record_input_queue_pressure(
                self.metadata_sink.as_ref(),
                self.context.as_ref(),
                port,
                QueuePressureBoundaryKind::ReceiveEmpty,
            ),
        }
        Ok(packet)
    }

    /// Receive one packet from a declared input port, waiting asynchronously.
    ///
    /// Returns `Ok(None)` when the port is declared but has no connected
    /// upstream edges. Connected ports wait until a packet arrives, every
    /// upstream edge disconnects, or cancellation is observed.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, all upstream senders have
    /// disconnected, or cancellation is observed.
    pub async fn recv(
        &mut self,
        port_id: &PortId,
        cancellation: &CancellationToken,
    ) -> Result<Option<PortPacket>, PortRecvError> {
        let port: &mut InputPortHandle = self
            .ports
            .iter_mut()
            .find(|port: &&mut InputPortHandle| port.port_id() == port_id)
            .ok_or_else(|| PortRecvError::UnknownPort {
                port_id: port_id.clone(),
            })?;
        Self::record_input_queue_pressure(
            self.metadata_sink.as_ref(),
            self.context.as_ref(),
            port,
            QueuePressureBoundaryKind::ReceiveAttempted,
        );
        let packet: Option<PortPacket> = match port.recv(cancellation).await {
            Ok(packet) => packet,
            Err(PortRecvError::Disconnected { .. }) => {
                Self::record_input_queue_pressure(
                    self.metadata_sink.as_ref(),
                    self.context.as_ref(),
                    port,
                    QueuePressureBoundaryKind::ReceiveClosed,
                );
                return Err(PortRecvError::Disconnected {
                    port_id: port_id.clone(),
                });
            }
            Err(err) => return Err(err),
        };
        match packet.as_ref() {
            Some(packet) => {
                Self::record_input_queue_pressure(
                    self.metadata_sink.as_ref(),
                    self.context.as_ref(),
                    port,
                    QueuePressureBoundaryKind::ReceiveReady,
                );
                Self::record_message_boundary(
                    self.metadata_sink.as_ref(),
                    MessageBoundaryKind::Dequeued,
                    packet.metadata(),
                );
            }
            None => Self::record_input_queue_pressure(
                self.metadata_sink.as_ref(),
                self.context.as_ref(),
                port,
                QueuePressureBoundaryKind::ReceiveEmpty,
            ),
        }
        Ok(packet)
    }

    /// Receive one packet from any declared input port, waiting asynchronously.
    ///
    /// Returns the port that produced the packet with the packet itself.
    /// Returns `Ok(None)` when the node has no declared inputs or every input
    /// is closed or disconnected.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation is observed while waiting.
    pub async fn recv_any(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<(PortId, PortPacket)>, PortRecvError> {
        if self.ports.is_empty() {
            return Ok(None);
        }

        let metadata_sink: Option<&Arc<dyn MetadataSink + Send + Sync>> =
            self.metadata_sink.as_ref();
        let context: Option<&NodeContext> = self.context.as_ref();
        let runtime_cx: Cx = Cx::current().unwrap_or_else(Cx::for_testing);
        for port in &self.ports {
            Self::record_input_queue_pressure(
                metadata_sink,
                context,
                port,
                QueuePressureBoundaryKind::ReceiveAttempted,
            );
        }
        std::future::poll_fn(|task_cx: &mut Context<'_>| {
            if cancellation.is_cancelled() {
                return self.ports.first().map_or(
                    Poll::Ready(Ok(None)),
                    |first_port: &InputPortHandle| {
                        Poll::Ready(Err(PortRecvError::Cancelled {
                            port_id: first_port.port_id().clone(),
                        }))
                    },
                );
            }

            let mut closed_port_count: usize = 0;
            for port in &mut self.ports {
                match port.poll_any(&runtime_cx, task_cx) {
                    InputPollResult::Packet(packet) => {
                        Self::record_input_queue_pressure(
                            metadata_sink,
                            context,
                            port,
                            QueuePressureBoundaryKind::ReceiveReady,
                        );
                        Self::record_message_boundary(
                            metadata_sink,
                            MessageBoundaryKind::Dequeued,
                            packet.metadata(),
                        );
                        return Poll::Ready(Ok(Some((port.port_id().clone(), *packet))));
                    }
                    InputPollResult::Closed => {
                        closed_port_count += 1;
                        Self::record_input_queue_pressure(
                            metadata_sink,
                            context,
                            port,
                            QueuePressureBoundaryKind::ReceiveClosed,
                        );
                    }
                    InputPollResult::Cancelled => {
                        return Poll::Ready(Err(PortRecvError::Cancelled {
                            port_id: port.port_id().clone(),
                        }));
                    }
                    InputPollResult::Pending => {}
                }
            }

            if closed_port_count == self.ports.len() {
                return Poll::Ready(Ok(None));
            }

            Poll::Pending
        })
        .await
    }

    fn record_message_boundary(
        metadata_sink: Option<&Arc<dyn MetadataSink + Send + Sync>>,
        kind: MessageBoundaryKind,
        metadata: &crate::message::MessageMetadata,
    ) {
        let Some(metadata_sink): Option<&Arc<dyn MetadataSink + Send + Sync>> = metadata_sink
        else {
            return;
        };

        let record: MetadataRecord =
            MetadataRecord::Message(MessageBoundaryRecord::new(kind, metadata.clone()));
        let _ = metadata_sink.record(&record);
    }

    fn record_input_queue_pressure(
        metadata_sink: Option<&Arc<dyn MetadataSink + Send + Sync>>,
        context: Option<&NodeContext>,
        port: &InputPortHandle,
        kind: QueuePressureBoundaryKind,
    ) {
        let Some(metadata_sink): Option<&Arc<dyn MetadataSink + Send + Sync>> = metadata_sink
        else {
            return;
        };

        let record: MetadataRecord = MetadataRecord::QueuePressure(QueuePressureRecord::new(
            context.cloned(),
            QueuePortDirection::Input,
            port.port_id().clone(),
            kind,
            port.connected_edge_count(),
            port.total_capacity(),
            Some(port.queued_count()),
        ));
        let _ = metadata_sink.record(&record);
    }
}

/// Declared output ports available to a node execution boundary.
#[derive(Clone, Default)]
pub struct PortsOut {
    port_ids: Vec<PortId>,
    ports: Vec<OutputPortHandle>,
    metadata_sink: Option<Arc<dyn MetadataSink + Send + Sync>>,
    validator: Option<Arc<dyn OutputPacketValidator>>,
    context: Option<NodeContext>,
}

impl PortsOut {
    /// Create output handles with declared port identifiers and no channels.
    #[must_use]
    pub fn new(port_ids: impl Into<Vec<PortId>>) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        Self::from_handles(port_ids, Vec::new())
    }

    /// Create output handles from declared ports and connected channel handles.
    #[must_use]
    pub fn from_handles(
        port_ids: impl Into<Vec<PortId>>,
        handles: impl Into<Vec<OutputPortHandle>>,
    ) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        let mut by_port: BTreeMap<PortId, OutputPortHandle> = BTreeMap::new();

        for handle in handles.into() {
            let port_id: PortId = handle.port_id().clone();
            if let Some(existing) = by_port.get_mut(&port_id) {
                existing.append(handle);
            } else {
                by_port.insert(port_id, handle);
            }
        }

        let mut ports: Vec<OutputPortHandle> = Vec::with_capacity(port_ids.len());
        for port_id in &port_ids {
            let handle: OutputPortHandle = by_port
                .remove(port_id)
                .unwrap_or_else(|| OutputPortHandle::disconnected(port_id.clone()));
            ports.push(handle);
        }

        Self {
            port_ids,
            ports,
            metadata_sink: None,
            validator: None,
            context: None,
        }
    }

    /// Attach a metadata sink for send-side observations.
    #[must_use]
    pub fn with_metadata_sink(
        mut self,
        metadata_sink: Arc<dyn MetadataSink + Send + Sync>,
    ) -> Self {
        self.metadata_sink = Some(metadata_sink);
        self
    }

    /// Attach node context for send-side queue observations.
    #[must_use]
    pub fn with_node_context(mut self, context: NodeContext) -> Self {
        self.context = Some(context);
        self
    }

    /// Attach a validator that runs before output packets enter graph edges.
    #[must_use]
    pub fn with_output_validator(mut self, validator: Arc<dyn OutputPacketValidator>) -> Self {
        self.validator = Some(validator);
        self
    }

    /// Declared output port identifiers for this node.
    #[must_use]
    pub fn port_ids(&self) -> &[PortId] {
        &self.port_ids
    }

    /// Return whether this node currently has no declared outputs.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.port_ids.is_empty()
    }

    /// Number of connected downstream edges for a declared output port.
    #[must_use]
    pub fn connected_edge_count(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .map(OutputPortHandle::connected_edge_count)
    }

    /// Capacity of the first connected downstream edge for a declared output port.
    #[must_use]
    pub fn capacity(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .and_then(OutputPortHandle::capacity)
    }

    /// Try to send one packet through a declared output port without blocking.
    ///
    /// Unconnected declared output ports accept and drop packets. That keeps
    /// early scaffold nodes simple while later beads define explicit fan-out
    /// and disconnected-edge policy. Connected sends reserve capacity before
    /// committing the packet, so cancellation or drop between those phases
    /// releases the reserved slots instead of creating partial messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, a downstream receiver has
    /// disconnected, or a bounded downstream edge is full.
    pub fn try_send(&self, port_id: &PortId, packet: PortPacket) -> Result<(), PortSendError> {
        self.try_reserve(port_id)?.send(packet)
    }

    /// Send one packet through a declared output port, waiting asynchronously
    /// for bounded downstream capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, a downstream receiver has
    /// disconnected, or cancellation is observed.
    pub async fn send(
        &self,
        port_id: &PortId,
        packet: PortPacket,
        cancellation: &CancellationToken,
    ) -> Result<(), PortSendError> {
        self.reserve(port_id, cancellation).await?.send(packet)
    }

    /// Try to reserve output capacity without committing a packet.
    ///
    /// Dropping the returned permit releases all reserved downstream slots.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, a downstream receiver has
    /// disconnected, or a bounded downstream edge is full.
    pub fn try_reserve(&self, port_id: &PortId) -> Result<PortSendPermit<'_>, PortSendError> {
        let port: &OutputPortHandle = self
            .ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .ok_or_else(|| PortSendError::UnknownPort {
                port_id: port_id.clone(),
            })?;
        port.try_reserve(
            self.metadata_sink.clone(),
            self.validator.clone(),
            self.context.as_ref(),
        )
        .map(|inner: OutputPortSendPermit<'_>| PortSendPermit { inner })
    }

    /// Reserve output capacity asynchronously without committing a packet.
    ///
    /// Dropping the returned permit releases all reserved downstream slots.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, a downstream receiver has
    /// disconnected, or cancellation is observed.
    pub async fn reserve(
        &self,
        port_id: &PortId,
        cancellation: &CancellationToken,
    ) -> Result<PortSendPermit<'_>, PortSendError> {
        let port: &OutputPortHandle = self
            .ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .ok_or_else(|| PortSendError::UnknownPort {
                port_id: port_id.clone(),
            })?;
        port.reserve(
            cancellation,
            self.metadata_sink.clone(),
            self.validator.clone(),
            self.context.as_ref(),
        )
        .await
        .map(|inner: OutputPortSendPermit<'_>| PortSendPermit { inner })
    }
}

fn record_output_queue_pressure(
    metadata_sink: Option<&Arc<dyn MetadataSink + Send + Sync>>,
    context: Option<&NodeContext>,
    port: &OutputPortHandle,
    kind: QueuePressureBoundaryKind,
) {
    let Some(metadata_sink): Option<&Arc<dyn MetadataSink + Send + Sync>> = metadata_sink else {
        return;
    };

    let record: MetadataRecord = MetadataRecord::QueuePressure(QueuePressureRecord::new(
        context.cloned(),
        QueuePortDirection::Output,
        port.port_id().clone(),
        kind,
        port.connected_edge_count(),
        port.total_capacity(),
        None,
    ));
    let _ = metadata_sink.record(&record);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use asupersync::runtime::{Runtime, RuntimeBuilder};
    use pureflow_types::{ExecutionId, MessageId, NodeId, WorkflowId};

    use crate::{
        context::{CancellationRequest, ExecutionMetadata},
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };

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

    fn packet(value: &[u8]) -> PortPacket {
        let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route);

        MessageEnvelope::new(metadata, PacketPayload::from(value.to_vec()))
    }

    fn block_on_port<F: Future>(future: F) -> F::Output {
        let runtime: Runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(future)
    }

    #[derive(Debug, Default)]
    struct RecordingMetadataSink {
        records: Mutex<Vec<MetadataRecord>>,
    }

    impl RecordingMetadataSink {
        fn records(&self) -> Vec<MetadataRecord> {
            self.records
                .lock()
                .expect("metadata sink lock should not be poisoned")
                .clone()
        }
    }

    impl MetadataSink for RecordingMetadataSink {
        fn record(&self, record: &MetadataRecord) -> crate::Result<()> {
            self.records
                .lock()
                .expect("metadata sink lock should not be poisoned")
                .push(record.clone());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct RejectingOutputValidator;

    impl OutputPacketValidator for RejectingOutputValidator {
        fn validate(&self, port_id: &PortId, _packet: &PortPacket) -> Result<(), PortSendError> {
            Err(PortSendError::Rejected {
                port_id: port_id.clone(),
                reason: "contract mismatch".to_owned(),
            })
        }
    }

    #[test]
    fn ports_preserve_declared_port_order() {
        let inputs: PortsIn = PortsIn::new(vec![port_id("left"), port_id("right")]);
        let outputs: PortsOut = PortsOut::new(vec![port_id("out")]);

        assert_eq!(
            inputs
                .port_ids()
                .iter()
                .map(PortId::as_str)
                .collect::<Vec<_>>(),
            vec!["left", "right"]
        );
        assert_eq!(
            outputs
                .port_ids()
                .iter()
                .map(PortId::as_str)
                .collect::<Vec<_>>(),
            vec!["out"]
        );
    }

    #[test]
    fn bounded_edge_channel_enforces_capacity() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        outputs
            .try_send(&port_id("out"), packet(b"first"))
            .expect("first packet should fit");
        let err: PortSendError = outputs
            .try_send(&port_id("out"), packet(b"second"))
            .expect_err("second packet should hit bounded capacity");

        assert_eq!(
            err,
            PortSendError::Full {
                port_id: port_id("out")
            }
        );
        assert_eq!(outputs.capacity(&port_id("out")), Some(1));
        assert_eq!(inputs.capacity(&port_id("in")), Some(1));

        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("packet should be queued");

        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"first"
        );
        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("empty receive should not fail")
                .is_none()
        );
    }

    #[test]
    fn reserved_output_capacity_commits_on_send() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        let permit: PortSendPermit<'_> = outputs
            .try_reserve(&port_id("out"))
            .expect("reservation should succeed");
        let err: PortSendError = outputs
            .try_send(&port_id("out"), packet(b"blocked"))
            .expect_err("reserved capacity should block another send");

        assert_eq!(
            err,
            PortSendError::Full {
                port_id: port_id("out")
            }
        );

        permit
            .send(packet(b"committed"))
            .expect("reserved packet should pass validation");

        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("committed packet should be queued");
        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"committed"
        );
    }

    #[test]
    fn dropped_output_permit_releases_capacity_without_message() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        let permit: PortSendPermit<'_> = outputs
            .try_reserve(&port_id("out"))
            .expect("reservation should succeed");
        drop(permit);

        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("dropped permit should not disconnect")
                .is_none()
        );

        outputs
            .try_send(&port_id("out"), packet(b"after-drop"))
            .expect("dropped permit should release capacity");
        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("new packet should be queued");

        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"after-drop"
        );
    }

    #[test]
    fn aborted_output_permit_releases_capacity_without_message() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        outputs
            .try_reserve(&port_id("out"))
            .expect("reservation should succeed")
            .abort();

        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("aborted permit should not disconnect")
                .is_none()
        );
        outputs
            .try_send(&port_id("out"), packet(b"after-abort"))
            .expect("aborted permit should release capacity");
    }

    #[test]
    fn send_and_recv_emit_message_boundary_metadata() {
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn =
            PortsIn::from_handles([port_id("in")], [input]).with_metadata_sink(sink.clone());
        let outputs: PortsOut =
            PortsOut::from_handles([port_id("out")], [output]).with_metadata_sink(sink.clone());

        outputs
            .try_send(&port_id("out"), packet(b"boundary"))
            .expect("send should succeed");
        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("packet should be queued");

        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"boundary"
        );
        assert_eq!(
            sink.records()
                .into_iter()
                .filter_map(|record| match record {
                    MetadataRecord::Message(message) => Some(message.kind()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![MessageBoundaryKind::Enqueued, MessageBoundaryKind::Dequeued]
        );
    }

    #[test]
    fn send_and_recv_emit_queue_pressure_metadata() {
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn =
            PortsIn::from_handles([port_id("in")], [input]).with_metadata_sink(sink.clone());
        let outputs: PortsOut =
            PortsOut::from_handles([port_id("out")], [output]).with_metadata_sink(sink.clone());

        outputs
            .try_send(&port_id("out"), packet(b"boundary"))
            .expect("send should succeed");
        let _received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("packet should be queued");

        let queue_records: Vec<QueuePressureRecord> = sink
            .records()
            .into_iter()
            .filter_map(|record: MetadataRecord| match record {
                MetadataRecord::QueuePressure(queue) => Some(queue),
                _ => None,
            })
            .collect();
        assert_eq!(
            queue_records
                .iter()
                .map(QueuePressureRecord::kind)
                .collect::<Vec<_>>(),
            vec![
                QueuePressureBoundaryKind::ReserveAttempted,
                QueuePressureBoundaryKind::ReserveReady,
                QueuePressureBoundaryKind::SendCommitted,
                QueuePressureBoundaryKind::ReceiveAttempted,
                QueuePressureBoundaryKind::ReceiveReady,
            ]
        );
        let reserve_attempt: &QueuePressureRecord = queue_records
            .iter()
            .find(|record: &&QueuePressureRecord| {
                record.kind() == QueuePressureBoundaryKind::ReserveAttempted
            })
            .expect("reserve attempt should be recorded");
        let receive_attempt: &QueuePressureRecord = queue_records
            .iter()
            .find(|record: &&QueuePressureRecord| {
                record.kind() == QueuePressureBoundaryKind::ReceiveAttempted
            })
            .expect("receive attempt should be recorded");
        let receive_ready: &QueuePressureRecord = queue_records
            .iter()
            .find(|record: &&QueuePressureRecord| {
                record.kind() == QueuePressureBoundaryKind::ReceiveReady
            })
            .expect("receive ready should be recorded");

        assert_eq!(reserve_attempt.capacity(), Some(1));
        assert_eq!(receive_attempt.queued_count(), Some(1));
        assert_eq!(receive_ready.queued_count(), Some(0));
    }

    #[test]
    fn unconnected_output_records_message_drop() {
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let outputs: PortsOut = PortsOut::new([port_id("out")]).with_metadata_sink(sink.clone());

        outputs
            .try_send(&port_id("out"), packet(b"dropped"))
            .expect("unconnected output should accept and drop packets");

        assert_eq!(
            sink.records()
                .into_iter()
                .filter_map(|record| match record {
                    MetadataRecord::Message(message) => Some(message.kind()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![MessageBoundaryKind::Dropped]
        );
    }

    #[test]
    fn async_send_and_recv_round_trip() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);
        let cancellation: CancellationToken = CancellationToken::active();

        let received: PortPacket = block_on_port(async {
            outputs
                .send(&port_id("out"), packet(b"async"), &cancellation)
                .await
                .expect("async send should succeed");
            inputs
                .recv(&port_id("in"), &cancellation)
                .await
                .expect("async receive should succeed")
                .expect("packet should be available")
        });

        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"async"
        );
    }

    #[test]
    fn async_reserve_commits_after_capacity_is_available() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);
        let cancellation: CancellationToken = CancellationToken::active();

        outputs
            .try_send(&port_id("out"), packet(b"queued"))
            .expect("first packet should fill the edge");
        let queued: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("queued packet should be present");
        assert_eq!(
            queued
                .payload()
                .as_bytes()
                .expect("queued packet should contain bytes")
                .as_ref(),
            b"queued"
        );

        block_on_port(async {
            outputs
                .reserve(&port_id("out"), &cancellation)
                .await
                .expect("capacity should be available")
                .send(packet(b"reserved"))
                .expect("reserved packet should pass validation");
        });

        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("reserved packet should be queued");
        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"reserved"
        );
    }

    #[test]
    fn async_recv_reports_disconnected_after_sender_drop() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let cancellation: CancellationToken = CancellationToken::active();
        drop(output);

        let err: PortRecvError = block_on_port(async {
            inputs
                .recv(&port_id("in"), &cancellation)
                .await
                .expect_err("disconnected input should fail")
        });

        assert_eq!(
            err,
            PortRecvError::Disconnected {
                port_id: port_id("in")
            }
        );
    }

    #[test]
    fn async_port_operations_observe_pre_cancelled_tokens() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);
        let cancellation: CancellationToken =
            CancellationToken::cancelled(CancellationRequest::new("test shutdown"));

        let recv_err: PortRecvError = block_on_port(async {
            inputs
                .recv(&port_id("in"), &cancellation)
                .await
                .expect_err("cancelled receive should fail")
        });
        let send_err: PortSendError = block_on_port(async {
            outputs
                .send(&port_id("out"), packet(b"cancelled"), &cancellation)
                .await
                .expect_err("cancelled send should fail")
        });

        assert_eq!(
            recv_err,
            PortRecvError::Cancelled {
                port_id: port_id("in")
            }
        );
        assert_eq!(
            send_err,
            PortSendError::Cancelled {
                port_id: port_id("out")
            }
        );
    }

    #[test]
    fn output_validator_rejects_before_enqueueing_packet() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output])
            .with_output_validator(Arc::new(RejectingOutputValidator));

        let err: PortSendError = outputs
            .try_send(&port_id("out"), packet(b"rejected"))
            .expect_err("validator should reject the packet");

        assert_eq!(
            err,
            PortSendError::Rejected {
                port_id: port_id("out"),
                reason: "contract mismatch".to_owned()
            }
        );
        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("receive should succeed")
                .is_none()
        );
    }

    #[test]
    fn recv_any_returns_first_ready_input_packet() {
        let (left_output, left_input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("left-out"), port_id("left"), NonZeroUsize::MIN);
        let (right_output, right_input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("right-out"), port_id("right"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles(
            [port_id("left"), port_id("right")],
            [left_input, right_input],
        );
        let right_outputs: PortsOut =
            PortsOut::from_handles([port_id("right-out")], [right_output]);
        let cancellation: CancellationToken = CancellationToken::active();
        let _left_output: OutputPortHandle = left_output;

        right_outputs
            .try_send(&port_id("right-out"), packet(b"right-ready"))
            .expect("right packet should queue");

        let (ready_port, received): (PortId, PortPacket) = block_on_port(async {
            inputs
                .recv_any(&cancellation)
                .await
                .expect("recv_any should succeed")
                .expect("one input should be ready")
        });

        assert_eq!(ready_port, port_id("right"));
        assert_eq!(
            received
                .payload()
                .as_bytes()
                .expect("received packet should contain bytes")
                .as_ref(),
            b"right-ready"
        );
    }

    #[test]
    fn recv_any_returns_none_when_all_inputs_are_closed() {
        let (left_output, left_input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("left-out"), port_id("left"), NonZeroUsize::MIN);
        let (right_output, right_input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("right-out"), port_id("right"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles(
            [port_id("left"), port_id("right")],
            [left_input, right_input],
        );
        let cancellation: CancellationToken = CancellationToken::active();
        drop(left_output);
        drop(right_output);

        let received: Option<(PortId, PortPacket)> = block_on_port(async {
            inputs
                .recv_any(&cancellation)
                .await
                .expect("closed inputs should end cleanly")
        });

        assert!(received.is_none());
    }

    #[test]
    fn recv_any_observes_pre_cancelled_tokens() {
        let (_output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let cancellation: CancellationToken =
            CancellationToken::cancelled(CancellationRequest::new("test shutdown"));

        let err: PortRecvError = block_on_port(async {
            inputs
                .recv_any(&cancellation)
                .await
                .expect_err("cancelled recv_any should fail")
        });

        assert_eq!(
            err,
            PortRecvError::Cancelled {
                port_id: port_id("in")
            }
        );
    }

    #[test]
    fn undeclared_ports_are_rejected() {
        let mut inputs: PortsIn = PortsIn::new([port_id("in")]);
        let outputs: PortsOut = PortsOut::new([port_id("out")]);

        assert_eq!(
            outputs
                .try_send(&port_id("missing"), packet(b"value"))
                .expect_err("unknown output must fail"),
            PortSendError::UnknownPort {
                port_id: port_id("missing")
            }
        );
        assert_eq!(
            inputs
                .try_recv(&port_id("missing"))
                .expect_err("unknown input must fail"),
            PortRecvError::UnknownPort {
                port_id: port_id("missing")
            }
        );
    }
}
