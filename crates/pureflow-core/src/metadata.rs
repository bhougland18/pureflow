//! Runtime metadata collection boundary.
//!
//! ## Fragment: metadata-collection-boundary
//!
//! Metadata remains split by source type: context metadata identifies an
//! execution attempt, message metadata travels with payloads, and lifecycle
//! metadata records runtime transitions. Message boundary records describe
//! send, receive, and drop observations at the port seam. The sink below is
//! only the collection seam. It lets runtime code report those existing facts
//! without collapsing them into a premature storage, tracing, or graph model.

use std::{
    fs::File,
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
};

use serde_json::{Value, json};

use pureflow_types::{NodeId, PortId, WorkflowId};

use crate::{
    PureflowError, Result,
    capability::EffectCapability,
    context::{CancellationState, ExecutionMetadata, NodeContext},
    lifecycle::{LifecycleEvent, LifecycleEventKind},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};

/// Where a message was observed at the port boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBoundaryKind {
    /// The message entered an output boundary.
    Enqueued,
    /// The message left an input boundary.
    Dequeued,
    /// The message was dropped at an output boundary.
    Dropped,
}

/// One message observation at a runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBoundaryRecord {
    kind: MessageBoundaryKind,
    metadata: MessageMetadata,
}

impl MessageBoundaryRecord {
    /// Create a message boundary observation.
    #[must_use]
    pub const fn new(kind: MessageBoundaryKind, metadata: MessageMetadata) -> Self {
        Self { kind, metadata }
    }

    /// Kind of port-boundary observation.
    #[must_use]
    pub const fn kind(&self) -> MessageBoundaryKind {
        self.kind
    }

    /// Message metadata observed at the boundary.
    #[must_use]
    pub const fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }
}

/// Direction of a queue observation relative to the current node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePortDirection {
    /// Input-side queue observation.
    Input,
    /// Output-side queue observation.
    Output,
}

/// Where queue pressure was observed at the port boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePressureBoundaryKind {
    /// A receive operation is about to inspect or wait on an input queue.
    ReceiveAttempted,
    /// A receive operation dequeued a packet.
    ReceiveReady,
    /// A receive operation found no currently available packet.
    ReceiveEmpty,
    /// A receive operation observed upstream closure.
    ReceiveClosed,
    /// An output reserve operation is about to inspect or wait on capacity.
    ReserveAttempted,
    /// An output reserve operation acquired capacity.
    ReserveReady,
    /// An output reserve operation found all connected capacity full.
    ReserveFull,
    /// A reserved output packet committed to the output boundary.
    SendCommitted,
    /// A committed output packet had no downstream edge and was dropped.
    SendDropped,
}

/// One queue pressure or capacity observation at a port boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePressureRecord {
    context: Option<NodeContext>,
    direction: QueuePortDirection,
    port_id: PortId,
    kind: QueuePressureBoundaryKind,
    connected_edge_count: usize,
    capacity: Option<usize>,
    queued_count: Option<usize>,
}

impl QueuePressureRecord {
    /// Create a queue pressure observation.
    #[must_use]
    pub const fn new(
        context: Option<NodeContext>,
        direction: QueuePortDirection,
        port_id: PortId,
        kind: QueuePressureBoundaryKind,
        connected_edge_count: usize,
        capacity: Option<usize>,
        queued_count: Option<usize>,
    ) -> Self {
        Self {
            context,
            direction,
            port_id,
            kind,
            connected_edge_count,
            capacity,
            queued_count,
        }
    }

    /// Runtime context for the node that observed queue pressure, when known.
    #[must_use]
    pub const fn context(&self) -> Option<&NodeContext> {
        self.context.as_ref()
    }

    /// Direction of the observed port.
    #[must_use]
    pub const fn direction(&self) -> QueuePortDirection {
        self.direction
    }

    /// Declared port identifier observed.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Boundary kind observed.
    #[must_use]
    pub const fn kind(&self) -> QueuePressureBoundaryKind {
        self.kind
    }

    /// Number of connected graph edges behind this declared port.
    #[must_use]
    pub const fn connected_edge_count(&self) -> usize {
        self.connected_edge_count
    }

    /// Total known bounded capacity across connected edges, when connected.
    #[must_use]
    pub const fn capacity(&self) -> Option<usize> {
        self.capacity
    }

    /// Total currently queued packets across connected input edges, when observable.
    #[must_use]
    pub const fn queued_count(&self) -> Option<usize> {
        self.queued_count
    }
}

/// Scope where an error was observed by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorMetadataKind {
    /// A node execution boundary returned an error.
    NodeFailed,
    /// A workflow run observed a terminal error.
    WorkflowFailed,
}

/// One structured error observation at a runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorMetadataRecord {
    kind: ErrorMetadataKind,
    workflow_id: WorkflowId,
    node_id: Option<NodeId>,
    execution: ExecutionMetadata,
    error: PureflowError,
    diagnostic: Option<ErrorDiagnosticMetadata>,
}

impl ErrorMetadataRecord {
    /// Create a node failure observation.
    #[must_use]
    pub fn node_failed(context: &NodeContext, error: PureflowError) -> Self {
        Self {
            kind: ErrorMetadataKind::NodeFailed,
            workflow_id: context.workflow_id().clone(),
            node_id: Some(context.node_id().clone()),
            execution: context.execution().clone(),
            error,
            diagnostic: None,
        }
    }

    /// Create a workflow failure observation.
    #[must_use]
    pub const fn workflow_failed(
        workflow_id: WorkflowId,
        execution: ExecutionMetadata,
        error: PureflowError,
    ) -> Self {
        Self {
            kind: ErrorMetadataKind::WorkflowFailed,
            workflow_id,
            node_id: None,
            execution,
            error,
            diagnostic: None,
        }
    }

    /// Create a workflow failure observation with structured diagnostic data.
    #[must_use]
    pub const fn workflow_failed_with_diagnostic(
        workflow_id: WorkflowId,
        execution: ExecutionMetadata,
        error: PureflowError,
        diagnostic: ErrorDiagnosticMetadata,
    ) -> Self {
        Self {
            kind: ErrorMetadataKind::WorkflowFailed,
            workflow_id,
            node_id: None,
            execution,
            error,
            diagnostic: Some(diagnostic),
        }
    }

    /// Error observation scope.
    #[must_use]
    pub const fn kind(&self) -> ErrorMetadataKind {
        self.kind
    }

    /// Workflow associated with this error.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Node associated with this error, when the error came from a node.
    #[must_use]
    pub const fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// Execution attempt associated with this error.
    #[must_use]
    pub const fn execution(&self) -> &ExecutionMetadata {
        &self.execution
    }

    /// Structured Pureflow error observed.
    #[must_use]
    pub const fn error(&self) -> &PureflowError {
        &self.error
    }

    /// Structured diagnostic payload associated with this error, when present.
    #[must_use]
    pub const fn diagnostic(&self) -> Option<&ErrorDiagnosticMetadata> {
        self.diagnostic.as_ref()
    }
}

/// Structured diagnostic payload attached to an error metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorDiagnosticMetadata {
    /// Workflow execution stopped making progress before the watchdog deadline.
    WorkflowDeadlock(DeadlockDiagnosticMetadata),
}

impl ErrorDiagnosticMetadata {
    /// Create workflow deadlock diagnostic metadata.
    #[must_use]
    pub const fn workflow_deadlock(diagnostic: DeadlockDiagnosticMetadata) -> Self {
        Self::WorkflowDeadlock(diagnostic)
    }
}

/// External effect observation kind at a node boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalEffectMetadataKind {
    /// A node requested an external effect.
    Requested,
    /// A node completed an external effect.
    Completed,
    /// A node observed an external effect failure.
    Failed,
}

/// One structured external effect observation at a node boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectMetadataRecord {
    kind: ExternalEffectMetadataKind,
    context: NodeContext,
    effect: EffectCapability,
    operation: String,
    target: String,
    response_status: Option<String>,
}

impl ExternalEffectMetadataRecord {
    /// Create an external effect observation.
    #[must_use]
    pub fn new(
        kind: ExternalEffectMetadataKind,
        context: NodeContext,
        effect: EffectCapability,
        operation: impl Into<String>,
        target: impl Into<String>,
        response_status: Option<String>,
    ) -> Self {
        Self {
            kind,
            context,
            effect,
            operation: operation.into(),
            target: target.into(),
            response_status,
        }
    }

    /// Create an external effect request observation.
    #[must_use]
    pub fn requested(
        context: NodeContext,
        effect: EffectCapability,
        operation: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self::new(
            ExternalEffectMetadataKind::Requested,
            context,
            effect,
            operation,
            target,
            None,
        )
    }

    /// Create a completed external effect observation.
    #[must_use]
    pub fn completed(
        context: NodeContext,
        effect: EffectCapability,
        operation: impl Into<String>,
        target: impl Into<String>,
        response_status: impl Into<String>,
    ) -> Self {
        Self::new(
            ExternalEffectMetadataKind::Completed,
            context,
            effect,
            operation,
            target,
            Some(response_status.into()),
        )
    }

    /// Create a failed external effect observation.
    #[must_use]
    pub fn failed(
        context: NodeContext,
        effect: EffectCapability,
        operation: impl Into<String>,
        target: impl Into<String>,
        response_status: Option<String>,
    ) -> Self {
        Self::new(
            ExternalEffectMetadataKind::Failed,
            context,
            effect,
            operation,
            target,
            response_status,
        )
    }

    /// External effect observation kind.
    #[must_use]
    pub const fn kind(&self) -> ExternalEffectMetadataKind {
        self.kind
    }

    /// Runtime context for the node that observed the external effect.
    #[must_use]
    pub const fn context(&self) -> &NodeContext {
        &self.context
    }

    /// Declared effect capability associated with this observation.
    #[must_use]
    pub const fn effect(&self) -> EffectCapability {
        self.effect
    }

    /// Operation or tool name associated with this observation.
    #[must_use]
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// External target associated with this observation.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Stable response status associated with this observation, when known.
    #[must_use]
    pub fn response_status(&self) -> Option<&str> {
        self.response_status.as_deref()
    }
}

/// Machine-facing workflow deadlock diagnostic metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlockDiagnosticMetadata {
    scheduled_node_count: usize,
    pending_node_count: usize,
    completed_node_count: usize,
    failed_node_count: usize,
    cancelled_node_count: usize,
    bounded_edge_count: usize,
    no_progress_timeout_ms: u64,
    cycle_policy: &'static str,
    feedback_loop_startup: Option<&'static str>,
    feedback_loop_termination: Option<&'static str>,
}

impl DeadlockDiagnosticMetadata {
    /// Create workflow deadlock diagnostic metadata.
    #[must_use]
    pub const fn new(
        scheduled_node_count: usize,
        pending_node_count: usize,
        bounded_edge_count: usize,
        no_progress_timeout_ms: u64,
        cycle_policy: &'static str,
    ) -> Self {
        Self {
            scheduled_node_count,
            pending_node_count,
            completed_node_count: 0,
            failed_node_count: 0,
            cancelled_node_count: 0,
            bounded_edge_count,
            no_progress_timeout_ms,
            cycle_policy,
            feedback_loop_startup: None,
            feedback_loop_termination: None,
        }
    }

    /// Attach terminal counts observed before the watchdog fired.
    #[must_use]
    pub const fn with_terminal_counts(
        mut self,
        completed_node_count: usize,
        failed_node_count: usize,
        cancelled_node_count: usize,
    ) -> Self {
        self.completed_node_count = completed_node_count;
        self.failed_node_count = failed_node_count;
        self.cancelled_node_count = cancelled_node_count;
        self
    }

    /// Attach feedback-loop policy details.
    #[must_use]
    pub const fn with_feedback_loop(
        mut self,
        startup: &'static str,
        termination: &'static str,
    ) -> Self {
        self.feedback_loop_startup = Some(startup);
        self.feedback_loop_termination = Some(termination);
        self
    }

    /// Nodes scheduled for the run.
    #[must_use]
    pub const fn scheduled_node_count(&self) -> usize {
        self.scheduled_node_count
    }

    /// Nodes still pending when the watchdog fired.
    #[must_use]
    pub const fn pending_node_count(&self) -> usize {
        self.pending_node_count
    }

    /// Nodes completed before the watchdog fired.
    #[must_use]
    pub const fn completed_node_count(&self) -> usize {
        self.completed_node_count
    }

    /// Nodes failed before the watchdog fired.
    #[must_use]
    pub const fn failed_node_count(&self) -> usize {
        self.failed_node_count
    }

    /// Nodes cancelled before the watchdog fired.
    #[must_use]
    pub const fn cancelled_node_count(&self) -> usize {
        self.cancelled_node_count
    }

    /// Bounded graph edges in the workflow.
    #[must_use]
    pub const fn bounded_edge_count(&self) -> usize {
        self.bounded_edge_count
    }

    /// No-progress interval in milliseconds.
    #[must_use]
    pub const fn no_progress_timeout_ms(&self) -> u64 {
        self.no_progress_timeout_ms
    }

    /// Cycle policy active when the watchdog fired.
    #[must_use]
    pub const fn cycle_policy(&self) -> &'static str {
        self.cycle_policy
    }

    /// Feedback-loop startup policy, when cycles were explicitly allowed.
    #[must_use]
    pub const fn feedback_loop_startup(&self) -> Option<&'static str> {
        self.feedback_loop_startup
    }

    /// Feedback-loop termination policy, when cycles were explicitly allowed.
    #[must_use]
    pub const fn feedback_loop_termination(&self) -> Option<&'static str> {
        self.feedback_loop_termination
    }
}

/// One metadata fact observed at a runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    /// Execution context metadata for one node boundary.
    ExecutionContext(NodeContext),
    /// Lifecycle transition emitted by the runtime.
    Lifecycle(LifecycleEvent),
    /// Message metadata observed at a port boundary.
    Message(MessageBoundaryRecord),
    /// Queue pressure or capacity observed at a port boundary.
    QueuePressure(QueuePressureRecord),
    /// Error observed at a node or workflow boundary.
    Error(ErrorMetadataRecord),
    /// External tool, service, database, or API effect observed by a node.
    ExternalEffect(ExternalEffectMetadataRecord),
}

/// Cost tier for one metadata record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTier {
    /// Control-plane facts that are cheap and useful for diagnostics.
    Control,
    /// Data-tier facts that may be sampled to bound metadata volume.
    Data,
    /// High-cost data-tier facts such as payload bytes or Arrow buffer detail.
    HighCostData,
}

/// Policy used by a tiered metadata sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieredMetadataPolicy {
    data_sample_rate: Option<NonZeroUsize>,
    record_high_cost_data: bool,
}

impl TieredMetadataPolicy {
    /// Record control facts only.
    #[must_use]
    pub const fn control_only() -> Self {
        Self {
            data_sample_rate: None,
            record_high_cost_data: false,
        }
    }

    /// Record every data-tier fact while still dropping high-cost data facts.
    #[must_use]
    pub const fn record_data() -> Self {
        Self {
            data_sample_rate: Some(NonZeroUsize::MIN),
            record_high_cost_data: false,
        }
    }

    /// Record one data-tier fact every `sample_rate` observations.
    #[must_use]
    pub const fn sample_data_every(sample_rate: NonZeroUsize) -> Self {
        Self {
            data_sample_rate: Some(sample_rate),
            record_high_cost_data: false,
        }
    }

    /// Allow high-cost data-tier facts to pass through.
    #[must_use]
    pub const fn with_high_cost_data(mut self) -> Self {
        self.record_high_cost_data = true;
        self
    }

    fn should_record_data(self, ordinal: usize) -> bool {
        self.data_sample_rate
            .is_some_and(|sample_rate: NonZeroUsize| ordinal.is_multiple_of(sample_rate.get()))
    }

    const fn should_record_high_cost_data(self) -> bool {
        self.record_high_cost_data
    }
}

impl Default for TieredMetadataPolicy {
    fn default() -> Self {
        Self::control_only()
    }
}

/// Convert one metadata record into the stable JSON shape used by JSONL sinks.
///
/// The projection intentionally omits wall-clock timestamps and process-local
/// facts so repeated runs with the same runtime facts can produce identical
/// log lines.
#[must_use]
pub fn metadata_record_to_json_value(record: &MetadataRecord) -> Value {
    match record {
        MetadataRecord::ExecutionContext(context) => json!({
            "record_type": "execution_context",
            "kind": "execution_context",
            "context": node_context_to_json_value(context),
        }),
        MetadataRecord::Lifecycle(event) => json!({
            "record_type": "lifecycle",
            "kind": lifecycle_event_kind_label(event.kind()),
            "context": node_context_to_json_value(event.context()),
        }),
        MetadataRecord::Message(message) => json!({
            "record_type": "message",
            "kind": message_boundary_kind_label(message.kind()),
            "message": message_metadata_to_json_value(message.metadata()),
        }),
        MetadataRecord::QueuePressure(queue) => json!({
            "record_type": "queue_pressure",
            "kind": queue_pressure_boundary_kind_label(queue.kind()),
            "direction": queue_port_direction_label(queue.direction()),
            "port_id": queue.port_id().as_str(),
            "context": queue
                .context()
                .map_or(Value::Null, node_context_to_json_value),
            "connected_edge_count": queue.connected_edge_count(),
            "capacity": queue.capacity(),
            "queued_count": queue.queued_count(),
        }),
        MetadataRecord::Error(error) => json!({
            "record_type": "error",
            "kind": error_metadata_kind_label(error.kind()),
            "workflow_id": error.workflow_id().as_str(),
            "node_id": error
                .node_id()
                .map_or(Value::Null, |node_id: &NodeId| json!(node_id.as_str())),
            "execution": execution_metadata_to_json_value(error.execution()),
            "error": pureflow_error_to_json_value(error.error()),
            "diagnostic": error
                .diagnostic()
                .map_or(Value::Null, error_diagnostic_metadata_to_json_value),
        }),
        MetadataRecord::ExternalEffect(effect) => json!({
            "record_type": "external_effect",
            "kind": external_effect_metadata_kind_label(effect.kind()),
            "context": node_context_to_json_value(effect.context()),
            "effect": effect.effect().as_str(),
            "operation": effect.operation(),
            "target": effect.target(),
            "response_status": effect.response_status(),
        }),
    }
}

/// Collection sink for runtime metadata records.
pub trait MetadataSink: Send + Sync {
    /// Record one metadata fact.
    ///
    /// # Errors
    ///
    /// Returns an error when the sink cannot preserve or forward the record.
    fn record(&self, record: &MetadataRecord) -> Result<()>;
}

/// Metadata sink that intentionally records nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetadataSink;

impl MetadataSink for NoopMetadataSink {
    fn record(&self, _record: &MetadataRecord) -> Result<()> {
        Ok(())
    }
}

/// Metadata sink adapter that applies a cost-tier policy before forwarding.
#[derive(Debug)]
pub struct TieredMetadataSink<S> {
    inner: S,
    policy: TieredMetadataPolicy,
    counters: Mutex<TieredMetadataCounters>,
}

#[derive(Debug, Default)]
struct TieredMetadataCounters {
    data_seen: usize,
}

impl<S> TieredMetadataSink<S> {
    /// Wrap a sink with the default control-only metadata policy.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self::with_policy(inner, TieredMetadataPolicy::control_only())
    }

    /// Wrap a sink with an explicit tiered metadata policy.
    #[must_use]
    pub const fn with_policy(inner: S, policy: TieredMetadataPolicy) -> Self {
        Self {
            inner,
            policy,
            counters: Mutex::new(TieredMetadataCounters { data_seen: 0 }),
        }
    }

    /// Return the configured tiered metadata policy.
    #[must_use]
    pub const fn policy(&self) -> TieredMetadataPolicy {
        self.policy
    }

    /// Return the wrapped sink.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }

    fn should_record(&self, tier: MetadataTier) -> Result<bool> {
        match tier {
            MetadataTier::Control => Ok(true),
            MetadataTier::Data => {
                let ordinal: usize = {
                    let mut counters: MutexGuard<'_, TieredMetadataCounters> =
                        self.counters.lock().map_err(
                            |_err: PoisonError<MutexGuard<'_, TieredMetadataCounters>>| {
                                tiered_lock_error()
                            },
                        )?;
                    let ordinal: usize = counters.data_seen;
                    counters.data_seen = counters.data_seen.saturating_add(1);
                    ordinal
                };
                Ok(self.policy.should_record_data(ordinal))
            }
            MetadataTier::HighCostData => Ok(self.policy.should_record_high_cost_data()),
        }
    }
}

impl<S> TieredMetadataSink<S>
where
    S: MetadataSink,
{
    /// Record one metadata fact with an explicit cost tier.
    ///
    /// # Errors
    ///
    /// Returns an error if the tier policy state cannot be read or if the
    /// wrapped sink rejects a record selected by the policy.
    pub fn record_with_tier(&self, tier: MetadataTier, record: &MetadataRecord) -> Result<()> {
        if self.should_record(tier)? {
            self.inner.record(record)
        } else {
            Ok(())
        }
    }
}

impl<S> MetadataSink for TieredMetadataSink<S>
where
    S: MetadataSink,
{
    fn record(&self, record: &MetadataRecord) -> Result<()> {
        self.record_with_tier(MetadataTier::Control, record)
    }
}

/// Metadata sink that writes one stable JSON object per line.
#[derive(Debug)]
pub struct JsonlMetadataSink<W> {
    writer: Mutex<W>,
}

impl<W> JsonlMetadataSink<W> {
    /// Create a JSONL metadata sink around an existing writer.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }

    /// Return the wrapped writer.
    ///
    /// # Errors
    ///
    /// Returns an error if a prior panic poisoned the writer lock.
    pub fn into_inner(self) -> Result<W> {
        self.writer
            .into_inner()
            .map_err(|_err: PoisonError<W>| jsonl_lock_error())
    }

    fn lock_writer(&self) -> Result<MutexGuard<'_, W>> {
        self.writer
            .lock()
            .map_err(|_err: PoisonError<MutexGuard<'_, W>>| jsonl_lock_error())
    }
}

impl<W> JsonlMetadataSink<W>
where
    W: Write,
{
    /// Flush the wrapped writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer cannot flush buffered metadata.
    pub fn flush(&self) -> Result<()> {
        let mut writer: MutexGuard<'_, W> = self.lock_writer()?;
        writer.flush().map_err(|source: std::io::Error| {
            crate::PureflowError::metadata(format!("failed to flush metadata JSONL: {source}"))
        })
    }
}

impl JsonlMetadataSink<BufWriter<File>> {
    /// Create a file-backed JSONL metadata sink, truncating any existing file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file: File = File::create(path).map_err(|source: std::io::Error| {
            crate::PureflowError::metadata(format!("failed to create metadata JSONL file: {source}"))
        })?;

        Ok(Self::new(BufWriter::new(file)))
    }
}

impl<W> MetadataSink for JsonlMetadataSink<W>
where
    W: Write + Send,
{
    fn record(&self, record: &MetadataRecord) -> Result<()> {
        let value: Value = metadata_record_to_json_value(record);
        let mut writer: MutexGuard<'_, W> = self.lock_writer()?;

        serde_json::to_writer(&mut *writer, &value).map_err(|source: serde_json::Error| {
            crate::PureflowError::metadata(format!(
                "failed to encode metadata JSONL record: {source}"
            ))
        })?;
        writer.write_all(b"\n").map_err(|source: std::io::Error| {
            crate::PureflowError::metadata(format!(
                "failed to write metadata JSONL newline: {source}"
            ))
        })
    }
}

fn node_context_to_json_value(context: &NodeContext) -> Value {
    json!({
        "workflow_id": context.workflow_id().as_str(),
        "node_id": context.node_id().as_str(),
        "execution": execution_metadata_to_json_value(context.execution()),
        "cancellation": cancellation_state_to_json_value(context.cancellation()),
    })
}

fn execution_metadata_to_json_value(execution: &ExecutionMetadata) -> Value {
    json!({
        "execution_id": execution.execution_id().as_str(),
        "attempt": execution.attempt().get(),
    })
}

fn cancellation_state_to_json_value(cancellation: CancellationState) -> Value {
    match cancellation {
        CancellationState::Active => json!({
            "state": "active",
        }),
        CancellationState::Requested(request) => json!({
            "state": "requested",
            "reason": request.reason(),
        }),
    }
}

const fn lifecycle_event_kind_label(kind: LifecycleEventKind) -> &'static str {
    match kind {
        LifecycleEventKind::NodeScheduled => "node_scheduled",
        LifecycleEventKind::NodeStarted => "node_started",
        LifecycleEventKind::NodeCompleted => "node_completed",
        LifecycleEventKind::NodeFailed => "node_failed",
        LifecycleEventKind::NodeCancelled => "node_cancelled",
    }
}

const fn message_boundary_kind_label(kind: MessageBoundaryKind) -> &'static str {
    match kind {
        MessageBoundaryKind::Enqueued => "enqueued",
        MessageBoundaryKind::Dequeued => "dequeued",
        MessageBoundaryKind::Dropped => "dropped",
    }
}

const fn queue_port_direction_label(direction: QueuePortDirection) -> &'static str {
    match direction {
        QueuePortDirection::Input => "input",
        QueuePortDirection::Output => "output",
    }
}

const fn queue_pressure_boundary_kind_label(kind: QueuePressureBoundaryKind) -> &'static str {
    match kind {
        QueuePressureBoundaryKind::ReceiveAttempted => "receive_attempted",
        QueuePressureBoundaryKind::ReceiveReady => "receive_ready",
        QueuePressureBoundaryKind::ReceiveEmpty => "receive_empty",
        QueuePressureBoundaryKind::ReceiveClosed => "receive_closed",
        QueuePressureBoundaryKind::ReserveAttempted => "reserve_attempted",
        QueuePressureBoundaryKind::ReserveReady => "reserve_ready",
        QueuePressureBoundaryKind::ReserveFull => "reserve_full",
        QueuePressureBoundaryKind::SendCommitted => "send_committed",
        QueuePressureBoundaryKind::SendDropped => "send_dropped",
    }
}

const fn error_metadata_kind_label(kind: ErrorMetadataKind) -> &'static str {
    match kind {
        ErrorMetadataKind::NodeFailed => "node_failed",
        ErrorMetadataKind::WorkflowFailed => "workflow_failed",
    }
}

const fn external_effect_metadata_kind_label(kind: ExternalEffectMetadataKind) -> &'static str {
    match kind {
        ExternalEffectMetadataKind::Requested => "external_effect_requested",
        ExternalEffectMetadataKind::Completed => "external_effect_completed",
        ExternalEffectMetadataKind::Failed => "external_effect_failed",
    }
}

fn pureflow_error_to_json_value(error: &PureflowError) -> Value {
    json!({
        "code": error.code().as_str(),
        "message": error.to_string(),
        "visibility": error_visibility_label(error.visibility()),
        "retry_disposition": retry_disposition_label(error.retry_disposition()),
    })
}

fn error_diagnostic_metadata_to_json_value(diagnostic: &ErrorDiagnosticMetadata) -> Value {
    match diagnostic {
        ErrorDiagnosticMetadata::WorkflowDeadlock(deadlock) => json!({
            "type": "workflow_deadlock",
            "scheduled_node_count": deadlock.scheduled_node_count(),
            "pending_node_count": deadlock.pending_node_count(),
            "completed_node_count": deadlock.completed_node_count(),
            "failed_node_count": deadlock.failed_node_count(),
            "cancelled_node_count": deadlock.cancelled_node_count(),
            "bounded_edge_count": deadlock.bounded_edge_count(),
            "no_progress_timeout_ms": deadlock.no_progress_timeout_ms(),
            "cycle_policy": deadlock.cycle_policy(),
            "feedback_loop_startup": deadlock.feedback_loop_startup(),
            "feedback_loop_termination": deadlock.feedback_loop_termination(),
        }),
    }
}

const fn error_visibility_label(visibility: crate::ErrorVisibility) -> &'static str {
    match visibility {
        crate::ErrorVisibility::User => "user",
        crate::ErrorVisibility::Internal => "internal",
    }
}

const fn retry_disposition_label(disposition: crate::RetryDisposition) -> &'static str {
    match disposition {
        crate::RetryDisposition::Never => "never",
        crate::RetryDisposition::Safe => "safe",
        crate::RetryDisposition::Unknown => "unknown",
    }
}

fn message_metadata_to_json_value(metadata: &MessageMetadata) -> Value {
    json!({
        "message_id": metadata.message_id().as_str(),
        "workflow_id": metadata.workflow_id().as_str(),
        "execution": execution_metadata_to_json_value(metadata.execution()),
        "route": message_route_to_json_value(metadata.route()),
    })
}

fn message_route_to_json_value(route: &MessageRoute) -> Value {
    json!({
        "source": route
            .source()
            .map_or(Value::Null, message_endpoint_to_json_value),
        "target": message_endpoint_to_json_value(route.target()),
    })
}

fn message_endpoint_to_json_value(endpoint: &MessageEndpoint) -> Value {
    json!({
        "node_id": endpoint.node_id().as_str(),
        "port_id": endpoint.port_id().as_str(),
    })
}

fn jsonl_lock_error() -> crate::PureflowError {
    crate::PureflowError::metadata("metadata JSONL writer lock poisoned")
}

fn tiered_lock_error() -> crate::PureflowError {
    crate::PureflowError::metadata("tiered metadata policy lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{CancellationRequest, ExecutionAttempt, ExecutionMetadata};
    use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
    use std::io;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use quickcheck::{Arbitrary, Gen, QuickCheck};

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn context() -> NodeContext {
        NodeContext::new(
            workflow_id("flow"),
            node_id("node"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
        )
    }

    fn message_metadata() -> MessageMetadata {
        let source: crate::message::MessageEndpoint =
            crate::message::MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: crate::message::MessageEndpoint =
            crate::message::MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: crate::message::MessageRoute =
            crate::message::MessageRoute::new(Some(source), target);

        MessageMetadata::new(
            MessageId::new("msg-1").expect("valid message id"),
            workflow_id("flow"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
            route,
        )
    }

    #[derive(Debug, Clone)]
    struct ArbitraryMetadataRecord(MetadataRecord);

    impl Arbitrary for ArbitraryMetadataRecord {
        fn arbitrary(g: &mut Gen) -> Self {
            let record = match u8::arbitrary(g) % 6 {
                0 => MetadataRecord::ExecutionContext(generated_context(g)),
                1 => MetadataRecord::Lifecycle(LifecycleEvent::new(
                    generated_lifecycle_kind(g),
                    generated_context(g),
                )),
                2 => MetadataRecord::Message(MessageBoundaryRecord::new(
                    generated_message_boundary_kind(g),
                    generated_message_metadata(g),
                )),
                3 => MetadataRecord::QueuePressure(QueuePressureRecord::new(
                    bool::arbitrary(g).then(|| generated_context(g)),
                    generated_queue_port_direction(g),
                    generated_port_id(g, "port"),
                    generated_queue_pressure_kind(g),
                    bounded_usize(g, 8),
                    bool::arbitrary(g).then(|| bounded_usize(g, 256)),
                    bool::arbitrary(g).then(|| bounded_usize(g, 256)),
                )),
                4 => MetadataRecord::Error(generated_error_metadata(g)),
                _ => MetadataRecord::ExternalEffect(generated_external_effect_metadata(g)),
            };

            Self(record)
        }
    }

    fn bounded_usize(g: &mut Gen, upper_exclusive: usize) -> usize {
        usize::arbitrary(g) % upper_exclusive
    }

    fn generated_execution(g: &mut Gen) -> ExecutionMetadata {
        let attempt = NonZeroU32::new(u32::arbitrary(g) % 32 + 1)
            .expect("generated execution attempt is nonzero");

        ExecutionMetadata::new(
            generated_execution_id(g, "run"),
            ExecutionAttempt::new(attempt),
        )
    }

    fn generated_context(g: &mut Gen) -> NodeContext {
        let context = NodeContext::new(
            generated_workflow_id(g, "flow"),
            generated_node_id(g, "node"),
            generated_execution(g),
        );

        if bool::arbitrary(g) {
            context.with_cancellation(CancellationRequest::new(format!(
                "cancelled_{}",
                bounded_usize(g, 16)
            )))
        } else {
            context
        }
    }

    fn generated_message_metadata(g: &mut Gen) -> MessageMetadata {
        let source = bool::arbitrary(g).then(|| {
            MessageEndpoint::new(generated_node_id(g, "source"), generated_port_id(g, "out"))
        });
        let target = MessageEndpoint::new(generated_node_id(g, "sink"), generated_port_id(g, "in"));

        MessageMetadata::new(
            generated_message_id(g, "msg"),
            generated_workflow_id(g, "flow"),
            generated_execution(g),
            MessageRoute::new(source, target),
        )
    }

    fn generated_error_metadata(g: &mut Gen) -> ErrorMetadataRecord {
        match u8::arbitrary(g) % 3 {
            0 => ErrorMetadataRecord::node_failed(&generated_context(g), generated_error(g)),
            1 => ErrorMetadataRecord::workflow_failed(
                generated_workflow_id(g, "flow"),
                generated_execution(g),
                generated_error(g),
            ),
            _ => ErrorMetadataRecord::workflow_failed_with_diagnostic(
                generated_workflow_id(g, "flow"),
                generated_execution(g),
                generated_error(g),
                ErrorDiagnosticMetadata::workflow_deadlock(
                    DeadlockDiagnosticMetadata::new(
                        bounded_usize(g, 16),
                        bounded_usize(g, 16),
                        bounded_usize(g, 16),
                        u64::arbitrary(g) % 60_000,
                        generated_cycle_policy(g),
                    )
                    .with_terminal_counts(
                        bounded_usize(g, 16),
                        bounded_usize(g, 16),
                        bounded_usize(g, 16),
                    )
                    .with_feedback_loop("start_all_nodes", "all_nodes_complete"),
                ),
            ),
        }
    }

    fn generated_external_effect_metadata(g: &mut Gen) -> ExternalEffectMetadataRecord {
        let context = generated_context(g);
        let effect = generated_effect(g);
        let operation = format!("operation_{}", bounded_usize(g, 32));
        let target = format!("target_{}", bounded_usize(g, 32));
        match u8::arbitrary(g) % 3 {
            0 => ExternalEffectMetadataRecord::requested(context, effect, operation, target),
            1 => ExternalEffectMetadataRecord::completed(
                context,
                effect,
                operation,
                target,
                format!("status_{}", bounded_usize(g, 16)),
            ),
            _ => ExternalEffectMetadataRecord::failed(
                context,
                effect,
                operation,
                target,
                bool::arbitrary(g).then(|| format!("status_{}", bounded_usize(g, 16))),
            ),
        }
    }

    fn generated_effect(g: &mut Gen) -> EffectCapability {
        match u8::arbitrary(g) % 8 {
            0 => EffectCapability::FileSystemRead,
            1 => EffectCapability::FileSystemWrite,
            2 => EffectCapability::NetworkOutbound,
            3 => EffectCapability::ExternalEffect,
            4 => EffectCapability::ProcessSpawn,
            5 => EffectCapability::EnvironmentRead,
            6 => EffectCapability::EnvironmentWrite,
            _ => EffectCapability::Clock,
        }
    }

    fn generated_error(g: &mut Gen) -> crate::PureflowError {
        let message = format!("generated_failure_{}", bounded_usize(g, 32));
        match u8::arbitrary(g) % 4 {
            0 => crate::PureflowError::execution(message),
            1 => crate::PureflowError::cancelled(message),
            2 => crate::PureflowError::lifecycle(message),
            _ => crate::PureflowError::metadata(message),
        }
    }

    fn generated_lifecycle_kind(g: &mut Gen) -> LifecycleEventKind {
        match u8::arbitrary(g) % 5 {
            0 => LifecycleEventKind::NodeScheduled,
            1 => LifecycleEventKind::NodeStarted,
            2 => LifecycleEventKind::NodeCompleted,
            3 => LifecycleEventKind::NodeFailed,
            _ => LifecycleEventKind::NodeCancelled,
        }
    }

    fn generated_message_boundary_kind(g: &mut Gen) -> MessageBoundaryKind {
        match u8::arbitrary(g) % 3 {
            0 => MessageBoundaryKind::Enqueued,
            1 => MessageBoundaryKind::Dequeued,
            _ => MessageBoundaryKind::Dropped,
        }
    }

    fn generated_queue_port_direction(g: &mut Gen) -> QueuePortDirection {
        if bool::arbitrary(g) {
            QueuePortDirection::Input
        } else {
            QueuePortDirection::Output
        }
    }

    fn generated_queue_pressure_kind(g: &mut Gen) -> QueuePressureBoundaryKind {
        match u8::arbitrary(g) % 9 {
            0 => QueuePressureBoundaryKind::ReceiveAttempted,
            1 => QueuePressureBoundaryKind::ReceiveReady,
            2 => QueuePressureBoundaryKind::ReceiveEmpty,
            3 => QueuePressureBoundaryKind::ReceiveClosed,
            4 => QueuePressureBoundaryKind::ReserveAttempted,
            5 => QueuePressureBoundaryKind::ReserveReady,
            6 => QueuePressureBoundaryKind::ReserveFull,
            7 => QueuePressureBoundaryKind::SendCommitted,
            _ => QueuePressureBoundaryKind::SendDropped,
        }
    }

    fn generated_cycle_policy(g: &mut Gen) -> &'static str {
        if bool::arbitrary(g) {
            "reject_cycles"
        } else {
            "allow_feedback_loops"
        }
    }

    fn generated_workflow_id(g: &mut Gen, prefix: &str) -> WorkflowId {
        workflow_id(&format!("{prefix}_{}", bounded_usize(g, 64)))
    }

    fn generated_execution_id(g: &mut Gen, prefix: &str) -> ExecutionId {
        execution_id(&format!("{prefix}_{}", bounded_usize(g, 64)))
    }

    fn generated_message_id(g: &mut Gen, prefix: &str) -> MessageId {
        MessageId::new(format!("{prefix}_{}", bounded_usize(g, 64))).expect("valid message id")
    }

    fn generated_node_id(g: &mut Gen, prefix: &str) -> NodeId {
        node_id(&format!("{prefix}_{}", bounded_usize(g, 64)))
    }

    fn generated_port_id(g: &mut Gen, prefix: &str) -> PortId {
        port_id(&format!("{prefix}_{}", bounded_usize(g, 64)))
    }

    fn top_level_kind(value: &Value) -> Option<&str> {
        value.as_object()?.get("kind")?.as_str()
    }

    fn is_known_kind(kind: &str) -> bool {
        matches!(
            kind,
            "execution_context"
                | "node_scheduled"
                | "node_started"
                | "node_completed"
                | "node_failed"
                | "node_cancelled"
                | "enqueued"
                | "dequeued"
                | "dropped"
                | "receive_attempted"
                | "receive_ready"
                | "receive_empty"
                | "receive_closed"
                | "reserve_attempted"
                | "reserve_ready"
                | "reserve_full"
                | "send_committed"
                | "send_dropped"
                | "workflow_failed"
                | "external_effect_requested"
                | "external_effect_completed"
                | "external_effect_failed"
        )
    }

    fn contains_reproducibility_sensitive_key(value: &Value) -> bool {
        match value {
            Value::Object(map) => map.iter().any(|(key, value)| {
                is_reproducibility_sensitive_key(key)
                    || contains_reproducibility_sensitive_key(value)
            }),
            Value::Array(values) => values.iter().any(contains_reproducibility_sensitive_key),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
        }
    }

    fn is_reproducibility_sensitive_key(key: &str) -> bool {
        matches!(
            key,
            "timestamp"
                | "wall_clock_timestamp"
                | "thread_id"
                | "process_id"
                | "pid"
                | "payload"
                | "payload_bytes"
                | "raw_payload_bytes"
        )
    }

    #[derive(Debug, Default)]
    struct RecordingMetadataSink {
        records: Mutex<Vec<MetadataRecord>>,
    }

    impl RecordingMetadataSink {
        fn len(&self) -> usize {
            self.records
                .lock()
                .expect("recording metadata sink lock should not be poisoned")
                .len()
        }

        fn records(&self) -> Vec<MetadataRecord> {
            self.records
                .lock()
                .expect("recording metadata sink lock should not be poisoned")
                .clone()
        }
    }

    impl MetadataSink for RecordingMetadataSink {
        fn record(&self, record: &MetadataRecord) -> Result<()> {
            self.records
                .lock()
                .expect("recording metadata sink lock should not be poisoned")
                .push(record.clone());
            Ok(())
        }
    }

    impl MetadataSink for Arc<RecordingMetadataSink> {
        fn record(&self, record: &MetadataRecord) -> Result<()> {
            self.as_ref().record(record)
        }
    }

    #[test]
    fn generated_metadata_json_preserves_stable_machine_contract() {
        #[allow(clippy::needless_pass_by_value)]
        fn property(record: ArbitraryMetadataRecord) -> bool {
            let value = metadata_record_to_json_value(&record.0);

            top_level_kind(&value).is_some_and(is_known_kind)
                && !contains_reproducibility_sensitive_key(&value)
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(ArbitraryMetadataRecord) -> bool);
    }

    #[test]
    fn metadata_record_keeps_context_shape_intact() {
        let record: MetadataRecord = MetadataRecord::ExecutionContext(context());

        assert!(matches!(
            record,
            MetadataRecord::ExecutionContext(ctx) if ctx.node_id().as_str() == "node"
        ));
    }

    #[test]
    fn noop_metadata_sink_accepts_records() {
        let record: MetadataRecord = MetadataRecord::ExecutionContext(context());

        NoopMetadataSink
            .record(&record)
            .expect("noop metadata sink should accept records");
    }

    #[test]
    fn tiered_metadata_sink_records_control_records_by_default() {
        let inner: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let sink: TieredMetadataSink<Arc<RecordingMetadataSink>> =
            TieredMetadataSink::new(inner.clone());
        let record: MetadataRecord = MetadataRecord::Lifecycle(LifecycleEvent::new(
            LifecycleEventKind::NodeStarted,
            context(),
        ));

        sink.record(&record)
            .expect("control metadata should pass through");

        assert_eq!(inner.records(), vec![record]);
    }

    #[test]
    fn tiered_metadata_sink_drops_data_and_high_cost_records_by_default() {
        let inner: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let sink: TieredMetadataSink<Arc<RecordingMetadataSink>> =
            TieredMetadataSink::new(inner.clone());
        let record: MetadataRecord = MetadataRecord::Message(MessageBoundaryRecord::new(
            MessageBoundaryKind::Enqueued,
            message_metadata(),
        ));

        sink.record_with_tier(MetadataTier::Data, &record)
            .expect("dropped data metadata should be accepted");
        sink.record_with_tier(MetadataTier::HighCostData, &record)
            .expect("dropped high-cost metadata should be accepted");

        assert_eq!(inner.len(), 0);
    }

    #[test]
    fn tiered_metadata_sink_samples_data_records() {
        let inner: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let policy: TieredMetadataPolicy =
            TieredMetadataPolicy::sample_data_every(NonZeroUsize::new(2).expect("nonzero"));
        let sink: TieredMetadataSink<Arc<RecordingMetadataSink>> =
            TieredMetadataSink::with_policy(inner.clone(), policy);
        let record: MetadataRecord = MetadataRecord::Message(MessageBoundaryRecord::new(
            MessageBoundaryKind::Dequeued,
            message_metadata(),
        ));

        sink.record_with_tier(MetadataTier::Data, &record)
            .expect("first sampled data metadata should pass through");
        sink.record_with_tier(MetadataTier::Data, &record)
            .expect("second sampled data metadata should be dropped");
        sink.record_with_tier(MetadataTier::Data, &record)
            .expect("third sampled data metadata should pass through");

        assert_eq!(inner.records(), vec![record.clone(), record]);
    }

    #[test]
    fn tiered_metadata_policy_can_enable_high_cost_records() {
        let inner: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let sink: TieredMetadataSink<Arc<RecordingMetadataSink>> = TieredMetadataSink::with_policy(
            inner.clone(),
            TieredMetadataPolicy::control_only().with_high_cost_data(),
        );
        let record: MetadataRecord = MetadataRecord::Message(MessageBoundaryRecord::new(
            MessageBoundaryKind::Dropped,
            message_metadata(),
        ));

        sink.record_with_tier(MetadataTier::HighCostData, &record)
            .expect("enabled high-cost metadata should pass through");

        assert_eq!(inner.records(), vec![record]);
    }

    #[test]
    fn message_boundary_record_keeps_shape_intact() {
        let target: crate::message::MessageEndpoint =
            crate::message::MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: crate::message::MessageRoute = crate::message::MessageRoute::new(None, target);
        let metadata: MessageMetadata = MessageMetadata::new(
            MessageId::new("msg-1").expect("valid message id"),
            workflow_id("flow"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
            route,
        );
        let record: MessageBoundaryRecord =
            MessageBoundaryRecord::new(MessageBoundaryKind::Enqueued, metadata);

        assert!(matches!(
            record,
            MessageBoundaryRecord {
                kind: MessageBoundaryKind::Enqueued,
                ..
            }
        ));
    }

    #[test]
    fn metadata_record_json_uses_stable_queue_pressure_shape() {
        let record: MetadataRecord = MetadataRecord::QueuePressure(QueuePressureRecord::new(
            Some(context()),
            QueuePortDirection::Input,
            port_id("in"),
            QueuePressureBoundaryKind::ReceiveReady,
            2,
            Some(8),
            Some(3),
        ));

        assert_eq!(
            metadata_record_to_json_value(&record),
            json!({
                "record_type": "queue_pressure",
                "kind": "receive_ready",
                "direction": "input",
                "port_id": "in",
                "context": {
                    "workflow_id": "flow",
                    "node_id": "node",
                    "execution": {
                        "execution_id": "run-1",
                        "attempt": 1,
                    },
                    "cancellation": {
                        "state": "active",
                    },
                },
                "connected_edge_count": 2,
                "capacity": 8,
                "queued_count": 3,
            })
        );
    }

    #[test]
    fn metadata_record_json_uses_stable_message_shape() {
        let record: MetadataRecord = MetadataRecord::Message(MessageBoundaryRecord::new(
            MessageBoundaryKind::Dequeued,
            message_metadata(),
        ));

        assert_eq!(
            metadata_record_to_json_value(&record),
            json!({
                "record_type": "message",
                "kind": "dequeued",
                "message": {
                    "message_id": "msg-1",
                    "workflow_id": "flow",
                    "execution": {
                        "execution_id": "run-1",
                        "attempt": 1,
                    },
                    "route": {
                        "source": {
                            "node_id": "source",
                            "port_id": "out",
                        },
                        "target": {
                            "node_id": "sink",
                            "port_id": "in",
                        },
                    },
                },
            })
        );
    }

    #[test]
    fn metadata_record_json_uses_stable_external_effect_shape() {
        let record: MetadataRecord =
            MetadataRecord::ExternalEffect(ExternalEffectMetadataRecord::completed(
                context(),
                EffectCapability::ExternalEffect,
                "tool_call",
                "get_weather",
                "ok",
            ));

        assert_eq!(
            metadata_record_to_json_value(&record),
            json!({
                "record_type": "external_effect",
                "kind": "external_effect_completed",
                "context": {
                    "workflow_id": "flow",
                    "node_id": "node",
                    "execution": {
                        "execution_id": "run-1",
                        "attempt": 1,
                    },
                    "cancellation": {
                        "state": "active",
                    },
                },
                "effect": "external_effect",
                "operation": "tool_call",
                "target": "get_weather",
                "response_status": "ok",
            })
        );
    }

    #[test]
    fn metadata_record_json_uses_stable_error_shape() {
        let record: MetadataRecord = MetadataRecord::Error(ErrorMetadataRecord::node_failed(
            &context(),
            crate::PureflowError::execution("executor failed"),
        ));

        assert_eq!(
            metadata_record_to_json_value(&record),
            json!({
                "record_type": "error",
                "kind": "node_failed",
                "workflow_id": "flow",
                "node_id": "node",
                "execution": {
                    "execution_id": "run-1",
                    "attempt": 1,
                },
                "error": {
                    "code": "CDT-EXEC-001",
                    "message": "CDT-EXEC-001: node execution failed: executor failed",
                    "visibility": "internal",
                    "retry_disposition": "unknown",
                },
                "diagnostic": null,
            })
        );
    }

    #[test]
    fn metadata_record_json_uses_stable_deadlock_diagnostic_shape() {
        let diagnostic: ErrorDiagnosticMetadata = ErrorDiagnosticMetadata::workflow_deadlock(
            DeadlockDiagnosticMetadata::new(2, 2, 2, 1, "allow_feedback_loops")
                .with_feedback_loop("start_all_nodes", "all_nodes_complete"),
        );
        let record: MetadataRecord =
            MetadataRecord::Error(ErrorMetadataRecord::workflow_failed_with_diagnostic(
                workflow_id("flow"),
                ExecutionMetadata::first_attempt(execution_id("run-1")),
                crate::PureflowError::execution("watchdog detected no workflow progress"),
                diagnostic,
            ));

        assert_eq!(
            metadata_record_to_json_value(&record),
            json!({
                "record_type": "error",
                "kind": "workflow_failed",
                "workflow_id": "flow",
                "node_id": null,
                "execution": {
                    "execution_id": "run-1",
                    "attempt": 1,
                },
                "error": {
                    "code": "CDT-EXEC-001",
                    "message": "CDT-EXEC-001: node execution failed: watchdog detected no workflow progress",
                    "visibility": "internal",
                    "retry_disposition": "unknown",
                },
                "diagnostic": {
                    "type": "workflow_deadlock",
                    "scheduled_node_count": 2,
                    "pending_node_count": 2,
                    "completed_node_count": 0,
                    "failed_node_count": 0,
                    "cancelled_node_count": 0,
                    "bounded_edge_count": 2,
                    "no_progress_timeout_ms": 1,
                    "cycle_policy": "allow_feedback_loops",
                    "feedback_loop_startup": "start_all_nodes",
                    "feedback_loop_termination": "all_nodes_complete",
                },
            })
        );
    }

    #[test]
    fn workflow_error_metadata_has_no_node_id() {
        let record: ErrorMetadataRecord = ErrorMetadataRecord::workflow_failed(
            workflow_id("flow"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
            crate::PureflowError::cancelled("shutdown"),
        );

        assert_eq!(record.kind(), ErrorMetadataKind::WorkflowFailed);
        assert!(record.node_id().is_none());
        assert!(record.diagnostic().is_none());
    }

    #[test]
    fn jsonl_metadata_sink_writes_reproducible_lines() {
        let sink: JsonlMetadataSink<Vec<u8>> = JsonlMetadataSink::new(Vec::new());
        let record: MetadataRecord = MetadataRecord::Lifecycle(LifecycleEvent::new(
            LifecycleEventKind::NodeStarted,
            context(),
        ));

        sink.record(&record)
            .expect("first metadata record should write");
        sink.record(&record)
            .expect("second metadata record should write");
        let output: String = String::from_utf8(sink.into_inner().expect("writer should return"))
            .expect("JSONL should be UTF-8");
        let mut lines = output.lines();
        let first = lines.next().expect("first JSONL line should exist");
        let second = lines.next().expect("second JSONL line should exist");

        assert_eq!(first, second);
        assert!(lines.next().is_none());
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("write failed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn jsonl_metadata_sink_maps_writer_failures() {
        let sink: JsonlMetadataSink<FailingWriter> = JsonlMetadataSink::new(FailingWriter);
        let record: MetadataRecord = MetadataRecord::ExecutionContext(context());
        let err: crate::PureflowError = sink
            .record(&record)
            .expect_err("writer failure should surface");

        assert_eq!(err.code(), crate::ErrorCode::MetadataCollectionFailed);
        assert!(err.to_string().contains("failed to encode"));
    }
}
