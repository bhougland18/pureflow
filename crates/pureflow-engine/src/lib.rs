//! High-level workflow orchestration for Pureflow.

use std::num::NonZeroUsize;
use std::time::Duration;
use std::{collections::BTreeMap, fmt, future::Future, sync::Arc};

use pureflow_contract::{NodeContract, PortContract, SchemaRef};
use pureflow_core::{
    BatchExecutor, BatchInputs, BatchOutputs, CancellationHandle, PureflowError, InputPortHandle,
    MetadataRecord, MetadataSink, NodeExecutor, OutputPacketValidator, OutputPortHandle,
    PortPacket, PortSendError, PortsIn, PortsOut, Result, bounded_edge_channel,
    context::{CancellationRequest, CancellationToken, ExecutionMetadata, NodeContext},
    lifecycle::{LifecycleHook, NoopLifecycleHook},
    message::MessageEndpoint,
    metadata::{
        DeadlockDiagnosticMetadata, ErrorDiagnosticMetadata, ErrorMetadataRecord, NoopMetadataSink,
    },
};
use pureflow_runtime::run_node_with_observers;
use pureflow_types::{NodeId, PortId, WorkflowId};
use pureflow_workflow::{
    NodeDefinition, PortDirection, WorkflowDefinition, WorkflowValidationError,
};
use futures::{
    channel::oneshot,
    future::{BoxFuture, Either, select},
    stream::{FuturesUnordered, Next, StreamExt},
};

const DEFAULT_EDGE_CAPACITY: NonZeroUsize = NonZeroUsize::MIN;

/// Runtime policy for executing one workflow graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowRunPolicy {
    cycle_policy: CycleRunPolicy,
    watchdog_policy: WorkflowWatchdogPolicy,
}

impl WorkflowRunPolicy {
    /// Reject cyclic workflows at run time.
    #[must_use]
    pub const fn acyclic() -> Self {
        Self {
            cycle_policy: CycleRunPolicy::Reject,
            watchdog_policy: WorkflowWatchdogPolicy::Disabled,
        }
    }

    /// Allow cycle-enabled workflow graphs to run as feedback loops.
    #[must_use]
    pub const fn feedback_loops(feedback_loop: FeedbackLoopRunPolicy) -> Self {
        Self {
            cycle_policy: CycleRunPolicy::AllowFeedbackLoops(feedback_loop),
            watchdog_policy: WorkflowWatchdogPolicy::Disabled,
        }
    }

    /// Configured cycle behavior.
    #[must_use]
    pub const fn cycle_policy(&self) -> CycleRunPolicy {
        self.cycle_policy
    }

    /// Return a copy of this policy with watchdog behavior attached.
    #[must_use]
    pub const fn with_watchdog(mut self, watchdog_policy: WorkflowWatchdogPolicy) -> Self {
        self.watchdog_policy = watchdog_policy;
        self
    }

    /// Configured no-progress watchdog behavior.
    #[must_use]
    pub const fn watchdog_policy(&self) -> WorkflowWatchdogPolicy {
        self.watchdog_policy
    }
}

impl Default for WorkflowRunPolicy {
    fn default() -> Self {
        Self::acyclic()
    }
}

/// Runtime behavior for cyclic workflow graphs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleRunPolicy {
    /// Reject directed cycles before starting nodes.
    Reject,
    /// Start a cycle-enabled graph as an explicit feedback loop.
    AllowFeedbackLoops(FeedbackLoopRunPolicy),
}

/// Startup and termination behavior for feedback-loop workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackLoopRunPolicy {
    startup: FeedbackLoopStartup,
    termination: FeedbackLoopTermination,
}

impl FeedbackLoopRunPolicy {
    /// Create an explicit feedback-loop policy.
    #[must_use]
    pub const fn new(startup: FeedbackLoopStartup, termination: FeedbackLoopTermination) -> Self {
        Self {
            startup,
            termination,
        }
    }

    /// Start every node immediately and finish when all node tasks finish.
    #[must_use]
    pub const fn start_all_nodes_until_complete() -> Self {
        Self::new(
            FeedbackLoopStartup::StartAllNodes,
            FeedbackLoopTermination::AllNodesComplete,
        )
    }

    /// Configured feedback-loop startup behavior.
    #[must_use]
    pub const fn startup(&self) -> FeedbackLoopStartup {
        self.startup
    }

    /// Configured feedback-loop termination behavior.
    #[must_use]
    pub const fn termination(&self) -> FeedbackLoopTermination {
        self.termination
    }
}

/// How a feedback-loop graph is started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackLoopStartup {
    /// Start every declared node immediately.
    StartAllNodes,
}

/// How a feedback-loop graph reaches a terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackLoopTermination {
    /// The run completes only after all node tasks complete successfully.
    AllNodesComplete,
}

/// Runtime watchdog behavior for workflow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkflowWatchdogPolicy {
    /// Do not monitor no-progress workflow execution.
    #[default]
    Disabled,
    /// Cancel the workflow if no scheduled node reaches a terminal state before
    /// the configured deadline. The deadline resets after each node result.
    Deadlock(DeadlockWatchdogPolicy),
}

impl WorkflowWatchdogPolicy {
    /// Return a disabled watchdog policy.
    #[must_use]
    pub const fn disabled() -> Self {
        Self::Disabled
    }

    /// Cancel the workflow after a no-progress interval.
    ///
    /// A zero duration is allowed and fires on the next watchdog poll, which is
    /// useful for deterministic tests.
    #[must_use]
    pub const fn deadlock_after(no_progress_timeout: Duration) -> Self {
        Self::Deadlock(DeadlockWatchdogPolicy::new(no_progress_timeout))
    }
}

/// Configuration for no-progress deadlock detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadlockWatchdogPolicy {
    no_progress_timeout: Duration,
}

impl DeadlockWatchdogPolicy {
    /// Create a no-progress watchdog policy.
    ///
    /// A zero duration is allowed and fires on the next watchdog poll, which is
    /// useful for deterministic tests.
    #[must_use]
    pub const fn new(no_progress_timeout: Duration) -> Self {
        Self {
            no_progress_timeout,
        }
    }

    /// Maximum interval allowed without any node reaching a terminal state.
    #[must_use]
    pub const fn no_progress_timeout(&self) -> Duration {
        self.no_progress_timeout
    }
}

/// Diagnostic state reported when the workflow watchdog detects no progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDeadlockDiagnostic {
    workflow_id: WorkflowId,
    scheduled_node_count: usize,
    pending_node_count: usize,
    completed_node_count: usize,
    failed_node_count: usize,
    cancelled_node_count: usize,
    bounded_edge_count: usize,
    no_progress_timeout: Duration,
    cycle_policy: CycleRunPolicy,
}

impl WorkflowDeadlockDiagnostic {
    fn from_run(
        workflow: &WorkflowDefinition,
        summary: &WorkflowRunSummary,
        policy: WorkflowRunPolicy,
        watchdog: DeadlockWatchdogPolicy,
    ) -> Self {
        Self {
            workflow_id: workflow.id().clone(),
            scheduled_node_count: summary.scheduled_node_count(),
            pending_node_count: summary.pending_node_count(),
            completed_node_count: summary.completed_node_count(),
            failed_node_count: summary.failed_node_count(),
            cancelled_node_count: summary.cancelled_node_count(),
            bounded_edge_count: workflow.edges().len(),
            no_progress_timeout: watchdog.no_progress_timeout(),
            cycle_policy: policy.cycle_policy(),
        }
    }

    /// Workflow that stopped making progress.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Nodes scheduled for this run.
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

    /// No-progress interval that elapsed before the watchdog fired.
    #[must_use]
    pub const fn no_progress_timeout(&self) -> Duration {
        self.no_progress_timeout
    }

    /// Cycle policy active when the watchdog fired.
    #[must_use]
    pub const fn cycle_policy(&self) -> CycleRunPolicy {
        self.cycle_policy
    }

    fn to_metadata_diagnostic(&self) -> ErrorDiagnosticMetadata {
        let metadata: DeadlockDiagnosticMetadata = DeadlockDiagnosticMetadata::new(
            self.scheduled_node_count,
            self.pending_node_count,
            self.bounded_edge_count,
            duration_millis_u64(self.no_progress_timeout),
            cycle_run_policy_label(self.cycle_policy),
        )
        .with_terminal_counts(
            self.completed_node_count,
            self.failed_node_count,
            self.cancelled_node_count,
        );

        match self.cycle_policy {
            CycleRunPolicy::Reject => ErrorDiagnosticMetadata::workflow_deadlock(metadata),
            CycleRunPolicy::AllowFeedbackLoops(feedback_loop) => {
                ErrorDiagnosticMetadata::workflow_deadlock(metadata.with_feedback_loop(
                    feedback_loop_startup_label(feedback_loop.startup()),
                    feedback_loop_termination_label(feedback_loop.termination()),
                ))
            }
        }
    }
}

impl fmt::Display for WorkflowDeadlockDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "workflow `{}` watchdog detected no workflow progress for {:?}: scheduled_nodes={}, pending_nodes={}, completed_nodes={}, failed_nodes={}, cancelled_nodes={}, bounded_edges={}, cycle_policy={:?}",
            self.workflow_id,
            self.no_progress_timeout,
            self.scheduled_node_count,
            self.pending_node_count,
            self.completed_node_count,
            self.failed_node_count,
            self.cancelled_node_count,
            self.bounded_edge_count,
            self.cycle_policy
        )
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

const fn cycle_run_policy_label(policy: CycleRunPolicy) -> &'static str {
    match policy {
        CycleRunPolicy::Reject => "reject",
        CycleRunPolicy::AllowFeedbackLoops(_feedback_loop) => "allow_feedback_loops",
    }
}

const fn feedback_loop_startup_label(startup: FeedbackLoopStartup) -> &'static str {
    match startup {
        FeedbackLoopStartup::StartAllNodes => "start_all_nodes",
    }
}

const fn feedback_loop_termination_label(termination: FeedbackLoopTermination) -> &'static str {
    match termination {
        FeedbackLoopTermination::AllNodesComplete => "all_nodes_complete",
    }
}

/// Terminal state for one workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowTerminalState {
    /// Every scheduled node completed successfully.
    Completed,
    /// At least one node failed with an execution, lifecycle, metadata, or
    /// validation error.
    Failed,
    /// The run terminated because cancellation was the first observed error.
    Cancelled,
}

/// Aggregate outcome for one workflow run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRunSummary {
    terminal_state: WorkflowTerminalState,
    scheduled_node_count: usize,
    completed_node_count: usize,
    failed_node_count: usize,
    cancelled_node_count: usize,
    observed_message_count: usize,
    error_count: usize,
    first_error: Option<PureflowError>,
    deadlock_diagnostic: Option<WorkflowDeadlockDiagnostic>,
}

impl WorkflowRunSummary {
    /// Start an empty summary for a run with `scheduled_node_count` nodes.
    #[must_use]
    pub const fn new(scheduled_node_count: usize) -> Self {
        Self {
            terminal_state: WorkflowTerminalState::Completed,
            scheduled_node_count,
            completed_node_count: 0,
            failed_node_count: 0,
            cancelled_node_count: 0,
            observed_message_count: 0,
            error_count: 0,
            first_error: None,
            deadlock_diagnostic: None,
        }
    }

    /// Terminal state after all scheduled node runs were observed.
    #[must_use]
    pub const fn terminal_state(&self) -> WorkflowTerminalState {
        self.terminal_state
    }

    /// Number of nodes scheduled for execution.
    #[must_use]
    pub const fn scheduled_node_count(&self) -> usize {
        self.scheduled_node_count
    }

    /// Number of nodes that completed successfully.
    #[must_use]
    pub const fn completed_node_count(&self) -> usize {
        self.completed_node_count
    }

    /// Number of nodes that returned a non-cancellation error.
    #[must_use]
    pub const fn failed_node_count(&self) -> usize {
        self.failed_node_count
    }

    /// Number of nodes that returned a cancellation error.
    #[must_use]
    pub const fn cancelled_node_count(&self) -> usize {
        self.cancelled_node_count
    }

    /// Number of message observations accounted for by the workflow runner.
    ///
    /// This remains zero until queue-pressure/message accounting is attached to
    /// the runner in the observability tranche.
    #[must_use]
    pub const fn observed_message_count(&self) -> usize {
        self.observed_message_count
    }

    /// Number of node results that ended in an error.
    #[must_use]
    pub const fn error_count(&self) -> usize {
        self.error_count
    }

    /// Number of scheduled nodes that have not yet reached a terminal state.
    #[must_use]
    pub const fn pending_node_count(&self) -> usize {
        self.scheduled_node_count.saturating_sub(
            self.completed_node_count
                .saturating_add(self.failed_node_count)
                .saturating_add(self.cancelled_node_count),
        )
    }

    /// First error observed by the workflow runner, if any.
    #[must_use]
    pub const fn first_error(&self) -> Option<&PureflowError> {
        self.first_error.as_ref()
    }

    /// Deadlock diagnostic captured by the workflow watchdog, if it fired.
    #[must_use]
    pub const fn deadlock_diagnostic(&self) -> Option<&WorkflowDeadlockDiagnostic> {
        self.deadlock_diagnostic.as_ref()
    }

    /// Convert a summary into the legacy `Result<()>` shape.
    ///
    /// # Errors
    ///
    /// Returns the first observed workflow error when the terminal state is not
    /// [`WorkflowTerminalState::Completed`].
    pub fn into_result(self) -> Result<()> {
        self.first_error.map_or(Ok(()), Err)
    }

    const fn record_success(&mut self) {
        self.completed_node_count = self.completed_node_count.saturating_add(1);
    }

    fn record_error(&mut self, err: PureflowError) {
        self.error_count = self.error_count.saturating_add(1);

        if matches!(err, PureflowError::Cancellation(_)) {
            self.cancelled_node_count = self.cancelled_node_count.saturating_add(1);
        } else {
            self.failed_node_count = self.failed_node_count.saturating_add(1);
        }

        if self.first_error.is_none() {
            self.terminal_state = if matches!(err, PureflowError::Cancellation(_)) {
                WorkflowTerminalState::Cancelled
            } else {
                WorkflowTerminalState::Failed
            };
            self.first_error = Some(err);
        }
    }

    fn record_workflow_error(&mut self, err: PureflowError) {
        self.error_count = self.error_count.saturating_add(1);

        if self.first_error.is_none() {
            self.terminal_state = if matches!(err, PureflowError::Cancellation(_)) {
                WorkflowTerminalState::Cancelled
            } else {
                WorkflowTerminalState::Failed
            };
            self.first_error = Some(err);
        }
    }

    fn record_deadlock_diagnostic(&mut self, diagnostic: WorkflowDeadlockDiagnostic) {
        self.deadlock_diagnostic = Some(diagnostic);
    }
}

/// Registry that resolves workflow nodes to runtime executors.
///
/// The registry owns node-to-executor selection while the workflow runner owns
/// graph wiring, cancellation, lifecycle, and metadata behavior.
pub trait NodeExecutorRegistry: Sync {
    /// Concrete executor type returned for nodes in this registry.
    type Executor: NodeExecutor + ?Sized;

    /// Resolve an executor for one workflow node.
    ///
    /// # Errors
    ///
    /// Returns an error when no executor is registered for the node.
    fn executor_for(&self, node_id: &NodeId) -> Result<&Self::Executor>;
}

/// Registry adapter that runs every workflow node through the same executor.
#[derive(Debug, Clone, Copy)]
pub struct SingleNodeExecutorRegistry<'a, E: ?Sized> {
    executor: &'a E,
}

impl<'a, E: ?Sized> SingleNodeExecutorRegistry<'a, E> {
    /// Create a registry that resolves every node to `executor`.
    #[must_use]
    pub const fn new(executor: &'a E) -> Self {
        Self { executor }
    }
}

impl<E> NodeExecutorRegistry for SingleNodeExecutorRegistry<'_, E>
where
    E: NodeExecutor + ?Sized,
{
    type Executor = E;

    fn executor_for(&self, _node_id: &NodeId) -> Result<&Self::Executor> {
        Ok(self.executor)
    }
}

/// In-memory registry keyed by workflow node identifier.
#[derive(Debug, Clone)]
pub struct StaticNodeExecutorRegistry<E> {
    executors: BTreeMap<NodeId, E>,
}

impl<E> StaticNodeExecutorRegistry<E> {
    /// Create a static registry from a node-to-executor map.
    #[must_use]
    pub const fn new(executors: BTreeMap<NodeId, E>) -> Self {
        Self { executors }
    }

    /// Return the registered executor map.
    #[must_use]
    pub const fn executors(&self) -> &BTreeMap<NodeId, E> {
        &self.executors
    }

    /// Insert or replace the executor for one node.
    pub fn insert(&mut self, node_id: NodeId, executor: E) -> Option<E> {
        self.executors.insert(node_id, executor)
    }
}

impl<E> NodeExecutorRegistry for StaticNodeExecutorRegistry<E>
where
    E: NodeExecutor,
{
    type Executor = E;

    fn executor_for(&self, node_id: &NodeId) -> Result<&Self::Executor> {
        self.executors.get(node_id).ok_or_else(|| {
            PureflowError::execution(format!(
                "no executor registered for workflow node `{node_id}`"
            ))
        })
    }
}

/// Node executor adapter for host-owned batch implementations such as WASM.
///
/// The adapter owns the host side of the batch boundary: it drains all
/// currently connected input edges, invokes the topology-blind batch executor,
/// and sends every returned packet through normal [`PortsOut`] validation
/// before a packet can enter graph edges.
#[derive(Debug)]
pub struct BatchNodeExecutor<E> {
    executor: E,
}

impl<E> BatchNodeExecutor<E> {
    /// Wrap one batch executor as a runtime node executor.
    #[must_use]
    pub const fn new(executor: E) -> Self {
        Self { executor }
    }

    /// Borrow the wrapped batch executor.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E> NodeExecutor for BatchNodeExecutor<E>
where
    E: BatchExecutor,
{
    type RunFuture<'a>
        = BoxFuture<'a, Result<()>>
    where
        Self: 'a;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(run_batch_node_executor(
            &self.executor,
            ctx,
            inputs,
            outputs,
        ))
    }
}

async fn run_batch_node_executor<E>(
    executor: &E,
    ctx: NodeContext,
    mut inputs: PortsIn,
    outputs: PortsOut,
) -> Result<()>
where
    E: BatchExecutor,
{
    let cancellation: CancellationToken = ctx.cancellation_token();
    let mut batch_inputs: BatchInputs = BatchInputs::new();
    while let Some((port_id, packet)) = inputs.recv_any(&cancellation).await? {
        batch_inputs.push(port_id, packet);
    }

    let batch_outputs: BatchOutputs = executor.invoke(batch_inputs)?;
    send_batch_outputs(&outputs, batch_outputs, &cancellation).await
}

async fn send_batch_outputs(
    outputs: &PortsOut,
    batch_outputs: BatchOutputs,
    cancellation: &CancellationToken,
) -> Result<()> {
    for (port_id, packets) in batch_outputs.into_packets_by_port() {
        send_batch_output_port(outputs, &port_id, packets, cancellation).await?;
    }
    Ok(())
}

async fn send_batch_output_port(
    outputs: &PortsOut,
    port_id: &PortId,
    packets: Vec<PortPacket>,
    cancellation: &CancellationToken,
) -> Result<()> {
    for packet in packets {
        outputs.send(port_id, packet, cancellation).await?;
    }
    Ok(())
}

/// Output-port contract subset used by the workflow runner.
#[derive(Debug, Clone)]
pub struct WorkflowOutputContracts {
    outputs_by_node: BTreeMap<NodeId, BTreeMap<PortId, Option<SchemaRef>>>,
}

impl WorkflowOutputContracts {
    /// Build output validation contracts for a workflow.
    ///
    /// # Errors
    ///
    /// Returns an error when any workflow output port lacks a matching output
    /// contract or a contract references an unknown workflow output.
    pub fn from_node_contracts(
        workflow: &WorkflowDefinition,
        contracts: &[NodeContract],
    ) -> Result<Self> {
        let contract_map: BTreeMap<&NodeId, &NodeContract> = contracts
            .iter()
            .map(|contract: &NodeContract| (contract.id(), contract))
            .collect();
        let mut outputs_by_node: BTreeMap<NodeId, BTreeMap<PortId, Option<SchemaRef>>> =
            BTreeMap::new();

        for node in workflow.nodes() {
            let Some(contract): Option<&NodeContract> = contract_map.get(node.id()).copied() else {
                if node.output_ports().is_empty() {
                    outputs_by_node.insert(node.id().clone(), BTreeMap::new());
                    continue;
                }

                return Err(PureflowError::execution(format!(
                    "no output contract supplied for workflow node `{}`",
                    node.id()
                )));
            };
            let mut output_contracts: BTreeMap<PortId, Option<SchemaRef>> = BTreeMap::new();

            for port_id in node.output_ports() {
                let port_contract: &PortContract = contract
                    .ports()
                    .iter()
                    .find(|port: &&PortContract| port.port_id() == port_id)
                    .ok_or_else(|| {
                        PureflowError::execution(format!(
                            "node `{}` output port `{port_id}` has no output contract",
                            node.id()
                        ))
                    })?;
                if port_contract.direction() != PortDirection::Output {
                    return Err(PureflowError::execution(format!(
                        "node `{}` port `{port_id}` contract is not an output contract",
                        node.id()
                    )));
                }
                output_contracts.insert(port_id.clone(), port_contract.schema().cloned());
            }

            for port_contract in contract.ports() {
                if port_contract.direction() == PortDirection::Output
                    && !node.output_ports().contains(port_contract.port_id())
                {
                    return Err(PureflowError::execution(format!(
                        "node `{}` contract references unknown output port `{}`",
                        node.id(),
                        port_contract.port_id()
                    )));
                }
            }

            outputs_by_node.insert(node.id().clone(), output_contracts);
        }

        for contract in contracts {
            if workflow
                .nodes()
                .iter()
                .all(|node: &NodeDefinition| node.id() != contract.id())
            {
                return Err(PureflowError::execution(format!(
                    "output contract references unknown workflow node `{}`",
                    contract.id()
                )));
            }
        }

        Ok(Self { outputs_by_node })
    }

    fn output_contracts_for(
        &self,
        node_id: &NodeId,
    ) -> Option<&BTreeMap<PortId, Option<SchemaRef>>> {
        self.outputs_by_node.get(node_id)
    }
}

#[derive(Debug, Clone)]
struct ContractOutputValidator {
    workflow_id: WorkflowId,
    node_id: NodeId,
    execution: ExecutionMetadata,
    output_contracts: BTreeMap<PortId, Option<SchemaRef>>,
}

impl ContractOutputValidator {
    const fn new(
        workflow_id: WorkflowId,
        node_id: NodeId,
        execution: ExecutionMetadata,
        output_contracts: BTreeMap<PortId, Option<SchemaRef>>,
    ) -> Self {
        Self {
            workflow_id,
            node_id,
            execution,
            output_contracts,
        }
    }

    fn reject(port_id: &PortId, reason: impl Into<String>) -> PortSendError {
        PortSendError::Rejected {
            port_id: port_id.clone(),
            reason: reason.into(),
        }
    }
}

impl OutputPacketValidator for ContractOutputValidator {
    fn validate(
        &self,
        port_id: &PortId,
        packet: &PortPacket,
    ) -> std::result::Result<(), PortSendError> {
        if !self.output_contracts.contains_key(port_id) {
            return Err(Self::reject(
                port_id,
                format!(
                    "node `{}` output port `{port_id}` has no output contract",
                    self.node_id
                ),
            ));
        }

        if packet.metadata().workflow_id() != &self.workflow_id {
            return Err(Self::reject(
                port_id,
                format!(
                    "packet workflow `{}` does not match workflow `{}`",
                    packet.metadata().workflow_id(),
                    self.workflow_id
                ),
            ));
        }

        if packet.metadata().execution() != &self.execution {
            return Err(Self::reject(
                port_id,
                format!(
                    "packet execution `{}` does not match execution `{}`",
                    packet.metadata().execution().execution_id(),
                    self.execution.execution_id()
                ),
            ));
        }

        let Some(source): Option<&MessageEndpoint> = packet.metadata().route().source() else {
            return Err(Self::reject(port_id, "packet route has no source endpoint"));
        };
        if source.node_id() != &self.node_id || source.port_id() != port_id {
            return Err(Self::reject(
                port_id,
                format!(
                    "packet source `{}:{}` does not match output `{}:{port_id}`",
                    source.node_id(),
                    source.port_id(),
                    self.node_id
                ),
            ));
        }

        Ok(())
    }
}

/// Execute the workflow by resolving one executor for each node from a registry.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// executor resolution fails.
pub async fn run_workflow_with_registry_summary<R>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
{
    let lifecycle_hook: NoopLifecycleHook = NoopLifecycleHook;
    run_workflow_with_registry_and_observers_summary(
        workflow,
        execution,
        registry,
        &lifecycle_hook,
        Arc::new(NoopMetadataSink),
    )
    .await
}

/// Execute the workflow through a registry with an explicit run policy.
///
/// # Errors
///
/// Returns an error if the run policy rejects the workflow shape or executor
/// resolution fails.
pub async fn run_workflow_with_registry_policy_summary<R>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    policy: WorkflowRunPolicy,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
{
    let lifecycle_hook: NoopLifecycleHook = NoopLifecycleHook;
    run_workflow_with_registry_and_observers_summary_inner(
        workflow,
        execution,
        registry,
        &lifecycle_hook,
        Arc::new(NoopMetadataSink),
        policy,
        None,
    )
    .await
}

/// Execute the workflow by resolving one executor for each node from a registry.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, or any node execution fails.
pub async fn run_workflow_with_registry<R>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
{
    run_workflow_with_registry_summary(workflow, execution, registry)
        .await?
        .into_result()
}

/// Execute the workflow through a registry with an explicit run policy.
///
/// # Errors
///
/// Returns an error if the run policy rejects the workflow shape, executor
/// resolution fails, or any node execution fails.
pub async fn run_workflow_with_registry_policy<R>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    policy: WorkflowRunPolicy,
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
{
    run_workflow_with_registry_policy_summary(workflow, execution, registry, policy)
        .await?
        .into_result()
}

/// Execute the workflow through a registry with an explicit run policy and
/// emit metadata records.
///
/// # Errors
///
/// Returns an error if the run policy rejects the workflow shape or executor
/// resolution fails.
pub async fn run_workflow_with_registry_policy_and_metadata_sink_summary<R, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    policy: WorkflowRunPolicy,
    metadata_sink: Arc<M>,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
    M: MetadataSink + 'static,
{
    let lifecycle_hook: NoopLifecycleHook = NoopLifecycleHook;
    run_workflow_with_registry_and_observers_summary_inner(
        workflow,
        execution,
        registry,
        &lifecycle_hook,
        metadata_sink,
        policy,
        None,
    )
    .await
}

/// Execute the workflow through a registry with an explicit run policy and
/// emit metadata records.
///
/// # Errors
///
/// Returns an error if the run policy rejects the workflow shape, executor
/// resolution fails, metadata collection fails, or any node execution fails.
pub async fn run_workflow_with_registry_policy_and_metadata_sink<R, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    policy: WorkflowRunPolicy,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
    M: MetadataSink + 'static,
{
    run_workflow_with_registry_policy_and_metadata_sink_summary(
        workflow,
        execution,
        registry,
        policy,
        metadata_sink,
    )
    .await?
    .into_result()
}

/// Execute the workflow through a registry and emit metadata records.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// executor resolution fails.
pub async fn run_workflow_with_registry_and_metadata_sink_summary<R, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    metadata_sink: Arc<M>,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
    M: MetadataSink + 'static,
{
    let lifecycle_hook: NoopLifecycleHook = NoopLifecycleHook;
    run_workflow_with_registry_and_observers_summary(
        workflow,
        execution,
        registry,
        &lifecycle_hook,
        metadata_sink,
    )
    .await
}

/// Execute the workflow through a registry and report observer records.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// executor resolution fails.
pub async fn run_workflow_with_registry_and_observers_summary<R, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    run_workflow_with_registry_and_observers_summary_inner(
        workflow,
        execution,
        registry,
        lifecycle_hook,
        metadata_sink,
        WorkflowRunPolicy::default(),
        None,
    )
    .await
}

async fn run_workflow_with_registry_and_observers_summary_inner<R, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
    policy: WorkflowRunPolicy,
    output_contracts: Option<&WorkflowOutputContracts>,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    validate_workflow_run_policy(workflow, policy)?;

    let (mut inputs_by_node, mut outputs_by_node): PortWiring = build_port_wiring(workflow);
    let cancellation: CancellationHandle = CancellationHandle::new();

    let node_runs: FuturesUnordered<_> = FuturesUnordered::new();
    for node in workflow.nodes() {
        let executor: &R::Executor = registry.executor_for(node.id())?;
        let node_id: NodeId = node.id().clone();
        let ctx: NodeContext =
            NodeContext::new(workflow.id().clone(), node_id.clone(), execution.clone())
                .with_cancellation_token(cancellation.token());
        let inputs: PortsIn = PortsIn::from_handles(
            node.input_ports().to_vec(),
            inputs_by_node.remove(node.id()).unwrap_or_default(),
        );
        let mut outputs: PortsOut = PortsOut::from_handles(
            node.output_ports().to_vec(),
            outputs_by_node.remove(node.id()).unwrap_or_default(),
        );
        if let Some(output_contracts) = output_contracts {
            let node_output_contracts: BTreeMap<PortId, Option<SchemaRef>> = output_contracts
                .output_contracts_for(node.id())
                .cloned()
                .ok_or_else(|| {
                    PureflowError::execution(format!(
                        "no output contracts supplied for workflow node `{}`",
                        node.id()
                    ))
                })?;
            outputs = outputs.with_output_validator(Arc::new(ContractOutputValidator::new(
                workflow.id().clone(),
                node_id.clone(),
                execution.clone(),
                node_output_contracts,
            )));
        }
        let metadata_sink: Arc<M> = metadata_sink.clone();
        node_runs.push(async move {
            let result: Result<()> = run_node_with_observers(
                executor,
                ctx,
                inputs,
                outputs,
                lifecycle_hook,
                metadata_sink,
            )
            .await;
            (node_id, result)
        });
    }

    collect_workflow_summary(
        node_runs,
        WorkflowCollectionContext::new(
            &cancellation,
            workflow,
            execution,
            metadata_sink.as_ref(),
            policy,
        ),
        workflow.nodes().len(),
    )
    .await
}

/// Execute the workflow through a registry with output contract validation.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, or output contract setup fails.
pub async fn run_workflow_with_registry_contracts_and_observers_summary<R, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    contracts: &[NodeContract],
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    let output_contracts: WorkflowOutputContracts =
        WorkflowOutputContracts::from_node_contracts(workflow, contracts)?;
    run_workflow_with_registry_and_observers_summary_inner(
        workflow,
        execution,
        registry,
        lifecycle_hook,
        metadata_sink,
        WorkflowRunPolicy::default(),
        Some(&output_contracts),
    )
    .await
}

/// Execute the workflow through a registry with output contract validation.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, output contract setup fails, output validation
/// fails, observation fails, metadata collection fails, or node execution fails.
pub async fn run_workflow_with_registry_contracts_and_observers<R, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    contracts: &[NodeContract],
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    run_workflow_with_registry_contracts_and_observers_summary(
        workflow,
        execution,
        registry,
        contracts,
        lifecycle_hook,
        metadata_sink,
    )
    .await?
    .into_result()
}

/// Execute the workflow through a registry with output contract validation.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, or output contract setup fails.
pub async fn run_workflow_with_registry_contracts_summary<R>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    contracts: &[NodeContract],
) -> Result<WorkflowRunSummary>
where
    R: NodeExecutorRegistry + ?Sized,
{
    let lifecycle_hook: NoopLifecycleHook = NoopLifecycleHook;
    run_workflow_with_registry_contracts_and_observers_summary(
        workflow,
        execution,
        registry,
        contracts,
        &lifecycle_hook,
        Arc::new(NoopMetadataSink),
    )
    .await
}

/// Execute the workflow through a registry with output contract validation.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, output contract setup fails, output validation
/// fails, or node execution fails.
pub async fn run_workflow_with_registry_contracts<R>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    contracts: &[NodeContract],
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
{
    run_workflow_with_registry_contracts_summary(workflow, execution, registry, contracts)
        .await?
        .into_result()
}

/// Execute the workflow through a registry and report observer records.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, observation fails, metadata collection fails, or
/// node execution fails.
pub async fn run_workflow_with_registry_and_observers<R, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    run_workflow_with_registry_and_observers_summary(
        workflow,
        execution,
        registry,
        lifecycle_hook,
        metadata_sink,
    )
    .await?
    .into_result()
}

/// Execute the workflow through a registry and emit metadata records.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// executor resolution fails, metadata collection fails, or node execution
/// fails.
pub async fn run_workflow_with_registry_and_metadata_sink<R, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    registry: &R,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    R: NodeExecutorRegistry + ?Sized,
    M: MetadataSink + 'static,
{
    run_workflow_with_registry_and_metadata_sink_summary(
        workflow,
        execution,
        registry,
        metadata_sink,
    )
    .await?
    .into_result()
}

/// Execute the workflow by invoking the provided executor for each node.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// any node execution fails.
pub async fn run_workflow<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
) -> Result<()> {
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry(workflow, execution, &registry).await
}

/// Execute the workflow through one executor and return an aggregate summary.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// registry-style executor setup fails.
pub async fn run_workflow_summary<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
) -> Result<WorkflowRunSummary> {
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_summary(workflow, execution, &registry).await
}

/// Execute the workflow through one executor with an explicit run policy.
///
/// # Errors
///
/// Returns an error if the run policy rejects the workflow shape or
/// registry-style executor setup fails.
pub async fn run_workflow_with_policy_summary<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    policy: WorkflowRunPolicy,
) -> Result<WorkflowRunSummary> {
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_policy_summary(workflow, execution, &registry, policy).await
}

/// Execute the workflow through one executor with an explicit run policy.
///
/// # Errors
///
/// Returns an error if the run policy rejects the workflow shape or any node
/// execution fails.
pub async fn run_workflow_with_policy<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    policy: WorkflowRunPolicy,
) -> Result<()> {
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_policy(workflow, execution, &registry, policy).await
}

/// Execute the workflow through one executor and report observer records.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// registry-style executor setup fails.
pub async fn run_workflow_with_observers_summary<E, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
) -> Result<WorkflowRunSummary>
where
    E: NodeExecutor + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_and_observers_summary(
        workflow,
        execution,
        &registry,
        lifecycle_hook,
        metadata_sink,
    )
    .await
}

/// Execute the workflow through one executor and report observer records.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// observation fails, metadata collection fails, or node execution fails.
pub async fn run_workflow_with_observers<E, H, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    lifecycle_hook: &H,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    E: NodeExecutor + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_and_observers(
        workflow,
        execution,
        &registry,
        lifecycle_hook,
        metadata_sink,
    )
    .await
}

/// Execute the workflow through one executor with output contract validation.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// output contract setup fails.
pub async fn run_workflow_with_contracts_summary<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    contracts: &[NodeContract],
) -> Result<WorkflowRunSummary> {
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_contracts_summary(workflow, execution, &registry, contracts).await
}

/// Execute the workflow through one executor with output contract validation.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// output contract setup fails, output validation fails, or node execution
/// fails.
pub async fn run_workflow_with_contracts<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    contracts: &[NodeContract],
) -> Result<()> {
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_contracts(workflow, execution, &registry, contracts).await
}

/// Execute the workflow with one executor and emit metadata records through a sink.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape,
/// metadata collection fails, or any node execution fails.
pub async fn run_workflow_with_metadata_sink<E, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    E: NodeExecutor + ?Sized,
    M: MetadataSink + 'static,
{
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_and_metadata_sink(workflow, execution, &registry, metadata_sink)
        .await
}

/// Execute the workflow with one executor, emit metadata, and return a summary.
///
/// # Errors
///
/// Returns an error if the default run policy rejects the workflow shape or
/// registry-style executor setup fails.
pub async fn run_workflow_with_metadata_sink_summary<E, M>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
    metadata_sink: Arc<M>,
) -> Result<WorkflowRunSummary>
where
    E: NodeExecutor + ?Sized,
    M: MetadataSink + 'static,
{
    let registry: SingleNodeExecutorRegistry<'_, E> = SingleNodeExecutorRegistry::new(executor);
    run_workflow_with_registry_and_metadata_sink_summary(
        workflow,
        execution,
        &registry,
        metadata_sink,
    )
    .await
}

#[derive(Clone, Copy)]
struct WorkflowCollectionContext<'a> {
    cancellation: &'a CancellationHandle,
    workflow: &'a WorkflowDefinition,
    execution: &'a ExecutionMetadata,
    metadata_sink: &'a dyn MetadataSink,
    policy: WorkflowRunPolicy,
}

impl<'a> WorkflowCollectionContext<'a> {
    const fn new(
        cancellation: &'a CancellationHandle,
        workflow: &'a WorkflowDefinition,
        execution: &'a ExecutionMetadata,
        metadata_sink: &'a dyn MetadataSink,
        policy: WorkflowRunPolicy,
    ) -> Self {
        Self {
            cancellation,
            workflow,
            execution,
            metadata_sink,
            policy,
        }
    }
}

async fn collect_workflow_summary<F>(
    mut node_runs: FuturesUnordered<F>,
    context: WorkflowCollectionContext<'_>,
    scheduled_node_count: usize,
) -> Result<WorkflowRunSummary>
where
    F: Future<Output = (NodeId, Result<()>)>,
{
    let mut summary: WorkflowRunSummary = WorkflowRunSummary::new(scheduled_node_count);

    match context.policy.watchdog_policy() {
        WorkflowWatchdogPolicy::Disabled => {
            collect_workflow_summary_until_complete(&mut node_runs, context, &mut summary).await?;
        }
        WorkflowWatchdogPolicy::Deadlock(watchdog) => {
            collect_workflow_summary_with_deadlock_watchdog(
                &mut node_runs,
                context,
                watchdog,
                &mut summary,
            )
            .await?;
        }
    }

    Ok(summary)
}

async fn collect_workflow_summary_until_complete<F>(
    node_runs: &mut FuturesUnordered<F>,
    context: WorkflowCollectionContext<'_>,
    summary: &mut WorkflowRunSummary,
) -> Result<()>
where
    F: Future<Output = (NodeId, Result<()>)>,
{
    while let Some((_node_id, result)) = node_runs.next().await {
        record_node_run_result(context, summary, result)?;
    }

    Ok(())
}

async fn collect_workflow_summary_with_deadlock_watchdog<F>(
    node_runs: &mut FuturesUnordered<F>,
    context: WorkflowCollectionContext<'_>,
    watchdog: DeadlockWatchdogPolicy,
    summary: &mut WorkflowRunSummary,
) -> Result<()>
where
    F: Future<Output = (NodeId, Result<()>)>,
{
    loop {
        if node_runs.is_empty() {
            return Ok(());
        }

        let next_node_result: Next<'_, FuturesUnordered<F>> = node_runs.next();
        let watchdog_deadline: BoxFuture<'static, ()> =
            deadlock_watchdog_deadline(watchdog.no_progress_timeout())?;
        futures::pin_mut!(next_node_result);
        futures::pin_mut!(watchdog_deadline);

        match select(next_node_result, watchdog_deadline).await {
            Either::Left((Some((_node_id, result)), _deadline)) => {
                record_node_run_result(context, summary, result)?;
            }
            Either::Left((None, _deadline)) => return Ok(()),
            Either::Right(((), _next_node_result)) => {
                let diagnostic: WorkflowDeadlockDiagnostic = WorkflowDeadlockDiagnostic::from_run(
                    context.workflow,
                    summary,
                    context.policy,
                    watchdog,
                );
                let err: PureflowError = PureflowError::execution(diagnostic.to_string());
                record_first_workflow_error_with_diagnostic(context, &err, &diagnostic)?;
                summary.record_workflow_error(err);
                summary.record_deadlock_diagnostic(diagnostic);
                return Ok(());
            }
        }
    }
}

fn record_node_run_result(
    context: WorkflowCollectionContext<'_>,
    summary: &mut WorkflowRunSummary,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => summary.record_success(),
        Err(err) => {
            if summary.first_error().is_none() {
                record_first_workflow_error(context, &err)?;
            }
            summary.record_error(err);
        }
    }

    Ok(())
}

fn record_first_workflow_error(
    context: WorkflowCollectionContext<'_>,
    err: &PureflowError,
) -> Result<()> {
    let record: MetadataRecord = MetadataRecord::Error(ErrorMetadataRecord::workflow_failed(
        context.workflow.id().clone(),
        context.execution.clone(),
        err.clone(),
    ));
    context.metadata_sink.record(&record)?;
    let _first_request: bool = context
        .cancellation
        .cancel(CancellationRequest::new(format!(
            "node execution failed: {err}"
        )));
    Ok(())
}

fn record_first_workflow_error_with_diagnostic(
    context: WorkflowCollectionContext<'_>,
    err: &PureflowError,
    diagnostic: &WorkflowDeadlockDiagnostic,
) -> Result<()> {
    let record: MetadataRecord =
        MetadataRecord::Error(ErrorMetadataRecord::workflow_failed_with_diagnostic(
            context.workflow.id().clone(),
            context.execution.clone(),
            err.clone(),
            diagnostic.to_metadata_diagnostic(),
        ));
    context.metadata_sink.record(&record)?;
    let _first_request: bool = context
        .cancellation
        .cancel(CancellationRequest::new(format!(
            "node execution failed: {err}"
        )));
    Ok(())
}

fn deadlock_watchdog_deadline(timeout: Duration) -> Result<BoxFuture<'static, ()>> {
    let (sender, receiver): (oneshot::Sender<()>, oneshot::Receiver<()>) = oneshot::channel();
    std::thread::Builder::new()
        .name(String::from("pureflow-deadlock-watchdog"))
        .spawn(move || {
            std::thread::sleep(timeout);
            let _send_result: std::result::Result<(), ()> = sender.send(());
        })
        .map_err(|err: std::io::Error| {
            PureflowError::execution(format!("failed to start workflow deadlock watchdog: {err}"))
        })?;

    Ok(Box::pin(async move {
        let _deadline_result: std::result::Result<(), oneshot::Canceled> = receiver.await;
    }))
}

fn validate_workflow_run_policy(
    workflow: &WorkflowDefinition,
    policy: WorkflowRunPolicy,
) -> Result<()> {
    match workflow.graph().topological_order() {
        Ok(_order) => Ok(()),
        Err(WorkflowValidationError::CycleDetected { cycle }) => {
            validate_cycle_run_policy(workflow, policy, &cycle)
        }
        Err(err) => Err(PureflowError::execution(format!(
            "workflow `{}` topology validation failed before execution: {err}",
            workflow.id()
        ))),
    }
}

fn validate_cycle_run_policy(
    workflow: &WorkflowDefinition,
    policy: WorkflowRunPolicy,
    cycle: &[NodeId],
) -> Result<()> {
    match policy.cycle_policy() {
        CycleRunPolicy::Reject => Err(PureflowError::execution(format!(
            "workflow `{}` contains directed cycle {}; use an explicit feedback-loop run policy to execute cyclic graphs",
            workflow.id(),
            cycle_label(cycle)
        ))),
        CycleRunPolicy::AllowFeedbackLoops(feedback_loop) => {
            match (feedback_loop.startup(), feedback_loop.termination()) {
                (FeedbackLoopStartup::StartAllNodes, FeedbackLoopTermination::AllNodesComplete) => {
                    Ok(())
                }
            }
        }
    }
}

fn cycle_label(cycle: &[NodeId]) -> String {
    let mut label: String = String::new();
    for (index, node_id) in cycle.iter().enumerate() {
        if index > 0 {
            label.push_str(" -> ");
        }
        label.push_str(node_id.as_str());
    }
    label
}

type PortWiring = (
    BTreeMap<NodeId, Vec<InputPortHandle>>,
    BTreeMap<NodeId, Vec<OutputPortHandle>>,
);

fn build_port_wiring(workflow: &WorkflowDefinition) -> PortWiring {
    let mut inputs_by_node: BTreeMap<NodeId, Vec<InputPortHandle>> = BTreeMap::new();
    let mut outputs_by_node: BTreeMap<NodeId, Vec<OutputPortHandle>> = BTreeMap::new();

    for edge in workflow.edges() {
        let capacity: NonZeroUsize = edge.capacity().resolve(DEFAULT_EDGE_CAPACITY);
        let (output, input): (OutputPortHandle, InputPortHandle) = bounded_edge_channel(
            edge.source().port_id().clone(),
            edge.target().port_id().clone(),
            capacity,
        );
        outputs_by_node
            .entry(edge.source().node_id().clone())
            .or_default()
            .push(output);
        inputs_by_node
            .entry(edge.target().node_id().clone())
            .or_default()
            .push(input);
    }

    (inputs_by_node, outputs_by_node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        future::{Ready, ready},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use pureflow_contract::{Determinism, ExecutionMode, PortContract, SchemaRef};
    use pureflow_core::{
        BatchExecutor, BatchInputs, BatchOutputs, PureflowError, ErrorCode, ErrorDiagnosticMetadata,
        ErrorMetadataKind, MetadataRecord, MetadataSink, PacketPayload, PortPacket, PortRecvError,
        PortSendError, RetryDisposition,
        lifecycle::{LifecycleEvent, LifecycleEventKind},
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use pureflow_test_kit::{
        FailingExecutor, NodeBuilder, RecordingExecutor, WorkflowBuilder, execution_metadata,
        node_id, port_id, workflow_id,
    };
    use pureflow_types::{ExecutionId, MessageId};
    use pureflow_workflow::EdgeDefinition;
    use futures::channel::oneshot;
    use futures::executor::block_on;
    use futures::future::BoxFuture;

    #[derive(Debug, Default)]
    struct ChannelExecutor {
        received: Mutex<Vec<Vec<u8>>>,
    }

    impl ChannelExecutor {
        fn received_payloads(&self) -> Vec<Vec<u8>> {
            self.received
                .lock()
                .expect("channel executor lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for ChannelExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                if ctx.node_id().as_str() == "source" {
                    let cancellation = ctx.cancellation_token();
                    outputs
                        .send(&port_id("out"), packet(b"hello"), &cancellation)
                        .await?;
                    outputs
                        .send(&port_id("out"), packet(b"world"), &cancellation)
                        .await?;
                } else if ctx.node_id().as_str() == "sink" {
                    let cancellation = ctx.cancellation_token();
                    for _packet_index in 0..2 {
                        let packet: PortPacket = inputs
                            .recv(&port_id("in"), &cancellation)
                            .await?
                            .expect("source should have queued a packet");
                        self.received
                            .lock()
                            .expect("channel executor lock should not be poisoned")
                            .push(
                                packet
                                    .into_payload()
                                    .as_bytes()
                                    .expect("channel test sends bytes")
                                    .to_vec(),
                            );
                    }
                }

                Ok(())
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum RegistryExecutorRole {
        Source,
        Sink,
    }

    #[derive(Debug, Clone)]
    struct RegistryExecutor {
        role: RegistryExecutorRole,
        received: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl RegistryExecutor {
        fn source(received: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
            Self {
                role: RegistryExecutorRole::Source,
                received,
            }
        }

        fn sink(received: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
            Self {
                role: RegistryExecutorRole::Sink,
                received,
            }
        }
    }

    impl NodeExecutor for RegistryExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let cancellation = ctx.cancellation_token();
                match self.role {
                    RegistryExecutorRole::Source => {
                        outputs
                            .send(&port_id("out"), packet(b"registered"), &cancellation)
                            .await?;
                    }
                    RegistryExecutorRole::Sink => {
                        let packet: PortPacket = inputs
                            .recv(&port_id("in"), &cancellation)
                            .await?
                            .expect("registered source should send one packet");
                        self.received
                            .lock()
                            .expect("registry executor lock should not be poisoned")
                            .push(packet_payload_bytes(packet));
                    }
                }

                Ok(())
            })
        }
    }

    #[derive(Debug, Default)]
    struct BoundedBackpressureExecutor {
        events: Mutex<Vec<String>>,
        received: Mutex<Vec<Vec<u8>>>,
    }

    impl BoundedBackpressureExecutor {
        fn events(&self) -> Vec<String> {
            self.events
                .lock()
                .expect("backpressure executor events lock should not be poisoned")
                .clone()
        }

        fn received_payloads(&self) -> Vec<Vec<u8>> {
            self.received
                .lock()
                .expect("backpressure executor received lock should not be poisoned")
                .clone()
        }

        fn push_event(&self, event: &str) {
            self.events
                .lock()
                .expect("backpressure executor events lock should not be poisoned")
                .push(event.to_owned());
        }

        fn push_received(&self, packet: PortPacket) {
            self.received
                .lock()
                .expect("backpressure executor received lock should not be poisoned")
                .push(packet_payload_bytes(packet));
        }
    }

    impl NodeExecutor for BoundedBackpressureExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let cancellation = ctx.cancellation_token();

                match ctx.node_id().as_str() {
                    "source" => {
                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(b"first", "source", "sink"),
                                &cancellation,
                            )
                            .await?;

                        let full_send: std::result::Result<(), PortSendError> = outputs.try_send(
                            &port_id("out"),
                            packet_between(b"blocked", "source", "sink"),
                        );
                        if matches!(full_send, Err(PortSendError::Full { .. })) {
                            self.push_event("source-observed-full-edge");
                        } else {
                            return Err(PureflowError::execution(
                                "bounded edge should reject a second immediate send",
                            ));
                        }

                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(b"second", "source", "sink"),
                                &cancellation,
                            )
                            .await?;
                        self.push_event("source-second-send-completed");
                    }
                    "sink" => {
                        for _packet_index in 0..2 {
                            let packet: PortPacket = inputs
                                .recv(&port_id("in"), &cancellation)
                                .await?
                                .expect("source should send two packets");
                            self.push_received(packet);
                        }
                    }
                    _ => {}
                }

                Ok(())
            })
        }
    }

    #[derive(Debug, Default)]
    struct FanOutExecutor {
        received_by_node: Mutex<BTreeMap<String, Vec<Vec<u8>>>>,
    }

    impl FanOutExecutor {
        fn received_by_node(&self) -> BTreeMap<String, Vec<Vec<u8>>> {
            self.received_by_node
                .lock()
                .expect("fan-out executor lock should not be poisoned")
                .clone()
        }

        fn push_received(&self, node_id: &str, packet: PortPacket) {
            self.received_by_node
                .lock()
                .expect("fan-out executor lock should not be poisoned")
                .entry(node_id.to_owned())
                .or_default()
                .push(packet_payload_bytes(packet));
        }
    }

    impl NodeExecutor for FanOutExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let cancellation = ctx.cancellation_token();

                if ctx.node_id().as_str() == "source" {
                    outputs
                        .send(
                            &port_id("out"),
                            packet_between(b"fan", "source", "left"),
                            &cancellation,
                        )
                        .await?;
                    return Ok(());
                }

                let node_name: String = ctx.node_id().to_string();
                let packet: PortPacket = inputs
                    .recv(&port_id("in"), &cancellation)
                    .await?
                    .expect("fan-out sink should receive one packet");
                self.push_received(&node_name, packet);

                Ok(())
            })
        }
    }

    #[derive(Debug, Default)]
    struct FanInClosureExecutor {
        received: Mutex<Vec<Vec<u8>>>,
        closure_observed: Mutex<bool>,
    }

    impl FanInClosureExecutor {
        fn received_payloads(&self) -> Vec<Vec<u8>> {
            self.received
                .lock()
                .expect("fan-in executor received lock should not be poisoned")
                .clone()
        }

        fn closure_observed(&self) -> bool {
            *self
                .closure_observed
                .lock()
                .expect("fan-in executor closure lock should not be poisoned")
        }

        fn push_received(&self, packet: PortPacket) {
            self.received
                .lock()
                .expect("fan-in executor received lock should not be poisoned")
                .push(packet_payload_bytes(packet));
        }
    }

    impl NodeExecutor for FanInClosureExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let cancellation = ctx.cancellation_token();

                match ctx.node_id().as_str() {
                    "left" | "right" => {
                        let source_node: String = ctx.node_id().to_string();
                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(source_node.as_bytes(), &source_node, "collector"),
                                &cancellation,
                            )
                            .await?;
                    }
                    "collector" => {
                        for _packet_index in 0..2 {
                            let packet: PortPacket = inputs
                                .recv(&port_id("in"), &cancellation)
                                .await?
                                .expect("fan-in collector should receive both packets");
                            self.push_received(packet);
                        }

                        let closed: std::result::Result<Option<PortPacket>, PortRecvError> =
                            inputs.recv(&port_id("in"), &cancellation).await;
                        if matches!(closed, Err(PortRecvError::Disconnected { .. })) {
                            *self
                                .closure_observed
                                .lock()
                                .expect("fan-in executor closure lock should not be poisoned") =
                                true;
                        } else {
                            return Err(PureflowError::execution(
                                "fan-in input should close after upstream senders finish",
                            ));
                        }
                    }
                    _ => {}
                }

                Ok(())
            })
        }
    }

    #[derive(Debug, Default)]
    struct AggregateFailureExecutor {
        visited: Mutex<Vec<String>>,
    }

    impl AggregateFailureExecutor {
        fn visited_node_names(&self) -> Vec<String> {
            self.visited
                .lock()
                .expect("aggregate failure executor lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for AggregateFailureExecutor {
        type RunFuture<'a> = Ready<Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            _inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            self.visited
                .lock()
                .expect("aggregate failure executor lock should not be poisoned")
                .push(ctx.node_id().to_string());

            if ctx.node_id().as_str() == "first" {
                return ready(Err(PureflowError::execution("first failed")));
            }

            ready(Ok(()))
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FailureMatrixRole {
        SourceFails,
        SourceForTransformFailure,
        TransformFails,
        PassthroughTransform,
        SinkWaits,
    }

    #[derive(Debug, Clone, Copy)]
    struct FailureMatrixExecutor {
        role: FailureMatrixRole,
    }

    impl FailureMatrixExecutor {
        const fn source_fails() -> Self {
            Self {
                role: FailureMatrixRole::SourceFails,
            }
        }

        const fn source_for_transform_failure() -> Self {
            Self {
                role: FailureMatrixRole::SourceForTransformFailure,
            }
        }

        const fn transform_fails() -> Self {
            Self {
                role: FailureMatrixRole::TransformFails,
            }
        }

        const fn passthrough_transform() -> Self {
            Self {
                role: FailureMatrixRole::PassthroughTransform,
            }
        }

        const fn sink_waits() -> Self {
            Self {
                role: FailureMatrixRole::SinkWaits,
            }
        }
    }

    impl NodeExecutor for FailureMatrixExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let cancellation = ctx.cancellation_token();

                match self.role {
                    FailureMatrixRole::SourceFails => {
                        Err(PureflowError::execution("matrix source failed"))
                    }
                    FailureMatrixRole::SourceForTransformFailure => outputs
                        .send(
                            &port_id("out"),
                            packet_between(b"source", "source", "transform"),
                            &cancellation,
                        )
                        .await
                        .map_err(PureflowError::from),
                    FailureMatrixRole::TransformFails => {
                        let _packet = inputs.recv(&port_id("in"), &cancellation).await?;
                        Err(PureflowError::execution("matrix transform failed"))
                    }
                    FailureMatrixRole::PassthroughTransform => {
                        let packet = inputs
                            .recv(&port_id("in"), &cancellation)
                            .await?
                            .expect("source should send transform input");
                        outputs
                            .send(&port_id("out"), packet, &cancellation)
                            .await
                            .map_err(PureflowError::from)
                    }
                    FailureMatrixRole::SinkWaits => {
                        let _packet = inputs.recv(&port_id("in"), &cancellation).await?;
                        Ok(())
                    }
                }
            })
        }
    }

    #[derive(Debug)]
    struct DisconnectedDownstreamExecutor {
        role: DisconnectedDownstreamRole,
        signal: Arc<DisconnectedDownstreamSignal>,
    }

    #[derive(Debug, Clone, Copy)]
    enum DisconnectedDownstreamRole {
        Source,
        Sink,
    }

    #[derive(Debug)]
    struct DisconnectedDownstreamSignal {
        sender: Mutex<Option<oneshot::Sender<()>>>,
        receiver: Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl DisconnectedDownstreamExecutor {
        fn pair() -> (Self, Self) {
            let (sender, receiver) = oneshot::channel();
            let signal = Arc::new(DisconnectedDownstreamSignal {
                sender: Mutex::new(Some(sender)),
                receiver: Mutex::new(Some(receiver)),
            });

            (
                Self {
                    role: DisconnectedDownstreamRole::Source,
                    signal: Arc::clone(&signal),
                },
                Self {
                    role: DisconnectedDownstreamRole::Sink,
                    signal,
                },
            )
        }
    }

    impl NodeExecutor for DisconnectedDownstreamExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            _inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                match self.role {
                    DisconnectedDownstreamRole::Source => {
                        let receiver = self
                            .signal
                            .receiver
                            .lock()
                            .expect("disconnect receiver lock should not be poisoned")
                            .take()
                            .expect("source should own disconnect receiver");
                        receiver
                            .await
                            .expect("sink should signal before source sends");
                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(b"late", "source", "sink"),
                                &ctx.cancellation_token(),
                            )
                            .await
                            .map_err(PureflowError::from)
                    }
                    DisconnectedDownstreamRole::Sink => {
                        if let Some(sender) = self
                            .signal
                            .sender
                            .lock()
                            .expect("disconnect sender lock should not be poisoned")
                            .take()
                        {
                            let _send_result = sender.send(());
                        }
                        Ok(())
                    }
                }
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FanOutPartialFailureRole {
        Source,
        GoodSink,
        DroppingSink,
    }

    #[derive(Debug)]
    struct FanOutPartialFailureExecutor {
        role: FanOutPartialFailureRole,
        state: Arc<FanOutPartialFailureState>,
    }

    #[derive(Debug)]
    struct FanOutPartialFailureState {
        dropped_sender: Mutex<Option<oneshot::Sender<()>>>,
        dropped_receiver: Mutex<Option<oneshot::Receiver<()>>>,
        good_sink_observation: Mutex<Option<String>>,
    }

    impl FanOutPartialFailureExecutor {
        fn registry() -> StaticNodeExecutorRegistry<Self> {
            let (dropped_sender, dropped_receiver) = oneshot::channel();
            let state = Arc::new(FanOutPartialFailureState {
                dropped_sender: Mutex::new(Some(dropped_sender)),
                dropped_receiver: Mutex::new(Some(dropped_receiver)),
                good_sink_observation: Mutex::new(None),
            });

            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("source"),
                    Self {
                        role: FanOutPartialFailureRole::Source,
                        state: Arc::clone(&state),
                    },
                ),
                (
                    node_id("good"),
                    Self {
                        role: FanOutPartialFailureRole::GoodSink,
                        state: Arc::clone(&state),
                    },
                ),
                (
                    node_id("drop"),
                    Self {
                        role: FanOutPartialFailureRole::DroppingSink,
                        state,
                    },
                ),
            ]))
        }

        fn good_sink_observation(&self) -> Option<String> {
            self.state
                .good_sink_observation
                .lock()
                .expect("fan-out partial observation lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for FanOutPartialFailureExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                match self.role {
                    FanOutPartialFailureRole::Source => {
                        let receiver = self
                            .state
                            .dropped_receiver
                            .lock()
                            .expect("fan-out dropped receiver lock should not be poisoned")
                            .take()
                            .expect("source should own dropped receiver");
                        receiver
                            .await
                            .expect("dropping sink should signal before source sends");
                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(b"fan", "source", "good"),
                                &ctx.cancellation_token(),
                            )
                            .await
                            .map_err(PureflowError::from)
                    }
                    FanOutPartialFailureRole::GoodSink => {
                        let observation =
                            match inputs.recv(&port_id("in"), &ctx.cancellation_token()).await {
                                Ok(Some(_packet)) => "unexpected_packet",
                                Ok(None) => "closed_without_packet",
                                Err(PortRecvError::Cancelled { .. }) => "cancelled_without_packet",
                                Err(PortRecvError::Disconnected { .. }) => {
                                    "disconnected_without_packet"
                                }
                                Err(PortRecvError::UnknownPort { .. }) => "unknown_port",
                            };
                        *self
                            .state
                            .good_sink_observation
                            .lock()
                            .expect("fan-out partial observation lock should not be poisoned") =
                            Some(observation.to_owned());
                        if observation == "unexpected_packet" {
                            return Err(PureflowError::execution(
                                "fan-out partial send delivered to good sink",
                            ));
                        }
                        Ok(())
                    }
                    FanOutPartialFailureRole::DroppingSink => {
                        if let Some(sender) = self
                            .state
                            .dropped_sender
                            .lock()
                            .expect("fan-out dropped sender lock should not be poisoned")
                            .take()
                        {
                            let _send_result = sender.send(());
                        }
                        Ok(())
                    }
                }
            })
        }
    }

    #[derive(Debug, Default)]
    struct CancelledExecutor;

    impl NodeExecutor for CancelledExecutor {
        type RunFuture<'a> = Ready<Result<()>>;

        fn run(
            &self,
            _ctx: NodeContext,
            _inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            ready(Err(PureflowError::cancelled("test cancellation")))
        }
    }

    #[derive(Debug, Default)]
    struct WaitingInputExecutor {
        visited: Mutex<Vec<String>>,
    }

    impl WaitingInputExecutor {
        fn visited_node_names(&self) -> Vec<String> {
            self.visited
                .lock()
                .expect("waiting input executor lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for WaitingInputExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let _held_outputs = outputs;
                self.visited
                    .lock()
                    .expect("waiting input executor lock should not be poisoned")
                    .push(ctx.node_id().to_string());

                let _packet = inputs
                    .recv(&port_id("in"), &ctx.cancellation_token())
                    .await?;

                Ok(())
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FeedbackLoopExecutorRole {
        Driver,
        Counter,
    }

    #[derive(Debug, Clone)]
    struct FeedbackLoopExecutor {
        role: FeedbackLoopExecutorRole,
        observed: Arc<Mutex<Vec<String>>>,
    }

    impl FeedbackLoopExecutor {
        fn driver(observed: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                role: FeedbackLoopExecutorRole::Driver,
                observed,
            }
        }

        fn counter(observed: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                role: FeedbackLoopExecutorRole::Counter,
                observed,
            }
        }

        fn push_observed(&self, value: String) {
            self.observed
                .lock()
                .expect("feedback loop executor lock should not be poisoned")
                .push(value);
        }
    }

    impl NodeExecutor for FeedbackLoopExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let cancellation = ctx.cancellation_token();
                match self.role {
                    FeedbackLoopExecutorRole::Driver => {
                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(b"seed", "first", "second"),
                                &cancellation,
                            )
                            .await?;
                        let packet: PortPacket = inputs
                            .recv(&port_id("in"), &cancellation)
                            .await?
                            .expect("counter should return one packet");
                        self.push_observed(format!(
                            "driver:{}",
                            String::from_utf8(packet_payload_bytes(packet))
                                .expect("feedback loop test payload should be UTF-8")
                        ));
                    }
                    FeedbackLoopExecutorRole::Counter => {
                        let packet: PortPacket = inputs
                            .recv(&port_id("in"), &cancellation)
                            .await?
                            .expect("driver should seed the loop");
                        self.push_observed(format!(
                            "counter:{}",
                            String::from_utf8(packet_payload_bytes(packet))
                                .expect("feedback loop test payload should be UTF-8")
                        ));
                        outputs
                            .send(
                                &port_id("out"),
                                packet_between(b"ack", "second", "first"),
                                &cancellation,
                            )
                            .await?;
                    }
                }

                Ok(())
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FeedbackLoopShutdownRole {
        Failing,
        ShutdownWatcher,
    }

    #[derive(Debug, Clone)]
    struct FeedbackLoopShutdownExecutor {
        role: FeedbackLoopShutdownRole,
        cancellation_observed: Arc<Mutex<bool>>,
    }

    impl FeedbackLoopShutdownExecutor {
        fn failing(cancellation_observed: Arc<Mutex<bool>>) -> Self {
            Self {
                role: FeedbackLoopShutdownRole::Failing,
                cancellation_observed,
            }
        }

        fn shutdown_watcher(cancellation_observed: Arc<Mutex<bool>>) -> Self {
            Self {
                role: FeedbackLoopShutdownRole::ShutdownWatcher,
                cancellation_observed,
            }
        }
    }

    impl NodeExecutor for FeedbackLoopShutdownExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            _inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                match self.role {
                    FeedbackLoopShutdownRole::Failing => {
                        Err(PureflowError::execution("feedback loop shutdown requested"))
                    }
                    FeedbackLoopShutdownRole::ShutdownWatcher => {
                        let cancellation = ctx.cancellation_token();
                        std::future::poll_fn(|task_cx: &mut std::task::Context<'_>| {
                            if cancellation.is_cancelled() {
                                *self
                                    .cancellation_observed
                                    .lock()
                                    .expect("shutdown executor lock should not be poisoned") = true;
                                std::task::Poll::Ready(Ok(()))
                            } else {
                                task_cx.waker().wake_by_ref();
                                std::task::Poll::Pending
                            }
                        })
                        .await
                    }
                }
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum ContractOutputMode {
        MatchingSource,
        MismatchedSource,
    }

    #[derive(Debug, Clone, Copy)]
    enum ContractBatchOutputMode {
        MatchingSource,
        MismatchedSource,
        UnknownPort,
    }

    #[derive(Debug, Clone, Copy)]
    struct ContractOutputExecutor {
        mode: ContractOutputMode,
    }

    #[derive(Debug, Clone, Copy)]
    struct ContractBatchExecutor {
        mode: ContractBatchOutputMode,
    }

    impl ContractOutputExecutor {
        const fn matching_source() -> Self {
            Self {
                mode: ContractOutputMode::MatchingSource,
            }
        }

        const fn mismatched_source() -> Self {
            Self {
                mode: ContractOutputMode::MismatchedSource,
            }
        }
    }

    impl ContractBatchExecutor {
        const fn matching_source() -> Self {
            Self {
                mode: ContractBatchOutputMode::MatchingSource,
            }
        }

        const fn mismatched_source() -> Self {
            Self {
                mode: ContractBatchOutputMode::MismatchedSource,
            }
        }

        const fn unknown_port() -> Self {
            Self {
                mode: ContractBatchOutputMode::UnknownPort,
            }
        }
    }

    impl NodeExecutor for ContractOutputExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            _inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                let source_node: &str = match self.mode {
                    ContractOutputMode::MatchingSource => "source",
                    ContractOutputMode::MismatchedSource => "other",
                };
                outputs
                    .send(
                        &port_id("out"),
                        packet_between(b"contracted", source_node, "sink"),
                        &ctx.cancellation_token(),
                    )
                    .await?;
                Ok(())
            })
        }
    }

    impl BatchExecutor for ContractBatchExecutor {
        fn invoke(&self, _inputs: BatchInputs) -> Result<BatchOutputs> {
            let (output_port, source_node): (&str, &str) = match self.mode {
                ContractBatchOutputMode::MatchingSource => ("out", "source"),
                ContractBatchOutputMode::MismatchedSource => ("out", "other"),
                ContractBatchOutputMode::UnknownPort => ("rogue", "source"),
            };
            let mut outputs: BatchOutputs = BatchOutputs::new();
            outputs.push(
                port_id(output_port),
                packet_between(b"contracted", source_node, "sink"),
            );
            Ok(outputs)
        }
    }

    #[derive(Debug)]
    struct RecordingSinkExecutor {
        received: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl RecordingSinkExecutor {
        fn new(received: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
            Self { received }
        }
    }

    impl NodeExecutor for RecordingSinkExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                match inputs.recv(&port_id("in"), &ctx.cancellation_token()).await {
                    Ok(Some(packet)) => {
                        self.received
                            .lock()
                            .expect("recording sink lock should not be poisoned")
                            .push(packet_payload_bytes(packet));
                    }
                    Ok(None)
                    | Err(PortRecvError::Disconnected { .. } | PortRecvError::Cancelled { .. }) => {
                    }
                    Err(err) => return Err(err.into()),
                }

                Ok(())
            })
        }
    }

    #[derive(Debug)]
    enum ContractBatchRegistryExecutor {
        Batch(BatchNodeExecutor<ContractBatchExecutor>),
        Sink(RecordingSinkExecutor),
    }

    impl NodeExecutor for ContractBatchRegistryExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
            match self {
                Self::Batch(executor) => executor.run(ctx, inputs, outputs),
                Self::Sink(executor) => executor.run(ctx, inputs, outputs),
            }
        }
    }

    #[derive(Debug, Default)]
    struct SiblingCancellationExecutor {
        cancellation_observed: Mutex<bool>,
    }

    impl SiblingCancellationExecutor {
        fn cancellation_observed(&self) -> bool {
            *self
                .cancellation_observed
                .lock()
                .expect("sibling cancellation executor lock should not be poisoned")
        }
    }

    #[derive(Debug, Default)]
    struct CapacityProbeExecutor {
        observed: Mutex<Vec<Option<usize>>>,
    }

    impl CapacityProbeExecutor {
        fn observed_capacities(&self) -> Vec<Option<usize>> {
            self.observed
                .lock()
                .expect("capacity probe executor lock should not be poisoned")
                .clone()
        }
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
        fn record(&self, record: &MetadataRecord) -> Result<()> {
            self.records
                .lock()
                .expect("metadata sink lock should not be poisoned")
                .push(record.clone());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingLifecycleHook {
        events: Mutex<Vec<LifecycleEventKind>>,
    }

    impl RecordingLifecycleHook {
        fn recorded(&self) -> Vec<LifecycleEventKind> {
            self.events
                .lock()
                .expect("lifecycle hook lock should not be poisoned")
                .clone()
        }
    }

    impl LifecycleHook for RecordingLifecycleHook {
        fn observe(&self, event: &LifecycleEvent) -> Result<()> {
            self.events
                .lock()
                .expect("lifecycle hook lock should not be poisoned")
                .push(event.kind());
            Ok(())
        }
    }

    impl NodeExecutor for CapacityProbeExecutor {
        type RunFuture<'a> = Ready<Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            if ctx.node_id().as_str() == "probe" {
                let capacity = inputs.capacity(&port_id("in"));
                self.observed
                    .lock()
                    .expect("capacity probe executor lock should not be poisoned")
                    .push(capacity);
            }

            ready(Ok(()))
        }
    }

    impl NodeExecutor for SiblingCancellationExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                if ctx.node_id().as_str() == "fail" {
                    return Err(PureflowError::execution("fail requested"));
                }

                if ctx.node_id().as_str() == "worker" {
                    let cancellation = ctx.cancellation_token();
                    let result: std::result::Result<Option<PortPacket>, PortRecvError> =
                        inputs.recv(&port_id("in"), &cancellation).await;
                    if matches!(result, Err(PortRecvError::Cancelled { .. })) {
                        *self
                            .cancellation_observed
                            .lock()
                            .expect("sibling cancellation executor lock should not be poisoned") =
                            true;
                        return Ok(());
                    }

                    return Err(PureflowError::execution(
                        "worker input should be cancelled after sibling failure",
                    ));
                }

                Ok(())
            })
        }
    }

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn message_id(value: &str) -> MessageId {
        MessageId::new(value).expect("valid message id")
    }

    fn packet(value: &[u8]) -> PortPacket {
        packet_between(value, "source", "sink")
    }

    fn packet_between(value: &[u8], source_node: &str, target_node: &str) -> PortPacket {
        let source: MessageEndpoint = MessageEndpoint::new(node_id(source_node), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id(target_node), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route);

        PortPacket::new(metadata, PacketPayload::from(value.to_vec()))
    }

    fn schema(value: &str) -> SchemaRef {
        SchemaRef::new(value).expect("valid schema ref")
    }

    fn source_output_contracts() -> Vec<NodeContract> {
        vec![
            NodeContract::new(
                node_id("source"),
                vec![PortContract::new(
                    port_id("out"),
                    PortDirection::Output,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
                Determinism::Unknown,
                RetryDisposition::Unknown,
            )
            .expect("valid source contract"),
        ]
    }

    fn cyclic_workflow() -> WorkflowDefinition {
        let first: NodeDefinition = NodeBuilder::new("first").input("in").output("out").build();
        let second: NodeDefinition = NodeBuilder::new("second").input("in").output("out").build();
        let graph = pureflow_workflow::WorkflowGraph::with_cycles_allowed(
            [first, second],
            [
                EdgeDefinition::new(
                    pureflow_workflow::EdgeEndpoint::new(node_id("first"), port_id("out")),
                    pureflow_workflow::EdgeEndpoint::new(node_id("second"), port_id("in")),
                ),
                EdgeDefinition::new(
                    pureflow_workflow::EdgeEndpoint::new(node_id("second"), port_id("out")),
                    pureflow_workflow::EdgeEndpoint::new(node_id("first"), port_id("in")),
                ),
            ],
        )
        .expect("cycle-allowed workflow graph should build");

        WorkflowDefinition::new(workflow_id("flow"), graph)
    }

    fn packet_payload_bytes(packet: PortPacket) -> Vec<u8> {
        packet
            .into_payload()
            .as_bytes()
            .expect("engine backpressure tests send bytes")
            .to_vec()
    }

    #[test]
    fn workflow_run_policy_names_feedback_loop_startup_and_termination() {
        let feedback_loop: FeedbackLoopRunPolicy =
            FeedbackLoopRunPolicy::start_all_nodes_until_complete();
        let policy: WorkflowRunPolicy = WorkflowRunPolicy::feedback_loops(feedback_loop);

        assert_eq!(feedback_loop.startup(), FeedbackLoopStartup::StartAllNodes);
        assert_eq!(
            feedback_loop.termination(),
            FeedbackLoopTermination::AllNodesComplete
        );
        assert_eq!(
            policy.cycle_policy(),
            CycleRunPolicy::AllowFeedbackLoops(feedback_loop)
        );
        assert_eq!(
            WorkflowRunPolicy::default().cycle_policy(),
            CycleRunPolicy::Reject
        );
        assert_eq!(
            WorkflowRunPolicy::default().watchdog_policy(),
            WorkflowWatchdogPolicy::Disabled
        );
    }

    #[test]
    fn run_workflow_passes_execution_metadata_to_each_node() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        let contexts: Vec<NodeContext> = executor.visited_contexts();
        assert_eq!(contexts[0].workflow_id().as_str(), "flow");
        assert_eq!(contexts[0].execution().execution_id().as_str(), "run-1");
        assert_eq!(executor.visited_node_names(), vec!["first", "second"]);
    }

    #[test]
    fn run_workflow_with_metadata_sink_records_lifecycle_events() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());

        block_on(run_workflow_with_metadata_sink(
            &workflow,
            &execution,
            &executor,
            sink.clone(),
        ))
        .expect("metadata workflow run should succeed");
        let lifecycle_count: usize = sink
            .records()
            .iter()
            .filter(|record: &&MetadataRecord| matches!(record, MetadataRecord::Lifecycle(_)))
            .count();

        assert_eq!(lifecycle_count, 4);
    }

    #[test]
    fn run_workflow_with_observers_summary_records_lifecycle_hook_and_metadata() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("node").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();
        let hook: RecordingLifecycleHook = RecordingLifecycleHook::default();
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());

        let summary: WorkflowRunSummary = block_on(run_workflow_with_observers_summary(
            &workflow,
            &execution,
            &executor,
            &hook,
            sink.clone(),
        ))
        .expect("observer workflow run should return a summary");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(summary.completed_node_count(), 1);
        assert_eq!(
            hook.recorded(),
            vec![
                LifecycleEventKind::NodeStarted,
                LifecycleEventKind::NodeCompleted
            ]
        );

        let lifecycle_records: Vec<MetadataRecord> = sink
            .records()
            .into_iter()
            .filter(|record: &MetadataRecord| matches!(record, MetadataRecord::Lifecycle(_)))
            .collect();
        assert_eq!(lifecycle_records.len(), 2);
    }

    #[test]
    fn run_workflow_with_registry_resolves_executor_per_node() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let registry: StaticNodeExecutorRegistry<RegistryExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("source"),
                    RegistryExecutor::source(Arc::clone(&received)),
                ),
                (
                    node_id("sink"),
                    RegistryExecutor::sink(Arc::clone(&received)),
                ),
            ]));

        block_on(run_workflow_with_registry(&workflow, &execution, &registry))
            .expect("registry workflow should run");

        assert_eq!(
            *received
                .lock()
                .expect("registry test lock should not be poisoned"),
            vec![b"registered".to_vec()]
        );
    }

    #[test]
    fn run_workflow_with_registry_rejects_missing_executor() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("missing").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let registry: StaticNodeExecutorRegistry<RegistryExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::new());

        let err: PureflowError =
            block_on(run_workflow_with_registry(&workflow, &execution, &registry))
                .expect_err("missing registry entry should fail");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert!(
            err.to_string()
                .contains("no executor registered for workflow node `missing`")
        );
    }

    #[test]
    fn run_workflow_summary_reports_successful_terminal_state() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();

        let summary: WorkflowRunSummary =
            block_on(run_workflow_summary(&workflow, &execution, &executor))
                .expect("summary workflow should run");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(summary.scheduled_node_count(), 2);
        assert_eq!(summary.completed_node_count(), 2);
        assert_eq!(summary.failed_node_count(), 0);
        assert_eq!(summary.cancelled_node_count(), 0);
        assert_eq!(summary.pending_node_count(), 0);
        assert_eq!(summary.error_count(), 0);
        assert_eq!(summary.observed_message_count(), 0);
        assert!(summary.first_error().is_none());
    }

    #[test]
    fn run_workflow_summary_rejects_cycle_allowed_graph_by_default() {
        let workflow: WorkflowDefinition = cyclic_workflow();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();

        let err: PureflowError = block_on(run_workflow_summary(&workflow, &execution, &executor))
            .expect_err("default run policy should reject cyclic workflow");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert!(err.to_string().contains("first -> second -> first"));
        assert!(
            err.to_string()
                .contains("explicit feedback-loop run policy")
        );
        assert!(executor.visited_contexts().is_empty());
    }

    #[test]
    fn run_workflow_with_feedback_loop_policy_allows_cycle_allowed_graph() {
        let workflow: WorkflowDefinition = cyclic_workflow();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();
        let policy: WorkflowRunPolicy = WorkflowRunPolicy::feedback_loops(
            FeedbackLoopRunPolicy::start_all_nodes_until_complete(),
        );

        let summary: WorkflowRunSummary = block_on(run_workflow_with_policy_summary(
            &workflow, &execution, &executor, policy,
        ))
        .expect("feedback-loop policy should allow cyclic workflow");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(summary.scheduled_node_count(), 2);
        assert_eq!(summary.completed_node_count(), 2);
        assert_eq!(
            executor.visited_node_names(),
            vec![String::from("first"), String::from("second")]
        );
    }

    #[test]
    fn feedback_loop_policy_runs_deterministic_cycle_messages() {
        let workflow: WorkflowDefinition = cyclic_workflow();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let observed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let registry: StaticNodeExecutorRegistry<FeedbackLoopExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("first"),
                    FeedbackLoopExecutor::driver(Arc::clone(&observed)),
                ),
                (
                    node_id("second"),
                    FeedbackLoopExecutor::counter(Arc::clone(&observed)),
                ),
            ]));
        let policy: WorkflowRunPolicy = WorkflowRunPolicy::feedback_loops(
            FeedbackLoopRunPolicy::start_all_nodes_until_complete(),
        );

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_policy_summary(
            &workflow, &execution, &registry, policy,
        ))
        .expect("explicit feedback-loop policy should run cyclic messages");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(summary.completed_node_count(), 2);
        assert_eq!(summary.pending_node_count(), 0);
        assert_eq!(
            *observed
                .lock()
                .expect("feedback loop observed lock should not be poisoned"),
            vec![String::from("counter:seed"), String::from("driver:ack")]
        );
    }

    #[test]
    fn feedback_loop_policy_cancels_siblings_for_shutdown() {
        let workflow: WorkflowDefinition = cyclic_workflow();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let cancellation_observed: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let registry: StaticNodeExecutorRegistry<FeedbackLoopShutdownExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("first"),
                    FeedbackLoopShutdownExecutor::failing(Arc::clone(&cancellation_observed)),
                ),
                (
                    node_id("second"),
                    FeedbackLoopShutdownExecutor::shutdown_watcher(Arc::clone(
                        &cancellation_observed,
                    )),
                ),
            ]));
        let policy: WorkflowRunPolicy = WorkflowRunPolicy::feedback_loops(
            FeedbackLoopRunPolicy::start_all_nodes_until_complete(),
        );

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_policy_summary(
            &workflow, &execution, &registry, policy,
        ))
        .expect("feedback-loop shutdown should return summary data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.completed_node_count(), 1);
        assert_eq!(summary.failed_node_count(), 1);
        assert_eq!(summary.pending_node_count(), 0);
        assert!(
            summary
                .first_error()
                .expect("shutdown failure should be retained")
                .to_string()
                .contains("feedback loop shutdown requested")
        );
        assert!(
            *cancellation_observed
                .lock()
                .expect("shutdown observed lock should not be poisoned")
        );
    }

    #[test]
    fn workflow_deadlock_watchdog_reports_stalled_cycle_state() {
        let workflow: WorkflowDefinition = cyclic_workflow();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: WaitingInputExecutor = WaitingInputExecutor::default();
        let policy: WorkflowRunPolicy = WorkflowRunPolicy::feedback_loops(
            FeedbackLoopRunPolicy::start_all_nodes_until_complete(),
        )
        .with_watchdog(WorkflowWatchdogPolicy::deadlock_after(
            Duration::from_millis(1),
        ));

        let summary: WorkflowRunSummary = block_on(run_workflow_with_policy_summary(
            &workflow, &execution, &executor, policy,
        ))
        .expect("deadlock watchdog should report a stalled cyclic workflow as summary data");
        let diagnostic: &WorkflowDeadlockDiagnostic = summary
            .deadlock_diagnostic()
            .expect("deadlock diagnostic should be captured");
        let err_text: String = summary
            .first_error()
            .expect("deadlock should be recorded as first error")
            .to_string();

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.error_count(), 1);
        assert_eq!(summary.pending_node_count(), 2);
        assert!(err_text.contains("watchdog detected no workflow progress"));
        assert_eq!(diagnostic.pending_node_count(), 2);
        assert_eq!(diagnostic.bounded_edge_count(), 2);
        assert_eq!(
            executor.visited_node_names(),
            vec![String::from("first"), String::from("second")]
        );
    }

    #[test]
    fn run_workflow_summary_retains_first_failure_without_returning_error() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: AggregateFailureExecutor = AggregateFailureExecutor::default();

        let summary: WorkflowRunSummary =
            block_on(run_workflow_summary(&workflow, &execution, &executor))
                .expect("summary should preserve node failures as data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.scheduled_node_count(), 2);
        assert_eq!(summary.completed_node_count(), 1);
        assert_eq!(summary.failed_node_count(), 1);
        assert_eq!(summary.cancelled_node_count(), 0);
        assert_eq!(summary.error_count(), 1);
        assert!(
            summary
                .first_error()
                .expect("summary should retain first error")
                .to_string()
                .contains("first failed")
        );
    }

    #[test]
    fn run_workflow_with_metadata_sink_records_workflow_error_metadata() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: AggregateFailureExecutor = AggregateFailureExecutor::default();
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());

        let summary: WorkflowRunSummary = block_on(run_workflow_with_metadata_sink_summary(
            &workflow,
            &execution,
            &executor,
            sink.clone(),
        ))
        .expect("summary should preserve node failures as data");
        let records: Vec<MetadataRecord> = sink.records();
        let workflow_error = records
            .iter()
            .find_map(|record: &MetadataRecord| match record {
                MetadataRecord::Error(error)
                    if error.kind() == ErrorMetadataKind::WorkflowFailed =>
                {
                    Some(error)
                }
                _ => None,
            })
            .expect("workflow error metadata should be recorded");
        let node_error_count: usize = records
            .iter()
            .filter(|record: &&MetadataRecord| {
                matches!(
                    record,
                    MetadataRecord::Error(error)
                        if error.kind() == ErrorMetadataKind::NodeFailed
                )
            })
            .count();

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(workflow_error.workflow_id().as_str(), "flow");
        assert!(workflow_error.node_id().is_none());
        assert!(workflow_error.error().to_string().contains("first failed"));
        assert!(workflow_error.diagnostic().is_none());
        assert_eq!(node_error_count, 1);
    }

    #[test]
    fn watchdog_metadata_records_deadlock_diagnostic_payload() {
        let workflow: WorkflowDefinition = cyclic_workflow();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: WaitingInputExecutor = WaitingInputExecutor::default();
        let registry: SingleNodeExecutorRegistry<'_, WaitingInputExecutor> =
            SingleNodeExecutorRegistry::new(&executor);
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());
        let policy: WorkflowRunPolicy = WorkflowRunPolicy::feedback_loops(
            FeedbackLoopRunPolicy::start_all_nodes_until_complete(),
        )
        .with_watchdog(WorkflowWatchdogPolicy::deadlock_after(
            Duration::from_millis(1),
        ));

        let summary: WorkflowRunSummary =
            block_on(run_workflow_with_registry_policy_and_metadata_sink_summary(
                &workflow,
                &execution,
                &registry,
                policy,
                sink.clone(),
            ))
            .expect("watchdog run should return summary data");
        let records: Vec<MetadataRecord> = sink.records();
        let workflow_error = records
            .iter()
            .find_map(|record: &MetadataRecord| match record {
                MetadataRecord::Error(error)
                    if error.kind() == ErrorMetadataKind::WorkflowFailed =>
                {
                    Some(error)
                }
                _ => None,
            })
            .expect("workflow error metadata should be recorded");

        assert!(summary.deadlock_diagnostic().is_some());
        match workflow_error.diagnostic() {
            Some(ErrorDiagnosticMetadata::WorkflowDeadlock(deadlock)) => {
                assert_eq!(deadlock.pending_node_count(), 2);
                assert_eq!(deadlock.bounded_edge_count(), 2);
                assert_eq!(deadlock.no_progress_timeout_ms(), 1);
                assert_eq!(deadlock.cycle_policy(), "allow_feedback_loops");
                assert_eq!(deadlock.feedback_loop_startup(), Some("start_all_nodes"));
                assert_eq!(
                    deadlock.feedback_loop_termination(),
                    Some("all_nodes_complete")
                );
            }
            _ => panic!("workflow error should include deadlock diagnostic metadata"),
        }
    }

    #[test]
    fn supervisor_summary_covers_failing_source_and_downstream_cancellation() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(
                NodeBuilder::new("transform")
                    .input("in")
                    .output("out")
                    .build(),
            )
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "transform", "in")
            .edge("transform", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let registry: StaticNodeExecutorRegistry<FailureMatrixExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (node_id("source"), FailureMatrixExecutor::source_fails()),
                (
                    node_id("transform"),
                    FailureMatrixExecutor::passthrough_transform(),
                ),
                (node_id("sink"), FailureMatrixExecutor::sink_waits()),
            ]));

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_summary(
            &workflow, &execution, &registry,
        ))
        .expect("summary should preserve source failure as data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.scheduled_node_count(), 3);
        assert_eq!(summary.completed_node_count(), 0);
        assert_eq!(summary.failed_node_count(), 1);
        assert_eq!(summary.cancelled_node_count(), 2);
        assert_eq!(summary.pending_node_count(), 0);
        assert_eq!(summary.error_count(), 3);
        assert!(
            summary
                .first_error()
                .expect("summary should retain source failure")
                .to_string()
                .contains("matrix source failed")
        );
    }

    #[test]
    fn supervisor_summary_covers_failing_transform_and_error_metadata() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(
                NodeBuilder::new("transform")
                    .input("in")
                    .output("out")
                    .build(),
            )
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "transform", "in")
            .edge("transform", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let registry: StaticNodeExecutorRegistry<FailureMatrixExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("source"),
                    FailureMatrixExecutor::source_for_transform_failure(),
                ),
                (
                    node_id("transform"),
                    FailureMatrixExecutor::transform_fails(),
                ),
                (node_id("sink"), FailureMatrixExecutor::sink_waits()),
            ]));
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());

        let summary: WorkflowRunSummary =
            block_on(run_workflow_with_registry_and_metadata_sink_summary(
                &workflow,
                &execution,
                &registry,
                sink.clone(),
            ))
            .expect("summary should preserve transform failure as data");
        let records = sink.records();
        let node_error_count = records
            .iter()
            .filter(|record| {
                matches!(
                    record,
                    MetadataRecord::Error(error)
                        if error.kind() == ErrorMetadataKind::NodeFailed
                )
            })
            .count();
        let workflow_error = records
            .iter()
            .find_map(|record| match record {
                MetadataRecord::Error(error)
                    if error.kind() == ErrorMetadataKind::WorkflowFailed =>
                {
                    Some(error)
                }
                _ => None,
            })
            .expect("workflow error metadata should be recorded");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.completed_node_count(), 1);
        assert_eq!(summary.failed_node_count(), 1);
        assert_eq!(summary.cancelled_node_count(), 1);
        assert_eq!(summary.pending_node_count(), 0);
        assert_eq!(summary.error_count(), 2);
        assert!(
            workflow_error
                .error()
                .to_string()
                .contains("matrix transform failed")
        );
        assert!(workflow_error.node_id().is_none());
        assert_eq!(node_error_count, 2);
    }

    #[test]
    fn supervisor_summary_covers_disconnected_downstream_send_failure() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let (source, sink) = DisconnectedDownstreamExecutor::pair();
        let registry: StaticNodeExecutorRegistry<DisconnectedDownstreamExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (node_id("source"), source),
                (node_id("sink"), sink),
            ]));

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_summary(
            &workflow, &execution, &registry,
        ))
        .expect("summary should preserve disconnected send failure as data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.completed_node_count(), 1);
        assert_eq!(summary.failed_node_count(), 1);
        assert_eq!(summary.cancelled_node_count(), 0);
        assert_eq!(summary.pending_node_count(), 0);
        assert_eq!(summary.error_count(), 1);
        assert!(
            summary
                .first_error()
                .expect("summary should retain disconnected send error")
                .to_string()
                .contains("disconnected")
        );
    }

    #[test]
    fn supervisor_rejects_fan_out_partial_send_after_one_downstream_disconnects() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("good").input("in").build())
            .node(NodeBuilder::new("drop").input("in").build())
            .edge("source", "out", "good", "in")
            .edge("source", "out", "drop", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let registry = FanOutPartialFailureExecutor::registry();

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_summary(
            &workflow, &execution, &registry,
        ))
        .expect("summary should preserve fan-out send failure as data");
        let good_sink_observation = registry
            .executors()
            .get(&node_id("good"))
            .expect("good sink executor should be registered")
            .good_sink_observation()
            .expect("good sink should record cancellation or closure");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.completed_node_count(), 2);
        assert_eq!(summary.failed_node_count(), 1);
        assert_eq!(summary.cancelled_node_count(), 0);
        assert_eq!(summary.pending_node_count(), 0);
        assert_eq!(summary.error_count(), 1);
        assert!(
            summary
                .first_error()
                .expect("summary should retain fan-out send error")
                .to_string()
                .contains("disconnected")
        );
        assert!(
            good_sink_observation == "cancelled_without_packet"
                || good_sink_observation == "disconnected_without_packet"
                || good_sink_observation == "closed_without_packet",
            "good sink must not receive a partial fan-out packet: {good_sink_observation}"
        );
    }

    #[test]
    fn run_workflow_summary_reports_cancellation_terminal_state() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("node").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: CancelledExecutor = CancelledExecutor;

        let summary: WorkflowRunSummary =
            block_on(run_workflow_summary(&workflow, &execution, &executor))
                .expect("summary should preserve cancellation as data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Cancelled);
        assert_eq!(summary.scheduled_node_count(), 1);
        assert_eq!(summary.completed_node_count(), 0);
        assert_eq!(summary.failed_node_count(), 0);
        assert_eq!(summary.cancelled_node_count(), 1);
        assert_eq!(summary.error_count(), 1);
    }

    #[test]
    fn run_workflow_with_contracts_summary_accepts_matching_output_source() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: ContractOutputExecutor = ContractOutputExecutor::matching_source();

        let summary: WorkflowRunSummary = block_on(run_workflow_with_contracts_summary(
            &workflow,
            &execution,
            &executor,
            &source_output_contracts(),
        ))
        .expect("contract-aware workflow should run");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(summary.completed_node_count(), 1);
        assert!(summary.first_error().is_none());
    }

    #[test]
    fn run_workflow_with_contracts_summary_rejects_mismatched_output_source() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: ContractOutputExecutor = ContractOutputExecutor::mismatched_source();

        let summary: WorkflowRunSummary = block_on(run_workflow_with_contracts_summary(
            &workflow,
            &execution,
            &executor,
            &source_output_contracts(),
        ))
        .expect("contract-aware summary should preserve output validation failures as data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert_eq!(summary.completed_node_count(), 0);
        assert_eq!(summary.failed_node_count(), 1);
        assert!(
            summary
                .first_error()
                .expect("summary should retain output validation error")
                .to_string()
                .contains("does not match output")
        );
    }

    #[test]
    fn batch_node_executor_sends_matching_outputs_through_graph_edges() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let registry: StaticNodeExecutorRegistry<ContractBatchRegistryExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("source"),
                    ContractBatchRegistryExecutor::Batch(BatchNodeExecutor::new(
                        ContractBatchExecutor::matching_source(),
                    )),
                ),
                (
                    node_id("sink"),
                    ContractBatchRegistryExecutor::Sink(RecordingSinkExecutor::new(Arc::clone(
                        &received,
                    ))),
                ),
            ]));

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_contracts_summary(
            &workflow,
            &execution,
            &registry,
            &source_output_contracts(),
        ))
        .expect("batch workflow should run through output contracts");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Completed);
        assert_eq!(
            *received
                .lock()
                .expect("batch sink received lock should not be poisoned"),
            vec![b"contracted".to_vec()]
        );
    }

    #[test]
    fn batch_node_executor_rejects_mismatched_output_before_sink_observes_it() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let registry: StaticNodeExecutorRegistry<ContractBatchRegistryExecutor> =
            StaticNodeExecutorRegistry::new(BTreeMap::from([
                (
                    node_id("source"),
                    ContractBatchRegistryExecutor::Batch(BatchNodeExecutor::new(
                        ContractBatchExecutor::mismatched_source(),
                    )),
                ),
                (
                    node_id("sink"),
                    ContractBatchRegistryExecutor::Sink(RecordingSinkExecutor::new(Arc::clone(
                        &received,
                    ))),
                ),
            ]));

        let summary: WorkflowRunSummary = block_on(run_workflow_with_registry_contracts_summary(
            &workflow,
            &execution,
            &registry,
            &source_output_contracts(),
        ))
        .expect("batch output validation failures should be preserved as summary data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert!(
            summary
                .first_error()
                .expect("summary should retain output validation error")
                .to_string()
                .contains("does not match output")
        );
        assert!(
            received
                .lock()
                .expect("batch sink received lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn batch_node_executor_rejects_undeclared_output_ports() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: BatchNodeExecutor<ContractBatchExecutor> =
            BatchNodeExecutor::new(ContractBatchExecutor::unknown_port());

        let summary: WorkflowRunSummary =
            block_on(run_workflow_summary(&workflow, &execution, &executor))
                .expect("batch output validation failures should be summary data");

        assert_eq!(summary.terminal_state(), WorkflowTerminalState::Failed);
        assert!(
            summary
                .first_error()
                .expect("summary should retain unknown output error")
                .to_string()
                .contains("output port `rogue` is not declared")
        );
    }

    #[test]
    fn run_workflow_propagates_executor_failures() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: FailingExecutor = FailingExecutor::execution("boom");

        let err = block_on(run_workflow(&workflow, &execution, &executor))
            .expect_err("workflow should surface executor failures");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
    }

    #[test]
    fn run_workflow_passes_declared_node_ports_to_executor() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        assert_eq!(
            executor.visited_input_port_names(),
            vec![Vec::<String>::new(), vec![String::from("in")]]
        );
        assert_eq!(
            executor.visited_output_port_names(),
            vec![vec![String::from("out")], Vec::<String>::new()]
        );
    }

    #[test]
    fn run_workflow_wires_edges_as_bounded_port_channels() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: ChannelExecutor = ChannelExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        assert_eq!(
            executor.received_payloads(),
            vec![b"hello".to_vec(), b"world".to_vec()]
        );
    }

    #[test]
    fn run_workflow_uses_explicit_edge_capacity() {
        let workflow: WorkflowDefinition = WorkflowDefinition::from_parts(
            workflow_id("flow"),
            [
                NodeBuilder::new("source").output("out").build(),
                NodeBuilder::new("probe").input("in").build(),
            ],
            [EdgeDefinition::with_capacity(
                pureflow_workflow::EdgeEndpoint::new(node_id("source"), port_id("out")),
                pureflow_workflow::EdgeEndpoint::new(node_id("probe"), port_id("in")),
                NonZeroUsize::new(3).expect("nonzero"),
            )],
        )
        .expect("workflow should be valid");
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: CapacityProbeExecutor = CapacityProbeExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        assert_eq!(
            executor.observed_capacities(),
            vec![Some(NonZeroUsize::new(3).expect("nonzero").get())]
        );
    }

    #[test]
    fn run_workflow_backpressure_blocks_until_downstream_receives() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge_with_capacity(
                "source",
                "out",
                "sink",
                "in",
                NonZeroUsize::new(1).expect("nonzero"),
            )
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: BoundedBackpressureExecutor = BoundedBackpressureExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        let events: Vec<String> = executor.events();
        assert!(
            events
                .iter()
                .any(|event: &String| event == "source-observed-full-edge")
        );
        assert!(
            events
                .iter()
                .any(|event: &String| event == "source-second-send-completed")
        );
        assert_eq!(
            executor.received_payloads(),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    }

    #[test]
    fn run_workflow_fans_out_one_output_to_all_downstream_inputs() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("left").input("in").build())
            .node(NodeBuilder::new("right").input("in").build())
            .edge("source", "out", "left", "in")
            .edge("source", "out", "right", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: FanOutExecutor = FanOutExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        let received_by_node: BTreeMap<String, Vec<Vec<u8>>> = executor.received_by_node();
        assert_eq!(received_by_node.get("left"), Some(&vec![b"fan".to_vec()]));
        assert_eq!(received_by_node.get("right"), Some(&vec![b"fan".to_vec()]));
    }

    #[test]
    fn run_workflow_fans_in_and_propagates_upstream_closure() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("left").output("out").build())
            .node(NodeBuilder::new("right").output("out").build())
            .node(NodeBuilder::new("collector").input("in").build())
            .edge("left", "out", "collector", "in")
            .edge("right", "out", "collector", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: FanInClosureExecutor = FanInClosureExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        let mut received: Vec<Vec<u8>> = executor.received_payloads();
        received.sort();
        assert_eq!(received, vec![b"left".to_vec(), b"right".to_vec()]);
        assert!(executor.closure_observed());
    }

    #[test]
    fn run_workflow_aggregates_terminal_results_after_polling_all_nodes() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: AggregateFailureExecutor = AggregateFailureExecutor::default();

        let err = block_on(run_workflow(&workflow, &execution, &executor))
            .expect_err("workflow should surface executor failures");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(
            executor.visited_node_names(),
            vec![String::from("first"), String::from("second")]
        );
    }

    #[test]
    fn run_workflow_cancels_siblings_after_first_node_failure() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("worker").input("in").build())
            .node(NodeBuilder::new("fail").output("out").build())
            .edge("fail", "out", "worker", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: SiblingCancellationExecutor = SiblingCancellationExecutor::default();

        let err = block_on(run_workflow(&workflow, &execution, &executor))
            .expect_err("workflow should surface the first node failure");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert!(executor.cancellation_observed());
    }
}
