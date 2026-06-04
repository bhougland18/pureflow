//! Execution context and cancellation boundary types.
//!
//! ## Fragment: context-runtime-boundary
//!
//! This module keeps the runtime-facing context deliberately small: workflow
//! identity, node identity, execution identity, and cancellation state. That
//! is enough for the foundation beads to define what a node is executing
//! without prematurely choosing an async runtime, scheduler, or transport.
//!
//! ## Fragment: context-cancellation-shape
//!
//! Cancellation is represented as a Pureflow-owned shared signal rather than as
//! an exposed async-runtime context. Runtime supervisors can request
//! cancellation after a node starts, and cloned parent or child contexts observe
//! the same request, but node APIs still see only Pureflow `NodeContext`
//! semantics rather than raw `asupersync::Cx`.
//!
//! ## Fragment: context-attempt-numbering
//!
//! Execution attempts are one-based on purpose. Retry counts are usually read
//! by humans in logs and diagnostics, and `attempt = 1` is less error-prone
//! than forcing every downstream consumer to translate from zero-based storage.

use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use pureflow_types::{ExecutionId, NodeId, WorkflowId};

/// One-based attempt number for an execution boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionAttempt(NonZeroU32);

impl ExecutionAttempt {
    /// Create an execution attempt from a one-based value.
    #[must_use]
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// First attempt for a workflow execution.
    #[must_use]
    pub const fn first() -> Self {
        Self(NonZeroU32::MIN)
    }

    /// Return the one-based attempt number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Metadata that identifies one workflow execution attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionMetadata {
    execution_id: ExecutionId,
    attempt: ExecutionAttempt,
}

impl ExecutionMetadata {
    /// Create execution metadata for an explicit attempt.
    #[must_use]
    pub const fn new(execution_id: ExecutionId, attempt: ExecutionAttempt) -> Self {
        Self {
            execution_id,
            attempt,
        }
    }

    /// Create execution metadata for the first attempt.
    #[must_use]
    pub const fn first_attempt(execution_id: ExecutionId) -> Self {
        Self::new(execution_id, ExecutionAttempt::first())
    }

    /// Identifier for this workflow execution.
    #[must_use]
    pub const fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    /// One-based attempt for this workflow execution.
    #[must_use]
    pub const fn attempt(&self) -> ExecutionAttempt {
        self.attempt
    }
}

/// Cancellation request visible at the runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancellationRequest {
    reason: String,
}

impl CancellationRequest {
    /// Create a cancellation request with a human-readable reason.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Human-readable reason for cancellation.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Cancellation state carried by a node execution context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationState {
    /// No cancellation has been requested.
    Active,
    /// Cancellation has been requested at the runtime boundary.
    Requested(CancellationRequest),
}

impl CancellationState {
    /// Return whether cancellation has been requested.
    #[must_use]
    pub const fn is_requested(&self) -> bool {
        matches!(self, Self::Requested(_))
    }
}

#[derive(Debug, Default)]
struct CancellationSignal {
    request: Mutex<Option<CancellationRequest>>,
}

/// Read-only cancellation view carried by a node execution context.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    signal: Arc<CancellationSignal>,
}

impl CancellationToken {
    /// Create an active cancellation token.
    #[must_use]
    pub fn active() -> Self {
        Self {
            signal: Arc::new(CancellationSignal::default()),
        }
    }

    /// Create a token that already has cancellation requested.
    #[must_use]
    pub fn cancelled(request: CancellationRequest) -> Self {
        let token: Self = Self::active();
        let _first_request: bool = token.request_cancellation(request);
        token
    }

    /// Return the current cancellation request, if any.
    #[must_use]
    pub fn request(&self) -> Option<CancellationRequest> {
        self.signal
            .request
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Return the current cancellation state.
    #[must_use]
    pub fn state(&self) -> CancellationState {
        self.request()
            .map_or(CancellationState::Active, |request: CancellationRequest| {
                CancellationState::Requested(request)
            })
    }

    /// Return whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.request().is_some()
    }

    fn request_cancellation(&self, request: CancellationRequest) -> bool {
        let mut guard: MutexGuard<'_, Option<CancellationRequest>> = self
            .signal
            .request
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if guard.is_some() {
            return false;
        }

        *guard = Some(request);
        true
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::active()
    }
}

impl PartialEq for CancellationToken {
    fn eq(&self, other: &Self) -> bool {
        self.request() == other.request()
    }
}

impl Eq for CancellationToken {}

/// Runtime-owned handle that can request cancellation for shared contexts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancellationHandle {
    token: CancellationToken,
}

impl CancellationHandle {
    /// Create a cancellation handle with an active token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            token: CancellationToken::active(),
        }
    }

    /// Return a read-only token suitable for attaching to a `NodeContext`.
    #[must_use]
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Request cancellation for every context sharing this handle's token.
    ///
    /// Returns `true` when this call recorded the first cancellation request.
    #[must_use]
    pub fn cancel(&self, request: CancellationRequest) -> bool {
        self.token.request_cancellation(request)
    }

    /// Return whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Return the current cancellation request, if any.
    #[must_use]
    pub fn request(&self) -> Option<CancellationRequest> {
        self.token.request()
    }
}

impl Default for CancellationHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Minimal execution context passed to runtime-managed nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeContext {
    workflow_id: WorkflowId,
    node_id: NodeId,
    execution: ExecutionMetadata,
    cancellation: CancellationToken,
}

impl NodeContext {
    /// Create an active node context for one execution attempt.
    #[must_use]
    pub fn new(workflow_id: WorkflowId, node_id: NodeId, execution: ExecutionMetadata) -> Self {
        Self {
            workflow_id,
            node_id,
            execution,
            cancellation: CancellationToken::active(),
        }
    }

    /// Create a copy of this context with cancellation requested.
    #[must_use]
    pub fn with_cancellation(mut self, request: CancellationRequest) -> Self {
        self.cancellation = CancellationToken::cancelled(request);
        self
    }

    /// Attach a shared cancellation token to this context.
    #[must_use]
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation = token;
        self
    }

    /// Workflow currently being executed.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Node currently being executed.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Execution metadata shared by nodes in the same run.
    #[must_use]
    pub const fn execution(&self) -> &ExecutionMetadata {
        &self.execution
    }

    /// Cancellation state visible to this node.
    #[must_use]
    pub fn cancellation(&self) -> CancellationState {
        self.cancellation.state()
    }

    /// Shared cancellation token visible to this node.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Return whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn execution() -> ExecutionMetadata {
        ExecutionMetadata::first_attempt(execution_id("run-1"))
    }

    #[test]
    fn first_execution_attempt_is_one_based() {
        assert_eq!(ExecutionAttempt::first().get(), 1);
    }

    #[test]
    fn node_context_starts_active_and_can_carry_cancellation() {
        let ctx: NodeContext = NodeContext::new(workflow_id("flow"), node_id("node"), execution());

        assert!(!ctx.is_cancelled());
        assert!(matches!(ctx.cancellation(), CancellationState::Active));

        let cancelled: NodeContext =
            ctx.with_cancellation(CancellationRequest::new("shutdown requested"));

        assert!(cancelled.is_cancelled());
        assert!(matches!(
            cancelled.cancellation(),
            CancellationState::Requested(request) if request.reason() == "shutdown requested"
        ));
    }

    #[test]
    fn shared_cancellation_handle_reaches_parent_and_child_contexts() {
        let handle: CancellationHandle = CancellationHandle::new();
        let parent: NodeContext =
            NodeContext::new(workflow_id("flow"), node_id("parent"), execution())
                .with_cancellation_token(handle.token());
        let child: NodeContext =
            NodeContext::new(workflow_id("flow"), node_id("child"), execution())
                .with_cancellation_token(parent.cancellation_token());

        assert!(!parent.is_cancelled());
        assert!(!child.is_cancelled());

        assert!(handle.cancel(CancellationRequest::new("supervisor shutdown")));
        assert!(!handle.cancel(CancellationRequest::new("ignored duplicate")));

        assert!(parent.is_cancelled());
        assert!(child.is_cancelled());
        assert!(matches!(
            child.cancellation(),
            CancellationState::Requested(request) if request.reason() == "supervisor shutdown"
        ));
    }
}
