//! Lifecycle events and observer hooks at the runtime boundary.
//!
//! ## Fragment: lifecycle-observer-seam
//!
//! The lifecycle surface exists before the runtime fully uses it because
//! observability is part of the public direction of Pureflow, not an afterthought.
//! The current seam is intentionally thin: it names the events that matter and
//! leaves registration, fan-out, and buffering policy to later runtime beads.
//!
//! ## Fragment: lifecycle-event-vocabulary
//!
//! The event kinds are phrased around runtime transitions rather than around
//! implementation details. That keeps the vocabulary stable if the execution
//! engine changes from a sequential scaffold to structured concurrency later.

use crate::{Result, context::NodeContext};

/// Lifecycle event emitted at runtime boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEventKind {
    /// A node has been selected for execution.
    NodeScheduled,
    /// A node is about to start execution.
    NodeStarted,
    /// A node completed successfully.
    NodeCompleted,
    /// A node failed and returned an error.
    NodeFailed,
    /// A node observed or received cancellation.
    NodeCancelled,
}

/// Runtime lifecycle event with the context needed by observers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvent {
    kind: LifecycleEventKind,
    context: NodeContext,
}

impl LifecycleEvent {
    /// Create a lifecycle event for a node context.
    #[must_use]
    pub const fn new(kind: LifecycleEventKind, context: NodeContext) -> Self {
        Self { kind, context }
    }

    /// Kind of lifecycle transition.
    #[must_use]
    pub const fn kind(&self) -> LifecycleEventKind {
        self.kind
    }

    /// Node context associated with the lifecycle transition.
    #[must_use]
    pub const fn context(&self) -> &NodeContext {
        &self.context
    }
}

/// Observer hook for runtime lifecycle transitions.
pub trait LifecycleHook: Sync {
    /// Observe one lifecycle event.
    ///
    /// # Errors
    ///
    /// Returns an error when the observer cannot record or react to the event.
    fn observe(&self, event: &LifecycleEvent) -> Result<()>;
}

/// Default lifecycle hook that intentionally records nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopLifecycleHook;

impl LifecycleHook for NoopLifecycleHook {
    fn observe(&self, _event: &LifecycleEvent) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ExecutionMetadata;
    use pureflow_types::{ExecutionId, NodeId, WorkflowId};

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
    fn lifecycle_event_carries_kind_and_context() {
        let context: NodeContext =
            NodeContext::new(workflow_id("flow"), node_id("node"), execution());
        let event: LifecycleEvent = LifecycleEvent::new(LifecycleEventKind::NodeStarted, context);

        assert_eq!(event.kind(), LifecycleEventKind::NodeStarted);
        assert_eq!(event.context().node_id().as_str(), "node");
    }

    #[test]
    fn noop_lifecycle_hook_accepts_events() {
        let context: NodeContext =
            NodeContext::new(workflow_id("flow"), node_id("node"), execution());
        let event: LifecycleEvent = LifecycleEvent::new(LifecycleEventKind::NodeCompleted, context);

        NoopLifecycleHook
            .observe(&event)
            .expect("noop hook should accept lifecycle events");
    }
}
