//! Shared error types for Pureflow runtime-facing APIs.
//!
//! ## Fragment: error-taxonomy
//!
//! The foundation layer uses one shared error enum because downstream crates
//! already need a consistent contract, but the taxonomy is still kept narrow:
//! validation, execution, cancellation, lifecycle observation, and metadata
//! collection. That is enough to remove stringly-typed errors without
//! inventing categories the runtime has not earned yet.
//!
//! ## Fragment: error-code-stability
//!
//! Error codes are explicit instead of being derived from enum names so logs,
//! tests, and future CLI or API surfaces can depend on stable identifiers even
//! if wording changes. The code surface is intentionally small and can grow
//! only when a new externally meaningful error condition appears.
//!
//! ## Fragment: error-visibility-and-retry
//!
//! Visibility and retry guidance live next to the error variants because they
//! are part of the policy, not just formatting. A validation failure should be
//! safe to show and not worth retrying, while an execution or lifecycle failure
//! is mostly diagnostic until the runtime grows more concrete recovery rules.
//!
//! ## Fragment: asupersync-error-boundary
//!
//! `asupersync` errors are runtime substrate details. The shared Pureflow error
//! model maps them into cancellation or execution failures at the boundary so
//! downstream node and workflow APIs do not grow a public dependency on raw
//! channel or task error types.

use std::error::Error;
use std::fmt;

use asupersync::channel::mpsc;
use asupersync::runtime::JoinError;
use pureflow_types::IdentifierError;

use crate::capability::CapabilityValidationError;
use crate::ports::{PortRecvError, PortSendError};

/// Stable machine-readable code for one Pureflow error condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// A user supplied identifier was malformed.
    InvalidIdentifier,
    /// A node capability descriptor violated capability rules.
    InvalidCapabilities,
    /// A node failed while executing work.
    NodeExecutionFailed,
    /// Execution ended because cancellation was requested.
    ExecutionCancelled,
    /// Runtime lifecycle observation failed.
    LifecycleObservationFailed,
    /// Runtime metadata collection failed.
    MetadataCollectionFailed,
}

impl ErrorCode {
    /// Render the stable code string for logs, tests, and future APIs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidIdentifier => "CDT-VAL-001",
            Self::InvalidCapabilities => "CDT-VAL-002",
            Self::NodeExecutionFailed => "CDT-EXEC-001",
            Self::ExecutionCancelled => "CDT-CANCEL-001",
            Self::LifecycleObservationFailed => "CDT-LIFE-001",
            Self::MetadataCollectionFailed => "CDT-META-001",
        }
    }
}

/// Whether an error should be surfaced directly to a human.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorVisibility {
    /// Safe and useful to show directly to the caller.
    User,
    /// Primarily diagnostic for runtime internals.
    Internal,
}

/// Retry guidance for one error condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    /// Retrying will not help until input or configuration changes.
    Never,
    /// Retrying the same operation can be reasonable.
    Safe,
    /// The runtime cannot determine retry safety from the current surface.
    Unknown,
}

/// Validation error exposed through the shared runtime-facing error model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    /// A Pureflow identifier failed validation.
    Identifier(IdentifierError),
    /// A node capability descriptor failed validation.
    Capability(CapabilityValidationError),
}

impl ValidationError {
    const fn code(&self) -> ErrorCode {
        match self {
            Self::Identifier(_) => ErrorCode::InvalidIdentifier,
            Self::Capability(_) => ErrorCode::InvalidCapabilities,
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identifier(err) => write!(f, "identifier validation failed: {err}"),
            Self::Capability(err) => write!(f, "capability validation failed: {err}"),
        }
    }
}

impl Error for ValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identifier(err) => Some(err),
            Self::Capability(err) => Some(err),
        }
    }
}

impl From<IdentifierError> for ValidationError {
    fn from(value: IdentifierError) -> Self {
        Self::Identifier(value)
    }
}

impl From<CapabilityValidationError> for ValidationError {
    fn from(value: CapabilityValidationError) -> Self {
        Self::Capability(value)
    }
}

/// Runtime execution failure from a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionError {
    message: String,
}

impl ExecutionError {
    /// Create an execution failure with a human-readable message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Human-readable execution failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node execution failed: {}", self.message)
    }
}

impl Error for ExecutionError {}

/// Cancellation observed at the runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancellationError {
    reason: String,
}

impl CancellationError {
    /// Create a cancellation error with a human-readable reason.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Human-readable cancellation reason.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for CancellationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "execution cancelled: {}", self.reason)
    }
}

impl Error for CancellationError {}

/// Failure while recording or reacting to a lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleError {
    message: String,
}

impl LifecycleError {
    /// Create a lifecycle observation failure.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Human-readable lifecycle failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lifecycle observation failed: {}", self.message)
    }
}

impl Error for LifecycleError {}

/// Failure while collecting runtime metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataError {
    message: String,
}

impl MetadataError {
    /// Create a metadata collection failure.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Human-readable metadata failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metadata collection failed: {}", self.message)
    }
}

impl Error for MetadataError {}

/// Shared runtime-facing error for the Pureflow foundation layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PureflowError {
    /// Invalid user- or config-provided data.
    Validation(ValidationError),
    /// Runtime node execution failed.
    Execution(ExecutionError),
    /// Runtime cancelled execution.
    Cancellation(CancellationError),
    /// Runtime failed while observing lifecycle transitions.
    Lifecycle(LifecycleError),
    /// Runtime failed while collecting metadata records.
    Metadata(MetadataError),
}

impl PureflowError {
    /// Create an execution error.
    #[must_use]
    pub fn execution(message: impl Into<String>) -> Self {
        Self::Execution(ExecutionError::new(message))
    }

    /// Create a cancellation error.
    #[must_use]
    pub fn cancelled(reason: impl Into<String>) -> Self {
        Self::Cancellation(CancellationError::new(reason))
    }

    /// Create a lifecycle observation error.
    #[must_use]
    pub fn lifecycle(message: impl Into<String>) -> Self {
        Self::Lifecycle(LifecycleError::new(message))
    }

    /// Create a metadata collection error.
    #[must_use]
    pub fn metadata(message: impl Into<String>) -> Self {
        Self::Metadata(MetadataError::new(message))
    }

    /// Stable error code for this failure.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Validation(err) => err.code(),
            Self::Execution(_) => ErrorCode::NodeExecutionFailed,
            Self::Cancellation(_) => ErrorCode::ExecutionCancelled,
            Self::Lifecycle(_) => ErrorCode::LifecycleObservationFailed,
            Self::Metadata(_) => ErrorCode::MetadataCollectionFailed,
        }
    }

    /// Whether this error should be shown directly to a human.
    #[must_use]
    pub const fn visibility(&self) -> ErrorVisibility {
        match self {
            Self::Validation(_) | Self::Cancellation(_) => ErrorVisibility::User,
            Self::Execution(_) | Self::Lifecycle(_) | Self::Metadata(_) => {
                ErrorVisibility::Internal
            }
        }
    }

    /// Retry guidance for this failure.
    #[must_use]
    pub const fn retry_disposition(&self) -> RetryDisposition {
        match self {
            Self::Validation(_) => RetryDisposition::Never,
            Self::Execution(_) | Self::Lifecycle(_) | Self::Metadata(_) => {
                RetryDisposition::Unknown
            }
            Self::Cancellation(_) => RetryDisposition::Safe,
        }
    }
}

impl fmt::Display for PureflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(err) => write!(f, "{}: {err}", self.code().as_str()),
            Self::Execution(err) => write!(f, "{}: {err}", self.code().as_str()),
            Self::Cancellation(err) => write!(f, "{}: {err}", self.code().as_str()),
            Self::Lifecycle(err) => write!(f, "{}: {err}", self.code().as_str()),
            Self::Metadata(err) => write!(f, "{}: {err}", self.code().as_str()),
        }
    }
}

impl Error for PureflowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Validation(err) => Some(err),
            Self::Execution(err) => Some(err),
            Self::Cancellation(err) => Some(err),
            Self::Lifecycle(err) => Some(err),
            Self::Metadata(err) => Some(err),
        }
    }
}

impl From<ValidationError> for PureflowError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

impl From<IdentifierError> for PureflowError {
    fn from(value: IdentifierError) -> Self {
        Self::Validation(value.into())
    }
}

impl From<CapabilityValidationError> for PureflowError {
    fn from(value: CapabilityValidationError) -> Self {
        Self::Validation(value.into())
    }
}

impl From<ExecutionError> for PureflowError {
    fn from(value: ExecutionError) -> Self {
        Self::Execution(value)
    }
}

impl From<CancellationError> for PureflowError {
    fn from(value: CancellationError) -> Self {
        Self::Cancellation(value)
    }
}

impl From<LifecycleError> for PureflowError {
    fn from(value: LifecycleError) -> Self {
        Self::Lifecycle(value)
    }
}

impl From<MetadataError> for PureflowError {
    fn from(value: MetadataError) -> Self {
        Self::Metadata(value)
    }
}

impl From<JoinError> for PureflowError {
    fn from(value: JoinError) -> Self {
        match value {
            JoinError::Cancelled(reason) => Self::cancelled(reason.to_string()),
            JoinError::Panicked(payload) => {
                Self::execution(format!("asupersync task panicked: {payload}"))
            }
            JoinError::PolledAfterCompletion => {
                Self::execution("asupersync task join polled after completion")
            }
        }
    }
}

impl<T> From<mpsc::SendError<T>> for PureflowError {
    fn from(value: mpsc::SendError<T>) -> Self {
        match value {
            mpsc::SendError::Disconnected(_) => {
                Self::execution("asupersync send failed: receiver disconnected")
            }
            mpsc::SendError::Cancelled(_) => Self::cancelled("asupersync send cancelled"),
            mpsc::SendError::Full(_) => {
                Self::execution("asupersync send failed: bounded channel full")
            }
        }
    }
}

impl From<mpsc::RecvError> for PureflowError {
    fn from(value: mpsc::RecvError) -> Self {
        match value {
            mpsc::RecvError::Disconnected => {
                Self::execution("asupersync receive failed: sender disconnected")
            }
            mpsc::RecvError::Cancelled => Self::cancelled("asupersync receive cancelled"),
            mpsc::RecvError::Empty => Self::execution("asupersync receive failed: channel empty"),
        }
    }
}

impl From<PortSendError> for PureflowError {
    fn from(value: PortSendError) -> Self {
        match value {
            PortSendError::Cancelled { .. } => Self::cancelled(value.to_string()),
            _ => Self::execution(value.to_string()),
        }
    }
}

impl From<PortRecvError> for PureflowError {
    fn from(value: PortRecvError) -> Self {
        match value {
            PortRecvError::Cancelled { .. } => Self::cancelled(value.to_string()),
            _ => Self::execution(value.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::capability::{EffectCapability, NodeCapabilities};
    use asupersync::types::{CancelReason, PanicPayload};
    use pureflow_types::NodeId;

    const ALL_ERROR_CODES: [ErrorCode; 6] = [
        ErrorCode::InvalidIdentifier,
        ErrorCode::InvalidCapabilities,
        ErrorCode::NodeExecutionFailed,
        ErrorCode::ExecutionCancelled,
        ErrorCode::LifecycleObservationFailed,
        ErrorCode::MetadataCollectionFailed,
    ];

    const fn expected_category_prefix(code: ErrorCode) -> &'static str {
        match code {
            ErrorCode::InvalidIdentifier | ErrorCode::InvalidCapabilities => "CDT-VAL-",
            ErrorCode::NodeExecutionFailed => "CDT-EXEC-",
            ErrorCode::ExecutionCancelled => "CDT-CANCEL-",
            ErrorCode::LifecycleObservationFailed => "CDT-LIFE-",
            ErrorCode::MetadataCollectionFailed => "CDT-META-",
        }
    }

    #[test]
    fn error_code_strings_are_unique_nonempty_and_category_prefixed() {
        let mut seen: BTreeSet<&'static str> = BTreeSet::new();

        for code in ALL_ERROR_CODES {
            let value: &'static str = code.as_str();

            assert!(!value.is_empty(), "error code must not be empty: {code:?}");
            assert!(
                value.starts_with(expected_category_prefix(code)),
                "error code {value} should match category for {code:?}"
            );
            assert!(seen.insert(value), "duplicate error code string: {value}");
        }

        assert_eq!(seen.len(), ALL_ERROR_CODES.len());
    }

    #[test]
    fn identifier_errors_map_to_user_facing_non_retryable_codes() {
        let err: PureflowError = IdentifierError::Whitespace {
            kind: pureflow_types::IdentifierKind::Workflow,
        }
        .into();

        assert_eq!(err.code(), ErrorCode::InvalidIdentifier);
        assert_eq!(err.code().as_str(), "CDT-VAL-001");
        assert_eq!(err.visibility(), ErrorVisibility::User);
        assert_eq!(err.retry_disposition(), RetryDisposition::Never);
        assert_eq!(
            err.to_string(),
            "CDT-VAL-001: identifier validation failed: workflow id must not contain whitespace"
        );
    }

    #[test]
    fn capability_errors_map_to_validation_codes() {
        let err: PureflowError = NodeCapabilities::new(
            NodeId::new("reader").expect("valid node id"),
            Vec::new(),
            [
                EffectCapability::FileSystemRead,
                EffectCapability::FileSystemRead,
            ],
        )
        .expect_err("duplicate effect must fail")
        .into();

        assert_eq!(err.code(), ErrorCode::InvalidCapabilities);
        assert_eq!(err.visibility(), ErrorVisibility::User);
        assert_eq!(err.retry_disposition(), RetryDisposition::Never);
    }

    #[test]
    fn execution_errors_are_internal_with_unknown_retry_safety() {
        let err: PureflowError = PureflowError::execution("executor returned failure");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(err.visibility(), ErrorVisibility::Internal);
        assert_eq!(err.retry_disposition(), RetryDisposition::Unknown);
        assert_eq!(
            err.to_string(),
            "CDT-EXEC-001: node execution failed: executor returned failure"
        );
    }

    #[test]
    fn cancellation_errors_are_user_facing_and_safe_to_retry() {
        let err: PureflowError = PureflowError::cancelled("shutdown requested");

        assert_eq!(err.code(), ErrorCode::ExecutionCancelled);
        assert_eq!(err.visibility(), ErrorVisibility::User);
        assert_eq!(err.retry_disposition(), RetryDisposition::Safe);
        assert_eq!(
            err.to_string(),
            "CDT-CANCEL-001: execution cancelled: shutdown requested"
        );
    }

    #[test]
    fn metadata_errors_are_internal_with_unknown_retry_safety() {
        let err: PureflowError = PureflowError::metadata("collector unavailable");

        assert_eq!(err.code(), ErrorCode::MetadataCollectionFailed);
        assert_eq!(err.visibility(), ErrorVisibility::Internal);
        assert_eq!(err.retry_disposition(), RetryDisposition::Unknown);
        assert_eq!(
            err.to_string(),
            "CDT-META-001: metadata collection failed: collector unavailable"
        );
    }

    #[test]
    fn asupersync_join_cancel_maps_to_cancellation() {
        let err: PureflowError = JoinError::Cancelled(CancelReason::user("shutdown")).into();

        assert_eq!(err.code(), ErrorCode::ExecutionCancelled);
        assert_eq!(err.visibility(), ErrorVisibility::User);
        assert_eq!(err.retry_disposition(), RetryDisposition::Safe);
        assert!(err.to_string().contains("shutdown"));
    }

    #[test]
    fn asupersync_join_panic_maps_to_execution_failure() {
        let err: PureflowError = JoinError::Panicked(PanicPayload::new("boom")).into();

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(err.visibility(), ErrorVisibility::Internal);
        assert_eq!(err.retry_disposition(), RetryDisposition::Unknown);
        assert_eq!(
            err.to_string(),
            "CDT-EXEC-001: node execution failed: asupersync task panicked: panic: boom"
        );
    }

    #[test]
    fn asupersync_send_cancel_maps_to_cancellation() {
        let err: PureflowError = mpsc::SendError::Cancelled(()).into();

        assert_eq!(err.code(), ErrorCode::ExecutionCancelled);
        assert_eq!(
            err.to_string(),
            "CDT-CANCEL-001: execution cancelled: asupersync send cancelled"
        );
    }

    #[test]
    fn asupersync_send_full_maps_to_execution_failure() {
        let err: PureflowError = mpsc::SendError::Full(()).into();

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(
            err.to_string(),
            "CDT-EXEC-001: node execution failed: asupersync send failed: bounded channel full"
        );
    }

    #[test]
    fn asupersync_recv_disconnected_maps_to_execution_failure() {
        let err: PureflowError = mpsc::RecvError::Disconnected.into();

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(
            err.to_string(),
            "CDT-EXEC-001: node execution failed: asupersync receive failed: sender disconnected"
        );
    }

    #[test]
    fn port_errors_map_to_execution_failures() {
        let port_id: pureflow_types::PortId =
            pureflow_types::PortId::new("out").expect("valid port id");
        let err: PureflowError = PortSendError::Full { port_id }.into();

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(
            err.to_string(),
            "CDT-EXEC-001: node execution failed: output port `out` is full"
        );
    }

    #[test]
    fn cancelled_port_errors_map_to_cancellation_failures() {
        let port_id: pureflow_types::PortId =
            pureflow_types::PortId::new("out").expect("valid port id");
        let err: PureflowError = PortSendError::Cancelled { port_id }.into();

        assert_eq!(err.code(), ErrorCode::ExecutionCancelled);
        assert_eq!(
            err.to_string(),
            "CDT-CANCEL-001: execution cancelled: output port `out` send cancelled"
        );
    }
}
