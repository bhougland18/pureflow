//! Core traits and contracts for Pureflow.

pub mod batch;
pub mod capability;
pub mod context;
pub mod error;
pub mod lifecycle;
pub mod message;
pub mod metadata;
pub mod ports;

use std::future::Future;

pub use batch::{BatchExecutor, BatchInputs, BatchOutputs, WasmModule};
use context::NodeContext;
pub use context::{CancellationHandle, CancellationToken};
pub use error::{
    CancellationError, PureflowError, ErrorCode, ErrorVisibility, ExecutionError, LifecycleError,
    MetadataError, RetryDisposition, ValidationError,
};
pub use message::PacketPayload;
pub use metadata::{
    DeadlockDiagnosticMetadata, ErrorDiagnosticMetadata, ErrorMetadataKind, ErrorMetadataRecord,
    ExternalEffectMetadataKind, ExternalEffectMetadataRecord, JsonlMetadataSink,
    MessageBoundaryKind, MessageBoundaryRecord, MetadataRecord, MetadataSink, MetadataTier,
    NoopMetadataSink, TieredMetadataPolicy, TieredMetadataSink, metadata_record_to_json_value,
};
pub use ports::{
    InputPortHandle, OutputPacketValidator, OutputPortHandle, PortPacket, PortRecvError,
    PortSendError, PortSendPermit, PortsIn, PortsOut, bounded_edge_channel,
};

/// Shared result type for runtime-facing APIs.
pub type Result<T> = std::result::Result<T, PureflowError>;

/// Async node interface for the first runtime skeleton.
///
/// The trait matches the proposal's intended boundary shape early, but the
/// `PortsIn` and `PortsOut` values remain Pureflow-owned adapters. Runtime
/// beads can change the transport behind those handles without leaking raw
/// async runtime primitives into every executor signature.
pub trait NodeExecutor: Sync {
    /// Future returned by one node execution attempt.
    type RunFuture<'a>: Future<Output = Result<()>> + Send + 'a
    where
        Self: 'a;

    /// Execute one runtime-managed node boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if the node cannot complete the requested unit of
    /// work.
    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_>;
}
