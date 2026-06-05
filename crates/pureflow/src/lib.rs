//! Pureflow — a flow-based programming (FBP) workflow engine for building concurrent data pipelines.
//!
//! # Feature flags
//!
//! | Feature | Enables |
//! |---------|---------|
//! | `wasm` | Wasmtime-backed WASM batch node execution |
//! | `introspection` | Read-only workflow introspection with JSON serialization |
//! | `tracing` | `tracing` crate integration for runtime observability |
//! | `arrow` | Apache Arrow columnar data support in message payloads |
//! | `toml` | TOML workflow definition parser |
//! | `yaml` | YAML workflow definition parser |
//! | `full` | All of the above |
//!
//! # Modules
//!
//! The full API surface of each internal crate is re-exported under a matching
//! module. Use the flat top-level re-exports for the most common items, and
//! reach into the modules for anything more specific.

/// Identifier types shared across Pureflow.
pub mod types {
    pub use pureflow_types::*;
}

/// Workflow structure: nodes, edges, and graph validation.
pub mod workflow {
    pub use pureflow_workflow::*;
}

/// Core traits and contracts for node implementors.
pub mod core {
    pub use pureflow_core::*;
}

/// Node contract metadata and port-level validation.
pub mod contract {
    pub use pureflow_contract::*;
}

/// Workflow execution engine: run policies, registries, and entry points.
pub mod engine {
    pub use pureflow_engine::*;
}

/// Low-level async runtime that backs the engine.
pub mod runtime {
    pub use pureflow_runtime::*;
}

/// Workflow definition parsers (JSON always; TOML and YAML behind features).
pub mod format {
    pub use pureflow_workflow_format::*;
}

/// Read-only workflow introspection projections.
#[cfg(feature = "introspection")]
pub mod introspection {
    pub use pureflow_introspection::*;
}

/// Wasmtime-backed WASM batch node execution.
#[cfg(feature = "wasm")]
pub mod wasm {
    pub use pureflow_wasm::*;
}

// Flat re-exports of the most commonly used items.

pub use pureflow_types::{ExecutionId, IdentifierError, MessageId, NodeId, PortId, WorkflowId};

pub use pureflow_workflow::{
    EdgeDefinition, EdgeEndpoint, NodeDefinition, PortDirection, WorkflowDefinition,
    WorkflowValidationError,
};

pub use pureflow_core::{
    CancellationHandle, CancellationToken, ExecutionError, LifecycleError, NodeExecutor,
    PacketPayload, PortPacket, PortsIn, PortsOut, PureflowError, Result,
};

pub use pureflow_contract::{Determinism, ExecutionMode, NodeContract, PortContract};

pub use pureflow_engine::{
    BatchNodeExecutor, StaticNodeExecutorRegistry, WorkflowRunPolicy, WorkflowRunSummary,
    WorkflowTerminalState, run_workflow_with_registry_policy_summary,
    run_workflow_with_registry_summary,
};
