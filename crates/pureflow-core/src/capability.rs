//! Capability descriptors that constrain runtime behavior without owning graph shape.
//!
//! ## Fragment: capability-structure-vs-boundary
//!
//! Workflow shape and node capability are intentionally modeled in separate
//! crates. The workflow model answers "what ports exist and how are they
//! connected?", while this module answers "what is a node allowed to receive,
//! emit, or ask the runtime to do?" Keeping those concerns apart prevents the
//! graph model from quietly becoming a security or isolation policy surface.
//!
//! ## Fragment: capability-port-claims
//!
//! Port capabilities are duplicated as claims instead of reusing workflow port
//! declarations directly because they serve a different purpose. Workflow ports
//! describe topology; capability ports describe permitted runtime behavior. The
//! duplication is intentional even though later validation must keep the two in
//! sync.
//!
//! ## Fragment: capability-workflow-cross-validation
//!
//! Cross-validation lives here rather than in the workflow crate because a
//! mismatch is still fundamentally a capability problem: the workflow only
//! promises that a port exists, while this module decides whether a node's
//! declared permissions line up with that topology. The validator therefore
//! consumes a `WorkflowDefinition` as read-only structure and keeps the
//! capability error vocabulary as the single place callers inspect.
//!
//! ## Fragment: capability-effect-taxonomy
//!
//! The current `EffectCapability` enum is intentionally modest and concrete.
//! It names host effects the runtime can plausibly mediate today without
//! pretending that native nodes are sandboxed. For native execution these
//! capabilities are advisory metadata; for future WASM or process-backed nodes
//! they become part of a real enforcement boundary. Observability concerns such
//! as logging, tracing, and metadata collection are deliberately excluded unless
//! a node asks the host to write to an external sink; routine runtime telemetry
//! belongs to the metadata layer.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use pureflow_types::{NodeId, PortId};
use pureflow_workflow::{NodeDefinition, WorkflowDefinition};

/// Direction of message flow a node claims for a port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortCapabilityDirection {
    /// The node may receive messages through the port.
    Receive,
    /// The node may emit messages through the port.
    Emit,
}

impl PortCapabilityDirection {
    const fn label(self) -> &'static str {
        match self {
            Self::Receive => "receive",
            Self::Emit => "emit",
        }
    }
}

/// External effect a node may request from the runtime boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EffectCapability {
    /// Read from host filesystem resources.
    FileSystemRead,
    /// Write to host filesystem resources.
    FileSystemWrite,
    /// Open outbound network connections.
    NetworkOutbound,
    /// Perform an external tool, service, database, or API effect.
    ExternalEffect,
    /// Spawn child processes.
    ProcessSpawn,
    /// Read process environment.
    EnvironmentRead,
    /// Mutate process environment.
    EnvironmentWrite,
    /// Use wall-clock time or timers.
    Clock,
}

impl EffectCapability {
    /// Stable machine-facing label for this effect capability.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FileSystemRead => "filesystem_read",
            Self::FileSystemWrite => "filesystem_write",
            Self::NetworkOutbound => "network_outbound",
            Self::ExternalEffect => "external_effect",
            Self::ProcessSpawn => "process_spawn",
            Self::EnvironmentRead => "environment_read",
            Self::EnvironmentWrite => "environment_write",
            Self::Clock => "clock",
        }
    }
}

/// A named claim that a node may use a port in one direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortCapability {
    port_id: PortId,
    direction: PortCapabilityDirection,
}

impl PortCapability {
    /// Create a port capability claim.
    #[must_use]
    pub const fn new(port_id: PortId, direction: PortCapabilityDirection) -> Self {
        Self { port_id, direction }
    }

    /// Port claimed by this capability.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Direction claimed by this capability.
    #[must_use]
    pub const fn direction(&self) -> PortCapabilityDirection {
        self.direction
    }
}

/// Validation error for node capability descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityValidationError {
    /// A node declared the same effect capability more than once.
    DuplicateEffect {
        /// Node whose capability descriptor is invalid.
        node_id: NodeId,
        /// Duplicated effect capability.
        effect: EffectCapability,
    },
    /// A node declared the same port and direction more than once.
    DuplicatePortCapability {
        /// Node whose capability descriptor is invalid.
        node_id: NodeId,
        /// Duplicated port.
        port_id: PortId,
        /// Duplicated direction.
        direction: PortCapabilityDirection,
    },
    /// A node declared one port as both receive and emit.
    ConflictingPortDirection {
        /// Node whose capability descriptor is invalid.
        node_id: NodeId,
        /// Port with conflicting direction claims.
        port_id: PortId,
    },
    /// A capability descriptor referenced a node absent from the workflow.
    UnknownCapabilityNode {
        /// Node whose capability descriptor does not match the workflow.
        node_id: NodeId,
    },
    /// A capability descriptor referenced a port absent from the workflow node.
    UnknownCapabilityPort {
        /// Node whose capability descriptor does not match the workflow.
        node_id: NodeId,
        /// Port claimed by the capability descriptor.
        port_id: PortId,
    },
    /// A capability descriptor claimed the wrong direction for a declared port.
    CapabilityDirectionMismatch {
        /// Node whose capability descriptor does not match the workflow.
        node_id: NodeId,
        /// Port whose workflow direction and capability claim disagree.
        port_id: PortId,
        /// Direction claimed by the capability descriptor.
        claimed: PortCapabilityDirection,
        /// Direction declared by the workflow topology.
        declared: PortCapabilityDirection,
    },
    /// A strict runtime boundary cannot enforce a declared effect capability.
    UnenforceableEffectCapability {
        /// Node whose effect cannot be enforced.
        node_id: NodeId,
        /// Effect that is not enforceable for the node's execution boundary.
        effect: EffectCapability,
    },
}

impl fmt::Display for CapabilityValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateEffect { node_id, effect } => {
                write!(
                    f,
                    "node `{node_id}` declares duplicate effect capability `{effect:?}`"
                )
            }
            Self::DuplicatePortCapability {
                node_id,
                port_id,
                direction,
            } => write!(
                f,
                "node `{node_id}` declares duplicate {} capability for port `{port_id}`",
                direction.label()
            ),
            Self::ConflictingPortDirection { node_id, port_id } => write!(
                f,
                "node `{node_id}` declares port `{port_id}` for both receive and emit"
            ),
            Self::UnknownCapabilityNode { node_id } => write!(
                f,
                "capability descriptor references unknown workflow node `{node_id}`"
            ),
            Self::UnknownCapabilityPort { node_id, port_id } => write!(
                f,
                "node `{node_id}` capability references unknown workflow port `{port_id}`"
            ),
            Self::CapabilityDirectionMismatch {
                node_id,
                port_id,
                claimed,
                declared,
            } => write!(
                f,
                "node `{node_id}` capability claims port `{port_id}` may {} but workflow declares {}",
                claimed.label(),
                declared.label()
            ),
            Self::UnenforceableEffectCapability { node_id, effect } => write!(
                f,
                "node `{node_id}` declares effect capability `{effect:?}` that is not enforceable by its execution boundary"
            ),
        }
    }
}

impl Error for CapabilityValidationError {}

/// Validated capability descriptor for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCapabilities {
    node_id: NodeId,
    ports: Vec<PortCapability>,
    effects: Vec<EffectCapability>,
}

impl NodeCapabilities {
    /// Create a validated node capability descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor repeats an effect, repeats a
    /// port-direction claim, or declares one port as both receiving and
    /// emitting.
    pub fn new(
        node_id: NodeId,
        ports: impl Into<Vec<PortCapability>>,
        effects: impl Into<Vec<EffectCapability>>,
    ) -> Result<Self, CapabilityValidationError> {
        let ports: Vec<PortCapability> = ports.into();
        let effects: Vec<EffectCapability> = effects.into();
        reject_duplicate_effects(&node_id, &effects)?;
        reject_invalid_port_capabilities(&node_id, &ports)?;

        Ok(Self {
            node_id,
            ports,
            effects,
        })
    }

    /// Create a validated native descriptor with no external effects.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor repeats a port-direction claim or
    /// declares one port as both receiving and emitting.
    pub fn native_passive(
        node_id: NodeId,
        ports: impl Into<Vec<PortCapability>>,
    ) -> Result<Self, CapabilityValidationError> {
        Self::new(node_id, ports, Vec::<EffectCapability>::new())
    }

    /// Node constrained by this capability descriptor.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port capabilities claimed by the node.
    #[must_use]
    pub fn ports(&self) -> &[PortCapability] {
        &self.ports
    }

    /// Effect capabilities claimed by the node.
    #[must_use]
    pub fn effects(&self) -> &[EffectCapability] {
        &self.effects
    }

    /// Return whether this descriptor grants a specific effect capability.
    #[must_use]
    pub fn allows_effect(&self, effect: EffectCapability) -> bool {
        self.effects.contains(&effect)
    }

    /// Return whether this descriptor grants a specific port-direction capability.
    #[must_use]
    pub fn allows_port(&self, port_id: &PortId, direction: PortCapabilityDirection) -> bool {
        self.ports
            .iter()
            .any(|port: &PortCapability| port.port_id() == port_id && port.direction() == direction)
    }
}

/// Validate that node capability descriptors align with one workflow topology.
///
/// # Errors
///
/// Returns an error if a capability descriptor references an unknown node,
/// references an unknown port on a known node, or claims a direction that
/// disagrees with the workflow declaration.
pub fn validate_workflow_capabilities(
    workflow: &WorkflowDefinition,
    capabilities: &[NodeCapabilities],
) -> Result<(), CapabilityValidationError> {
    for capability in capabilities {
        let node: &NodeDefinition = workflow
            .nodes()
            .iter()
            .find(|node: &&NodeDefinition| node.id() == capability.node_id())
            .ok_or_else(|| CapabilityValidationError::UnknownCapabilityNode {
                node_id: capability.node_id().clone(),
            })?;

        for port in capability.ports() {
            let declared: PortCapabilityDirection = workflow_direction_for(node, port.port_id())
                .ok_or_else(|| CapabilityValidationError::UnknownCapabilityPort {
                    node_id: capability.node_id().clone(),
                    port_id: port.port_id().clone(),
                })?;

            if port.direction() != declared {
                return Err(CapabilityValidationError::CapabilityDirectionMismatch {
                    node_id: capability.node_id().clone(),
                    port_id: port.port_id().clone(),
                    claimed: port.direction(),
                    declared,
                });
            }
        }
    }

    Ok(())
}

fn workflow_direction_for(
    node: &NodeDefinition,
    port_id: &PortId,
) -> Option<PortCapabilityDirection> {
    if node.input_ports().contains(port_id) {
        Some(PortCapabilityDirection::Receive)
    } else if node.output_ports().contains(port_id) {
        Some(PortCapabilityDirection::Emit)
    } else {
        None
    }
}

fn reject_duplicate_effects(
    node_id: &NodeId,
    effects: &[EffectCapability],
) -> Result<(), CapabilityValidationError> {
    let mut seen: BTreeSet<EffectCapability> = BTreeSet::new();

    for effect in effects {
        if !seen.insert(*effect) {
            return Err(CapabilityValidationError::DuplicateEffect {
                node_id: node_id.clone(),
                effect: *effect,
            });
        }
    }

    Ok(())
}

fn reject_invalid_port_capabilities(
    node_id: &NodeId,
    ports: &[PortCapability],
) -> Result<(), CapabilityValidationError> {
    let mut receives: BTreeSet<PortId> = BTreeSet::new();
    let mut emits: BTreeSet<PortId> = BTreeSet::new();

    for port in ports {
        let current: &mut BTreeSet<PortId> = match port.direction() {
            PortCapabilityDirection::Receive => &mut receives,
            PortCapabilityDirection::Emit => &mut emits,
        };

        if !current.insert(port.port_id().clone()) {
            return Err(CapabilityValidationError::DuplicatePortCapability {
                node_id: node_id.clone(),
                port_id: port.port_id().clone(),
                direction: port.direction(),
            });
        }
    }

    if let Some(port_id) = receives.intersection(&emits).next() {
        return Err(CapabilityValidationError::ConflictingPortDirection {
            node_id: node_id.clone(),
            port_id: port_id.clone(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_types::WorkflowId;

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn receive(port: &str) -> PortCapability {
        PortCapability::new(port_id(port), PortCapabilityDirection::Receive)
    }

    fn emit(port: &str) -> PortCapability {
        PortCapability::new(port_id(port), PortCapabilityDirection::Emit)
    }

    fn workflow() -> WorkflowDefinition {
        WorkflowDefinition::from_parts(
            WorkflowId::new("flow").expect("valid workflow id"),
            [
                NodeDefinition::new(node_id("source"), Vec::new(), [port_id("out")])
                    .expect("valid source"),
                NodeDefinition::new(node_id("sink"), [port_id("in")], Vec::new())
                    .expect("valid sink"),
            ],
            Vec::new(),
        )
        .expect("valid workflow")
    }

    #[test]
    fn valid_capabilities_keep_ports_and_effects_separate() {
        let capabilities: NodeCapabilities = NodeCapabilities::new(
            node_id("reader"),
            [receive("input"), emit("output")],
            [EffectCapability::FileSystemRead, EffectCapability::Clock],
        )
        .expect("valid capabilities");

        assert_eq!(capabilities.node_id().as_str(), "reader");
        assert!(capabilities.allows_effect(EffectCapability::FileSystemRead));
        assert!(capabilities.allows_port(&port_id("input"), PortCapabilityDirection::Receive));
        assert!(!capabilities.allows_effect(EffectCapability::ProcessSpawn));
    }

    #[test]
    fn effect_capability_labels_are_stable() {
        assert_eq!(EffectCapability::ExternalEffect.as_str(), "external_effect");
        assert_eq!(
            EffectCapability::NetworkOutbound.as_str(),
            "network_outbound"
        );
    }

    #[test]
    fn native_passive_capabilities_have_no_effects() {
        let capabilities: NodeCapabilities =
            NodeCapabilities::native_passive(node_id("reader"), [receive("input")])
                .expect("valid passive capabilities");

        assert_eq!(capabilities.effects(), []);
        assert!(capabilities.allows_port(&port_id("input"), PortCapabilityDirection::Receive));
    }

    #[test]
    fn duplicate_effects_are_rejected() {
        let err: CapabilityValidationError = NodeCapabilities::new(
            node_id("reader"),
            Vec::<PortCapability>::new(),
            [
                EffectCapability::FileSystemRead,
                EffectCapability::FileSystemRead,
            ],
        )
        .expect_err("duplicate effect must fail");

        assert_eq!(
            err,
            CapabilityValidationError::DuplicateEffect {
                node_id: node_id("reader"),
                effect: EffectCapability::FileSystemRead
            }
        );
    }

    #[test]
    fn duplicate_port_direction_is_rejected() {
        let err: CapabilityValidationError = NodeCapabilities::new(
            node_id("reader"),
            [receive("input"), receive("input")],
            Vec::<EffectCapability>::new(),
        )
        .expect_err("duplicate port direction must fail");

        assert_eq!(
            err,
            CapabilityValidationError::DuplicatePortCapability {
                node_id: node_id("reader"),
                port_id: port_id("input"),
                direction: PortCapabilityDirection::Receive
            }
        );
    }

    #[test]
    fn conflicting_port_directions_are_rejected() {
        let err: CapabilityValidationError = NodeCapabilities::new(
            node_id("router"),
            [receive("data"), emit("data")],
            Vec::<EffectCapability>::new(),
        )
        .expect_err("conflicting port direction must fail");

        assert_eq!(
            err,
            CapabilityValidationError::ConflictingPortDirection {
                node_id: node_id("router"),
                port_id: port_id("data")
            }
        );
    }

    #[test]
    fn workflow_capabilities_accept_matching_node_ports() {
        let workflow: WorkflowDefinition = workflow();
        let capabilities: Vec<NodeCapabilities> = vec![
            NodeCapabilities::new(
                node_id("source"),
                [emit("out")],
                Vec::<EffectCapability>::new(),
            )
            .expect("valid source capabilities"),
            NodeCapabilities::new(
                node_id("sink"),
                [receive("in")],
                Vec::<EffectCapability>::new(),
            )
            .expect("valid sink capabilities"),
        ];

        validate_workflow_capabilities(&workflow, &capabilities)
            .expect("matching workflow capabilities should validate");
    }

    #[test]
    fn workflow_capabilities_reject_unknown_node() {
        let workflow: WorkflowDefinition = workflow();
        let capabilities: Vec<NodeCapabilities> = vec![
            NodeCapabilities::new(
                node_id("ghost"),
                [emit("out")],
                Vec::<EffectCapability>::new(),
            )
            .expect("self-consistent capability descriptor"),
        ];

        let err: CapabilityValidationError =
            validate_workflow_capabilities(&workflow, &capabilities)
                .expect_err("unknown workflow node must fail");

        assert_eq!(
            err,
            CapabilityValidationError::UnknownCapabilityNode {
                node_id: node_id("ghost")
            }
        );
    }

    #[test]
    fn workflow_capabilities_reject_unknown_port() {
        let workflow: WorkflowDefinition = workflow();
        let capabilities: Vec<NodeCapabilities> = vec![
            NodeCapabilities::new(
                node_id("sink"),
                [receive("missing")],
                Vec::<EffectCapability>::new(),
            )
            .expect("self-consistent capability descriptor"),
        ];

        let err: CapabilityValidationError =
            validate_workflow_capabilities(&workflow, &capabilities)
                .expect_err("unknown workflow port must fail");

        assert_eq!(
            err,
            CapabilityValidationError::UnknownCapabilityPort {
                node_id: node_id("sink"),
                port_id: port_id("missing")
            }
        );
    }

    #[test]
    fn workflow_capabilities_reject_direction_mismatch() {
        let workflow: WorkflowDefinition = workflow();
        let capabilities: Vec<NodeCapabilities> = vec![
            NodeCapabilities::new(
                node_id("sink"),
                [emit("in")],
                Vec::<EffectCapability>::new(),
            )
            .expect("self-consistent capability descriptor"),
        ];

        let err: CapabilityValidationError =
            validate_workflow_capabilities(&workflow, &capabilities)
                .expect_err("direction mismatch must fail");

        assert_eq!(
            err,
            CapabilityValidationError::CapabilityDirectionMismatch {
                node_id: node_id("sink"),
                port_id: port_id("in"),
                claimed: PortCapabilityDirection::Emit,
                declared: PortCapabilityDirection::Receive,
            }
        );
    }

    #[test]
    fn unenforceable_effect_capability_is_a_capability_error() {
        let err: CapabilityValidationError =
            CapabilityValidationError::UnenforceableEffectCapability {
                node_id: node_id("wasm"),
                effect: EffectCapability::Clock,
            };

        assert!(err.to_string().contains("not enforceable"));
    }
}
