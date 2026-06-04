//! Pure workflow and contract introspection projections.
//!
//! ## Fragment: introspection-pure-projection
//!
//! Introspection is intentionally a read-only view over already-authored
//! workflow, contract, and capability data. It performs the same validation
//! required before execution planning, but it does not require a runtime,
//! allocate channels, or ask nodes to run.
//!
//! ## Fragment: introspection-enforcement-language
//!
//! The enforcement level names the execution boundary, not a security promise
//! that applies uniformly to every mode. Native capabilities are advisory
//! metadata for inspection, while WASM and future process-backed execution are
//! strict boundaries whose declared effects must be enforceable before the
//! workflow can pass contract validation.

use std::collections::BTreeMap;

use pureflow_contract::{
    ContractValidationError, Determinism, ExecutionMode, NodeContract, PortContract, SchemaRef,
    validate_workflow_contracts,
};
use pureflow_core::{
    RetryDisposition,
    capability::{EffectCapability, NodeCapabilities, PortCapability, PortCapabilityDirection},
};
use pureflow_types::{NodeId, PortId, WorkflowId};
use pureflow_workflow::{
    EdgeCapacity, EdgeDefinition, EdgeEndpoint, NodeDefinition, PortDirection, WorkflowDefinition,
};

/// Runtime boundary strength declared by a node contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementLevel {
    /// Native host code is inspectable but not sandboxed by Pureflow.
    Advisory,
    /// A strict boundary must be able to enforce every declared effect.
    Strict,
}

/// Introspectable view of one validated workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowIntrospection {
    workflow_id: WorkflowId,
    nodes: Vec<NodeIntrospection>,
    edges: Vec<EdgeIntrospection>,
}

impl WorkflowIntrospection {
    /// Workflow being described.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Node views in workflow declaration order.
    #[must_use]
    pub fn nodes(&self) -> &[NodeIntrospection] {
        &self.nodes
    }

    /// Edge views in workflow declaration order.
    #[must_use]
    pub fn edges(&self) -> &[EdgeIntrospection] {
        &self.edges
    }
}

/// Introspectable view of one workflow node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIntrospection {
    node_id: NodeId,
    ports: Vec<PortIntrospection>,
    execution_mode: ExecutionMode,
    enforcement: EnforcementLevel,
    determinism: Determinism,
    retry: RetryDisposition,
    effects: Vec<EffectCapability>,
}

impl NodeIntrospection {
    /// Node being described.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port views in workflow input-then-output order.
    #[must_use]
    pub fn ports(&self) -> &[PortIntrospection] {
        &self.ports
    }

    /// Contract-declared execution mode.
    #[must_use]
    pub const fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Enforcement strength implied by the execution mode.
    #[must_use]
    pub const fn enforcement(&self) -> EnforcementLevel {
        self.enforcement
    }

    /// Contract-declared determinism.
    #[must_use]
    pub const fn determinism(&self) -> Determinism {
        self.determinism
    }

    /// Contract-declared retry disposition.
    #[must_use]
    pub const fn retry(&self) -> RetryDisposition {
        self.retry
    }

    /// Declared external effect capabilities.
    #[must_use]
    pub fn effects(&self) -> &[EffectCapability] {
        &self.effects
    }
}

/// Introspectable view of one node port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortIntrospection {
    port_id: PortId,
    direction: PortDirection,
    schema: Option<SchemaRef>,
    capability: PortCapabilityDirection,
}

impl PortIntrospection {
    /// Port being described.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Workflow and contract-declared port direction.
    #[must_use]
    pub const fn direction(&self) -> PortDirection {
        self.direction
    }

    /// Contract-declared schema reference, if present.
    #[must_use]
    pub const fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Capability claim that permits use of this port.
    #[must_use]
    pub const fn capability(&self) -> PortCapabilityDirection {
        self.capability
    }
}

/// Introspectable view of one workflow edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeIntrospection {
    source: EndpointIntrospection,
    target: EndpointIntrospection,
    capacity: EdgeCapacity,
    source_schema: Option<SchemaRef>,
    target_schema: Option<SchemaRef>,
}

impl EdgeIntrospection {
    /// Source endpoint for the edge.
    #[must_use]
    pub const fn source(&self) -> &EndpointIntrospection {
        &self.source
    }

    /// Target endpoint for the edge.
    #[must_use]
    pub const fn target(&self) -> &EndpointIntrospection {
        &self.target
    }

    /// Edge capacity policy.
    #[must_use]
    pub const fn capacity(&self) -> EdgeCapacity {
        self.capacity
    }

    /// Schema declared by the source port, if any.
    #[must_use]
    pub const fn source_schema(&self) -> Option<&SchemaRef> {
        self.source_schema.as_ref()
    }

    /// Schema declared by the target port, if any.
    #[must_use]
    pub const fn target_schema(&self) -> Option<&SchemaRef> {
        self.target_schema.as_ref()
    }
}

/// Node and port address for one edge endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointIntrospection {
    node_id: NodeId,
    port_id: PortId,
}

impl EndpointIntrospection {
    fn from_endpoint(endpoint: &EdgeEndpoint) -> Self {
        Self {
            node_id: endpoint.node_id().clone(),
            port_id: endpoint.port_id().clone(),
        }
    }

    /// Endpoint node identifier.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Endpoint port identifier.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }
}

/// Build a pure data introspection view from validated workflow inputs.
///
/// # Errors
///
/// Returns an error if the contracts or capability descriptors do not match
/// the workflow topology, if connected schemas are incompatible, or if a
/// strict execution mode declares an unenforceable effect.
pub fn introspect_workflow(
    workflow: &WorkflowDefinition,
    contracts: &[NodeContract],
    capabilities: &[NodeCapabilities],
) -> Result<WorkflowIntrospection, ContractValidationError> {
    validate_workflow_contracts(workflow, contracts, capabilities)?;

    let contracts_by_node: BTreeMap<&NodeId, &NodeContract> = contracts
        .iter()
        .map(|contract: &NodeContract| (contract.id(), contract))
        .collect();
    let capabilities_by_node: BTreeMap<&NodeId, &NodeCapabilities> = capabilities
        .iter()
        .map(|capability: &NodeCapabilities| (capability.node_id(), capability))
        .collect();

    let nodes: Vec<NodeIntrospection> = workflow
        .nodes()
        .iter()
        .map(|node: &NodeDefinition| {
            let contract: &NodeContract =
                contracts_by_node.get(node.id()).copied().ok_or_else(|| {
                    ContractValidationError::UnknownWorkflowNode {
                        node_id: node.id().clone(),
                    }
                })?;
            let capability: &NodeCapabilities = capabilities_by_node
                .get(node.id())
                .copied()
                .ok_or_else(|| ContractValidationError::MissingCapabilityDescriptor {
                    node_id: node.id().clone(),
                })?;

            introspect_node(node, contract, capability)
        })
        .collect::<Result<_, ContractValidationError>>()?;
    let edges: Vec<EdgeIntrospection> = workflow
        .edges()
        .iter()
        .map(|edge: &EdgeDefinition| introspect_edge(edge, &contracts_by_node))
        .collect();

    Ok(WorkflowIntrospection {
        workflow_id: workflow.id().clone(),
        nodes,
        edges,
    })
}

fn introspect_node(
    node: &NodeDefinition,
    contract: &NodeContract,
    capability: &NodeCapabilities,
) -> Result<NodeIntrospection, ContractValidationError> {
    let contract_ports: BTreeMap<&PortId, &PortContract> = contract
        .ports()
        .iter()
        .map(|port: &PortContract| (port.port_id(), port))
        .collect();
    let capability_ports: BTreeMap<&PortId, &PortCapability> = capability
        .ports()
        .iter()
        .map(|port: &PortCapability| (port.port_id(), port))
        .collect();
    let ports: Vec<PortIntrospection> = node
        .input_ports()
        .iter()
        .map(|port_id: &PortId| {
            introspect_port(
                node.id(),
                port_id,
                PortDirection::Input,
                &contract_ports,
                &capability_ports,
            )
        })
        .chain(node.output_ports().iter().map(|port_id: &PortId| {
            introspect_port(
                node.id(),
                port_id,
                PortDirection::Output,
                &contract_ports,
                &capability_ports,
            )
        }))
        .collect::<Result<_, ContractValidationError>>()?;

    Ok(NodeIntrospection {
        node_id: node.id().clone(),
        ports,
        execution_mode: contract.execution_mode(),
        enforcement: enforcement_for(contract.execution_mode()),
        determinism: contract.determinism(),
        retry: contract.retry(),
        effects: capability.effects().to_vec(),
    })
}

fn introspect_port(
    node_id: &NodeId,
    port_id: &PortId,
    direction: PortDirection,
    contract_ports: &BTreeMap<&PortId, &PortContract>,
    capability_ports: &BTreeMap<&PortId, &PortCapability>,
) -> Result<PortIntrospection, ContractValidationError> {
    let contract: &PortContract = contract_ports.get(port_id).copied().ok_or_else(|| {
        ContractValidationError::UnknownWorkflowPort {
            node_id: node_id.clone(),
            port_id: port_id.clone(),
            direction,
        }
    })?;
    let capability: &PortCapability = capability_ports.get(port_id).copied().ok_or_else(|| {
        ContractValidationError::MissingCapabilityDescriptor {
            node_id: node_id.clone(),
        }
    })?;

    Ok(PortIntrospection {
        port_id: port_id.clone(),
        direction,
        schema: contract.schema().cloned(),
        capability: capability.direction(),
    })
}

fn introspect_edge(
    edge: &EdgeDefinition,
    contracts_by_node: &BTreeMap<&NodeId, &NodeContract>,
) -> EdgeIntrospection {
    EdgeIntrospection {
        source: EndpointIntrospection::from_endpoint(edge.source()),
        target: EndpointIntrospection::from_endpoint(edge.target()),
        capacity: edge.capacity(),
        source_schema: endpoint_schema(edge.source(), contracts_by_node),
        target_schema: endpoint_schema(edge.target(), contracts_by_node),
    }
}

fn endpoint_schema(
    endpoint: &EdgeEndpoint,
    contracts_by_node: &BTreeMap<&NodeId, &NodeContract>,
) -> Option<SchemaRef> {
    contracts_by_node
        .get(endpoint.node_id())
        .and_then(|contract: &&NodeContract| {
            contract
                .ports()
                .iter()
                .find(|port: &&PortContract| port.port_id() == endpoint.port_id())
        })
        .and_then(PortContract::schema)
        .cloned()
}

const fn enforcement_for(execution_mode: ExecutionMode) -> EnforcementLevel {
    match execution_mode {
        ExecutionMode::Native => EnforcementLevel::Advisory,
        ExecutionMode::Wasm | ExecutionMode::Process => EnforcementLevel::Strict,
    }
}

/// Build a stable JSON value for AI and CLI inspection consumers.
#[cfg(feature = "serde")]
#[must_use]
pub fn workflow_introspection_to_json_value(
    introspection: &WorkflowIntrospection,
) -> serde_json::Value {
    serde_json::json!({
        "workflow_id": introspection.workflow_id().as_str(),
        "nodes": introspection
            .nodes()
            .iter()
            .map(node_to_json)
            .collect::<Vec<serde_json::Value>>(),
        "edges": introspection
            .edges()
            .iter()
            .map(edge_to_json)
            .collect::<Vec<serde_json::Value>>(),
    })
}

/// Render workflow introspection as stable, pretty-printed JSON.
///
/// # Errors
///
/// Returns an error if `serde_json` cannot encode the generated JSON value.
#[cfg(feature = "serde")]
pub fn workflow_introspection_to_json_string(
    introspection: &WorkflowIntrospection,
) -> Result<String, IntrospectionJsonError> {
    serde_json::to_string_pretty(&workflow_introspection_to_json_value(introspection))
        .map_err(|source: serde_json::Error| IntrospectionJsonError { source })
}

/// Error returned when workflow introspection JSON cannot be encoded.
#[cfg(feature = "serde")]
#[derive(Debug)]
pub struct IntrospectionJsonError {
    source: serde_json::Error,
}

#[cfg(feature = "serde")]
impl IntrospectionJsonError {
    /// Underlying JSON encoder error.
    #[must_use]
    pub const fn source_error(&self) -> &serde_json::Error {
        &self.source
    }
}

#[cfg(feature = "serde")]
impl std::fmt::Display for IntrospectionJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to encode workflow introspection JSON: {}",
            self.source
        )
    }
}

#[cfg(feature = "serde")]
impl std::error::Error for IntrospectionJsonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(feature = "serde")]
fn node_to_json(node: &NodeIntrospection) -> serde_json::Value {
    serde_json::json!({
        "node_id": node.node_id().as_str(),
        "execution_mode": execution_mode_label(node.execution_mode()),
        "enforcement": enforcement_label(node.enforcement()),
        "determinism": determinism_label(node.determinism()),
        "retry": retry_label(node.retry()),
        "effects": node
            .effects()
            .iter()
            .map(|effect: &EffectCapability| effect_label(*effect))
            .collect::<Vec<&'static str>>(),
        "ports": node
            .ports()
            .iter()
            .map(port_to_json)
            .collect::<Vec<serde_json::Value>>(),
    })
}

#[cfg(feature = "serde")]
fn port_to_json(port: &PortIntrospection) -> serde_json::Value {
    serde_json::json!({
        "port_id": port.port_id().as_str(),
        "direction": port_direction_label(port.direction()),
        "schema": port.schema().map(SchemaRef::as_str),
        "capability": port_capability_label(port.capability()),
    })
}

#[cfg(feature = "serde")]
fn edge_to_json(edge: &EdgeIntrospection) -> serde_json::Value {
    serde_json::json!({
        "source": endpoint_to_json(edge.source()),
        "target": endpoint_to_json(edge.target()),
        "capacity": edge_capacity_to_json(edge.capacity()),
        "source_schema": edge.source_schema().map(SchemaRef::as_str),
        "target_schema": edge.target_schema().map(SchemaRef::as_str),
    })
}

#[cfg(feature = "serde")]
fn endpoint_to_json(endpoint: &EndpointIntrospection) -> serde_json::Value {
    serde_json::json!({
        "node_id": endpoint.node_id().as_str(),
        "port_id": endpoint.port_id().as_str(),
    })
}

#[cfg(feature = "serde")]
fn edge_capacity_to_json(capacity: EdgeCapacity) -> serde_json::Value {
    match capacity {
        EdgeCapacity::Default => serde_json::json!({ "kind": "default" }),
        EdgeCapacity::Explicit(value) => {
            serde_json::json!({ "kind": "explicit", "value": value.get() })
        }
    }
}

#[cfg(feature = "serde")]
const fn execution_mode_label(execution_mode: ExecutionMode) -> &'static str {
    match execution_mode {
        ExecutionMode::Native => "native",
        ExecutionMode::Wasm => "wasm",
        ExecutionMode::Process => "process",
    }
}

#[cfg(feature = "serde")]
const fn enforcement_label(enforcement: EnforcementLevel) -> &'static str {
    match enforcement {
        EnforcementLevel::Advisory => "advisory",
        EnforcementLevel::Strict => "strict",
    }
}

#[cfg(feature = "serde")]
const fn determinism_label(determinism: Determinism) -> &'static str {
    match determinism {
        Determinism::Deterministic => "deterministic",
        Determinism::NonDeterministic => "non_deterministic",
        Determinism::Unknown => "unknown",
    }
}

#[cfg(feature = "serde")]
const fn retry_label(retry: RetryDisposition) -> &'static str {
    match retry {
        RetryDisposition::Never => "never",
        RetryDisposition::Safe => "safe",
        RetryDisposition::Unknown => "unknown",
    }
}

#[cfg(feature = "serde")]
const fn port_direction_label(direction: PortDirection) -> &'static str {
    match direction {
        PortDirection::Input => "input",
        PortDirection::Output => "output",
    }
}

#[cfg(feature = "serde")]
const fn port_capability_label(capability: PortCapabilityDirection) -> &'static str {
    match capability {
        PortCapabilityDirection::Receive => "receive",
        PortCapabilityDirection::Emit => "emit",
    }
}

#[cfg(feature = "serde")]
const fn effect_label(effect: EffectCapability) -> &'static str {
    effect.as_str()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use pureflow_contract::{Determinism, ExecutionMode, NodeContract, PortContract, SchemaRef};
    use pureflow_core::{
        RetryDisposition,
        capability::{EffectCapability, NodeCapabilities, PortCapability, PortCapabilityDirection},
    };
    use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, node_id, port_id};

    fn schema(value: &str) -> SchemaRef {
        SchemaRef::new(value).expect("test schema ref must be valid")
    }

    fn contract(
        node: &str,
        ports: Vec<PortContract>,
        execution_mode: ExecutionMode,
        determinism: Determinism,
    ) -> NodeContract {
        NodeContract::new(
            node_id(node),
            ports,
            execution_mode,
            determinism,
            RetryDisposition::Safe,
        )
        .expect("test contract must be valid")
    }

    fn capabilities(
        node: &str,
        ports: Vec<PortCapability>,
        effects: Vec<EffectCapability>,
    ) -> NodeCapabilities {
        NodeCapabilities::new(node_id(node), ports, effects)
            .expect("test capabilities must be valid")
    }

    #[cfg(feature = "serde")]
    fn sample_linear_introspection() -> WorkflowIntrospection {
        let capacity = NonZeroUsize::new(8).expect("non-zero capacity");
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge_with_capacity("source", "out", "sink", "in", capacity)
            .build();
        let contracts = vec![
            contract(
                "source",
                vec![PortContract::new(
                    port_id("out"),
                    PortDirection::Output,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
                Determinism::Deterministic,
            ),
            contract(
                "sink",
                vec![PortContract::new(
                    port_id("in"),
                    PortDirection::Input,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
                Determinism::NonDeterministic,
            ),
        ];
        let capabilities = vec![
            capabilities(
                "source",
                vec![PortCapability::new(
                    port_id("out"),
                    PortCapabilityDirection::Emit,
                )],
                vec![EffectCapability::Clock],
            ),
            capabilities(
                "sink",
                vec![PortCapability::new(
                    port_id("in"),
                    PortCapabilityDirection::Receive,
                )],
                Vec::new(),
            ),
        ];

        introspect_workflow(&workflow, &contracts, &capabilities)
            .expect("matching introspection inputs must validate")
    }

    #[cfg(feature = "serde")]
    fn sample_mixed_native_wasm_introspection() -> WorkflowIntrospection {
        let capacity = NonZeroUsize::new(4).expect("non-zero capacity");
        let workflow = WorkflowBuilder::new("mixed")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("guest").input("in").output("out").build())
            .edge_with_capacity("source", "out", "guest", "in", capacity)
            .build();
        let contracts = vec![
            contract(
                "source",
                vec![PortContract::new(
                    port_id("out"),
                    PortDirection::Output,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
                Determinism::Deterministic,
            ),
            contract(
                "guest",
                vec![
                    PortContract::new(
                        port_id("in"),
                        PortDirection::Input,
                        Some(schema("schema://packet")),
                    ),
                    PortContract::new(
                        port_id("out"),
                        PortDirection::Output,
                        Some(schema("schema://uppercase-packet")),
                    ),
                ],
                ExecutionMode::Wasm,
                Determinism::Unknown,
            ),
        ];
        let capabilities = vec![
            capabilities(
                "source",
                vec![PortCapability::new(
                    port_id("out"),
                    PortCapabilityDirection::Emit,
                )],
                vec![EffectCapability::Clock],
            ),
            capabilities(
                "guest",
                vec![
                    PortCapability::new(port_id("in"), PortCapabilityDirection::Receive),
                    PortCapability::new(port_id("out"), PortCapabilityDirection::Emit),
                ],
                Vec::new(),
            ),
        ];

        introspect_workflow(&workflow, &contracts, &capabilities)
            .expect("mixed native and wasm introspection inputs must validate")
    }

    #[test]
    fn introspection_projects_workflow_contracts_and_capabilities() {
        let capacity = NonZeroUsize::new(8).expect("non-zero capacity");
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge_with_capacity("source", "out", "sink", "in", capacity)
            .build();
        let contracts = vec![
            contract(
                "source",
                vec![PortContract::new(
                    port_id("out"),
                    PortDirection::Output,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
                Determinism::Deterministic,
            ),
            contract(
                "sink",
                vec![PortContract::new(
                    port_id("in"),
                    PortDirection::Input,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
                Determinism::NonDeterministic,
            ),
        ];
        let capabilities = vec![
            capabilities(
                "source",
                vec![PortCapability::new(
                    port_id("out"),
                    PortCapabilityDirection::Emit,
                )],
                vec![EffectCapability::Clock],
            ),
            capabilities(
                "sink",
                vec![PortCapability::new(
                    port_id("in"),
                    PortCapabilityDirection::Receive,
                )],
                Vec::new(),
            ),
        ];

        let view = introspect_workflow(&workflow, &contracts, &capabilities)
            .expect("matching introspection inputs must validate");

        assert_eq!(view.workflow_id().as_str(), "flow");
        assert_eq!(view.nodes().len(), 2);
        assert_eq!(view.nodes()[0].node_id().as_str(), "source");
        assert_eq!(view.nodes()[0].execution_mode(), ExecutionMode::Native);
        assert_eq!(view.nodes()[0].enforcement(), EnforcementLevel::Advisory);
        assert_eq!(view.nodes()[0].determinism(), Determinism::Deterministic);
        assert_eq!(view.nodes()[0].retry(), RetryDisposition::Safe);
        assert_eq!(view.nodes()[0].effects(), &[EffectCapability::Clock]);
        assert_eq!(view.nodes()[0].ports()[0].port_id().as_str(), "out");
        assert_eq!(
            view.nodes()[0].ports()[0].direction(),
            PortDirection::Output
        );
        assert_eq!(
            view.nodes()[0].ports()[0]
                .schema()
                .expect("source schema")
                .as_str(),
            "schema://packet"
        );
        assert_eq!(
            view.nodes()[0].ports()[0].capability(),
            PortCapabilityDirection::Emit
        );
        assert_eq!(view.nodes()[1].determinism(), Determinism::NonDeterministic);
        assert_eq!(view.edges().len(), 1);
        assert_eq!(view.edges()[0].capacity(), EdgeCapacity::Explicit(capacity));
        assert_eq!(view.edges()[0].source().node_id().as_str(), "source");
        assert_eq!(view.edges()[0].target().port_id().as_str(), "in");
        assert_eq!(
            view.edges()[0]
                .source_schema()
                .expect("source edge schema")
                .as_str(),
            "schema://packet"
        );
        assert_eq!(
            view.edges()[0]
                .target_schema()
                .expect("target edge schema")
                .as_str(),
            "schema://packet"
        );
    }

    #[test]
    fn introspection_reuses_contract_validation() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let contracts = vec![
            contract(
                "source",
                vec![PortContract::new(
                    port_id("out"),
                    PortDirection::Output,
                    Some(schema("schema://a")),
                )],
                ExecutionMode::Native,
                Determinism::Deterministic,
            ),
            contract(
                "sink",
                vec![PortContract::new(
                    port_id("in"),
                    PortDirection::Input,
                    Some(schema("schema://b")),
                )],
                ExecutionMode::Native,
                Determinism::Deterministic,
            ),
        ];
        let capabilities = vec![
            capabilities(
                "source",
                vec![PortCapability::new(
                    port_id("out"),
                    PortCapabilityDirection::Emit,
                )],
                Vec::new(),
            ),
            capabilities(
                "sink",
                vec![PortCapability::new(
                    port_id("in"),
                    PortCapabilityDirection::Receive,
                )],
                Vec::new(),
            ),
        ];

        let err = introspect_workflow(&workflow, &contracts, &capabilities)
            .expect_err("schema mismatch must fail before projection");

        assert!(matches!(
            err,
            ContractValidationError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn introspection_rejects_missing_workflow_node_contract() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let contracts = vec![contract(
            "other",
            vec![PortContract::new(
                port_id("out"),
                PortDirection::Output,
                None,
            )],
            ExecutionMode::Native,
            Determinism::Deterministic,
        )];
        let capabilities = vec![capabilities(
            "source",
            vec![PortCapability::new(
                port_id("out"),
                PortCapabilityDirection::Emit,
            )],
            Vec::new(),
        )];

        let err = introspect_workflow(&workflow, &contracts, &capabilities)
            .expect_err("missing workflow node contract must fail");

        assert_eq!(
            err,
            ContractValidationError::UnknownWorkflowNode {
                node_id: node_id("source")
            }
        );
    }

    #[test]
    fn introspection_rejects_port_direction_mismatch() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let contracts = vec![contract(
            "source",
            vec![PortContract::new(
                port_id("out"),
                PortDirection::Input,
                None,
            )],
            ExecutionMode::Native,
            Determinism::Deterministic,
        )];
        let capabilities = vec![capabilities(
            "source",
            vec![PortCapability::new(
                port_id("out"),
                PortCapabilityDirection::Emit,
            )],
            Vec::new(),
        )];

        let err = introspect_workflow(&workflow, &contracts, &capabilities)
            .expect_err("port direction mismatch must fail");

        assert_eq!(
            err,
            ContractValidationError::PortDirectionMismatch {
                node_id: node_id("source"),
                port_id: port_id("out"),
                workflow: PortDirection::Output,
                contract: PortDirection::Input,
            }
        );
    }

    #[test]
    fn wasm_contracts_report_strict_enforcement() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("guest").input("in").output("out").build())
            .build();
        let contracts = vec![contract(
            "guest",
            vec![
                PortContract::new(port_id("in"), PortDirection::Input, None),
                PortContract::new(port_id("out"), PortDirection::Output, None),
            ],
            ExecutionMode::Wasm,
            Determinism::Unknown,
        )];
        let capabilities = vec![capabilities(
            "guest",
            vec![
                PortCapability::new(port_id("in"), PortCapabilityDirection::Receive),
                PortCapability::new(port_id("out"), PortCapabilityDirection::Emit),
            ],
            Vec::new(),
        )];

        let view = introspect_workflow(&workflow, &contracts, &capabilities)
            .expect("effect-free wasm boundary should validate");

        assert_eq!(view.nodes()[0].execution_mode(), ExecutionMode::Wasm);
        assert_eq!(view.nodes()[0].enforcement(), EnforcementLevel::Strict);
        assert_eq!(view.nodes()[0].determinism(), Determinism::Unknown);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn introspection_json_has_stable_ai_facing_shape() {
        let view = sample_linear_introspection();
        let json =
            workflow_introspection_to_json_string(&view).expect("introspection JSON should encode");

        insta::assert_snapshot!(json, @r###"
        {
          "edges": [
            {
              "capacity": {
                "kind": "explicit",
                "value": 8
              },
              "source": {
                "node_id": "source",
                "port_id": "out"
              },
              "source_schema": "schema://packet",
              "target": {
                "node_id": "sink",
                "port_id": "in"
              },
              "target_schema": "schema://packet"
            }
          ],
          "nodes": [
            {
              "determinism": "deterministic",
              "effects": [
                "clock"
              ],
              "enforcement": "advisory",
              "execution_mode": "native",
              "node_id": "source",
              "ports": [
                {
                  "capability": "emit",
                  "direction": "output",
                  "port_id": "out",
                  "schema": "schema://packet"
                }
              ],
              "retry": "safe"
            },
            {
              "determinism": "non_deterministic",
              "effects": [],
              "enforcement": "advisory",
              "execution_mode": "native",
              "node_id": "sink",
              "ports": [
                {
                  "capability": "receive",
                  "direction": "input",
                  "port_id": "in",
                  "schema": "schema://packet"
                }
              ],
              "retry": "safe"
            }
          ],
          "workflow_id": "flow"
        }
        "###);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn mixed_native_wasm_introspection_json_has_stable_shape() {
        let view = sample_mixed_native_wasm_introspection();
        let json =
            workflow_introspection_to_json_string(&view).expect("introspection JSON should encode");

        insta::assert_snapshot!(json, @r###"
        {
          "edges": [
            {
              "capacity": {
                "kind": "explicit",
                "value": 4
              },
              "source": {
                "node_id": "source",
                "port_id": "out"
              },
              "source_schema": "schema://packet",
              "target": {
                "node_id": "guest",
                "port_id": "in"
              },
              "target_schema": "schema://packet"
            }
          ],
          "nodes": [
            {
              "determinism": "deterministic",
              "effects": [
                "clock"
              ],
              "enforcement": "advisory",
              "execution_mode": "native",
              "node_id": "source",
              "ports": [
                {
                  "capability": "emit",
                  "direction": "output",
                  "port_id": "out",
                  "schema": "schema://packet"
                }
              ],
              "retry": "safe"
            },
            {
              "determinism": "unknown",
              "effects": [],
              "enforcement": "strict",
              "execution_mode": "wasm",
              "node_id": "guest",
              "ports": [
                {
                  "capability": "receive",
                  "direction": "input",
                  "port_id": "in",
                  "schema": "schema://packet"
                },
                {
                  "capability": "emit",
                  "direction": "output",
                  "port_id": "out",
                  "schema": "schema://uppercase-packet"
                }
              ],
              "retry": "safe"
            }
          ],
          "workflow_id": "mixed"
        }
        "###);
    }
}
