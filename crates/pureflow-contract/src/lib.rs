//! Node contract data and validation for Pureflow.
//!
//! ## Fragment: contract-data-boundary
//!
//! This crate keeps the contract layer thin: it owns typed contract metadata
//! and validation against validated workflow topology plus existing capability
//! descriptors. It does not add a second capability model, a parser, or a
//! runtime.
//!
//! ## Fragment: contract-validation-boundary
//!
//! Validation is intentionally staged. Workflow structure is still owned by
//! `pureflow-workflow`, capability descriptors remain in `pureflow-core`, and
//! this crate only verifies that the two line up with the contract metadata
//! supplied for inspection or execution planning.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use pureflow_core::{
    RetryDisposition,
    capability::{
        CapabilityValidationError, EffectCapability, NodeCapabilities,
        validate_workflow_capabilities,
    },
};
use pureflow_types::{NodeId, PortId};
use pureflow_workflow::{PortDirection, WorkflowDefinition};

/// Opaque schema reference attached to a contract port.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaRef(String);

impl SchemaRef {
    /// Create an opaque schema reference.
    ///
    /// # Errors
    ///
    /// Returns an error if the supplied reference is empty or whitespace-only.
    pub fn new(value: impl Into<String>) -> Result<Self, ContractValidationError> {
        let value: String = value.into();
        if value.trim().is_empty() {
            return Err(ContractValidationError::EmptySchemaRef);
        }

        Ok(Self(value))
    }

    /// Raw schema reference string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SchemaRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Declared execution mode for one node contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Host-native execution.
    Native,
    /// WASM component execution.
    Wasm,
    /// Future process-backed execution.
    Process,
}

/// Contract-level determinism declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Determinism {
    /// The node is deterministic for the same input and execution metadata.
    Deterministic,
    /// The node may vary across runs.
    NonDeterministic,
    /// The contract does not yet declare determinism.
    Unknown,
}

/// Contract-side declaration for one node port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortContract {
    port_id: PortId,
    direction: PortDirection,
    schema: Option<SchemaRef>,
}

impl PortContract {
    /// Create a port contract.
    #[must_use]
    pub const fn new(port_id: PortId, direction: PortDirection, schema: Option<SchemaRef>) -> Self {
        Self {
            port_id,
            direction,
            schema,
        }
    }

    /// Declared port identifier.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Declared port direction.
    #[must_use]
    pub const fn direction(&self) -> PortDirection {
        self.direction
    }

    /// Schema reference attached to this port, if any.
    #[must_use]
    pub const fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }
}

/// Validation error for contract metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractValidationError {
    /// A schema reference was empty or whitespace-only.
    EmptySchemaRef,
    /// A node contract duplicated the same port identifier.
    DuplicatePortContract {
        /// Node whose contract is invalid.
        node_id: NodeId,
        /// Duplicated port identifier.
        port_id: PortId,
    },
    /// A contract referenced a workflow node that does not exist.
    UnknownWorkflowNode {
        /// Missing node identifier.
        node_id: NodeId,
    },
    /// A contract referenced a port that does not exist on the workflow node.
    UnknownWorkflowPort {
        /// Node identifier.
        node_id: NodeId,
        /// Port identifier.
        port_id: PortId,
        /// Direction declared by the contract.
        direction: PortDirection,
    },
    /// A contract declared a port with a different direction than the workflow.
    PortDirectionMismatch {
        /// Node identifier.
        node_id: NodeId,
        /// Port identifier.
        port_id: PortId,
        /// Direction declared by the workflow.
        workflow: PortDirection,
        /// Direction declared by the contract.
        contract: PortDirection,
    },
    /// A contract referenced a node with no matching capability descriptor.
    MissingCapabilityDescriptor {
        /// Node identifier.
        node_id: NodeId,
    },
    /// A contract declared a schema mismatch on a workflow edge.
    SchemaMismatch {
        /// Zero-based edge index.
        edge_index: usize,
        /// Source node identifier.
        source_node_id: NodeId,
        /// Source port identifier.
        source_port_id: PortId,
        /// Target node identifier.
        target_node_id: NodeId,
        /// Target port identifier.
        target_port_id: PortId,
    },
    /// Capability validation failed.
    Capability {
        /// Underlying capability validation error.
        error: CapabilityValidationError,
    },
}

impl fmt::Display for ContractValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySchemaRef => write!(f, "schema reference cannot be empty"),
            Self::DuplicatePortContract { node_id, port_id } => {
                write!(
                    f,
                    "node `{node_id}` declares duplicate contract port `{port_id}`"
                )
            }
            Self::UnknownWorkflowNode { node_id } => {
                write!(f, "contract references unknown workflow node `{node_id}`")
            }
            Self::UnknownWorkflowPort {
                node_id,
                port_id,
                direction,
            } => write!(
                f,
                "node `{node_id}` contract references unknown {} workflow port `{port_id}`",
                port_direction_label(*direction)
            ),
            Self::PortDirectionMismatch {
                node_id,
                port_id,
                workflow,
                contract,
            } => write!(
                f,
                "node `{node_id}` contract port `{port_id}` is {} but workflow declares {}",
                port_direction_label(*contract),
                port_direction_label(*workflow)
            ),
            Self::MissingCapabilityDescriptor { node_id } => {
                write!(
                    f,
                    "no capability descriptor supplied for workflow node `{node_id}`"
                )
            }
            Self::SchemaMismatch {
                edge_index,
                source_node_id,
                source_port_id,
                target_node_id,
                target_port_id,
            } => write!(
                f,
                "edge {edge_index} from `{source_node_id}:{source_port_id}` to `{target_node_id}:{target_port_id}` has incompatible schemas"
            ),
            Self::Capability { error } => write!(f, "capability validation failed: {error}"),
        }
    }
}

impl Error for ContractValidationError {}

impl From<CapabilityValidationError> for ContractValidationError {
    fn from(error: CapabilityValidationError) -> Self {
        Self::Capability { error }
    }
}

/// Contract metadata for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeContract {
    id: NodeId,
    ports: Vec<PortContract>,
    execution_mode: ExecutionMode,
    determinism: Determinism,
    retry: RetryDisposition,
}

impl NodeContract {
    /// Create a contract for one node.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract repeats a port identifier.
    pub fn new(
        id: NodeId,
        ports: impl Into<Vec<PortContract>>,
        execution_mode: ExecutionMode,
        determinism: Determinism,
        retry: RetryDisposition,
    ) -> Result<Self, ContractValidationError> {
        let ports: Vec<PortContract> = ports.into();
        reject_duplicate_ports(&id, &ports)?;

        Ok(Self {
            id,
            ports,
            execution_mode,
            determinism,
            retry,
        })
    }

    /// Node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Declared ports.
    #[must_use]
    pub fn ports(&self) -> &[PortContract] {
        &self.ports
    }

    /// Declared execution mode.
    #[must_use]
    pub const fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Declared determinism.
    #[must_use]
    pub const fn determinism(&self) -> Determinism {
        self.determinism
    }

    /// Declared retry disposition.
    #[must_use]
    pub const fn retry(&self) -> RetryDisposition {
        self.retry
    }

    fn port_map(&self) -> BTreeMap<&PortId, &PortContract> {
        self.ports
            .iter()
            .map(|port: &PortContract| (port.port_id(), port))
            .collect()
    }
}

/// Validate workflow topology, capability descriptors, and node contracts together.
///
/// # Errors
///
/// Returns an error if contracts do not match the workflow topology, if a
/// capability descriptor is missing or inconsistent, or if schemas disagree on
/// a connected edge.
pub fn validate_workflow_contracts(
    workflow: &WorkflowDefinition,
    contracts: &[NodeContract],
    capabilities: &[NodeCapabilities],
) -> Result<(), ContractValidationError> {
    validate_workflow_capabilities(workflow, capabilities)?;

    let contract_map: BTreeMap<&NodeId, &NodeContract> = contracts
        .iter()
        .map(|contract: &NodeContract| (contract.id(), contract))
        .collect();
    let capability_map: BTreeMap<&NodeId, &NodeCapabilities> = capabilities
        .iter()
        .map(|capability: &NodeCapabilities| (capability.node_id(), capability))
        .collect();

    for node in workflow.nodes() {
        let contract: &NodeContract = contract_map.get(node.id()).copied().ok_or_else(|| {
            ContractValidationError::UnknownWorkflowNode {
                node_id: node.id().clone(),
            }
        })?;
        let capability: &NodeCapabilities =
            capability_map.get(node.id()).copied().ok_or_else(|| {
                ContractValidationError::MissingCapabilityDescriptor {
                    node_id: node.id().clone(),
                }
            })?;

        validate_node_contract(node, contract)?;
        validate_enforceable_capabilities(contract, capability)?;
    }

    for contract in contracts {
        if workflow
            .nodes()
            .iter()
            .all(|node: &pureflow_workflow::NodeDefinition| node.id() != contract.id())
        {
            return Err(ContractValidationError::UnknownWorkflowNode {
                node_id: contract.id().clone(),
            });
        }
    }

    validate_edge_schema_compatibility(workflow, &contract_map)
}

fn validate_enforceable_capabilities(
    contract: &NodeContract,
    capability: &NodeCapabilities,
) -> Result<(), ContractValidationError> {
    match contract.execution_mode() {
        ExecutionMode::Native => Ok(()),
        ExecutionMode::Wasm | ExecutionMode::Process => {
            for effect in capability.effects() {
                if !strict_boundary_supports_effect(contract.execution_mode(), *effect) {
                    return Err(CapabilityValidationError::UnenforceableEffectCapability {
                        node_id: capability.node_id().clone(),
                        effect: *effect,
                    }
                    .into());
                }
            }

            Ok(())
        }
    }
}

const fn strict_boundary_supports_effect(
    _execution_mode: ExecutionMode,
    _effect: EffectCapability,
) -> bool {
    // The current strict boundaries import no host effects. Future WASI/process
    // adapters should add explicit allowlist entries here as imports land.
    false
}

fn validate_node_contract(
    node: &pureflow_workflow::NodeDefinition,
    contract: &NodeContract,
) -> Result<(), ContractValidationError> {
    let workflow_ports: BTreeMap<&PortId, PortDirection> = node
        .input_ports()
        .iter()
        .map(|port_id: &PortId| (port_id, PortDirection::Input))
        .chain(
            node.output_ports()
                .iter()
                .map(|port_id: &PortId| (port_id, PortDirection::Output)),
        )
        .collect();
    let contract_ports: BTreeMap<&PortId, &PortContract> = contract.port_map();

    for (port_id, workflow_direction) in &workflow_ports {
        let contract_port: &&PortContract = contract_ports.get(port_id).ok_or_else(|| {
            ContractValidationError::UnknownWorkflowPort {
                node_id: node.id().clone(),
                port_id: (*port_id).clone(),
                direction: *workflow_direction,
            }
        })?;

        if contract_port.direction() != *workflow_direction {
            return Err(ContractValidationError::PortDirectionMismatch {
                node_id: node.id().clone(),
                port_id: (*port_id).clone(),
                workflow: *workflow_direction,
                contract: contract_port.direction(),
            });
        }
    }

    for (port_id, contract_port) in &contract_ports {
        if !workflow_ports.contains_key(port_id) {
            return Err(ContractValidationError::UnknownWorkflowPort {
                node_id: node.id().clone(),
                port_id: (*port_id).clone(),
                direction: contract_port.direction(),
            });
        }
    }

    Ok(())
}

fn validate_edge_schema_compatibility(
    workflow: &WorkflowDefinition,
    contracts: &BTreeMap<&NodeId, &NodeContract>,
) -> Result<(), ContractValidationError> {
    // Schema compatibility is exact equality while SchemaRef remains opaque.
    for (edge_index, edge) in workflow.edges().iter().enumerate() {
        let source_contract: &&NodeContract =
            contracts.get(edge.source().node_id()).ok_or_else(|| {
                ContractValidationError::UnknownWorkflowNode {
                    node_id: edge.source().node_id().clone(),
                }
            })?;
        let target_contract: &&NodeContract =
            contracts.get(edge.target().node_id()).ok_or_else(|| {
                ContractValidationError::UnknownWorkflowNode {
                    node_id: edge.target().node_id().clone(),
                }
            })?;

        let source_schema: Option<&SchemaRef> = source_contract
            .ports()
            .iter()
            .find(|port: &&PortContract| port.port_id() == edge.source().port_id())
            .and_then(PortContract::schema);
        let target_schema: Option<&SchemaRef> = target_contract
            .ports()
            .iter()
            .find(|port: &&PortContract| port.port_id() == edge.target().port_id())
            .and_then(PortContract::schema);

        if let (Some(source_schema), Some(target_schema)) = (source_schema, target_schema)
            && source_schema != target_schema
        {
            return Err(ContractValidationError::SchemaMismatch {
                edge_index,
                source_node_id: edge.source().node_id().clone(),
                source_port_id: edge.source().port_id().clone(),
                target_node_id: edge.target().node_id().clone(),
                target_port_id: edge.target().port_id().clone(),
            });
        }
    }

    Ok(())
}

fn reject_duplicate_ports(
    node_id: &NodeId,
    ports: &[PortContract],
) -> Result<(), ContractValidationError> {
    let mut seen: BTreeMap<&PortId, PortDirection> = BTreeMap::new();

    for port in ports {
        if seen.insert(port.port_id(), port.direction()).is_some() {
            return Err(ContractValidationError::DuplicatePortContract {
                node_id: node_id.clone(),
                port_id: port.port_id().clone(),
            });
        }
    }

    Ok(())
}

const fn port_direction_label(direction: PortDirection) -> &'static str {
    match direction {
        PortDirection::Input => "input",
        PortDirection::Output => "output",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use pureflow_core::{
        RetryDisposition,
        capability::{EffectCapability, NodeCapabilities, PortCapability, PortCapabilityDirection},
    };
    use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, node_id, port_id, workflow_id};
    use pureflow_workflow::{EdgeDefinition, EdgeEndpoint, NodeDefinition};
    use quickcheck::{Arbitrary, Gen, QuickCheck};

    fn schema(value: &str) -> SchemaRef {
        SchemaRef::new(value).expect("valid schema ref")
    }

    fn contract(
        node: &str,
        ports: Vec<PortContract>,
        execution_mode: ExecutionMode,
    ) -> NodeContract {
        NodeContract::new(
            node_id(node),
            ports,
            execution_mode,
            Determinism::Deterministic,
            RetryDisposition::Safe,
        )
        .expect("valid contract")
    }

    fn capabilities(node: &str, ports: Vec<PortCapability>) -> NodeCapabilities {
        NodeCapabilities::new(node_id(node), ports, [EffectCapability::Clock])
            .expect("valid capability")
    }

    fn passive_capabilities(node: &str, ports: Vec<PortCapability>) -> NodeCapabilities {
        NodeCapabilities::native_passive(node_id(node), ports).expect("valid capability")
    }

    #[derive(Debug, Clone)]
    struct NonEmptySchemaString(String);

    impl Arbitrary for NonEmptySchemaString {
        fn arbitrary(g: &mut Gen) -> Self {
            let value = String::arbitrary(g);
            if value.trim().is_empty() {
                Self("schema://generated".to_string())
            } else {
                Self(value)
            }
        }
    }

    #[derive(Debug, Clone)]
    struct MatchingWorkflowContractCase {
        workflow: WorkflowDefinition,
        contracts: Vec<NodeContract>,
        capabilities: Vec<NodeCapabilities>,
    }

    impl Arbitrary for MatchingWorkflowContractCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let node_count = usize::arbitrary(g) % 5 + 1;
            let mut inputs_by_node: Vec<BTreeSet<PortId>> = vec![BTreeSet::new(); node_count];
            let mut outputs_by_node: Vec<BTreeSet<PortId>> = vec![BTreeSet::new(); node_count];
            let mut edges = Vec::new();

            for source in 0..node_count {
                for target in (source + 1)..node_count {
                    if bool::arbitrary(g) {
                        let source_node = generated_node_id(source);
                        let target_node = generated_node_id(target);
                        let source_port = generated_output_port(target);
                        let target_port = generated_input_port(source);

                        outputs_by_node[source].insert(source_port.clone());
                        inputs_by_node[target].insert(target_port.clone());
                        edges.push(EdgeDefinition::new(
                            EdgeEndpoint::new(source_node, source_port),
                            EdgeEndpoint::new(target_node, target_port),
                        ));
                    }
                }
            }

            let nodes: Vec<NodeDefinition> = (0..node_count)
                .map(|index| {
                    NodeDefinition::new(
                        generated_node_id(index),
                        inputs_by_node[index].iter().cloned().collect::<Vec<_>>(),
                        outputs_by_node[index].iter().cloned().collect::<Vec<_>>(),
                    )
                    .expect("generated node topology is valid")
                })
                .collect();
            let workflow =
                WorkflowDefinition::from_parts(workflow_id("generated_flow"), nodes, edges)
                    .expect("generated workflow is acyclic and valid");
            let mut contracts = Vec::new();
            let mut capabilities = Vec::new();

            for node in workflow.nodes() {
                let mut contract_ports = Vec::new();
                let mut capability_ports = Vec::new();

                for input in node.input_ports() {
                    contract_ports.push(PortContract::new(
                        input.clone(),
                        PortDirection::Input,
                        Some(schema("schema://generated-packet")),
                    ));
                    capability_ports.push(PortCapability::new(
                        input.clone(),
                        PortCapabilityDirection::Receive,
                    ));
                }

                for output in node.output_ports() {
                    contract_ports.push(PortContract::new(
                        output.clone(),
                        PortDirection::Output,
                        Some(schema("schema://generated-packet")),
                    ));
                    capability_ports.push(PortCapability::new(
                        output.clone(),
                        PortCapabilityDirection::Emit,
                    ));
                }

                contracts.push(
                    NodeContract::new(
                        node.id().clone(),
                        contract_ports,
                        ExecutionMode::Native,
                        Determinism::Deterministic,
                        RetryDisposition::Safe,
                    )
                    .expect("generated contract matches workflow ports"),
                );
                capabilities.push(
                    NodeCapabilities::native_passive(node.id().clone(), capability_ports)
                        .expect("generated capabilities match workflow ports"),
                );
            }

            Self {
                workflow,
                contracts,
                capabilities,
            }
        }
    }

    fn generated_node_id(index: usize) -> NodeId {
        node_id(&format!("node_{index}"))
    }

    fn generated_input_port(source_index: usize) -> PortId {
        port_id(&format!("in_from_{source_index}"))
    }

    fn generated_output_port(target_index: usize) -> PortId {
        port_id(&format!("out_to_{target_index}"))
    }

    #[test]
    fn generated_non_empty_schema_refs_round_trip() {
        fn property(input: NonEmptySchemaString) -> bool {
            let schema = SchemaRef::new(input.0.clone()).expect("generated schema ref is valid");
            schema.as_str() == input.0
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(NonEmptySchemaString) -> bool);
    }

    #[test]
    fn generated_matching_workflow_contracts_validate() {
        fn property(case: MatchingWorkflowContractCase) -> bool {
            validate_workflow_contracts(&case.workflow, &case.contracts, &case.capabilities).is_ok()
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(MatchingWorkflowContractCase) -> bool);
    }

    #[test]
    fn node_contract_rejects_duplicate_ports() {
        let err = NodeContract::new(
            node_id("worker"),
            [
                PortContract::new(port_id("in"), PortDirection::Input, None),
                PortContract::new(port_id("in"), PortDirection::Output, None),
            ],
            ExecutionMode::Native,
            Determinism::Deterministic,
            RetryDisposition::Safe,
        )
        .expect_err("duplicate ports must fail");

        assert_eq!(
            err,
            ContractValidationError::DuplicatePortContract {
                node_id: node_id("worker"),
                port_id: port_id("in")
            }
        );
    }

    #[test]
    fn schema_ref_rejects_empty_values() {
        let err = SchemaRef::new("   ").expect_err("empty schema ref must fail");

        assert_eq!(err, ContractValidationError::EmptySchemaRef);
    }

    #[test]
    fn validate_workflow_contracts_accepts_matching_contracts() {
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
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
            ),
            contract(
                "sink",
                vec![PortContract::new(
                    port_id("in"),
                    PortDirection::Input,
                    Some(schema("schema://packet")),
                )],
                ExecutionMode::Native,
            ),
        ];
        let capabilities = vec![
            capabilities(
                "source",
                vec![PortCapability::new(
                    port_id("out"),
                    PortCapabilityDirection::Emit,
                )],
            ),
            capabilities(
                "sink",
                vec![PortCapability::new(
                    port_id("in"),
                    PortCapabilityDirection::Receive,
                )],
            ),
        ];

        validate_workflow_contracts(&workflow, &contracts, &capabilities)
            .expect("matching contracts should validate");
    }

    #[test]
    fn validate_workflow_contracts_rejects_schema_mismatch() {
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
                    Some(schema("schema://packet-a")),
                )],
                ExecutionMode::Native,
            ),
            contract(
                "sink",
                vec![PortContract::new(
                    port_id("in"),
                    PortDirection::Input,
                    Some(schema("schema://packet-b")),
                )],
                ExecutionMode::Native,
            ),
        ];
        let capabilities = vec![
            capabilities(
                "source",
                vec![PortCapability::new(
                    port_id("out"),
                    PortCapabilityDirection::Emit,
                )],
            ),
            capabilities(
                "sink",
                vec![PortCapability::new(
                    port_id("in"),
                    PortCapabilityDirection::Receive,
                )],
            ),
        ];

        let err = validate_workflow_contracts(&workflow, &contracts, &capabilities)
            .expect_err("schema mismatch must fail");

        assert!(matches!(
            err,
            ContractValidationError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn validate_workflow_contracts_rejects_missing_capability_descriptor() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let contracts = vec![contract(
            "source",
            vec![PortContract::new(
                port_id("out"),
                PortDirection::Output,
                Some(schema("schema://packet")),
            )],
            ExecutionMode::Native,
        )];

        let err = validate_workflow_contracts(&workflow, &contracts, &[])
            .expect_err("missing capability descriptor must fail");

        assert_eq!(
            err,
            ContractValidationError::MissingCapabilityDescriptor {
                node_id: node_id("source")
            }
        );
    }

    #[test]
    fn validate_workflow_contracts_accepts_native_advisory_effects() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .build();
        let contracts = vec![contract(
            "source",
            vec![PortContract::new(
                port_id("out"),
                PortDirection::Output,
                Some(schema("schema://packet")),
            )],
            ExecutionMode::Native,
        )];
        let capabilities = vec![capabilities(
            "source",
            vec![PortCapability::new(
                port_id("out"),
                PortCapabilityDirection::Emit,
            )],
        )];

        validate_workflow_contracts(&workflow, &contracts, &capabilities)
            .expect("native effects are advisory");
    }

    #[test]
    fn validate_workflow_contracts_accepts_wasm_without_host_effects() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("wasm").input("in").output("out").build())
            .build();
        let contracts = vec![contract(
            "wasm",
            vec![
                PortContract::new(
                    port_id("in"),
                    PortDirection::Input,
                    Some(schema("schema://p")),
                ),
                PortContract::new(
                    port_id("out"),
                    PortDirection::Output,
                    Some(schema("schema://p")),
                ),
            ],
            ExecutionMode::Wasm,
        )];
        let capabilities = vec![passive_capabilities(
            "wasm",
            vec![
                PortCapability::new(port_id("in"), PortCapabilityDirection::Receive),
                PortCapability::new(port_id("out"), PortCapabilityDirection::Emit),
            ],
        )];

        validate_workflow_contracts(&workflow, &contracts, &capabilities)
            .expect("import-free WASM contract should validate");
    }

    #[test]
    fn validate_workflow_contracts_rejects_wasm_effects_without_imports() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("wasm").input("in").build())
            .build();
        let contracts = vec![contract(
            "wasm",
            vec![PortContract::new(
                port_id("in"),
                PortDirection::Input,
                Some(schema("schema://packet")),
            )],
            ExecutionMode::Wasm,
        )];
        let capabilities = vec![capabilities(
            "wasm",
            vec![PortCapability::new(
                port_id("in"),
                PortCapabilityDirection::Receive,
            )],
        )];

        let err = validate_workflow_contracts(&workflow, &contracts, &capabilities)
            .expect_err("WASM effect without host import must fail");

        assert_eq!(
            err,
            ContractValidationError::Capability {
                error: CapabilityValidationError::UnenforceableEffectCapability {
                    node_id: node_id("wasm"),
                    effect: EffectCapability::Clock,
                }
            }
        );
    }

    #[test]
    fn validate_workflow_contracts_rejects_process_effects_without_adapter() {
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("worker").input("in").build())
            .build();
        let contracts = vec![contract(
            "worker",
            vec![PortContract::new(
                port_id("in"),
                PortDirection::Input,
                Some(schema("schema://packet")),
            )],
            ExecutionMode::Process,
        )];
        let capabilities = vec![capabilities(
            "worker",
            vec![PortCapability::new(
                port_id("in"),
                PortCapabilityDirection::Receive,
            )],
        )];

        let err = validate_workflow_contracts(&workflow, &contracts, &capabilities)
            .expect_err("process effect without adapter must fail");

        assert!(matches!(
            err,
            ContractValidationError::Capability {
                error: CapabilityValidationError::UnenforceableEffectCapability { .. }
            }
        ));
    }
}
