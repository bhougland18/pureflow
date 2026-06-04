//! Versioned raw workflow definitions for external workflow formats.
//!
//! This crate intentionally owns the boundary between parser-specific raw data
//! and validated in-memory workflow types. Parser crates and features can build
//! these raw structs without adding JSON, TOML, or YAML dependencies to
//! `pureflow-workflow`.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use pureflow_types::{IdentifierError, NodeId, PortId, WorkflowId};
use pureflow_workflow::{
    EdgeDefinition, EdgeEndpoint, NodeDefinition, WorkflowDefinition, WorkflowValidationError,
};
use serde::{Deserialize, Serialize};

/// Current supported external workflow format version.
pub const CURRENT_PUREFLOW_VERSION: &str = "1";

/// Raw workflow document after parser-level decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawWorkflowDefinition {
    /// Required Pureflow workflow format version.
    pub pureflow_version: Option<String>,
    /// Workflow identifier.
    pub id: String,
    /// Declared node topology.
    pub nodes: Vec<RawNodeDefinition>,
    /// Declared directed edges.
    pub edges: Vec<RawEdgeDefinition>,
}

impl RawWorkflowDefinition {
    /// Convert this raw workflow into a validated domain workflow.
    ///
    /// # Errors
    ///
    /// Returns a typed format error for missing or unsupported versions,
    /// malformed identifiers, invalid explicit capacities, or workflow graph
    /// validation failures.
    pub fn to_workflow(&self) -> Result<WorkflowDefinition, WorkflowFormatError> {
        validate_version(self.pureflow_version.as_deref())?;

        let workflow_id: WorkflowId =
            WorkflowId::new(self.id.clone()).map_err(|source: IdentifierError| {
                WorkflowFormatError::Identifier {
                    context: IdentifierContext::Workflow,
                    source,
                }
            })?;

        let nodes: Vec<NodeDefinition> = self
            .nodes
            .iter()
            .enumerate()
            .map(raw_node_to_definition)
            .collect::<Result<Vec<NodeDefinition>, WorkflowFormatError>>()?;

        let edges: Vec<EdgeDefinition> = self
            .edges
            .iter()
            .enumerate()
            .map(raw_edge_to_definition)
            .collect::<Result<Vec<EdgeDefinition>, WorkflowFormatError>>()?;

        WorkflowDefinition::from_parts(workflow_id, nodes, edges)
            .map_err(|source: WorkflowValidationError| WorkflowFormatError::Workflow { source })
    }
}

/// Raw node declaration after parser-level decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawNodeDefinition {
    /// Node identifier.
    pub id: String,
    /// Declared input port identifiers.
    pub inputs: Vec<String>,
    /// Declared output port identifiers.
    pub outputs: Vec<String>,
}

/// Raw endpoint declaration after parser-level decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawEdgeEndpoint {
    /// Referenced node identifier.
    pub node: String,
    /// Referenced port identifier.
    pub port: String,
}

/// Raw edge declaration after parser-level decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawEdgeDefinition {
    /// Upstream output endpoint.
    pub source: RawEdgeEndpoint,
    /// Downstream input endpoint.
    pub target: RawEdgeEndpoint,
    /// Optional explicit bounded capacity. `None` uses the engine default.
    pub capacity: Option<usize>,
}

/// Identifier location in a raw workflow document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifierContext {
    /// Top-level workflow identifier.
    Workflow,
    /// Node identifier at the given node index.
    Node {
        /// Zero-based node index.
        node_index: usize,
    },
    /// Input port identifier at the given node and port index.
    InputPort {
        /// Zero-based node index.
        node_index: usize,
        /// Zero-based input port index.
        port_index: usize,
    },
    /// Output port identifier at the given node and port index.
    OutputPort {
        /// Zero-based node index.
        node_index: usize,
        /// Zero-based output port index.
        port_index: usize,
    },
    /// Source node identifier for an edge.
    EdgeSourceNode {
        /// Zero-based edge index.
        edge_index: usize,
    },
    /// Source port identifier for an edge.
    EdgeSourcePort {
        /// Zero-based edge index.
        edge_index: usize,
    },
    /// Target node identifier for an edge.
    EdgeTargetNode {
        /// Zero-based edge index.
        edge_index: usize,
    },
    /// Target port identifier for an edge.
    EdgeTargetPort {
        /// Zero-based edge index.
        edge_index: usize,
    },
}

impl fmt::Display for IdentifierContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Workflow => f.write_str("workflow id"),
            Self::Node { node_index } => write!(f, "node {node_index} id"),
            Self::InputPort {
                node_index,
                port_index,
            } => write!(f, "node {node_index} input port {port_index}"),
            Self::OutputPort {
                node_index,
                port_index,
            } => write!(f, "node {node_index} output port {port_index}"),
            Self::EdgeSourceNode { edge_index } => write!(f, "edge {edge_index} source node"),
            Self::EdgeSourcePort { edge_index } => write!(f, "edge {edge_index} source port"),
            Self::EdgeTargetNode { edge_index } => write!(f, "edge {edge_index} target node"),
            Self::EdgeTargetPort { edge_index } => write!(f, "edge {edge_index} target port"),
        }
    }
}

/// Error returned while converting raw workflow format data into domain types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowFormatError {
    /// The document omitted its format version.
    MissingVersion,
    /// The document used a version this crate does not support.
    UnsupportedVersion {
        /// Unsupported version value from the raw document.
        version: String,
    },
    /// A raw identifier failed domain identifier validation.
    Identifier {
        /// Where the identifier appeared in the raw document.
        context: IdentifierContext,
        /// Identifier validation failure.
        source: IdentifierError,
    },
    /// An explicit edge capacity was present but invalid.
    InvalidEdgeCapacity {
        /// Zero-based edge index.
        edge_index: usize,
        /// Invalid raw capacity value.
        capacity: usize,
    },
    /// The converted graph failed workflow validation.
    Workflow {
        /// Workflow validation failure.
        source: WorkflowValidationError,
    },
}

/// Error returned while decoding or encoding canonical JSON workflow documents.
#[derive(Debug)]
pub enum WorkflowJsonError {
    /// JSON decoding failed before raw workflow validation.
    Decode {
        /// JSON parser failure.
        source: serde_json::Error,
    },
    /// Raw workflow data failed format or domain validation.
    Format {
        /// Format validation failure.
        source: WorkflowFormatError,
    },
    /// JSON encoding failed.
    Encode {
        /// JSON serialization failure.
        source: serde_json::Error,
    },
}

/// Error returned while decoding human-authored TOML workflow documents.
#[cfg(feature = "toml")]
#[derive(Debug)]
pub enum WorkflowTomlError {
    /// TOML decoding failed before raw workflow validation.
    Decode {
        /// TOML parser failure.
        source: toml::de::Error,
    },
    /// Raw workflow data failed format or domain validation.
    Format {
        /// Format validation failure.
        source: WorkflowFormatError,
    },
}

/// Error returned while decoding optional YAML workflow documents.
#[cfg(feature = "yaml")]
#[derive(Debug)]
pub enum WorkflowYamlError {
    /// YAML decoding failed before raw workflow validation.
    Decode {
        /// YAML parser failure.
        source: serde_yml::Error,
    },
    /// Raw workflow data failed format or domain validation.
    Format {
        /// Format validation failure.
        source: WorkflowFormatError,
    },
}

impl fmt::Display for WorkflowJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { source } => write!(f, "failed to decode workflow JSON: {source}"),
            Self::Format { source } => write!(f, "workflow JSON is invalid: {source}"),
            Self::Encode { source } => write!(f, "failed to encode workflow JSON: {source}"),
        }
    }
}

#[cfg(feature = "toml")]
impl fmt::Display for WorkflowTomlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { source } => write!(f, "failed to decode workflow TOML: {source}"),
            Self::Format { source } => write!(f, "workflow TOML is invalid: {source}"),
        }
    }
}

#[cfg(feature = "yaml")]
impl fmt::Display for WorkflowYamlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { source } => write!(f, "failed to decode workflow YAML: {source}"),
            Self::Format { source } => write!(f, "workflow YAML is invalid: {source}"),
        }
    }
}

impl Error for WorkflowJsonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } | Self::Encode { source } => Some(source),
            Self::Format { source } => Some(source),
        }
    }
}

#[cfg(feature = "toml")]
impl Error for WorkflowTomlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } => Some(source),
            Self::Format { source } => Some(source),
        }
    }
}

#[cfg(feature = "yaml")]
impl Error for WorkflowYamlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } => Some(source),
            Self::Format { source } => Some(source),
        }
    }
}

/// Decode a canonical JSON workflow document into raw workflow data.
///
/// # Errors
///
/// Returns a JSON decode error if the document does not match the canonical raw
/// workflow shape.
pub fn raw_workflow_from_json_str(input: &str) -> Result<RawWorkflowDefinition, WorkflowJsonError> {
    serde_json::from_str(input)
        .map_err(|source: serde_json::Error| WorkflowJsonError::Decode { source })
}

/// Decode a canonical JSON workflow document into a validated workflow.
///
/// # Errors
///
/// Returns JSON decode errors or typed workflow format/domain validation errors.
pub fn workflow_from_json_str(input: &str) -> Result<WorkflowDefinition, WorkflowJsonError> {
    let raw: RawWorkflowDefinition = raw_workflow_from_json_str(input)?;
    raw.to_workflow()
        .map_err(|source: WorkflowFormatError| WorkflowJsonError::Format { source })
}

/// Decode a human-authored TOML workflow document into raw workflow data.
///
/// # Errors
///
/// Returns a TOML decode error if the document does not match the canonical raw
/// workflow shape.
#[cfg(feature = "toml")]
pub fn raw_workflow_from_toml_str(input: &str) -> Result<RawWorkflowDefinition, WorkflowTomlError> {
    toml::from_str(input).map_err(|source: toml::de::Error| WorkflowTomlError::Decode { source })
}

/// Decode a human-authored TOML workflow document into a validated workflow.
///
/// # Errors
///
/// Returns TOML decode errors or typed workflow format/domain validation errors.
#[cfg(feature = "toml")]
pub fn workflow_from_toml_str(input: &str) -> Result<WorkflowDefinition, WorkflowTomlError> {
    let raw: RawWorkflowDefinition = raw_workflow_from_toml_str(input)?;
    raw.to_workflow()
        .map_err(|source: WorkflowFormatError| WorkflowTomlError::Format { source })
}

/// Decode an optional YAML workflow document into raw workflow data.
///
/// # Errors
///
/// Returns a YAML decode error if the document does not match the canonical raw
/// workflow shape.
#[cfg(feature = "yaml")]
pub fn raw_workflow_from_yaml_str(input: &str) -> Result<RawWorkflowDefinition, WorkflowYamlError> {
    serde_yml::from_str(input)
        .map_err(|source: serde_yml::Error| WorkflowYamlError::Decode { source })
}

/// Decode an optional YAML workflow document into a validated workflow.
///
/// # Errors
///
/// Returns YAML decode errors or typed workflow format/domain validation errors.
#[cfg(feature = "yaml")]
pub fn workflow_from_yaml_str(input: &str) -> Result<WorkflowDefinition, WorkflowYamlError> {
    let raw: RawWorkflowDefinition = raw_workflow_from_yaml_str(input)?;
    raw.to_workflow()
        .map_err(|source: WorkflowFormatError| WorkflowYamlError::Format { source })
}

/// Encode raw workflow data as canonical pretty JSON.
///
/// # Errors
///
/// Returns a JSON encode error if serialization fails.
pub fn raw_workflow_to_json_string(
    raw: &RawWorkflowDefinition,
) -> Result<String, WorkflowJsonError> {
    serde_json::to_string_pretty(raw)
        .map_err(|source: serde_json::Error| WorkflowJsonError::Encode { source })
}

impl fmt::Display for WorkflowFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingVersion => f.write_str("workflow document is missing pureflow_version"),
            Self::UnsupportedVersion { version } => {
                write!(
                    f,
                    "workflow document uses unsupported pureflow_version `{version}`"
                )
            }
            Self::Identifier { context, source } => {
                write!(f, "{context} is invalid: {source}")
            }
            Self::InvalidEdgeCapacity {
                edge_index,
                capacity,
            } => write!(
                f,
                "edge {edge_index} explicit capacity must be greater than zero, got {capacity}"
            ),
            Self::Workflow { source } => write!(f, "workflow validation failed: {source}"),
        }
    }
}

impl Error for WorkflowFormatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identifier { source, .. } => Some(source),
            Self::Workflow { source } => Some(source),
            Self::MissingVersion
            | Self::UnsupportedVersion { .. }
            | Self::InvalidEdgeCapacity { .. } => None,
        }
    }
}

fn validate_version(version: Option<&str>) -> Result<(), WorkflowFormatError> {
    match version {
        Some(CURRENT_PUREFLOW_VERSION) => Ok(()),
        Some(version) => Err(WorkflowFormatError::UnsupportedVersion {
            version: version.to_owned(),
        }),
        None => Err(WorkflowFormatError::MissingVersion),
    }
}

fn raw_node_to_definition(
    (node_index, raw): (usize, &RawNodeDefinition),
) -> Result<NodeDefinition, WorkflowFormatError> {
    let node_id: NodeId = NodeId::new(raw.id.clone()).map_err(|source: IdentifierError| {
        WorkflowFormatError::Identifier {
            context: IdentifierContext::Node { node_index },
            source,
        }
    })?;

    let inputs: Vec<PortId> = raw
        .inputs
        .iter()
        .enumerate()
        .map(|(port_index, port): (usize, &String)| {
            PortId::new(port.clone()).map_err(|source: IdentifierError| {
                WorkflowFormatError::Identifier {
                    context: IdentifierContext::InputPort {
                        node_index,
                        port_index,
                    },
                    source,
                }
            })
        })
        .collect::<Result<Vec<PortId>, WorkflowFormatError>>()?;

    let outputs: Vec<PortId> = raw
        .outputs
        .iter()
        .enumerate()
        .map(|(port_index, port): (usize, &String)| {
            PortId::new(port.clone()).map_err(|source: IdentifierError| {
                WorkflowFormatError::Identifier {
                    context: IdentifierContext::OutputPort {
                        node_index,
                        port_index,
                    },
                    source,
                }
            })
        })
        .collect::<Result<Vec<PortId>, WorkflowFormatError>>()?;

    NodeDefinition::new(node_id, inputs, outputs)
        .map_err(|source: WorkflowValidationError| WorkflowFormatError::Workflow { source })
}

fn raw_edge_to_definition(
    (edge_index, raw): (usize, &RawEdgeDefinition),
) -> Result<EdgeDefinition, WorkflowFormatError> {
    let source: EdgeEndpoint = raw_endpoint_to_definition(
        &raw.source,
        IdentifierContext::EdgeSourceNode { edge_index },
        IdentifierContext::EdgeSourcePort { edge_index },
    )?;
    let target: EdgeEndpoint = raw_endpoint_to_definition(
        &raw.target,
        IdentifierContext::EdgeTargetNode { edge_index },
        IdentifierContext::EdgeTargetPort { edge_index },
    )?;

    let capacity: usize = match raw.capacity {
        Some(capacity) => capacity,
        None => return Ok(EdgeDefinition::new(source, target)),
    };

    let capacity: NonZeroUsize =
        NonZeroUsize::new(capacity).ok_or(WorkflowFormatError::InvalidEdgeCapacity {
            edge_index,
            capacity,
        })?;
    Ok(EdgeDefinition::with_capacity(source, target, capacity))
}

fn raw_endpoint_to_definition(
    raw: &RawEdgeEndpoint,
    node_context: IdentifierContext,
    port_context: IdentifierContext,
) -> Result<EdgeEndpoint, WorkflowFormatError> {
    let node_id: NodeId = NodeId::new(raw.node.clone()).map_err(|source: IdentifierError| {
        WorkflowFormatError::Identifier {
            context: node_context,
            source,
        }
    })?;
    let port_id: PortId = PortId::new(raw.port.clone()).map_err(|source: IdentifierError| {
        WorkflowFormatError::Identifier {
            context: port_context,
            source,
        }
    })?;

    Ok(EdgeEndpoint::new(node_id, port_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_types::IdentifierKind;
    use pureflow_workflow::{EdgeCapacity, EdgeEndpointRole, PortDirection};
    use quickcheck::{Arbitrary, Gen, QuickCheck};
    use std::collections::BTreeSet;

    fn valid_raw_workflow() -> RawWorkflowDefinition {
        RawWorkflowDefinition {
            pureflow_version: Some(CURRENT_PUREFLOW_VERSION.to_owned()),
            id: "flow".to_owned(),
            nodes: vec![
                RawNodeDefinition {
                    id: "source".to_owned(),
                    inputs: Vec::new(),
                    outputs: vec!["out".to_owned()],
                },
                RawNodeDefinition {
                    id: "sink".to_owned(),
                    inputs: vec!["in".to_owned()],
                    outputs: Vec::new(),
                },
            ],
            edges: vec![RawEdgeDefinition {
                source: RawEdgeEndpoint {
                    node: "source".to_owned(),
                    port: "out".to_owned(),
                },
                target: RawEdgeEndpoint {
                    node: "sink".to_owned(),
                    port: "in".to_owned(),
                },
                capacity: Some(4),
            }],
        }
    }

    #[derive(Debug, Clone)]
    struct ArbitraryWorkflow(WorkflowDefinition);

    impl Arbitrary for ArbitraryWorkflow {
        fn arbitrary(g: &mut Gen) -> Self {
            let node_count: usize = usize::from(u8::arbitrary(g) % 5) + 1;
            let candidate_edge_count: usize =
                node_count.saturating_mul(node_count.saturating_sub(1)) / 2;
            let mut selected_edges: Vec<(usize, usize, EdgeCapacity)> = Vec::new();

            for source in 0..node_count {
                for target in source.saturating_add(1)..node_count {
                    if bool::arbitrary(g) {
                        selected_edges.push((source, target, arbitrary_capacity(g)));
                    }
                }
            }

            if selected_edges.is_empty() && candidate_edge_count > 0 {
                selected_edges.push((0, 1, arbitrary_capacity(g)));
            }

            let mut input_ports: Vec<BTreeSet<String>> = vec![BTreeSet::new(); node_count];
            let mut output_ports: Vec<BTreeSet<String>> = vec![BTreeSet::new(); node_count];
            for (source, target, _capacity) in &selected_edges {
                output_ports[*source].insert(format!("out_{target}"));
                input_ports[*target].insert(format!("in_{source}"));
            }

            let nodes: Vec<NodeDefinition> = (0..node_count)
                .map(|index: usize| {
                    let inputs: Vec<PortId> = input_ports[index]
                        .iter()
                        .map(|port: &String| PortId::new(port.clone()).expect("generated input id"))
                        .collect();
                    let outputs: Vec<PortId> = output_ports[index]
                        .iter()
                        .map(|port: &String| {
                            PortId::new(port.clone()).expect("generated output id")
                        })
                        .collect();
                    NodeDefinition::new(
                        NodeId::new(format!("node_{index}")).expect("generated node id"),
                        inputs,
                        outputs,
                    )
                    .expect("generated node should validate")
                })
                .collect();
            let edges: Vec<EdgeDefinition> = selected_edges
                .into_iter()
                .map(|(source, target, capacity): (usize, usize, EdgeCapacity)| {
                    let edge = EdgeDefinition::new(
                        EdgeEndpoint::new(
                            NodeId::new(format!("node_{source}")).expect("generated source id"),
                            PortId::new(format!("out_{target}")).expect("generated source port id"),
                        ),
                        EdgeEndpoint::new(
                            NodeId::new(format!("node_{target}")).expect("generated target id"),
                            PortId::new(format!("in_{source}")).expect("generated target port id"),
                        ),
                    );
                    match capacity {
                        EdgeCapacity::Default => edge,
                        EdgeCapacity::Explicit(capacity) => EdgeDefinition::with_capacity(
                            edge.source().clone(),
                            edge.target().clone(),
                            capacity,
                        ),
                    }
                })
                .collect();
            let workflow = WorkflowDefinition::from_parts(
                WorkflowId::new("generated_flow").expect("generated workflow id"),
                nodes,
                edges,
            )
            .expect("generated workflow should validate");

            Self(workflow)
        }
    }

    fn arbitrary_capacity(g: &mut Gen) -> EdgeCapacity {
        if bool::arbitrary(g) {
            EdgeCapacity::Default
        } else {
            let value: usize = usize::from(u8::arbitrary(g) % 16) + 1;
            EdgeCapacity::Explicit(NonZeroUsize::new(value).expect("generated nonzero capacity"))
        }
    }

    fn raw_from_workflow(workflow: &WorkflowDefinition) -> RawWorkflowDefinition {
        RawWorkflowDefinition {
            pureflow_version: Some(CURRENT_PUREFLOW_VERSION.to_owned()),
            id: workflow.id().to_string(),
            nodes: workflow
                .nodes()
                .iter()
                .map(|node: &NodeDefinition| RawNodeDefinition {
                    id: node.id().to_string(),
                    inputs: node.input_ports().iter().map(ToString::to_string).collect(),
                    outputs: node
                        .output_ports()
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                })
                .collect(),
            edges: workflow
                .edges()
                .iter()
                .map(|edge: &EdgeDefinition| RawEdgeDefinition {
                    source: RawEdgeEndpoint {
                        node: edge.source().node_id().to_string(),
                        port: edge.source().port_id().to_string(),
                    },
                    target: RawEdgeEndpoint {
                        node: edge.target().node_id().to_string(),
                        port: edge.target().port_id().to_string(),
                    },
                    capacity: match edge.capacity() {
                        EdgeCapacity::Default => None,
                        EdgeCapacity::Explicit(capacity) => Some(capacity.get()),
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn generated_workflows_round_trip_through_json() {
        fn property(generated: ArbitraryWorkflow) -> bool {
            let workflow: WorkflowDefinition = generated.0;
            let raw: RawWorkflowDefinition = raw_from_workflow(&workflow);
            let encoded: String =
                raw_workflow_to_json_string(&raw).expect("generated workflow should serialize");
            let decoded: WorkflowDefinition =
                workflow_from_json_str(&encoded).expect("encoded workflow should parse");

            decoded == workflow
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(ArbitraryWorkflow) -> bool);
    }

    #[test]
    fn raw_workflow_converts_to_validated_domain_workflow() {
        let workflow: WorkflowDefinition = valid_raw_workflow()
            .to_workflow()
            .expect("raw workflow should validate");

        assert_eq!(workflow.id().as_str(), "flow");
        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(workflow.edges().len(), 1);
        assert_eq!(
            workflow.edges()[0].capacity(),
            EdgeCapacity::Explicit(NonZeroUsize::new(4).expect("nonzero"))
        );
    }

    #[test]
    fn missing_version_is_rejected_before_domain_conversion() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.pureflow_version = None;

        let err: WorkflowFormatError = raw.to_workflow().expect_err("version is required");

        assert_eq!(err, WorkflowFormatError::MissingVersion);
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.pureflow_version = Some("2".to_owned());

        let err: WorkflowFormatError = raw.to_workflow().expect_err("version must match");

        assert_eq!(
            err,
            WorkflowFormatError::UnsupportedVersion {
                version: "2".to_owned()
            }
        );
    }

    #[test]
    fn identifier_errors_keep_raw_document_context() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.nodes[1].inputs[0] = "bad port".to_owned();

        let err: WorkflowFormatError = raw
            .to_workflow()
            .expect_err("whitespace port id should fail");

        assert_eq!(
            err,
            WorkflowFormatError::Identifier {
                context: IdentifierContext::InputPort {
                    node_index: 1,
                    port_index: 0
                },
                source: IdentifierError::Whitespace {
                    kind: IdentifierKind::Port
                }
            }
        );
    }

    #[test]
    fn zero_explicit_edge_capacity_is_rejected() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.edges[0].capacity = Some(0);

        let err: WorkflowFormatError = raw.to_workflow().expect_err("zero capacity should fail");

        assert_eq!(
            err,
            WorkflowFormatError::InvalidEdgeCapacity {
                edge_index: 0,
                capacity: 0
            }
        );
    }

    #[test]
    fn omitted_edge_capacity_uses_domain_default() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.edges[0].capacity = None;

        let workflow: WorkflowDefinition = raw.to_workflow().expect("default capacity is valid");

        assert_eq!(workflow.edges()[0].capacity(), EdgeCapacity::Default);
    }

    #[test]
    fn workflow_validation_errors_are_preserved() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.edges[0].target.node = "missing".to_owned();

        let err: WorkflowFormatError = raw.to_workflow().expect_err("missing node should fail");

        assert!(matches!(
            err,
            WorkflowFormatError::Workflow {
                source: WorkflowValidationError::UnknownNode {
                    edge_index: 0,
                    endpoint: EdgeEndpointRole::Target,
                    ..
                }
            }
        ));
    }

    #[test]
    fn workflow_cycle_errors_are_preserved() {
        let raw: RawWorkflowDefinition = RawWorkflowDefinition {
            pureflow_version: Some(CURRENT_PUREFLOW_VERSION.to_owned()),
            id: "cycle".to_owned(),
            nodes: vec![
                RawNodeDefinition {
                    id: "first".to_owned(),
                    inputs: vec!["in".to_owned()],
                    outputs: vec!["out".to_owned()],
                },
                RawNodeDefinition {
                    id: "second".to_owned(),
                    inputs: vec!["in".to_owned()],
                    outputs: vec!["out".to_owned()],
                },
            ],
            edges: vec![
                RawEdgeDefinition {
                    source: RawEdgeEndpoint {
                        node: "first".to_owned(),
                        port: "out".to_owned(),
                    },
                    target: RawEdgeEndpoint {
                        node: "second".to_owned(),
                        port: "in".to_owned(),
                    },
                    capacity: None,
                },
                RawEdgeDefinition {
                    source: RawEdgeEndpoint {
                        node: "second".to_owned(),
                        port: "out".to_owned(),
                    },
                    target: RawEdgeEndpoint {
                        node: "first".to_owned(),
                        port: "in".to_owned(),
                    },
                    capacity: None,
                },
            ],
        };

        let err: WorkflowFormatError = raw.to_workflow().expect_err("cycles are rejected");

        assert!(matches!(
            err,
            WorkflowFormatError::Workflow {
                source: WorkflowValidationError::CycleDetected { .. }
            }
        ));
    }

    #[test]
    fn edge_endpoint_identifier_errors_keep_context() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.edges[0].source.port = "bad port".to_owned();

        let err: WorkflowFormatError = raw
            .to_workflow()
            .expect_err("bad edge endpoint should fail");

        assert_eq!(
            err,
            WorkflowFormatError::Identifier {
                context: IdentifierContext::EdgeSourcePort { edge_index: 0 },
                source: IdentifierError::Whitespace {
                    kind: IdentifierKind::Port
                }
            }
        );
    }

    #[test]
    fn wrong_port_direction_is_a_workflow_validation_error() {
        let mut raw: RawWorkflowDefinition = valid_raw_workflow();
        raw.edges[0].target.port = "out".to_owned();

        let err: WorkflowFormatError = raw
            .to_workflow()
            .expect_err("target must reference an input");

        assert!(matches!(
            err,
            WorkflowFormatError::Workflow {
                source: WorkflowValidationError::UnknownPort {
                    edge_index: 0,
                    endpoint: EdgeEndpointRole::Target,
                    expected: PortDirection::Input,
                    ..
                }
            }
        ));
    }

    #[test]
    fn json_workflow_decodes_to_validated_domain_workflow() {
        let input: &str = r#"{
  "pureflow_version": "1",
  "id": "flow",
  "nodes": [
    { "id": "source", "inputs": [], "outputs": ["out"] },
    { "id": "sink", "inputs": ["in"], "outputs": [] }
  ],
  "edges": [
    {
      "source": { "node": "source", "port": "out" },
      "target": { "node": "sink", "port": "in" },
      "capacity": 4
    }
  ]
}"#;

        let workflow: WorkflowDefinition =
            workflow_from_json_str(input).expect("JSON workflow should validate");

        assert_eq!(workflow.id().as_str(), "flow");
        assert_eq!(
            workflow.edges()[0].capacity(),
            EdgeCapacity::Explicit(NonZeroUsize::new(4).expect("nonzero"))
        );
    }

    #[test]
    fn json_round_trips_raw_workflow_shape() {
        let raw: RawWorkflowDefinition = valid_raw_workflow();
        let encoded: String =
            raw_workflow_to_json_string(&raw).expect("raw workflow should serialize");

        let decoded: RawWorkflowDefinition =
            raw_workflow_from_json_str(&encoded).expect("encoded workflow should decode");

        assert_eq!(decoded, raw);
    }

    #[test]
    fn json_missing_version_keeps_typed_format_diagnostic() {
        let input: &str = r#"{
  "id": "flow",
  "nodes": [],
  "edges": []
}"#;

        let err: WorkflowJsonError =
            workflow_from_json_str(input).expect_err("version is required");

        assert!(matches!(
            err,
            WorkflowJsonError::Format {
                source: WorkflowFormatError::MissingVersion
            }
        ));
    }

    #[test]
    fn json_unsupported_version_keeps_typed_format_diagnostic() {
        let input: &str = r#"{
  "pureflow_version": "2",
  "id": "flow",
  "nodes": [],
  "edges": []
}"#;

        let err: WorkflowJsonError =
            workflow_from_json_str(input).expect_err("version must be supported");

        assert!(matches!(
            err,
            WorkflowJsonError::Format {
                source: WorkflowFormatError::UnsupportedVersion { version }
            } if version == "2"
        ));
    }

    #[test]
    fn json_decode_errors_stay_separate_from_format_errors() {
        let err: WorkflowJsonError =
            workflow_from_json_str("{not-json").expect_err("malformed JSON should fail");

        assert!(matches!(err, WorkflowJsonError::Decode { .. }));
    }

    #[test]
    fn json_unknown_fields_are_rejected() {
        let input: &str = r#"{
  "pureflow_version": "1",
  "id": "flow",
  "nodes": [],
  "edges": [],
  "extra": true
}"#;

        let err: WorkflowJsonError =
            raw_workflow_from_json_str(input).expect_err("unknown field should fail");

        assert!(matches!(err, WorkflowJsonError::Decode { .. }));
    }

    #[cfg(feature = "toml")]
    #[test]
    fn toml_workflow_decodes_to_validated_domain_workflow() {
        let input: &str = r#"
pureflow_version = "1"
id = "flow"

[[nodes]]
id = "source"
inputs = []
outputs = ["out"]

[[nodes]]
id = "sink"
inputs = ["in"]
outputs = []

[[edges]]
capacity = 4

[edges.source]
node = "source"
port = "out"

[edges.target]
node = "sink"
port = "in"
"#;

        let workflow: WorkflowDefinition =
            workflow_from_toml_str(input).expect("TOML workflow should validate");

        assert_eq!(workflow.id().as_str(), "flow");
        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(
            workflow.edges()[0].capacity(),
            EdgeCapacity::Explicit(NonZeroUsize::new(4).expect("nonzero"))
        );
    }

    #[cfg(feature = "toml")]
    #[test]
    fn toml_workflow_matches_json_domain_shape() {
        let json: &str = r#"{
  "pureflow_version": "1",
  "id": "flow",
  "nodes": [
    { "id": "source", "inputs": [], "outputs": ["out"] },
    { "id": "sink", "inputs": ["in"], "outputs": [] }
  ],
  "edges": [
    {
      "source": { "node": "source", "port": "out" },
      "target": { "node": "sink", "port": "in" },
      "capacity": 4
    }
  ]
}"#;
        let toml: &str = r#"
pureflow_version = "1"
id = "flow"

[[nodes]]
id = "source"
inputs = []
outputs = ["out"]

[[nodes]]
id = "sink"
inputs = ["in"]
outputs = []

[[edges]]
capacity = 4

[edges.source]
node = "source"
port = "out"

[edges.target]
node = "sink"
port = "in"
"#;

        let json_workflow: WorkflowDefinition =
            workflow_from_json_str(json).expect("JSON workflow should validate");
        let toml_workflow: WorkflowDefinition =
            workflow_from_toml_str(toml).expect("TOML workflow should validate");

        assert_eq!(toml_workflow, json_workflow);
    }

    #[cfg(feature = "toml")]
    #[test]
    fn toml_missing_version_keeps_typed_format_diagnostic() {
        let input: &str = r#"
id = "flow"
nodes = []
edges = []
"#;

        let err: WorkflowTomlError =
            workflow_from_toml_str(input).expect_err("version is required");

        assert!(matches!(
            err,
            WorkflowTomlError::Format {
                source: WorkflowFormatError::MissingVersion
            }
        ));
    }

    #[cfg(feature = "toml")]
    #[test]
    fn toml_decode_errors_stay_separate_from_format_errors() {
        let err: WorkflowTomlError =
            workflow_from_toml_str("not = ").expect_err("malformed TOML should fail");

        assert!(matches!(err, WorkflowTomlError::Decode { .. }));
    }

    #[cfg(feature = "toml")]
    #[test]
    fn toml_unknown_fields_are_rejected() {
        let input: &str = r#"
pureflow_version = "1"
id = "flow"
nodes = []
edges = []
extra = true
"#;

        let err: WorkflowTomlError =
            raw_workflow_from_toml_str(input).expect_err("unknown field should fail");

        assert!(matches!(err, WorkflowTomlError::Decode { .. }));
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn yaml_workflow_decodes_to_validated_domain_workflow() {
        let input: &str = r#"
pureflow_version: "1"
id: flow
nodes:
  - id: source
    inputs: []
    outputs:
      - out
  - id: sink
    inputs:
      - in
    outputs: []
edges:
  - source:
      node: source
      port: out
    target:
      node: sink
      port: in
    capacity: 4
"#;

        let workflow: WorkflowDefinition =
            workflow_from_yaml_str(input).expect("YAML workflow should validate");

        assert_eq!(workflow.id().as_str(), "flow");
        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(
            workflow.edges()[0].capacity(),
            EdgeCapacity::Explicit(NonZeroUsize::new(4).expect("nonzero"))
        );
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn yaml_workflow_matches_json_domain_shape() {
        let json: &str = r#"{
  "pureflow_version": "1",
  "id": "flow",
  "nodes": [
    { "id": "source", "inputs": [], "outputs": ["out"] },
    { "id": "sink", "inputs": ["in"], "outputs": [] }
  ],
  "edges": [
    {
      "source": { "node": "source", "port": "out" },
      "target": { "node": "sink", "port": "in" },
      "capacity": 4
    }
  ]
}"#;
        let yaml: &str = r#"
pureflow_version: "1"
id: flow
nodes:
  - id: source
    inputs: []
    outputs:
      - out
  - id: sink
    inputs:
      - in
    outputs: []
edges:
  - source:
      node: source
      port: out
    target:
      node: sink
      port: in
    capacity: 4
"#;

        let json_workflow: WorkflowDefinition =
            workflow_from_json_str(json).expect("JSON workflow should validate");
        let yaml_workflow: WorkflowDefinition =
            workflow_from_yaml_str(yaml).expect("YAML workflow should validate");

        assert_eq!(yaml_workflow, json_workflow);
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn yaml_missing_version_keeps_typed_format_diagnostic() {
        let input: &str = r"
id: flow
nodes: []
edges: []
";

        let err: WorkflowYamlError =
            workflow_from_yaml_str(input).expect_err("version is required");

        assert!(matches!(
            err,
            WorkflowYamlError::Format {
                source: WorkflowFormatError::MissingVersion
            }
        ));
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn yaml_decode_errors_stay_separate_from_format_errors() {
        let err: WorkflowYamlError =
            workflow_from_yaml_str("id: [").expect_err("malformed YAML should fail");

        assert!(matches!(err, WorkflowYamlError::Decode { .. }));
    }

    #[cfg(feature = "yaml")]
    #[test]
    fn yaml_unknown_fields_are_rejected() {
        let input: &str = r#"
pureflow_version: "1"
id: flow
nodes: []
edges: []
extra: true
"#;

        let err: WorkflowYamlError =
            raw_workflow_from_yaml_str(input).expect_err("unknown field should fail");

        assert!(matches!(err, WorkflowYamlError::Decode { .. }));
    }
}
