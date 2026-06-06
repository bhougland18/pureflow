//! Versioned raw workflow definitions for external workflow formats.
//!
//! This crate intentionally owns the boundary between parser-specific raw data
//! and validated in-memory workflow types. Parser crates and features can build
//! these raw structs without adding JSON, TOML, or YAML dependencies to
//! `pureflow-workflow`.

pub mod rule_source;
pub use rule_source::{
    EmbeddedSource, LocalFsSource, RuleSetSource, RuleSourceError, SourceRegistry,
};

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use pureflow_rules::RuleSet;
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

    /// Resolve every node's `rule_set_ref` URI into an inline `rule_set`,
    /// returning a new raw document with all references inlined.
    ///
    /// Inline rule sets that are already present are validated by parsing them
    /// into a [`RuleSet`] and re-serialising the canonical form, so the returned
    /// document is guaranteed to carry only well-formed rule sets. After this
    /// call no node carries a `rule_set_ref`; references have been replaced by
    /// the resolved inline rule set. This is the inlining step a caller performs
    /// before handing the document to graph validation.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowFormatError::RuleSet`] if a node specifies both an
    /// inline rule set and a reference, if a reference fails to resolve, or if
    /// an inline rule set is not a valid [`RuleSet`].
    pub async fn resolve_rule_sets(
        &self,
        registry: &SourceRegistry,
    ) -> Result<Self, WorkflowFormatError> {
        let mut resolved_nodes: Vec<RawNodeDefinition> = Vec::with_capacity(self.nodes.len());
        for (node_index, node) in self.nodes.iter().enumerate() {
            let rule_set: Option<RuleSet> =
                resolve_node_rule_set(node_index, node, registry).await?;
            let rule_set_json: Option<serde_json::Value> = match rule_set {
                Some(rule_set) => Some(serde_json::to_value(&rule_set).map_err(
                    |source: serde_json::Error| WorkflowFormatError::RuleSet {
                        node_index,
                        source: RuleSetBindingError::Parse {
                            reason: source.to_string(),
                        },
                    },
                )?),
                None => None,
            };
            resolved_nodes.push(RawNodeDefinition {
                id: node.id.clone(),
                inputs: node.inputs.clone(),
                outputs: node.outputs.clone(),
                rule_set: rule_set_json,
                rule_set_ref: None,
            });
        }

        Ok(Self {
            pureflow_version: self.pureflow_version.clone(),
            id: self.id.clone(),
            nodes: resolved_nodes,
            edges: self.edges.clone(),
        })
    }

    /// Resolve all rule sets and validate the workflow graph in one pass.
    ///
    /// Returns the validated [`WorkflowDefinition`] together with the typed
    /// [`RuleSet`] resolved for each node that declared one (keyed by node id).
    /// Nodes without a rule set are absent from the map.
    ///
    /// # Errors
    ///
    /// Returns the same [`WorkflowFormatError`] variants as [`Self::to_workflow`]
    /// for version, identifier, and graph failures, plus
    /// [`WorkflowFormatError::RuleSet`] for rule set conflicts, resolution
    /// failures, or malformed inline rule sets.
    pub async fn load_workflow(
        &self,
        registry: &SourceRegistry,
    ) -> Result<LoadedWorkflow, WorkflowFormatError> {
        let workflow: WorkflowDefinition = self.to_workflow()?;

        let mut rule_sets: BTreeMap<NodeId, RuleSet> = BTreeMap::new();
        for (node_index, node) in self.nodes.iter().enumerate() {
            let Some(rule_set) = resolve_node_rule_set(node_index, node, registry).await? else {
                continue;
            };
            // `to_workflow` already validated every node identifier, so this
            // re-parse cannot fail; map any error into the same identifier
            // context rather than panicking.
            let node_id: NodeId =
                NodeId::new(node.id.clone()).map_err(|source: IdentifierError| {
                    WorkflowFormatError::Identifier {
                        context: IdentifierContext::Node { node_index },
                        source,
                    }
                })?;
            rule_sets.insert(node_id, rule_set);
        }

        Ok(LoadedWorkflow {
            workflow,
            rule_sets,
        })
    }
}

/// A validated workflow paired with the rule sets resolved for its nodes.
///
/// Produced by [`RawWorkflowDefinition::load_workflow`]. The `rule_sets` map
/// only contains entries for nodes that declared an inline `rule_set` or a
/// resolvable `rule_set_ref`.
#[derive(Debug, Clone)]
pub struct LoadedWorkflow {
    /// Validated workflow graph.
    pub workflow: WorkflowDefinition,
    /// Resolved rule sets keyed by the owning node identifier.
    pub rule_sets: BTreeMap<NodeId, RuleSet>,
}

/// Resolve the rule set for a single raw node, if any.
///
/// Enforces that `rule_set` and `rule_set_ref` are mutually exclusive, parses
/// an inline rule set, or resolves a reference through the registry.
async fn resolve_node_rule_set(
    node_index: usize,
    node: &RawNodeDefinition,
    registry: &SourceRegistry,
) -> Result<Option<RuleSet>, WorkflowFormatError> {
    match (&node.rule_set, &node.rule_set_ref) {
        (Some(_), Some(_)) => Err(WorkflowFormatError::RuleSet {
            node_index,
            source: RuleSetBindingError::Conflict,
        }),
        (Some(inline), None) => {
            let rule_set: RuleSet =
                serde_json::from_value(inline.clone()).map_err(|source: serde_json::Error| {
                    WorkflowFormatError::RuleSet {
                        node_index,
                        source: RuleSetBindingError::Parse {
                            reason: source.to_string(),
                        },
                    }
                })?;
            Ok(Some(rule_set))
        }
        (None, Some(ref_uri)) => {
            let rule_set: RuleSet =
                registry
                    .load(ref_uri)
                    .await
                    .map_err(|source: RuleSourceError| WorkflowFormatError::RuleSet {
                        node_index,
                        source: RuleSetBindingError::Resolve { source },
                    })?;
            Ok(Some(rule_set))
        }
        (None, None) => Ok(None),
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
    /// Optional inline rule set for this node, embedded directly in the
    /// workflow document as raw JSON. Mutually exclusive with `rule_set_ref`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<serde_json::Value>,
    /// Optional side-car rule set reference URI resolved through a
    /// [`SourceRegistry`] at load time. Mutually exclusive with `rule_set`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_set_ref: Option<String>,
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
    /// A node's rule set could not be bound (conflict, resolution, or parse).
    RuleSet {
        /// Zero-based node index.
        node_index: usize,
        /// Rule set binding failure.
        source: RuleSetBindingError,
    },
}

/// Failure binding a node's inline or referenced rule set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleSetBindingError {
    /// The node declared both an inline `rule_set` and a `rule_set_ref`.
    Conflict,
    /// A `rule_set_ref` URI failed to resolve through the source registry.
    Resolve {
        /// Underlying source resolution failure.
        source: RuleSourceError,
    },
    /// An inline `rule_set` was not a valid rule set document.
    Parse {
        /// Human-readable description of the parse failure.
        reason: String,
    },
}

impl fmt::Display for RuleSetBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict => {
                f.write_str("node declares both an inline rule_set and a rule_set_ref")
            }
            Self::Resolve { source } => write!(f, "failed to resolve rule_set_ref: {source}"),
            Self::Parse { reason } => write!(f, "inline rule_set is invalid: {reason}"),
        }
    }
}

impl Error for RuleSetBindingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Resolve { source } => Some(source),
            Self::Conflict | Self::Parse { .. } => None,
        }
    }
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
            Self::RuleSet { node_index, source } => {
                write!(f, "node {node_index} rule set is invalid: {source}")
            }
        }
    }
}

impl Error for WorkflowFormatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identifier { source, .. } => Some(source),
            Self::Workflow { source } => Some(source),
            Self::RuleSet { source, .. } => Some(source),
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
                    rule_set: None,
                    rule_set_ref: None,
                },
                RawNodeDefinition {
                    id: "sink".to_owned(),
                    inputs: vec!["in".to_owned()],
                    outputs: Vec::new(),
                    rule_set: None,
                    rule_set_ref: None,
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
                    rule_set: None,
                    rule_set_ref: None,
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
                    rule_set: None,
                    rule_set_ref: None,
                },
                RawNodeDefinition {
                    id: "second".to_owned(),
                    inputs: vec!["in".to_owned()],
                    outputs: vec!["out".to_owned()],
                    rule_set: None,
                    rule_set_ref: None,
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

    // --- Rule set loading (pu-rna) ---------------------------------------

    use pureflow_rules::{Condition, EvaluationStrategy, Rule, RuleAction, RuleSet};

    fn sample_rule_set() -> RuleSet {
        RuleSet::new(
            "router",
            EvaluationStrategy::FirstMatch,
            vec![
                Rule::new(
                    "always-route",
                    Condition::Always,
                    RuleAction::Route(PortId::new("out").expect("port id")),
                    10,
                    "route everything",
                )
                .expect("rule is valid"),
            ],
            RuleAction::Drop,
            false,
        )
        .expect("rule set is valid")
    }

    /// Single-node workflow whose node carries no rule set yet. Callers set the
    /// `rule_set` / `rule_set_ref` fields to exercise loading.
    fn single_node_raw() -> RawWorkflowDefinition {
        RawWorkflowDefinition {
            pureflow_version: Some(CURRENT_PUREFLOW_VERSION.to_owned()),
            id: "flow".to_owned(),
            nodes: vec![RawNodeDefinition {
                id: "router".to_owned(),
                inputs: vec!["in".to_owned()],
                outputs: vec!["out".to_owned()],
                rule_set: None,
                rule_set_ref: None,
            }],
            edges: Vec::new(),
        }
    }

    #[test]
    fn load_workflow_binds_inline_rule_set() {
        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set =
            Some(serde_json::to_value(sample_rule_set()).expect("rule set serializes"));

        let registry = SourceRegistry::new();
        let loaded: LoadedWorkflow = futures::executor::block_on(raw.load_workflow(&registry))
            .expect("inline rule set should load");

        let node_id: NodeId = NodeId::new("router").expect("node id");
        assert_eq!(loaded.workflow.id().as_str(), "flow");
        assert_eq!(loaded.rule_sets.len(), 1);
        assert_eq!(loaded.rule_sets[&node_id], sample_rule_set());
    }

    #[test]
    fn load_workflow_omits_nodes_without_rule_sets() {
        let raw: RawWorkflowDefinition = single_node_raw();

        let registry = SourceRegistry::new();
        let loaded: LoadedWorkflow = futures::executor::block_on(raw.load_workflow(&registry))
            .expect("workflow without rule sets should load");

        assert!(loaded.rule_sets.is_empty());
    }

    #[test]
    fn load_workflow_resolves_rule_set_ref_via_registry() {
        let dir = tempfile::tempdir().expect("temp dir");
        let content = serde_json::to_string(&sample_rule_set()).expect("serializes");
        std::fs::write(dir.path().join("router.rules.json"), content).expect("write file");

        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set_ref = Some("router.rules.json".to_owned());

        let registry = SourceRegistry::with_base_dir(dir.path());
        let loaded: LoadedWorkflow = futures::executor::block_on(raw.load_workflow(&registry))
            .expect("referenced rule set should resolve");

        let node_id: NodeId = NodeId::new("router").expect("node id");
        assert_eq!(loaded.rule_sets[&node_id], sample_rule_set());
    }

    #[test]
    fn load_workflow_rejects_node_with_inline_and_ref() {
        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set = Some(serde_json::to_value(sample_rule_set()).expect("serializes"));
        raw.nodes[0].rule_set_ref = Some("router.rules.json".to_owned());

        let registry = SourceRegistry::new();
        let err: WorkflowFormatError = futures::executor::block_on(raw.load_workflow(&registry))
            .expect_err("conflicting rule set sources must fail");

        assert_eq!(
            err,
            WorkflowFormatError::RuleSet {
                node_index: 0,
                source: RuleSetBindingError::Conflict,
            }
        );
    }

    #[test]
    fn load_workflow_rejects_malformed_inline_rule_set() {
        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set = Some(serde_json::json!({ "not": "a rule set" }));

        let registry = SourceRegistry::new();
        let err: WorkflowFormatError = futures::executor::block_on(raw.load_workflow(&registry))
            .expect_err("malformed inline rule set must fail");

        assert!(matches!(
            err,
            WorkflowFormatError::RuleSet {
                node_index: 0,
                source: RuleSetBindingError::Parse { .. }
            }
        ));
    }

    #[test]
    fn load_workflow_surfaces_unknown_scheme_resolution_error() {
        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set_ref = Some("guardiandb://acct/rules".to_owned());

        let registry = SourceRegistry::new();
        let err: WorkflowFormatError = futures::executor::block_on(raw.load_workflow(&registry))
            .expect_err("unknown scheme must fail");

        assert!(matches!(
            err,
            WorkflowFormatError::RuleSet {
                node_index: 0,
                source: RuleSetBindingError::Resolve {
                    source: RuleSourceError::UnknownScheme { .. }
                }
            }
        ));
    }

    #[test]
    fn resolve_rule_sets_inlines_references() {
        let dir = tempfile::tempdir().expect("temp dir");
        let content = serde_json::to_string(&sample_rule_set()).expect("serializes");
        std::fs::write(dir.path().join("router.rules.json"), content).expect("write file");

        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set_ref = Some("router.rules.json".to_owned());

        let registry = SourceRegistry::with_base_dir(dir.path());
        let resolved: RawWorkflowDefinition =
            futures::executor::block_on(raw.resolve_rule_sets(&registry))
                .expect("reference should inline");

        // The reference is gone; the inline rule set is present and parses back
        // to the original rule set.
        assert!(resolved.nodes[0].rule_set_ref.is_none());
        let inlined: &serde_json::Value = resolved.nodes[0]
            .rule_set
            .as_ref()
            .expect("rule set inlined");
        let parsed: RuleSet =
            serde_json::from_value(inlined.clone()).expect("inlined rule set parses");
        assert_eq!(parsed, sample_rule_set());
    }

    #[test]
    fn raw_node_with_inline_rule_set_round_trips_through_json() {
        let mut raw: RawWorkflowDefinition = single_node_raw();
        raw.nodes[0].rule_set = Some(serde_json::to_value(sample_rule_set()).expect("serializes"));

        let encoded: String = raw_workflow_to_json_string(&raw).expect("raw workflow serializes");
        let decoded: RawWorkflowDefinition =
            raw_workflow_from_json_str(&encoded).expect("raw workflow decodes");

        assert_eq!(decoded, raw);
    }

    #[test]
    fn raw_node_without_rule_set_omits_fields_in_json() {
        let raw: RawWorkflowDefinition = single_node_raw();
        let encoded: String = raw_workflow_to_json_string(&raw).expect("raw workflow serializes");

        assert!(!encoded.contains("rule_set"));
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
