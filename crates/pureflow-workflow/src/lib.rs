//! External workflow definitions and validation entrypoints.
//!
//! ## Fragment: workflow-structural-boundary
//!
//! This crate owns the static workflow graph shape. It validates structural
//! facts that must be true before any runtime can reason about execution:
//! nodes are uniquely named, ports are uniquely named within a node, and edges
//! connect declared output ports to declared input ports. Runtime concerns such
//! as scheduling policy, cycles, payload compatibility, cancellation, and
//! backpressure are intentionally left to later layers.
//!
//! ## Fragment: workflow-validation-scope
//!
//! The validation rules stop at structural honesty on purpose. A graph can be
//! structurally valid and still semantically wrong for a later runtime or data
//! model. Keeping that line clear prevents the workflow crate from accumulating
//! scheduling, typing, or capability policy that belongs elsewhere.
//!
//! ## Fragment: workflow-deterministic-errors
//!
//! Validation uses ordered maps and sets so duplicate detection and missing-edge
//! errors are reported deterministically. The graphs are small enough that this
//! tradeoff favors stable diagnostics and test output over marginal hash-table
//! speed.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use pureflow_types::{IdentifierError, NodeId, PortId, WorkflowId};

/// Direction of a port in a node's static topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortDirection {
    /// A port that receives data or control from an upstream node.
    Input,
    /// A port that emits data or control to a downstream node.
    Output,
}

impl PortDirection {
    const fn label(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

/// Which side of an edge failed validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeEndpointRole {
    /// The upstream endpoint of an edge.
    Source,
    /// The downstream endpoint of an edge.
    Target,
}

impl EdgeEndpointRole {
    const fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
        }
    }
}

/// Error returned when a workflow graph is structurally invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowValidationError {
    /// Two nodes in the graph used the same identifier.
    DuplicateNode {
        /// Duplicated node identifier.
        node_id: NodeId,
    },
    /// A node declared the same port identifier more than once.
    DuplicatePort {
        /// Node that owns the duplicated port.
        node_id: NodeId,
        /// Duplicated port identifier.
        port_id: PortId,
    },
    /// An edge referenced a node that is not declared in the graph.
    UnknownNode {
        /// Zero-based index of the invalid edge.
        edge_index: usize,
        /// Endpoint role that referenced the missing node.
        endpoint: EdgeEndpointRole,
        /// Missing node identifier.
        node_id: NodeId,
    },
    /// An edge referenced a port that is not declared for the required direction.
    UnknownPort {
        /// Zero-based index of the invalid edge.
        edge_index: usize,
        /// Endpoint role that referenced the missing port.
        endpoint: EdgeEndpointRole,
        /// Node that should own the port.
        node_id: NodeId,
        /// Missing port identifier.
        port_id: PortId,
        /// Direction required by this endpoint.
        expected: PortDirection,
    },
    /// The graph contains a directed cycle.
    CycleDetected {
        /// One detected cycle, reported in traversal order.
        cycle: Vec<NodeId>,
    },
}

impl fmt::Display for WorkflowValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode { node_id } => {
                write!(f, "workflow graph contains duplicate node `{node_id}`")
            }
            Self::DuplicatePort { node_id, port_id } => {
                write!(f, "node `{node_id}` contains duplicate port `{port_id}`")
            }
            Self::UnknownNode {
                edge_index,
                endpoint,
                node_id,
            } => write!(
                f,
                "edge {edge_index} {} references unknown node `{node_id}`",
                endpoint.label()
            ),
            Self::UnknownPort {
                edge_index,
                endpoint,
                node_id,
                port_id,
                expected,
            } => write!(
                f,
                "edge {edge_index} {} references unknown {} port `{port_id}` on node `{node_id}`",
                endpoint.label(),
                expected.label()
            ),
            Self::CycleDetected { cycle } => {
                write!(f, "workflow graph contains a cycle involving")?;
                for node_id in cycle {
                    write!(f, " `{node_id}`")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for WorkflowValidationError {}

/// Static endpoint for one side of a workflow edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEndpoint {
    node_id: NodeId,
    port_id: PortId,
}

impl EdgeEndpoint {
    /// Create an edge endpoint from a node and port identifier.
    #[must_use]
    pub const fn new(node_id: NodeId, port_id: PortId) -> Self {
        Self { node_id, port_id }
    }

    /// Node referenced by this endpoint.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port referenced by this endpoint.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }
}

/// Capacity policy for a workflow edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeCapacity {
    /// Use the engine default capacity.
    Default,
    /// Use an explicit bounded capacity.
    Explicit(NonZeroUsize),
}

impl EdgeCapacity {
    /// Resolve this capacity policy against the runtime default.
    #[must_use]
    pub const fn resolve(self, default: NonZeroUsize) -> NonZeroUsize {
        match self {
            Self::Default => default,
            Self::Explicit(capacity) => capacity,
        }
    }
}

/// Directed connection from one output port to one input port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeDefinition {
    source: EdgeEndpoint,
    target: EdgeEndpoint,
    capacity: EdgeCapacity,
}

impl EdgeDefinition {
    /// Create an edge from an upstream endpoint to a downstream endpoint.
    #[must_use]
    pub const fn new(source: EdgeEndpoint, target: EdgeEndpoint) -> Self {
        Self {
            source,
            target,
            capacity: EdgeCapacity::Default,
        }
    }

    /// Create an edge with an explicit bounded capacity.
    #[must_use]
    pub const fn with_capacity(
        source: EdgeEndpoint,
        target: EdgeEndpoint,
        capacity: NonZeroUsize,
    ) -> Self {
        Self {
            source,
            target,
            capacity: EdgeCapacity::Explicit(capacity),
        }
    }

    /// Upstream output endpoint.
    #[must_use]
    pub const fn source(&self) -> &EdgeEndpoint {
        &self.source
    }

    /// Downstream input endpoint.
    #[must_use]
    pub const fn target(&self) -> &EdgeEndpoint {
        &self.target
    }

    /// Capacity policy for this edge.
    #[must_use]
    pub const fn capacity(&self) -> EdgeCapacity {
        self.capacity
    }
}

/// Static node declaration and its input/output port topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDefinition {
    id: NodeId,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
}

impl NodeDefinition {
    /// Create a node with declared input and output ports.
    ///
    /// # Errors
    ///
    /// Returns an error if a port identifier is repeated within this node,
    /// including reuse across input and output directions.
    pub fn new(
        id: NodeId,
        input_ports: impl Into<Vec<PortId>>,
        output_ports: impl Into<Vec<PortId>>,
    ) -> Result<Self, WorkflowValidationError> {
        let input_ports: Vec<PortId> = input_ports.into();
        let output_ports: Vec<PortId> = output_ports.into();
        reject_duplicate_ports(&id, &input_ports, &output_ports)?;

        Ok(Self {
            id,
            input_ports,
            output_ports,
        })
    }

    /// Node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Declared input ports.
    #[must_use]
    pub fn input_ports(&self) -> &[PortId] {
        &self.input_ports
    }

    /// Declared output ports.
    #[must_use]
    pub fn output_ports(&self) -> &[PortId] {
        &self.output_ports
    }
}

/// Validated graph-level workflow structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowGraph {
    nodes: Vec<NodeDefinition>,
    edges: Vec<EdgeDefinition>,
}

impl WorkflowGraph {
    /// Create and validate a workflow graph.
    ///
    /// # Errors
    ///
    /// Returns an error when nodes or ports are duplicated, when an edge
    /// references an undeclared node or the wrong port direction, or when the
    /// graph contains a directed cycle.
    pub fn new(
        nodes: impl Into<Vec<NodeDefinition>>,
        edges: impl Into<Vec<EdgeDefinition>>,
    ) -> Result<Self, WorkflowValidationError> {
        Self::build(nodes, edges, false)
    }

    /// Create and validate a workflow graph while allowing cycles.
    ///
    /// # Errors
    ///
    /// Returns an error when nodes or ports are duplicated, or when an edge
    /// references an undeclared node or the wrong port direction.
    pub fn with_cycles_allowed(
        nodes: impl Into<Vec<NodeDefinition>>,
        edges: impl Into<Vec<EdgeDefinition>>,
    ) -> Result<Self, WorkflowValidationError> {
        Self::build(nodes, edges, true)
    }

    /// Create an empty graph with no nodes or edges.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Declared nodes in stable workflow order.
    #[must_use]
    pub fn nodes(&self) -> &[NodeDefinition] {
        &self.nodes
    }

    /// Declared edges in stable workflow order.
    #[must_use]
    pub fn edges(&self) -> &[EdgeDefinition] {
        &self.edges
    }

    /// Return a deterministic topological order for the nodes in this graph.
    ///
    /// # Errors
    ///
    /// Returns an error when the graph is structurally invalid or contains a
    /// directed cycle.
    pub fn topological_order(&self) -> Result<Vec<NodeId>, WorkflowValidationError> {
        let topology: GraphTopology = GraphTopology::from_graph(&self.nodes, &self.edges)?;
        topology.topological_order()
    }

    fn build(
        nodes: impl Into<Vec<NodeDefinition>>,
        edges: impl Into<Vec<EdgeDefinition>>,
        allow_cycles: bool,
    ) -> Result<Self, WorkflowValidationError> {
        let graph: Self = Self {
            nodes: nodes.into(),
            edges: edges.into(),
        };
        graph.validate(allow_cycles)?;
        Ok(graph)
    }

    fn validate(&self, allow_cycles: bool) -> Result<(), WorkflowValidationError> {
        let topology: GraphTopology = GraphTopology::from_graph(&self.nodes, &self.edges)?;
        if !allow_cycles {
            topology.topological_order()?;
        }
        Ok(())
    }
}

/// Parsed workflow definition independent of runtime execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDefinition {
    id: WorkflowId,
    graph: WorkflowGraph,
}

impl WorkflowDefinition {
    /// Create a workflow definition from an already validated graph.
    #[must_use]
    pub const fn new(id: WorkflowId, graph: WorkflowGraph) -> Self {
        Self { id, graph }
    }

    /// Create a workflow definition from raw graph parts.
    ///
    /// # Errors
    ///
    /// Returns an error when the graph is structurally invalid.
    pub fn from_parts(
        id: WorkflowId,
        nodes: impl Into<Vec<NodeDefinition>>,
        edges: impl Into<Vec<EdgeDefinition>>,
    ) -> Result<Self, WorkflowValidationError> {
        let graph: WorkflowGraph = WorkflowGraph::new(nodes, edges)?;
        Ok(Self::new(id, graph))
    }

    /// Create a placeholder workflow with no nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow identifier is invalid.
    pub fn empty(name: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self::new(WorkflowId::new(name)?, WorkflowGraph::empty()))
    }

    /// Workflow identifier.
    #[must_use]
    pub const fn id(&self) -> &WorkflowId {
        &self.id
    }

    /// Validated workflow graph.
    #[must_use]
    pub const fn graph(&self) -> &WorkflowGraph {
        &self.graph
    }

    /// Declared nodes in stable workflow order.
    #[must_use]
    pub fn nodes(&self) -> &[NodeDefinition] {
        self.graph.nodes()
    }

    /// Declared edges in stable workflow order.
    #[must_use]
    pub fn edges(&self) -> &[EdgeDefinition] {
        self.graph.edges()
    }
}

struct GraphTopology {
    node_ids: Vec<NodeId>,
    inputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>>,
    outputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>>,
    outgoing_by_node: BTreeMap<NodeId, BTreeSet<NodeId>>,
    indegree_by_node: BTreeMap<NodeId, usize>,
}

impl GraphTopology {
    fn from_graph(
        nodes: &[NodeDefinition],
        edges: &[EdgeDefinition],
    ) -> Result<Self, WorkflowValidationError> {
        reject_duplicate_nodes(nodes)?;

        let mut inputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>> = BTreeMap::new();
        let mut outputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>> = BTreeMap::new();
        let mut outgoing_by_node: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
        let mut indegree_by_node: BTreeMap<NodeId, usize> = BTreeMap::new();
        let mut node_ids: Vec<NodeId> = Vec::with_capacity(nodes.len());

        for node in nodes {
            let node_id: NodeId = node.id().clone();
            node_ids.push(node_id.clone());
            inputs_by_node.insert(
                node_id.clone(),
                node.input_ports().iter().cloned().collect(),
            );
            outputs_by_node.insert(
                node_id.clone(),
                node.output_ports().iter().cloned().collect(),
            );
            outgoing_by_node.insert(node_id.clone(), BTreeSet::new());
            indegree_by_node.insert(node_id, 0);
        }

        let mut topology: Self = Self {
            node_ids,
            inputs_by_node,
            outputs_by_node,
            outgoing_by_node,
            indegree_by_node,
        };

        for (edge_index, edge) in edges.iter().enumerate() {
            topology.validate_endpoint(
                edge_index,
                EdgeEndpointRole::Source,
                edge.source(),
                PortDirection::Output,
            )?;
            topology.validate_endpoint(
                edge_index,
                EdgeEndpointRole::Target,
                edge.target(),
                PortDirection::Input,
            )?;

            let Some(outgoing): Option<&mut BTreeSet<NodeId>> =
                topology.outgoing_by_node.get_mut(edge.source().node_id())
            else {
                return Err(WorkflowValidationError::UnknownNode {
                    edge_index,
                    endpoint: EdgeEndpointRole::Source,
                    node_id: edge.source().node_id().clone(),
                });
            };
            outgoing.insert(edge.target().node_id().clone());

            let Some(indegree): Option<&mut usize> =
                topology.indegree_by_node.get_mut(edge.target().node_id())
            else {
                return Err(WorkflowValidationError::UnknownNode {
                    edge_index,
                    endpoint: EdgeEndpointRole::Target,
                    node_id: edge.target().node_id().clone(),
                });
            };
            *indegree += 1;
        }

        Ok(topology)
    }

    fn validate_endpoint(
        &self,
        edge_index: usize,
        endpoint: EdgeEndpointRole,
        edge_endpoint: &EdgeEndpoint,
        expected: PortDirection,
    ) -> Result<(), WorkflowValidationError> {
        let ports_by_node: &BTreeMap<NodeId, BTreeSet<PortId>> = match expected {
            PortDirection::Input => &self.inputs_by_node,
            PortDirection::Output => &self.outputs_by_node,
        };

        let ports: &BTreeSet<PortId> =
            ports_by_node.get(edge_endpoint.node_id()).ok_or_else(|| {
                WorkflowValidationError::UnknownNode {
                    edge_index,
                    endpoint,
                    node_id: edge_endpoint.node_id().clone(),
                }
            })?;

        if !ports.contains(edge_endpoint.port_id()) {
            return Err(WorkflowValidationError::UnknownPort {
                edge_index,
                endpoint,
                node_id: edge_endpoint.node_id().clone(),
                port_id: edge_endpoint.port_id().clone(),
                expected,
            });
        }

        Ok(())
    }

    fn topological_order(&self) -> Result<Vec<NodeId>, WorkflowValidationError> {
        let mut indegree_by_node: BTreeMap<NodeId, usize> = self.indegree_by_node.clone();
        let mut ready: BTreeSet<NodeId> = indegree_by_node
            .iter()
            .filter_map(|(node_id, indegree): (&NodeId, &usize)| {
                (*indegree == 0).then_some(node_id.clone())
            })
            .collect();
        let mut order: Vec<NodeId> = Vec::with_capacity(indegree_by_node.len());

        while let Some(node_id) = ready.pop_first() {
            order.push(node_id.clone());

            let Some(children): Option<&BTreeSet<NodeId>> = self.outgoing_by_node.get(&node_id)
            else {
                continue;
            };

            for child in children {
                let Some(indegree): Option<&mut usize> = indegree_by_node.get_mut(child) else {
                    continue;
                };
                *indegree -= 1;
                if *indegree == 0 {
                    ready.insert(child.clone());
                }
            }
        }

        if order.len() == self.node_ids.len() {
            return Ok(order);
        }

        let remaining: BTreeSet<NodeId> = self
            .node_ids
            .iter()
            .filter(|node_id: &&NodeId| !order.contains(node_id))
            .cloned()
            .collect();
        let cycle: Vec<NodeId> = self.find_cycle(&remaining);
        Err(WorkflowValidationError::CycleDetected { cycle })
    }

    fn find_cycle(&self, remaining: &BTreeSet<NodeId>) -> Vec<NodeId> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum VisitState {
            Visiting,
            Visited,
        }

        fn dfs(
            node_id: &NodeId,
            topology: &GraphTopology,
            remaining: &BTreeSet<NodeId>,
            states: &mut BTreeMap<NodeId, VisitState>,
            stack: &mut Vec<NodeId>,
        ) -> Option<Vec<NodeId>> {
            states.insert(node_id.clone(), VisitState::Visiting);
            stack.push(node_id.clone());

            let Some(children): Option<&BTreeSet<NodeId>> = topology.outgoing_by_node.get(node_id)
            else {
                stack.pop();
                states.insert(node_id.clone(), VisitState::Visited);
                return None;
            };

            for child in children {
                if !remaining.contains(child) {
                    continue;
                }

                match states.get(child) {
                    Some(VisitState::Visiting) => {
                        if let Some(cycle) = cycle_from_stack(stack, child) {
                            return Some(cycle);
                        }
                    }
                    Some(VisitState::Visited) => {}
                    None => {
                        if let Some(cycle) = dfs(child, topology, remaining, states, stack) {
                            return Some(cycle);
                        }
                    }
                }
            }

            stack.pop();
            states.insert(node_id.clone(), VisitState::Visited);
            None
        }

        fn cycle_from_stack(stack: &[NodeId], child: &NodeId) -> Option<Vec<NodeId>> {
            let start_index: usize = stack.iter().position(|entry: &NodeId| entry == child)?;
            let mut cycle: Vec<NodeId> = stack.iter().skip(start_index).cloned().collect();
            cycle.push(child.clone());
            Some(cycle)
        }

        let mut states: BTreeMap<NodeId, VisitState> = BTreeMap::new();
        let mut stack: Vec<NodeId> = Vec::new();

        for node_id in &self.node_ids {
            if !remaining.contains(node_id) || states.contains_key(node_id) {
                continue;
            }

            if let Some(cycle) = dfs(node_id, self, remaining, &mut states, &mut stack) {
                return cycle;
            }
        }

        remaining.iter().cloned().collect()
    }
}

fn reject_duplicate_nodes(nodes: &[NodeDefinition]) -> Result<(), WorkflowValidationError> {
    let mut seen: BTreeSet<NodeId> = BTreeSet::new();

    for node in nodes {
        if !seen.insert(node.id().clone()) {
            return Err(WorkflowValidationError::DuplicateNode {
                node_id: node.id().clone(),
            });
        }
    }

    Ok(())
}

fn reject_duplicate_ports(
    node_id: &NodeId,
    input_ports: &[PortId],
    output_ports: &[PortId],
) -> Result<(), WorkflowValidationError> {
    let mut seen: BTreeSet<PortId> = BTreeSet::new();

    for port_id in input_ports.iter().chain(output_ports) {
        if !seen.insert(port_id.clone()) {
            return Err(WorkflowValidationError::DuplicatePort {
                node_id: node_id.clone(),
                port_id: port_id.clone(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_types::IdentifierKind;
    use proptest::{collection::hash_set, prelude::*};
    use quickcheck::{Arbitrary as QuickArbitrary, Gen, QuickCheck};
    use std::num::NonZeroUsize;
    use std::panic::{self, AssertUnwindSafe};

    fn valid_identifier_strategy() -> impl Strategy<Value = String> {
        prop::collection::vec(
            any::<char>().prop_filter(
                "identifier characters must not be whitespace or control",
                |ch| !ch.is_whitespace() && !ch.is_control(),
            ),
            1..16,
        )
        .prop_map(|chars: Vec<char>| chars.into_iter().collect())
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn endpoint(node: &str, port: &str) -> EdgeEndpoint {
        EdgeEndpoint::new(node_id(node), port_id(port))
    }

    #[derive(Debug, Clone)]
    struct GeneratedValidGraph {
        nodes: Vec<NodeDefinition>,
        edges: Vec<EdgeDefinition>,
    }

    impl QuickArbitrary for GeneratedValidGraph {
        fn arbitrary(g: &mut Gen) -> Self {
            let node_count = generated_count(g, 1, 6);
            let nodes: Vec<NodeDefinition> = (0..node_count)
                .map(|index| generated_routable_node(index))
                .collect();
            let mut edges = Vec::new();

            for source in 0..node_count {
                for target in (source + 1)..node_count {
                    if generated_bool(g) {
                        edges.push(generated_edge(source, target));
                    }
                }
            }

            Self { nodes, edges }
        }
    }

    #[derive(Debug, Clone)]
    struct SmallNodeCount(usize);

    impl QuickArbitrary for SmallNodeCount {
        fn arbitrary(g: &mut Gen) -> Self {
            Self(generated_count(g, 1, 6))
        }
    }

    #[derive(Debug, Clone)]
    struct GeneratedValidationCase {
        scenario: ValidationScenario,
    }

    #[derive(Debug, Clone)]
    enum ValidationScenario {
        DuplicatePort {
            node_id: NodeId,
            port_id: PortId,
        },
        Graph {
            nodes: Vec<NodeDefinition>,
            edges: Vec<EdgeDefinition>,
            expected: ExpectedGraphResult,
        },
    }

    #[derive(Debug, Clone, Copy)]
    enum ExpectedGraphResult {
        Ok,
        DuplicateNode,
        UnknownNode(EdgeEndpointRole),
        UnknownPort(EdgeEndpointRole, PortDirection),
        CycleDetected,
    }

    impl QuickArbitrary for GeneratedValidationCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let scenario = match generated_u8(g) % 8 {
                0 => {
                    let graph = GeneratedValidGraph::arbitrary(g);
                    ValidationScenario::Graph {
                        nodes: graph.nodes,
                        edges: graph.edges,
                        expected: ExpectedGraphResult::Ok,
                    }
                }
                1 => ValidationScenario::Graph {
                    nodes: vec![generated_empty_node(0), generated_empty_node(0)],
                    edges: Vec::new(),
                    expected: ExpectedGraphResult::DuplicateNode,
                },
                2 => ValidationScenario::DuplicatePort {
                    node_id: generated_node_id(0),
                    port_id: port_id("dup"),
                },
                3 => ValidationScenario::Graph {
                    nodes: vec![generated_sink_node(0)],
                    edges: vec![EdgeDefinition::new(
                        EdgeEndpoint::new(node_id("missing_source"), port_id("out")),
                        EdgeEndpoint::new(generated_node_id(0), port_id("in")),
                    )],
                    expected: ExpectedGraphResult::UnknownNode(EdgeEndpointRole::Source),
                },
                4 => ValidationScenario::Graph {
                    nodes: vec![generated_source_node(0)],
                    edges: vec![EdgeDefinition::new(
                        EdgeEndpoint::new(generated_node_id(0), port_id("out")),
                        EdgeEndpoint::new(node_id("missing_target"), port_id("in")),
                    )],
                    expected: ExpectedGraphResult::UnknownNode(EdgeEndpointRole::Target),
                },
                5 => ValidationScenario::Graph {
                    nodes: vec![generated_sink_node(0), generated_sink_node(1)],
                    edges: vec![EdgeDefinition::new(
                        EdgeEndpoint::new(generated_node_id(0), port_id("in")),
                        EdgeEndpoint::new(generated_node_id(1), port_id("in")),
                    )],
                    expected: ExpectedGraphResult::UnknownPort(
                        EdgeEndpointRole::Source,
                        PortDirection::Output,
                    ),
                },
                6 => ValidationScenario::Graph {
                    nodes: vec![generated_source_node(0), generated_source_node(1)],
                    edges: vec![EdgeDefinition::new(
                        EdgeEndpoint::new(generated_node_id(0), port_id("out")),
                        EdgeEndpoint::new(generated_node_id(1), port_id("out")),
                    )],
                    expected: ExpectedGraphResult::UnknownPort(
                        EdgeEndpointRole::Target,
                        PortDirection::Input,
                    ),
                },
                _ => {
                    let (nodes, edges) = generated_cycle_graph(g);
                    ValidationScenario::Graph {
                        nodes,
                        edges,
                        expected: ExpectedGraphResult::CycleDetected,
                    }
                }
            };

            Self { scenario }
        }
    }

    fn generated_count(g: &mut Gen, min: usize, max_exclusive: usize) -> usize {
        min + (generated_usize(g) % (max_exclusive - min))
    }

    fn generated_bool(g: &mut Gen) -> bool {
        <bool as QuickArbitrary>::arbitrary(g)
    }

    fn generated_u8(g: &mut Gen) -> u8 {
        <u8 as QuickArbitrary>::arbitrary(g)
    }

    fn generated_usize(g: &mut Gen) -> usize {
        <usize as QuickArbitrary>::arbitrary(g)
    }

    fn generated_node_id(index: usize) -> NodeId {
        node_id(&format!("node_{index}"))
    }

    fn generated_routable_node(index: usize) -> NodeDefinition {
        NodeDefinition::new(generated_node_id(index), [port_id("in")], [port_id("out")])
            .expect("generated routable node is valid")
    }

    fn generated_source_node(index: usize) -> NodeDefinition {
        NodeDefinition::new(
            generated_node_id(index),
            Vec::<PortId>::new(),
            [port_id("out")],
        )
        .expect("generated source node is valid")
    }

    fn generated_sink_node(index: usize) -> NodeDefinition {
        NodeDefinition::new(
            generated_node_id(index),
            [port_id("in")],
            Vec::<PortId>::new(),
        )
        .expect("generated sink node is valid")
    }

    fn generated_empty_node(index: usize) -> NodeDefinition {
        NodeDefinition::new(
            generated_node_id(index),
            Vec::<PortId>::new(),
            Vec::<PortId>::new(),
        )
        .expect("generated empty node is valid")
    }

    fn generated_edge(source: usize, target: usize) -> EdgeDefinition {
        EdgeDefinition::new(
            EdgeEndpoint::new(generated_node_id(source), port_id("out")),
            EdgeEndpoint::new(generated_node_id(target), port_id("in")),
        )
    }

    fn generated_cycle_graph(g: &mut Gen) -> (Vec<NodeDefinition>, Vec<EdgeDefinition>) {
        let node_count = generated_count(g, 2, 7);
        let nodes = (0..node_count).map(generated_routable_node).collect();
        let edges = (0..node_count)
            .map(|source| generated_edge(source, (source + 1) % node_count))
            .collect();

        (nodes, edges)
    }

    fn generated_fan_out_graph(target_count: usize) -> (Vec<NodeDefinition>, Vec<EdgeDefinition>) {
        let mut nodes = vec![generated_source_node(0)];
        let mut edges = Vec::new();

        for target in 1..=target_count {
            nodes.push(generated_sink_node(target));
            edges.push(generated_edge(0, target));
        }

        (nodes, edges)
    }

    fn generated_fan_in_graph(source_count: usize) -> (Vec<NodeDefinition>, Vec<EdgeDefinition>) {
        let sink_index = source_count;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        for source in 0..source_count {
            nodes.push(generated_source_node(source));
            edges.push(generated_edge(source, sink_index));
        }

        nodes.push(generated_sink_node(sink_index));
        (nodes, edges)
    }

    fn validate_generated_case(case: &GeneratedValidationCase) -> bool {
        match &case.scenario {
            ValidationScenario::DuplicatePort { node_id, port_id } => matches!(
                NodeDefinition::new(node_id.clone(), [port_id.clone()], [port_id.clone()]),
                Err(WorkflowValidationError::DuplicatePort { .. })
            ),
            ValidationScenario::Graph {
                nodes,
                edges,
                expected,
            } => graph_result_matches(WorkflowGraph::new(nodes.clone(), edges.clone()), *expected),
        }
    }

    fn graph_result_matches(
        result: Result<WorkflowGraph, WorkflowValidationError>,
        expected: ExpectedGraphResult,
    ) -> bool {
        match (result, expected) {
            (Ok(_), ExpectedGraphResult::Ok) => true,
            (
                Err(WorkflowValidationError::DuplicateNode { .. }),
                ExpectedGraphResult::DuplicateNode,
            ) => true,
            (
                Err(WorkflowValidationError::UnknownNode { endpoint, .. }),
                ExpectedGraphResult::UnknownNode(expected_endpoint),
            ) => endpoint == expected_endpoint,
            (
                Err(WorkflowValidationError::UnknownPort {
                    endpoint, expected, ..
                }),
                ExpectedGraphResult::UnknownPort(expected_endpoint, expected_direction),
            ) => endpoint == expected_endpoint && expected == expected_direction,
            (
                Err(WorkflowValidationError::CycleDetected { cycle }),
                ExpectedGraphResult::CycleDetected,
            ) => !cycle.is_empty(),
            _ => false,
        }
    }

    #[test]
    fn empty_workflow_uses_valid_identifier() {
        let workflow = WorkflowDefinition::empty("pureflow-scaffold").expect("valid id");

        assert_eq!(workflow.id().as_str(), "pureflow-scaffold");
        assert!(workflow.nodes().is_empty());
        assert!(workflow.edges().is_empty());
    }

    #[test]
    fn empty_workflow_rejects_invalid_identifier() {
        let err = WorkflowDefinition::empty("bad workflow").expect_err("whitespace must fail");
        assert_eq!(
            err,
            IdentifierError::Whitespace {
                kind: IdentifierKind::Workflow
            }
        );
    }

    #[test]
    fn valid_workflow_represents_nodes_ports_and_edges() {
        let producer = NodeDefinition::new(
            node_id("producer"),
            Vec::<PortId>::new(),
            [port_id("records")],
        )
        .expect("valid producer");
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("producer", "records"),
            endpoint("consumer", "records"),
        );

        let workflow =
            WorkflowDefinition::from_parts(workflow_id("ingest"), [producer, consumer], [edge])
                .expect("valid graph");

        assert_eq!(workflow.id().as_str(), "ingest");
        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(workflow.edges().len(), 1);
    }

    #[test]
    fn edge_capacity_defaults_to_engine_default_policy() {
        let edge = EdgeDefinition::new(endpoint("producer", "records"), endpoint("consumer", "in"));

        assert_eq!(edge.capacity(), EdgeCapacity::Default);
        assert_eq!(
            edge.capacity()
                .resolve(NonZeroUsize::new(7).expect("nonzero")),
            NonZeroUsize::new(7).expect("nonzero")
        );
    }

    #[test]
    fn edge_capacity_round_trips_explicit_value() {
        let capacity: NonZeroUsize = NonZeroUsize::new(3).expect("nonzero");
        let edge = EdgeDefinition::with_capacity(
            endpoint("producer", "records"),
            endpoint("consumer", "in"),
            capacity,
        );

        assert_eq!(edge.capacity(), EdgeCapacity::Explicit(capacity));
        assert_eq!(
            edge.capacity()
                .resolve(NonZeroUsize::new(7).expect("nonzero")),
            capacity
        );
    }

    #[test]
    fn topological_order_returns_sources_before_sinks() {
        let producer =
            NodeDefinition::new(node_id("producer"), Vec::<PortId>::new(), [port_id("out")])
                .expect("valid producer");
        let consumer =
            NodeDefinition::new(node_id("consumer"), [port_id("in")], Vec::<PortId>::new())
                .expect("valid consumer");
        let edge = EdgeDefinition::new(endpoint("producer", "out"), endpoint("consumer", "in"));
        let graph = WorkflowGraph::new([producer, consumer], [edge]).expect("valid graph");

        assert_eq!(
            graph
                .topological_order()
                .expect("acyclic graph should order"),
            vec![node_id("producer"), node_id("consumer")]
        );
    }

    #[test]
    fn workflow_graph_rejects_cycles_by_default() {
        let first = NodeDefinition::new(node_id("first"), [port_id("in")], [port_id("out")])
            .expect("valid first node");
        let second = NodeDefinition::new(node_id("second"), [port_id("in")], [port_id("out")])
            .expect("valid second node");
        let edges = [
            EdgeDefinition::new(endpoint("first", "out"), endpoint("second", "in")),
            EdgeDefinition::new(endpoint("second", "out"), endpoint("first", "in")),
        ];

        let err = WorkflowGraph::new([first, second], edges).expect_err("cycle must fail");

        assert!(
            matches!(err, WorkflowValidationError::CycleDetected { cycle } if cycle.contains(&node_id("first")) && cycle.contains(&node_id("second")))
        );
    }

    #[test]
    fn workflow_graph_with_cycles_allowed_keeps_ordering_diagnostics_available() {
        let first = NodeDefinition::new(node_id("first"), [port_id("in")], [port_id("out")])
            .expect("valid first node");
        let second = NodeDefinition::new(node_id("second"), [port_id("in")], [port_id("out")])
            .expect("valid second node");
        let edges = [
            EdgeDefinition::new(endpoint("first", "out"), endpoint("second", "in")),
            EdgeDefinition::new(endpoint("second", "out"), endpoint("first", "in")),
        ];

        let graph = WorkflowGraph::with_cycles_allowed([first, second], edges)
            .expect("cycle-allowed graph should build");

        let err = graph
            .topological_order()
            .expect_err("cycle should still be reported by ordering");
        assert!(matches!(err, WorkflowValidationError::CycleDetected { .. }));
    }

    #[test]
    fn duplicate_nodes_are_rejected() {
        let first =
            NodeDefinition::new(node_id("step"), Vec::<PortId>::new(), Vec::<PortId>::new())
                .expect("valid node");
        let second =
            NodeDefinition::new(node_id("step"), Vec::<PortId>::new(), Vec::<PortId>::new())
                .expect("valid node");

        let err = WorkflowGraph::new([first, second], Vec::<EdgeDefinition>::new())
            .expect_err("duplicate nodes must fail");

        assert_eq!(
            err,
            WorkflowValidationError::DuplicateNode {
                node_id: node_id("step")
            }
        );
    }

    #[test]
    fn duplicate_ports_on_one_node_are_rejected() {
        let err = NodeDefinition::new(node_id("step"), [port_id("value")], [port_id("value")])
            .expect_err("duplicate ports must fail");

        assert_eq!(
            err,
            WorkflowValidationError::DuplicatePort {
                node_id: node_id("step"),
                port_id: port_id("value")
            }
        );
    }

    #[test]
    fn edge_source_must_reference_existing_node() {
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("missing", "records"),
            endpoint("consumer", "records"),
        );

        let err = WorkflowGraph::new([consumer], [edge]).expect_err("missing source must fail");

        assert_eq!(
            err,
            WorkflowValidationError::UnknownNode {
                edge_index: 0,
                endpoint: EdgeEndpointRole::Source,
                node_id: node_id("missing")
            }
        );
    }

    #[test]
    fn edge_source_must_reference_output_port() {
        let producer = NodeDefinition::new(
            node_id("producer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid producer");
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("producer", "records"),
            endpoint("consumer", "records"),
        );

        let err = WorkflowGraph::new([producer, consumer], [edge])
            .expect_err("input source port must fail");

        assert_eq!(
            err,
            WorkflowValidationError::UnknownPort {
                edge_index: 0,
                endpoint: EdgeEndpointRole::Source,
                node_id: node_id("producer"),
                port_id: port_id("records"),
                expected: PortDirection::Output
            }
        );
    }

    #[test]
    fn edge_target_must_reference_input_port() {
        let producer = NodeDefinition::new(
            node_id("producer"),
            Vec::<PortId>::new(),
            [port_id("records")],
        )
        .expect("valid producer");
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            Vec::<PortId>::new(),
            [port_id("records")],
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("producer", "records"),
            endpoint("consumer", "records"),
        );

        let err = WorkflowGraph::new([producer, consumer], [edge])
            .expect_err("output target port must fail");

        assert_eq!(
            err,
            WorkflowValidationError::UnknownPort {
                edge_index: 0,
                endpoint: EdgeEndpointRole::Target,
                node_id: node_id("consumer"),
                port_id: port_id("records"),
                expected: PortDirection::Input
            }
        );
    }

    #[test]
    fn generated_acyclic_graphs_with_disconnected_nodes_validate() {
        fn property(graph: GeneratedValidGraph) -> bool {
            WorkflowGraph::new(graph.nodes, graph.edges).is_ok()
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(GeneratedValidGraph) -> bool);
    }

    #[test]
    fn generated_validation_cases_return_consistent_error_variants_without_panicking() {
        fn property(case: GeneratedValidationCase) -> bool {
            panic::catch_unwind(AssertUnwindSafe(|| validate_generated_case(&case)))
                .unwrap_or(false)
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(GeneratedValidationCase) -> bool);
    }

    #[test]
    fn generated_fan_out_topologies_validate() {
        fn property(count: SmallNodeCount) -> bool {
            let (nodes, edges) = generated_fan_out_graph(count.0);

            WorkflowGraph::new(nodes, edges).is_ok()
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(SmallNodeCount) -> bool);
    }

    #[test]
    fn generated_fan_in_topologies_validate() {
        fn property(count: SmallNodeCount) -> bool {
            let (nodes, edges) = generated_fan_in_graph(count.0);

            WorkflowGraph::new(nodes, edges).is_ok()
        }

        QuickCheck::new()
            .tests(128)
            .quickcheck(property as fn(SmallNodeCount) -> bool);
    }

    fn build_linear_workflow(node_names: &[String]) -> WorkflowDefinition {
        let mut nodes: Vec<NodeDefinition> = Vec::new();
        let mut edges: Vec<EdgeDefinition> = Vec::new();

        for (index, node_name) in node_names.iter().enumerate() {
            let mut input_ports: Vec<PortId> = Vec::new();
            let mut output_ports: Vec<PortId> = Vec::new();

            if index > 0 {
                input_ports.push(port_id("in"));
            }

            if index + 1 < node_names.len() {
                output_ports.push(port_id("out"));
            }

            nodes.push(
                NodeDefinition::new(node_id(node_name), input_ports, output_ports)
                    .expect("linear workflow nodes must be valid"),
            );
        }

        for edge in node_names.windows(2) {
            edges.push(EdgeDefinition::new(
                endpoint(&edge[0], "out"),
                endpoint(&edge[1], "in"),
            ));
        }

        WorkflowDefinition::from_parts(workflow_id("flow"), nodes, edges)
            .expect("linear workflow must be valid")
    }

    proptest! {
        #[test]
        fn linear_workflows_with_unique_valid_node_ids_validate(
            node_names in hash_set(valid_identifier_strategy(), 1..6)
        ) {
            let mut node_names: Vec<String> = node_names.into_iter().collect();
            node_names.sort();

            let workflow: WorkflowDefinition = build_linear_workflow(&node_names);

            prop_assert_eq!(workflow.nodes().len(), node_names.len());
            prop_assert_eq!(workflow.edges().len(), node_names.len().saturating_sub(1));
        }
    }
}
