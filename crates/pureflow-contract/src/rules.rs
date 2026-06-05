//! Rule set validation against workflow topology.
//!
//! ## Fragment: rules-contract-boundary
//!
//! A [`RuleSet`] is authored independently of the workflow graph it runs in:
//! its actions name output ports and its conditions name source nodes and
//! arrival ports. Those references are only meaningful relative to the node
//! that hosts the rule set and the surrounding graph. This module performs that
//! cross-check at load time — before any execution — so a misrouted `Route`,
//! a missing dead-letter port, or a provenance condition that can never fire is
//! reported as a typed diagnostic rather than discovered at runtime.
//!
//! Validation here is purely structural. It does not evaluate conditions or
//! execute actions; it only verifies that every port and node a rule set refers
//! to exists, has the right direction, and is reachable.

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

use pureflow_rules::{Condition, RuleAction, RuleSet};
use pureflow_types::{NodeId, PortId};
use pureflow_workflow::WorkflowDefinition;

/// Conventional output port a `RuleNode` routes `DeadLetter` actions to.
///
/// Mirrors the port the native executor sends to in
/// `pureflow_rules::RuleNode`; a rule set that uses `DeadLetter` is only valid
/// when its host node declares this output port.
pub const DEAD_LETTER_PORT: &str = "dead_letter";

/// Where in a rule set a terminal action that failed validation appeared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionSite {
    /// The action belongs to the named rule.
    Rule {
        /// Identifier of the offending rule.
        rule_id: String,
    },
    /// The action is the rule set's `default_action`.
    DefaultAction,
}

impl fmt::Display for ActionSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rule { rule_id } => write!(f, "rule `{rule_id}`"),
            Self::DefaultAction => f.write_str("default action"),
        }
    }
}

/// A typed diagnostic for a rule set that does not line up with its host node or
/// the surrounding workflow graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleContractViolation {
    /// The node hosting the rule set is not present in the workflow.
    UnknownRuleNode {
        /// Rule set whose host node is missing.
        rule_set_id: String,
        /// Missing host node identifier.
        node_id: NodeId,
    },
    /// A `Route` action targets a port the host node does not declare as output.
    UnknownRoutePort {
        /// Rule set that produced the action.
        rule_set_id: String,
        /// Where the action appeared.
        site: ActionSite,
        /// Host node hosting the rule set.
        node_id: NodeId,
        /// Output port that does not exist on the host node.
        port_id: PortId,
    },
    /// A `DeadLetter` action is used but the host node declares no dead-letter
    /// output port.
    MissingDeadLetterPort {
        /// Rule set that produced the action.
        rule_set_id: String,
        /// Where the action appeared.
        site: ActionSite,
        /// Host node hosting the rule set.
        node_id: NodeId,
    },
    /// A `SourceNode` condition references a node absent from the workflow.
    UnknownSourceNode {
        /// Rule set that produced the condition.
        rule_set_id: String,
        /// Rule that carries the condition.
        rule_id: String,
        /// Source node that does not exist in the graph.
        node_id: NodeId,
    },
    /// A `SourceNode` condition references a real node that can never deliver a
    /// packet to the host node (no directed path).
    UnreachableSourceNode {
        /// Rule set that produced the condition.
        rule_set_id: String,
        /// Rule that carries the condition.
        rule_id: String,
        /// Host node hosting the rule set.
        node_id: NodeId,
        /// Source node with no path to the host node.
        source_node_id: NodeId,
    },
    /// An `ArrivedOnPort` condition references a port the host node does not
    /// declare as input.
    UnknownArrivalPort {
        /// Rule set that produced the condition.
        rule_set_id: String,
        /// Rule that carries the condition.
        rule_id: String,
        /// Host node hosting the rule set.
        node_id: NodeId,
        /// Input port that does not exist on the host node.
        port_id: PortId,
    },
}

impl fmt::Display for RuleContractViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRuleNode {
                rule_set_id,
                node_id,
            } => write!(
                f,
                "rule set `{rule_set_id}` is hosted by node `{node_id}` which is not in the workflow"
            ),
            Self::UnknownRoutePort {
                rule_set_id,
                site,
                node_id,
                port_id,
            } => write!(
                f,
                "rule set `{rule_set_id}` {site} routes to output port `{port_id}` not declared on node `{node_id}`"
            ),
            Self::MissingDeadLetterPort {
                rule_set_id,
                site,
                node_id,
            } => write!(
                f,
                "rule set `{rule_set_id}` {site} dead-letters but node `{node_id}` declares no `{DEAD_LETTER_PORT}` output port"
            ),
            Self::UnknownSourceNode {
                rule_set_id,
                rule_id,
                node_id,
            } => write!(
                f,
                "rule set `{rule_set_id}` rule `{rule_id}` references unknown source node `{node_id}`"
            ),
            Self::UnreachableSourceNode {
                rule_set_id,
                rule_id,
                node_id,
                source_node_id,
            } => write!(
                f,
                "rule set `{rule_set_id}` rule `{rule_id}` references source node `{source_node_id}` with no path to host node `{node_id}`"
            ),
            Self::UnknownArrivalPort {
                rule_set_id,
                rule_id,
                node_id,
                port_id,
            } => write!(
                f,
                "rule set `{rule_set_id}` rule `{rule_id}` references arrival port `{port_id}` not declared as input on node `{node_id}`"
            ),
        }
    }
}

impl Error for RuleContractViolation {}

/// Validate a rule set against its host node and the workflow graph, returning
/// the first violation found.
///
/// This is a convenience wrapper over [`rule_set_violations`] for callers that
/// only need pass/fail. Use [`rule_set_violations`] to collect every diagnostic.
///
/// # Errors
///
/// Returns the first [`RuleContractViolation`] detected, or `Ok(())` if the rule
/// set is consistent with the graph.
pub fn validate_rule_set(
    workflow: &WorkflowDefinition,
    rule_node_id: &NodeId,
    rule_set: &RuleSet,
) -> Result<(), RuleContractViolation> {
    match rule_set_violations(workflow, rule_node_id, rule_set)
        .into_iter()
        .next()
    {
        Some(violation) => Err(violation),
        None => Ok(()),
    }
}

/// Collect every [`RuleContractViolation`] for a rule set hosted by `rule_node_id`.
///
/// Returns an empty vector when the rule set is consistent with the workflow.
/// Checks performed:
///
/// - Every `Route(port)` (in any rule or the default action) targets a declared
///   output port of the host node.
/// - Every `DeadLetter` action requires a declared [`DEAD_LETTER_PORT`] output.
/// - Every `SourceNode(node)` condition references a graph node with a directed
///   path to the host node.
/// - Every `ArrivedOnPort(port)` condition references a declared input port of
///   the host node.
///
/// If the host node itself is missing, a single [`RuleContractViolation::UnknownRuleNode`]
/// is returned, since port-level checks cannot be performed without it.
#[must_use]
pub fn rule_set_violations(
    workflow: &WorkflowDefinition,
    rule_node_id: &NodeId,
    rule_set: &RuleSet,
) -> Vec<RuleContractViolation> {
    let mut violations: Vec<RuleContractViolation> = Vec::new();

    let Some(host) = workflow
        .nodes()
        .iter()
        .find(|node: &&pureflow_workflow::NodeDefinition| node.id() == rule_node_id)
    else {
        violations.push(RuleContractViolation::UnknownRuleNode {
            rule_set_id: rule_set.id.clone(),
            node_id: rule_node_id.clone(),
        });
        return violations;
    };

    let output_ports: BTreeSet<&PortId> = host.output_ports().iter().collect();
    let input_ports: BTreeSet<&PortId> = host.input_ports().iter().collect();
    let has_dead_letter: bool = host
        .output_ports()
        .iter()
        .any(|port: &PortId| port.as_str() == DEAD_LETTER_PORT);

    // Validate every action: each rule's action plus the default action.
    for rule in &rule_set.rules {
        validate_action(
            rule_set,
            &ActionSite::Rule {
                rule_id: rule.id.clone(),
            },
            rule_node_id,
            &rule.action,
            &output_ports,
            has_dead_letter,
            &mut violations,
        );
    }
    validate_action(
        rule_set,
        &ActionSite::DefaultAction,
        rule_node_id,
        &rule_set.default_action,
        &output_ports,
        has_dead_letter,
        &mut violations,
    );

    // Validate provenance conditions: collect SourceNode / ArrivedOnPort
    // references (recursing through combinators) for each rule.
    for rule in &rule_set.rules {
        let mut refs = ConditionRefs::default();
        collect_condition_refs(&rule.condition, &mut refs);

        for source_node_id in refs.source_nodes {
            if !node_exists(workflow, source_node_id) {
                violations.push(RuleContractViolation::UnknownSourceNode {
                    rule_set_id: rule_set.id.clone(),
                    rule_id: rule.id.clone(),
                    node_id: source_node_id.clone(),
                });
            } else if !has_path(workflow, source_node_id, rule_node_id) {
                violations.push(RuleContractViolation::UnreachableSourceNode {
                    rule_set_id: rule_set.id.clone(),
                    rule_id: rule.id.clone(),
                    node_id: rule_node_id.clone(),
                    source_node_id: source_node_id.clone(),
                });
            }
        }

        for arrival_port in refs.arrival_ports {
            if !input_ports.contains(arrival_port) {
                violations.push(RuleContractViolation::UnknownArrivalPort {
                    rule_set_id: rule_set.id.clone(),
                    rule_id: rule.id.clone(),
                    node_id: rule_node_id.clone(),
                    port_id: arrival_port.clone(),
                });
            }
        }
    }

    violations
}

#[allow(clippy::too_many_arguments)]
fn validate_action(
    rule_set: &RuleSet,
    site: &ActionSite,
    rule_node_id: &NodeId,
    action: &RuleAction,
    output_ports: &BTreeSet<&PortId>,
    has_dead_letter: bool,
    violations: &mut Vec<RuleContractViolation>,
) {
    match action {
        RuleAction::Route(port_id) => {
            if !output_ports.contains(port_id) {
                violations.push(RuleContractViolation::UnknownRoutePort {
                    rule_set_id: rule_set.id.clone(),
                    site: site.clone(),
                    node_id: rule_node_id.clone(),
                    port_id: port_id.clone(),
                });
            }
        }
        RuleAction::DeadLetter(_) => {
            if !has_dead_letter {
                violations.push(RuleContractViolation::MissingDeadLetterPort {
                    rule_set_id: rule_set.id.clone(),
                    site: site.clone(),
                    node_id: rule_node_id.clone(),
                });
            }
        }
        // Drop / Tag / Halt make no port or graph reference.
        RuleAction::Drop | RuleAction::Tag { .. } | RuleAction::Halt(_) => {}
    }
}

/// Provenance references gathered from a condition tree.
#[derive(Default)]
struct ConditionRefs<'a> {
    source_nodes: Vec<&'a NodeId>,
    arrival_ports: Vec<&'a PortId>,
}

fn collect_condition_refs<'a>(condition: &'a Condition, refs: &mut ConditionRefs<'a>) {
    match condition {
        Condition::SourceNode { node_id } => refs.source_nodes.push(node_id),
        Condition::ArrivedOnPort { port_id } => refs.arrival_ports.push(port_id),
        Condition::And(children) | Condition::Or(children) => {
            for child in children {
                collect_condition_refs(child, refs);
            }
        }
        Condition::Not(inner) => collect_condition_refs(inner, refs),
        // All remaining conditions evaluate against payload, tags, hop count,
        // or execution context — no graph or port reference to validate.
        _ => {}
    }
}

fn node_exists(workflow: &WorkflowDefinition, node_id: &NodeId) -> bool {
    workflow
        .nodes()
        .iter()
        .any(|node: &pureflow_workflow::NodeDefinition| node.id() == node_id)
}

/// Return whether a directed path exists from `from` to `to` through workflow edges.
///
/// Used to confirm a packet produced by a `SourceNode` could actually reach the
/// host rule node. The workflow is acyclic, so a node never reaches itself.
fn has_path(workflow: &WorkflowDefinition, from: &NodeId, to: &NodeId) -> bool {
    let mut queue: VecDeque<&NodeId> = VecDeque::new();
    let mut visited: BTreeSet<&NodeId> = BTreeSet::new();
    queue.push_back(from);
    visited.insert(from);

    while let Some(current) = queue.pop_front() {
        for edge in workflow.edges() {
            if edge.source().node_id() != current {
                continue;
            }
            let next: &NodeId = edge.target().node_id();
            if next == to {
                return true;
            }
            if visited.insert(next) {
                queue.push_back(next);
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_rules::{EvaluationStrategy, Rule, RuleSet};
    use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, node_id, port_id};

    fn rule(id: &str, condition: Condition, action: RuleAction) -> Rule {
        Rule::new(id, condition, action, 10, "test rule").expect("valid rule")
    }

    fn rule_set(id: &str, rules: Vec<Rule>, default_action: RuleAction) -> RuleSet {
        RuleSet::new(
            id,
            EvaluationStrategy::FirstMatch,
            rules,
            default_action,
            false,
        )
        .expect("valid rule set")
    }

    /// Workflow: source -> router -> sink, where `router` hosts the rule set and
    /// declares input `in` and outputs `out` plus `dead_letter`.
    fn router_workflow() -> WorkflowDefinition {
        WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(
                NodeBuilder::new("router")
                    .input("in")
                    .output("out")
                    .output("dead_letter")
                    .build(),
            )
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "router", "in")
            .edge("router", "out", "sink", "in")
            .build()
    }

    #[test]
    fn valid_rule_set_has_no_violations() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![
                rule(
                    "route-known",
                    Condition::SourceNode {
                        node_id: node_id("source"),
                    },
                    RuleAction::Route(port_id("out")),
                ),
                rule(
                    "arrived-known",
                    Condition::ArrivedOnPort {
                        port_id: port_id("in"),
                    },
                    RuleAction::DeadLetter("bad".to_owned()),
                ),
            ],
            RuleAction::Drop,
        );

        assert!(rule_set_violations(&workflow, &node_id("router"), &rs).is_empty());
        assert!(validate_rule_set(&workflow, &node_id("router"), &rs).is_ok());
    }

    #[test]
    fn route_to_undeclared_output_port_is_a_violation() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "route-missing",
                Condition::Always,
                RuleAction::Route(port_id("nowhere")),
            )],
            RuleAction::Drop,
        );

        let err = validate_rule_set(&workflow, &node_id("router"), &rs)
            .expect_err("route to undeclared port must fail");

        assert_eq!(
            err,
            RuleContractViolation::UnknownRoutePort {
                rule_set_id: "router-rules".to_owned(),
                site: ActionSite::Rule {
                    rule_id: "route-missing".to_owned()
                },
                node_id: node_id("router"),
                port_id: port_id("nowhere"),
            }
        );
    }

    #[test]
    fn default_action_route_is_validated() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule("noop", Condition::Always, RuleAction::Drop)],
            RuleAction::Route(port_id("nowhere")),
        );

        let err = validate_rule_set(&workflow, &node_id("router"), &rs)
            .expect_err("default route to undeclared port must fail");

        assert_eq!(
            err,
            RuleContractViolation::UnknownRoutePort {
                rule_set_id: "router-rules".to_owned(),
                site: ActionSite::DefaultAction,
                node_id: node_id("router"),
                port_id: port_id("nowhere"),
            }
        );
    }

    #[test]
    fn dead_letter_without_dead_letter_port_is_a_violation() {
        // Workflow whose router declares no `dead_letter` output.
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("router").input("in").output("out").build())
            .edge("source", "out", "router", "in")
            .build();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "to-dead-letter",
                Condition::Always,
                RuleAction::DeadLetter("oops".to_owned()),
            )],
            RuleAction::Drop,
        );

        let err = validate_rule_set(&workflow, &node_id("router"), &rs)
            .expect_err("dead-letter without port must fail");

        assert_eq!(
            err,
            RuleContractViolation::MissingDeadLetterPort {
                rule_set_id: "router-rules".to_owned(),
                site: ActionSite::Rule {
                    rule_id: "to-dead-letter".to_owned()
                },
                node_id: node_id("router"),
            }
        );
    }

    #[test]
    fn dead_letter_with_declared_port_is_valid() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "to-dead-letter",
                Condition::Always,
                RuleAction::DeadLetter("ok".to_owned()),
            )],
            RuleAction::Drop,
        );

        assert!(rule_set_violations(&workflow, &node_id("router"), &rs).is_empty());
    }

    #[test]
    fn source_node_referencing_unknown_node_is_a_violation() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "from-ghost",
                Condition::SourceNode {
                    node_id: node_id("ghost"),
                },
                RuleAction::Route(port_id("out")),
            )],
            RuleAction::Drop,
        );

        let err = validate_rule_set(&workflow, &node_id("router"), &rs)
            .expect_err("unknown source node must fail");

        assert_eq!(
            err,
            RuleContractViolation::UnknownSourceNode {
                rule_set_id: "router-rules".to_owned(),
                rule_id: "from-ghost".to_owned(),
                node_id: node_id("ghost"),
            }
        );
    }

    #[test]
    fn source_node_with_no_path_to_host_is_a_violation() {
        // `sink` exists but is downstream of `router`, so no packet from `sink`
        // can ever arrive at `router`.
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "from-sink",
                Condition::SourceNode {
                    node_id: node_id("sink"),
                },
                RuleAction::Route(port_id("out")),
            )],
            RuleAction::Drop,
        );

        let err = validate_rule_set(&workflow, &node_id("router"), &rs)
            .expect_err("unreachable source node must fail");

        assert_eq!(
            err,
            RuleContractViolation::UnreachableSourceNode {
                rule_set_id: "router-rules".to_owned(),
                rule_id: "from-sink".to_owned(),
                node_id: node_id("router"),
                source_node_id: node_id("sink"),
            }
        );
    }

    #[test]
    fn source_node_reachable_through_multiple_hops_is_valid() {
        // a -> b -> router; `a` is two hops upstream but still reachable.
        let workflow = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("a").output("out").build())
            .node(NodeBuilder::new("b").input("in").output("out").build())
            .node(NodeBuilder::new("router").input("in").output("out").build())
            .edge("a", "out", "b", "in")
            .edge("b", "out", "router", "in")
            .build();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "from-a",
                Condition::SourceNode {
                    node_id: node_id("a"),
                },
                RuleAction::Route(port_id("out")),
            )],
            RuleAction::Drop,
        );

        assert!(rule_set_violations(&workflow, &node_id("router"), &rs).is_empty());
    }

    #[test]
    fn arrived_on_undeclared_input_port_is_a_violation() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "arrived-wrong",
                Condition::ArrivedOnPort {
                    port_id: port_id("not-an-input"),
                },
                RuleAction::Route(port_id("out")),
            )],
            RuleAction::Drop,
        );

        let err = validate_rule_set(&workflow, &node_id("router"), &rs)
            .expect_err("unknown arrival port must fail");

        assert_eq!(
            err,
            RuleContractViolation::UnknownArrivalPort {
                rule_set_id: "router-rules".to_owned(),
                rule_id: "arrived-wrong".to_owned(),
                node_id: node_id("router"),
                port_id: port_id("not-an-input"),
            }
        );
    }

    #[test]
    fn conditions_inside_combinators_are_validated() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![rule(
                "nested",
                Condition::And(vec![
                    Condition::Not(Box::new(Condition::ArrivedOnPort {
                        port_id: port_id("ghost-port"),
                    })),
                    Condition::Or(vec![Condition::SourceNode {
                        node_id: node_id("ghost-node"),
                    }]),
                ]),
                RuleAction::Route(port_id("out")),
            )],
            RuleAction::Drop,
        );

        let violations = rule_set_violations(&workflow, &node_id("router"), &rs);

        assert!(
            violations.contains(&RuleContractViolation::UnknownArrivalPort {
                rule_set_id: "router-rules".to_owned(),
                rule_id: "nested".to_owned(),
                node_id: node_id("router"),
                port_id: port_id("ghost-port"),
            })
        );
        assert!(
            violations.contains(&RuleContractViolation::UnknownSourceNode {
                rule_set_id: "router-rules".to_owned(),
                rule_id: "nested".to_owned(),
                node_id: node_id("ghost-node"),
            })
        );
    }

    #[test]
    fn unknown_host_node_reports_single_violation() {
        let workflow = router_workflow();
        let rs = rule_set(
            "orphan-rules",
            vec![rule("noop", Condition::Always, RuleAction::Drop)],
            RuleAction::Drop,
        );

        let violations = rule_set_violations(&workflow, &node_id("missing"), &rs);

        assert_eq!(
            violations,
            vec![RuleContractViolation::UnknownRuleNode {
                rule_set_id: "orphan-rules".to_owned(),
                node_id: node_id("missing"),
            }]
        );
    }

    #[test]
    fn multiple_violations_are_all_collected() {
        let workflow = router_workflow();
        let rs = rule_set(
            "router-rules",
            vec![
                rule(
                    "bad-route",
                    Condition::Always,
                    RuleAction::Route(port_id("nope")),
                ),
                rule(
                    "bad-source",
                    Condition::SourceNode {
                        node_id: node_id("ghost"),
                    },
                    RuleAction::Route(port_id("out")),
                ),
            ],
            RuleAction::Drop,
        );

        let violations = rule_set_violations(&workflow, &node_id("router"), &rs);

        assert_eq!(violations.len(), 2);
    }
}
