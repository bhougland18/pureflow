//! Introspection projection for rule sets.

use pureflow_core::RuleEvalStrategy;
use pureflow_rules::{Condition, ConditionSurface, EvaluationStrategy, RuleAction, RuleSet};
use pureflow_types::PortId;

/// Introspectable view of a rule set, produced without executing any rules.
///
/// Exposes the full rule definition, which ports each rule can target, which
/// condition surfaces each rule draws from, and unreachable-rule analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSetIntrospection {
    /// Identifier of the rule set.
    pub id: String,
    /// Evaluation strategy.
    pub strategy: RuleEvalStrategy,
    /// Per-rule introspection views in evaluation order (ascending priority).
    pub rules: Vec<RuleIntrospection>,
    /// Action applied when no rule matches.
    pub default_action: RuleActionSummary,
    /// Whether condition tracing is enabled on this rule set.
    pub trace_conditions: bool,
    /// Rules identified as unreachable at projection time.
    ///
    /// Detection is conservative: only `Never` conditions, `Always` shadows,
    /// and exact `FieldEq` subsumptions are flagged. Combinatorially complex
    /// cases are not analysed to avoid false positives.
    pub unreachable_rules: Vec<UnreachableRule>,
}

/// Introspectable view of one rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleIntrospection {
    /// Rule identifier.
    pub id: String,
    /// Evaluation order (lower value = higher priority).
    pub priority: u32,
    /// Human-readable description.
    pub description: String,
    /// Terminal or non-terminal action this rule takes.
    pub action: RuleActionSummary,
    /// Which packet surfaces this rule's condition evaluates against.
    pub condition_surfaces: ConditionSurfaceSummary,
    /// Output ports this rule can send to (from `Route` actions).
    pub target_ports: Vec<PortId>,
}

/// Summary of a rule action for introspection purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleActionSummary {
    /// Routes the packet to the named output port.
    Route(PortId),
    /// Drops the packet.
    Drop,
    /// Routes to the dead-letter port.
    DeadLetter,
    /// Applies a tag and continues evaluation (non-terminal).
    Tag {
        /// The tag key.
        key: String,
    },
    /// Halts the node with an error.
    Halt,
}

/// Which packet surfaces a condition evaluates against.
///
/// A rule may draw from multiple surfaces (e.g. `And` combining a payload
/// field check with a tag check). All surfaces present in the condition tree
/// are reflected here.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConditionSurfaceSummary {
    /// Condition evaluates against packet payload fields.
    pub payload: bool,
    /// Condition evaluates against upstream-applied tags.
    pub tag: bool,
    /// Condition evaluates against packet provenance (source node, port, hops).
    pub provenance: bool,
    /// Condition evaluates against execution context (workflow, metadata).
    pub execution_context: bool,
    /// Condition is a constant (`Always` or `Never`).
    pub constant: bool,
}

impl ConditionSurfaceSummary {
    /// Return `true` if this condition only uses constant values and never
    /// depends on packet content.
    #[must_use]
    pub const fn is_constant_only(&self) -> bool {
        self.constant && !self.payload && !self.tag && !self.provenance && !self.execution_context
    }
}

/// A rule that can never fire, identified at projection time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableRule {
    /// Identifier of the unreachable rule.
    pub rule_id: String,
    /// Why this rule can never fire.
    pub reason: UnreachableReason,
}

/// Why a rule was identified as unreachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnreachableReason {
    /// The rule's condition is `Never` — it can never be true.
    ConditionIsNever,
    /// A higher-priority rule has an `Always` condition that will always fire
    /// first in `FirstMatch` evaluation, preventing this rule from being reached.
    ShadowedByAlways {
        /// Identifier of the always-matching rule that shadows this one.
        shadowing_rule_id: String,
    },
    /// A higher-priority rule has the exact same `FieldEq` condition and the
    /// same path and value, making this rule a duplicate that can never fire.
    ExactFieldEqSubsumed {
        /// Identifier of the higher-priority rule with the identical condition.
        shadowing_rule_id: String,
    },
}

/// Build a `RuleSetIntrospection` view from a validated rule set.
#[must_use]
pub fn introspect_rule_set(rule_set: &RuleSet) -> RuleSetIntrospection {
    let rules: Vec<RuleIntrospection> = rule_set
        .rules
        .iter()
        .map(|rule| {
            let surfaces = collect_surfaces(&rule.condition);
            let target_ports = collect_target_ports(&rule.action);
            RuleIntrospection {
                id: rule.id.clone(),
                priority: rule.priority,
                description: rule.description.clone(),
                action: action_summary(&rule.action),
                condition_surfaces: surfaces,
                target_ports,
            }
        })
        .collect();

    let unreachable_rules = detect_unreachable(rule_set);

    RuleSetIntrospection {
        id: rule_set.id.clone(),
        strategy: strategy_to_eval(rule_set.strategy),
        rules,
        default_action: action_summary(&rule_set.default_action),
        trace_conditions: rule_set.trace_conditions,
        unreachable_rules,
    }
}

/// Collect the condition surfaces used anywhere in a condition tree.
fn collect_surfaces(condition: &Condition) -> ConditionSurfaceSummary {
    let mut summary = ConditionSurfaceSummary::default();
    collect_surfaces_inner(condition, &mut summary);
    summary
}

fn collect_surfaces_inner(condition: &Condition, out: &mut ConditionSurfaceSummary) {
    match condition.surface() {
        ConditionSurface::Payload => out.payload = true,
        ConditionSurface::Tag => out.tag = true,
        ConditionSurface::Provenance => out.provenance = true,
        ConditionSurface::ExecutionContext => out.execution_context = true,
        ConditionSurface::Constant => out.constant = true,
        ConditionSurface::Combinator => {}
    }
    // Recurse into combinators.
    match condition {
        Condition::And(inner) | Condition::Or(inner) => {
            for c in inner {
                collect_surfaces_inner(c, out);
            }
        }
        Condition::Not(inner) => collect_surfaces_inner(inner, out),
        _ => {}
    }
}

/// Collect output ports targeted by an action.
fn collect_target_ports(action: &RuleAction) -> Vec<PortId> {
    match action {
        RuleAction::Route(port) => vec![port.clone()],
        _ => vec![],
    }
}

fn action_summary(action: &RuleAction) -> RuleActionSummary {
    match action {
        RuleAction::Route(port) => RuleActionSummary::Route(port.clone()),
        RuleAction::Drop => RuleActionSummary::Drop,
        RuleAction::DeadLetter(_) => RuleActionSummary::DeadLetter,
        RuleAction::Tag { key, .. } => RuleActionSummary::Tag { key: key.clone() },
        RuleAction::Halt(_) => RuleActionSummary::Halt,
    }
}

const fn strategy_to_eval(s: EvaluationStrategy) -> RuleEvalStrategy {
    match s {
        EvaluationStrategy::FirstMatch => RuleEvalStrategy::FirstMatch,
        EvaluationStrategy::AllMatches => RuleEvalStrategy::AllMatches,
        EvaluationStrategy::Score => RuleEvalStrategy::Score,
    }
}

/// Detect unreachable rules using conservative analysis.
///
/// Only flags:
/// - `Never` conditions (always false).
/// - Rules shadowed by a higher-priority `Always` condition in `FirstMatch`.
/// - Rules with exact duplicate `FieldEq(path, value)` conditions shadowed
///   by a higher-priority rule in `FirstMatch`.
fn detect_unreachable(rule_set: &RuleSet) -> Vec<UnreachableRule> {
    let mut unreachable = Vec::new();

    for rule in &rule_set.rules {
        if is_never(&rule.condition) {
            unreachable.push(UnreachableRule {
                rule_id: rule.id.clone(),
                reason: UnreachableReason::ConditionIsNever,
            });
        }
    }

    if rule_set.strategy == EvaluationStrategy::FirstMatch {
        // Find the first Always rule; everything after it is shadowed.
        let mut always_rule_id: Option<&str> = None;
        for rule in &rule_set.rules {
            if let Some(shadower) = always_rule_id
                && !is_never(&rule.condition)
            {
                // Don't double-report a Never rule already caught above.
                unreachable.push(UnreachableRule {
                    rule_id: rule.id.clone(),
                    reason: UnreachableReason::ShadowedByAlways {
                        shadowing_rule_id: shadower.to_owned(),
                    },
                });
            }
            if matches!(&rule.condition, Condition::Always) && always_rule_id.is_none() {
                always_rule_id = Some(rule.id.as_str());
            }
        }

        // Exact FieldEq subsumption: scan for duplicate conditions.
        for (i, rule) in rule_set.rules.iter().enumerate() {
            if let Condition::FieldEq {
                path: p1,
                value: v1,
            } = &rule.condition
            {
                // Look at all later (lower-priority) rules.
                for later in &rule_set.rules[i + 1..] {
                    if let Condition::FieldEq {
                        path: p2,
                        value: v2,
                    } = &later.condition
                        && p1 == p2
                        && v1 == v2
                        && !unreachable.iter().any(|u| u.rule_id == later.id)
                    {
                        unreachable.push(UnreachableRule {
                            rule_id: later.id.clone(),
                            reason: UnreachableReason::ExactFieldEqSubsumed {
                                shadowing_rule_id: rule.id.clone(),
                            },
                        });
                    }
                }
            }
        }
    }

    unreachable
}

const fn is_never(condition: &Condition) -> bool {
    matches!(condition, Condition::Never)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_rules::{
        Condition, EvaluationStrategy, FieldPath, Rule, RuleAction, RuleSet, ScalarValue,
    };
    use pureflow_types::PortId;

    fn port(s: &str) -> PortId {
        PortId::new(s).unwrap()
    }
    fn field(s: &str) -> FieldPath {
        FieldPath::new(s).unwrap()
    }

    fn rule(id: &str, cond: Condition, action: RuleAction, pri: u32) -> Rule {
        Rule::new(id, cond, action, pri, "test rule").unwrap()
    }

    fn first_match_set(rules: Vec<Rule>) -> RuleSet {
        RuleSet::new(
            "test",
            EvaluationStrategy::FirstMatch,
            rules,
            RuleAction::Drop,
            false,
        )
        .unwrap()
    }

    #[test]
    fn introspects_rule_count_and_strategy() {
        let rules = vec![
            rule("r1", Condition::Always, RuleAction::Route(port("out")), 10),
            rule("r2", Condition::Always, RuleAction::Route(port("out")), 20),
        ];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert_eq!(view.id, "test");
        assert_eq!(view.rules.len(), 2);
        assert_eq!(view.strategy, RuleEvalStrategy::FirstMatch);
    }

    #[test]
    fn never_condition_is_flagged_unreachable() {
        let rules = vec![rule("dead", Condition::Never, RuleAction::Drop, 10)];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert_eq!(view.unreachable_rules.len(), 1);
        assert_eq!(view.unreachable_rules[0].rule_id, "dead");
        assert_eq!(
            view.unreachable_rules[0].reason,
            UnreachableReason::ConditionIsNever
        );
    }

    #[test]
    fn always_shadows_lower_priority_rules_in_first_match() {
        let rules = vec![
            rule(
                "catch-all",
                Condition::Always,
                RuleAction::Route(port("out")),
                10,
            ),
            rule(
                "unreachable",
                Condition::FieldGte {
                    path: field("amount"),
                    value: ScalarValue::Integer(100),
                },
                RuleAction::Route(port("other")),
                20,
            ),
        ];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert_eq!(view.unreachable_rules.len(), 1);
        assert_eq!(view.unreachable_rules[0].rule_id, "unreachable");
        assert!(matches!(
            view.unreachable_rules[0].reason,
            UnreachableReason::ShadowedByAlways { ref shadowing_rule_id }
            if shadowing_rule_id == "catch-all"
        ));
    }

    #[test]
    fn always_does_not_shadow_in_all_matches() {
        let rules = vec![
            rule(
                "t1",
                Condition::Always,
                RuleAction::Tag {
                    key: "a".into(),
                    value: ScalarValue::Boolean(true),
                },
                10,
            ),
            rule(
                "t2",
                Condition::Always,
                RuleAction::Tag {
                    key: "b".into(),
                    value: ScalarValue::Boolean(true),
                },
                20,
            ),
        ];
        let rs = RuleSet::new(
            "test",
            EvaluationStrategy::AllMatches,
            rules,
            RuleAction::Drop,
            false,
        )
        .unwrap();
        let view = introspect_rule_set(&rs);
        // AllMatches evaluates all rules — no shadowing.
        assert!(
            view.unreachable_rules
                .iter()
                .all(|u| u.reason != UnreachableReason::ConditionIsNever)
        );
    }

    #[test]
    fn exact_field_eq_subsumption_is_flagged() {
        let rules = vec![
            rule(
                "first",
                Condition::FieldEq {
                    path: field("status"),
                    value: ScalarValue::String("approved".into()),
                },
                RuleAction::Route(port("fast")),
                10,
            ),
            rule(
                "duplicate",
                Condition::FieldEq {
                    path: field("status"),
                    value: ScalarValue::String("approved".into()),
                },
                RuleAction::Route(port("slow")),
                20,
            ),
        ];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert_eq!(view.unreachable_rules.len(), 1);
        assert_eq!(view.unreachable_rules[0].rule_id, "duplicate");
        assert!(matches!(
            view.unreachable_rules[0].reason,
            UnreachableReason::ExactFieldEqSubsumed { ref shadowing_rule_id }
            if shadowing_rule_id == "first"
        ));
    }

    #[test]
    fn different_field_values_are_not_subsumed() {
        let rules = vec![
            rule(
                "approved",
                Condition::FieldEq {
                    path: field("status"),
                    value: ScalarValue::String("approved".into()),
                },
                RuleAction::Route(port("approved-out")),
                10,
            ),
            rule(
                "declined",
                Condition::FieldEq {
                    path: field("status"),
                    value: ScalarValue::String("declined".into()),
                },
                RuleAction::Route(port("declined-out")),
                20,
            ),
        ];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert!(view.unreachable_rules.is_empty());
    }

    #[test]
    fn condition_surface_summary_payload_condition() {
        let cond = Condition::FieldGte {
            path: field("amount"),
            value: ScalarValue::Integer(100),
        };
        let summary = collect_surfaces(&cond);
        assert!(summary.payload);
        assert!(!summary.tag);
        assert!(!summary.provenance);
    }

    #[test]
    fn condition_surface_summary_mixed_and_condition() {
        let cond = Condition::And(vec![
            Condition::FieldGte {
                path: field("amount"),
                value: ScalarValue::Integer(100),
            },
            Condition::TagEq {
                key: "priority".into(),
                value: ScalarValue::String("high".into()),
            },
        ]);
        let summary = collect_surfaces(&cond);
        assert!(summary.payload);
        assert!(summary.tag);
        assert!(!summary.provenance);
    }

    #[test]
    fn rule_introspection_target_ports_for_route() {
        let rules = vec![rule(
            "r",
            Condition::Always,
            RuleAction::Route(port("out")),
            10,
        )];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert_eq!(view.rules[0].target_ports, vec![port("out")]);
    }

    #[test]
    fn rule_introspection_no_target_ports_for_drop() {
        let rules = vec![rule("d", Condition::Always, RuleAction::Drop, 10)];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        assert!(view.rules[0].target_ports.is_empty());
    }

    #[test]
    fn never_shadowed_by_always_are_not_double_reported() {
        let rules = vec![
            rule(
                "catch-all",
                Condition::Always,
                RuleAction::Route(port("out")),
                10,
            ),
            rule("never-rule", Condition::Never, RuleAction::Drop, 20),
        ];
        let rs = first_match_set(rules);
        let view = introspect_rule_set(&rs);
        // never-rule is shadowed by catch-all BUT was already flagged as Never.
        // Only one report per rule.
        let count = view
            .unreachable_rules
            .iter()
            .filter(|u| u.rule_id == "never-rule")
            .count();
        assert_eq!(count, 1, "never-rule should only be reported once");
        assert_eq!(
            view.unreachable_rules[0].reason,
            UnreachableReason::ConditionIsNever
        );
    }
}
