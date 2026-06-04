//! Rule, RuleSet, EvaluationStrategy, and RuleDecision types.

use crate::action::RuleAction;
use crate::condition::{Condition, ScalarValue};
use crate::error::RuleError;

/// How a rule set evaluates its rules against a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EvaluationStrategy {
    /// Evaluate rules in priority order; stop at the first matching rule.
    FirstMatch,
    /// Evaluate all rules; collect all `Tag` applications, then apply the
    /// `default_action`. Only `Tag` actions are permitted in `AllMatches` rule
    /// sets — terminal actions are rejected at construction time.
    AllMatches,
    /// Evaluate all rules; select the action from the highest-scoring match.
    Score,
}

/// One named, serializable predicate over a packet and its Pureflow context.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Rule {
    /// Scoped identifier for this rule, e.g. `"filter.high-value"`.
    pub id: String,
    /// Predicate evaluated against the packet.
    pub condition: Condition,
    /// Action taken when the condition matches.
    pub action: RuleAction,
    /// Evaluation order within the rule set (lower value = higher priority).
    pub priority: u32,
    /// Human and AI-readable description of what this rule does.
    pub description: String,
}

impl Rule {
    /// Create a rule.
    ///
    /// # Errors
    ///
    /// Returns [`RuleError::EmptyRuleId`] if `id` is empty or whitespace-only.
    pub fn new(
        id: impl Into<String>,
        condition: Condition,
        action: RuleAction,
        priority: u32,
        description: impl Into<String>,
    ) -> Result<Self, RuleError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(RuleError::EmptyRuleId);
        }
        Ok(Self {
            id,
            condition,
            action,
            priority,
            description: description.into(),
        })
    }
}

/// An ordered collection of rules evaluated against a packet.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RuleSet {
    /// Unique identifier for this rule set.
    pub id: String,
    /// How rules in this set are evaluated.
    pub strategy: EvaluationStrategy,
    /// Rules in this set, sorted by ascending priority on construction.
    pub rules: Vec<Rule>,
    /// Action taken when no rule matches (or as the terminal for `AllMatches`).
    pub default_action: RuleAction,
    /// When `true`, the evaluator records a [`ConditionTrace`] for every
    /// condition checked during evaluation. Default is `false` for zero
    /// per-condition allocation overhead on high-throughput nodes.
    ///
    /// [`ConditionTrace`]: crate::metadata::ConditionTrace
    pub trace_conditions: bool,
}

impl RuleSet {
    /// Create and validate a rule set.
    ///
    /// Rules are sorted by ascending `priority` value (lower = evaluated first).
    ///
    /// # Errors
    ///
    /// - [`RuleError::EmptyRuleSetId`] if `id` is empty.
    /// - [`RuleError::AllMatchesTerminalAction`] if `strategy` is
    ///   [`EvaluationStrategy::AllMatches`] and any rule uses a terminal action.
    pub fn new(
        id: impl Into<String>,
        strategy: EvaluationStrategy,
        mut rules: Vec<Rule>,
        default_action: RuleAction,
        trace_conditions: bool,
    ) -> Result<Self, RuleError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(RuleError::EmptyRuleSetId);
        }
        if strategy == EvaluationStrategy::AllMatches {
            for rule in &rules {
                if rule.action.is_terminal() {
                    return Err(RuleError::AllMatchesTerminalAction {
                        rule_id: rule.id.clone(),
                        action: rule.action.clone(),
                    });
                }
            }
        }
        rules.sort_by_key(|r| r.priority);
        Ok(Self {
            id,
            strategy,
            rules,
            default_action,
            trace_conditions,
        })
    }
}

/// The decision produced by evaluating a rule set against one packet.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleDecision {
    /// Terminal action to take for this packet.
    pub action: RuleAction,
    /// Id of the rule that produced the terminal action, if any rule matched.
    /// `None` when the `default_action` was applied.
    pub matched_rule: Option<String>,
    /// Tags applied to the packet during evaluation (from `Tag` actions).
    pub tags_applied: Vec<(String, ScalarValue)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::{FieldPath, ScalarValue};
    use pureflow_types::PortId;

    fn port(id: &str) -> PortId {
        PortId::new(id).expect("valid port id")
    }

    fn route_rule(id: &str, priority: u32) -> Rule {
        Rule::new(
            id,
            Condition::Always,
            RuleAction::Route(port("out")),
            priority,
            "test rule",
        )
        .expect("valid rule")
    }

    fn tag_rule(id: &str, priority: u32) -> Rule {
        Rule::new(
            id,
            Condition::Always,
            RuleAction::Tag {
                key: "k".into(),
                value: ScalarValue::Boolean(true),
            },
            priority,
            "tag rule",
        )
        .expect("valid rule")
    }

    #[test]
    fn rule_rejects_empty_id() {
        let err = Rule::new("", Condition::Always, RuleAction::Drop, 0, "").unwrap_err();
        assert_eq!(err, RuleError::EmptyRuleId);
    }

    #[test]
    fn rule_set_rejects_empty_id() {
        let err = RuleSet::new("", EvaluationStrategy::FirstMatch, vec![], RuleAction::Drop, false)
            .unwrap_err();
        assert_eq!(err, RuleError::EmptyRuleSetId);
    }

    #[test]
    fn rule_set_sorts_by_priority() {
        let rules = vec![route_rule("b", 20), route_rule("a", 10), route_rule("c", 30)];
        let rs =
            RuleSet::new("test", EvaluationStrategy::FirstMatch, rules, RuleAction::Drop, false)
                .expect("valid rule set");

        let ids: Vec<&str> = rs.rules.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn all_matches_rejects_route_action() {
        let rules = vec![route_rule("r", 10)];
        let err =
            RuleSet::new("test", EvaluationStrategy::AllMatches, rules, RuleAction::Drop, false)
                .unwrap_err();

        assert!(matches!(
            err,
            RuleError::AllMatchesTerminalAction { ref rule_id, .. } if rule_id == "r"
        ));
    }

    #[test]
    fn all_matches_rejects_halt_action() {
        let rule = Rule::new("h", Condition::Always, RuleAction::Halt("stop".into()), 0, "")
            .expect("valid rule");
        let err = RuleSet::new("test", EvaluationStrategy::AllMatches, vec![rule], RuleAction::Drop, false)
            .unwrap_err();

        assert!(matches!(err, RuleError::AllMatchesTerminalAction { .. }));
    }

    #[test]
    fn all_matches_accepts_tag_only_rules() {
        let rules = vec![tag_rule("t1", 10), tag_rule("t2", 20)];
        RuleSet::new("test", EvaluationStrategy::AllMatches, rules, RuleAction::Drop, false)
            .expect("AllMatches with Tag-only rules must succeed");
    }

    #[test]
    fn first_match_accepts_all_terminal_actions() {
        let rules = vec![
            route_rule("r", 10),
            Rule::new("h", Condition::Always, RuleAction::Halt("e".into()), 20, "")
                .expect("valid"),
        ];
        RuleSet::new("test", EvaluationStrategy::FirstMatch, rules, RuleAction::Drop, false)
            .expect("FirstMatch accepts terminal actions");
    }

    #[test]
    fn field_path_rejects_empty() {
        assert!(FieldPath::new("").is_err());
        assert!(FieldPath::new("   ").is_err());
    }

    #[test]
    fn field_path_accepts_dotted_paths() {
        let p = FieldPath::new("account.type").expect("valid");
        assert_eq!(p.as_str(), "account.type");
    }

    #[cfg(feature = "serde")]
    mod serde_tests {
        use super::*;

        fn field(path: &str) -> FieldPath {
            FieldPath::new(path).expect("valid field path")
        }

        #[test]
        fn rule_set_round_trips_as_json() {
            let rules = vec![
                Rule::new(
                    "high-value",
                    Condition::FieldGte {
                        path: field("amount"),
                        value: ScalarValue::Integer(10000),
                    },
                    RuleAction::Route(port("high-value-out")),
                    10,
                    "Route large amounts",
                )
                .expect("valid rule"),
                Rule::new(
                    "standard",
                    Condition::Always,
                    RuleAction::Route(port("standard-out")),
                    20,
                    "Default path",
                )
                .expect("valid rule"),
            ];
            let original = RuleSet::new(
                "account-router",
                EvaluationStrategy::FirstMatch,
                rules,
                RuleAction::Drop,
                false,
            )
            .expect("valid rule set");

            let json = serde_json::to_string(&original).expect("serializes");
            let restored: RuleSet = serde_json::from_str(&json).expect("deserializes");

            assert_eq!(original, restored);
        }
    }
}
