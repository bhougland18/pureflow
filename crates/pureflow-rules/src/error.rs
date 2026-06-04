//! Typed errors for rule set construction and validation.

use std::error::Error;
use std::fmt;

use crate::action::RuleAction;

/// Error returned when a rule set or rule is structurally invalid.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleError {
    /// An `AllMatches` rule set contained a rule with a terminal action.
    ///
    /// `AllMatches` rules may only use `Tag` actions. To route after tagging,
    /// place an `AllMatches` node upstream of a `FirstMatch` node.
    AllMatchesTerminalAction {
        /// Identifier of the offending rule.
        rule_id: String,
        /// The terminal action that was rejected.
        action: RuleAction,
    },
    /// A rule identifier was empty.
    EmptyRuleId,
    /// A rule set identifier was empty.
    EmptyRuleSetId,
    /// A field path was empty.
    EmptyFieldPath,
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllMatchesTerminalAction { rule_id, action } => write!(
                f,
                "rule `{rule_id}` in AllMatches rule set uses terminal action `{action}`: \
                 AllMatches rules may only use Tag actions"
            ),
            Self::EmptyRuleId => write!(f, "rule id must not be empty"),
            Self::EmptyRuleSetId => write!(f, "rule set id must not be empty"),
            Self::EmptyFieldPath => write!(f, "field path must not be empty"),
        }
    }
}

impl Error for RuleError {}
