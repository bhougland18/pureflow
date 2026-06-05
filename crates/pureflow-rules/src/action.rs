//! Rule action types produced by rule evaluation.

use std::fmt;

use pureflow_types::PortId;

use crate::condition::ScalarValue;

/// Action produced when a rule condition matches.
///
/// Terminal actions (`Route`, `Drop`, `DeadLetter`, `Halt`) end evaluation and
/// determine the packet's fate. `Tag` is non-terminal: it annotates the packet
/// and continues evaluation.
///
/// In an `AllMatches` rule set, only `Tag` actions are permitted. All other
/// actions are rejected at rule set construction time.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RuleAction {
    /// Send the packet to the named output port. Terminal.
    Route(PortId),
    /// Discard the packet silently. Terminal.
    Drop,
    /// Route the packet to the configured dead-letter output port. Terminal.
    DeadLetter(String),
    /// Annotate the packet with a key-value tag and continue evaluation. Non-terminal.
    Tag {
        /// Tag key.
        key: String,
        /// Tag value.
        value: ScalarValue,
    },
    /// Fail the node with a structured error message. Terminal.
    Halt(String),
}

impl RuleAction {
    /// Return whether this action ends evaluation.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Tag { .. })
    }
}

impl fmt::Display for RuleAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Route(port) => write!(f, "Route({port})"),
            Self::Drop => write!(f, "Drop"),
            Self::DeadLetter(reason) => write!(f, "DeadLetter({reason})"),
            Self::Tag { key, value } => write!(f, "Tag({key}={value})"),
            Self::Halt(msg) => write!(f, "Halt({msg})"),
        }
    }
}
