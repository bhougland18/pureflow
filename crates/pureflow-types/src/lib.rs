//! Shared domain types for Pureflow.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

/// Maximum identifier length in raw UTF-8 bytes.
///
/// Internal identifiers are opaque slugs, not user-facing text; the cap
/// protects transport and storage boundaries rather than display width.
pub const MAX_IDENTIFIER_LEN: usize = 256;

/// Kinds of opaque identifiers used by Pureflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierKind {
    /// A workflow identifier.
    Workflow,
    /// An execution identifier for one workflow run.
    Execution,
    /// A message identifier for one envelope.
    Message,
    /// A node identifier within a workflow graph.
    Node,
    /// A port identifier on a node.
    Port,
}

impl IdentifierKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Workflow => "workflow id",
            Self::Execution => "execution id",
            Self::Message => "message id",
            Self::Node => "node id",
            Self::Port => "port id",
        }
    }
}

/// Error returned when an identifier is malformed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifierError {
    /// The identifier was empty or only whitespace.
    Empty {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
    },
    /// The identifier contained whitespace.
    Whitespace {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
    },
    /// The identifier contained a control character.
    Control {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
    },
    /// The identifier exceeded the maximum allowed length.
    TooLong {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
        /// Maximum allowed byte length.
        limit: usize,
    },
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { kind } => write!(f, "{} must not be empty", kind.label()),
            Self::Whitespace { kind } => write!(f, "{} must not contain whitespace", kind.label()),
            Self::Control { kind } => {
                write!(f, "{} must not contain control characters", kind.label())
            }
            Self::TooLong { kind, limit } => {
                write!(f, "{} must not exceed {} bytes", kind.label(), limit)
            }
        }
    }
}

impl Error for IdentifierError {}

fn validate_identifier(kind: IdentifierKind, value: &str) -> Result<(), IdentifierError> {
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(IdentifierError::TooLong {
            kind,
            limit: MAX_IDENTIFIER_LEN,
        });
    }

    if value.trim().is_empty() {
        return Err(IdentifierError::Empty { kind });
    }

    if value.chars().any(char::is_whitespace) {
        return Err(IdentifierError::Whitespace { kind });
    }

    if value.chars().any(char::is_control) {
        return Err(IdentifierError::Control { kind });
    }

    Ok(())
}

macro_rules! id_type {
    ($name:ident, $kind:expr, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        pub struct $name(String);

        impl $name {
            /// Create a validated identifier.
            ///
            /// # Errors
            ///
            /// Returns an error if the value is empty, contains whitespace, or
            /// contains a control character.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                validate_identifier($kind, &value)?;
                Ok(Self(value))
            }

            /// View the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

id_type!(
    WorkflowId,
    IdentifierKind::Workflow,
    "Stable workflow identifier."
);
id_type!(
    ExecutionId,
    IdentifierKind::Execution,
    "Stable identifier for one workflow execution."
);
id_type!(
    MessageId,
    IdentifierKind::Message,
    "Stable identifier for one message envelope."
);
id_type!(
    NodeId,
    IdentifierKind::Node,
    "Stable node identifier inside a workflow graph."
);
id_type!(
    PortId,
    IdentifierKind::Port,
    "Stable port identifier on a node."
);

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    fn assert_identifier_round_trip<T>(value: &str)
    where
        T: FromStr<Err = IdentifierError> + AsRef<str>,
    {
        let parsed: T = value.parse().expect("generated identifier must parse");
        assert_eq!(parsed.as_ref(), value);
    }

    #[test]
    fn workflow_id_rejects_empty_values() {
        let err = WorkflowId::new("").expect_err("empty identifiers must fail");
        assert_eq!(
            err,
            IdentifierError::Empty {
                kind: IdentifierKind::Workflow
            }
        );
    }

    #[test]
    fn node_id_rejects_whitespace() {
        let err = NodeId::new("node one").expect_err("whitespace identifiers must fail");
        assert_eq!(
            err,
            IdentifierError::Whitespace {
                kind: IdentifierKind::Node
            }
        );
    }

    #[test]
    fn execution_id_rejects_empty_values() {
        let err = ExecutionId::new(" ").expect_err("blank identifiers must fail");
        assert_eq!(
            err,
            IdentifierError::Empty {
                kind: IdentifierKind::Execution
            }
        );
    }

    #[test]
    fn message_id_rejects_control_characters() {
        let err = MessageId::new("msg\u{001f}one").expect_err("control characters must fail");
        assert_eq!(
            err,
            IdentifierError::Control {
                kind: IdentifierKind::Message
            }
        );
    }

    #[test]
    fn port_id_round_trips_through_display_and_parse() {
        let id = PortId::new("out-1").expect("valid identifier");
        let parsed = PortId::from_str(id.as_str()).expect("round-trip should succeed");

        assert_eq!(id, parsed);
        assert_eq!(id.to_string(), "out-1");
        assert_eq!(id.as_ref(), "out-1");
    }

    #[test]
    fn identifiers_reject_control_characters() {
        let err = WorkflowId::new("flow\u{0007}one").expect_err("control characters must fail");
        assert!(matches!(
            err,
            IdentifierError::Control {
                kind: IdentifierKind::Workflow
            }
        ));
    }

    #[test]
    fn identifiers_reject_values_over_length_cap() {
        let value: String = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        let err = PortId::new(value).expect_err("overlong identifiers must fail");

        assert_eq!(
            err,
            IdentifierError::TooLong {
                kind: IdentifierKind::Port,
                limit: MAX_IDENTIFIER_LEN,
            }
        );
    }

    proptest! {
        #[test]
        fn generated_valid_identifiers_are_accepted(value in valid_identifier_strategy()) {
            assert_identifier_round_trip::<WorkflowId>(&value);
            assert_identifier_round_trip::<ExecutionId>(&value);
            assert_identifier_round_trip::<MessageId>(&value);
            assert_identifier_round_trip::<NodeId>(&value);
            assert_identifier_round_trip::<PortId>(&value);
        }

        #[test]
        fn generated_valid_identifiers_reject_appended_whitespace(value in valid_identifier_strategy()) {
            let invalid: String = format!("{value} ");
            prop_assert_eq!(
                WorkflowId::new(invalid),
                Err(IdentifierError::Whitespace { kind: IdentifierKind::Workflow })
            );
        }

        #[test]
        fn generated_valid_identifiers_respect_length_cap(value in valid_identifier_strategy()) {
            prop_assert!(value.len() <= MAX_IDENTIFIER_LEN);
            prop_assert!(WorkflowId::new(value).is_ok());
        }
    }
}
