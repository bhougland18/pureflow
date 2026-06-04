//! Condition expression language for rule predicates.

use std::collections::BTreeMap;
use std::fmt;

use pureflow_types::{NodeId, PortId, WorkflowId};

/// A scalar value used in condition comparisons.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ScalarValue {
    /// UTF-8 string value.
    String(String),
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit float.
    Float(f64),
    /// Boolean.
    Boolean(bool),
    /// Null / absent value.
    Null,
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Integer(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Null => write!(f, "null"),
        }
    }
}

/// Dot-separated key path into a packet payload object.
///
/// Example: `"account.type"` or `"metrics.latency_ms"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FieldPath(String);

impl FieldPath {
    /// Create a field path.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::RuleError::EmptyFieldPath`] if the path is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, crate::error::RuleError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(crate::error::RuleError::EmptyFieldPath);
        }
        Ok(Self(value))
    }

    /// View the path as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Resolve the path against a JSON object, returning the value if found.
    #[must_use]
    pub fn resolve<'a>(&self, root: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
        let mut current = root;
        for segment in self.0.split('.') {
            current = current.get(segment)?;
        }
        Some(current)
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Glob pattern for string matching in conditions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GlobPattern(String);

impl GlobPattern {
    /// Create a glob pattern.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// View the pattern as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Test whether the pattern matches a string value.
    ///
    /// Supports `*` (any sequence) and `?` (any single character).
    #[must_use]
    pub fn matches(&self, value: &str) -> bool {
        glob_match(self.0.as_str(), value)
    }
}

impl fmt::Display for GlobPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn glob_match(pattern: &str, value: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    glob_match_inner(&pattern, &value)
}

fn glob_match_inner(pattern: &[char], value: &[char]) -> bool {
    match (pattern, value) {
        ([], []) => true,
        (['*', rest_p @ ..], _) => {
            // * matches zero or more characters
            if glob_match_inner(rest_p, value) {
                return true;
            }
            if !value.is_empty() {
                return glob_match_inner(pattern, &value[1..]);
            }
            false
        }
        (['?', rest_p @ ..], [_, rest_v @ ..]) => glob_match_inner(rest_p, rest_v),
        ([p, rest_p @ ..], [v, rest_v @ ..]) if p == v => glob_match_inner(rest_p, rest_v),
        _ => false,
    }
}

/// Which surface a condition draws its data from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConditionSurface {
    /// Condition evaluates against the packet payload fields.
    Payload,
    /// Condition evaluates against tags applied by upstream `Tag` actions.
    Tag,
    /// Condition evaluates against packet provenance (source node, arrival port, hop count).
    Provenance,
    /// Condition evaluates against execution context (workflow id, execution metadata).
    ExecutionContext,
    /// Condition is a logical combinator over other conditions.
    Combinator,
    /// Condition always evaluates to the same value regardless of packet content.
    Constant,
}

/// Context provided to the rule evaluator for one packet evaluation.
///
/// The caller constructs this from a `PortPacket` and the current node context.
/// Tags represent annotations accumulated from upstream `Tag` actions.
/// Execution metadata is an optional bag of key-value pairs that can be
/// set by the workflow runtime for `ExecutionMetadataEq` conditions.
///
/// `EvalContext` is not serializable because it holds borrowed references.
/// Serialize the [`RuleSet`] and [`Condition`] types instead.
///
/// [`RuleSet`]: crate::rule::RuleSet
#[derive(Debug)]
pub struct EvalContext<'a> {
    /// Packet payload. Payload conditions evaluate against this.
    pub payload: &'a pureflow_core::PacketPayload,
    /// Tags accumulated by upstream `Tag` actions for this packet.
    pub tags: &'a BTreeMap<String, ScalarValue>,
    /// Source node that produced this packet, derived from packet routing metadata.
    pub source_node: Option<&'a NodeId>,
    /// Port on which this packet arrived, derived from packet routing metadata.
    pub arrival_port: Option<&'a PortId>,
    /// Number of rule nodes this packet has passed through.
    pub hop_count: u32,
    /// Workflow currently executing.
    pub workflow_id: &'a WorkflowId,
    /// Optional bag of key-value execution metadata for `ExecutionMetadataEq` conditions.
    pub execution_metadata: &'a BTreeMap<String, ScalarValue>,
}

/// A predicate over a packet and its Pureflow context.
///
/// Conditions are intentionally restricted. There is no `Eval`, `Script`, or
/// `Code` variant — arbitrary code in a condition breaks inspectability and
/// the audit guarantee.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Condition {
    // --- Payload conditions ---
    /// Packet payload field equals a scalar value.
    FieldEq {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Expected value.
        value: ScalarValue,
    },
    /// Packet payload field does not equal a scalar value.
    FieldNeq {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Value the field must not equal.
        value: ScalarValue,
    },
    /// Packet payload field is numerically greater than a value.
    FieldGt {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Threshold value.
        value: ScalarValue,
    },
    /// Packet payload field is numerically less than a value.
    FieldLt {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Threshold value.
        value: ScalarValue,
    },
    /// Packet payload field is numerically greater than or equal to a value.
    FieldGte {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Threshold value.
        value: ScalarValue,
    },
    /// Packet payload field is numerically less than or equal to a value.
    FieldLte {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Threshold value.
        value: ScalarValue,
    },
    /// Packet payload field value is in a set of scalars.
    FieldIn {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Allowed values.
        values: Vec<ScalarValue>,
    },
    /// Packet payload field exists (is present and non-null).
    FieldExists {
        /// Dot-separated path into the payload.
        path: FieldPath,
    },
    /// Packet payload field is absent (missing or null).
    FieldAbsent {
        /// Dot-separated path into the payload.
        path: FieldPath,
    },
    /// Packet payload string field matches a glob pattern.
    FieldMatches {
        /// Dot-separated path into the payload.
        path: FieldPath,
        /// Glob pattern (`*` = any sequence, `?` = any single char).
        pattern: GlobPattern,
    },

    // --- Tag conditions ---
    /// A tag key equals a value.
    TagEq {
        /// Tag key.
        key: String,
        /// Expected tag value.
        value: ScalarValue,
    },
    /// A tag key exists.
    TagExists {
        /// Tag key.
        key: String,
    },
    /// A tag key is absent.
    TagAbsent {
        /// Tag key.
        key: String,
    },

    // --- Provenance conditions ---
    /// Packet was produced by the named source node.
    SourceNode {
        /// Expected source node.
        node_id: NodeId,
    },
    /// Packet arrived on the named input port.
    ArrivedOnPort {
        /// Expected arrival port.
        port_id: PortId,
    },
    /// Packet has passed through more than `n` rule nodes.
    HopCountGt {
        /// Hop count threshold.
        n: u32,
    },
    /// Packet has passed through at most `n` rule nodes.
    HopCountLte {
        /// Hop count threshold.
        n: u32,
    },

    // --- Execution context conditions ---
    /// The executing workflow matches the given id.
    WorkflowIs {
        /// Expected workflow identifier.
        workflow_id: WorkflowId,
    },
    /// An execution metadata key equals a value.
    ExecutionMetadataEq {
        /// Metadata key.
        key: String,
        /// Expected value.
        value: ScalarValue,
    },

    // --- Logical combinators ---
    /// All sub-conditions must be true.
    And(Vec<Condition>),
    /// At least one sub-condition must be true.
    Or(Vec<Condition>),
    /// The sub-condition must be false.
    Not(Box<Condition>),
    /// Always true regardless of packet content.
    Always,
    /// Always false regardless of packet content.
    Never,
}

impl Condition {
    /// Return the primary evaluation surface for this condition.
    ///
    /// For combinators, this returns [`ConditionSurface::Combinator`].
    #[must_use]
    pub const fn surface(&self) -> ConditionSurface {
        match self {
            Self::FieldEq { .. }
            | Self::FieldNeq { .. }
            | Self::FieldGt { .. }
            | Self::FieldLt { .. }
            | Self::FieldGte { .. }
            | Self::FieldLte { .. }
            | Self::FieldIn { .. }
            | Self::FieldExists { .. }
            | Self::FieldAbsent { .. }
            | Self::FieldMatches { .. } => ConditionSurface::Payload,
            Self::TagEq { .. } | Self::TagExists { .. } | Self::TagAbsent { .. } => {
                ConditionSurface::Tag
            }
            Self::SourceNode { .. }
            | Self::ArrivedOnPort { .. }
            | Self::HopCountGt { .. }
            | Self::HopCountLte { .. } => ConditionSurface::Provenance,
            Self::WorkflowIs { .. } | Self::ExecutionMetadataEq { .. } => {
                ConditionSurface::ExecutionContext
            }
            Self::And(_) | Self::Or(_) | Self::Not(_) => ConditionSurface::Combinator,
            Self::Always | Self::Never => ConditionSurface::Constant,
        }
    }
}
