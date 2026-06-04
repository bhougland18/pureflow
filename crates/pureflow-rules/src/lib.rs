//! Declarative rules engine types and evaluator for Pureflow.
//!
//! This crate provides the core rule types, condition expression language,
//! and context surface for the Pureflow rules engine. It does not depend on
//! `pureflow-engine` or any runtime crate.
//!
//! ## Overview
//!
//! A [`RuleSet`] is an ordered collection of [`Rule`] values evaluated against
//! a [`PortPacket`] and its [`EvalContext`]. Each rule pairs a [`Condition`]
//! predicate with a [`RuleAction`]. When a condition matches, the action
//! determines the packet's fate.
//!
//! Rules are data: serializable, inspectable, and validated before execution.
//!
//! ## Condition surfaces
//!
//! Conditions can examine:
//! - **Payload** fields via dot-separated [`FieldPath`] expressions
//! - **Tags** applied by upstream [`RuleAction::Tag`] actions
//! - **Provenance** (source node, arrival port, hop count)
//! - **Execution context** (workflow id, execution metadata key-value pairs)
//!
//! ## `AllMatches` constraint
//!
//! Rule sets with [`EvaluationStrategy::AllMatches`] may only contain
//! [`RuleAction::Tag`] actions. Terminal actions are rejected at
//! [`RuleSet::new`] time with a typed [`RuleError`].
//!
//! ## Serde
//!
//! Enable the `serde` feature to derive `Serialize`/`Deserialize` for all
//! public types. Rule sets can then be embedded inline in workflow JSON or
//! loaded from side-car files via `rule_set_ref`.

pub mod action;
pub mod condition;
pub mod error;
pub mod eval;
pub mod rule;

pub use action::RuleAction;
pub use condition::{Condition, ConditionSurface, EvalContext, FieldPath, GlobPattern, ScalarValue};
pub use error::RuleError;
pub use eval::RuleSetEvaluator;
pub use rule::{EvaluationStrategy, Rule, RuleDecision, RuleSet};
