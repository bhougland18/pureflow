//! Rule set evaluator — pure function over rule sets and packets.

use std::collections::BTreeMap;

use pureflow_core::{
    ConditionSurfaceRecord, ConditionTrace, PacketPayload,
    ports::PortPacket,
};

use crate::{
    action::RuleAction,
    condition::{Condition, ConditionSurface, EvalContext, FieldPath, ScalarValue},
    error::RuleError,
    rule::{EvaluationStrategy, Rule, RuleDecision, RuleSet},
};

/// Pure-function evaluator for rule sets.
///
/// `RuleSetEvaluator` is stateless. Multiple instances (or the same instance
/// used concurrently) share no mutable state — each call to [`evaluate`] is
/// fully independent. This makes fan-out parallelism over a shared
/// `Arc<RuleSet>` zero-cost from an evaluation standpoint.
///
/// [`evaluate`]: RuleSetEvaluator::evaluate
#[derive(Debug, Default, Clone, Copy)]
pub struct RuleSetEvaluator;

impl RuleSetEvaluator {
    /// Evaluate a rule set against one packet and return a routing decision.
    ///
    /// The evaluator never performs I/O. It operates purely over the rule set,
    /// the packet payload, and the provided context.
    ///
    /// Condition traces are collected only when `rule_set.trace_conditions` is
    /// `true`. When disabled, `RuleDecision::conditions_evaluated` is empty and
    /// contributes zero allocation overhead.
    ///
    /// # Errors
    ///
    /// Returns [`RuleError`] only for structural problems detected at evaluation
    /// time (none currently — all structural errors are caught at [`RuleSet::new`]
    /// construction). Returns `Ok` for every reachable evaluation path including
    /// the default action path.
    ///
    /// [`RuleSet::new`]: crate::rule::RuleSet::new
    pub fn evaluate(
        &self,
        rule_set: &RuleSet,
        packet: &PortPacket,
        context: &EvalContext<'_>,
    ) -> Result<RuleDecision, RuleError> {
        match rule_set.strategy {
            EvaluationStrategy::FirstMatch => {
                self.evaluate_first_match(rule_set, packet, context)
            }
            EvaluationStrategy::AllMatches => {
                self.evaluate_all_matches(rule_set, packet, context)
            }
            EvaluationStrategy::Score => self.evaluate_score(rule_set, packet, context),
        }
    }

    fn evaluate_first_match(
        &self,
        rule_set: &RuleSet,
        packet: &PortPacket,
        context: &EvalContext<'_>,
    ) -> Result<RuleDecision, RuleError> {
        let payload = packet.payload();
        let mut traces: Vec<ConditionTrace> = if rule_set.trace_conditions {
            Vec::with_capacity(rule_set.rules.len())
        } else {
            Vec::new()
        };
        let mut tags_applied: Vec<(String, ScalarValue)> = Vec::new();

        for rule in &rule_set.rules {
            let matched = eval_condition(&rule.condition, payload, context);
            if rule_set.trace_conditions {
                traces.push(condition_trace(rule, &rule.condition, matched));
            }
            if matched {
                apply_terminal_or_tag(
                    rule,
                    &mut tags_applied,
                    &mut traces,
                    rule_set.trace_conditions,
                );
                if rule.action.is_terminal() {
                    return Ok(RuleDecision {
                        action: rule.action.clone(),
                        matched_rule: Some(rule.id.clone()),
                        tags_applied,
                        conditions_evaluated: traces,
                    });
                }
            }
        }

        Ok(RuleDecision {
            action: rule_set.default_action.clone(),
            matched_rule: None,
            tags_applied,
            conditions_evaluated: traces,
        })
    }

    fn evaluate_all_matches(
        &self,
        rule_set: &RuleSet,
        packet: &PortPacket,
        context: &EvalContext<'_>,
    ) -> Result<RuleDecision, RuleError> {
        // AllMatches: only Tag rules are permitted (validated at construction).
        // Evaluate all rules, collect every tag from matching rules, then apply
        // default_action as the single terminal.
        let payload = packet.payload();
        let mut traces: Vec<ConditionTrace> = if rule_set.trace_conditions {
            Vec::with_capacity(rule_set.rules.len())
        } else {
            Vec::new()
        };
        let mut tags_applied: Vec<(String, ScalarValue)> = Vec::new();

        for rule in &rule_set.rules {
            let matched = eval_condition(&rule.condition, payload, context);
            if rule_set.trace_conditions {
                traces.push(condition_trace(rule, &rule.condition, matched));
            }
            if matched {
                if let RuleAction::Tag { key, value } = &rule.action {
                    tags_applied.push((key.clone(), value.clone()));
                }
            }
        }

        Ok(RuleDecision {
            action: rule_set.default_action.clone(),
            matched_rule: None,
            tags_applied,
            conditions_evaluated: traces,
        })
    }

    fn evaluate_score(
        &self,
        rule_set: &RuleSet,
        packet: &PortPacket,
        context: &EvalContext<'_>,
    ) -> Result<RuleDecision, RuleError> {
        // Score: evaluate ALL rules, collect matching ones, return the
        // highest-priority (lowest priority number) match. If none match,
        // apply default_action.
        let payload = packet.payload();
        let mut traces: Vec<ConditionTrace> = if rule_set.trace_conditions {
            Vec::with_capacity(rule_set.rules.len())
        } else {
            Vec::new()
        };
        let mut tags_applied: Vec<(String, ScalarValue)> = Vec::new();
        let mut best_match: Option<&Rule> = None;

        for rule in &rule_set.rules {
            let matched = eval_condition(&rule.condition, payload, context);
            if rule_set.trace_conditions {
                traces.push(condition_trace(rule, &rule.condition, matched));
            }
            if matched {
                if let RuleAction::Tag { key, value } = &rule.action {
                    tags_applied.push((key.clone(), value.clone()));
                }
                // Rules are sorted ascending by priority; first terminal match
                // is the best (lowest priority number = highest priority).
                if best_match.is_none() && rule.action.is_terminal() {
                    best_match = Some(rule);
                }
            }
        }

        match best_match {
            Some(rule) => Ok(RuleDecision {
                action: rule.action.clone(),
                matched_rule: Some(rule.id.clone()),
                tags_applied,
                conditions_evaluated: traces,
            }),
            None => Ok(RuleDecision {
                action: rule_set.default_action.clone(),
                matched_rule: None,
                tags_applied,
                conditions_evaluated: traces,
            }),
        }
    }
}

fn apply_terminal_or_tag(
    rule: &Rule,
    tags_applied: &mut Vec<(String, ScalarValue)>,
    _traces: &mut Vec<ConditionTrace>,
    _trace: bool,
) {
    if let RuleAction::Tag { key, value } = &rule.action {
        tags_applied.push((key.clone(), value.clone()));
    }
}

fn condition_trace(rule: &Rule, condition: &Condition, matched: bool) -> ConditionTrace {
    ConditionTrace::new(
        rule.id.clone(),
        condition_description(condition),
        matched,
        surface_to_record(condition.surface()),
    )
}

fn surface_to_record(surface: ConditionSurface) -> ConditionSurfaceRecord {
    match surface {
        ConditionSurface::Payload => ConditionSurfaceRecord::Payload,
        ConditionSurface::Tag => ConditionSurfaceRecord::Tag,
        ConditionSurface::Provenance => ConditionSurfaceRecord::Provenance,
        ConditionSurface::ExecutionContext => ConditionSurfaceRecord::ExecutionContext,
        ConditionSurface::Combinator => ConditionSurfaceRecord::Combinator,
        ConditionSurface::Constant => ConditionSurfaceRecord::Constant,
    }
}

/// Evaluate one condition against a packet payload and context.
fn eval_condition(
    condition: &Condition,
    payload: &PacketPayload,
    context: &EvalContext<'_>,
) -> bool {
    match condition {
        // --- Payload conditions ---
        Condition::FieldEq { path, value } => {
            resolve_payload(payload, path)
                .map(|v| json_value_eq_scalar(v, value))
                .unwrap_or(false)
        }
        Condition::FieldNeq { path, value } => {
            resolve_payload(payload, path)
                .map(|v| !json_value_eq_scalar(v, value))
                .unwrap_or(false)
        }
        Condition::FieldGt { path, value } => {
            resolve_payload(payload, path)
                .and_then(|v| json_value_cmp_scalar(v, value))
                .map(|ord| ord == std::cmp::Ordering::Greater)
                .unwrap_or(false)
        }
        Condition::FieldLt { path, value } => {
            resolve_payload(payload, path)
                .and_then(|v| json_value_cmp_scalar(v, value))
                .map(|ord| ord == std::cmp::Ordering::Less)
                .unwrap_or(false)
        }
        Condition::FieldGte { path, value } => {
            resolve_payload(payload, path)
                .and_then(|v| json_value_cmp_scalar(v, value))
                .map(|ord| ord != std::cmp::Ordering::Less)
                .unwrap_or(false)
        }
        Condition::FieldLte { path, value } => {
            resolve_payload(payload, path)
                .and_then(|v| json_value_cmp_scalar(v, value))
                .map(|ord| ord != std::cmp::Ordering::Greater)
                .unwrap_or(false)
        }
        Condition::FieldIn { path, values } => {
            resolve_payload(payload, path)
                .map(|v| values.iter().any(|s| json_value_eq_scalar(v, s)))
                .unwrap_or(false)
        }
        Condition::FieldExists { path } => resolve_payload(payload, path).is_some(),
        Condition::FieldAbsent { path } => {
            resolve_payload(payload, path)
                .map(|v| v.is_null())
                .unwrap_or(true)
        }
        Condition::FieldMatches { path, pattern } => {
            resolve_payload(payload, path)
                .and_then(|v| v.as_str())
                .map(|s| pattern.matches(s))
                .unwrap_or(false)
        }

        // --- Tag conditions ---
        Condition::TagEq { key, value } => context
            .tags
            .get(key.as_str())
            .map(|v| scalar_eq(v, value))
            .unwrap_or(false),
        Condition::TagExists { key } => context.tags.contains_key(key.as_str()),
        Condition::TagAbsent { key } => !context.tags.contains_key(key.as_str()),

        // --- Provenance conditions ---
        Condition::SourceNode { node_id } => context
            .source_node
            .map(|n| n == node_id)
            .unwrap_or(false),
        Condition::ArrivedOnPort { port_id } => context
            .arrival_port
            .map(|p| p == port_id)
            .unwrap_or(false),
        Condition::HopCountGt { n } => context.hop_count > *n,
        Condition::HopCountLte { n } => context.hop_count <= *n,

        // --- Execution context conditions ---
        Condition::WorkflowIs { workflow_id } => context.workflow_id == workflow_id,
        Condition::ExecutionMetadataEq { key, value } => context
            .execution_metadata
            .get(key.as_str())
            .map(|v| scalar_eq(v, value))
            .unwrap_or(false),

        // --- Logical combinators ---
        Condition::And(conditions) => conditions
            .iter()
            .all(|c| eval_condition(c, payload, context)),
        Condition::Or(conditions) => conditions
            .iter()
            .any(|c| eval_condition(c, payload, context)),
        Condition::Not(inner) => !eval_condition(inner, payload, context),

        // --- Constants ---
        Condition::Always => true,
        Condition::Never => false,
    }
}

/// Resolve a field path against a Control or Structured payload.
/// Returns `None` for Bytes payloads (no field access without a schema).
fn resolve_payload<'a>(
    payload: &'a PacketPayload,
    path: &FieldPath,
) -> Option<&'a serde_json::Value> {
    match payload {
        PacketPayload::Control(value) => path.resolve(value),
        PacketPayload::Bytes(_) => None,
        #[cfg(feature = "arrow")]
        PacketPayload::Arrow(_) => None,
    }
}

fn json_value_eq_scalar(json: &serde_json::Value, scalar: &ScalarValue) -> bool {
    match (json, scalar) {
        (serde_json::Value::String(s), ScalarValue::String(r)) => s == r,
        (serde_json::Value::Number(n), ScalarValue::Integer(i)) => n.as_i64() == Some(*i),
        (serde_json::Value::Number(n), ScalarValue::Float(f)) => {
            n.as_f64().map(|v| v == *f).unwrap_or(false)
        }
        (serde_json::Value::Bool(b), ScalarValue::Boolean(r)) => b == r,
        (serde_json::Value::Null, ScalarValue::Null) => true,
        _ => false,
    }
}

fn json_value_cmp_scalar(
    json: &serde_json::Value,
    scalar: &ScalarValue,
) -> Option<std::cmp::Ordering> {
    let json_f: f64 = json.as_f64()?;
    let scalar_f: f64 = match scalar {
        ScalarValue::Integer(i) => *i as f64,
        ScalarValue::Float(f) => *f,
        _ => return None,
    };
    json_f.partial_cmp(&scalar_f)
}

fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::String(s1), ScalarValue::String(s2)) => s1 == s2,
        (ScalarValue::Integer(i1), ScalarValue::Integer(i2)) => i1 == i2,
        (ScalarValue::Float(f1), ScalarValue::Float(f2)) => f1 == f2,
        (ScalarValue::Boolean(b1), ScalarValue::Boolean(b2)) => b1 == b2,
        (ScalarValue::Null, ScalarValue::Null) => true,
        _ => false,
    }
}

/// Produce a human-readable description of a condition for trace records.
fn condition_description(condition: &Condition) -> String {
    match condition {
        Condition::FieldEq { path, value } => format!("{path} == {value}"),
        Condition::FieldNeq { path, value } => format!("{path} != {value}"),
        Condition::FieldGt { path, value } => format!("{path} > {value}"),
        Condition::FieldLt { path, value } => format!("{path} < {value}"),
        Condition::FieldGte { path, value } => format!("{path} >= {value}"),
        Condition::FieldLte { path, value } => format!("{path} <= {value}"),
        Condition::FieldIn { path, values } => {
            let vals: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            format!("{path} in [{}]", vals.join(", "))
        }
        Condition::FieldExists { path } => format!("{path} exists"),
        Condition::FieldAbsent { path } => format!("{path} absent"),
        Condition::FieldMatches { path, pattern } => format!("{path} matches {pattern}"),
        Condition::TagEq { key, value } => format!("tag:{key} == {value}"),
        Condition::TagExists { key } => format!("tag:{key} exists"),
        Condition::TagAbsent { key } => format!("tag:{key} absent"),
        Condition::SourceNode { node_id } => format!("source_node == {node_id}"),
        Condition::ArrivedOnPort { port_id } => format!("arrived_on == {port_id}"),
        Condition::HopCountGt { n } => format!("hop_count > {n}"),
        Condition::HopCountLte { n } => format!("hop_count <= {n}"),
        Condition::WorkflowIs { workflow_id } => format!("workflow == {workflow_id}"),
        Condition::ExecutionMetadataEq { key, value } => {
            format!("exec_meta:{key} == {value}")
        }
        Condition::And(inner) => {
            let parts: Vec<String> = inner.iter().map(condition_description).collect();
            format!("({})", parts.join(" AND "))
        }
        Condition::Or(inner) => {
            let parts: Vec<String> = inner.iter().map(condition_description).collect();
            format!("({})", parts.join(" OR "))
        }
        Condition::Not(inner) => format!("NOT ({})", condition_description(inner)),
        Condition::Always => "always".into(),
        Condition::Never => "never".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use bytes::Bytes;
    use pureflow_core::message::{MessageEndpoint, MessageMetadata, MessageRoute};
    use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
    use pureflow_core::context::ExecutionMetadata;
    use serde_json::json;

    use crate::{
        condition::{EvalContext, FieldPath, GlobPattern, ScalarValue},
        rule::{EvaluationStrategy, Rule, RuleSet},
        action::RuleAction,
    };

    // ── helpers ──────────────────────────────────────────────────────────────

    fn node_id(s: &str) -> NodeId { NodeId::new(s).unwrap() }
    fn port_id(s: &str) -> PortId { PortId::new(s).unwrap() }
    fn workflow_id(s: &str) -> WorkflowId { WorkflowId::new(s).unwrap() }
    fn field(s: &str) -> FieldPath { FieldPath::new(s).unwrap() }

    fn packet(payload: PacketPayload) -> PortPacket {
        let src = MessageEndpoint::new(node_id("src"), port_id("out"));
        let tgt = MessageEndpoint::new(node_id("dst"), port_id("in"));
        let route = MessageRoute::new(Some(src), tgt);
        let meta = MessageMetadata::new(
            MessageId::new("m1").unwrap(),
            workflow_id("flow"),
            ExecutionMetadata::first_attempt(ExecutionId::new("run-1").unwrap()),
            route,
        );
        PortPacket::new(meta, payload)
    }

    fn control_packet(value: serde_json::Value) -> PortPacket {
        packet(PacketPayload::control(value))
    }

    fn empty_ctx<'a>(
        tags: &'a BTreeMap<String, ScalarValue>,
        exec_meta: &'a BTreeMap<String, ScalarValue>,
        wf_id: &'a WorkflowId,
    ) -> EvalContext<'a> {
        EvalContext {
            tags,
            source_node: None,
            arrival_port: None,
            hop_count: 0,
            workflow_id: wf_id,
            execution_metadata: exec_meta,
        }
    }

    fn rule(id: &str, cond: Condition, action: RuleAction, pri: u32) -> Rule {
        Rule::new(id, cond, action, pri, "").unwrap()
    }

    fn first_match_set(rules: Vec<Rule>) -> RuleSet {
        RuleSet::new("test", EvaluationStrategy::FirstMatch, rules, RuleAction::Drop, false).unwrap()
    }

    fn evaluator() -> RuleSetEvaluator { RuleSetEvaluator }

    // ── Constant conditions ───────────────────────────────────────────────────

    #[test]
    fn always_matches() {
        let wf = workflow_id("flow");
        let tags = BTreeMap::new();
        let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        assert!(eval_condition(&Condition::Always, &PacketPayload::control(json!({})), &ctx));
    }

    #[test]
    fn never_does_not_match() {
        let wf = workflow_id("flow");
        let tags = BTreeMap::new();
        let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        assert!(!eval_condition(&Condition::Never, &PacketPayload::control(json!({})), &ctx));
    }

    // ── Payload conditions ────────────────────────────────────────────────────

    #[test]
    fn field_eq_matches_string() {
        let p = PacketPayload::control(json!({"status": "approved"}));
        let cond = Condition::FieldEq { path: field("status"), value: ScalarValue::String("approved".into()) };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_eq_rejects_mismatch() {
        let p = PacketPayload::control(json!({"status": "declined"}));
        let cond = Condition::FieldEq { path: field("status"), value: ScalarValue::String("approved".into()) };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(!eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_gte_integer() {
        let p = PacketPayload::control(json!({"amount": 15000}));
        let cond = Condition::FieldGte { path: field("amount"), value: ScalarValue::Integer(10000) };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_lt_integer_fails_when_equal() {
        let p = PacketPayload::control(json!({"amount": 100}));
        let cond = Condition::FieldLt { path: field("amount"), value: ScalarValue::Integer(100) };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(!eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_in_matches_member() {
        let p = PacketPayload::control(json!({"tier": "gold"}));
        let cond = Condition::FieldIn {
            path: field("tier"),
            values: vec![ScalarValue::String("silver".into()), ScalarValue::String("gold".into())],
        };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_exists_on_present_key() {
        let p = PacketPayload::control(json!({"region": "us-east-1"}));
        let cond = Condition::FieldExists { path: field("region") };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_absent_on_missing_key() {
        let p = PacketPayload::control(json!({"region": "us-east-1"}));
        let cond = Condition::FieldAbsent { path: field("currency") };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn field_matches_glob() {
        let p = PacketPayload::control(json!({"path": "/api/v2/users"}));
        let cond = Condition::FieldMatches { path: field("path"), pattern: GlobPattern::new("/api/v*/users") };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn dotted_field_path_resolves_nested_field() {
        let p = PacketPayload::control(json!({"account": {"type": "premium"}}));
        let cond = Condition::FieldEq { path: field("account.type"), value: ScalarValue::String("premium".into()) };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn bytes_payload_returns_false_for_field_conditions() {
        let p = PacketPayload::Bytes(bytes::Bytes::from_static(b"raw"));
        let cond = Condition::FieldExists { path: field("x") };
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        assert!(!eval_condition(&cond, &p, &empty_ctx(&tags, &meta, &wf)));
    }

    // ── Tag conditions ────────────────────────────────────────────────────────

    #[test]
    fn tag_eq_matches_present_tag() {
        let wf = workflow_id("flow");
        let mut tags = BTreeMap::new();
        tags.insert("priority".into(), ScalarValue::String("high".into()));
        let meta = BTreeMap::new();
        let cond = Condition::TagEq { key: "priority".into(), value: ScalarValue::String("high".into()) };
        assert!(eval_condition(&cond, &PacketPayload::control(json!({})), &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn tag_absent_when_key_missing() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let cond = Condition::TagAbsent { key: "urgent".into() };
        assert!(eval_condition(&cond, &PacketPayload::control(json!({})), &empty_ctx(&tags, &meta, &wf)));
    }

    #[test]
    fn tag_exists_false_when_key_missing() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let cond = Condition::TagExists { key: "urgent".into() };
        assert!(!eval_condition(&cond, &PacketPayload::control(json!({})), &empty_ctx(&tags, &meta, &wf)));
    }

    // ── Provenance conditions ─────────────────────────────────────────────────

    #[test]
    fn source_node_matches() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let src = node_id("validator");
        let ctx = EvalContext {
            tags: &tags, source_node: Some(&src), arrival_port: None,
            hop_count: 0, workflow_id: &wf, execution_metadata: &meta,
        };
        let p = PacketPayload::control(json!({}));
        let cond = Condition::SourceNode { node_id: node_id("validator") };
        assert!(eval_condition(&cond, &p, &ctx));
    }

    #[test]
    fn hop_count_gt_fires_when_exceeded() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let ctx = EvalContext {
            tags: &tags, source_node: None, arrival_port: None,
            hop_count: 5, workflow_id: &wf, execution_metadata: &meta,
        };
        let p = PacketPayload::control(json!({}));
        assert!(eval_condition(&Condition::HopCountGt { n: 3 }, &p, &ctx));
        assert!(!eval_condition(&Condition::HopCountGt { n: 5 }, &p, &ctx));
    }

    #[test]
    fn workflow_is_matches_correct_workflow() {
        let wf = workflow_id("payment-flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        let p = PacketPayload::control(json!({}));
        let cond = Condition::WorkflowIs { workflow_id: workflow_id("payment-flow") };
        assert!(eval_condition(&cond, &p, &ctx));
        let wrong = Condition::WorkflowIs { workflow_id: workflow_id("other-flow") };
        assert!(!eval_condition(&wrong, &p, &ctx));
    }

    // ── Logical combinators ───────────────────────────────────────────────────

    #[test]
    fn and_requires_all_true() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        let p = PacketPayload::control(json!({}));
        assert!(eval_condition(&Condition::And(vec![Condition::Always, Condition::Always]), &p, &ctx));
        assert!(!eval_condition(&Condition::And(vec![Condition::Always, Condition::Never]), &p, &ctx));
    }

    #[test]
    fn or_requires_at_least_one_true() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        let p = PacketPayload::control(json!({}));
        assert!(eval_condition(&Condition::Or(vec![Condition::Never, Condition::Always]), &p, &ctx));
        assert!(!eval_condition(&Condition::Or(vec![Condition::Never, Condition::Never]), &p, &ctx));
    }

    #[test]
    fn not_inverts_condition() {
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        let p = PacketPayload::control(json!({}));
        assert!(eval_condition(&Condition::Not(Box::new(Condition::Never)), &p, &ctx));
        assert!(!eval_condition(&Condition::Not(Box::new(Condition::Always)), &p, &ctx));
    }

    #[test]
    fn deeply_nested_and_or_not() {
        // NOT (amount < 100 AND status == "pending") → true when amount >= 100
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let ctx = empty_ctx(&tags, &meta, &wf);
        let p = PacketPayload::control(json!({"amount": 500, "status": "pending"}));
        let nested = Condition::Not(Box::new(Condition::And(vec![
            Condition::FieldLt { path: field("amount"), value: ScalarValue::Integer(100) },
            Condition::FieldEq { path: field("status"), value: ScalarValue::String("pending".into()) },
        ])));
        assert!(eval_condition(&nested, &p, &ctx));
    }

    // ── Strategy: FirstMatch ──────────────────────────────────────────────────

    #[test]
    fn first_match_routes_to_first_matching_rule() {
        let rules = vec![
            rule("high", Condition::FieldGte { path: field("amount"), value: ScalarValue::Integer(10000) },
                 RuleAction::Route(port_id("high-out")), 10),
            rule("standard", Condition::Always, RuleAction::Route(port_id("std-out")), 20),
        ];
        let rs = first_match_set(rules);
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({"amount": 50000}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        assert_eq!(decision.matched_rule.as_deref(), Some("high"));
        assert_eq!(decision.action, RuleAction::Route(port_id("high-out")));
    }

    #[test]
    fn first_match_falls_through_to_default_when_no_rule_matches() {
        let rules = vec![
            rule("big", Condition::FieldGte { path: field("amount"), value: ScalarValue::Integer(10000) },
                 RuleAction::Route(port_id("big-out")), 10),
        ];
        let rs = RuleSet::new("test", EvaluationStrategy::FirstMatch, rules, RuleAction::Drop, false).unwrap();
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({"amount": 50}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        assert!(decision.matched_rule.is_none());
        assert_eq!(decision.action, RuleAction::Drop);
    }

    #[test]
    fn first_match_applies_tag_actions_before_terminal() {
        let rules = vec![
            rule("tag-it", Condition::Always,
                 RuleAction::Tag { key: "flagged".into(), value: ScalarValue::Boolean(true) }, 5),
            rule("route-it", Condition::Always, RuleAction::Route(port_id("out")), 10),
        ];
        let rs = first_match_set(rules);
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        assert_eq!(decision.tags_applied.len(), 1);
        assert_eq!(decision.tags_applied[0].0, "flagged");
        assert_eq!(decision.action, RuleAction::Route(port_id("out")));
    }

    // ── Strategy: AllMatches ──────────────────────────────────────────────────

    #[test]
    fn all_matches_collects_all_tags_and_applies_default() {
        let rules = vec![
            rule("t1", Condition::Always,
                 RuleAction::Tag { key: "a".into(), value: ScalarValue::Integer(1) }, 10),
            rule("t2", Condition::Always,
                 RuleAction::Tag { key: "b".into(), value: ScalarValue::Integer(2) }, 20),
        ];
        let rs = RuleSet::new("test", EvaluationStrategy::AllMatches, rules, RuleAction::Route(port_id("out")), false).unwrap();
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        assert_eq!(decision.tags_applied.len(), 2);
        assert_eq!(decision.action, RuleAction::Route(port_id("out")));
        assert!(decision.matched_rule.is_none());
    }

    // ── Strategy: Score ───────────────────────────────────────────────────────

    #[test]
    fn score_selects_highest_priority_matching_rule() {
        let rules = vec![
            rule("low-pri", Condition::Always, RuleAction::Route(port_id("low")), 100),
            rule("high-pri", Condition::Always, RuleAction::Route(port_id("high")), 10),
        ];
        let rs = RuleSet::new("test", EvaluationStrategy::Score, rules, RuleAction::Drop, false).unwrap();
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        // priority 10 (high-pri) wins over priority 100 (low-pri)
        assert_eq!(decision.matched_rule.as_deref(), Some("high-pri"));
        assert_eq!(decision.action, RuleAction::Route(port_id("high")));
    }

    // ── ConditionTrace ────────────────────────────────────────────────────────

    #[test]
    fn trace_disabled_produces_empty_conditions_evaluated() {
        let rules = vec![rule("r", Condition::Always, RuleAction::Route(port_id("out")), 10)];
        let rs = RuleSet::new("test", EvaluationStrategy::FirstMatch, rules, RuleAction::Drop, false).unwrap();
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        assert!(decision.conditions_evaluated.is_empty());
    }

    #[test]
    fn trace_enabled_records_conditions_with_surface() {
        let rules = vec![
            rule("check-amount",
                 Condition::FieldGte { path: field("amount"), value: ScalarValue::Integer(100) },
                 RuleAction::Route(port_id("out")), 10),
        ];
        let rs = RuleSet::new("test", EvaluationStrategy::FirstMatch, rules, RuleAction::Drop, true).unwrap();
        let wf = workflow_id("flow"); let tags = BTreeMap::new(); let meta = BTreeMap::new();
        let p = control_packet(json!({"amount": 200}));
        let ctx = EvalContext { tags: &tags, source_node: None,
            arrival_port: None, hop_count: 0, workflow_id: &wf, execution_metadata: &meta };
        let decision = evaluator().evaluate(&rs, &p, &ctx).unwrap();
        assert_eq!(decision.conditions_evaluated.len(), 1);
        assert_eq!(decision.conditions_evaluated[0].rule_id, "check-amount");
        assert!(decision.conditions_evaluated[0].matched);
        assert_eq!(decision.conditions_evaluated[0].surface, ConditionSurfaceRecord::Payload);
    }
}
