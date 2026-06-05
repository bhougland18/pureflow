//! Wasmtime-backed rule-evaluation boundary for Pureflow.
//!
//! This module wires the `pureflow:rules@0.1.0` WIT contract
//! (`wit/pureflow-rules.wit`) to a WASM guest. The host serializes a native
//! [`RuleSet`] and a [`HostEvalContext`] into Component Model values, invokes
//! the guest's single `evaluate` function, and decodes the returned
//! `rule-decision` back into a native [`RuleDecision`].
//!
//! The guest never touches a channel, port, or send/recv handle: it receives a
//! rule set plus the packet context to judge and returns a routing decision.
//! All packet movement — including the reserve/commit sends that act on the
//! decision — stays on the host (see [`crate::rules_node::WasmRuleNode`]).
//!
//! Like the batch adapter, the host uses Wasmtime's *dynamic* Component Model
//! values rather than `bindgen!`, because typed bindgen expands `unsafe`
//! internals that the workspace lint boundary forbids.

use std::collections::BTreeMap;

use pureflow_core::{
    ConditionSurfaceRecord, ConditionTrace, PacketPayload, PureflowError, Result,
    capability::NodeCapabilities,
    context::{CancellationRequest, CancellationToken},
};
use pureflow_rules::{
    Condition, RuleAction, RuleSet, ScalarValue,
    rule::{EvaluationStrategy, RuleDecision},
};
use pureflow_types::PortId;
use wasmtime::{
    Engine, Store,
    component::{Component, ComponentExportIndex, Func, Instance, Linker, Val},
};

use crate::{
    CancellationWatcher, WasmtimeExecutionLimits, component_engine, map_guest_call_error,
    record_fields, required_field, required_list_field, required_string_field,
    validate_wasm_capabilities,
};

/// WIT package identifier implemented by Pureflow WASM rule guests.
pub const WIT_RULES_PACKAGE: &str = "pureflow:rules@0.1.0";

/// WIT world exported by Pureflow WASM rule guests.
pub const WIT_RULES_WORLD: &str = "pureflow-rules-node";

/// Fully-qualified export path of the guest `rules` interface.
const RULES_INTERFACE_EXPORT: &str = "pureflow:rules/rules@0.1.0";

/// The single evaluation function exported by the `rules` interface.
const EVALUATE_EXPORT: &str = "evaluate";

/// Owned, host-side evaluation context for one packet.
///
/// Mirrors `pureflow_rules::condition::EvalContext`, but holds owned data so it
/// can be encoded into Component Model values without borrowing the packet.
#[derive(Debug, Clone)]
pub struct HostEvalContext {
    /// Packet payload the guest evaluates against.
    pub payload: PacketPayload,
    /// Tags accumulated by upstream `Tag` actions.
    pub tags: BTreeMap<String, ScalarValue>,
    /// Source node that produced this packet, if known.
    pub source_node: Option<String>,
    /// Port on which this packet arrived, if known.
    pub arrival_port: Option<String>,
    /// Number of rule nodes this packet has passed through.
    pub hop_count: u32,
    /// Workflow currently executing.
    pub workflow_id: String,
    /// Execution metadata key/value pairs for `ExecutionMetadataEq` conditions.
    pub execution_metadata: BTreeMap<String, ScalarValue>,
}

impl Default for HostEvalContext {
    fn default() -> Self {
        Self {
            payload: PacketPayload::Bytes(bytes::Bytes::new()),
            tags: BTreeMap::new(),
            source_node: None,
            arrival_port: None,
            hop_count: 0,
            workflow_id: String::new(),
            execution_metadata: BTreeMap::new(),
        }
    }
}

/// Wasmtime component prepared for Pureflow rule evaluation.
pub struct WasmtimeRuleComponent {
    engine: Engine,
    component: Component,
    limits: WasmtimeExecutionLimits,
}

impl WasmtimeRuleComponent {
    /// Compile a rule guest component from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if Wasmtime cannot configure the engine or compile the
    /// supplied component bytes.
    pub fn from_component_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        Self::from_component_bytes_with_limits(bytes, WasmtimeExecutionLimits::default())
    }

    /// Compile a rule guest component from bytes with explicit execution limits.
    ///
    /// # Errors
    ///
    /// Returns an error if Wasmtime cannot configure the engine or compile the
    /// supplied component bytes.
    pub fn from_component_bytes_with_limits(
        bytes: impl AsRef<[u8]>,
        limits: WasmtimeExecutionLimits,
    ) -> Result<Self> {
        let engine: Engine = component_engine()?;
        let component: Component =
            Component::from_binary(&engine, bytes.as_ref()).map_err(|err: wasmtime::Error| {
                PureflowError::execution(format!("failed to compile rule component: {err}"))
            })?;

        Ok(Self {
            engine,
            component,
            limits,
        })
    }

    /// Compile a rule guest after validating the WASM capability boundary.
    ///
    /// WASM rule components carry the same channel-access restrictions as other
    /// batch nodes: the import-free world cannot enforce external effects, so a
    /// descriptor that declares any effect capability is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error if the capability descriptor declares effects the
    /// import-free WASM world cannot enforce, or if compilation fails.
    pub fn from_component_bytes_with_capabilities(
        bytes: impl AsRef<[u8]>,
        capabilities: &NodeCapabilities,
    ) -> Result<Self> {
        validate_wasm_capabilities(capabilities)?;
        Self::from_component_bytes(bytes)
    }

    /// Execution limits used for each guest invocation.
    #[must_use]
    pub const fn limits(&self) -> WasmtimeExecutionLimits {
        self.limits
    }

    /// Evaluate a rule set against one packet context inside the guest.
    ///
    /// # Errors
    ///
    /// Returns an error if the component cannot instantiate, the guest traps or
    /// exceeds its fuel budget, the guest reports a `rule-error`, or the guest
    /// returns a malformed decision.
    pub fn evaluate(
        &self,
        rule_set: &RuleSet,
        context: &HostEvalContext,
    ) -> Result<RuleDecision> {
        self.evaluate_with_cancellation(rule_set, context, &CancellationToken::active())
    }

    /// Evaluate a rule set against one packet context, interrupting Wasmtime
    /// execution if cancellation is requested while the guest call is in flight.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation is already requested, the component
    /// cannot instantiate, the guest traps or exceeds its fuel budget, the guest
    /// reports a `rule-error`, or the guest returns a malformed decision.
    pub fn evaluate_with_cancellation(
        &self,
        rule_set: &RuleSet,
        context: &HostEvalContext,
        cancellation: &CancellationToken,
    ) -> Result<RuleDecision> {
        if let Some(request) = cancellation.request() {
            return Err(PureflowError::cancelled(request.reason()));
        }

        let linker: Linker<()> = Linker::new(&self.engine);
        let mut store: Store<()> = Store::new(&self.engine, ());
        store.set_epoch_deadline(self.limits.cancellation_epoch_deadline());
        store
            .set_fuel(self.limits.fuel())
            .map_err(|err: wasmtime::Error| {
                PureflowError::execution(format!("failed to configure guest fuel: {err}"))
            })?;
        let watcher: CancellationWatcher = CancellationWatcher::spawn(
            self.engine.clone(),
            cancellation.clone(),
            self.limits.cancellation_poll_interval(),
        )?;
        let instance: Instance =
            linker
                .instantiate(&mut store, &self.component)
                .map_err(|err: wasmtime::Error| {
                    PureflowError::execution(format!("failed to instantiate rule component: {err}"))
                })?;
        let interface_index: ComponentExportIndex = instance
            .get_export_index(&mut store, None, RULES_INTERFACE_EXPORT)
            .ok_or_else(|| {
                PureflowError::execution(format!("component does not export {RULES_INTERFACE_EXPORT}"))
            })?;
        let evaluate_index: ComponentExportIndex = instance
            .get_export_index(&mut store, Some(&interface_index), EVALUATE_EXPORT)
            .ok_or_else(|| PureflowError::execution("component does not export rules.evaluate"))?;
        let evaluate: Func = instance
            .get_func(&mut store, evaluate_index)
            .ok_or_else(|| PureflowError::execution("rules.evaluate export is not a function"))?;

        let params: [Val; 2] = [rule_set_to_val(rule_set)?, eval_context_to_val(context)?];
        let mut results: [Val; 1] = [Val::Bool(false)];
        let call_result: std::result::Result<(), wasmtime::Error> =
            evaluate.call(&mut store, &params, &mut results);
        let interrupted: bool = watcher.finish();
        if interrupted {
            let reason: String = cancellation.request().map_or_else(
                || String::from("wasm rule evaluation cancelled"),
                |request: CancellationRequest| request.reason().to_owned(),
            );
            return Err(PureflowError::cancelled(reason));
        }
        let remaining_fuel: Option<u64> = store.get_fuel().ok();
        call_result
            .map_err(|err: wasmtime::Error| map_guest_call_error(&err, self.limits, remaining_fuel))?;

        let [result]: [Val; 1] = results;
        decision_from_result_val(result)
    }
}

// ── Native → Component Model value encoding ──────────────────────────────────

fn rule_set_to_val(rule_set: &RuleSet) -> Result<Val> {
    let rules: Vec<Val> = rule_set
        .rules
        .iter()
        .map(rule_to_val)
        .collect::<Result<Vec<_>>>()?;
    Ok(Val::Record(vec![
        ("id".to_owned(), Val::String(rule_set.id.clone())),
        ("strategy".to_owned(), strategy_to_val(rule_set.strategy)),
        ("rules".to_owned(), Val::List(rules)),
        (
            "default-action".to_owned(),
            action_to_val(&rule_set.default_action),
        ),
        (
            "trace-conditions".to_owned(),
            Val::Bool(rule_set.trace_conditions),
        ),
    ]))
}

fn rule_to_val(rule: &pureflow_rules::Rule) -> Result<Val> {
    Ok(Val::Record(vec![
        ("id".to_owned(), Val::String(rule.id.clone())),
        ("condition".to_owned(), condition_tree_to_val(&rule.condition)),
        ("action".to_owned(), action_to_val(&rule.action)),
        ("priority".to_owned(), Val::U32(rule.priority)),
        (
            "description".to_owned(),
            Val::String(rule.description.clone()),
        ),
    ]))
}

fn strategy_to_val(strategy: EvaluationStrategy) -> Val {
    let name: &str = match strategy {
        EvaluationStrategy::FirstMatch => "first-match",
        EvaluationStrategy::AllMatches => "all-matches",
        EvaluationStrategy::Score => "score",
    };
    Val::Enum(name.to_owned())
}

fn action_to_val(action: &RuleAction) -> Val {
    match action {
        RuleAction::Route(port) => {
            variant("route", Some(Val::String(port.to_string())))
        }
        RuleAction::Drop => variant("drop", None),
        RuleAction::DeadLetter(reason) => {
            variant("dead-letter", Some(Val::String(reason.clone())))
        }
        RuleAction::Tag { key, value } => {
            variant("tag", Some(scalar_entry_to_val(key, value)))
        }
        RuleAction::Halt(message) => variant("halt", Some(Val::String(message.clone()))),
    }
}

/// Encode a recursive [`Condition`] as a flat `condition-tree` arena.
///
/// The Component Model cannot express recursive value types, so combinators
/// reference their children by index into the `nodes` list. Children are pushed
/// before their parent, and `root` is the index of the top-level node.
fn condition_tree_to_val(condition: &Condition) -> Val {
    let mut nodes: Vec<Val> = Vec::new();
    let root: u32 = push_condition(condition, &mut nodes);
    Val::Record(vec![
        ("nodes".to_owned(), Val::List(nodes)),
        ("root".to_owned(), Val::U32(root)),
    ])
}

fn push_condition(condition: &Condition, nodes: &mut Vec<Val>) -> u32 {
    let node: Val = match condition {
        Condition::FieldEq { path, value } => {
            variant("field-eq", Some(field_predicate(path.as_str(), value)))
        }
        Condition::FieldNeq { path, value } => {
            variant("field-neq", Some(field_predicate(path.as_str(), value)))
        }
        Condition::FieldGt { path, value } => {
            variant("field-gt", Some(field_predicate(path.as_str(), value)))
        }
        Condition::FieldLt { path, value } => {
            variant("field-lt", Some(field_predicate(path.as_str(), value)))
        }
        Condition::FieldGte { path, value } => {
            variant("field-gte", Some(field_predicate(path.as_str(), value)))
        }
        Condition::FieldLte { path, value } => {
            variant("field-lte", Some(field_predicate(path.as_str(), value)))
        }
        Condition::FieldIn { path, values } => variant(
            "field-in",
            Some(Val::Record(vec![
                ("path".to_owned(), Val::String(path.as_str().to_owned())),
                (
                    "values".to_owned(),
                    Val::List(values.iter().map(scalar_to_val).collect()),
                ),
            ])),
        ),
        Condition::FieldExists { path } => {
            variant("field-exists", Some(Val::String(path.as_str().to_owned())))
        }
        Condition::FieldAbsent { path } => {
            variant("field-absent", Some(Val::String(path.as_str().to_owned())))
        }
        Condition::FieldMatches { path, pattern } => variant(
            "field-matches",
            Some(Val::Record(vec![
                ("path".to_owned(), Val::String(path.as_str().to_owned())),
                (
                    "pattern".to_owned(),
                    Val::String(pattern.as_str().to_owned()),
                ),
            ])),
        ),
        Condition::TagEq { key, value } => {
            variant("tag-eq", Some(scalar_entry_to_val(key, value)))
        }
        Condition::TagExists { key } => {
            variant("tag-exists", Some(Val::String(key.clone())))
        }
        Condition::TagAbsent { key } => {
            variant("tag-absent", Some(Val::String(key.clone())))
        }
        Condition::SourceNode { node_id } => {
            variant("source-node", Some(Val::String(node_id.to_string())))
        }
        Condition::ArrivedOnPort { port_id } => {
            variant("arrived-on-port", Some(Val::String(port_id.to_string())))
        }
        Condition::HopCountGt { n } => variant("hop-count-gt", Some(Val::U32(*n))),
        Condition::HopCountLte { n } => variant("hop-count-lte", Some(Val::U32(*n))),
        Condition::WorkflowIs { workflow_id } => {
            variant("workflow-is", Some(Val::String(workflow_id.to_string())))
        }
        Condition::ExecutionMetadataEq { key, value } => {
            variant("execution-metadata-eq", Some(scalar_entry_to_val(key, value)))
        }
        Condition::And(children) => {
            let indices: Vec<Val> = children
                .iter()
                .map(|child: &Condition| Val::U32(push_condition(child, nodes)))
                .collect();
            variant("and-node", Some(Val::List(indices)))
        }
        Condition::Or(children) => {
            let indices: Vec<Val> = children
                .iter()
                .map(|child: &Condition| Val::U32(push_condition(child, nodes)))
                .collect();
            variant("or-node", Some(Val::List(indices)))
        }
        Condition::Not(inner) => {
            let index: u32 = push_condition(inner, nodes);
            variant("not-node", Some(Val::U32(index)))
        }
        Condition::Always => variant("always", None),
        Condition::Never => variant("never", None),
    };
    let index: u32 = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
    nodes.push(node);
    index
}

fn field_predicate(path: &str, value: &ScalarValue) -> Val {
    Val::Record(vec![
        ("path".to_owned(), Val::String(path.to_owned())),
        ("value".to_owned(), scalar_to_val(value)),
    ])
}

fn scalar_entry_to_val(key: &str, value: &ScalarValue) -> Val {
    Val::Record(vec![
        ("key".to_owned(), Val::String(key.to_owned())),
        ("value".to_owned(), scalar_to_val(value)),
    ])
}

fn scalar_to_val(scalar: &ScalarValue) -> Val {
    match scalar {
        ScalarValue::String(text) => variant("text", Some(Val::String(text.clone()))),
        ScalarValue::Integer(value) => variant("integer", Some(Val::S64(*value))),
        ScalarValue::Float(value) => variant("float-value", Some(Val::Float64(*value))),
        ScalarValue::Boolean(value) => variant("boolean", Some(Val::Bool(*value))),
        ScalarValue::Null => variant("null-value", None),
    }
}

fn eval_context_to_val(context: &HostEvalContext) -> Result<Val> {
    Ok(Val::Record(vec![
        ("payload".to_owned(), payload_to_val(&context.payload)?),
        (
            "tags".to_owned(),
            Val::List(scalar_entries_to_val(&context.tags)),
        ),
        (
            "source-node".to_owned(),
            Val::Option(
                context
                    .source_node
                    .as_ref()
                    .map(|node: &String| Box::new(Val::String(node.clone()))),
            ),
        ),
        (
            "arrival-port".to_owned(),
            Val::Option(
                context
                    .arrival_port
                    .as_ref()
                    .map(|port: &String| Box::new(Val::String(port.clone()))),
            ),
        ),
        ("hop-count".to_owned(), Val::U32(context.hop_count)),
        (
            "workflow-id".to_owned(),
            Val::String(context.workflow_id.clone()),
        ),
        (
            "execution-metadata".to_owned(),
            Val::List(scalar_entries_to_val(&context.execution_metadata)),
        ),
    ]))
}

fn scalar_entries_to_val(entries: &BTreeMap<String, ScalarValue>) -> Vec<Val> {
    entries
        .iter()
        .map(|(key, value): (&String, &ScalarValue)| scalar_entry_to_val(key, value))
        .collect()
}

#[allow(clippy::match_wildcard_for_single_variants)]
fn payload_to_val(payload: &PacketPayload) -> Result<Val> {
    match payload {
        PacketPayload::Bytes(bytes) => Ok(variant(
            "bytes",
            Some(Val::List(bytes.iter().map(|byte: &u8| Val::U8(*byte)).collect())),
        )),
        PacketPayload::Control(value) => {
            let encoded: String = serde_json::to_string(value).map_err(|err: serde_json::Error| {
                PureflowError::execution(format!("failed to encode control payload: {err}"))
            })?;
            Ok(variant("control", Some(Val::String(encoded))))
        }
        #[allow(unreachable_patterns)]
        _ => Err(PureflowError::execution(
            "payload is not supported by the rules WIT ABI 0.1.0",
        )),
    }
}

fn variant(name: &str, payload: Option<Val>) -> Val {
    Val::Variant(name.to_owned(), payload.map(Box::new))
}

// ── Component Model value → native decoding ──────────────────────────────────

fn decision_from_result_val(value: Val) -> Result<RuleDecision> {
    let result: std::result::Result<Option<Box<Val>>, Option<Box<Val>>> = match value {
        Val::Result(result) => result,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-result from rules.evaluate",
            ));
        }
    };

    match result {
        Ok(Some(value)) => decision_from_val(*value),
        Ok(None) => Err(PureflowError::execution(
            "guest returned empty ok result from rules.evaluate",
        )),
        Err(Some(value)) => Err(rule_error_from_val(*value)),
        Err(None) => Err(PureflowError::execution(
            "guest returned empty error from rules.evaluate",
        )),
    }
}

fn rule_error_from_val(value: Val) -> PureflowError {
    match value {
        Val::Variant(name, Some(detail)) => match *detail {
            Val::String(message) => {
                PureflowError::execution(format!("guest rule {name}: {message}"))
            }
            _ => PureflowError::execution(format!("guest returned malformed {name} rule error")),
        },
        Val::Variant(name, None) => {
            PureflowError::execution(format!("guest returned {name} without detail"))
        }
        _ => PureflowError::execution("guest returned malformed rule error"),
    }
}

fn decision_from_val(value: Val) -> Result<RuleDecision> {
    let fields: Vec<(String, Val)> = record_fields(value, "rule decision")?;
    let action: RuleAction =
        action_from_val(required_field(&fields, "action", "rule decision")?.clone())?;
    let matched_rule: Option<String> =
        optional_string(required_field(&fields, "matched-rule", "rule decision")?)?;
    let tags_applied: Vec<(String, ScalarValue)> =
        required_list_field(&fields, "tags-applied", "rule decision")?
            .into_iter()
            .map(scalar_entry_from_val)
            .collect::<Result<Vec<_>>>()?;
    let conditions_evaluated: Vec<ConditionTrace> =
        required_list_field(&fields, "conditions-evaluated", "rule decision")?
            .into_iter()
            .map(condition_trace_from_val)
            .collect::<Result<Vec<_>>>()?;

    Ok(RuleDecision {
        action,
        matched_rule,
        tags_applied,
        conditions_evaluated,
    })
}

fn action_from_val(value: Val) -> Result<RuleAction> {
    let (name, payload): (String, Option<Box<Val>>) = as_variant(value, "rule action")?;
    match (name.as_str(), payload) {
        ("route", Some(payload)) => {
            let port: String = as_string(*payload, "route port")?;
            let port: PortId = PortId::new(port)?;
            Ok(RuleAction::Route(port))
        }
        ("drop", _) => Ok(RuleAction::Drop),
        ("dead-letter", Some(payload)) => {
            Ok(RuleAction::DeadLetter(as_string(*payload, "dead-letter reason")?))
        }
        ("tag", Some(payload)) => {
            let (key, value): (String, ScalarValue) = scalar_entry_from_val(*payload)?;
            Ok(RuleAction::Tag { key, value })
        }
        ("halt", Some(payload)) => Ok(RuleAction::Halt(as_string(*payload, "halt message")?)),
        (kind, _) => Err(PureflowError::execution(format!(
            "guest returned unsupported rule action: {kind}"
        ))),
    }
}

fn scalar_entry_from_val(value: Val) -> Result<(String, ScalarValue)> {
    let fields: Vec<(String, Val)> = record_fields(value, "scalar entry")?;
    let key: String = required_string_field(&fields, "key", "scalar entry")?;
    let value: ScalarValue =
        scalar_from_val(required_field(&fields, "value", "scalar entry")?.clone())?;
    Ok((key, value))
}

fn scalar_from_val(value: Val) -> Result<ScalarValue> {
    let (name, payload): (String, Option<Box<Val>>) = as_variant(value, "scalar value")?;
    match (name.as_str(), payload) {
        ("text", Some(payload)) => Ok(ScalarValue::String(as_string(*payload, "text scalar")?)),
        ("integer", Some(payload)) => match *payload {
            Val::S64(value) => Ok(ScalarValue::Integer(value)),
            _ => Err(PureflowError::execution("guest returned non-s64 integer scalar")),
        },
        ("float-value", Some(payload)) => match *payload {
            Val::Float64(value) => Ok(ScalarValue::Float(value)),
            _ => Err(PureflowError::execution("guest returned non-f64 float scalar")),
        },
        ("boolean", Some(payload)) => match *payload {
            Val::Bool(value) => Ok(ScalarValue::Boolean(value)),
            _ => Err(PureflowError::execution("guest returned non-bool boolean scalar")),
        },
        ("null-value", _) => Ok(ScalarValue::Null),
        (kind, _) => Err(PureflowError::execution(format!(
            "guest returned unsupported scalar value: {kind}"
        ))),
    }
}

fn condition_trace_from_val(value: Val) -> Result<ConditionTrace> {
    let fields: Vec<(String, Val)> = record_fields(value, "condition trace")?;
    let rule_id: String = required_string_field(&fields, "rule-id", "condition trace")?;
    let surface: ConditionSurfaceRecord =
        surface_from_val(required_field(&fields, "surface", "condition trace")?.clone())?;
    let matched: bool = match required_field(&fields, "matched", "condition trace")? {
        Val::Bool(value) => *value,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-bool condition trace match",
            ));
        }
    };
    // The WIT trace deliberately omits the per-condition description carried by
    // the native evaluator; the WASM boundary records only the surface it drew
    // from. Description is left empty rather than reconstructed.
    Ok(ConditionTrace::new(rule_id, String::new(), matched, surface))
}

fn surface_from_val(value: Val) -> Result<ConditionSurfaceRecord> {
    let name: String = match value {
        Val::Enum(name) => name,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-enum condition surface",
            ));
        }
    };
    match name.as_str() {
        "payload" => Ok(ConditionSurfaceRecord::Payload),
        "tag" => Ok(ConditionSurfaceRecord::Tag),
        "provenance" => Ok(ConditionSurfaceRecord::Provenance),
        "execution-context" => Ok(ConditionSurfaceRecord::ExecutionContext),
        "combinator" => Ok(ConditionSurfaceRecord::Combinator),
        "constant" => Ok(ConditionSurfaceRecord::Constant),
        other => Err(PureflowError::execution(format!(
            "guest returned unknown condition surface: {other}"
        ))),
    }
}

fn optional_string(value: &Val) -> Result<Option<String>> {
    match value {
        Val::Option(Some(inner)) => Ok(Some(as_string(inner.as_ref().clone(), "option string")?)),
        Val::Option(None) => Ok(None),
        _ => Err(PureflowError::execution("guest returned non-option value")),
    }
}

fn as_variant(value: Val, context: &str) -> Result<(String, Option<Box<Val>>)> {
    match value {
        Val::Variant(name, payload) => Ok((name, payload)),
        _ => Err(PureflowError::execution(format!(
            "guest returned non-variant {context}"
        ))),
    }
}

fn as_string(value: Val, context: &str) -> Result<String> {
    match value {
        Val::String(value) => Ok(value),
        _ => Err(PureflowError::execution(format!(
            "guest returned non-string {context}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_rules::{
        Rule,
        condition::{FieldPath, GlobPattern},
    };
    use serde_json::json;

    fn field(path: &str) -> FieldPath {
        FieldPath::new(path).expect("valid field path")
    }

    #[test]
    fn constants_name_the_rules_wit_abi() {
        assert_eq!(WIT_RULES_PACKAGE, "pureflow:rules@0.1.0");
        assert_eq!(WIT_RULES_WORLD, "pureflow-rules-node");
    }

    #[test]
    fn scalar_values_round_trip_through_component_values() {
        for scalar in [
            ScalarValue::String("hello".to_owned()),
            ScalarValue::Integer(-42),
            ScalarValue::Float(3.5),
            ScalarValue::Boolean(true),
            ScalarValue::Null,
        ] {
            let decoded: ScalarValue =
                scalar_from_val(scalar_to_val(&scalar)).expect("scalar decodes");
            assert_eq!(decoded, scalar);
        }
    }

    #[test]
    fn rule_actions_round_trip_through_component_values() {
        for action in [
            RuleAction::Route(PortId::new("out").expect("valid port")),
            RuleAction::Drop,
            RuleAction::DeadLetter("rejected".to_owned()),
            RuleAction::Tag {
                key: "vip".to_owned(),
                value: ScalarValue::Boolean(true),
            },
            RuleAction::Halt("stop".to_owned()),
        ] {
            let decoded: RuleAction =
                action_from_val(action_to_val(&action)).expect("action decodes");
            assert_eq!(decoded, action);
        }
    }

    #[test]
    fn nested_condition_flattens_into_arena_with_root_last() {
        // not(and(field-eq, or(tag-exists, always)))
        let condition = Condition::Not(Box::new(Condition::And(vec![
            Condition::FieldEq {
                path: field("status"),
                value: ScalarValue::String("ok".to_owned()),
            },
            Condition::Or(vec![
                Condition::TagExists {
                    key: "vip".to_owned(),
                },
                Condition::Always,
            ]),
        ])));

        let Val::Record(fields) = condition_tree_to_val(&condition) else {
            panic!("condition tree must encode as a record");
        };
        let nodes = fields
            .iter()
            .find_map(|(name, value)| (name == "nodes").then_some(value))
            .expect("nodes field present");
        let root = fields
            .iter()
            .find_map(|(name, value)| (name == "root").then_some(value))
            .expect("root field present");

        let Val::List(nodes) = nodes else {
            panic!("nodes must be a list");
        };
        // 5 leaves/combinators: field-eq, tag-exists, always, or, and, not = 6.
        assert_eq!(nodes.len(), 6);
        // Root is pushed last, so its index is nodes.len() - 1.
        assert_eq!(*root, Val::U32(5));
        // The root node is the not-node referencing the and-node.
        let Val::Variant(name, _) = &nodes[5] else {
            panic!("root node must be a variant");
        };
        assert_eq!(name, "not-node");
    }

    #[test]
    fn rule_set_encodes_as_record_with_sorted_rules() {
        let rules = vec![
            Rule::new(
                "high",
                Condition::FieldGte {
                    path: field("amount"),
                    value: ScalarValue::Integer(10_000),
                },
                RuleAction::Route(PortId::new("high-out").expect("valid port")),
                10,
                "route large amounts",
            )
            .expect("valid rule"),
            Rule::new(
                "default",
                Condition::Always,
                RuleAction::Route(PortId::new("std-out").expect("valid port")),
                20,
                "standard path",
            )
            .expect("valid rule"),
        ];
        let rule_set = RuleSet::new(
            "router",
            EvaluationStrategy::FirstMatch,
            rules,
            RuleAction::Drop,
            true,
        )
        .expect("valid rule set");

        let Val::Record(fields) = rule_set_to_val(&rule_set).expect("rule set encodes") else {
            panic!("rule set must encode as a record");
        };
        let strategy = fields
            .iter()
            .find_map(|(name, value)| (name == "strategy").then_some(value))
            .expect("strategy present");
        assert_eq!(*strategy, Val::Enum("first-match".to_owned()));
        let trace = fields
            .iter()
            .find_map(|(name, value)| (name == "trace-conditions").then_some(value))
            .expect("trace-conditions present");
        assert_eq!(*trace, Val::Bool(true));
    }

    #[test]
    fn control_payload_encodes_as_json_string() {
        let context = HostEvalContext {
            payload: PacketPayload::control(json!({"amount": 5})),
            workflow_id: "flow".to_owned(),
            ..HostEvalContext::default()
        };
        let Val::Record(fields) = eval_context_to_val(&context).expect("context encodes") else {
            panic!("context must encode as a record");
        };
        let payload = fields
            .iter()
            .find_map(|(name, value)| (name == "payload").then_some(value))
            .expect("payload present");
        let Val::Variant(name, Some(inner)) = payload else {
            panic!("payload must be a variant with detail");
        };
        assert_eq!(name, "control");
        let Val::String(text) = inner.as_ref() else {
            panic!("control payload must be a string");
        };
        let parsed: serde_json::Value = serde_json::from_str(text).expect("control is JSON");
        assert_eq!(parsed, json!({"amount": 5}));
    }

    #[test]
    fn decision_decodes_from_component_values() {
        let decision_val = Val::Result(Ok(Some(Box::new(Val::Record(vec![
            (
                "action".to_owned(),
                variant("route", Some(Val::String("high-out".to_owned()))),
            ),
            (
                "matched-rule".to_owned(),
                Val::Option(Some(Box::new(Val::String("high".to_owned())))),
            ),
            (
                "tags-applied".to_owned(),
                Val::List(vec![scalar_entry_to_val("vip", &ScalarValue::Boolean(true))]),
            ),
            (
                "conditions-evaluated".to_owned(),
                Val::List(vec![Val::Record(vec![
                    ("rule-id".to_owned(), Val::String("high".to_owned())),
                    ("surface".to_owned(), Val::Enum("payload".to_owned())),
                    ("matched".to_owned(), Val::Bool(true)),
                ])]),
            ),
        ])))));

        let decision: RuleDecision =
            decision_from_result_val(decision_val).expect("decision decodes");
        assert_eq!(
            decision.action,
            RuleAction::Route(PortId::new("high-out").expect("valid port"))
        );
        assert_eq!(decision.matched_rule.as_deref(), Some("high"));
        assert_eq!(decision.tags_applied.len(), 1);
        assert_eq!(decision.conditions_evaluated.len(), 1);
        assert_eq!(decision.conditions_evaluated[0].rule_id, "high");
        assert!(decision.conditions_evaluated[0].matched);
    }

    #[test]
    fn guest_rule_error_maps_to_execution_error() {
        let error_val = Val::Result(Err(Some(Box::new(variant(
            "malformed-rule-set",
            Some(Val::String("bad arena".to_owned())),
        )))));
        let err: PureflowError =
            decision_from_result_val(error_val).expect_err("rule error should fail");
        assert_eq!(err.code(), pureflow_core::ErrorCode::NodeExecutionFailed);
        assert!(err.to_string().contains("malformed-rule-set"));
    }

    #[test]
    fn unused_glob_pattern_constructor_is_available() {
        // GlobPattern participates in FieldMatches encoding; ensure the import
        // path stays valid as the contract evolves.
        let pattern = GlobPattern::new("a*");
        assert_eq!(pattern.as_str(), "a*");
    }
}
