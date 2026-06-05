//! Sample Pureflow rule-evaluation WASM guest.
//!
//! Implements `pureflow:rules/rules.evaluate` over the flattened condition-tree
//! arena defined in `wit/pureflow-rules.wit`. The guest evaluates a rule set
//! against one packet context and returns a routing decision; it never touches
//! a channel, port, or send handle — all packet movement stays on the host.
//!
//! The evaluation semantics mirror the native `pureflow_rules` evaluator so a
//! WASM rule node and a native rule node produce equivalent outcomes for the
//! same rule set. Like the uppercase batch guest, this component is `no_std`
//! and import-free so the world imports nothing and the host capability check
//! can keep it sandboxed.

#![cfg_attr(target_arch = "wasm32", no_std)]

#[cfg(target_arch = "wasm32")]
extern crate alloc;

// Point at the specific package file rather than the `wit/` directory: the
// directory holds two independent packages (pureflow:batch and pureflow:rules)
// as sibling files, which wit-bindgen's directory parser rejects.
wit_bindgen::generate!({
    path: "../../wit/pureflow-rules.wit",
    world: "pureflow-rules-node",
});

#[cfg(target_arch = "wasm32")]
use alloc::vec::Vec;
#[cfg(target_arch = "wasm32")]
use core::{
    alloc::{GlobalAlloc, Layout},
    panic::PanicInfo,
    ptr,
};

use serde_json::Value;

use exports::pureflow::rules::rules::{
    ConditionNode, ConditionSurface, ConditionTrace, EvalContext, EvaluationStrategy, Guest,
    PacketPayload, Rule, RuleAction, RuleDecision, RuleError, ScalarEntry, ScalarValue,
};

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOCATOR: BumpAllocator = BumpAllocator;

#[cfg(target_arch = "wasm32")]
const WASM_PAGE_SIZE: usize = 64 * 1024;

#[cfg(target_arch = "wasm32")]
static mut HEAP_NEXT: usize = 0;

#[cfg(target_arch = "wasm32")]
unsafe extern "C" {
    static __heap_base: u8;
}

#[cfg(target_arch = "wasm32")]
struct BumpAllocator;

#[cfg(target_arch = "wasm32")]
unsafe impl GlobalAlloc for BumpAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some(align) = normalize_align(layout.align()) else {
            return ptr::null_mut();
        };
        let size = layout.size();
        if size == 0 {
            return align as *mut u8;
        }

        let heap_next = unsafe {
            if HEAP_NEXT == 0 {
                HEAP_NEXT = heap_base();
            }
            HEAP_NEXT
        };
        let Some(aligned) = align_up(heap_next, align) else {
            return ptr::null_mut();
        };
        let Some(next) = aligned.checked_add(size) else {
            return ptr::null_mut();
        };
        if !grow_memory_to(next) {
            return ptr::null_mut();
        }
        unsafe {
            HEAP_NEXT = next;
        }

        aligned as *mut u8
    }

    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {}

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size == 0 {
            return layout.align() as *mut u8;
        }
        let new_ptr =
            unsafe { self.alloc(Layout::from_size_align_unchecked(new_size, layout.align())) };
        if !new_ptr.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(ptr, new_ptr, layout.size().min(new_size));
            }
        }
        new_ptr
    }
}

#[cfg(target_arch = "wasm32")]
fn align_up(value: usize, align: usize) -> Option<usize> {
    value
        .checked_add(align.checked_sub(1)?)
        .map(|value| value & !(align - 1))
}

#[cfg(target_arch = "wasm32")]
fn normalize_align(align: usize) -> Option<usize> {
    let align = align.max(1);
    if align.is_power_of_two() {
        Some(align)
    } else {
        align.checked_next_power_of_two()
    }
}

#[cfg(target_arch = "wasm32")]
fn heap_base() -> usize {
    ptr::addr_of!(__heap_base) as usize
}

#[cfg(target_arch = "wasm32")]
fn grow_memory_to(required_end: usize) -> bool {
    let current_pages = core::arch::wasm32::memory_size(0);
    let current_size = current_pages.saturating_mul(WASM_PAGE_SIZE);
    if required_end <= current_size {
        return true;
    }

    let Some(additional_bytes) = required_end.checked_sub(current_size) else {
        return false;
    };
    let additional_pages = additional_bytes.div_ceil(WASM_PAGE_SIZE);
    core::arch::wasm32::memory_grow(0, additional_pages) != usize::MAX
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
unsafe extern "C" fn cabi_realloc(
    old_ptr: *mut u8,
    old_len: usize,
    align: usize,
    new_len: usize,
) -> *mut u8 {
    let Some(align) = normalize_align(align) else {
        return ptr::null_mut();
    };
    if old_len == 0 {
        if new_len == 0 {
            return align as *mut u8;
        }
        let layout = unsafe { Layout::from_size_align_unchecked(new_len, align) };
        return unsafe { ALLOCATOR.alloc(layout) };
    }

    let layout = unsafe { Layout::from_size_align_unchecked(old_len, align) };
    unsafe { ALLOCATOR.realloc(old_ptr, layout, new_len) }
}

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &PanicInfo<'_>) -> ! {
    core::arch::wasm32::unreachable()
}

// serde_json's slice comparisons lower to `memcmp`, which the import-free
// `no_std` sysroot does not provide (compiler-builtins ships it only behind the
// `mem` feature). Supplying it here keeps the component free of an `env` import.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
unsafe extern "C" fn memcmp(a: *const u8, b: *const u8, n: usize) -> i32 {
    let mut index: usize = 0;
    while index < n {
        let lhs: u8 = unsafe { *a.add(index) };
        let rhs: u8 = unsafe { *b.add(index) };
        if lhs != rhs {
            return i32::from(lhs) - i32::from(rhs);
        }
        index += 1;
    }
    0
}

struct RulesGuest;

impl Guest for RulesGuest {
    fn evaluate(
        rule_set: exports::pureflow::rules::rules::RuleSet,
        context: EvalContext,
    ) -> Result<RuleDecision, RuleError> {
        // Parse the control payload once. Bytes payloads expose no fields, so
        // payload-surface conditions never match against them — matching the
        // native evaluator, which resolves fields only for control payloads.
        let payload: Option<Value> = match &context.payload {
            PacketPayload::Control(text) => serde_json::from_str(text).ok(),
            PacketPayload::Bytes(_) => None,
        };
        let trace: bool = rule_set.trace_conditions;
        let mut traces: Vec<ConditionTrace> = Vec::new();
        let mut tags_applied: Vec<ScalarEntry> = Vec::new();

        match rule_set.strategy {
            EvaluationStrategy::FirstMatch => {
                for rule in &rule_set.rules {
                    let matched = eval_rule(rule, &payload, &context);
                    if trace {
                        traces.push(trace_of(rule, matched));
                    }
                    if matched {
                        if let RuleAction::Tag(entry) = &rule.action {
                            tags_applied.push(entry.clone());
                        }
                        if is_terminal(&rule.action) {
                            return Ok(RuleDecision {
                                action: rule.action.clone(),
                                matched_rule: Some(rule.id.clone()),
                                tags_applied,
                                conditions_evaluated: traces,
                            });
                        }
                    }
                }
                Ok(default_decision(
                    rule_set.default_action,
                    tags_applied,
                    traces,
                ))
            }
            EvaluationStrategy::AllMatches => {
                for rule in &rule_set.rules {
                    let matched = eval_rule(rule, &payload, &context);
                    if trace {
                        traces.push(trace_of(rule, matched));
                    }
                    if matched {
                        if let RuleAction::Tag(entry) = &rule.action {
                            tags_applied.push(entry.clone());
                        }
                    }
                }
                Ok(default_decision(
                    rule_set.default_action,
                    tags_applied,
                    traces,
                ))
            }
            EvaluationStrategy::Score => {
                let mut best: Option<&Rule> = None;
                for rule in &rule_set.rules {
                    let matched = eval_rule(rule, &payload, &context);
                    if trace {
                        traces.push(trace_of(rule, matched));
                    }
                    if matched {
                        if let RuleAction::Tag(entry) = &rule.action {
                            tags_applied.push(entry.clone());
                        }
                        if best.is_none() && is_terminal(&rule.action) {
                            best = Some(rule);
                        }
                    }
                }
                match best {
                    Some(rule) => Ok(RuleDecision {
                        action: rule.action.clone(),
                        matched_rule: Some(rule.id.clone()),
                        tags_applied,
                        conditions_evaluated: traces,
                    }),
                    None => Ok(default_decision(
                        rule_set.default_action,
                        tags_applied,
                        traces,
                    )),
                }
            }
        }
    }
}

fn default_decision(
    action: RuleAction,
    tags_applied: Vec<ScalarEntry>,
    conditions_evaluated: Vec<ConditionTrace>,
) -> RuleDecision {
    RuleDecision {
        action,
        matched_rule: None,
        tags_applied,
        conditions_evaluated,
    }
}

fn is_terminal(action: &RuleAction) -> bool {
    !matches!(action, RuleAction::Tag(_))
}

fn eval_rule(rule: &Rule, payload: &Option<Value>, ctx: &EvalContext) -> bool {
    eval_node(&rule.condition.nodes, rule.condition.root, payload, ctx)
}

fn eval_node(
    nodes: &[ConditionNode],
    index: u32,
    payload: &Option<Value>,
    ctx: &EvalContext,
) -> bool {
    let Some(node) = nodes.get(index as usize) else {
        return false;
    };
    match node {
        ConditionNode::FieldEq(p) => resolve(payload, &p.path)
            .map(|v| json_eq(v, &p.value))
            .unwrap_or(false),
        ConditionNode::FieldNeq(p) => resolve(payload, &p.path)
            .map(|v| !json_eq(v, &p.value))
            .unwrap_or(false),
        ConditionNode::FieldGt(p) => resolve(payload, &p.path)
            .and_then(|v| json_cmp(v, &p.value))
            .map(|o| o == core::cmp::Ordering::Greater)
            .unwrap_or(false),
        ConditionNode::FieldLt(p) => resolve(payload, &p.path)
            .and_then(|v| json_cmp(v, &p.value))
            .map(|o| o == core::cmp::Ordering::Less)
            .unwrap_or(false),
        ConditionNode::FieldGte(p) => resolve(payload, &p.path)
            .and_then(|v| json_cmp(v, &p.value))
            .map(|o| o != core::cmp::Ordering::Less)
            .unwrap_or(false),
        ConditionNode::FieldLte(p) => resolve(payload, &p.path)
            .and_then(|v| json_cmp(v, &p.value))
            .map(|o| o != core::cmp::Ordering::Greater)
            .unwrap_or(false),
        ConditionNode::FieldIn(p) => resolve(payload, &p.path)
            .map(|v| p.values.iter().any(|s| json_eq(v, s)))
            .unwrap_or(false),
        ConditionNode::FieldExists(path) => resolve(payload, path).is_some(),
        ConditionNode::FieldAbsent(path) => {
            resolve(payload, path).map(Value::is_null).unwrap_or(true)
        }
        ConditionNode::FieldMatches(p) => resolve(payload, &p.path)
            .and_then(Value::as_str)
            .map(|s| glob_match(&p.pattern, s))
            .unwrap_or(false),
        ConditionNode::TagEq(entry) => tag_value(&ctx.tags, &entry.key)
            .map(|v| scalar_eq(v, &entry.value))
            .unwrap_or(false),
        ConditionNode::TagExists(key) => tag_value(&ctx.tags, key).is_some(),
        ConditionNode::TagAbsent(key) => tag_value(&ctx.tags, key).is_none(),
        ConditionNode::SourceNode(node_id) => {
            ctx.source_node.as_deref() == Some(node_id.as_str())
        }
        ConditionNode::ArrivedOnPort(port_id) => {
            ctx.arrival_port.as_deref() == Some(port_id.as_str())
        }
        ConditionNode::HopCountGt(n) => ctx.hop_count > *n,
        ConditionNode::HopCountLte(n) => ctx.hop_count <= *n,
        ConditionNode::WorkflowIs(id) => ctx.workflow_id == *id,
        ConditionNode::ExecutionMetadataEq(entry) => tag_value(&ctx.execution_metadata, &entry.key)
            .map(|v| scalar_eq(v, &entry.value))
            .unwrap_or(false),
        ConditionNode::AndNode(children) => children
            .iter()
            .all(|c| eval_node(nodes, *c, payload, ctx)),
        ConditionNode::OrNode(children) => children
            .iter()
            .any(|c| eval_node(nodes, *c, payload, ctx)),
        ConditionNode::NotNode(child) => !eval_node(nodes, *child, payload, ctx),
        ConditionNode::Always => true,
        ConditionNode::Never => false,
    }
}

fn trace_of(rule: &Rule, matched: bool) -> ConditionTrace {
    let surface = rule
        .condition
        .nodes
        .get(rule.condition.root as usize)
        .map(surface_of)
        .unwrap_or(ConditionSurface::Constant);
    ConditionTrace {
        rule_id: rule.id.clone(),
        surface,
        matched,
    }
}

fn surface_of(node: &ConditionNode) -> ConditionSurface {
    match node {
        ConditionNode::FieldEq(_)
        | ConditionNode::FieldNeq(_)
        | ConditionNode::FieldGt(_)
        | ConditionNode::FieldLt(_)
        | ConditionNode::FieldGte(_)
        | ConditionNode::FieldLte(_)
        | ConditionNode::FieldIn(_)
        | ConditionNode::FieldExists(_)
        | ConditionNode::FieldAbsent(_)
        | ConditionNode::FieldMatches(_) => ConditionSurface::Payload,
        ConditionNode::TagEq(_) | ConditionNode::TagExists(_) | ConditionNode::TagAbsent(_) => {
            ConditionSurface::Tag
        }
        ConditionNode::SourceNode(_)
        | ConditionNode::ArrivedOnPort(_)
        | ConditionNode::HopCountGt(_)
        | ConditionNode::HopCountLte(_) => ConditionSurface::Provenance,
        ConditionNode::WorkflowIs(_) | ConditionNode::ExecutionMetadataEq(_) => {
            ConditionSurface::ExecutionContext
        }
        ConditionNode::AndNode(_) | ConditionNode::OrNode(_) | ConditionNode::NotNode(_) => {
            ConditionSurface::Combinator
        }
        ConditionNode::Always | ConditionNode::Never => ConditionSurface::Constant,
    }
}

fn resolve<'a>(payload: &'a Option<Value>, path: &str) -> Option<&'a Value> {
    let mut current = payload.as_ref()?;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn json_eq(json: &Value, scalar: &ScalarValue) -> bool {
    match (json, scalar) {
        (Value::String(s), ScalarValue::Text(r)) => s == r,
        (Value::Number(n), ScalarValue::Integer(i)) => n.as_i64() == Some(*i),
        (Value::Number(n), ScalarValue::FloatValue(f)) => n.as_f64().map(|v| v == *f).unwrap_or(false),
        (Value::Bool(b), ScalarValue::Boolean(r)) => b == r,
        (Value::Null, ScalarValue::NullValue) => true,
        _ => false,
    }
}

fn json_cmp(json: &Value, scalar: &ScalarValue) -> Option<core::cmp::Ordering> {
    let json_f: f64 = json.as_f64()?;
    let scalar_f: f64 = match scalar {
        ScalarValue::Integer(i) => *i as f64,
        ScalarValue::FloatValue(f) => *f,
        _ => return None,
    };
    json_f.partial_cmp(&scalar_f)
}

fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Text(x), ScalarValue::Text(y)) => x == y,
        (ScalarValue::Integer(x), ScalarValue::Integer(y)) => x == y,
        (ScalarValue::FloatValue(x), ScalarValue::FloatValue(y)) => x == y,
        (ScalarValue::Boolean(x), ScalarValue::Boolean(y)) => x == y,
        (ScalarValue::NullValue, ScalarValue::NullValue) => true,
        _ => false,
    }
}

fn tag_value<'a>(entries: &'a [ScalarEntry], key: &str) -> Option<&'a ScalarValue> {
    entries
        .iter()
        .find(|entry| entry.key == key)
        .map(|entry| &entry.value)
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

export!(RulesGuest);
