//! Validates the rule-evaluation WIT contract (pu-bm4).
//!
//! Parses `wit/pureflow-rules.wit` with `wit-parser` — the same front end
//! `wit-bindgen` uses — and asserts the boundary shape: the core records exist,
//! the only function is `evaluate`, and no channel/port/send/recv types leak
//! across the host/guest boundary.

use std::path::Path;

use wit_parser::Resolve;

fn load() -> Resolve {
    let mut resolve = Resolve::new();
    let wit = Path::new(env!("CARGO_MANIFEST_DIR")).join("wit/pureflow-rules.wit");
    resolve
        .push_path(&wit)
        .unwrap_or_else(|e| panic!("rules WIT should parse under wit-parser: {e}"));
    resolve
}

#[test]
fn rules_wit_parses_and_exposes_core_records() {
    let resolve = load();

    let type_names: Vec<&str> = resolve
        .types
        .iter()
        .filter_map(|(_, ty)| ty.name.as_deref())
        .collect();

    for required in [
        "rule-set",
        "eval-context",
        "rule-decision",
        "rule",
        "rule-action",
        "condition-tree",
        "condition-node",
        "scalar-value",
    ] {
        assert!(
            type_names.contains(&required),
            "WIT should define `{required}`; found {type_names:?}"
        );
    }
}

#[test]
fn rules_interface_only_evaluates() {
    let resolve = load();
    let (_, rules) = resolve
        .interfaces
        .iter()
        .find(|(_, iface)| iface.name.as_deref() == Some("rules"))
        .expect("interface `rules` should exist");

    let funcs: Vec<&str> = rules.functions.keys().map(String::as_str).collect();
    assert_eq!(
        funcs,
        ["evaluate"],
        "the only boundary function must be `evaluate`"
    );
}

#[test]
fn no_channel_or_port_types_cross_the_boundary() {
    let resolve = load();
    // The host owns all packet movement; no boundary type may name a channel,
    // port, or send/recv concept.
    for (_, ty) in resolve.types.iter() {
        if let Some(name) = ty.name.as_deref() {
            let lower = name.to_ascii_lowercase();
            for banned in [
                "channel",
                "port-batch",
                "send",
                "recv",
                "stream",
                "writer",
                "reader",
            ] {
                assert!(
                    !lower.contains(banned),
                    "WIT type `{name}` must not expose channel/port access (`{banned}`)"
                );
            }
        }
    }
}

#[test]
fn world_exports_rules_and_imports_nothing() {
    let resolve = load();
    let (_, world) = resolve
        .worlds
        .iter()
        .find(|(_, world)| world.name == "pureflow-rules-node")
        .expect("world `pureflow-rules-node` should exist");

    assert!(
        world.imports.is_empty(),
        "the rules world needs no imports (no host effects)"
    );
    assert_eq!(
        world.exports.len(),
        1,
        "the rules world should export exactly the rules interface"
    );
}
