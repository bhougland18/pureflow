//! Public API regression check for runtime substrate type leaks.

use std::fs;
use std::path::{Path, PathBuf};

const LEAK_PATTERNS: &[&str] = &["asupersync::", "wasmtime::", " mpsc::"];

const LEAK_IDENTIFIERS: &[&str] = &[
    "AsupersyncRuntime",
    "Config",
    "Component",
    "ComponentExportIndex",
    "Cx",
    "Engine",
    "Func",
    "Instance",
    "Linker",
    "RecvError",
    "Runtime",
    "RuntimeBuilder",
    "SendPermit",
    "SendError",
    "Store",
    "Val",
    "WasmtimeBatchComponent",
    "WasmtimeExecutionLimits",
];

#[test]
fn public_api_does_not_expose_runtime_substrate_types() {
    let workspace_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("pureflow-core should live under crates/")
        .to_path_buf();
    let mut leaks: Vec<String> = Vec::new();

    for source_file in rust_source_files(&workspace_root.join("crates")) {
        let source: String = fs::read_to_string(&source_file).expect("source file should read");
        let mut public_item: String = String::new();

        for line in source.lines() {
            let trimmed: &str = line.trim_start();
            if starts_public_item(trimmed) {
                public_item = trimmed.to_owned();
            } else if !public_item.is_empty() && !trimmed.is_empty() {
                public_item.push(' ');
                public_item.push_str(trimmed);
            }

            if !public_item.is_empty() && (public_item.contains('{') || public_item.contains(';')) {
                if contains_forbidden_substrate_type(&source_file, &public_item) {
                    leaks.push(format!("{}: {public_item}", source_file.display()));
                }
                public_item.clear();
            }
        }
    }

    assert!(
        leaks.is_empty(),
        "public API exposes runtime substrate types:\n{}",
        leaks.join("\n")
    );
}

fn rust_source_files(root: &Path) -> Vec<PathBuf> {
    let mut pending: Vec<PathBuf> = vec![root.to_path_buf()];
    let mut files: Vec<PathBuf> = Vec::new();

    while let Some(path) = pending.pop() {
        if path
            .components()
            .any(|component| component.as_os_str() == "target")
        {
            continue;
        }

        let metadata = fs::metadata(&path).expect("path metadata should read");
        if metadata.is_dir() {
            for entry in fs::read_dir(&path).expect("directory should read") {
                pending.push(entry.expect("directory entry should read").path());
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }

    files
}

fn starts_public_item(line: &str) -> bool {
    line.starts_with("pub struct ")
        || line.starts_with("pub enum ")
        || line.starts_with("pub trait ")
        || line.starts_with("pub type ")
        || line.starts_with("pub fn ")
        || line.starts_with("pub const fn ")
        || line.starts_with("pub async fn ")
        || line.starts_with("pub use ")
}

fn contains_forbidden_substrate_type(source_file: &Path, item: &str) -> bool {
    contains_substrate_type(item) && !is_intentional_adapter_public_item(source_file, item)
}

fn contains_substrate_type(item: &str) -> bool {
    LEAK_PATTERNS
        .iter()
        .any(|pattern: &&str| item.contains(pattern))
        || LEAK_IDENTIFIERS
            .iter()
            .any(|identifier: &&str| contains_identifier(item, identifier))
}

fn is_intentional_adapter_public_item(source_file: &Path, item: &str) -> bool {
    let Some(crate_name) = crate_name(source_file) else {
        return false;
    };

    match crate_name.as_str() {
        "pureflow-runtime" => item.contains("AsupersyncRuntime"),
        "pureflow-wasm" => {
            item.contains("WasmtimeBatchComponent") || item.contains("WasmtimeExecutionLimits")
        }
        _ => false,
    }
}

fn crate_name(source_file: &Path) -> Option<String> {
    let mut components = source_file.components();
    while let Some(component) = components.next() {
        if component.as_os_str() == "crates" {
            return components
                .next()
                .map(|crate_component| crate_component.as_os_str().to_string_lossy().into_owned());
        }
    }

    None
}

fn contains_identifier(item: &str, identifier: &str) -> bool {
    item.split(|character: char| !(character == '_' || character.is_ascii_alphanumeric()))
        .any(|token: &str| token == identifier)
}

#[test]
fn substrate_detection_catches_qualified_asupersync_type() {
    let item = "pub struct Foo { inner: asupersync::Runtime }";

    assert!(contains_substrate_type(item));
}

#[test]
fn substrate_detection_catches_qualified_wasmtime_type() {
    let item = "pub fn build_store() -> wasmtime::Store<()>;";

    assert!(contains_substrate_type(item));
}

#[test]
fn substrate_detection_catches_bare_imported_substrate_type() {
    let item = "pub struct Foo { inner: Runtime }";

    assert!(contains_substrate_type(item));
}

#[test]
fn substrate_detection_does_not_match_unrelated_identifier_substrings() {
    let item = "pub struct WorkflowRuntime { name: String }";

    assert!(!contains_substrate_type(item));
}

#[test]
fn adapter_public_wrapper_names_are_allowed_only_in_adapter_crates() {
    let runtime_file = PathBuf::from("crates/pureflow-runtime/src/lib.rs");
    let wasm_file = PathBuf::from("crates/pureflow-wasm/src/lib.rs");
    let core_file = PathBuf::from("crates/pureflow-core/src/lib.rs");

    assert!(!contains_forbidden_substrate_type(
        &runtime_file,
        "pub struct AsupersyncRuntime {"
    ));
    assert!(!contains_forbidden_substrate_type(
        &wasm_file,
        "pub struct WasmtimeBatchComponent {"
    ));
    assert!(contains_forbidden_substrate_type(
        &core_file,
        "pub struct AsupersyncRuntime {"
    ));
}
