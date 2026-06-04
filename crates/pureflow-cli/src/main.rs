//! CLI entrypoint for Pureflow workflow validation, inspection, and scaffold runs.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fmt::{self, Write as FmtWrite},
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
};

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{Shell, generate};
use pureflow_contract::{
    ContractValidationError, Determinism, ExecutionMode, NodeContract, PortContract,
};
use pureflow_core::{
    PureflowError, ErrorVisibility, JsonlMetadataSink, NodeExecutor, PacketPayload, PortPacket,
    PortRecvError, PortsIn, PortsOut, RetryDisposition, TieredMetadataSink,
    capability::{
        CapabilityValidationError, NodeCapabilities, PortCapability, PortCapabilityDirection,
    },
    context::{CancellationToken, ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::{
    BatchNodeExecutor, CycleRunPolicy, FeedbackLoopStartup, FeedbackLoopTermination,
    NodeExecutorRegistry, StaticNodeExecutorRegistry, WorkflowDeadlockDiagnostic,
    WorkflowRunSummary, WorkflowTerminalState,
    run_workflow_with_registry_and_metadata_sink_summary,
};
use pureflow_introspection::{
    IntrospectionJsonError, WorkflowIntrospection, introspect_workflow,
    workflow_introspection_to_json_string,
};
use pureflow_runtime::AsupersyncRuntime;
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId};
use pureflow_wasm::{WasmtimeBatchComponent, WasmtimeExecutionLimits};
use pureflow_workflow::{EdgeCapacity, NodeDefinition, PortDirection, WorkflowDefinition};
use pureflow_workflow_format::{
    CURRENT_PUREFLOW_VERSION, WorkflowJsonError, WorkflowTomlError, WorkflowYamlError,
    workflow_from_json_str, workflow_from_toml_str, workflow_from_yaml_str,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing_subscriber::{
    filter::{ParseError, Targets},
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

type CliResult<T> = Result<T, CliError>;

const PUREFLOW_TRACE_ENV: &str = "PUREFLOW_TRACE";
const LEGACY_CONDUIT_TRACE_ENV: &str = "CONDUIT_TRACE";
const RUST_LOG_ENV: &str = "RUST_LOG";

#[derive(Debug, Parser)]
#[command(name = "pureflow", about = "Pureflow workflow engine CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Validate a workflow JSON document.
    Validate {
        /// Path to the workflow JSON file.
        workflow: PathBuf,
    },
    /// Inspect workflow topology, contracts, and capabilities as JSON.
    Inspect {
        /// Path to the workflow JSON file.
        workflow: PathBuf,
    },
    /// Explain workflow topology and metadata policy.
    Explain {
        /// Path to the workflow JSON file.
        workflow: PathBuf,
    },
    /// Run a workflow and write metadata JSONL.
    Run {
        /// Emit a machine-facing JSON summary to stdout.
        #[arg(long)]
        json: bool,
        /// Path to a WASM component manifest JSON file.
        #[arg(long, value_name = "COMPONENTS")]
        wasm_components: Option<PathBuf>,
        /// Path to the workflow JSON file.
        workflow: PathBuf,
        /// Path to write metadata JSONL output.
        metadata: PathBuf,
    },
    /// Validate a WASM component manifest JSON file.
    ValidateManifest {
        /// Path to the workflow JSON file; checks that all manifest nodes exist in the workflow.
        #[arg(long, value_name = "WORKFLOW")]
        workflow: Option<PathBuf>,
        /// Path to the WASM component manifest JSON file.
        manifest: PathBuf,
    },
    /// Emit JSON Schema for workflow and WASM manifest authoring.
    Schema {
        /// Schema document to emit.
        schema: SchemaKind,
    },
    /// Generate shell completion scripts.
    Completions {
        /// Shell to generate completions for.
        shell: Shell,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SchemaKind {
    /// Workflow document schema.
    Workflow,
    /// WASM component manifest schema.
    WasmManifest,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(error.exit_code());
    }
}

fn run() -> CliResult<()> {
    initialize_tracing_from_env()?;
    let cli = Cli::try_parse().unwrap_or_else(|err| err.exit());
    let output = dispatch_command(&cli.command, read_file, read_bytes_file, write_file)?;
    print!("{output}");
    Ok(())
}

fn initialize_tracing_from_env() -> CliResult<()> {
    let Some(targets): Option<Targets> =
        tracing_targets_from_env(|name: &str| env::var(name).ok())?
    else {
        return Ok(());
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false),
        )
        .with(targets)
        .try_init()
        .map_err(|source: TryInitError| {
            CliError::Tracing(format!("failed to initialize tracing subscriber: {source}"))
        })
}

fn tracing_targets_from_env(
    read_env: impl Fn(&str) -> Option<String>,
) -> CliResult<Option<Targets>> {
    if let Some(value) = read_env(PUREFLOW_TRACE_ENV) {
        return tracing_targets_from_value(PUREFLOW_TRACE_ENV, &value);
    }
    if let Some(value) = read_env(LEGACY_CONDUIT_TRACE_ENV) {
        return tracing_targets_from_value(LEGACY_CONDUIT_TRACE_ENV, &value);
    }
    if let Some(value) = read_env(RUST_LOG_ENV) {
        return tracing_targets_from_value(RUST_LOG_ENV, &value);
    }

    Ok(None)
}

fn tracing_targets_from_value(env_name: &'static str, value: &str) -> CliResult<Option<Targets>> {
    let trimmed: &str = value.trim();
    if tracing_value_disables_output(trimmed) {
        return Ok(None);
    }

    let filter: &str = if tracing_value_uses_default_filter(trimmed) {
        "info"
    } else {
        trimmed
    };

    Targets::from_str(filter)
        .map(Some)
        .map_err(|source: ParseError| {
            CliError::Tracing(format!(
                "{env_name} has invalid tracing filter `{value}`: {source}"
            ))
        })
}

fn tracing_value_disables_output(value: &str) -> bool {
    let lowercase: String = value.to_ascii_lowercase();
    matches!(lowercase.as_str(), "" | "0" | "false" | "off")
}

fn tracing_value_uses_default_filter(value: &str) -> bool {
    let lowercase: String = value.to_ascii_lowercase();
    matches!(lowercase.as_str(), "1" | "true" | "yes")
}

fn dispatch_command(
    cmd: &Commands,
    read: impl Fn(&Path) -> CliResult<String>,
    read_bytes: impl Fn(&Path) -> CliResult<Vec<u8>>,
    write: impl Fn(&Path, &str) -> CliResult<()>,
) -> CliResult<String> {
    match cmd {
        Commands::Validate { workflow } => {
            let input: String = read(workflow)?;
            validate_workflow_json(&input, workflow)
        }
        Commands::Inspect { workflow } => {
            let input: String = read(workflow)?;
            inspect_workflow_json(&input, workflow)
        }
        Commands::Explain { workflow } => {
            let input: String = read(workflow)?;
            explain_workflow_json(&input, workflow)
        }
        Commands::ValidateManifest { workflow, manifest } => {
            let manifest_input: String = read(manifest)?;
            let workflow_input: Option<String> = workflow.as_deref().map(read).transpose()?;
            validate_wasm_component_manifest(
                &manifest_input,
                manifest,
                workflow_input.as_deref(),
                Path::exists,
            )
        }
        Commands::Schema { schema } => schema_json_string(*schema),
        Commands::Completions { shell } => {
            let mut buf: Vec<u8> = Vec::new();
            generate(*shell, &mut Cli::command(), "pureflow", &mut buf);
            String::from_utf8(buf).map_err(|source: std::string::FromUtf8Error| {
                CliError::Runtime(PureflowError::execution(format!(
                    "completion script was not valid UTF-8: {source}"
                )))
            })
        }
        Commands::Run {
            json,
            wasm_components,
            workflow,
            metadata,
        } => {
            let input: String = read(workflow)?;
            let run: CliRunOutput = if let Some(wasm_path) = wasm_components {
                let manifest_input: String = read(wasm_path)?;
                let components: Vec<CliWasmComponentSpec> =
                    wasm_component_specs_from_manifest(&manifest_input, wasm_path)?;
                run_workflow_json_with_wasm_components(&input, workflow, components, &read_bytes)?
            } else {
                run_workflow_json(&input, workflow)?
            };
            let metadata_str: String = metadata.display().to_string();
            write(metadata, &run.metadata_jsonl)?;
            if *json {
                cli_run_output_to_json_string(&run, &metadata_str)
            } else {
                Ok(format!(
                    "ran workflow `{}`\nnodes: {}\nedges: {}\nmetadata: {}\nrecords: {}\n",
                    run.workflow_id, run.node_count, run.edge_count, metadata_str, run.record_count
                ))
            }
        }
    }
}

fn schema_json_string(kind: SchemaKind) -> CliResult<String> {
    let schema: Value = match kind {
        SchemaKind::Workflow => workflow_schema_json(),
        SchemaKind::WasmManifest => wasm_component_manifest_schema_json(),
    };
    let mut output: String =
        serde_json::to_string_pretty(&schema).map_err(|source: serde_json::Error| {
            CliError::Runtime(PureflowError::execution(format!(
                "failed to encode JSON Schema: {source}"
            )))
        })?;
    output.push('\n');
    Ok(output)
}

fn workflow_schema_json() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": format!(
            "https://pureflow.dev/schemas/workflow-v{}.schema.json",
            CURRENT_PUREFLOW_VERSION
        ),
        "title": "Pureflow workflow document",
        "type": "object",
        "additionalProperties": false,
        "required": ["pureflow_version", "id", "nodes", "edges"],
        "properties": {
            "pureflow_version": {
                "type": "string",
                "const": CURRENT_PUREFLOW_VERSION,
                "description": "Required Pureflow workflow format version."
            },
            "id": identifier_schema_json("Workflow identifier."),
            "nodes": {
                "type": "array",
                "description": "Declared node topology.",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["id", "inputs", "outputs"],
                    "properties": {
                        "id": identifier_schema_json("Node identifier."),
                        "inputs": identifier_array_schema_json("Declared input port identifiers."),
                        "outputs": identifier_array_schema_json("Declared output port identifiers.")
                    }
                }
            },
            "edges": {
                "type": "array",
                "description": "Declared directed edges.",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["source", "target"],
                    "properties": {
                        "source": edge_endpoint_schema_json("Upstream output endpoint."),
                        "target": edge_endpoint_schema_json("Downstream input endpoint."),
                        "capacity": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Optional explicit bounded capacity. Omit to use the engine default."
                        }
                    }
                }
            }
        }
    })
}

fn wasm_component_manifest_schema_json() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://pureflow.dev/schemas/wasm-component-manifest.schema.json",
        "title": "Pureflow WASM component manifest",
        "type": "object",
        "additionalProperties": false,
        "required": ["components"],
        "properties": {
            "components": {
                "type": "array",
                "description": "WASM component entries keyed by workflow node.",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["node", "component"],
                    "properties": {
                        "node": identifier_schema_json("Workflow node served by this component."),
                        "component": {
                            "type": "string",
                            "minLength": 1,
                            "description": "Path to the component file. Relative paths resolve from the manifest directory."
                        },
                        "fuel": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Optional Wasmtime fuel limit. Omit to use the runtime default."
                        }
                    }
                }
            }
        }
    })
}

fn identifier_schema_json(description: &str) -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 256,
        "pattern": "^\\S+$",
        "description": description
    })
}

fn identifier_array_schema_json(description: &str) -> Value {
    json!({
        "type": "array",
        "description": description,
        "items": identifier_schema_json("Identifier value.")
    })
}

fn edge_endpoint_schema_json(description: &str) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["node", "port"],
        "description": description,
        "properties": {
            "node": identifier_schema_json("Referenced node identifier."),
            "port": identifier_schema_json("Referenced port identifier.")
        }
    })
}

fn read_file(path: &Path) -> CliResult<String> {
    fs::read_to_string(path).map_err(|source: std::io::Error| CliError::Io {
        action: "read",
        path: path.display().to_string(),
        source,
    })
}

fn read_bytes_file(path: &Path) -> CliResult<Vec<u8>> {
    fs::read(path).map_err(|source: std::io::Error| CliError::Io {
        action: "read",
        path: path.display().to_string(),
        source,
    })
}

fn write_file(path: &Path, contents: &str) -> CliResult<()> {
    fs::write(path, contents).map_err(|source: std::io::Error| CliError::Io {
        action: "write",
        path: path.display().to_string(),
        source,
    })
}

fn load_workflow(input: &str, path: &Path) -> CliResult<WorkflowDefinition> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("toml") => workflow_from_toml_str(input).map_err(CliError::WorkflowToml),
        Some("yaml" | "yml") => workflow_from_yaml_str(input).map_err(CliError::WorkflowYaml),
        Some("json") | None => workflow_from_json_str(input).map_err(CliError::WorkflowJson),
        Some(ext) => Err(CliError::WorkflowFormat(format!(
            "unsupported workflow file extension `.{ext}`; supported: .json, .toml, .yaml, .yml"
        ))),
    }
}

fn validate_workflow_json(input: &str, path: &Path) -> CliResult<String> {
    let workflow: WorkflowDefinition = load_workflow(input, path)?;

    Ok(format!(
        "valid workflow `{}`\nnodes: {}\nedges: {}\n",
        workflow.id(),
        workflow.nodes().len(),
        workflow.edges().len()
    ))
}

fn validate_wasm_component_manifest(
    manifest_input: &str,
    manifest_path: &Path,
    workflow_input: Option<&str>,
    path_exists: impl Fn(&Path) -> bool,
) -> CliResult<String> {
    let specs: Vec<CliWasmComponentSpec> =
        wasm_component_specs_from_manifest(manifest_input, manifest_path)?;

    let mut seen_nodes: BTreeSet<NodeId> = BTreeSet::new();
    for spec in &specs {
        if !seen_nodes.insert(spec.node_id.clone()) {
            return Err(CliError::WasmManifest(format!(
                "duplicate component entry for node `{}`",
                spec.node_id
            )));
        }
    }

    for spec in &specs {
        if !path_exists(&spec.component_path) {
            return Err(CliError::WasmManifest(format!(
                "component path `{}` for node `{}` is not readable",
                spec.component_path.display(),
                spec.node_id
            )));
        }
    }

    if let Some(input) = workflow_input {
        let workflow: WorkflowDefinition =
            workflow_from_json_str(input).map_err(CliError::WorkflowJson)?;
        let workflow_nodes: BTreeSet<&NodeId> =
            workflow.nodes().iter().map(NodeDefinition::id).collect();
        for spec in &specs {
            if !workflow_nodes.contains(&spec.node_id) {
                return Err(CliError::WasmManifest(format!(
                    "manifest node `{}` does not exist in workflow `{}`",
                    spec.node_id,
                    workflow.id()
                )));
            }
        }
        Ok(format!(
            "valid manifest `{}`\ncomponents: {}\nworkflow: `{}`\n",
            manifest_path.display(),
            specs.len(),
            workflow.id()
        ))
    } else {
        Ok(format!(
            "valid manifest `{}`\ncomponents: {}\n",
            manifest_path.display(),
            specs.len()
        ))
    }
}

fn inspect_workflow_json(input: &str, path: &Path) -> CliResult<String> {
    let workflow: WorkflowDefinition = load_workflow(input, path)?;
    let (contracts, capabilities): (Vec<NodeContract>, Vec<NodeCapabilities>) =
        passive_native_contracts_for_workflow(&workflow)?;
    let introspection: WorkflowIntrospection =
        introspect_workflow(&workflow, &contracts, &capabilities)?;
    let mut output: String = workflow_introspection_to_json_string(&introspection)?;
    output.push('\n');
    Ok(output)
}

fn explain_workflow_json(input: &str, path: &Path) -> CliResult<String> {
    let workflow: WorkflowDefinition = load_workflow(input, path)?;
    let mut output: String = format!(
        "workflow `{}`\nstatus: valid\nnodes: {}\nedges: {}\nexecution: native-registry\nmetadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy\n",
        workflow.id(),
        workflow.nodes().len(),
        workflow.edges().len()
    );

    output.push_str("node order:\n");
    for node in workflow.nodes() {
        writeln!(
            &mut output,
            "  - {} inputs={} outputs={}",
            node.id(),
            node.input_ports().len(),
            node.output_ports().len()
        )
        .map_err(|_err: fmt::Error| PureflowError::execution("failed to format explanation"))?;
    }

    output.push_str("edges:\n");
    for edge in workflow.edges() {
        let capacity: String = match edge.capacity() {
            EdgeCapacity::Default => String::from("default"),
            EdgeCapacity::Explicit(capacity) => capacity.get().to_string(),
        };
        writeln!(
            &mut output,
            "  - {}.{} -> {}.{} capacity={}",
            edge.source().node_id(),
            edge.source().port_id(),
            edge.target().node_id(),
            edge.target().port_id(),
            capacity
        )
        .map_err(|_err: fmt::Error| PureflowError::execution("failed to format explanation"))?;
    }

    Ok(output)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliRunOutput {
    workflow_id: String,
    node_count: usize,
    edge_count: usize,
    metadata_jsonl: String,
    record_count: usize,
    summary: WorkflowRunSummary,
}

fn run_workflow_json(input: &str, path: &Path) -> CliResult<CliRunOutput> {
    let workflow: WorkflowDefinition = load_workflow(input, path)?;
    let registry: StaticNodeExecutorRegistry<CliNativeExecutor> =
        native_registry_for_workflow(&workflow);

    run_workflow_with_registry_json(&workflow, &registry)
}

fn run_workflow_json_with_wasm_components(
    input: &str,
    path: &Path,
    components: Vec<CliWasmComponentSpec>,
    read_bytes: impl Fn(&Path) -> CliResult<Vec<u8>>,
) -> CliResult<CliRunOutput> {
    let workflow: WorkflowDefinition = load_workflow(input, path)?;
    let registry: StaticNodeExecutorRegistry<CliNodeExecutor> =
        mixed_registry_for_workflow(&workflow, components, read_bytes)?;

    run_workflow_with_registry_json(&workflow, &registry)
}

fn run_workflow_with_registry_json<R>(
    workflow: &WorkflowDefinition,
    registry: &R,
) -> CliResult<CliRunOutput>
where
    R: NodeExecutorRegistry,
{
    let runtime: AsupersyncRuntime = AsupersyncRuntime::new()?;
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("cli-run-1")?);
    let metadata_sink: Arc<TieredMetadataSink<JsonlMetadataSink<Vec<u8>>>> =
        Arc::new(TieredMetadataSink::new(JsonlMetadataSink::new(Vec::new())));

    let summary: WorkflowRunSummary =
        runtime.block_on(run_workflow_with_registry_and_metadata_sink_summary(
            workflow,
            &execution,
            registry,
            metadata_sink.clone(),
        ))?;

    let metadata_sink: TieredMetadataSink<JsonlMetadataSink<Vec<u8>>> =
        Arc::try_unwrap(metadata_sink).map_err(
            |_sink: Arc<TieredMetadataSink<JsonlMetadataSink<Vec<u8>>>>| {
                PureflowError::metadata("metadata sink still has active references")
            },
        )?;
    let metadata_bytes: Vec<u8> = metadata_sink.into_inner().into_inner()?;
    let metadata_jsonl: String =
        String::from_utf8(metadata_bytes).map_err(|source: std::string::FromUtf8Error| {
            PureflowError::metadata(format!("metadata JSONL was not valid UTF-8: {source}"))
        })?;
    let record_count: usize = metadata_jsonl.lines().count();

    Ok(CliRunOutput {
        workflow_id: workflow.id().to_string(),
        node_count: workflow.nodes().len(),
        edge_count: workflow.edges().len(),
        metadata_jsonl,
        record_count,
        summary,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliWasmComponentSpec {
    node_id: NodeId,
    component_path: PathBuf,
    limits: WasmtimeExecutionLimits,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWasmComponentManifest {
    components: Vec<RawWasmComponentSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWasmComponentSpec {
    node: String,
    component: String,
    fuel: Option<u64>,
}

fn wasm_component_specs_from_manifest(
    input: &str,
    manifest_path: &Path,
) -> CliResult<Vec<CliWasmComponentSpec>> {
    let manifest: RawWasmComponentManifest =
        serde_json::from_str(input).map_err(|source: serde_json::Error| {
            CliError::WasmManifestJson {
                path: manifest_path.display().to_string(),
                source,
            }
        })?;
    let manifest_dir: &Path = manifest_path.parent().unwrap_or_else(|| Path::new(""));

    manifest
        .components
        .into_iter()
        .map(|raw: RawWasmComponentSpec| {
            let node_id: NodeId = NodeId::new(raw.node)?;
            let component_path: PathBuf =
                resolve_manifest_component_path(manifest_dir, &raw.component);
            let limits: WasmtimeExecutionLimits = raw.fuel.map_or_else(
                WasmtimeExecutionLimits::default,
                WasmtimeExecutionLimits::new,
            );

            Ok(CliWasmComponentSpec {
                node_id,
                component_path,
                limits,
            })
        })
        .collect()
}

fn resolve_manifest_component_path(manifest_dir: &Path, component: &str) -> PathBuf {
    let component_path: &Path = Path::new(component);
    if component_path.is_absolute() {
        component_path.to_path_buf()
    } else {
        manifest_dir.join(component_path)
    }
}

fn cli_run_output_to_json_string(run: &CliRunOutput, metadata_path: &str) -> CliResult<String> {
    let mut output: String = serde_json::to_string_pretty(&json!({
        "status": workflow_terminal_state_label(run.summary.terminal_state()),
        "error": run
            .summary
            .first_error()
            .map_or(Value::Null, pureflow_error_to_json_value),
        "workflow": {
            "id": run.workflow_id,
            "node_count": run.node_count,
            "edge_count": run.edge_count,
        },
        "metadata": {
            "path": metadata_path,
            "record_count": run.record_count,
        },
        "summary": workflow_run_summary_to_json_value(&run.summary),
    }))
    .map_err(|source: serde_json::Error| {
        PureflowError::metadata(format!("failed to encode run summary JSON: {source}"))
    })?;
    output.push('\n');
    Ok(output)
}

fn workflow_run_summary_to_json_value(summary: &WorkflowRunSummary) -> Value {
    json!({
        "terminal_state": workflow_terminal_state_label(summary.terminal_state()),
        "scheduled_node_count": summary.scheduled_node_count(),
        "completed_node_count": summary.completed_node_count(),
        "failed_node_count": summary.failed_node_count(),
        "cancelled_node_count": summary.cancelled_node_count(),
        "pending_node_count": summary.pending_node_count(),
        "observed_message_count": summary.observed_message_count(),
        "error_count": summary.error_count(),
        "first_error": summary
            .first_error()
            .map_or(Value::Null, pureflow_error_to_json_value),
        "deadlock_diagnostic": summary
            .deadlock_diagnostic()
            .map_or(Value::Null, workflow_deadlock_diagnostic_to_json_value),
    })
}

fn workflow_deadlock_diagnostic_to_json_value(diagnostic: &WorkflowDeadlockDiagnostic) -> Value {
    json!({
        "workflow_id": diagnostic.workflow_id().as_str(),
        "scheduled_node_count": diagnostic.scheduled_node_count(),
        "pending_node_count": diagnostic.pending_node_count(),
        "completed_node_count": diagnostic.completed_node_count(),
        "failed_node_count": diagnostic.failed_node_count(),
        "cancelled_node_count": diagnostic.cancelled_node_count(),
        "bounded_edge_count": diagnostic.bounded_edge_count(),
        "no_progress_timeout_ms": duration_millis_u64(diagnostic.no_progress_timeout()),
        "cycle_policy": cycle_run_policy_label(diagnostic.cycle_policy()),
        "feedback_loop_startup": feedback_loop_startup_value(diagnostic.cycle_policy()),
        "feedback_loop_termination": feedback_loop_termination_value(diagnostic.cycle_policy()),
    })
}

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

const fn cycle_run_policy_label(policy: CycleRunPolicy) -> &'static str {
    match policy {
        CycleRunPolicy::Reject => "reject",
        CycleRunPolicy::AllowFeedbackLoops(_feedback_loop) => "allow_feedback_loops",
    }
}

fn feedback_loop_startup_value(policy: CycleRunPolicy) -> Value {
    match policy {
        CycleRunPolicy::Reject => Value::Null,
        CycleRunPolicy::AllowFeedbackLoops(feedback_loop) => {
            json!(feedback_loop_startup_label(feedback_loop.startup()))
        }
    }
}

fn feedback_loop_termination_value(policy: CycleRunPolicy) -> Value {
    match policy {
        CycleRunPolicy::Reject => Value::Null,
        CycleRunPolicy::AllowFeedbackLoops(feedback_loop) => {
            json!(feedback_loop_termination_label(feedback_loop.termination()))
        }
    }
}

const fn feedback_loop_startup_label(startup: FeedbackLoopStartup) -> &'static str {
    match startup {
        FeedbackLoopStartup::StartAllNodes => "start_all_nodes",
    }
}

const fn feedback_loop_termination_label(termination: FeedbackLoopTermination) -> &'static str {
    match termination {
        FeedbackLoopTermination::AllNodesComplete => "all_nodes_complete",
    }
}

const fn workflow_terminal_state_label(state: WorkflowTerminalState) -> &'static str {
    match state {
        WorkflowTerminalState::Completed => "completed",
        WorkflowTerminalState::Failed => "failed",
        WorkflowTerminalState::Cancelled => "cancelled",
    }
}

fn pureflow_error_to_json_value(error: &PureflowError) -> Value {
    json!({
        "code": error.code().as_str(),
        "message": error.to_string(),
        "visibility": error_visibility_label(error.visibility()),
        "retry_disposition": retry_disposition_label(error.retry_disposition()),
    })
}

const fn error_visibility_label(visibility: ErrorVisibility) -> &'static str {
    match visibility {
        ErrorVisibility::User => "user",
        ErrorVisibility::Internal => "internal",
    }
}

const fn retry_disposition_label(disposition: RetryDisposition) -> &'static str {
    match disposition {
        RetryDisposition::Never => "never",
        RetryDisposition::Safe => "safe",
        RetryDisposition::Unknown => "unknown",
    }
}

#[derive(Debug, Clone)]
struct CliNativeExecutor {
    input_ports: Vec<PortId>,
    output_routes: BTreeMap<PortId, Vec<MessageEndpoint>>,
}

impl CliNativeExecutor {
    const fn new(
        input_ports: Vec<PortId>,
        output_routes: BTreeMap<PortId, Vec<MessageEndpoint>>,
    ) -> Self {
        Self {
            input_ports,
            output_routes,
        }
    }
}

impl NodeExecutor for CliNativeExecutor {
    type RunFuture<'a> = Pin<Box<dyn Future<Output = pureflow_core::Result<()>> + Send + 'a>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(run_cli_native_executor(self, ctx, inputs, outputs))
    }
}

async fn run_cli_native_executor(
    executor: &CliNativeExecutor,
    ctx: NodeContext,
    mut inputs: PortsIn,
    outputs: PortsOut,
) -> pureflow_core::Result<()> {
    let cancellation: CancellationToken = ctx.cancellation_token();
    let mut received_count: usize = 0;

    for input_port in &executor.input_ports {
        let port_count: usize =
            drain_cli_input_port(&mut inputs, input_port, &cancellation).await?;
        received_count = received_count.saturating_add(port_count);
    }

    for (output_index, (output_port, targets)) in executor.output_routes.iter().enumerate() {
        send_cli_output_packet(
            &ctx,
            &outputs,
            output_port,
            targets,
            output_index,
            received_count,
            &cancellation,
        )
        .await?;
    }

    Ok(())
}

async fn drain_cli_input_port(
    inputs: &mut PortsIn,
    input_port: &PortId,
    cancellation: &CancellationToken,
) -> pureflow_core::Result<usize> {
    let mut received_count: usize = 0;
    loop {
        match inputs.recv(input_port, cancellation).await {
            Ok(Some(_packet)) => received_count = received_count.saturating_add(1),
            Ok(None) | Err(PortRecvError::Disconnected { .. }) => return Ok(received_count),
            Err(err) => return Err(err.into()),
        }
    }
}

async fn send_cli_output_packet(
    ctx: &NodeContext,
    outputs: &PortsOut,
    output_port: &PortId,
    targets: &[MessageEndpoint],
    output_index: usize,
    received_count: usize,
    cancellation: &CancellationToken,
) -> pureflow_core::Result<()> {
    let target: MessageEndpoint = targets
        .first()
        .cloned()
        .unwrap_or_else(|| MessageEndpoint::new(ctx.node_id().clone(), output_port.clone()));
    let source: MessageEndpoint = MessageEndpoint::new(ctx.node_id().clone(), output_port.clone());
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let message_id: MessageId = MessageId::new(format!(
        "cli-{}-{}-{output_index}",
        ctx.node_id(),
        output_port
    ))?;
    let metadata: MessageMetadata = MessageMetadata::new(
        message_id,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );
    let payload: PacketPayload = PacketPayload::bytes(format!(
        "cli-native node={} output={} received={received_count}",
        ctx.node_id(),
        output_port
    ));

    outputs
        .send(
            output_port,
            PortPacket::new(metadata, payload),
            cancellation,
        )
        .await?;
    Ok(())
}

enum CliNodeExecutor {
    Native(CliNativeExecutor),
    Wasm(BatchNodeExecutor<WasmtimeBatchComponent>),
}

impl NodeExecutor for CliNodeExecutor {
    type RunFuture<'a> = Pin<Box<dyn Future<Output = pureflow_core::Result<()>> + Send + 'a>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        match self {
            Self::Native(executor) => executor.run(ctx, inputs, outputs),
            Self::Wasm(executor) => executor.run(ctx, inputs, outputs),
        }
    }
}

fn native_registry_for_workflow(
    workflow: &WorkflowDefinition,
) -> StaticNodeExecutorRegistry<CliNativeExecutor> {
    let mut routes_by_node: BTreeMap<NodeId, BTreeMap<PortId, Vec<MessageEndpoint>>> =
        BTreeMap::new();
    for edge in workflow.edges() {
        routes_by_node
            .entry(edge.source().node_id().clone())
            .or_default()
            .entry(edge.source().port_id().clone())
            .or_default()
            .push(MessageEndpoint::new(
                edge.target().node_id().clone(),
                edge.target().port_id().clone(),
            ));
    }

    let mut executors: BTreeMap<NodeId, CliNativeExecutor> = BTreeMap::new();
    for node in workflow.nodes() {
        let mut output_routes: BTreeMap<PortId, Vec<MessageEndpoint>> = BTreeMap::new();
        for output_port in node.output_ports() {
            let targets: Vec<MessageEndpoint> = routes_by_node
                .get(node.id())
                .and_then(|routes: &BTreeMap<PortId, Vec<MessageEndpoint>>| routes.get(output_port))
                .cloned()
                .unwrap_or_default();
            output_routes.insert(output_port.clone(), targets);
        }
        let executor: CliNativeExecutor =
            CliNativeExecutor::new(node.input_ports().to_vec(), output_routes);
        executors.insert(node.id().clone(), executor);
    }

    StaticNodeExecutorRegistry::new(executors)
}

fn mixed_registry_for_workflow(
    workflow: &WorkflowDefinition,
    components: Vec<CliWasmComponentSpec>,
    read_bytes: impl Fn(&Path) -> CliResult<Vec<u8>>,
) -> CliResult<StaticNodeExecutorRegistry<CliNodeExecutor>> {
    let native_registry: StaticNodeExecutorRegistry<CliNativeExecutor> =
        native_registry_for_workflow(workflow);
    let mut components_by_node: BTreeMap<NodeId, CliWasmComponentSpec> = BTreeMap::new();
    for component in components {
        let node_id: NodeId = component.node_id.clone();
        if components_by_node
            .insert(node_id.clone(), component)
            .is_some()
        {
            return Err(CliError::WasmManifest(format!(
                "duplicate WASM component entry for node `{node_id}`"
            )));
        }
    }

    let mut executors: BTreeMap<NodeId, CliNodeExecutor> = BTreeMap::new();
    for node in workflow.nodes() {
        let executor: CliNodeExecutor =
            if let Some(component) = components_by_node.remove(node.id()) {
                let component_bytes: Vec<u8> = read_bytes(&component.component_path)?;
                let wasm_component: WasmtimeBatchComponent =
                    WasmtimeBatchComponent::from_component_bytes_with_limits(
                        component_bytes,
                        component.limits,
                    )?;
                CliNodeExecutor::Wasm(BatchNodeExecutor::new(wasm_component))
            } else {
                let native_executor: CliNativeExecutor = native_registry
                    .executors()
                    .get(node.id())
                    .ok_or_else(|| {
                        CliError::WasmManifest(format!(
                            "failed to construct native executor for workflow node `{}`",
                            node.id()
                        ))
                    })?
                    .clone();
                CliNodeExecutor::Native(native_executor)
            };
        executors.insert(node.id().clone(), executor);
    }

    if let Some(node_id) = components_by_node.keys().next() {
        return Err(CliError::WasmManifest(format!(
            "WASM component manifest references unknown workflow node `{node_id}`"
        )));
    }

    Ok(StaticNodeExecutorRegistry::new(executors))
}

fn passive_native_contracts_for_workflow(
    workflow: &WorkflowDefinition,
) -> CliResult<(Vec<NodeContract>, Vec<NodeCapabilities>)> {
    let contracts: Vec<NodeContract> = workflow
        .nodes()
        .iter()
        .map(passive_native_contract_for_node)
        .collect::<CliResult<Vec<NodeContract>>>()?;
    let capabilities: Vec<NodeCapabilities> = workflow
        .nodes()
        .iter()
        .map(passive_native_capabilities_for_node)
        .collect::<CliResult<Vec<NodeCapabilities>>>()?;

    Ok((contracts, capabilities))
}

fn passive_native_contract_for_node(node: &NodeDefinition) -> CliResult<NodeContract> {
    let mut ports: Vec<PortContract> =
        Vec::with_capacity(node.input_ports().len() + node.output_ports().len());
    for port_id in node.input_ports() {
        ports.push(PortContract::new(
            port_id.clone(),
            PortDirection::Input,
            None,
        ));
    }
    for port_id in node.output_ports() {
        ports.push(PortContract::new(
            port_id.clone(),
            PortDirection::Output,
            None,
        ));
    }

    NodeContract::new(
        node.id().clone(),
        ports,
        ExecutionMode::Native,
        Determinism::Unknown,
        RetryDisposition::Unknown,
    )
    .map_err(CliError::Contract)
}

fn passive_native_capabilities_for_node(node: &NodeDefinition) -> CliResult<NodeCapabilities> {
    let mut ports: Vec<PortCapability> =
        Vec::with_capacity(node.input_ports().len() + node.output_ports().len());
    for port_id in node.input_ports() {
        ports.push(PortCapability::new(
            port_id.clone(),
            PortCapabilityDirection::Receive,
        ));
    }
    for port_id in node.output_ports() {
        ports.push(PortCapability::new(
            port_id.clone(),
            PortCapabilityDirection::Emit,
        ));
    }

    NodeCapabilities::native_passive(node.id().clone(), ports).map_err(CliError::Capability)
}

#[derive(Debug)]
enum CliError {
    Io {
        action: &'static str,
        path: String,
        source: std::io::Error,
    },
    WorkflowJson(WorkflowJsonError),
    WorkflowToml(WorkflowTomlError),
    WorkflowYaml(WorkflowYamlError),
    WorkflowFormat(String),
    Contract(ContractValidationError),
    Capability(CapabilityValidationError),
    IntrospectionJson(IntrospectionJsonError),
    Runtime(PureflowError),
    Tracing(String),
    WasmManifest(String),
    WasmManifestJson {
        path: String,
        source: serde_json::Error,
    },
}

impl CliError {
    const fn exit_code(&self) -> i32 {
        match self {
            Self::Io { .. }
            | Self::WorkflowJson(_)
            | Self::WorkflowToml(_)
            | Self::WorkflowYaml(_)
            | Self::WorkflowFormat(_)
            | Self::Contract(_)
            | Self::Capability(_)
            | Self::IntrospectionJson(_)
            | Self::Runtime(_)
            | Self::Tracing(_)
            | Self::WasmManifest(_)
            | Self::WasmManifestJson { .. } => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                action,
                path,
                source,
            } => write!(f, "failed to {action} `{path}`: {source}"),
            Self::WorkflowJson(source) => write!(f, "{source}"),
            Self::WorkflowToml(source) => write!(f, "{source}"),
            Self::WorkflowYaml(source) => write!(f, "{source}"),
            Self::WorkflowFormat(message) | Self::Tracing(message) => write!(f, "{message}"),
            Self::Contract(source) => write!(f, "workflow contract validation failed: {source}"),
            Self::Capability(source) => {
                write!(f, "workflow capability validation failed: {source}")
            }
            Self::IntrospectionJson(source) => write!(f, "{source}"),
            Self::Runtime(source) => write!(f, "{source}"),
            Self::WasmManifest(message) => write!(f, "invalid WASM component manifest: {message}"),
            Self::WasmManifestJson { path, source } => {
                write!(
                    f,
                    "failed to decode WASM component manifest `{path}`: {source}"
                )
            }
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Tracing(_) | Self::WasmManifest(_) | Self::WorkflowFormat(_) => None,
            Self::Io { source, .. } => Some(source),
            Self::WorkflowJson(source) => Some(source),
            Self::WorkflowToml(source) => Some(source),
            Self::WorkflowYaml(source) => Some(source),
            Self::Contract(source) => Some(source),
            Self::Capability(source) => Some(source),
            Self::IntrospectionJson(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::WasmManifestJson { source, .. } => Some(source),
        }
    }
}

impl From<WorkflowJsonError> for CliError {
    fn from(source: WorkflowJsonError) -> Self {
        Self::WorkflowJson(source)
    }
}

impl From<ContractValidationError> for CliError {
    fn from(source: ContractValidationError) -> Self {
        Self::Contract(source)
    }
}

impl From<IntrospectionJsonError> for CliError {
    fn from(source: IntrospectionJsonError) -> Self {
        Self::IntrospectionJson(source)
    }
}

impl From<PureflowError> for CliError {
    fn from(source: PureflowError) -> Self {
        Self::Runtime(source)
    }
}

impl From<pureflow_types::IdentifierError> for CliError {
    fn from(source: pureflow_types::IdentifierError) -> Self {
        Self::Runtime(PureflowError::from(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    const WORKFLOW_JSON: &str = r#"{
  "pureflow_version": "1",
  "id": "flow",
  "nodes": [
    { "id": "source", "inputs": [], "outputs": ["out"] },
    { "id": "sink", "inputs": ["in"], "outputs": [] }
  ],
  "edges": [
    {
      "source": { "node": "source", "port": "out" },
      "target": { "node": "sink", "port": "in" },
      "capacity": 8
    }
  ]
}"#;
    #[test]
    fn tracing_targets_are_opt_in() {
        let disabled: Option<Targets> =
            tracing_targets_from_env(|_name: &str| None).expect("missing env should parse");
        let explicit_off: Option<Targets> =
            tracing_targets_from_value(CONDUIT_TRACE_ENV, "off").expect("off should parse");
        let explicit_true: Option<Targets> =
            tracing_targets_from_value(CONDUIT_TRACE_ENV, "true").expect("true should parse");
        let rust_log_directive: Option<Targets> =
            tracing_targets_from_value(RUST_LOG_ENV, "pureflow.runtime=debug")
                .expect("target directive should parse");

        assert!(disabled.is_none());
        assert!(explicit_off.is_none());
        assert!(explicit_true.is_some());
        assert!(rust_log_directive.is_some());
    }

    #[test]
    fn pureflow_trace_takes_precedence_over_rust_log() {
        let targets: Option<Targets> = tracing_targets_from_env(|name: &str| match name {
            CONDUIT_TRACE_ENV => Some(String::from("off")),
            RUST_LOG_ENV => Some(String::from("trace")),
            _ => None,
        })
        .expect("env should parse");

        assert!(targets.is_none());
    }

    #[test]
    fn invalid_tracing_filter_reports_env_name() {
        let err: CliError = tracing_targets_from_value(RUST_LOG_ENV, "pureflow.runtime=verbose")
            .expect_err("invalid tracing filter should fail");

        assert!(err.to_string().contains(RUST_LOG_ENV));
        assert!(err.to_string().contains("invalid tracing filter"));
    }

    #[test]
    fn validate_reports_valid_workflow_summary() {
        let output = validate_workflow_json(WORKFLOW_JSON, Path::new("workflow.json"))
            .expect("workflow should validate");

        assert_eq!(output, "valid workflow `flow`\nnodes: 2\nedges: 1\n");
    }

    #[test]
    fn inspect_renders_introspection_json() {
        let output = inspect_workflow_json(WORKFLOW_JSON, Path::new("workflow.json"))
            .expect("workflow should inspect");

        assert!(output.contains("\"workflow_id\": \"flow\""));
        assert!(output.contains("\"execution_mode\": \"native\""));
        assert!(output.contains("\"capacity\""));
    }

    #[test]
    fn explain_reports_valid_topology_and_metadata_policy() {
        let output = explain_workflow_json(WORKFLOW_JSON, Path::new("workflow.json"))
            .expect("workflow should explain");

        assert!(output.contains("workflow `flow`"));
        assert!(output.contains("execution: native-registry"));
        assert!(output.contains("metadata: jsonl lifecycle, message, and queue-pressure records"));
        assert!(output.contains("source.out -> sink.in capacity=8"));
    }

    #[test]
    fn run_writes_reproducible_metadata_jsonl() {
        let output = run_workflow_json(WORKFLOW_JSON, Path::new("workflow.json"))
            .expect("workflow should run");

        assert_eq!(output.workflow_id, "flow");
        assert_eq!(output.node_count, 2);
        assert_eq!(output.edge_count, 1);
        assert_eq!(output.record_count, 13);
        assert_eq!(
            output.summary.terminal_state(),
            WorkflowTerminalState::Completed
        );
        assert_eq!(output.summary.scheduled_node_count(), 2);
        assert_eq!(output.summary.completed_node_count(), 2);
        assert_eq!(output.summary.error_count(), 0);
        assert!(output.summary.first_error().is_none());
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"lifecycle\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"message\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"queue_pressure\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"execution_id\":\"cli-run-1\"")
        );
        insta::assert_snapshot!(output.metadata_jsonl, @r###"
{"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"source","workflow_id":"flow"},"kind":"node_started","record_type":"lifecycle"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"source","workflow_id":"flow"},"direction":"output","kind":"reserve_attempted","port_id":"out","queued_count":null,"record_type":"queue_pressure"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"source","workflow_id":"flow"},"direction":"output","kind":"reserve_ready","port_id":"out","queued_count":null,"record_type":"queue_pressure"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"source","workflow_id":"flow"},"direction":"output","kind":"send_committed","port_id":"out","queued_count":null,"record_type":"queue_pressure"}
{"kind":"enqueued","message":{"execution":{"attempt":1,"execution_id":"cli-run-1"},"message_id":"cli-source-out-0","route":{"source":{"node_id":"source","port_id":"out"},"target":{"node_id":"sink","port_id":"in"}},"workflow_id":"flow"},"record_type":"message"}
{"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"source","workflow_id":"flow"},"kind":"node_completed","record_type":"lifecycle"}
{"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"sink","workflow_id":"flow"},"kind":"node_started","record_type":"lifecycle"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"sink","workflow_id":"flow"},"direction":"input","kind":"receive_attempted","port_id":"in","queued_count":1,"record_type":"queue_pressure"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"sink","workflow_id":"flow"},"direction":"input","kind":"receive_ready","port_id":"in","queued_count":0,"record_type":"queue_pressure"}
{"kind":"dequeued","message":{"execution":{"attempt":1,"execution_id":"cli-run-1"},"message_id":"cli-source-out-0","route":{"source":{"node_id":"source","port_id":"out"},"target":{"node_id":"sink","port_id":"in"}},"workflow_id":"flow"},"record_type":"message"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"sink","workflow_id":"flow"},"direction":"input","kind":"receive_attempted","port_id":"in","queued_count":0,"record_type":"queue_pressure"}
{"capacity":8,"connected_edge_count":1,"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"sink","workflow_id":"flow"},"direction":"input","kind":"receive_closed","port_id":"in","queued_count":0,"record_type":"queue_pressure"}
{"context":{"cancellation":{"state":"active"},"execution":{"attempt":1,"execution_id":"cli-run-1"},"node_id":"sink","workflow_id":"flow"},"kind":"node_completed","record_type":"lifecycle"}
"###);
    }

    #[test]
    fn run_json_output_reports_machine_facing_summary_fields() {
        let output = run_workflow_json(WORKFLOW_JSON, Path::new("workflow.json"))
            .expect("workflow should run");
        let json_output = cli_run_output_to_json_string(&output, "metadata.jsonl")
            .expect("run JSON should encode");
        let value: Value = serde_json::from_str(&json_output).expect("run output should be JSON");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["error"], Value::Null);
        assert_eq!(value["workflow"]["id"], "flow");
        assert_eq!(value["workflow"]["node_count"], 2);
        assert_eq!(value["workflow"]["edge_count"], 1);
        assert_eq!(value["metadata"]["path"], "metadata.jsonl");
        assert_eq!(value["metadata"]["record_count"], 13);
        assert_eq!(value["summary"]["terminal_state"], "completed");
        assert_eq!(value["summary"]["scheduled_node_count"], 2);
        assert_eq!(value["summary"]["completed_node_count"], 2);
        assert_eq!(value["summary"]["failed_node_count"], 0);
        assert_eq!(value["summary"]["cancelled_node_count"], 0);
        assert_eq!(value["summary"]["pending_node_count"], 0);
        assert_eq!(value["summary"]["observed_message_count"], 0);
        assert_eq!(value["summary"]["error_count"], 0);
        assert_eq!(value["summary"]["first_error"], Value::Null);
        assert_eq!(value["summary"]["deadlock_diagnostic"], Value::Null);
        insta::assert_snapshot!(json_output, @r###"
{
  "error": null,
  "metadata": {
    "path": "metadata.jsonl",
    "record_count": 13
  },
  "status": "completed",
  "summary": {
    "cancelled_node_count": 0,
    "completed_node_count": 2,
    "deadlock_diagnostic": null,
    "error_count": 0,
    "failed_node_count": 0,
    "first_error": null,
    "observed_message_count": 0,
    "pending_node_count": 0,
    "scheduled_node_count": 2,
    "terminal_state": "completed"
  },
  "workflow": {
    "edge_count": 1,
    "id": "flow",
    "node_count": 2
  }
}
"###);
    }

    #[test]
    fn run_json_command_writes_metadata_and_returns_json() {
        let cli = Cli::try_parse_from([
            "pureflow",
            "run",
            "--json",
            "workflow.json",
            "metadata.jsonl",
        ])
        .expect("args should parse");
        let written_metadata: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let written_metadata_for_closure: Arc<Mutex<Option<String>>> = written_metadata.clone();

        let output = dispatch_command(
            &cli.command,
            |path: &Path| {
                assert_eq!(path, Path::new("workflow.json"));
                Ok(String::from(WORKFLOW_JSON))
            },
            |_path: &Path| panic!("run without wasm components should not read bytes"),
            |path: &Path, contents: &str| {
                assert_eq!(path, Path::new("metadata.jsonl"));
                *written_metadata_for_closure
                    .lock()
                    .expect("metadata write lock should not be poisoned") =
                    Some(contents.to_owned());
                Ok(())
            },
        )
        .expect("run command should succeed");
        let value: Value = serde_json::from_str(&output).expect("run output should be JSON");
        let metadata = written_metadata
            .lock()
            .expect("metadata write lock should not be poisoned")
            .clone()
            .expect("metadata should be written");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["metadata"]["record_count"], metadata.lines().count());
        assert!(metadata.contains("\"record_type\":\"lifecycle\""));
    }

    #[test]
    fn run_command_accepts_wasm_components_manifest() {
        let cli = Cli::try_parse_from([
            "pureflow",
            "run",
            "--wasm-components",
            "components.json",
            "workflow.json",
            "metadata.jsonl",
        ])
        .expect("args should parse");
        let written_metadata: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let written_metadata_for_closure: Arc<Mutex<Option<String>>> = written_metadata.clone();

        let output = dispatch_command(
            &cli.command,
            |path: &Path| match path.to_str().expect("test path should be UTF-8") {
                "workflow.json" => Ok(String::from(WORKFLOW_JSON)),
                "components.json" => Ok(String::from(r#"{"components":[]}"#)),
                other => panic!("unexpected text read path {other}"),
            },
            |_path: &Path| panic!("empty wasm manifest should not read component bytes"),
            |path: &Path, contents: &str| {
                assert_eq!(path, Path::new("metadata.jsonl"));
                *written_metadata_for_closure
                    .lock()
                    .expect("metadata write lock should not be poisoned") =
                    Some(contents.to_owned());
                Ok(())
            },
        )
        .expect("run command should succeed");
        let metadata = written_metadata
            .lock()
            .expect("metadata write lock should not be poisoned")
            .clone()
            .expect("metadata should be written");

        assert!(output.contains("ran workflow `flow`"));
        assert!(metadata.contains("\"record_type\":\"lifecycle\""));
    }

    #[test]
    fn wasm_component_manifest_resolves_relative_component_paths() {
        let specs: Vec<CliWasmComponentSpec> = wasm_component_specs_from_manifest(
            r#"{"components":[{"node":"transform","component":"components/uppercase.wasm","fuel":123}]}"#,
            Path::new("manifests/wasm-components.json"),
        )
        .expect("manifest should parse");

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].node_id.as_str(), "transform");
        assert_eq!(
            specs[0].component_path,
            PathBuf::from("manifests/components/uppercase.wasm")
        );
        assert_eq!(specs[0].limits.fuel(), 123);
    }

    #[test]
    fn mixed_registry_rejects_unknown_wasm_node_before_reading_component() {
        let workflow: WorkflowDefinition =
            workflow_from_json_str(WORKFLOW_JSON).expect("workflow should parse");
        let specs = vec![CliWasmComponentSpec {
            node_id: node_id("missing"),
            component_path: PathBuf::from("missing.wasm"),
            limits: WasmtimeExecutionLimits::default(),
        }];

        let result = mixed_registry_for_workflow(&workflow, specs, |_path: &Path| {
            panic!("unknown node should fail before component bytes are read")
        });
        let err = match result {
            Ok(_registry) => panic!("unknown node should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("unknown workflow node `missing`"));
    }

    #[test]
    fn help_output_names_binary_and_lists_all_subcommands() {
        let help = Cli::command().render_help().to_string();

        assert!(
            help.contains("pureflow"),
            "help should name the binary pureflow"
        );
        assert!(
            !help.contains("pureflow-cli"),
            "help should not expose the package name"
        );
        assert!(help.contains("validate"));
        assert!(help.contains("inspect"));
        assert!(help.contains("explain"));
        assert!(help.contains("run"));
        assert!(help.contains("schema"));
        assert!(help.contains("completions"));
    }

    #[test]
    fn schema_command_emits_workflow_schema() {
        let cli = Cli::try_parse_from(["pureflow", "schema", "workflow"])
            .expect("schema args should parse");

        let output = dispatch_command(
            &cli.command,
            |_path: &Path| unreachable!("schema should not read text files"),
            |_path: &Path| unreachable!("schema should not read bytes"),
            |_path: &Path, _contents: &str| unreachable!("schema should not write files"),
        )
        .expect("schema command should succeed");
        let value: Value = serde_json::from_str(&output).expect("schema output should be JSON");

        assert_eq!(value["title"], "Pureflow workflow document");
        assert_eq!(
            value["properties"]["pureflow_version"]["const"],
            CURRENT_PUREFLOW_VERSION
        );
        assert_eq!(value["additionalProperties"], false);
        assert_eq!(
            value["properties"]["nodes"]["items"]["additionalProperties"],
            false
        );
        assert_eq!(
            value["properties"]["edges"]["items"]["properties"]["capacity"]["minimum"],
            1
        );
    }

    #[test]
    fn schema_command_emits_wasm_manifest_schema() {
        let cli = Cli::try_parse_from(["pureflow", "schema", "wasm-manifest"])
            .expect("schema args should parse");

        let output = dispatch_command(
            &cli.command,
            |_path: &Path| unreachable!("schema should not read text files"),
            |_path: &Path| unreachable!("schema should not read bytes"),
            |_path: &Path, _contents: &str| unreachable!("schema should not write files"),
        )
        .expect("schema command should succeed");
        let value: Value = serde_json::from_str(&output).expect("schema output should be JSON");

        assert_eq!(value["title"], "Pureflow WASM component manifest");
        assert_eq!(value["additionalProperties"], false);
        assert_eq!(
            value["properties"]["components"]["items"]["additionalProperties"],
            false
        );
        assert_eq!(
            value["properties"]["components"]["items"]["properties"]["fuel"]["minimum"],
            0
        );
    }

    #[test]
    fn completions_subcommand_generates_nonempty_bash_script() {
        let cli =
            Cli::try_parse_from(["pureflow", "completions", "bash"]).expect("args should parse");

        let output = dispatch_command(
            &cli.command,
            |_path: &Path| unreachable!("completions should not read files"),
            |_path: &Path| unreachable!("completions should not read bytes"),
            |_path: &Path, _contents: &str| unreachable!("completions should not write files"),
        )
        .expect("completions should succeed");

        assert!(!output.is_empty());
        assert!(output.contains("pureflow"));
    }

    const MANIFEST_JSON_EMPTY: &str = r#"{"components":[]}"#;
    const MANIFEST_JSON_ONE: &str = r#"{"components":[{"node":"transform","component":"components/uppercase.wasm","fuel":100000}]}"#;

    #[test]
    fn validate_manifest_accepts_empty_manifest() {
        let output = validate_wasm_component_manifest(
            MANIFEST_JSON_EMPTY,
            Path::new("components.json"),
            None,
            |_path: &Path| unreachable!("empty manifest should not check paths"),
        )
        .expect("empty manifest should be valid");

        assert!(output.contains("valid manifest"));
        assert!(output.contains("components: 0"));
    }

    #[test]
    fn validate_manifest_accepts_valid_single_component() {
        let output = validate_wasm_component_manifest(
            MANIFEST_JSON_ONE,
            Path::new("components.json"),
            None,
            |_path: &Path| true,
        )
        .expect("valid manifest should pass");

        assert!(output.contains("components: 1"));
    }

    #[test]
    fn validate_manifest_rejects_unknown_fields() {
        let err = validate_wasm_component_manifest(
            r#"{"components":[],"extra":true}"#,
            Path::new("components.json"),
            None,
            |_path: &Path| unreachable!(),
        )
        .expect_err("unknown field should fail");

        assert!(err.to_string().contains("failed to decode"));
    }

    #[test]
    fn validate_manifest_rejects_invalid_node_id() {
        let err = validate_wasm_component_manifest(
            r#"{"components":[{"node":"bad node id","component":"x.wasm"}]}"#,
            Path::new("components.json"),
            None,
            |_path: &Path| unreachable!(),
        )
        .expect_err("invalid node id should fail");

        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn validate_manifest_rejects_duplicate_node_entries() {
        let err = validate_wasm_component_manifest(
            r#"{"components":[{"node":"a","component":"a.wasm"},{"node":"a","component":"b.wasm"}]}"#,
            Path::new("components.json"),
            None,
            |_path: &Path| true,
        )
        .expect_err("duplicate node should fail");

        assert!(
            err.to_string()
                .contains("duplicate component entry for node `a`")
        );
    }

    #[test]
    fn validate_manifest_rejects_unreadable_component_paths() {
        let err = validate_wasm_component_manifest(
            MANIFEST_JSON_ONE,
            Path::new("components.json"),
            None,
            |_path: &Path| false,
        )
        .expect_err("unreadable path should fail");

        assert!(err.to_string().contains("not readable"));
        assert!(err.to_string().contains("transform"));
    }

    #[test]
    fn validate_manifest_with_workflow_rejects_missing_nodes() {
        let err = validate_wasm_component_manifest(
            MANIFEST_JSON_ONE,
            Path::new("components.json"),
            Some(WORKFLOW_JSON),
            |_path: &Path| true,
        )
        .expect_err("node not in workflow should fail");

        assert!(err.to_string().contains("`transform`"));
        assert!(err.to_string().contains("workflow `flow`"));
    }

    #[test]
    fn validate_manifest_with_workflow_accepts_known_node() {
        let manifest = r#"{"components":[{"node":"source","component":"source.wasm"}]}"#;
        let output = validate_wasm_component_manifest(
            manifest,
            Path::new("components.json"),
            Some(WORKFLOW_JSON),
            |_path: &Path| true,
        )
        .expect("known node should pass");

        assert!(output.contains("workflow: `flow`"));
        assert!(output.contains("components: 1"));
    }

    const WORKFLOW_TOML: &str = r#"
pureflow_version = "1"
id = "flow"

[[nodes]]
id = "source"
inputs = []
outputs = ["out"]

[[nodes]]
id = "sink"
inputs = ["in"]
outputs = []

[[edges]]
capacity = 8

[edges.source]
node = "source"
port = "out"

[edges.target]
node = "sink"
port = "in"
"#;

    const WORKFLOW_YAML: &str = r#"
pureflow_version: "1"
id: flow
nodes:
  - id: source
    inputs: []
    outputs: [out]
  - id: sink
    inputs: [in]
    outputs: []
edges:
  - source: { node: source, port: out }
    target: { node: sink, port: in }
    capacity: 8
"#;

    #[test]
    fn validate_dispatches_toml_by_extension() {
        let output = validate_workflow_json(WORKFLOW_TOML, Path::new("workflow.toml"))
            .expect("TOML workflow should validate");

        assert_eq!(output, "valid workflow `flow`\nnodes: 2\nedges: 1\n");
    }

    #[test]
    fn validate_dispatches_yaml_by_extension() {
        let output = validate_workflow_json(WORKFLOW_YAML, Path::new("workflow.yaml"))
            .expect("YAML workflow should validate");

        assert_eq!(output, "valid workflow `flow`\nnodes: 2\nedges: 1\n");
    }

    #[test]
    fn validate_dispatches_yml_extension() {
        let output = validate_workflow_json(WORKFLOW_YAML, Path::new("flow.yml"))
            .expect(".yml extension should validate");

        assert_eq!(output, "valid workflow `flow`\nnodes: 2\nedges: 1\n");
    }

    #[test]
    fn validate_rejects_unsupported_extension() {
        let err = validate_workflow_json(WORKFLOW_JSON, Path::new("workflow.xml"))
            .expect_err("unknown extension should fail");

        assert!(
            err.to_string()
                .contains("unsupported workflow file extension `.xml`")
        );
    }

    #[test]
    fn validate_falls_back_to_json_with_no_extension() {
        let output = validate_workflow_json(WORKFLOW_JSON, Path::new("workflow"))
            .expect("no extension should fall back to JSON");

        assert_eq!(output, "valid workflow `flow`\nnodes: 2\nedges: 1\n");
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("test node id should be valid")
    }
}
