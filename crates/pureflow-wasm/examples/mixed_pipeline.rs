//! Native source -> WASM batch boundary -> native sink example.
//!
//! The middle node is a real `wasm32-wasip2` component invoked through
//! `WasmtimeBatchComponent`. The host still owns graph channels: native nodes
//! use `PortsIn`/`PortsOut`, while the WASM node is adapted through
//! `BatchNodeExecutor`.

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Arc, Mutex},
};

use pureflow_core::{
    NodeExecutor, PacketPayload, PortPacket, PortsIn, PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::{BatchNodeExecutor, StaticNodeExecutorRegistry, run_workflow_with_registry};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use pureflow_wasm::WasmtimeBatchComponent;
use pureflow_workflow::{EdgeDefinition, EdgeEndpoint, NodeDefinition, WorkflowDefinition};
use futures::{executor::block_on, future::BoxFuture};

const UPPERCASE_FIXTURE_MANIFEST: &str = "fixtures/uppercase-guest/Cargo.toml";
const UPPERCASE_FIXTURE_ARTIFACT: &str =
    "wasm32-wasip2/release/pureflow_wasm_uppercase_guest_fixture.wasm";

fn main() -> Result<()> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(execution_id("mixed-run-1"));
    let received_payloads: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let registry: StaticNodeExecutorRegistry<MixedPipelineExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("native-source"),
                MixedPipelineExecutor::Native(NativePipelineExecutor::source()),
            ),
            (
                node_id("wasm-upper"),
                MixedPipelineExecutor::Wasm(BatchNodeExecutor::new(wasm_uppercase_component()?)),
            ),
            (
                node_id("native-sink"),
                MixedPipelineExecutor::Native(NativePipelineExecutor::sink(Arc::clone(
                    &received_payloads,
                ))),
            ),
        ]));

    block_on(run_workflow_with_registry(&workflow, &execution, &registry))?;

    assert_eq!(
        received_payloads
            .lock()
            .expect("received payload lock should not be poisoned")
            .clone(),
        vec![b"HELLO FROM WASM".to_vec()]
    );
    Ok(())
}

enum MixedPipelineExecutor {
    Native(NativePipelineExecutor),
    Wasm(BatchNodeExecutor<WasmtimeBatchComponent>),
}

impl NodeExecutor for MixedPipelineExecutor {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        match self {
            Self::Native(executor) => executor.run(ctx, inputs, outputs),
            Self::Wasm(executor) => executor.run(ctx, inputs, outputs),
        }
    }
}

#[derive(Clone)]
enum NativePipelineExecutor {
    Source,
    Sink {
        received_payloads: Arc<Mutex<Vec<Vec<u8>>>>,
    },
}

impl NativePipelineExecutor {
    const fn source() -> Self {
        Self::Source
    }

    const fn sink(received_payloads: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        Self::Sink { received_payloads }
    }
}

impl NodeExecutor for NativePipelineExecutor {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self {
                Self::Source => {
                    outputs
                        .send(
                            &port_id("out"),
                            packet(
                                b"hello from wasm".to_vec(),
                                "native-source",
                                "out",
                                "wasm-upper",
                                "in",
                            ),
                            &cancellation,
                        )
                        .await?;
                }
                Self::Sink { received_payloads } => {
                    let packet: PortPacket = inputs
                        .recv(&port_id("in"), &cancellation)
                        .await?
                        .expect("WASM node should send one packet");
                    received_payloads
                        .lock()
                        .expect("received payload lock should not be poisoned")
                        .push(
                            packet
                                .payload()
                                .as_bytes()
                                .expect("example sends byte payloads")
                                .to_vec(),
                        );
                }
            }

            Ok(())
        })
    }
}

fn workflow() -> WorkflowDefinition {
    WorkflowDefinition::from_parts(
        workflow_id("mixed-flow"),
        [
            NodeDefinition::new(
                node_id("native-source"),
                Vec::<PortId>::new(),
                [port_id("out")],
            )
            .expect("valid source"),
            NodeDefinition::new(node_id("wasm-upper"), [port_id("in")], [port_id("out")])
                .expect("valid wasm node"),
            NodeDefinition::new(
                node_id("native-sink"),
                [port_id("in")],
                Vec::<PortId>::new(),
            )
            .expect("valid sink"),
        ],
        [
            EdgeDefinition::new(
                EdgeEndpoint::new(node_id("native-source"), port_id("out")),
                EdgeEndpoint::new(node_id("wasm-upper"), port_id("in")),
            ),
            EdgeDefinition::new(
                EdgeEndpoint::new(node_id("wasm-upper"), port_id("out")),
                EdgeEndpoint::new(node_id("native-sink"), port_id("in")),
            ),
        ],
    )
    .expect("valid mixed workflow")
}

fn packet(
    payload: Vec<u8>,
    source_node: &str,
    source_port: &str,
    target_node: &str,
    target_port: &str,
) -> PortPacket {
    let route: MessageRoute = MessageRoute::new(
        Some(MessageEndpoint::new(
            node_id(source_node),
            port_id(source_port),
        )),
        MessageEndpoint::new(node_id(target_node), port_id(target_port)),
    );
    let metadata: MessageMetadata = MessageMetadata::new(
        message_id("msg-1"),
        workflow_id("mixed-flow"),
        ExecutionMetadata::first_attempt(execution_id("mixed-run-1")),
        route,
    );

    PortPacket::new(metadata, PacketPayload::from(payload))
}

fn wasm_uppercase_component() -> Result<WasmtimeBatchComponent> {
    let fixture_path: PathBuf = build_uppercase_guest_fixture();
    let fixture_bytes: Vec<u8> = fs::read(&fixture_path).map_err(|err: std::io::Error| {
        pureflow_core::PureflowError::execution(format!(
            "failed to read fixture component `{}`: {err}",
            fixture_path.display()
        ))
    })?;
    WasmtimeBatchComponent::from_component_bytes(fixture_bytes)
}

fn build_uppercase_guest_fixture() -> PathBuf {
    let crate_dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest_path: PathBuf = crate_dir.join(UPPERCASE_FIXTURE_MANIFEST);
    let target_dir: PathBuf = env::temp_dir().join(format!(
        "pureflow-wasm-uppercase-guest-example-{}",
        std::process::id()
    ));
    let artifact_path: PathBuf = target_dir.join(UPPERCASE_FIXTURE_ARTIFACT);
    let cargo: OsString = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let output: Output = Command::new(cargo)
        .args([
            "build",
            "--manifest-path",
            path_as_str(&manifest_path),
            "--target",
            "wasm32-wasip2",
            "--release",
            "--target-dir",
            path_as_str(&target_dir),
        ])
        .env_remove("RUSTFLAGS")
        .output()
        .expect("fixture build command should run");

    assert!(
        output.status.success(),
        "fixture build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        artifact_path.is_file(),
        "fixture artifact was not written to {}",
        artifact_path.display(),
    );

    artifact_path
}

fn path_as_str(path: &Path) -> &str {
    path.to_str().expect("fixture path should be UTF-8")
}

fn execution_id(value: &str) -> ExecutionId {
    ExecutionId::new(value).expect("valid execution id")
}

fn message_id(value: &str) -> MessageId {
    MessageId::new(value).expect("valid message id")
}

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).expect("valid node id")
}

fn port_id(value: &str) -> PortId {
    PortId::new(value).expect("valid port id")
}

fn workflow_id(value: &str) -> WorkflowId {
    WorkflowId::new(value).expect("valid workflow id")
}
