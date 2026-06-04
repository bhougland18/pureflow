//! Wasmtime-backed batch adapter boundary for Pureflow.
//!
//! The crate owns the Component Model/WIT ABI and keeps Wasmtime types out of
//! `pureflow-core`. Guest components implement `pureflow:batch/pureflow-node`
//! from `wit/pureflow-batch.wit`; the host remains responsible for output port
//! validation before packets are sent through `PortsOut`.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use pureflow_core::{
    BatchExecutor, BatchInputs, BatchOutputs, PureflowError, PacketPayload, PortPacket, Result,
    capability::{CapabilityValidationError, NodeCapabilities},
    context::{CancellationRequest, CancellationToken, ExecutionAttempt, ExecutionMetadata},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use serde_json::Value;
use wasmtime::{
    Config, Engine, Store,
    component::{Component, ComponentExportIndex, Func, Instance, Linker, Val},
};

/// WIT package identifier implemented by Pureflow WASM batch guests.
pub const WIT_PACKAGE: &str = "pureflow:batch@0.1.0";

/// WIT world exported by Pureflow WASM batch guests.
pub const WIT_WORLD: &str = "pureflow-node";

const DEFAULT_GUEST_FUEL: u64 = 100_000_000;
const DEFAULT_CANCELLATION_EPOCH_DEADLINE: u64 = 1;
const DEFAULT_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Execution limits applied to each Wasmtime guest invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmtimeExecutionLimits {
    fuel: u64,
    cancellation_epoch_deadline: u64,
    cancellation_poll_interval: Duration,
}

impl WasmtimeExecutionLimits {
    /// Create limits with a fuel budget per invocation.
    #[must_use]
    pub const fn new(fuel: u64) -> Self {
        Self {
            fuel,
            cancellation_epoch_deadline: DEFAULT_CANCELLATION_EPOCH_DEADLINE,
            cancellation_poll_interval: DEFAULT_CANCELLATION_POLL_INTERVAL,
        }
    }

    /// Fuel units available to one guest invocation.
    #[must_use]
    pub const fn fuel(&self) -> u64 {
        self.fuel
    }

    /// Epoch ticks after which a cancellation increment interrupts the store.
    #[must_use]
    pub const fn cancellation_epoch_deadline(&self) -> u64 {
        self.cancellation_epoch_deadline
    }

    /// Poll interval used by the synchronous cancellation watcher.
    #[must_use]
    pub const fn cancellation_poll_interval(&self) -> Duration {
        self.cancellation_poll_interval
    }

    /// Return limits with a different epoch deadline.
    #[must_use]
    pub const fn with_cancellation_epoch_deadline(mut self, ticks: u64) -> Self {
        self.cancellation_epoch_deadline = ticks;
        self
    }

    /// Return limits with a different cancellation poll interval.
    #[must_use]
    pub const fn with_cancellation_poll_interval(mut self, interval: Duration) -> Self {
        self.cancellation_poll_interval = interval;
        self
    }
}

impl Default for WasmtimeExecutionLimits {
    fn default() -> Self {
        Self::new(DEFAULT_GUEST_FUEL)
    }
}

/// Wasmtime component prepared for Pureflow batch execution.
pub struct WasmtimeBatchComponent {
    engine: Engine,
    component: Component,
    limits: WasmtimeExecutionLimits,
}

impl WasmtimeBatchComponent {
    /// Compile a guest component from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if Wasmtime cannot configure the engine or compile the
    /// supplied component bytes.
    pub fn from_component_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        Self::from_component_bytes_with_limits(bytes, WasmtimeExecutionLimits::default())
    }

    /// Compile a guest component from bytes with explicit execution limits.
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
                PureflowError::execution(format!("failed to compile component: {err}"))
            })?;

        Ok(Self {
            engine,
            component,
            limits,
        })
    }

    /// Compile a guest component after validating the WASM capability boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if the capability descriptor declares effects that the
    /// current import-free WASM world cannot enforce, or if component
    /// compilation fails.
    pub fn from_component_bytes_with_capabilities(
        bytes: impl AsRef<[u8]>,
        capabilities: &NodeCapabilities,
    ) -> Result<Self> {
        Self::from_component_bytes_with_capabilities_and_limits(
            bytes,
            capabilities,
            WasmtimeExecutionLimits::default(),
        )
    }

    /// Compile a guest component after validating the WASM capability boundary
    /// and applying explicit execution limits.
    ///
    /// # Errors
    ///
    /// Returns an error if the capability descriptor declares effects that the
    /// current import-free WASM world cannot enforce, or if component
    /// compilation fails.
    pub fn from_component_bytes_with_capabilities_and_limits(
        bytes: impl AsRef<[u8]>,
        capabilities: &NodeCapabilities,
        limits: WasmtimeExecutionLimits,
    ) -> Result<Self> {
        validate_wasm_capabilities(capabilities)?;
        Self::from_component_bytes_with_limits(bytes, limits)
    }

    /// Execution limits used for each guest invocation.
    #[must_use]
    pub const fn limits(&self) -> WasmtimeExecutionLimits {
        self.limits
    }

    /// Instantiate and invoke the guest component with one batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the component cannot instantiate, the guest traps,
    /// or the guest returns malformed Pureflow data.
    pub fn invoke(&self, inputs: &BatchInputs) -> Result<BatchOutputs> {
        self.invoke_with_cancellation(inputs, &CancellationToken::active())
    }

    /// Instantiate and invoke the guest component with one batch, interrupting
    /// Wasmtime execution if cancellation is requested while the synchronous
    /// guest call is in progress.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation is already requested, the component
    /// cannot instantiate, the guest traps or exceeds its fuel budget, or the
    /// guest returns malformed Pureflow data.
    pub fn invoke_with_cancellation(
        &self,
        inputs: &BatchInputs,
        cancellation: &CancellationToken,
    ) -> Result<BatchOutputs> {
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
                    PureflowError::execution(format!("failed to instantiate component: {err}"))
                })?;
        let batch_index: ComponentExportIndex = instance
            .get_export_index(&mut store, None, "pureflow:batch/batch@0.1.0")
            .ok_or_else(|| {
                PureflowError::execution("component does not export pureflow:batch/batch@0.1.0")
            })?;
        let invoke_index: ComponentExportIndex = instance
            .get_export_index(&mut store, Some(&batch_index), "invoke")
            .ok_or_else(|| PureflowError::execution("component does not export batch.invoke"))?;
        let invoke: Func = instance
            .get_func(&mut store, invoke_index)
            .ok_or_else(|| PureflowError::execution("batch.invoke export is not a function"))?;

        let params: [Val; 1] = [batch_inputs_to_val(inputs)?];
        let mut results: [Val; 1] = [Val::Bool(false)];
        let call_result: std::result::Result<(), wasmtime::Error> =
            invoke.call(&mut store, &params, &mut results);
        let interrupted: bool = watcher.finish();
        if interrupted {
            let reason: String = cancellation.request().map_or_else(
                || String::from("wasm guest invocation cancelled"),
                |request: CancellationRequest| request.reason().to_owned(),
            );
            return Err(PureflowError::cancelled(reason));
        }
        let remaining_fuel: Option<u64> = store.get_fuel().ok();
        call_result.map_err(|err: wasmtime::Error| {
            map_guest_call_error(&err, self.limits, remaining_fuel)
        })?;

        let [result]: [Val; 1] = results;
        batch_outputs_from_result_val(result)
    }
}

impl BatchExecutor for WasmtimeBatchComponent {
    fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs> {
        Self::invoke(self, &inputs)
    }
}

/// Validate a capability descriptor for the current import-free WASM world.
///
/// # Errors
///
/// Returns an error if the descriptor declares any external effect capability.
pub fn validate_wasm_capabilities(capabilities: &NodeCapabilities) -> Result<()> {
    if let Some(effect) = capabilities.effects().first() {
        return Err(CapabilityValidationError::UnenforceableEffectCapability {
            node_id: capabilities.node_id().clone(),
            effect: *effect,
        }
        .into());
    }

    Ok(())
}

/// Convert Pureflow batch inputs to the WIT-facing ordered port batch shape.
///
/// # Errors
///
/// Returns an error if a payload cannot be represented by WIT ABI `0.1.0`.
pub fn to_wit_port_batches(inputs: &BatchInputs) -> Result<Vec<WitPortBatch>> {
    inputs
        .packets_by_port()
        .iter()
        .map(|(port_id, packets): (&PortId, &Vec<PortPacket>)| {
            Ok(WitPortBatch {
                port_id: port_id.to_string(),
                packets: packets
                    .iter()
                    .map(to_wit_packet)
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect()
}

/// Convert WIT-facing ordered port batches back to Pureflow batch outputs.
///
/// # Errors
///
/// Returns an error if a port identifier or packet metadata identifier fails
/// Pureflow validation, or if a control payload is not valid JSON.
pub fn from_wit_port_batches(port_batches: Vec<WitPortBatch>) -> Result<BatchOutputs> {
    let mut outputs: BatchOutputs = BatchOutputs::new();
    for port_batch in port_batches {
        let port_id: PortId = PortId::new(port_batch.port_id)?;
        for packet in port_batch.packets {
            outputs.push(port_id.clone(), from_wit_packet(packet)?);
        }
    }

    Ok(outputs)
}

/// WIT-facing port batch representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitPortBatch {
    /// Port identifier.
    pub port_id: String,
    /// Packets for the port, preserving batch order.
    pub packets: Vec<WitPacket>,
}

/// WIT-facing packet representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitPacket {
    /// Message metadata.
    pub metadata: pureflow_core::message::MessageMetadata,
    /// Packet payload.
    pub payload: WitPayload,
}

/// WIT-facing packet payload representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WitPayload {
    /// Byte payload.
    Bytes(Vec<u8>),
    /// JSON-encoded control payload.
    Control(String),
}

fn component_engine() -> Result<Engine> {
    let mut config: Config = Config::new();
    config.wasm_component_model(true);
    config.epoch_interruption(true);
    config.consume_fuel(true);
    Engine::new(&config).map_err(|err: wasmtime::Error| {
        PureflowError::execution(format!("failed to create Wasmtime engine: {err}"))
    })
}

struct CancellationWatcher {
    complete: Arc<AtomicBool>,
    interrupted: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl CancellationWatcher {
    fn spawn(engine: Engine, cancellation: CancellationToken, interval: Duration) -> Result<Self> {
        let complete: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let interrupted: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let thread_complete: Arc<AtomicBool> = Arc::clone(&complete);
        let thread_interrupted: Arc<AtomicBool> = Arc::clone(&interrupted);
        let thread: JoinHandle<()> = thread::Builder::new()
            .name(String::from("pureflow-wasm-cancellation"))
            .spawn(move || {
                while !thread_complete.load(Ordering::Acquire) {
                    if cancellation.is_cancelled() {
                        thread_interrupted.store(true, Ordering::Release);
                        engine.increment_epoch();
                        break;
                    }
                    thread::sleep(interval);
                }
            })
            .map_err(|err: std::io::Error| {
                PureflowError::execution(format!("failed to start WASM cancellation watcher: {err}"))
            })?;

        Ok(Self {
            complete,
            interrupted,
            thread: Some(thread),
        })
    }

    fn finish(mut self) -> bool {
        self.complete.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.interrupted.load(Ordering::Acquire)
    }
}

impl Drop for CancellationWatcher {
    fn drop(&mut self) {
        self.complete.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn map_guest_call_error(
    err: &wasmtime::Error,
    limits: WasmtimeExecutionLimits,
    remaining_fuel: Option<u64>,
) -> PureflowError {
    let message: String = err.to_string();
    if remaining_fuel == Some(0) || message.to_ascii_lowercase().contains("fuel") {
        PureflowError::execution(format!(
            "guest exceeded Wasmtime fuel limit of {} units",
            limits.fuel()
        ))
    } else {
        PureflowError::execution(format!("guest invoke failed: {err}"))
    }
}

fn batch_inputs_to_val(inputs: &BatchInputs) -> Result<Val> {
    Ok(Val::List(
        to_wit_port_batches(inputs)?
            .into_iter()
            .map(port_batch_to_val)
            .collect(),
    ))
}

fn port_batch_to_val(port_batch: WitPortBatch) -> Val {
    Val::Record(vec![
        ("port-id".to_owned(), Val::String(port_batch.port_id)),
        (
            "packets".to_owned(),
            Val::List(port_batch.packets.into_iter().map(packet_to_val).collect()),
        ),
    ])
}

fn packet_to_val(packet: WitPacket) -> Val {
    Val::Record(vec![
        ("metadata".to_owned(), metadata_to_val(&packet.metadata)),
        ("payload".to_owned(), payload_to_val(packet.payload)),
    ])
}

fn metadata_to_val(metadata: &MessageMetadata) -> Val {
    Val::Record(vec![
        (
            "message-id".to_owned(),
            Val::String(metadata.message_id().to_string()),
        ),
        (
            "workflow-id".to_owned(),
            Val::String(metadata.workflow_id().to_string()),
        ),
        (
            "execution".to_owned(),
            Val::Record(vec![
                (
                    "execution-id".to_owned(),
                    Val::String(metadata.execution().execution_id().to_string()),
                ),
                (
                    "attempt".to_owned(),
                    Val::U32(metadata.execution().attempt().get()),
                ),
            ]),
        ),
        ("route".to_owned(), route_to_val(metadata.route())),
    ])
}

fn route_to_val(route: &MessageRoute) -> Val {
    Val::Record(vec![
        (
            "source".to_owned(),
            Val::Option(
                route
                    .source()
                    .map(|source: &MessageEndpoint| Box::new(endpoint_to_val(source))),
            ),
        ),
        ("target".to_owned(), endpoint_to_val(route.target())),
    ])
}

fn endpoint_to_val(endpoint: &MessageEndpoint) -> Val {
    Val::Record(vec![
        (
            "node-id".to_owned(),
            Val::String(endpoint.node_id().to_string()),
        ),
        (
            "port-id".to_owned(),
            Val::String(endpoint.port_id().to_string()),
        ),
    ])
}

fn payload_to_val(payload: WitPayload) -> Val {
    match payload {
        WitPayload::Bytes(bytes) => {
            Val::Variant("bytes".to_owned(), Some(Box::new(bytes_to_list_val(bytes))))
        }
        WitPayload::Control(value) => {
            Val::Variant("control".to_owned(), Some(Box::new(Val::String(value))))
        }
    }
}

fn bytes_to_list_val(bytes: Vec<u8>) -> Val {
    Val::List(bytes.into_iter().map(Val::U8).collect())
}

fn batch_outputs_from_result_val(value: Val) -> Result<BatchOutputs> {
    let result: std::result::Result<Option<Box<Val>>, Option<Box<Val>>> = match value {
        Val::Result(result) => result,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-result from batch.invoke",
            ));
        }
    };

    match result {
        Ok(Some(value)) => port_batches_from_val(*value).and_then(from_wit_port_batches),
        Ok(None) => Err(PureflowError::execution(
            "guest returned empty ok result from batch.invoke",
        )),
        Err(Some(value)) => Err(batch_error_from_val(*value)),
        Err(None) => Err(PureflowError::execution(
            "guest returned empty error from batch.invoke",
        )),
    }
}

fn batch_error_from_val(value: Val) -> PureflowError {
    match value {
        Val::Variant(name, Some(detail)) => match *detail {
            Val::String(message) => {
                PureflowError::execution(format!("guest returned {name}: {message}"))
            }
            _ => PureflowError::execution(format!("guest returned malformed {name} error")),
        },
        Val::Variant(name, None) => {
            PureflowError::execution(format!("guest returned {name} without detail"))
        }
        _ => PureflowError::execution("guest returned malformed batch error"),
    }
}

fn port_batches_from_val(value: Val) -> Result<Vec<WitPortBatch>> {
    let values: Vec<Val> = match value {
        Val::List(values) => values,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-list batch output",
            ));
        }
    };

    values.into_iter().map(port_batch_from_val).collect()
}

fn port_batch_from_val(value: Val) -> Result<WitPortBatch> {
    let fields: Vec<(String, Val)> = record_fields(value, "port batch")?;
    let port_id: String = required_string_field(&fields, "port-id", "port batch")?;
    let packets: Vec<WitPacket> = required_list_field(&fields, "packets", "port batch")?
        .into_iter()
        .map(packet_from_val)
        .collect::<Result<Vec<_>>>()?;

    Ok(WitPortBatch { port_id, packets })
}

fn packet_from_val(value: Val) -> Result<WitPacket> {
    let fields: Vec<(String, Val)> = record_fields(value, "packet")?;
    let metadata: MessageMetadata =
        metadata_from_val(required_field(&fields, "metadata", "packet")?.clone())?;
    let payload: WitPayload =
        payload_from_val(required_field(&fields, "payload", "packet")?.clone())?;

    Ok(WitPacket { metadata, payload })
}

fn metadata_from_val(value: Val) -> Result<MessageMetadata> {
    let fields: Vec<(String, Val)> = record_fields(value, "message metadata")?;
    let message_id: MessageId = MessageId::new(required_string_field(
        &fields,
        "message-id",
        "message metadata",
    )?)?;
    let workflow_id: WorkflowId = WorkflowId::new(required_string_field(
        &fields,
        "workflow-id",
        "message metadata",
    )?)?;
    let execution: ExecutionMetadata =
        execution_from_val(required_field(&fields, "execution", "message metadata")?.clone())?;
    let route: MessageRoute =
        route_from_val(required_field(&fields, "route", "message metadata")?.clone())?;

    Ok(MessageMetadata::new(
        message_id,
        workflow_id,
        execution,
        route,
    ))
}

fn execution_from_val(value: Val) -> Result<ExecutionMetadata> {
    let fields: Vec<(String, Val)> = record_fields(value, "execution metadata")?;
    let execution_id: ExecutionId = ExecutionId::new(required_string_field(
        &fields,
        "execution-id",
        "execution metadata",
    )?)?;
    let attempt: u32 = required_u32_field(&fields, "attempt", "execution metadata")?;
    let attempt: ExecutionAttempt = NonZeroU32::new(attempt)
        .map(ExecutionAttempt::new)
        .ok_or_else(|| PureflowError::execution("guest returned zero execution attempt"))?;

    Ok(ExecutionMetadata::new(execution_id, attempt))
}

fn route_from_val(value: Val) -> Result<MessageRoute> {
    let fields: Vec<(String, Val)> = record_fields(value, "message route")?;
    let source: Option<MessageEndpoint> = match required_field(&fields, "source", "message route")?
    {
        Val::Option(Some(source)) => Some(endpoint_from_val(source.as_ref().clone())?),
        Val::Option(None) => None,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-option route source",
            ));
        }
    };
    let target: MessageEndpoint =
        endpoint_from_val(required_field(&fields, "target", "message route")?.clone())?;

    Ok(MessageRoute::new(source, target))
}

fn endpoint_from_val(value: Val) -> Result<MessageEndpoint> {
    let fields: Vec<(String, Val)> = record_fields(value, "message endpoint")?;
    let node_id: NodeId = NodeId::new(required_string_field(
        &fields,
        "node-id",
        "message endpoint",
    )?)?;
    let port_id: PortId = PortId::new(required_string_field(
        &fields,
        "port-id",
        "message endpoint",
    )?)?;

    Ok(MessageEndpoint::new(node_id, port_id))
}

fn payload_from_val(value: Val) -> Result<WitPayload> {
    let (name, payload): (String, Option<Box<Val>>) = match value {
        Val::Variant(name, payload) => (name, payload),
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-variant payload",
            ));
        }
    };
    match (name.as_str(), payload) {
        ("bytes", Some(value)) => Ok(WitPayload::Bytes(bytes_from_val(*value)?)),
        ("control", Some(value)) => {
            let value: String = match *value {
                Val::String(value) => value,
                _ => {
                    return Err(PureflowError::execution(
                        "guest returned non-string control payload",
                    ));
                }
            };
            Ok(WitPayload::Control(value))
        }
        (kind, _) => Err(PureflowError::execution(format!(
            "guest returned unsupported payload variant: {kind}"
        ))),
    }
}

fn bytes_from_val(value: Val) -> Result<Vec<u8>> {
    let values: Vec<Val> = match value {
        Val::List(values) => values,
        _ => {
            return Err(PureflowError::execution(
                "guest returned non-list bytes payload",
            ));
        }
    };
    values
        .into_iter()
        .map(|value: Val| match value {
            Val::U8(byte) => Ok(byte),
            _ => Err(PureflowError::execution(
                "guest returned non-u8 byte payload element",
            )),
        })
        .collect()
}

fn record_fields(value: Val, context: &str) -> Result<Vec<(String, Val)>> {
    let fields: Vec<(String, Val)> = match value {
        Val::Record(fields) => fields,
        _ => {
            return Err(PureflowError::execution(format!(
                "guest returned non-record {context}"
            )));
        }
    };
    Ok(fields)
}

fn required_field<'a>(fields: &'a [(String, Val)], name: &str, context: &str) -> Result<&'a Val> {
    fields
        .iter()
        .find_map(|(field_name, value): &(String, Val)| (field_name == name).then_some(value))
        .ok_or_else(|| PureflowError::execution(format!("guest omitted {context} field {name}")))
}

fn required_string_field(fields: &[(String, Val)], name: &str, context: &str) -> Result<String> {
    match required_field(fields, name, context)? {
        Val::String(value) => Ok(value.clone()),
        _ => Err(PureflowError::execution(format!(
            "guest returned non-string {context} field {name}"
        ))),
    }
}

fn required_u32_field(fields: &[(String, Val)], name: &str, context: &str) -> Result<u32> {
    match required_field(fields, name, context)? {
        Val::U32(value) => Ok(*value),
        _ => Err(PureflowError::execution(format!(
            "guest returned non-u32 {context} field {name}"
        ))),
    }
}

fn required_list_field(fields: &[(String, Val)], name: &str, context: &str) -> Result<Vec<Val>> {
    match required_field(fields, name, context)? {
        Val::List(values) => Ok(values.clone()),
        _ => Err(PureflowError::execution(format!(
            "guest returned non-list {context} field {name}"
        ))),
    }
}

#[allow(clippy::match_wildcard_for_single_variants)]
fn to_wit_packet(packet: &PortPacket) -> Result<WitPacket> {
    let payload: WitPayload = match packet.payload() {
        PacketPayload::Bytes(bytes) => WitPayload::Bytes(bytes.to_vec()),
        PacketPayload::Control(value) => WitPayload::Control(value.to_string()),
        #[allow(unreachable_patterns)]
        _ => {
            return Err(PureflowError::execution(
                "payload is not supported by WIT ABI 0.1.0",
            ));
        }
    };

    Ok(WitPacket {
        metadata: packet.metadata().clone(),
        payload,
    })
}

fn from_wit_packet(packet: WitPacket) -> Result<PortPacket> {
    let payload: PacketPayload = match packet.payload {
        WitPayload::Bytes(bytes) => PacketPayload::from(bytes),
        WitPayload::Control(value) => {
            let value: Value = serde_json::from_str(&value).map_err(|err: serde_json::Error| {
                PureflowError::execution(format!("guest returned invalid control payload: {err}"))
            })?;
            PacketPayload::from(value)
        }
    };

    Ok(PortPacket::new(packet.metadata, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pureflow_core::{
        capability::{EffectCapability, PortCapability, PortCapabilityDirection},
        context::{CancellationRequest, CancellationToken, ExecutionAttempt, ExecutionMetadata},
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use pureflow_types::{ExecutionId, MessageId, NodeId, WorkflowId};
    use quickcheck::{Arbitrary, Gen, QuickCheck};
    use serde::Deserialize;
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        env,
        ffi::OsString,
        fs,
        num::NonZeroU32,
        path::{Path, PathBuf},
        process::{Command, Output},
        sync::OnceLock,
    };

    const UPPERCASE_FIXTURE_INPUTS_JSON: &str =
        include_str!("../fixtures/uppercase-guest/testdata/inputs.json");
    const UPPERCASE_FIXTURE_EXPECTED_OUTPUTS_JSON: &str =
        include_str!("../fixtures/uppercase-guest/testdata/expected-outputs.json");
    const UPPERCASE_FIXTURE_MANIFEST: &str = "fixtures/uppercase-guest/Cargo.toml";
    const UPPERCASE_FIXTURE_ARTIFACT: &str =
        "wasm32-wasip2/release/pureflow_wasm_uppercase_guest_fixture.wasm";
    static QUICKCHECK_UPPERCASE_COMPONENT: OnceLock<WasmtimeBatchComponent> = OnceLock::new();

    #[derive(Debug, Deserialize)]
    struct FixturePortBatch {
        #[serde(rename = "port-id")]
        port_id: String,
        packets: Vec<FixturePacket>,
    }

    impl FixturePortBatch {
        fn into_wit(self) -> WitPortBatch {
            WitPortBatch {
                port_id: self.port_id,
                packets: self
                    .packets
                    .into_iter()
                    .map(FixturePacket::into_wit)
                    .collect(),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct FixturePacket {
        metadata: FixtureMessageMetadata,
        payload: FixturePayload,
    }

    impl FixturePacket {
        fn into_wit(self) -> WitPacket {
            WitPacket {
                metadata: self.metadata.into_message_metadata(),
                payload: self.payload.into_wit(),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct FixtureMessageMetadata {
        #[serde(rename = "message-id")]
        message_id: String,
        #[serde(rename = "workflow-id")]
        workflow_id: String,
        execution: FixtureExecution,
        route: FixtureRoute,
    }

    impl FixtureMessageMetadata {
        fn into_message_metadata(self) -> MessageMetadata {
            MessageMetadata::new(
                message_id(&self.message_id),
                workflow_id(&self.workflow_id),
                self.execution.into_execution_metadata(),
                self.route.into_message_route(),
            )
        }
    }

    #[derive(Debug, Deserialize)]
    struct FixtureExecution {
        #[serde(rename = "execution-id")]
        execution_id: String,
        attempt: u32,
    }

    impl FixtureExecution {
        fn into_execution_metadata(self) -> ExecutionMetadata {
            let attempt: NonZeroU32 =
                NonZeroU32::new(self.attempt).expect("fixture attempt must be non-zero");
            ExecutionMetadata::new(
                execution_id(&self.execution_id),
                ExecutionAttempt::new(attempt),
            )
        }
    }

    #[derive(Debug, Deserialize)]
    struct FixtureRoute {
        source: Option<FixtureEndpoint>,
        target: FixtureEndpoint,
    }

    impl FixtureRoute {
        fn into_message_route(self) -> MessageRoute {
            MessageRoute::new(
                self.source.map(FixtureEndpoint::into_message_endpoint),
                self.target.into_message_endpoint(),
            )
        }
    }

    #[derive(Debug, Deserialize)]
    struct FixtureEndpoint {
        #[serde(rename = "node-id")]
        node_id: String,
        #[serde(rename = "port-id")]
        port_id: String,
    }

    impl FixtureEndpoint {
        fn into_message_endpoint(self) -> MessageEndpoint {
            MessageEndpoint::new(node_id(&self.node_id), port_id(&self.port_id))
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum FixturePayload {
        Bytes(Vec<u8>),
        Control(String),
    }

    impl FixturePayload {
        fn into_wit(self) -> WitPayload {
            match self {
                Self::Bytes(bytes) => WitPayload::Bytes(bytes),
                Self::Control(value) => WitPayload::Control(value),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct GeneratedBatch {
        ports: Vec<GeneratedPortBatch>,
    }

    impl GeneratedBatch {
        fn into_wit(self) -> Vec<WitPortBatch> {
            self.ports
                .into_iter()
                .map(GeneratedPortBatch::into_wit)
                .collect()
        }
    }

    impl Arbitrary for GeneratedBatch {
        fn arbitrary(g: &mut Gen) -> Self {
            let port_count = usize::arbitrary(g) % 4;
            let ports = (0..port_count)
                .map(|port_index| GeneratedPortBatch::arbitrary(g, port_index))
                .collect();

            Self { ports }
        }
    }

    #[derive(Debug, Clone)]
    struct GeneratedPortBatch {
        port_id: String,
        packets: Vec<GeneratedPacket>,
    }

    impl GeneratedPortBatch {
        fn arbitrary(g: &mut Gen, port_index: usize) -> Self {
            let packet_count = usize::arbitrary(g) % 5;
            let port_id = format!("in{port_index}");
            let packets = (0..packet_count)
                .map(|packet_index| GeneratedPacket::arbitrary(g, port_index, packet_index))
                .collect();

            Self { port_id, packets }
        }

        fn into_wit(self) -> WitPortBatch {
            WitPortBatch {
                port_id: self.port_id,
                packets: self
                    .packets
                    .into_iter()
                    .map(GeneratedPacket::into_wit)
                    .collect(),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct GeneratedPacket {
        metadata: MessageMetadata,
        bytes: Vec<u8>,
    }

    impl GeneratedPacket {
        fn arbitrary(g: &mut Gen, port_index: usize, packet_index: usize) -> Self {
            let byte_count = usize::arbitrary(g) % 65;
            let bytes = (0..byte_count).map(|_| u8::arbitrary(g)).collect();
            let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
            let target: MessageEndpoint =
                MessageEndpoint::new(node_id("wasm"), port_id(&format!("in{port_index}")));
            let route: MessageRoute = MessageRoute::new(Some(source), target);
            let execution: ExecutionMetadata =
                ExecutionMetadata::first_attempt(execution_id("run-quickcheck"));
            let metadata = MessageMetadata::new(
                message_id(&format!("msg-{port_index}-{packet_index}")),
                workflow_id("flow-quickcheck"),
                execution,
                route,
            );

            Self { metadata, bytes }
        }

        fn into_wit(self) -> WitPacket {
            WitPacket {
                metadata: self.metadata,
                payload: WitPayload::Bytes(self.bytes),
            }
        }
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

    fn metadata() -> MessageMetadata {
        let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id("wasm"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route)
    }

    fn fixture_port_batches_from_json(json: &str) -> Vec<WitPortBatch> {
        serde_json::from_str::<Vec<FixturePortBatch>>(json)
            .expect("fixture JSON must parse")
            .into_iter()
            .map(FixturePortBatch::into_wit)
            .collect()
    }

    fn batch_inputs_from_wit_port_batches(port_batches: Vec<WitPortBatch>) -> BatchInputs {
        let mut packets_by_port: BTreeMap<PortId, Vec<PortPacket>> = BTreeMap::new();
        for port_batch in port_batches {
            let port_id: PortId = port_id(&port_batch.port_id);
            let packets: Vec<PortPacket> = port_batch
                .packets
                .into_iter()
                .map(|packet: WitPacket| from_wit_packet(packet).expect("fixture packet decodes"))
                .collect();
            packets_by_port.insert(port_id, packets);
        }

        BatchInputs::from_packets(packets_by_port)
    }

    fn uppercase_fixture_outputs(inputs: &[WitPortBatch]) -> Vec<WitPortBatch> {
        let mut packets: Vec<WitPacket> = Vec::new();

        for port_batch in inputs {
            for packet in &port_batch.packets {
                let mut packet: WitPacket = packet.clone();
                let WitPayload::Bytes(bytes) = packet.payload else {
                    panic!("uppercase fixture success vectors must contain only byte payloads");
                };
                packet.payload = WitPayload::Bytes(
                    bytes
                        .into_iter()
                        .map(|byte: u8| byte.to_ascii_uppercase())
                        .collect(),
                );
                packets.push(packet);
            }
        }

        vec![WitPortBatch {
            port_id: "out".to_owned(),
            packets,
        }]
    }

    fn build_uppercase_guest_fixture() -> PathBuf {
        let crate_dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let manifest_path: PathBuf = crate_dir.join(UPPERCASE_FIXTURE_MANIFEST);
        let target_dir: PathBuf = env::temp_dir().join(format!(
            "pureflow-wasm-uppercase-guest-fixture-{}",
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

    fn wasm32_wasip2_target_available() -> bool {
        let rustc: OsString = env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc"));
        let Ok(output) = Command::new(rustc)
            .args(["--print", "target-libdir", "--target", "wasm32-wasip2"])
            .env_remove("RUSTFLAGS")
            .output()
        else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        let libdir: PathBuf = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        fs::read_dir(libdir).is_ok_and(|entries| {
            entries.filter_map(std::result::Result::ok).any(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with("libcore-")
                        && Path::new(name)
                            .extension()
                            .is_some_and(|extension| extension.eq_ignore_ascii_case("rlib"))
                })
            })
        })
    }

    fn path_as_str(path: &Path) -> &str {
        path.to_str().expect("fixture path should be UTF-8")
    }

    #[test]
    fn constants_name_the_wit_abi() {
        assert_eq!(WIT_PACKAGE, "pureflow:batch@0.1.0");
        assert_eq!(WIT_WORLD, "pureflow-node");
    }

    #[test]
    fn wasm_capabilities_accept_import_free_descriptor() {
        let capabilities: NodeCapabilities = NodeCapabilities::native_passive(
            node_id("wasm"),
            [
                PortCapability::new(port_id("in"), PortCapabilityDirection::Receive),
                PortCapability::new(port_id("out"), PortCapabilityDirection::Emit),
            ],
        )
        .expect("valid capabilities");

        validate_wasm_capabilities(&capabilities).expect("no host imports required");
    }

    #[test]
    fn wasm_capabilities_reject_effects_without_imports() {
        let capabilities: NodeCapabilities = NodeCapabilities::new(
            node_id("wasm"),
            [PortCapability::new(
                port_id("in"),
                PortCapabilityDirection::Receive,
            )],
            [EffectCapability::Clock],
        )
        .expect("valid descriptor shape");

        let err: PureflowError =
            validate_wasm_capabilities(&capabilities).expect_err("effect must be denied");

        assert_eq!(err.code(), pureflow_core::ErrorCode::InvalidCapabilities);
        assert!(err.to_string().contains("not enforceable"));
    }

    #[test]
    fn port_batches_round_trip_bytes_and_control_payloads() {
        let mut inputs: BatchInputs = BatchInputs::new();
        inputs.push(
            port_id("in"),
            PortPacket::new(
                metadata(),
                PacketPayload::from(b"bytes".as_slice().to_vec()),
            ),
        );
        inputs.push(
            port_id("control"),
            PortPacket::new(metadata(), PacketPayload::from(json!({"op": "flush"}))),
        );

        let wit_batches: Vec<WitPortBatch> =
            to_wit_port_batches(&inputs).expect("inputs should encode as WIT batches");
        let outputs: BatchOutputs =
            from_wit_port_batches(wit_batches).expect("WIT batches should decode");

        assert_eq!(outputs.packets(&port_id("in")).len(), 1);
        assert_eq!(outputs.packets(&port_id("control")).len(), 1);
    }

    #[test]
    fn invalid_control_payload_is_rejected() {
        let packet: WitPacket = WitPacket {
            metadata: metadata(),
            payload: WitPayload::Control("not-json".to_owned()),
        };

        let err: PureflowError = from_wit_packet(packet).expect_err("invalid JSON should fail");

        assert_eq!(err.code(), pureflow_core::ErrorCode::NodeExecutionFailed);
    }

    #[test]
    fn dynamic_result_value_decodes_outputs() {
        let output = WitPortBatch {
            port_id: "out".to_owned(),
            packets: vec![WitPacket {
                metadata: metadata(),
                payload: WitPayload::Bytes(b"payload".to_vec()),
            }],
        };
        let result = Val::Result(Ok(Some(Box::new(Val::List(vec![port_batch_to_val(
            output,
        )])))));

        let outputs = batch_outputs_from_result_val(result).expect("result should decode");

        assert_eq!(outputs.packets(&port_id("out")).len(), 1);
    }

    #[test]
    fn dynamic_guest_error_maps_to_execution_error() {
        let result = Val::Result(Err(Some(Box::new(Val::Variant(
            "guest-failure".to_owned(),
            Some(Box::new(Val::String("boom".to_owned()))),
        )))));

        let err = batch_outputs_from_result_val(result).expect_err("guest error should fail");

        assert_eq!(err.code(), pureflow_core::ErrorCode::NodeExecutionFailed);
    }

    #[test]
    fn uppercase_guest_fixture_testdata_matches_wit_shape() {
        let inputs: Vec<WitPortBatch> =
            fixture_port_batches_from_json(UPPERCASE_FIXTURE_INPUTS_JSON);
        let expected_outputs: Vec<WitPortBatch> =
            fixture_port_batches_from_json(UPPERCASE_FIXTURE_EXPECTED_OUTPUTS_JSON);

        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].port_id, "in");
        assert_eq!(expected_outputs, uppercase_fixture_outputs(&inputs));

        let outputs: BatchOutputs =
            from_wit_port_batches(expected_outputs).expect("expected fixture outputs must decode");
        assert_eq!(outputs.packets(&port_id("out")).len(), 2);
    }

    #[test]
    fn wasmtime_adapter_invokes_real_uppercase_guest_fixture() {
        if !wasm32_wasip2_target_available() {
            eprintln!(
                "skipping real WASM guest conformance test; run through `nix develop .` to provide wasm32-wasip2"
            );
            return;
        }

        let fixture_path: PathBuf = build_uppercase_guest_fixture();
        let fixture_bytes: Vec<u8> = fs::read(fixture_path).expect("fixture component is readable");
        let component: WasmtimeBatchComponent =
            WasmtimeBatchComponent::from_component_bytes(fixture_bytes)
                .expect("fixture component compiles");
        let empty_outputs: BatchOutputs = component
            .invoke(&BatchInputs::new())
            .expect("fixture guest accepts an empty batch");
        assert!(empty_outputs.packets(&port_id("out")).is_empty());

        let inputs: Vec<WitPortBatch> =
            fixture_port_batches_from_json(UPPERCASE_FIXTURE_INPUTS_JSON);
        let expected_outputs: BatchOutputs = from_wit_port_batches(fixture_port_batches_from_json(
            UPPERCASE_FIXTURE_EXPECTED_OUTPUTS_JSON,
        ))
        .expect("expected fixture outputs decode");

        let actual_outputs: BatchOutputs = component
            .invoke(&batch_inputs_from_wit_port_batches(inputs))
            .expect("fixture guest invocation succeeds");

        assert_eq!(actual_outputs, expected_outputs);
    }

    #[test]
    fn wasmtime_adapter_rejects_pre_cancelled_invocation() {
        if !wasm32_wasip2_target_available() {
            eprintln!(
                "skipping WASM cancellation test; run through `nix develop .` to provide wasm32-wasip2"
            );
            return;
        }

        let fixture_path: PathBuf = build_uppercase_guest_fixture();
        let fixture_bytes: Vec<u8> = fs::read(fixture_path).expect("fixture component is readable");
        let component: WasmtimeBatchComponent =
            WasmtimeBatchComponent::from_component_bytes(fixture_bytes)
                .expect("fixture component compiles");
        let cancellation: CancellationToken =
            CancellationToken::cancelled(CancellationRequest::new("test shutdown"));

        let err: PureflowError = component
            .invoke_with_cancellation(&BatchInputs::new(), &cancellation)
            .expect_err("pre-cancelled invocation must fail before guest execution");

        assert_eq!(err.code(), pureflow_core::ErrorCode::ExecutionCancelled);
        assert!(err.to_string().contains("test shutdown"));
    }

    #[test]
    fn wasmtime_adapter_reports_stable_fuel_limit_error() {
        if !wasm32_wasip2_target_available() {
            eprintln!(
                "skipping WASM fuel limit test; run through `nix develop .` to provide wasm32-wasip2"
            );
            return;
        }

        let fixture_path: PathBuf = build_uppercase_guest_fixture();
        let fixture_bytes: Vec<u8> = fs::read(fixture_path).expect("fixture component is readable");
        let component: WasmtimeBatchComponent =
            WasmtimeBatchComponent::from_component_bytes_with_limits(
                fixture_bytes,
                WasmtimeExecutionLimits::new(0),
            )
            .expect("fixture component compiles");
        let inputs: Vec<WitPortBatch> =
            fixture_port_batches_from_json(UPPERCASE_FIXTURE_INPUTS_JSON);

        let err: PureflowError = component
            .invoke(&batch_inputs_from_wit_port_batches(inputs))
            .expect_err("zero fuel should trap with a stable host error");

        assert_eq!(err.code(), pureflow_core::ErrorCode::NodeExecutionFailed);
        assert!(
            err.to_string()
                .contains("guest exceeded Wasmtime fuel limit of 0 units"),
            "unexpected fuel error: {err}"
        );
    }

    #[test]
    fn wasmtime_adapter_preserves_generated_byte_batches_across_component_boundary() {
        if !wasm32_wasip2_target_available() {
            eprintln!(
                "skipping generated WASM boundary conformance test; run through `nix develop .` to provide wasm32-wasip2"
            );
            return;
        }

        let fixture_path: PathBuf = build_uppercase_guest_fixture();
        let fixture_bytes: Vec<u8> = fs::read(fixture_path).expect("fixture component is readable");
        let component: WasmtimeBatchComponent =
            WasmtimeBatchComponent::from_component_bytes(fixture_bytes)
                .expect("fixture component compiles");
        let _ = QUICKCHECK_UPPERCASE_COMPONENT.set(component);

        QuickCheck::new()
            .tests(32)
            .quickcheck(generated_byte_batch_boundary_holds as fn(GeneratedBatch) -> bool);
    }

    fn generated_byte_batch_boundary_holds(generated: GeneratedBatch) -> bool {
        let component = QUICKCHECK_UPPERCASE_COMPONENT
            .get()
            .expect("quickcheck component initialized");
        let inputs: Vec<WitPortBatch> = generated.into_wit();
        let expected_outputs: BatchOutputs =
            from_wit_port_batches(uppercase_fixture_outputs(&inputs))
                .expect("generated expected outputs decode");
        let actual_outputs: BatchOutputs = component
            .invoke(&batch_inputs_from_wit_port_batches(inputs))
            .expect("generated fixture guest invocation succeeds");

        actual_outputs == expected_outputs
    }
}
