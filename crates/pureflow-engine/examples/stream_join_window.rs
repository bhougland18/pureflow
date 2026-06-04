//! Runnable stream join/window workload using `PortsIn::recv_any`.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use pureflow_core::{
    PureflowError, JsonlMetadataSink, NodeExecutor, PacketPayload, PortPacket, PortsIn, PortsOut,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_engine::{
    StaticNodeExecutorRegistry, run_workflow_with_registry_and_metadata_sink_summary,
};
use pureflow_test_kit::{NodeBuilder, WorkflowBuilder, drain_port};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId};
use pureflow_workflow::WorkflowDefinition;
use futures::{executor::block_on, future::BoxFuture};

const EVENT_SOURCE_PACKETS: &[ScheduledPacket] = &[
    ScheduledPacket::event("w1", "alpha", "click"),
    ScheduledPacket::event("w1", "beta", "open"),
    ScheduledPacket::event("w2", "alpha", "checkout"),
    ScheduledPacket::event("w1", "gamma", "orphan-event"),
    ScheduledPacket::event("w4", "epsilon", "late-orphan"),
];

const PROFILE_SOURCE_PACKETS: &[ScheduledPacket] = &[
    ScheduledPacket::profile("w1", "alpha", "gold"),
    ScheduledPacket::profile("w1", "beta", "silver"),
    ScheduledPacket::profile("w2", "alpha", "platinum"),
    ScheduledPacket::profile("w3", "delta", "orphan-profile"),
];

#[derive(Debug, Clone, Copy)]
enum ScheduledPacket {
    Event {
        window: &'static str,
        account: &'static str,
        action: &'static str,
    },
    Profile {
        window: &'static str,
        account: &'static str,
        tier: &'static str,
    },
}

impl ScheduledPacket {
    const fn event(window: &'static str, account: &'static str, action: &'static str) -> Self {
        Self::Event {
            window,
            account,
            action,
        }
    }

    const fn profile(window: &'static str, account: &'static str, tier: &'static str) -> Self {
        Self::Profile {
            window,
            account,
            tier,
        }
    }
}

impl fmt::Display for ScheduledPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event {
                window,
                account,
                action,
            } => write!(f, "event:{window}:{account}:{action}"),
            Self::Profile {
                window,
                account,
                tier,
            } => write!(f, "profile:{window}:{account}:{tier}"),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct JoinDiagnostics {
    recv_any_order: Vec<String>,
    unmatched_events: Vec<String>,
    unmatched_profiles: Vec<String>,
}

#[derive(Debug, Clone)]
enum StreamJoinExecutor {
    Source {
        output_port: &'static str,
        packets: &'static [ScheduledPacket],
    },
    JoinWindow {
        diagnostics: Arc<Mutex<JoinDiagnostics>>,
    },
    Sink {
        received: Arc<Mutex<Vec<String>>>,
    },
}

impl NodeExecutor for StreamJoinExecutor {
    type RunFuture<'a> = BoxFuture<'a, pureflow_core::Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match self {
                Self::Source {
                    output_port,
                    packets,
                } => {
                    for (index, scheduled) in packets.iter().enumerate() {
                        let payload: String = scheduled.to_string();
                        outputs
                            .send(
                                &port_id(output_port)?,
                                packet(&ctx, output_port, payload.as_bytes(), index)?,
                                &cancellation,
                            )
                            .await?;
                    }
                }
                Self::JoinWindow { diagnostics } => {
                    let mut state: JoinState = JoinState::default();
                    let mut joined_index: usize = 0;
                    while let Some((input_port, received_packet)) =
                        inputs.recv_any(&cancellation).await?
                    {
                        let payload: String = packet_payload_string(received_packet)?;
                        diagnostics
                            .lock()
                            .expect("join diagnostics lock should not be poisoned")
                            .recv_any_order
                            .push(format!("{input_port}:{payload}"));
                        if let Some(joined) = state.accept(&input_port, &payload)? {
                            outputs
                                .send(
                                    &port_id("joined")?,
                                    packet(&ctx, "joined", joined.as_bytes(), joined_index)?,
                                    &cancellation,
                                )
                                .await?;
                            joined_index += 1;
                        }
                    }

                    let (unmatched_events, unmatched_profiles) = state.unmatched_payloads();
                    let mut diagnostics = diagnostics
                        .lock()
                        .expect("join diagnostics lock should not be poisoned");
                    diagnostics.recv_any_order.push(String::from("closed"));
                    diagnostics.unmatched_events = unmatched_events;
                    diagnostics.unmatched_profiles = unmatched_profiles;
                }
                Self::Sink { received } => {
                    let packets: Vec<PortPacket> =
                        drain_port(&mut inputs, &port_id("joined")?, &cancellation).await?;
                    let rows: Vec<String> = packets
                        .into_iter()
                        .map(packet_payload_string)
                        .collect::<pureflow_core::Result<Vec<String>>>()?;
                    received
                        .lock()
                        .expect("sink rows lock should not be poisoned")
                        .extend(rows);
                }
            }

            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WindowKey {
    window: String,
    account: String,
}

impl WindowKey {
    fn new(window: &str, account: &str) -> Self {
        Self {
            window: window.to_owned(),
            account: account.to_owned(),
        }
    }
}

#[derive(Debug, Default)]
struct JoinState {
    events: BTreeMap<WindowKey, String>,
    profiles: BTreeMap<WindowKey, String>,
}

impl JoinState {
    fn accept(&mut self, port_id: &PortId, payload: &str) -> pureflow_core::Result<Option<String>> {
        match port_id.as_str() {
            "events" => self.accept_event(payload),
            "profiles" => self.accept_profile(payload),
            other => Err(PureflowError::execution(format!(
                "join-window received packet on unexpected input `{other}`"
            ))),
        }
    }

    fn accept_event(&mut self, payload: &str) -> pureflow_core::Result<Option<String>> {
        let event: EventRecord = parse_event(payload)?;
        let key: WindowKey = WindowKey::new(&event.window, &event.account);
        if let Some(tier) = self.profiles.remove(&key) {
            return Ok(Some(joined_payload(
                &event.window,
                &event.account,
                &event.action,
                &tier,
            )));
        }
        self.events.insert(key, event.action);
        Ok(None)
    }

    fn accept_profile(&mut self, payload: &str) -> pureflow_core::Result<Option<String>> {
        let profile: ProfileRecord = parse_profile(payload)?;
        let key: WindowKey = WindowKey::new(&profile.window, &profile.account);
        if let Some(action) = self.events.remove(&key) {
            return Ok(Some(joined_payload(
                &profile.window,
                &profile.account,
                &action,
                &profile.tier,
            )));
        }
        self.profiles.insert(key, profile.tier);
        Ok(None)
    }

    fn unmatched_payloads(self) -> (Vec<String>, Vec<String>) {
        let events: Vec<String> = self
            .events
            .into_iter()
            .map(|(key, action)| format!("event:{}:{}:{action}", key.window, key.account))
            .collect();
        let profiles: Vec<String> = self
            .profiles
            .into_iter()
            .map(|(key, tier)| format!("profile:{}:{}:{tier}", key.window, key.account))
            .collect();
        (events, profiles)
    }
}

#[derive(Debug)]
struct EventRecord {
    window: String,
    account: String,
    action: String,
}

#[derive(Debug)]
struct ProfileRecord {
    window: String,
    account: String,
    tier: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("stream-join-window-example")?);
    let diagnostics: Arc<Mutex<JoinDiagnostics>> = Arc::new(Mutex::new(JoinDiagnostics::default()));
    let collected: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let registry: StaticNodeExecutorRegistry<StreamJoinExecutor> =
        StaticNodeExecutorRegistry::new(BTreeMap::from([
            (
                node_id("event-source")?,
                StreamJoinExecutor::Source {
                    output_port: "events",
                    packets: EVENT_SOURCE_PACKETS,
                },
            ),
            (
                node_id("profile-source")?,
                StreamJoinExecutor::Source {
                    output_port: "profiles",
                    packets: PROFILE_SOURCE_PACKETS,
                },
            ),
            (
                node_id("join-window")?,
                StreamJoinExecutor::JoinWindow {
                    diagnostics: diagnostics.clone(),
                },
            ),
            (
                node_id("sink")?,
                StreamJoinExecutor::Sink {
                    received: collected.clone(),
                },
            ),
        ]));
    let metadata_sink: Arc<JsonlMetadataSink<Vec<u8>>> =
        Arc::new(JsonlMetadataSink::new(Vec::new()));

    let summary = block_on(run_workflow_with_registry_and_metadata_sink_summary(
        &workflow,
        &execution,
        &registry,
        metadata_sink.clone(),
    ))?;
    metadata_sink.flush()?;

    let collected_rows: Vec<String> = collected
        .lock()
        .expect("sink rows lock should not be poisoned")
        .clone();
    let diagnostics: JoinDiagnostics = diagnostics
        .lock()
        .expect("join diagnostics lock should not be poisoned")
        .clone();
    let metadata_jsonl: String = metadata_jsonl_from_sink(metadata_sink)?;
    let metadata_counts: MetadataCounts = count_metadata_records(&metadata_jsonl);

    assert_expected_output(&collected_rows, &diagnostics, metadata_counts)?;

    println!("stream join/window workflow `{}` completed", workflow.id());
    println!("event packets: {}", EVENT_SOURCE_PACKETS.len());
    println!("profile packets: {}", PROFILE_SOURCE_PACKETS.len());
    println!("joined rows: {}", collected_rows.len());
    println!("joined payloads: {}", collected_rows.join(", "));
    println!("recv_any order: {}", diagnostics.recv_any_order.join(", "));
    println!(
        "unmatched events: {}",
        diagnostics.unmatched_events.join(", ")
    );
    println!(
        "unmatched profiles: {}",
        diagnostics.unmatched_profiles.join(", ")
    );
    println!("scheduled nodes: {}", summary.scheduled_node_count());
    println!("completed nodes: {}", summary.completed_node_count());
    println!("metadata records: {}", metadata_counts.total);
    println!("metadata lifecycle records: {}", metadata_counts.lifecycle);
    println!("metadata message records: {}", metadata_counts.message);
    println!(
        "metadata queue_pressure records: {}",
        metadata_counts.queue_pressure
    );

    summary.into_result()?;
    Ok(())
}

fn workflow() -> WorkflowDefinition {
    WorkflowBuilder::new("stream-join-window-workload")
        .node(NodeBuilder::new("event-source").output("events").build())
        .node(
            NodeBuilder::new("profile-source")
                .output("profiles")
                .build(),
        )
        .node(
            NodeBuilder::new("join-window")
                .input("events")
                .input("profiles")
                .output("joined")
                .build(),
        )
        .node(NodeBuilder::new("sink").input("joined").build())
        .edge_with_capacity(
            "event-source",
            "events",
            "join-window",
            "events",
            capacity(1),
        )
        .edge_with_capacity(
            "profile-source",
            "profiles",
            "join-window",
            "profiles",
            capacity(1),
        )
        .edge_with_capacity("join-window", "joined", "sink", "joined", capacity(1))
        .build()
}

fn assert_expected_output(
    collected_rows: &[String],
    diagnostics: &JoinDiagnostics,
    metadata_counts: MetadataCounts,
) -> pureflow_core::Result<()> {
    let expected_rows: Vec<String> = vec![
        String::from("joined:w1:alpha:click:gold"),
        String::from("joined:w1:beta:open:silver"),
        String::from("joined:w2:alpha:checkout:platinum"),
    ];
    if collected_rows != expected_rows {
        return Err(PureflowError::execution(format!(
            "joined rows did not match expected stream join output: got {collected_rows:?}"
        )));
    }

    if diagnostics
        .recv_any_order
        .last()
        .is_none_or(|last| last != "closed")
    {
        return Err(PureflowError::execution(
            "join-window did not observe recv_any closure",
        ));
    }

    let expected_unmatched_events: Vec<String> = vec![
        String::from("event:w1:gamma:orphan-event"),
        String::from("event:w4:epsilon:late-orphan"),
    ];
    if diagnostics.unmatched_events != expected_unmatched_events {
        return Err(PureflowError::execution(format!(
            "unmatched events did not match expected window remainder: {:?}",
            diagnostics.unmatched_events
        )));
    }

    if diagnostics.unmatched_profiles != [String::from("profile:w3:delta:orphan-profile")] {
        return Err(PureflowError::execution(format!(
            "unmatched profiles did not match expected window remainder: {:?}",
            diagnostics.unmatched_profiles
        )));
    }

    if metadata_counts.lifecycle == 0
        || metadata_counts.message == 0
        || metadata_counts.queue_pressure == 0
    {
        return Err(PureflowError::metadata(format!(
            "metadata shape was incomplete: {metadata_counts:?}"
        )));
    }

    Ok(())
}

fn parse_event(payload: &str) -> pureflow_core::Result<EventRecord> {
    let [kind, window, account, action]: [&str; 4] = payload_fields(payload)?;
    if kind != "event" {
        return Err(PureflowError::execution(format!(
            "expected event payload, got `{payload}`"
        )));
    }
    Ok(EventRecord {
        window: window.to_owned(),
        account: account.to_owned(),
        action: action.to_owned(),
    })
}

fn parse_profile(payload: &str) -> pureflow_core::Result<ProfileRecord> {
    let [kind, window, account, tier]: [&str; 4] = payload_fields(payload)?;
    if kind != "profile" {
        return Err(PureflowError::execution(format!(
            "expected profile payload, got `{payload}`"
        )));
    }
    Ok(ProfileRecord {
        window: window.to_owned(),
        account: account.to_owned(),
        tier: tier.to_owned(),
    })
}

fn payload_fields(payload: &str) -> pureflow_core::Result<[&str; 4]> {
    let fields: Vec<&str> = payload.split(':').collect();
    fields.try_into().map_err(|fields: Vec<&str>| {
        PureflowError::execution(format!(
            "payload `{payload}` had {} fields; expected 4",
            fields.len()
        ))
    })
}

fn joined_payload(window: &str, account: &str, action: &str, tier: &str) -> String {
    format!("joined:{window}:{account}:{action}:{tier}")
}

fn packet(
    ctx: &NodeContext,
    output_port: &str,
    payload: &[u8],
    index: usize,
) -> pureflow_core::Result<PortPacket> {
    let source: MessageEndpoint =
        MessageEndpoint::new(ctx.node_id().clone(), port_id(output_port)?);
    let target: MessageEndpoint =
        MessageEndpoint::new(ctx.node_id().clone(), port_id(output_port)?);
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let message_id: MessageId = MessageId::new(format!("{}-{output_port}-{index}", ctx.node_id()))?;
    let metadata: MessageMetadata = MessageMetadata::new(
        message_id,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );

    Ok(PortPacket::new(
        metadata,
        PacketPayload::from(payload.to_vec()),
    ))
}

fn packet_payload_string(packet: PortPacket) -> pureflow_core::Result<String> {
    let bytes: Vec<u8> = packet
        .into_payload()
        .as_bytes()
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| PureflowError::execution("stream join workload expected byte payload"))?;
    String::from_utf8(bytes)
        .map_err(|source| PureflowError::execution(format!("payload was not UTF-8: {source}")))
}

fn metadata_jsonl_from_sink(
    metadata_sink: Arc<JsonlMetadataSink<Vec<u8>>>,
) -> pureflow_core::Result<String> {
    let sink: JsonlMetadataSink<Vec<u8>> = match Arc::try_unwrap(metadata_sink) {
        Ok(sink) => sink,
        Err(_arc) => {
            return Err(PureflowError::metadata(
                "metadata sink still had multiple references after run",
            ));
        }
    };
    let bytes: Vec<u8> = sink.into_inner()?;
    String::from_utf8(bytes)
        .map_err(|source| PureflowError::metadata(format!("metadata JSONL was not UTF-8: {source}")))
}

#[derive(Debug, Clone, Copy)]
struct MetadataCounts {
    total: usize,
    lifecycle: usize,
    message: usize,
    queue_pressure: usize,
}

fn count_metadata_records(metadata_jsonl: &str) -> MetadataCounts {
    MetadataCounts {
        total: metadata_jsonl.lines().count(),
        lifecycle: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"lifecycle\""))
            .count(),
        message: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"message\""))
            .count(),
        queue_pressure: metadata_jsonl
            .lines()
            .filter(|line| line.contains("\"record_type\":\"queue_pressure\""))
            .count(),
    }
}

const fn capacity(value: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(value).expect("example capacity must be non-zero")
}

fn node_id(value: &str) -> Result<NodeId, pureflow_types::IdentifierError> {
    NodeId::new(value)
}

fn port_id(value: &str) -> Result<PortId, pureflow_types::IdentifierError> {
    PortId::new(value)
}
