//! Filesystem-watching node for the Pureflow workflow engine.
//!
//! Provides [`FsWatcherNode`], a [`NodeExecutor`] that watches a filesystem
//! path and emits one packet per change event on the `changes` output port.
//! The node exits cleanly when it receives any packet on the `control` input
//! port.

use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use asupersync::runtime::yield_now;
use futures::future::BoxFuture;
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use pureflow_core::{
    NodeExecutor, PacketPayload, PortPacket, PortsIn, PortsOut, PureflowError, Result,
    context::NodeContext,
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_types::{MessageId, PortId};

/// Watches a filesystem path and emits change events as packets downstream.
///
/// # Ports
///
/// - **`changes`** (output): one packet per change event; payload is the
///   changed file path as UTF-8 bytes.
/// - **`control`** (input): any received packet triggers a clean shutdown.
///
/// # Debouncing
///
/// Rapid successive events within `debounce_ms` milliseconds of a prior
/// emission are suppressed. This bounds packet rate under write storms without
/// dropping distinct change windows.
#[derive(Debug, Clone)]
pub struct FsWatcherNode {
    /// Filesystem path to watch (non-recursive).
    pub watch_path: PathBuf,
    /// Minimum milliseconds between successive emissions for the same change
    /// window. Defaults to 50 ms.
    pub debounce_ms: u64,
}

impl FsWatcherNode {
    /// Create a watcher with default debounce (50 ms).
    #[must_use]
    pub fn new(watch_path: impl Into<PathBuf>) -> Self {
        Self {
            watch_path: watch_path.into(),
            debounce_ms: 50,
        }
    }
}

impl NodeExecutor for FsWatcherNode {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        let watch_path = self.watch_path.clone();
        let debounce = Duration::from_millis(self.debounce_ms);

        Box::pin(async move {
            let cancellation = ctx.cancellation_token();

            let (tx, rx) = std_mpsc::channel::<Event>();
            let mut watcher = recommended_watcher(move |res: notify::Result<Event>| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            })
            .map_err(|e| PureflowError::execution(format!("fs watcher init failed: {e}")))?;

            watcher
                .watch(&watch_path, RecursiveMode::NonRecursive)
                .map_err(|e| PureflowError::execution(format!("watch path failed: {e}")))?;

            let changes_port = PortId::new("changes").expect("valid port id");
            let control_port = PortId::new("control").expect("valid port id");
            let mut last_emit: Option<Instant> = None;
            let mut seq: u64 = 0;

            loop {
                // Shutdown on cancellation.
                if cancellation.is_cancelled() {
                    return Err(PureflowError::cancelled("fs watcher cancelled"));
                }

                // Shutdown on any control packet.
                match inputs.try_recv(&control_port)? {
                    Some(_) => return Ok(()),
                    None => {}
                }

                // Drain all pending fs events from the std channel.
                let mut got_event = false;
                loop {
                    match rx.try_recv() {
                        Ok(event) => {
                            got_event = true;
                            let now = Instant::now();
                            let within_debounce = last_emit
                                .map(|t| now.duration_since(t) < debounce)
                                .unwrap_or(false);
                            if within_debounce {
                                continue;
                            }
                            last_emit = Some(now);
                            for path in &event.paths {
                                let payload: Vec<u8> =
                                    path.to_string_lossy().into_owned().into_bytes();
                                let packet = make_packet(&ctx, &changes_port, payload, seq)?;
                                seq += 1;
                                outputs.send(&changes_port, packet, &cancellation).await?;
                            }
                        }
                        Err(std_mpsc::TryRecvError::Empty) => break,
                        Err(std_mpsc::TryRecvError::Disconnected) => return Ok(()),
                    }
                }

                if !got_event {
                    yield_now().await;
                }
            }
        })
    }
}

fn make_packet(
    ctx: &NodeContext,
    port: &PortId,
    payload: Vec<u8>,
    seq: u64,
) -> Result<PortPacket> {
    let source = MessageEndpoint::new(ctx.node_id().clone(), port.clone());
    let target = MessageEndpoint::new(ctx.node_id().clone(), port.clone());
    let route = MessageRoute::new(Some(source), target);
    let msg_id = MessageId::new(format!("{}-changes-{seq}", ctx.node_id()))
        .map_err(|e| PureflowError::execution(format!("message id build failed: {e}")))?;
    let metadata = MessageMetadata::new(
        msg_id,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );
    Ok(PortPacket::new(metadata, PacketPayload::from(payload)))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use pureflow_core::{InputPortHandle, OutputPortHandle, PortsIn, PortsOut, bounded_edge_channel};
    use pureflow_runtime::AsupersyncRuntime;
    use pureflow_test_kit::{execution_metadata, node_id, port_id, test_packet, workflow_id};
    use tempfile::TempDir;

    use super::*;

    fn test_ctx() -> NodeContext {
        NodeContext::new(workflow_id("test-flow"), node_id("watcher"), execution_metadata("run-1"))
    }

    fn make_edge(src: &str, dst: &str) -> (OutputPortHandle, InputPortHandle) {
        bounded_edge_channel(
            port_id(src),
            port_id(dst),
            NonZeroUsize::new(32).expect("nonzero"),
        )
    }

    #[test]
    fn emits_packet_when_file_written() {
        let dir = TempDir::new().expect("temp dir");
        let watch_path = dir.path().to_owned();
        let node = FsWatcherNode::new(watch_path.clone());

        let (changes_out, changes_in) = make_edge("changes", "changes");
        let (control_out, control_in) = make_edge("control", "control");

        let outputs = PortsOut::from_handles([port_id("changes")], [changes_out]);
        let inputs = PortsIn::from_handles([port_id("control")], [control_in]);

        let rt = AsupersyncRuntime::new().expect("runtime");
        let ctx = test_ctx();
        let cancellation = ctx.cancellation_token();

        // Run the node in a background thread so we can interact with it.
        let handle = std::thread::spawn(move || rt.run_node(&node, ctx, inputs, outputs));

        // Give the watcher time to initialize.
        std::thread::sleep(Duration::from_millis(100));

        // Write a file to trigger a change event.
        std::fs::write(watch_path.join("test.txt"), b"hello").expect("write");

        // Wait for the event to propagate.
        std::thread::sleep(Duration::from_millis(300));

        // Send a control packet to shut the node down via its control port.
        let control_outputs = PortsOut::from_handles([port_id("control")], [control_out]);
        let rt2 = AsupersyncRuntime::new().expect("runtime2");
        rt2.block_on(async move {
            control_outputs
                .send(&port_id("control"), test_packet(b"stop"), &cancellation)
                .await
                .ok();
        });

        handle.join().expect("thread join").expect("node ok");

        // Verify at least one changes packet was received.
        let mut sink_inputs = PortsIn::from_handles([port_id("changes")], [changes_in]);
        let packet = sink_inputs.try_recv(&port_id("changes")).expect("recv ok");
        assert!(packet.is_some(), "expected at least one changes packet");
        let bytes = packet
            .unwrap()
            .into_payload()
            .as_bytes()
            .expect("bytes payload")
            .to_vec();
        let path_str = String::from_utf8(bytes).expect("utf8");
        assert!(
            path_str.contains("test.txt"),
            "path should contain test.txt, got: {path_str}"
        );
    }

    #[test]
    fn exits_cleanly_on_control_packet() {
        let dir = TempDir::new().expect("temp dir");
        let node = FsWatcherNode::new(dir.path());

        let (changes_out, _changes_in) = make_edge("changes", "changes");
        let (control_out, control_in) = make_edge("control", "control");

        let outputs = PortsOut::from_handles([port_id("changes")], [changes_out]);
        let inputs = PortsIn::from_handles([port_id("control")], [control_in]);

        let rt = AsupersyncRuntime::new().expect("runtime");
        let ctx = test_ctx();
        let cancellation = ctx.cancellation_token();

        let handle = std::thread::spawn(move || rt.run_node(&node, ctx, inputs, outputs));

        std::thread::sleep(Duration::from_millis(50));

        let control_outputs = PortsOut::from_handles([port_id("control")], [control_out]);
        let rt2 = AsupersyncRuntime::new().expect("runtime2");
        rt2.block_on(async move {
            control_outputs
                .send(&port_id("control"), test_packet(b"stop"), &cancellation)
                .await
                .ok();
        });

        let result = handle.join().expect("thread join");
        assert!(result.is_ok(), "node should exit cleanly: {result:?}");
    }
}
