//! Criterion benchmarks for metadata sink overhead.

use std::{hint::black_box, io, num::NonZeroUsize};

use pureflow_core::{
    JsonlMetadataSink, MessageBoundaryKind, MessageBoundaryRecord, MetadataRecord, MetadataSink,
    MetadataTier, NoopMetadataSink, TieredMetadataPolicy, TieredMetadataSink,
    context::{ExecutionMetadata, NodeContext},
    lifecycle::{LifecycleEvent, LifecycleEventKind},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use pureflow_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

fn workflow_id(value: &str) -> WorkflowId {
    WorkflowId::new(value).expect("benchmark workflow id should be valid")
}

fn execution_id(value: &str) -> ExecutionId {
    ExecutionId::new(value).expect("benchmark execution id should be valid")
}

fn message_id(value: &str) -> MessageId {
    MessageId::new(value).expect("benchmark message id should be valid")
}

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).expect("benchmark node id should be valid")
}

fn port_id(value: &str) -> PortId {
    PortId::new(value).expect("benchmark port id should be valid")
}

fn execution() -> ExecutionMetadata {
    ExecutionMetadata::first_attempt(execution_id("bench-run"))
}

fn context() -> NodeContext {
    NodeContext::new(
        workflow_id("bench-flow"),
        node_id("bench-node"),
        execution(),
    )
}

fn lifecycle_record() -> MetadataRecord {
    MetadataRecord::Lifecycle(LifecycleEvent::new(
        LifecycleEventKind::NodeCompleted,
        context(),
    ))
}

fn message_record() -> MetadataRecord {
    let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
    let target: MessageEndpoint = MessageEndpoint::new(node_id("sink"), port_id("in"));
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let metadata: MessageMetadata = MessageMetadata::new(
        message_id("bench-message"),
        workflow_id("bench-flow"),
        execution(),
        route,
    );

    MetadataRecord::Message(MessageBoundaryRecord::new(
        MessageBoundaryKind::Dequeued,
        metadata,
    ))
}

fn record_or_panic(sink: &impl MetadataSink, record: &MetadataRecord) {
    let result: pureflow_core::Result<()> = sink.record(black_box(record));
    assert!(result.is_ok());
}

fn record_with_tier_or_panic(
    sink: &TieredMetadataSink<NoopMetadataSink>,
    tier: MetadataTier,
    record: &MetadataRecord,
) {
    let result: pureflow_core::Result<()> = sink.record_with_tier(tier, black_box(record));
    assert!(result.is_ok());
}

fn metadata_sink_overhead(c: &mut Criterion) {
    let control_record: MetadataRecord = lifecycle_record();
    let data_record: MetadataRecord = message_record();
    let mut group = c.benchmark_group("metadata_sink_record");
    group.throughput(Throughput::Elements(1));

    group.bench_function("noop_control", |b| {
        let sink: NoopMetadataSink = NoopMetadataSink;
        b.iter(|| record_or_panic(&sink, &control_record));
    });

    group.bench_function("jsonl_default_control", |b| {
        let sink: JsonlMetadataSink<io::Sink> = JsonlMetadataSink::new(io::sink());
        b.iter(|| record_or_panic(&sink, &control_record));
    });

    group.bench_function("tiered_noop_control", |b| {
        let sink: TieredMetadataSink<NoopMetadataSink> = TieredMetadataSink::new(NoopMetadataSink);
        b.iter(|| record_or_panic(&sink, &control_record));
    });

    group.bench_function("tiered_jsonl_control", |b| {
        let inner: JsonlMetadataSink<io::Sink> = JsonlMetadataSink::new(io::sink());
        let sink: TieredMetadataSink<JsonlMetadataSink<io::Sink>> = TieredMetadataSink::new(inner);
        b.iter(|| record_or_panic(&sink, &control_record));
    });

    group.bench_function("tiered_noop_data_drop", |b| {
        let sink: TieredMetadataSink<NoopMetadataSink> = TieredMetadataSink::new(NoopMetadataSink);
        b.iter(|| record_with_tier_or_panic(&sink, MetadataTier::Data, &data_record));
    });

    group.bench_function("tiered_noop_data_sample_8", |b| {
        let policy: TieredMetadataPolicy = TieredMetadataPolicy::sample_data_every(
            NonZeroUsize::new(8).expect("sample rate should be nonzero"),
        );
        let sink: TieredMetadataSink<NoopMetadataSink> =
            TieredMetadataSink::with_policy(NoopMetadataSink, policy);
        b.iter(|| record_with_tier_or_panic(&sink, MetadataTier::Data, &data_record));
    });

    group.finish();
}

criterion_group!(benches, metadata_sink_overhead);
criterion_main!(benches);
