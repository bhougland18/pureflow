# Data-Tier Pressure Review — 2026-05-08

This note reviews the five workload examples completed under Epic 17
(cdt-jrf.1 through cdt-jrf.5) and decides whether to undefer cdt-pyg
(Arrow/DataFusion).

## Workloads Reviewed

| Bead      | Workload                   | Packet format             |
|-----------|----------------------------|---------------------------|
| cdt-jrf.1 | fan-out/fan-in             | UTF-8 row strings         |
| cdt-jrf.2 | stream join/window         | colon-delimited text keys |
| cdt-jrf.3 | watcher cancellation       | file path strings         |
| cdt-jrf.4 | replay/branch evaluation   | tag/reverse string pairs  |
| cdt-jrf.5 | AI-call orchestration mock | prompt/tool/result text   |

## Observations

**Packet content is uniformly text-over-bytes.** Every workload passes UTF-8
strings as raw `Vec<u8>` payloads. None required a typed schema, a columnar
record batch, or an in-process query engine.

**The join workload (cdt-jrf.2) used text key matching, not SQL.** The
stream-join-window join state is a `BTreeMap<WindowKey, String>` over string
fields parsed from colon-delimited payloads. A DataFusion-backed equi-join
would add significant dependency weight for a pattern that works cleanly at
the native node level.

**No workload produced batch data sizes that justify columnar encoding.**
All workloads processed between 3 and 9 packets. Even if workload scale grew
to thousands of packets, the current `PacketPayload::Bytes` representation
would not be the bottleneck; queue pressure and scheduling overhead would
dominate first.

**The most concrete unmet need is external-effect capability enforcement.**
The AI orchestration workload (cdt-jrf.5) identified that tool-executor nodes
need a declared `external-effect` capability and a corresponding metadata
record family. This is an enforcement and observability gap, not a data
representation gap.

**Fan-out fan-in, multi-input recv_any, and cancellation are the live
runtime surfaces.** Five workloads now cover these patterns. All of them are
stable and produce consistent metadata counts. There is no new signal from
these workloads pointing toward Arrow schemas or DataFusion query execution.

## Decision: Keep Arrow/DataFusion Deferred

The workload pressure review does not surface any concrete need to undefer
cdt-pyg. The byte-message runtime and WASM boundary are the right current
focus.

**Keep deferred:**
- `cdt-pyg.1` (arrow-schema-compatibility-plan)
- `cdt-pyg.2` (datafusion-node-crate-spike)
- `cdt-pyg.3` (arrow-copy-latency-benchmarks)
- `cdt-pyg` epic

The trigger for revisiting these would be: a concrete workload that requires
typed schema validation at the packet level, a query-over-stream use case that
cannot be implemented as a native node, or a benchmark that shows `Bytes`
encoding as the bottleneck in a realistic load scenario. None of those
conditions exist today.

## Follow-On Candidates

Two gaps surfaced by these workloads are worth tracking as future beads if
concrete workloads justify them:

1. **External-effect capability tag and metadata**: documented in
   `examples/workloads/ai-call-orchestration.md`. The gap is an enforcement
   and observability hole, not a data tier question. It belongs in a future
   capability-enforcement epic, not cdt-pyg.

2. **Multi-turn feedback-loop orchestration**: the AI orchestration workload
   is acyclic (one LLM turn). A multi-turn agent would require
   `WorkflowRunPolicy::feedback_loops` plus a `should_continue` exit signal.
   This is a runtime policy and node contract question, not a data tier
   question.

Neither of these is urgent. Both are documented in their respective workload
markdown files for future reference.
