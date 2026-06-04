# Replay/Branch Evaluation Workload

This workload exercises parallel branch evaluation from a shared input. A single
source fans out to two independent processing branches; an evaluator drains each
branch's output port separately and compares per-branch results to verify
deterministic packet counts and payload correspondence.

Run commands from the repository root.

## Topology

Workflow file:

- `examples/workloads/replay-branch-eval.workflow.json`

Runnable native executor example:

- `crates/pureflow-engine/examples/replay_branch_eval.rs`

Shape:

```text
source.out -> branch-a.in
source.out -> branch-b.in
branch-a.out -> evaluator.a
branch-b.out -> evaluator.b
```

`source` fans its single output port to two downstream edges, exercising
fan-out reservation. `evaluator` holds two distinct input ports (`a` and `b`)
that are drained sequentially — this is not fan-in but independent per-branch
draining, making metadata attributable to each branch separately.

## Validate And Inspect

```bash
cargo run -p pureflow-cli -- validate examples/workloads/replay-branch-eval.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/replay-branch-eval.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/replay-branch-eval.workflow.json
```

Expected `validate` output:

```text
valid workflow `replay-branch-eval-workload`
nodes: 4
edges: 4
```

Expected `explain` output:

```text
workflow `replay-branch-eval-workload`
status: valid
nodes: 4
edges: 4
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
node order:
  - source inputs=0 outputs=1
  - branch-a inputs=1 outputs=1
  - branch-b inputs=1 outputs=1
  - evaluator inputs=2 outputs=0
edges:
  - source.out -> branch-a.in capacity=4
  - source.out -> branch-b.in capacity=4
  - branch-a.out -> evaluator.a capacity=4
  - branch-b.out -> evaluator.b capacity=4
```

## Run

```bash
cargo run -p pureflow-engine --example replay_branch_eval
```

Expected output:

```text
replay/branch eval workflow `replay-branch-eval-workload` completed
source inputs: 3
branch-a outputs: 3
branch-b outputs: 3
  row[0]: tag:alpha | rev:ahpla
  row[1]: tag:beta | rev:ateb
  row[2]: tag:gamma | rev:ammag
scheduled nodes: 4
completed nodes: 4
metadata records: 88
metadata lifecycle records: 8
metadata message records: 21
metadata queue_pressure records: 59
```

`branch-a` applies a `tag:` prefix transform; `branch-b` reverses each payload
and applies a `rev:` prefix. The evaluator confirms both branches received and
processed the same number of packets (3) and shows them side by side. If the
counts diverge or the payloads deviate from the deterministic transforms, the
example exits with an error.

The exact metadata counts are a regression signal for the current runtime. If
metadata emission changes intentionally, update this page and the example output
together.

## Branch Comparison

The evaluator drains port `a` (branch-a results) and port `b` (branch-b results)
as independent sequential drains. This differs from fan-in (where both upstream
sources merge into one port) and from `recv_any` (where arrival order is
non-deterministic). Because each branch is drained independently, per-branch
packet counts and payloads are fully attributable and comparable.

This models a replay evaluation pattern: the same source data is replayed
through two independent processing variants, and the evaluator compares their
outputs without combining them.

## Metadata Shape

The example captures three metadata families:

- `lifecycle`: four started and four completed node records (source, branch-a,
  branch-b, evaluator)
- `message`: enqueue/dequeue observations for each packet crossing each edge —
  source emits 3 packets that fan out to 6 edge deliveries total, plus 6
  more deliveries from branches to evaluator
- `queue_pressure`: capacity and closure observations on all four edges

The important workload pressure is the fan-out reservation on `source.out`:
one send operation reserves capacity on two downstream edges simultaneously.
The `queue_pressure` records for both `source.out -> branch-a.in` and
`source.out -> branch-b.in` reflect this backpressure interplay.
