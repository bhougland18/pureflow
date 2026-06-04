# Benchmark Operations

Pureflow currently has two Criterion benchmark suites:

- `pureflow-core` metadata sink overhead
- `pureflow-engine` workflow backpressure capacity

Run commands from the repository root. Use the Nix devshell when you want the
same toolchain used by the rest of project validation:

```bash
nix develop . --command cargo bench -p pureflow-core --bench metadata_overhead
nix develop . --command cargo bench -p pureflow-engine --bench backpressure_capacity
```

For CI-style compile checks without executing measurements:

```bash
cargo bench -p pureflow-core --bench metadata_overhead --no-run
cargo bench -p pureflow-engine --bench backpressure_capacity --no-run
```

Criterion writes reports and historical samples under `target/criterion/`.
Those files are generated output and should not be committed.

The first recorded local measurement baseline is
[benchmark-baseline-2026-05-08.md](benchmark-baseline-2026-05-08.md). Treat it
as a same-machine comparison anchor, not a portable performance guarantee.

## Metadata Overhead

Source:

- `crates/pureflow-core/benches/metadata_overhead.rs`

Run:

```bash
cargo bench -p pureflow-core --bench metadata_overhead
```

Compile only:

```bash
cargo bench -p pureflow-core --bench metadata_overhead --no-run
```

Criterion group:

- `metadata_sink_record`

Benchmarks:

- `noop_control`
- `jsonl_default_control`
- `tiered_noop_control`
- `tiered_jsonl_control`
- `tiered_noop_data_drop`
- `tiered_noop_data_sample_8`

What it measures:

- baseline cost of recording one control metadata record into `NoopMetadataSink`
- JSONL serialization overhead for one lifecycle control record into `io::Sink`
- wrapper cost of `TieredMetadataSink` for control records
- combined tiered-policy plus JSONL serialization cost
- data-tier drop cost when the default control-only policy rejects data records
- deterministic data-tier sampling overhead with sample rate 8

Use this benchmark when changing:

- `MetadataRecord`
- `JsonlMetadataSink`
- `TieredMetadataPolicy`
- `TieredMetadataSink`
- lifecycle or message record JSON projection

Interpretation:

- `noop_control` is the lower-bound control path.
- `jsonl_default_control` shows serialization cost without tier policy.
- `tiered_jsonl_control` is closest to the CLI default metadata path.
- `tiered_noop_data_drop` should stay cheap; it protects the default policy from
  paying serialization cost for data-tier records it drops.
- `tiered_noop_data_sample_8` is useful for checking the branch/counter overhead
  of sampled data-tier observations.

## Backpressure Capacity

Source:

- `crates/pureflow-engine/benches/backpressure_capacity.rs`

Run:

```bash
cargo bench -p pureflow-engine --bench backpressure_capacity
```

Compile only:

```bash
cargo bench -p pureflow-engine --bench backpressure_capacity --no-run
```

Criterion group:

- `engine_backpressure_capacity`

Benchmarks:

- `linear_capacity_1`
- `linear_default_capacity`
- `fan_out_default_capacity`
- `fan_in_default_capacity`

What it measures:

- full `run_workflow` execution with a bounded source -> sink edge at explicit
  capacity 1
- full `run_workflow` execution with the default edge capacity
- fan-out delivery from one source output to two sink inputs
- fan-in delivery from two source outputs to one collector input

Each benchmark sends 32 byte messages per source. Fan-out and fan-in report
throughput as 64 delivered messages because two downstream paths are exercised.

Use this benchmark when changing:

- `run_workflow`
- `PortsIn` or `PortsOut`
- bounded edge channel construction
- output fan-out behavior
- multi-input receive behavior
- cancellation or task supervision that may affect scheduling overhead

Interpretation:

- `linear_capacity_1` is the highest-pressure linear case. Regressions here may
  indicate additional reserve/send/receive overhead.
- `linear_default_capacity` is the default linear baseline.
- `fan_out_default_capacity` is sensitive to cloning and sending one output
  packet to multiple downstream edges.
- `fan_in_default_capacity` is sensitive to multi-source scheduling and input
  draining.

## Comparing Runs

For local comparisons:

1. Run the benchmark once on the baseline checkout.
2. Make the code change.
3. Run the same benchmark command again.
4. Read Criterion's terminal `change` summary and the reports under
   `target/criterion/`.

Keep comparisons narrow:

- Compare the same benchmark group and benchmark id.
- Use the same machine, power profile, and shell environment.
- Avoid comparing a full measurement run against a `--no-run` compile check.
- Compare against [benchmark-baseline-2026-05-08.md](benchmark-baseline-2026-05-08.md)
  only when the machine and toolchain are comparable; otherwise capture a fresh
  local baseline first.
- Treat small changes near Criterion noise as directional until repeated.

For metadata changes, compare:

- `jsonl_default_control` against `tiered_jsonl_control` to separate JSONL cost
  from tier-policy overhead
- `tiered_noop_data_drop` against `tiered_noop_data_sample_8` to isolate
  sampling overhead

For backpressure changes, compare:

- `linear_capacity_1` against `linear_default_capacity` to isolate pressure from
  default capacity behavior
- `fan_out_default_capacity` against `fan_in_default_capacity` to separate
  cloning/fan-out cost from multi-source fan-in scheduling

## Reporting Results

When summarizing benchmark results in a PR or handoff, include:

- command that was run
- benchmark group and id
- baseline commit or JJ change
- changed commit or JJ change
- Criterion-reported percentage change and confidence interval
- whether the run was measurement or `--no-run`

Example summary:

```text
cargo bench -p pureflow-core --bench metadata_overhead
metadata_sink_record/tiered_jsonl_control:
  Criterion reported +2.1% mean time, confidence interval crossed zero.
  Treat as no clear regression without repeated confirmation.
```
