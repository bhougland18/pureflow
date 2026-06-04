# Benchmark Baseline 2026-05-08

Initial local Criterion measurement baseline for metadata overhead and engine
backpressure capacity.

Environment:

- Date: 2026-05-08
- Checkout: `e03d186`
- Plot backend: Criterion used plotters because `gnuplot` was not installed
- Commands:
  - `cargo bench -p pureflow-core --bench metadata_overhead`
  - `cargo bench -p pureflow-engine --bench backpressure_capacity`

Generated Criterion reports remain under `target/criterion/` and are not source
artifacts.

## Metadata Overhead

Group: `metadata_sink_record`

| Benchmark | Mean time | Throughput |
| --- | ---: | ---: |
| `noop_control` | 198.41 ps | 5.0400 Gelem/s |
| `jsonl_default_control` | 2.2062 us | 453.27 Kelem/s |
| `tiered_noop_control` | 427.26 ps | 2.3405 Gelem/s |
| `tiered_jsonl_control` | 2.2158 us | 451.30 Kelem/s |
| `tiered_noop_data_drop` | 6.7862 ns | 147.36 Melem/s |
| `tiered_noop_data_sample_8` | 3.3196 ns | 301.24 Melem/s |

Baseline interpretation:

- JSONL serialization dominates the control metadata path at about 2.2 us per
  record on this machine.
- Tiered control routing adds negligible overhead relative to JSONL
  serialization in `tiered_jsonl_control`.
- Data-tier drop and sample decisions stay in single-digit nanoseconds.

## Backpressure Capacity

Group: `engine_backpressure_capacity`

| Benchmark | Mean time | Throughput |
| --- | ---: | ---: |
| `linear_capacity_1` | 198.06 us | 161.57 Kelem/s |
| `linear_default_capacity` | 118.96 us | 269.01 Kelem/s |
| `fan_out_default_capacity` | 211.86 us | 302.08 Kelem/s |
| `fan_in_default_capacity` | 272.08 us | 235.23 Kelem/s |

Baseline interpretation:

- Explicit capacity 1 remains slower than the default linear capacity, as
  expected for the highest-pressure bounded edge case.
- Fan-out and fan-in exercise 64 delivered messages per run. Fan-in is the
  slowest topology in this baseline because it combines two source tasks with
  collector-side draining.
