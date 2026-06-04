# Architecture Overview

## System Shape

Pureflow is an experimental Flow-Based Programming engine written in Rust.
It validates workflow documents, executes node graphs through bounded channels,
and emits machine-facing metadata and run summaries.

The architecture is intentionally layered:

- `pureflow-cli` is the human and automation entrypoint.
- `pureflow-engine` validates execution preconditions and orchestrates runs.
- `pureflow-runtime` bridges engine scheduling into the async substrate.
- `pureflow-core` owns runtime-facing types, ports, metadata, errors, and capabilities.
- `pureflow-workflow` owns structural workflow validation.
- `pureflow-contract` owns node contracts and capability/schema alignment.
- `pureflow-wasm` owns Wasmtime and WIT/component ABI details.
- `pureflow-introspection` provides read-only projections for inspect/explain flows.
- `pureflow-test-kit` provides builders and fixtures for tests and examples.

## Core Responsibilities

Stable responsibilities:

- Workflow documents are validated before execution.
- Graph topology and port references are checked separately from runtime policy.
- Contracts and capabilities must agree before nodes run.
- Runtime execution uses bounded channels, explicit cancellation, and metadata emission.
- Run summaries and metadata JSONL are separate machine-facing artifacts.
- Native and WASM execution are both supported, but the public model stays Pureflow-owned.

Important architectural idea:

- Pureflow owns the workflow model, contracts, ports, metadata, and capabilities.
- `asupersync` is the runtime substrate, not the public model.
- WASM is an adapter boundary, not the center of the design.

## External Boundaries

Primary ingress and egress:

- Ingress: workflow documents from CLI or automation.
- Egress: terminal run summary JSON and metadata JSONL.
- Observability: lifecycle, message, queue-pressure, error, and external-effect records.
- Execution adapters: native executors and WASM batch executors.

Boundary facts to remember:

- `pureflow-runtime` may use `asupersync` internally.
- `pureflow-core` should not expose raw `asupersync` or Wasmtime types in its public API.
- WASM guests use a Pureflow-defined WIT world and host-owned channels.
- Capability enforcement is explicit and becomes a real boundary for WASM and future process-backed nodes.
