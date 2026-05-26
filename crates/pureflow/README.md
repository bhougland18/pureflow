# Pureflow

A flow-based programming (FBP) workflow engine for building concurrent data pipelines in Rust.

## Overview

Pureflow models computation as a directed graph of nodes connected by typed message-passing edges. Nodes run concurrently, communicate through bounded channels, and are composed into workflows that the engine orchestrates end-to-end.

## Quick start

```toml
[dependencies]
pureflow = "0.1"
```

Implement the `NodeExecutor` trait for each node in your graph:

```rust
use pureflow::{NodeExecutor, PortsIn, PortsOut, Result};
use pureflow::core::context::NodeContext;

struct MyNode;

impl NodeExecutor for MyNode {
    type RunFuture<'a> = impl std::future::Future<Output = Result<()>> + Send + 'a
    where Self: 'a;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        async move {
            // receive, transform, send
            Ok(())
        }
    }
}
```

Define a workflow, register your nodes, and run:

```rust
use pureflow::{StaticNodeExecutorRegistry, WorkflowRunPolicy, run_workflow_with_registry_summary};

let registry = StaticNodeExecutorRegistry::new(/* ... */);
let summary = run_workflow_with_registry_summary(&workflow, &registry).await?;
```

## Feature flags

| Feature | Description |
|---------|-------------|
| `wasm` | Wasmtime-backed WebAssembly batch node execution |
| `introspection` | Read-only workflow introspection with JSON serialization |
| `tracing` | `tracing` crate integration for runtime observability |
| `arrow` | Apache Arrow columnar data support in message payloads |
| `toml` | TOML workflow definition parser |
| `yaml` | YAML workflow definition parser |
| `full` | All of the above |

Enable features in your `Cargo.toml`:

```toml
pureflow = { version = "0.1", features = ["toml", "tracing"] }
```

## Crate structure

The `pureflow` crate is a facade over a set of focused internal crates. Each is
also available individually if you only need part of the stack:

| Crate | Role |
|-------|------|
| `pureflow-types` | Opaque identifier types |
| `pureflow-workflow` | Workflow graph structure and validation |
| `pureflow-core` | `NodeExecutor` trait, ports, messages, errors |
| `pureflow-contract` | Node contract metadata and port schemas |
| `pureflow-engine` | Orchestration, run policies, registries |
| `pureflow-runtime` | Low-level async node runner |
| `pureflow-workflow-format` | JSON / TOML / YAML workflow parsers |
| `pureflow-introspection` | Read-only workflow introspection |
| `pureflow-wasm` | Wasmtime batch node adapter |
| `pureflow-test-kit` | Test builders and property strategies |

## License

MIT
