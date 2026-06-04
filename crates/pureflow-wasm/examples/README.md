# pureflow-wasm examples

## `mixed_pipeline`

`mixed_pipeline` proves the host-owned MVP shape:

- native source node sends a byte packet through normal `PortsOut`
- batch-backed middle node invokes a real `wasm32-wasip2` guest through
  `WasmtimeBatchComponent` and `BatchNodeExecutor`
- native sink node receives the transformed packet through normal `PortsIn`

The example builds the existing uppercase guest fixture into a temporary
`wasm32-wasip2` component before running the graph. Run it from the repo dev
shell so the target and Wasmtime tools are available.

Run it with:

```sh
nix develop . --command cargo run -p pureflow-wasm --example mixed_pipeline
```
