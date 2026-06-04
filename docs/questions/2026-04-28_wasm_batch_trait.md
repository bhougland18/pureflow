# WASM Batch Trait Questions

Question recorded while implementing `cdt-mcu.1`.

Current implementation choice:

- `BatchInputs` and `BatchOutputs` are host-owned maps from `PortId` to ordered `Vec<PortPacket>`.
- `BatchExecutor::invoke` is synchronous and runtime-neutral.
- `WasmModule` is an opaque wrapper around `Box<dyn BatchExecutor>`.
- The trait does not choose Wasmtime, WASI, async execution, batch sizing, or output validation yet.

Why this choice:

- it matches the proposal's host-owned channel model
- it keeps direct port/channel access out of WASM guests
- it lets the later Wasmtime adapter own engine-specific invocation, limits, and cancellation behavior

Open follow-ups:

- should empty input batches be accepted by all adapters, or should adapters declare a minimum input policy?
- should the later host adapter validate output ports before returning `BatchOutputs`, or should the engine validate before sending into `PortsOut`?
- should `BatchExecutor` stay synchronous for the Wasmtime MVP, or should a later adapter expose async cancellation around a blocking guest call?
