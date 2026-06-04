# Wasmtime Adapter ABI Questions

Question recorded while assessing `cdt-mcu.2`.

Current constraint:

- `BatchExecutor`, `BatchInputs`, and `BatchOutputs` are runtime-neutral in `pureflow-core`.
- `pureflow-wasm` should hide Wasmtime churn behind that batch boundary.
- The proposals say the host calls a WASM component with message metadata and payload bytes, then validates outputs before sending through `PortsOut`.

Open decision:

- What guest ABI should `pureflow-wasm` implement for the first Wasmtime adapter?

Options:

- Core Wasm export convention: guest exports memory plus a `pureflow_invoke(input_ptr, input_len) -> output_handle` style function, with explicit allocation/free exports.
- Component Model/WIT convention: define a WIT package for batch inputs and outputs, then generate bindings with `wit-bindgen`.
- Host-function pull/push convention: guest calls imported host functions to read input packets and emit output packets while still not seeing channels directly.

Current leaning:

- Prefer Component Model/WIT if the goal is a stable product boundary.
- Prefer a narrow core-Wasm export convention if the goal is the smallest executable spike.
- Do not implement output port validation inside the Wasmtime adapter; keep that at the host engine boundary per the WASM batch trait response.

Why this is being asked:

- choosing the ABI now determines how metadata, bytes/control payloads, allocation, error crossing, and cancellation are represented
- a guessed ABI would be hard to unwind once sample guest code and tests exist
