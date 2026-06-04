# Runtime Lifecycle

## Execution Stages

Runtime execution follows a predictable sequence:

1. Parse the workflow document.
2. Validate workflow structure.
3. Validate node contracts and capabilities.
4. Construct the runtime registry for executors.
5. Schedule execution and move packets through bounded ports.
6. Emit lifecycle, queue-pressure, message, error, and external-effect records.
7. Produce the terminal run summary.

The runtime is designed to keep setup, execution, and terminalization distinct.
That makes failures easier to classify and keeps automation simple.

## State and Flow

Useful state terms:

- Bounded channels: workflow edges with fixed capacity.
- Backpressure: the signal that downstream work cannot keep up.
- Packet: one item moving across a port.
- Port: a named input or output on a node.
- Cancellation: a Pureflow-owned shared signal visible to the runtime and node context.
- Metadata JSONL: the canonical event stream for run forensics.
- Run summary: the compact terminal machine-facing answer for automation.

Ownership model:

- `pureflow-cli` handles input and output.
- `pureflow-engine` owns orchestration and policy decisions.
- `pureflow-runtime` bridges into the async substrate and observer hooks.
- Node execution sees Pureflow-owned context and port semantics rather than raw runtime types.

## Failure Handling

Stable failure behavior:

- Validation errors happen before scheduling starts.
- Runtime errors happen during node execution or transport.
- Terminal summary error is the canonical end-state error surface for `run --json`.
- Error payloads use stable `CDT-*` taxonomy so consumers can branch without string parsing.
- Retry/recovery policy lives with the runtime and contract model, not in CLI-only logic.

Observability and failure are linked:

- Lifecycle records show what the run did.
- Queue-pressure records show where pressure built up.
- Message records show packet movement.
- External-effect records make side effects explicit.
