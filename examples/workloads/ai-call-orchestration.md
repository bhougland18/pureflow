# AI-Call Orchestration Mock Workload

This workload models one prompt → LLM → tool-call → tool-result → final-response
turn as a deterministic linear graph with no real network calls. Packets carry
structured byte messages (`type:field:...` colon-delimited) that stand in for
a real LLM protocol.

The topology is intentionally acyclic (one turn). The capability gap section
below records what a multi-turn execution policy and an external-effect
capability would need to add for a full orchestration runtime.

Run commands from the repository root.

## Topology

Workflow file:

- `examples/workloads/ai-call-orchestration.workflow.json`

Runnable native executor example:

- `crates/pureflow-engine/examples/ai_call_orchestration.rs`

Shape:

```text
prompter.prompt → llm-mock.prompt
llm-mock.tool-call → tool-executor.call
tool-executor.result → finalizer.context
finalizer.response → collector.response
```

Each node models one stage of a single-turn ReAct-style exchange:

- `prompter`: emits the initial user prompt
- `llm-mock`: receives the prompt and deterministically emits a tool call
- `tool-executor`: receives the tool call and emits a mock tool result
- `finalizer`: receives the tool result and emits the final natural-language
  response
- `collector`: receives and verifies the final response

## Validate And Inspect

```bash
cargo run -p pureflow-cli -- validate examples/workloads/ai-call-orchestration.workflow.json
cargo run -p pureflow-cli -- inspect examples/workloads/ai-call-orchestration.workflow.json
cargo run -p pureflow-cli -- explain examples/workloads/ai-call-orchestration.workflow.json
```

Expected `validate` output:

```text
valid workflow `ai-call-orchestration-workload`
nodes: 5
edges: 4
```

Expected `explain` output:

```text
workflow `ai-call-orchestration-workload`
status: valid
nodes: 5
edges: 4
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
node order:
  - prompter inputs=0 outputs=1
  - llm-mock inputs=1 outputs=1
  - tool-executor inputs=1 outputs=1
  - finalizer inputs=1 outputs=1
  - collector inputs=1 outputs=0
edges:
  - prompter.prompt -> llm-mock.prompt capacity=4
  - llm-mock.tool-call -> tool-executor.call capacity=4
  - tool-executor.result -> finalizer.context capacity=4
  - finalizer.response -> collector.response capacity=4
```

## Run

```bash
cargo run -p pureflow-engine --example ai_call_orchestration
```

Expected output:

```text
ai orchestration workflow `ai-call-orchestration-workload` completed
prompt: prompt:what is the weather in sf?
tool call: tool_call:get_weather:SF
tool result: tool_result:get_weather:72F:sunny
final response: response:The weather in SF is 72F and sunny.
scheduled nodes: 5
completed nodes: 5
metadata records: 46
metadata lifecycle records: 10
metadata message records: 8
metadata queue_pressure records: 28
```

The exact metadata counts are a regression signal for the current runtime. If
metadata emission changes intentionally, update this page and the example output
together.

## Message Shape

Packets use a simple colon-delimited format that a real implementation would
replace with a structured schema:

| Stage        | Example payload                               |
|--------------|-----------------------------------------------|
| prompt       | `prompt:what is the weather in sf?`           |
| tool_call    | `tool_call:get_weather:SF`                    |
| tool_result  | `tool_result:get_weather:72F:sunny`           |
| response     | `response:The weather in SF is 72F and sunny.`|

The `type:` prefix makes the message family explicit, which would support a
real router or capability check that gates which edges may carry which message
types.

## Capability Gap Analysis

Running this workload exercises the current runtime against an AI orchestration
scenario and surfaces two gaps that concrete workloads would need to close:

### 1. Multi-turn feedback loops

The single-turn model above is acyclic. A real multi-turn agent would need to
route `tool_result` back into the LLM for a second inference turn, potentially
many times before arriving at a final response without a tool call.

What a multi-turn policy would require:

- `WorkflowRunPolicy::feedback_loops` to permit cyclic graph execution
- A no-progress watchdog (`with_watchdog`) to bound infinite loops
- A `should_continue` node decision boundary so the LLM node can signal
  "done" versus "call another tool"

The current `WorkflowRunPolicy::acyclic` (CLI default) rejects cycles at
validation time. The feedback-loop policy is available via the library API
(`pureflow-engine`) and already exercised by `crates/pureflow-engine/examples/feedback_loop.rs`.

### 2. External-effect capability enforcement

`tool-executor` in this workload is a native mock. A real implementation would
make outbound HTTP calls, query a database, or call an external API. Pureflow now
has the first vocabulary pieces for this path:

- `EffectCapability::ExternalEffect` declares that a node may perform a tool,
  service, database, or API effect.
- `ExternalEffectMetadataRecord` captures requested, completed, and failed
  effect observations as a metadata family distinct from `message` and
  `queue_pressure`.

What still remains future work:

- runtime or host adapters that automatically emit external-effect metadata
- strict enforcement for non-native execution boundaries that can mediate
  external effects
- latency/timing capture outside the stable metadata JSONL surface

Neither gap blocks the current workload - the mock is deterministic and
acyclic. The remaining enforcement work belongs in a future AI-orchestration or
capability-enforcement product epic.
