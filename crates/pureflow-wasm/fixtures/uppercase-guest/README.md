# uppercase-guest — WASM Node Authoring Template

`uppercase-guest` is the canonical example for authoring a Pureflow Component
Model batch node. Copy this fixture as a starting point for any new WASM guest.

## Fixture Layout

```text
uppercase-guest/
├── Cargo.toml          # cdylib crate, wit-bindgen dep, standalone [workspace]
├── Cargo.lock          # checked in for deterministic guest builds
├── src/
│   └── lib.rs          # WIT bindings + Guest impl + wasm32 allocator boilerplate
└── testdata/
    ├── inputs.json          # stable list<port-batch> test vector (input side)
    └── expected-outputs.json  # stable list<port-batch> test vector (output side)
```

`target/` and any compiled `.wasm` artifacts are excluded by `.gitignore`; do
not commit them.

## WIT Contract

All guests implement the `pureflow:batch@0.1.0` `pureflow-node` world defined in
`crates/pureflow-wasm/wit/pureflow-batch.wit`:

```wit
package pureflow:batch@0.1.0;

interface batch {
    record endpoint {
        node-id: string,
        port-id: string,
    }

    record route {
        source: option<endpoint>,
        target: endpoint,
    }

    record execution {
        execution-id: string,
        attempt: u32,
    }

    record message-metadata {
        message-id: string,
        workflow-id: string,
        execution: execution,
        route: route,
    }

    variant payload {
        bytes(list<u8>),
        control(string),
    }

    record packet {
        metadata: message-metadata,
        payload: payload,
    }

    record port-batch {
        port-id: string,
        packets: list<packet>,
    }

    variant batch-error {
        guest-failure(string),
        unsupported-payload(string),
    }

    invoke: func(inputs: list<port-batch>) -> result<list<port-batch>, batch-error>;
}

world pureflow-node {
    export batch;
}
```

The guest exports exactly one function: `batch.invoke`. There are no imports —
guest components are pure computation with no access to the host environment.

## Batch Input/Output Shape

The host calls `invoke` with a `list<port-batch>`. Each `port-batch` groups all
packets that arrived on one named input port during a graph step. The guest
returns a `Result<list<port-batch>, batch-error>` — success returns output
batches keyed by output port name, failure returns a typed error that cancels
the workflow.

**Input shape** (`testdata/inputs.json`):

```json
[
  {
    "port-id": "in",
    "packets": [
      {
        "metadata": {
          "message-id": "msg-1",
          "workflow-id": "flow",
          "execution": { "execution-id": "run-1", "attempt": 1 },
          "route": {
            "source": { "node-id": "source", "port-id": "out" },
            "target": { "node-id": "wasm",   "port-id": "in"  }
          }
        },
        "payload": { "bytes": [104, 101, 108, 108, 111, 32, 102, 114, 111, 109, 32, 119, 97, 115, 109] }
      }
    ]
  }
]
```

The `bytes` array is the UTF-8 encoding of `"hello from wasm"`.

**Output shape** (`testdata/expected-outputs.json`):

```json
[
  {
    "port-id": "out",
    "packets": [
      {
        "metadata": { "...same metadata as input..." },
        "payload": { "bytes": [72, 69, 76, 76, 79, 32, 70, 82, 79, 77, 32, 87, 65, 83, 77] }
      }
    ]
  }
]
```

The `bytes` array is `"HELLO FROM WASM"`. Packet metadata is passed through
unchanged; only payloads are transformed.

**Payload variants:**

| Variant | Description |
|---------|-------------|
| `bytes(list<u8>)` | Raw byte payload — the common case for data pipelines |
| `control(string)` | JSON control message — guests that do not handle control payloads should return `batch-error::unsupported-payload` |

**Error variants:**

| Variant | When to use |
|---------|-------------|
| `batch-error::guest-failure(string)` | Internal guest failure (logic error, OOM, etc.) |
| `batch-error::unsupported-payload(string)` | Guest received a payload type it cannot handle |

## Build

Build from the repo dev shell, which provides the `wasm32-wasip2` standard
library and Wasmtime tooling:

```bash
nix develop . --command cargo build \
  --manifest-path crates/pureflow-wasm/fixtures/uppercase-guest/Cargo.toml \
  --target wasm32-wasip2 \
  --release
```

The compiled component is written to:

```text
crates/pureflow-wasm/fixtures/uppercase-guest/target/wasm32-wasip2/release/pureflow_wasm_uppercase_guest_fixture.wasm
```

Use `--release` — debug WASM components are substantially larger and slower.

## Target Requirements

| Requirement | Value |
|-------------|-------|
| Rust target | `wasm32-wasip2` |
| WASI version | 0.2 (Component Model preview2) |
| Crate type | `cdylib` |
| Standard library | `no_std` on `wasm32`, `std` on native |
| Allocator | Custom bump allocator (see `src/lib.rs`) — no external allocator dep |
| WIT bindgen | `wit-bindgen = { features = ["macros", "realloc"] }` |

The `nix develop .` shell pins the correct nightly toolchain and enables the
`wasm32-wasip2` sysroot. Do not build guests outside the dev shell unless you
have verified the same toolchain version is installed.

## Authoring a New Guest

1. **Copy the fixture:**

   ```bash
   cp -r crates/pureflow-wasm/fixtures/uppercase-guest \
          crates/pureflow-wasm/fixtures/my-node
   ```

2. **Rename the package** in `my-node/Cargo.toml`:

   ```toml
   [package]
   name = "pureflow-wasm-my-node-fixture"
   ```

3. **Implement the `Guest` trait** in `src/lib.rs`. The `invoke` function
   receives all input port batches and must return output port batches:

   ```rust
   struct MyNode;

   impl Guest for MyNode {
       fn invoke(inputs: Vec<PortBatch>) -> Result<Vec<PortBatch>, BatchError> {
           // Process inputs, produce outputs.
           // Return Err(BatchError::GuestFailure(...)) on unrecoverable errors.
           Ok(vec![PortBatch {
               port_id: String::from("out"),
               packets: transformed,
           }])
       }
   }

   export!(MyNode);
   ```

   Keep the allocator and `cabi_realloc` boilerplate at the top of `lib.rs`
   unchanged — it is required for the Component Model ABI.

4. **Add test vectors** in `testdata/inputs.json` and
   `testdata/expected-outputs.json` that cover the happy path and any error
   paths. The host test suite can load these files to verify the WIT contract
   stays aligned without building the guest.

5. **Build and verify:**

   ```bash
   nix develop . --command cargo build \
     --manifest-path crates/pureflow-wasm/fixtures/my-node/Cargo.toml \
     --target wasm32-wasip2 \
     --release
   ```

6. **Wire the component into a workflow** using a component manifest:

   ```json
   {
     "components": [
       {
         "node": "my-node",
         "component": "fixtures/my-node/target/wasm32-wasip2/release/pureflow_wasm_my_node_fixture.wasm",
         "fuel": 100000000
       }
     ]
   }
   ```

   Pass the manifest to `pureflow run --wasm-components` or validate it first
   with `pureflow validate-manifest --workflow workflow.json manifest.json`.

## What Not to Commit

`target/` and compiled `.wasm` files are build artifacts — do not commit them.
This fixture's `.gitignore` excludes both. When you copy the fixture, copy the
`.gitignore` too:

```gitignore
target/
*.wasm
```

The checked-in `Cargo.lock` is intentional: it ensures that `wit-bindgen` and
any other guest dependencies resolve to the same versions across machines.
