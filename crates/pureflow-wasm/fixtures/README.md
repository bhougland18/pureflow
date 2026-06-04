# WASM Guest Fixtures

This directory contains source fixtures for Pureflow Component Model batch
guests. Fixtures are intentionally kept outside the workspace membership so the
default workspace checks do not require a `wasm32-wasip2` standard library.

## Uppercase Guest

`uppercase-guest` implements the `pureflow:batch@0.1.0` `pureflow-node` world from
`../wit/pureflow-batch.wit`. It accepts byte payload packets, uppercases ASCII
letters, and emits every transformed packet on the `out` port. Control payloads
return `batch-error::unsupported-payload`.

Build it through the repo dev shell, which enables the `rust_wasm` feature and
provides the `wasm32-wasip2` Rust standard library plus `wasm-tools`,
`wasmtime`, and `binaryen`:

```bash
nix develop . --command cargo build \
  --manifest-path crates/pureflow-wasm/fixtures/uppercase-guest/Cargo.toml \
  --target wasm32-wasip2 \
  --release
```

The component artifact is written to:

```text
crates/pureflow-wasm/fixtures/uppercase-guest/target/wasm32-wasip2/release/pureflow_wasm_uppercase_guest_fixture.wasm
```

The `testdata` directory contains stable WIT-shaped JSON vectors:

- `inputs.json` is the ordered `list<port-batch>` passed to `batch.invoke`.
- `expected-outputs.json` is the ordered `list<port-batch>` returned on success.

The host crate parses these vectors in its normal unit tests so the checked
fixture inputs stay aligned with the WIT ABI even when the guest component is not
built by default.

See [`uppercase-guest/README.md`](uppercase-guest/README.md) for the full
authoring template: WIT contract, batch input/output shape, build instructions,
target requirements, and a step-by-step guide for creating a new guest.
