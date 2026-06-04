default:
  @just --list

fmt:
  cargo fmt --all

check:
  cargo check --workspace

test:
  cargo test --workspace

dylint-list:
  cargo-dylint-nightly list

dylint-all:
  cargo-dylint-nightly --all

dylint LIB:
  cargo-dylint-nightly --lib {{LIB}}
