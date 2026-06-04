{
  description = "Pureflow development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
    codex-cli-nix.url = "github:sadjow/codex-cli-nix";
    llm-agents.url = "github:numtide/llm-agents.nix";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      flake-parts,
      nixpkgs,
      codex-cli-nix,
      llm-agents,
      crane,
      fenix,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];

      imports = [ ./nix/devshell ];

      perSystem =
        { system, ... }:
        let
          pkgs = import nixpkgs {
            inherit system;
            config = {
              allowUnfree = true;
              android_sdk.accept_license = true;
            };
          };
          fenixToolchain = fenix.packages.${system}.stable.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rustc"
            "rustfmt"
          ];
          fenixWasmToolchain = fenix.packages.${system}.combine [
            fenixToolchain
            fenix.packages.${system}.targets.wasm32-wasip2.stable.rust-std
          ];
          dylintNightlyBase = fenix.packages.${system}.toolchainOf {
            channel = "nightly";
            date = "2025-09-18";
            sha256 = "sha256-JuyNmA7iixvGBDN+0DpivQofDODFrd2qh+kE4B3X3I8=";
          };
          dylintNightlyToolchain = dylintNightlyBase.withComponents [
            "cargo"
            "clippy"
            "llvm-tools-preview"
            "rust-src"
            "rustc"
            "rustc-dev"
            "rustfmt"
          ];
          fenixRustSrc = "${fenix.packages.${system}.stable.rust-src}/lib/rustlib/src/rust/library";
          craneLib = crane.mkLib pkgs;
        in
        {
          _module.args = {
            inherit
              pkgs
              fenixToolchain
              fenixWasmToolchain
              fenixRustSrc
              craneLib
              codex-cli-nix
              llm-agents
              dylintNightlyToolchain
              ;
            dylintNightlyChannel = "nightly-2025-09-18";
          };

          dendritic.devShell = {
            description = "Pureflow ACFS development shell";
            env.RUSTUP_TOOLCHAIN = "nightly-2025-09-18";
            packages = [ ];

            features = {
              acfs.enable = true;
              direnv.enable = true;
              jujutsu.enable = true;
              documentation.enable = true;
              rust.enable = true;
              rust_wasm.enable = true;
              rust_devtools.enable = true;
              rust_lint_dylint.enable = true;

              android.enable = false;
              cargo_polylith.enable = false;
              crane.enable = false;
              flutter.enable = false;
              rinf.enable = false;
              stac.enable = false;
            };
          };
        };
    };
}
