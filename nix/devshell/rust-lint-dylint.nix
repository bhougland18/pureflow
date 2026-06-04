{ ... }:
{
  perSystem =
    {
      config,
      lib,
      pkgs,
      craneLib,
      fenixToolchain,
      dylintNightlyToolchain,
      dylintNightlyChannel,
      ...
    }:
    let
      cfg = config.dendritic.devShell.features.rust_lint_dylint;
      nativeRuntimeDeps = [
        pkgs.openssl
        pkgs.zlib
      ];
      dylint-driver-toolchain = "${dylintNightlyChannel}-${pkgs.stdenv.hostPlatform.config}";

      mkCrateCli =
        {
          pname,
          version,
          hash,
        }:
        let
          archive = pkgs.fetchurl {
            url = "https://crates.io/api/v1/crates/${pname}/${version}/download";
            name = "${pname}-${version}.crate";
            inherit hash;
          };

          src = pkgs.runCommandLocal "${pname}-${version}-source" { } ''
            mkdir -p "$out"
            tar -xzf ${archive} -C "$out" --strip-components=1
          '';
        in
        pkgs.rustPlatform.buildRustPackage {
          inherit pname version src;
          cargoLock.lockFile = "${src}/Cargo.lock";
          doCheck = false;
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [
            pkgs.openssl
            pkgs.zlib
          ];
        };

      dylint-workspace-src = pkgs.fetchFromGitHub {
        owner = "trailofbits";
        repo = "dylint";
        rev = "v5.0.0";
        hash = "sha256-Q06arUQ0p6nWtAbpTGJdW34F9Gg6k2rXqRqkLHGe7Zs=";
      };

      dylint-src = lib.cleanSourceWith {
        src = dylint-workspace-src;
        filter =
          path: type: (craneLib.filterCargoSources path type) || lib.hasSuffix "/internal/template.tar" path;
      };

      cargo-dylint-common-args = {
        src = dylint-src;
        strictDeps = true;
        pname = "cargo-dylint";
        version = "5.0.0";
        nativeBuildInputs = [ pkgs.pkg-config ];
        buildInputs = [
          pkgs.openssl
          pkgs.zlib
        ];
        cargoExtraArgs = "-p cargo-dylint";
      };

      cargo-dylint-artifacts = craneLib.buildDepsOnly (
        cargo-dylint-common-args
        // {
          pname = "cargo-dylint-deps";
        }
      );

      cargo-dylint = craneLib.buildPackage (
        cargo-dylint-common-args
        // {
          cargoArtifacts = cargo-dylint-artifacts;
          doCheck = false;
        }
      );

      dylint-link = mkCrateCli {
        pname = "dylint-link";
        version = "5.0.0";
        hash = "sha256-ozWoppKp8ePiQQiyF4yZussc1ujQRr+No1nAnDCf+Jc=";
      };

      wrapped-dylint-link = pkgs.symlinkJoin {
        name = "dylint-link-wrapped";
        paths = [ dylint-link ];
        nativeBuildInputs = [ pkgs.makeWrapper ];
        postBuild = ''
          wrapProgram "$out/bin/dylint-link" \
            --set-default RUSTUP_TOOLCHAIN ${lib.escapeShellArg dylint-driver-toolchain}
        '';
      };

      rustup-shim = pkgs.writeShellScriptBin "rustup" ''
        find_toolchain_file() {
          local dir
          dir="$PWD"

          while [ "$dir" != "/" ]; do
            if [ -f "$dir/rust-toolchain.toml" ]; then
              printf '%s\n' "$dir/rust-toolchain.toml"
              return 0
            fi

            if [ -f "$dir/rust-toolchain" ]; then
              printf '%s\n' "$dir/rust-toolchain"
              return 0
            fi

            dir="$(dirname "$dir")"
          done

          return 1
        }

        read_channel() {
          local toolchain_file
          toolchain_file="$1"

          if grep -q '^\[toolchain\]' "$toolchain_file"; then
            sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$toolchain_file" | head -n 1
            return 0
          fi

          head -n 1 "$toolchain_file"
        }

        toolchain_file="$(find_toolchain_file || true)"

        if [ -n "$toolchain_file" ]; then
          toolchain="$(read_channel "$toolchain_file")"
        else
          toolchain=""
        fi

        if [ -z "$toolchain" ]; then
          toolchain="stable"
        fi

        if [ "$1" = "show" ] && [ "$2" = "active-toolchain" ]; then
          printf '%s (default)\n' "$toolchain"
          exit 0
        fi

        if [ "$1" = "which" ] && [ "$2" = "rustc" ]; then
          command -v rustc
          exit 0
        fi

        echo "This dev shell provides a minimal rustup shim for Dylint consumption." >&2
        echo "Supported commands: rustup show active-toolchain, rustup which rustc" >&2
        exit 1
      '';

      cargo-dylint-nightly = pkgs.writeShellScriptBin "cargo-dylint-nightly" ''
        export PATH="${dylintNightlyToolchain}/bin:${wrapped-dylint-link}/bin:$PATH"
        export RUSTUP_TOOLCHAIN="${dylint-driver-toolchain}"
        export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="dylint-link"
        export DYLINT_DRIVER_PATH="$PWD/.cache/dylint-drivers"
        export DYLINT_DRIVER_BUILD_ROOT="$PWD/.cache/dylint-driver-build"
        exec cargo dylint "$@"
      '';

      dylint-tools = pkgs.symlinkJoin {
        name = "dylint-tools";
        paths = [
          cargo-dylint
          wrapped-dylint-link
          cargo-dylint-nightly
          rustup-shim
        ];
      };

    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [
          dylint-tools
          pkgs.pkg-config
          pkgs.openssl
        ];

        dendritic.devShell.env = {
          DYLINT_DRIVER_BUILD_TOOLCHAIN = dylint-driver-toolchain;
        };

        dendritic.devShell.shellHookFragments = [
          ''
                        export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath nativeRuntimeDeps}:$LD_LIBRARY_PATH"
                        export DYLINT_DRIVER_PATH="$PWD/.cache/dylint-drivers"
                        export DYLINT_DRIVER_BUILD_ROOT="$PWD/.cache/dylint-driver-build"
                        dylint_driver_toolchain="$DYLINT_DRIVER_BUILD_TOOLCHAIN"
                        dylint_driver_channel="${dylintNightlyChannel}"

                        if [ -n "$dylint_driver_toolchain" ]; then
                          mkdir -p "$DYLINT_DRIVER_PATH/$dylint_driver_toolchain"
                          mkdir -p "$DYLINT_DRIVER_PATH/$dylint_driver_channel"
                          if [ ! -x "$DYLINT_DRIVER_PATH/$dylint_driver_toolchain/dylint-driver" ] || ! "$DYLINT_DRIVER_PATH/$dylint_driver_toolchain/dylint-driver" -V >/dev/null 2>&1; then
                            mkdir -p "$DYLINT_DRIVER_BUILD_ROOT/src"

                            if [ ! -f "$DYLINT_DRIVER_BUILD_ROOT/Cargo.toml" ]; then
                              cp -r ${dylint-src}/driver "$DYLINT_DRIVER_BUILD_ROOT/driver"
                              cp -r ${dylint-src}/internal "$DYLINT_DRIVER_BUILD_ROOT/internal"

                              cat > "$DYLINT_DRIVER_BUILD_ROOT/Cargo.toml" <<'EOF'
            [package]
            name = "dylint-driver-runner"
            version = "5.0.0"
            edition = "2024"

            [dependencies]
            anyhow = "1.0"
            env_logger = "0.11"
            dylint_driver = { path = "driver" }

            [workspace]
            exclude = ["driver"]

            [workspace.dependencies]
            anstyle = "1.0"
            anyhow = "1.0"
            assert_cmd = "2.0"
            bitflags = "2.9"
            cargo-util = "0.2"
            cargo_metadata = "0.23"
            ctor = "0.6"
            env_logger = "0.11"
            git2 = "0.20"
            home = "=0.5.9"
            if_chain = "1.0"
            log = "0.4"
            predicates = "3.1"
            regex = "1.11"
            rustversion = "1.0"
            semver = "1.0"
            serde = "1.0"
            tar = "0.4"
            tempfile = "3.23"
            thiserror = "2.0"
            toml = "0.9"
            toml_edit = "0.23"
            walkdir = "2.5"

            [workspace.lints.clippy]
            nursery = { level = "warn", priority = -1 }
            pedantic = { level = "warn", priority = -1 }
            option-if-let-else = "allow"
            missing-errors-doc = "allow"
            missing-panics-doc = "allow"
            significant-drop-tightening = "allow"
            struct-field-names = "allow"

            [workspace.lints.rust.unexpected_cfgs]
            level = "deny"
            check-cfg = [
                "cfg(coverage)",
                "cfg(dylint_lib, values(any()))",
                "cfg(nightly)",
                "cfg(__cargo_cli)",
                "cfg(__library_packages)",
            ]
            EOF

                              cat > "$DYLINT_DRIVER_BUILD_ROOT/src/main.rs" <<'EOF'
            #![feature(rustc_private)]

            use anyhow::Result;
            use std::env;

            fn main() -> Result<()> {
                env_logger::init();

                let args: Vec<_> = env::args_os().collect();
                dylint_driver::dylint_driver(&args)
            }
            EOF
                            fi

                            (
                              cd "$DYLINT_DRIVER_BUILD_ROOT"
                              export RUSTUP_TOOLCHAIN="$dylint_driver_toolchain"
                              export PATH="${dylintNightlyToolchain}/bin:$PATH"
                              export RUSTFLAGS="''${RUSTFLAGS:-} -C link-args=-Wl,-rpath,${dylintNightlyToolchain}/lib"
                              cargo build --quiet
                            )

                            ln -sfn \
                              "$DYLINT_DRIVER_BUILD_ROOT/target/debug/dylint-driver-runner" \
                              "$DYLINT_DRIVER_PATH/$dylint_driver_toolchain/dylint-driver"
                          fi

                          ln -sfn \
                            "$DYLINT_DRIVER_PATH/$dylint_driver_toolchain/dylint-driver" \
                            "$DYLINT_DRIVER_PATH/$dylint_driver_channel/dylint-driver"
                        fi

                        export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="dylint-link"
          ''
        ];
      };
    };
}
