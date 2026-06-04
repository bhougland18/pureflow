{ lib, ... }:
{
  perSystem =
    {
      config,
      lib,
      pkgs,
      ...
    }:
    let
      cfg = config.dendritic.devShell;
      exportLines = lib.mapAttrsToList (
        name: value: "export ${name}=${lib.escapeShellArg value}"
      ) cfg.env;
    in
    {
      options.dendritic.devShell = {
        description = lib.mkOption {
          type = lib.types.str;
          default = "Dendritic development shell";
        };

        packages = lib.mkOption {
          type = lib.types.listOf lib.types.package;
          default = [ ];
        };

        env = lib.mkOption {
          type = lib.types.attrsOf lib.types.str;
          default = { };
        };

        shellHookFragments = lib.mkOption {
          type = lib.types.listOf lib.types.lines;
          default = [ ];
        };

        features = {
          acfs.enable = lib.mkEnableOption "ACFS-style Rust + Jujutsu agent workspace tooling";
          agent_mail.enable = lib.mkEnableOption "MCP Agent Mail tooling";
          ai_tools.enable = lib.mkEnableOption "AI assistant CLI tooling";
          android.enable = lib.mkEnableOption "Android SDK and Java tooling";
          beads.enable = lib.mkEnableOption "Beads Rust issue-tracking tooling";
          crane.enable = lib.mkEnableOption "Crane Rust build orchestration";
          cargo_polylith.enable = lib.mkEnableOption "cargo-polylith scaffolding";
          direnv.enable = lib.mkEnableOption "direnv and nix-direnv support";
          flutter.enable = lib.mkEnableOption "Flutter and Dart tooling";
          jujutsu.enable = lib.mkEnableOption "Jujutsu and agentjj tooling";
          native_cc.enable = lib.mkEnableOption "Native C/C++ interop toolchain support";
          ntm.enable = lib.mkEnableOption "Named Tmux Manager orchestration tooling";
          documentation.enable = lib.mkEnableOption "Documentation, PDF, and diagram tooling";
          rinf.enable = lib.mkEnableOption "rinf bridge tooling";
          rust_devtools.enable = lib.mkEnableOption "Optional Rust workflow and profiling CLI tooling";
          rust_lint_dylint.enable = lib.mkEnableOption "Dylint runtime and lint authoring support";
          rust_wasm.enable = lib.mkEnableOption "Rust wasm32-wasip2 Component Model tooling";
          rust.enable = lib.mkEnableOption "Rust toolchain support";
          stac.enable = lib.mkEnableOption "STAC CLI tooling";
          ubs.enable = lib.mkEnableOption "Ultimate Bug Scanner tooling";
        };
      };

      config = {
        dendritic.devShell = {
          description = "Dendritic Flutter + Rust workspace shell";
          features = {
            acfs.enable = lib.mkDefault false;
            agent_mail.enable = lib.mkDefault false;
            ai_tools.enable = lib.mkDefault true;
            android.enable = lib.mkDefault true;
            beads.enable = lib.mkDefault false;
            crane.enable = lib.mkDefault true;
            cargo_polylith.enable = lib.mkDefault true;
            direnv.enable = lib.mkDefault true;
            flutter.enable = lib.mkDefault true;
            jujutsu.enable = lib.mkDefault true;
            native_cc.enable = lib.mkDefault true;
            ntm.enable = lib.mkDefault false;
            documentation.enable = lib.mkDefault false;
            rinf.enable = lib.mkDefault true;
            rust_devtools.enable = lib.mkDefault true;
            rust_lint_dylint.enable = lib.mkDefault false;
            rust_wasm.enable = lib.mkDefault false;
            rust.enable = lib.mkDefault true;
            stac.enable = lib.mkDefault true;
            ubs.enable = lib.mkDefault false;
          };
        };

        devShells.default = pkgs.mkShell {
          packages = cfg.packages;
          shellHook = lib.concatStringsSep "\n" (exportLines ++ cfg.shellHookFragments);
        };

        formatter = pkgs.nixfmt-rfc-style;
      };
    };
}
