{ ... }:
{
  perSystem =
    {
      config,
      lib,
      pkgs,
      ...
    }:
    let
      cfg = config.dendritic.devShell.features.acfs;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.description = lib.mkDefault "Dendritic ACFS-style Rust + JJ agent workspace shell";

        dendritic.devShell.features = {
          agent_mail.enable = lib.mkOverride 900 true;
          ai_tools.enable = lib.mkOverride 900 true;
          beads.enable = lib.mkOverride 900 true;
          direnv.enable = lib.mkOverride 900 true;
          jujutsu.enable = lib.mkOverride 900 true;
          native_cc.enable = lib.mkOverride 900 true;
          ntm.enable = lib.mkOverride 900 true;
          rust.enable = lib.mkOverride 900 true;
          rust_devtools.enable = lib.mkOverride 900 true;
          ubs.enable = lib.mkOverride 900 true;
        };

        dendritic.devShell.packages = with pkgs; [
          bacon
          cargo-nextest
          claude-code
          fd
          gh
          jq
          just
          tmux
          watchexec
        ];

        dendritic.devShell.env = {
          CARGO_TERM_COLOR = "always";
          RUST_BACKTRACE = "1";
        };

        dendritic.devShell.shellHookFragments = [
          ''
            export ACFS_VCS=jj
            export ACFS_GIT_BACKEND=1
          ''
        ];
      };
    };
}
