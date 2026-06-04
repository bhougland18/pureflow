{ ... }:
{
  perSystem = {
    config,
    lib,
    pkgs,
    fenixToolchain,
    fenixRustSrc,
    ...
  }:
    let
      cfg = config.dendritic.devShell.features.rust;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = with pkgs; [
          fenixToolchain
          rust-analyzer
        ];

        dendritic.devShell.env = {
          RUST_SRC_PATH = fenixRustSrc;
        };
      };
    };
}
