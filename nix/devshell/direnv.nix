{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.direnv;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [
          pkgs.direnv
          pkgs.nix-direnv
        ];

        dendritic.devShell.env = {
          # nix-direnv provides the `use flake` implementation loaded by direnv.
          NIX_DIRENV_RC = "${pkgs.nix-direnv}/share/nix-direnv/direnvrc";
        };
      };
    };
}
