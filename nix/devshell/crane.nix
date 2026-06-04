{ ... }:
{
  perSystem = { config, lib, ... }:
    let
      cfg = config.dendritic.devShell.features.crane;
    in
    {
      config = lib.mkIf cfg.enable { };
    };
}
