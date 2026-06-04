{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.stac;
      stac-cli = pkgs.stdenv.mkDerivation rec {
        pname = "stac-cli";
        version = "1.6.0";

        src = pkgs.fetchurl {
          url = "https://github.com/StacDev/cli-installer/releases/download/stac-cli-v${version}/stac_cli_${version}_linux_x64.tar.gz";
          sha256 = "sha256-nMHjBKMwOT2yA2dxjIVAG3uf8bSUAwAm8OH0i3QXuF4=";
        };

        nativeBuildInputs = [ pkgs.autoPatchelfHook ];
        buildInputs = [ pkgs.stdenv.cc.cc.lib ];
        sourceRoot = ".";

        installPhase = ''
          mkdir -p $out/bin
          cp stac $out/bin/
          chmod +x $out/bin/stac
        '';
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ stac-cli ];
      };
    };
}
