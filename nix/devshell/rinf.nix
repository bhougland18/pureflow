{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.rinf;
      rinf-cli = pkgs.rustPlatform.buildRustPackage rec {
        pname = "rinf_cli";
        version = "8.10.0";

        src = pkgs.fetchCrate {
          inherit pname version;
          hash = "sha256-yzBWdobzVIwdqf93U1DGXvbD7VaOSIyzY5MspDwMm1I=";
        };

        cargoLock.lockFile = "${src}/Cargo.lock";
        nativeBuildInputs = [ pkgs.pkg-config ];
        buildInputs = [
          pkgs.wayland
          pkgs.libx11
        ];
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ rinf-cli ];
      };
    };
}
