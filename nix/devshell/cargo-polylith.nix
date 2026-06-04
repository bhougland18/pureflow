{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.cargo_polylith;
      cargo-polylith = pkgs.rustPlatform.buildRustPackage rec {
        pname = "cargo-polylith";
        version = "0.10.1";

        src = pkgs.fetchFromGitHub {
          owner = "johlrogge";
          repo = "cargo-polylith";
          rev = "aa6f4fe00c9f82c50b2a3cc3e6cf32de4505ec62";
          hash = "sha256-c0pcQvlHIjsANi8vW+Nbf9apyRtRndE8wCryk4Sicm0=";
        };

        cargoLock.lockFile = "${src}/Cargo.lock";
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ cargo-polylith ];
      };
    };
}
