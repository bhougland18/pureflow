{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.jujutsu;
      agentjjPkg = pkgs.rustPlatform.buildRustPackage rec {
        pname = "agentjj";
        version = "0.3.0";

        src = pkgs.fetchCrate {
          inherit pname version;
          hash = "sha256-CafOy6+fN7EMr36UndStLgUt3XbkqLZqwOHngytLC+Q=";
        };

        cargoLock.lockFile = "${src}/Cargo.lock";
        nativeCheckInputs = [ pkgs.git ];
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = with pkgs; [
          git
          jujutsu
          agentjjPkg
        ];
      };
    };
}
