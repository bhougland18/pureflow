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
      cfg = config.dendritic.devShell.features.beads;

      releaseBySystem = {
        x86_64-linux = {
          platform = "linux_amd64";
          hash = "sha256-V4+id8Ejpom7EHdEXLIJPolMJQUXq8lE1HFaTgVO45E=";
        };
      };

      release = releaseBySystem.${pkgs.stdenv.hostPlatform.system} or null;

      br =
        if release == null then
          throw "beads feature is not packaged for ${pkgs.stdenv.hostPlatform.system} yet"
        else
          pkgs.stdenvNoCC.mkDerivation rec {
            pname = "beads-rust";
            version = "0.1.45";

            src = pkgs.fetchurl {
              url = "https://github.com/Dicklesworthstone/beads_rust/releases/download/v${version}/br-v${version}-${release.platform}.tar.gz";
              inherit (release) hash;
            };

            nativeBuildInputs = [ pkgs.gnutar ];

            sourceRoot = ".";

            installPhase = ''
              runHook preInstall
              install -Dm755 br $out/bin/br
              runHook postInstall
            '';
          };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ br ];
      };
    };
}
