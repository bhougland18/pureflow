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
      cfg = config.dendritic.devShell.features.ntm;

      ntm = pkgs.buildGoModule rec {
        pname = "ntm";
        version = "unstable-2026-04-22";

        src = pkgs.fetchFromGitHub {
          owner = "Dicklesworthstone";
          repo = "ntm";
          rev = "b583b1a5952598fe6bb8c88f178a718532150dc3";
          hash = "sha256-90Jr0KClS0gs0sW1whqdtFKhx+75V5SyDKUUzozhwPY=";
        };

        vendorHash = "sha256-srGlFXdP48TfD/84437kk1eUi9HWLKMwVEkzy1OMOMs=";
        subPackages = [ "cmd/ntm" ];

        ldflags = [
          "-s"
          "-w"
          "-X main.version=${version}"
        ];

        doCheck = false;
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [
          ntm
          pkgs.tmux
        ];
      };
    };
}
