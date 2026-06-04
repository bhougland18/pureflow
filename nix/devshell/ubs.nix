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
      cfg = config.dendritic.devShell.features.ubs;

      ubsRuntimePath = lib.makeBinPath [
        pkgs.ast-grep
        pkgs.bash
        pkgs.coreutils
        pkgs.curl
        pkgs.findutils
        pkgs.git
        pkgs.gnugrep
        pkgs.gnused
        pkgs.jq
        pkgs.nodejs
        pkgs.python3
        pkgs.ripgrep
        pkgs.typescript
        pkgs.typos
        pkgs.wget
      ];

      ubs = pkgs.stdenvNoCC.mkDerivation rec {
        pname = "ultimate-bug-scanner";
        version = "unstable-2026-04-22";

        src = pkgs.fetchFromGitHub {
          owner = "Dicklesworthstone";
          repo = "ultimate_bug_scanner";
          rev = "eca5cb1783c6e04f365a27e54ea025d3efa78308";
          hash = "sha256-/TQEpo/PF0qMPnm7Xpu29Ez8cO5pre6bzSKuAIvtxU4=";
        };

        nativeBuildInputs = [ pkgs.makeWrapper ];

        installPhase = ''
          runHook preInstall

          install -Dm755 ubs $out/libexec/ubs
          mkdir -p $out/share/ultimate_bug_scanner
          cp -r modules $out/share/ultimate_bug_scanner/
          install -Dm644 README.md $out/share/doc/ultimate_bug_scanner/README.md

          makeWrapper $out/libexec/ubs $out/bin/ubs \
            --set MODULE_DIR $out/share/ultimate_bug_scanner/modules \
            --set UBS_AST_GREP_BIN ${pkgs.ast-grep}/bin/ast-grep \
            --set UBS_NO_AUTO_UPDATE 1 \
            --prefix PATH : ${ubsRuntimePath}

          runHook postInstall
        '';
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ ubs ];
      };
    };
}
