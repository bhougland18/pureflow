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
      cfg = config.dendritic.devShell.features.agent_mail;
      runtimeLibPath = lib.makeLibraryPath [
        pkgs.openssl
        pkgs.sqlite
        pkgs.zlib
      ];

      releaseBySystem = {
        x86_64-linux = {
          target = "x86_64-unknown-linux-gnu";
          hash = "sha256-WkJniPA6vwnVC4F0H+Gm/6wMkXOLcwp8IYcHRr6tuR0=";
        };
      };

      release = releaseBySystem.${pkgs.stdenv.hostPlatform.system} or null;

      agent-mail =
        if release == null then
          throw "agent_mail feature is not packaged for ${pkgs.stdenv.hostPlatform.system} yet"
        else
          pkgs.stdenvNoCC.mkDerivation rec {
            pname = "mcp-agent-mail";
            version = "0.2.46";

            src = pkgs.fetchurl {
              url = "https://github.com/Dicklesworthstone/mcp_agent_mail_rust/releases/download/v${version}/mcp-agent-mail-${release.target}.tar.xz";
              inherit (release) hash;
            };

            nativeBuildInputs = [
              pkgs.makeWrapper
              pkgs.xz
            ];

            sourceRoot = "mcp-agent-mail-${release.target}";

            installPhase = ''
              runHook preInstall
              install -Dm755 am $out/libexec/am
              install -Dm755 mcp-agent-mail $out/libexec/mcp-agent-mail
              install -Dm644 README.md $out/share/doc/mcp-agent-mail/README.md
              install -Dm644 LICENSE $out/share/doc/mcp-agent-mail/LICENSE

              makeWrapper $out/libexec/am $out/bin/am \
                --prefix LD_LIBRARY_PATH : ${runtimeLibPath}
              makeWrapper $out/libexec/mcp-agent-mail $out/bin/mcp-agent-mail \
                --prefix LD_LIBRARY_PATH : ${runtimeLibPath}

              runHook postInstall
            '';
          };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ agent-mail ];
      };
    };
}
