{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.flutter;
      nativeDeps = with pkgs; [
        at-spi2-atk
        atk
        cairo
        dbus
        gdk-pixbuf
        glib
        gtk3
        libdatrie
        libepoxy
        libselinux
        libsepol
        libthai
        libxkbcommon
        pango
        pcre
        pcre2
        xz
        libGL
        vulkan-loader
        libx11
        libxcursor
        libxext
        libxfixes
        libxi
        libxrandr
        libxrender
        libxtst
        libsysprof-capture
      ];
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages =
          [ pkgs.flutter pkgs.dart ] ++ nativeDeps;

        dendritic.devShell.env = {
          NIX_FLUTTER_SDK = "${pkgs.flutter}";
        };

        dendritic.devShell.shellHookFragments = [
          ''
            export LOCAL_FLUTTER_ROOT="$PWD/.cache/flutter-sdk"
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath nativeDeps}:$LD_LIBRARY_PATH"

            if [ -x "$LOCAL_FLUTTER_ROOT/bin/flutter" ]; then
              export FLUTTER_ROOT="$LOCAL_FLUTTER_ROOT"
              export PATH="$FLUTTER_ROOT/bin:$PATH"
            else
              export FLUTTER_ROOT="$NIX_FLUTTER_SDK"
              export PATH="$NIX_FLUTTER_SDK/bin:$PATH"
              echo "Bootstrap required: run 'bash ./scripts/bootstrap-dev-workspace.sh' once in this workspace."
            fi
          ''
        ];
      };
    };
}
