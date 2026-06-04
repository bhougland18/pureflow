{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.native_cc;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = with pkgs; [
          rustPlatform.bindgenHook
          pkg-config
          cmake
          ninja
          clang
          llvmPackages.libclang.lib
        ];

        dendritic.devShell.env = {
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
        };

        dendritic.devShell.shellHookFragments = [
          ''
            export BINDGEN_EXTRA_CLANG_ARGS="$NIX_CFLAGS_COMPILE"
          ''
        ];
      };
    };
}
