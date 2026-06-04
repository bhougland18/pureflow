{ ... }:
{
  perSystem =
    {
      config,
      lib,
      pkgs,
      fenixWasmToolchain,
      ...
    }:
    let
      cfg = config.dendritic.devShell.features.rust_wasm;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = with pkgs; [
          fenixWasmToolchain
          binaryen
          wasm-tools
          wasmtime
        ];
      };
    };
}
