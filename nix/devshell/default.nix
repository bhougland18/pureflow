{ lib, ... }:

let
  allNixFiles = lib.filesystem.listFilesRecursive ./.;
  featureModules = lib.filter (
    path:
      lib.hasSuffix ".nix" (builtins.toString path)
      && (builtins.toString path) != (builtins.toString ./default.nix)
  ) allNixFiles;
in
{
  imports = featureModules;
}
