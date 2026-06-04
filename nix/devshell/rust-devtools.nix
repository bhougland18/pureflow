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
      cfg = config.dendritic.devShell.features.rust_devtools;

      mkCrateCli =
        {
          pname,
          version,
          hash,
        }:
        let
          archive = pkgs.fetchurl {
            url = "https://crates.io/api/v1/crates/${pname}/${version}/download";
            name = "${pname}-${version}.crate";
            inherit hash;
          };

          src = pkgs.runCommandLocal "${pname}-${version}-source" { } ''
            mkdir -p "$out"
            tar -xzf ${archive} -C "$out" --strip-components=1
          '';
        in
        pkgs.rustPlatform.buildRustPackage {
          inherit pname version src;
          cargoLock.lockFile = "${src}/Cargo.lock";
          doCheck = false;
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [
            pkgs.openssl
            pkgs.zlib
          ];
        };

      deslop = mkCrateCli {
        pname = "deslop";
        version = "0.2.0";
        hash = "sha256-8gulw81FSKiymjsr9l58JtC4Bv2WdCgAtdF1iLR1Xbg=";
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [
          pkgs."cargo-flamegraph"
          deslop
        ];
      };
    };
}
