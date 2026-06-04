{ ... }:
{
  perSystem =
    {
      config,
      fenixToolchain,
      lib,
      pkgs,
      ...
    }:
    let
      cfg = config.dendritic.devShell.features.documentation;

      rustPlatform = pkgs.makeRustPlatform {
        cargo = fenixToolchain;
        rustc = fenixToolchain;
      };

      frankenNetworkx = pkgs.fetchFromGitHub {
        owner = "Dicklesworthstone";
        repo = "franken_networkx";
        rev = "cb8bdb590573b9de5ddbc26b948279057ac4049e";
        hash = "sha256-XOBJxlSjnn1o+u4nCGleIZCxB+2iLOCSpS80hxFI53U=";
      };

      frankenmermaidSource = pkgs.fetchFromGitHub {
        owner = "Dicklesworthstone";
        repo = "frankenmermaid";
        rev = "fe575e227ac86b12e2b3b3a2092780095957be55";
        hash = "sha256-AoYxjJhfClwC2RF3KyJcwpeujmX/eu95k24sSDTLMLM=";
      };

      frankenmermaidPatchedSource = pkgs.runCommand "frankenmermaid-source" { } ''
        cp -R ${frankenmermaidSource} $out
        chmod -R u+w $out

        mkdir -p $out/.vendor
        cp -R ${frankenNetworkx} $out/.vendor/franken_networkx
        chmod -R u+w $out/.vendor/franken_networkx

        substituteInPlace $out/.vendor/franken_networkx/crates/fnx-runtime/Cargo.toml \
          --replace-fail 'asupersync-integration = ["dep:asupersync"]' 'asupersync-integration = []' \
          --replace-fail 'ftui-integration = ["dep:ftui"]' 'ftui-integration = []' \
          --replace-fail 'asupersync = { version = "0.2.0", optional = true, default-features = false }' "" \
          --replace-fail 'ftui = { path = "/dp/frankentui/crates/ftui", optional = true, default-features = false }' ""

        substituteInPlace $out/Cargo.toml \
          --replace-fail 'fnx-runtime = { git = "https://github.com/Dicklesworthstone/franken_networkx.git", rev = "cb8bdb590573b9de5ddbc26b948279057ac4049e", default-features = false }' 'fnx-runtime = { path = ".vendor/franken_networkx/crates/fnx-runtime", default-features = false }' \
          --replace-fail 'fnx-classes = { git = "https://github.com/Dicklesworthstone/franken_networkx.git", rev = "cb8bdb590573b9de5ddbc26b948279057ac4049e", default-features = false }' 'fnx-classes = { path = ".vendor/franken_networkx/crates/fnx-classes", default-features = false }' \
          --replace-fail 'fnx-algorithms = { git = "https://github.com/Dicklesworthstone/franken_networkx.git", rev = "cb8bdb590573b9de5ddbc26b948279057ac4049e", default-features = false }' 'fnx-algorithms = { path = ".vendor/franken_networkx/crates/fnx-algorithms", default-features = false }' \
          --replace-fail 'fnx-views = { git = "https://github.com/Dicklesworthstone/franken_networkx.git", rev = "cb8bdb590573b9de5ddbc26b948279057ac4049e", default-features = false }' 'fnx-views = { path = ".vendor/franken_networkx/crates/fnx-views", default-features = false }'

        substituteInPlace $out/Cargo.lock \
          --replace-fail 'source = "git+https://github.com/Dicklesworthstone/franken_networkx.git?rev=cb8bdb590573b9de5ddbc26b948279057ac4049e#cb8bdb590573b9de5ddbc26b948279057ac4049e"' ""
      '';

      frankenmermaid = rustPlatform.buildRustPackage {
        pname = "frankenmermaid-cli";
        version = "0.1.0-unstable-2026-05-11";

        src = frankenmermaidPatchedSource;
        cargoHash = "sha256-6utyY2/pv3jW90q/UwE2s2o1OswLIZH2Ke2oAEWtZcc=";

        nativeBuildInputs = [
          pkgs.cmake
          pkgs.llvmPackages.libclang
          pkgs.makeWrapper
          pkgs.pkg-config
        ];

        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.glibc.dev}/include";
        LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

        buildInputs = [
          pkgs.openssl
          pkgs.zlib
        ];

        buildAndTestSubdir = "crates/fm-cli";
        doCheck = false;

        postFixup = ''
          for binary in "$out"/bin/*; do
            wrapProgram "$binary" \
              --prefix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib ]}"
          done
        '';
      };
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [
          pkgs.quarto
          pkgs.typst
          pkgs.chromium
          frankenmermaid
        ];
      };
    };
}
