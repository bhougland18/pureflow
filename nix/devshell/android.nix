{ ... }:
{
  perSystem = { config, lib, pkgs, ... }:
    let
      cfg = config.dendritic.devShell.features.android;
      androidSdk = (
        pkgs.androidenv.composeAndroidPackages {
          platformVersions = [
            "34"
            "36"
          ];
          buildToolsVersions = [
            "35.0.0"
            "36.1.0"
          ];
          abiVersions = [ "arm64-v8a" ];
          includeEmulator = false;
          includeCmake = true;
          cmakeVersions = [ "3.22.1" ];
          includeNDK = true;
          ndkVersion = "28.2.13676358";
          ndkVersions = [ "28.2.13676358" ];
          useGoogleAPIs = false;
          useGoogleTVAddOns = false;
        }
      ).androidsdk;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = with pkgs; [
          openjdk17
          android-tools
          androidSdk
        ];

        dendritic.devShell.env = {
          JAVA_HOME = "${pkgs.openjdk17}";
          ANDROID_HOME = "${androidSdk}/libexec/android-sdk";
          ANDROID_SDK_ROOT = "${androidSdk}/libexec/android-sdk";
          ANDROID_NDK_HOME = "${androidSdk}/libexec/android-sdk/ndk/28.2.13676358";
          ANDROID_NDK_BIN = "${androidSdk}/libexec/android-sdk/ndk/28.2.13676358/toolchains/llvm/prebuilt/linux-x86_64/bin";
        };

        dendritic.devShell.shellHookFragments = [
          ''
            if [ -d "$ANDROID_NDK_BIN" ]; then
              export PATH="$ANDROID_NDK_BIN:$PATH"
              export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$ANDROID_NDK_BIN/aarch64-linux-android21-clang"
              export CC_aarch64_linux_android="$ANDROID_NDK_BIN/aarch64-linux-android21-clang"
              export AR_aarch64_linux_android="$ANDROID_NDK_BIN/llvm-ar"
            fi
          ''
        ];
      };
    };
}
