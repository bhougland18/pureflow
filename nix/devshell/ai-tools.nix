{ ... }:
{
  perSystem = { config, lib, pkgs, codex-cli-nix, llm-agents, system, ... }:
    let
      cfg = config.dendritic.devShell.features.ai_tools;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = with pkgs; [
          codex-cli-nix.packages.${system}.default
          llm-agents.packages.${system}.gemini-cli
        ];
      };
    };
}
