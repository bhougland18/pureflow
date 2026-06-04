# Tooling and Documentation Generation

## API Docs

Canonical API documentation is generated with Cargo:

```bash
cargo doc --workspace --no-deps
```

The human architecture guide is separate from the API docs.

## Human Guide

The human-facing architecture guide is rendered with Quarto from
`docs/architecture-guide/`.

Common render command:

```bash
nix develop . --command bash -lc 'cd docs/architecture-guide && quarto render --to pdf'
```

Useful outputs:

- `docs/architecture-guide/_output/Pureflow-Architecture-Guide.pdf`
- `docs/architecture-guide/_output/*.html`

## Diagram Tooling

Diagram source and artifacts:

- Checked Mermaid source: `docs/architecture-guide/figures/*.mmd`
- Rendered SVGs: `docs/architecture-guide/figures/generated/*.svg`
- PDF-safe PNGs: `docs/architecture-guide/figures/generated-png/*.png`

Generation conventions:

- Keep the chapter narrative authoritative.
- Keep labels short enough to read in PDF.
- Prefer PNGs for PDF stability when SVG rendering is unreliable.
- Keep generated artifacts checked in so review can see both source and result.
