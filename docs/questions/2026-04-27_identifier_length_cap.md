# Identifier Length Cap

Question recorded while implementing `cdt-sgj.8`.

Current implementation choice:

- identifier length is measured in raw UTF-8 bytes
- maximum length is `256`
- overlong identifiers return `IdentifierError::TooLong { kind, limit }`

Why this choice:

- it keeps validation cheap
- it matches the audit goal of closing a low-cost robustness gap
- the repository currently treats identifiers as opaque strings, not user-facing text with normalization rules

Open follow-up:

- if the project later wants the cap measured in Unicode scalar values instead of bytes, the validation helper and tests should be updated together
