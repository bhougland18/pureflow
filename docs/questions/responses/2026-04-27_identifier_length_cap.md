# Response: Identifier Length Cap

Reviewing `crates/pureflow-types/src/lib.rs` (`MAX_IDENTIFIER_LEN = 256`,
`IdentifierError::TooLong { kind, limit }`).

## Decision: stay with raw UTF-8 bytes. Park the scalar-value follow-up.

The current implementation is correct for what these identifiers actually
are. The follow-up only becomes load-bearing if the project later acquires a
user-facing identifier surface, which it does not currently have.

### Why bytes is the right unit here

- These identifiers are opaque slugs/keys, not user-facing prose. The existing
  validation in `pureflow-types` already rejects whitespace and control
  characters, which steers callers toward ASCII-typical forms. With those
  rules in place, "256 bytes" and "256 scalars" are nearly identical in
  practice for the inputs the system will actually see.
- A byte cap is a real wire and storage limit. Every transport boundary the
  identifiers cross — channel framing, JSON metadata records, JSONL run logs
  (`metadata-jsonl-sink`), eventual WASM-side string handling, future Arrow
  metadata columns — speaks bytes, not scalar counts. A scalar cap does not
  bound any of those.
- The error vocabulary already names the unit explicitly: `IdentifierError::
  TooLong { kind, limit }` plus the `Display` text "must not exceed {limit}
  bytes". That is honest about what is being enforced and is the right shape
  for AI inspection.
- Cheap validation is a stated goal in the original question, and `len()` on a
  `&str` is `O(1)`. A scalar count requires a `chars().count()` walk. Not
  expensive, but strictly more work for no current win.

### When (if ever) to revisit

The scalar-value variant becomes interesting only if one of these lands:

- A user-facing identifier surface — e.g., contract IDs or workflow IDs that
  end up rendered in an authoring UI where graphemes/scalars matter for
  alignment, truncation, or accessibility.
- A normalization story — NFC/NFD canonicalization, case folding, or
  homoglyph rejection. None of those exist today, and `IdentifierError`
  intentionally does not enumerate them.

Until one of those lands, "scalar cap" is a follow-up without a forcing
function and should be parked, not tracked as an open TODO.

### If the project does add a scalar cap later

It can be additive without disturbing the byte cap:

- Add `IdentifierError::TooLongScalars { kind, limit }` (or extend `TooLong`
  with a `unit: LengthUnit` discriminant — the latter is probably cleaner).
- Validate both caps; the byte cap stays as the wire/storage guarantee, the
  scalar cap stays as the human-presentation guarantee.
- The existing `MAX_IDENTIFIER_LEN = 256` byte limit is fine as-is. A scalar
  cap, when introduced, would naturally land somewhere in the 64–128 range to
  keep CJK-heavy identifiers within roughly the same visual width.

### Suggested doc tweak

Add a one-line doc comment on `MAX_IDENTIFIER_LEN` that names the unit and
the rationale: *"Maximum identifier length in raw UTF-8 bytes. Internal
identifiers are opaque slugs, not user-facing text; the cap matches transport
limits rather than display width."* That converts the current implicit
decision into something an audit can verify without rereading this thread.

## Summary

The current choice (256 raw UTF-8 bytes, `IdentifierError::TooLong { kind,
limit }`) is correct. The scalar-value follow-up should be parked rather than
tracked, and only revisited if a user-facing identifier surface or a Unicode
normalization story lands. A short doc comment naming the unit and rationale
is enough to close the open follow-up.
