# Pureflow crates.io Publishing Options

Date: 2026-05-21

## Purpose

This proposal compares the realistic crates.io publishing strategies for
`pureflow` and recommends the least risky path for adoption.

The current repository is a multi-crate Cargo workspace with internal
`path`-based dependencies and a non-publishable workspace baseline. That is a
good internal architecture, but it is not the same thing as a public crates.io
consumption model.

The question here is not whether the code should stay modular internally.
It should.

The question is what the public boundary should be.

## Option 1: Publish One Public Facade Crate

### Shape

Publish a single public crate, likely named `pureflow`, that is the only crate
users are expected to depend on.

The internal `pureflow-*` crates remain workspace implementation details or are
folded into modules inside the facade crate.

### Why This Works

- Users get one dependency instead of a crate graph.
- Semver becomes simpler.
- The public docs surface is smaller and easier to understand.
- Optional integrations can be hidden behind features or separate add-on crates
  later.

### Risks

- The repo needs a consolidation step before publication.
- The public crate must not depend on unpublished path crates.
- The facade can become bloated if the internal boundaries are not cleaned up
  carefully.
- A badly designed umbrella crate can hide ownership boundaries instead of
  clarifying them.

### Best Use Case

This is the best option when the public goal is:

- one thing for users to learn
- one crate to version
- one crate to publish
- optional integrations added later as separate add-ons

## Option 2: Publish Multiple Public Crates

### Shape

Publish the current `pureflow-*` crates separately, in dependency order.

Typical public split:

- `pureflow-types`
- `pureflow-workflow`
- `pureflow-workflow-format`
- `pureflow-core`
- `pureflow-contract`
- `pureflow-runtime`
- `pureflow-introspection`
- `pureflow-engine`
- `pureflow-wasm`

Optional integrations would become their own crates, such as
`pureflow-guardiandb`.

### Why This Works

- It matches the internal ownership boundaries already present in the repo.
- Optional integrations can stay optional.
- Small, focused crates can be reused independently.
- The architecture maps cleanly to Polylith-style component thinking.

### Risks

- Adoption cost is higher because users must pick a subset of crates.
- Version coordination becomes a real maintenance burden.
- Publishing order matters and must be preserved.
- Some crates will be awkward on their own, especially the ones that are mainly
  support layers.

### Best Use Case

This is the best option when:

- downstream users actually want to compose against the individual boundaries
- integrations are meant to be optional plugin-style crates
- the team is willing to maintain several public APIs

## Option 3: Keep The Workspace Internal For Now

### Shape

Do not publish to crates.io yet.

Keep the workspace private, use the crates internally, and defer public release
until the public API boundary is clearer.

### Why This Works

- No premature public API commitment.
- No crates.io maintenance burden yet.
- The team can continue iterating on the architecture.
- Internal refactors remain easy.

### Risks

- No public distribution.
- No external adoption.
- The repo can delay a real packaging decision indefinitely.

### Best Use Case

This is the best option when the code is still changing too much to justify a
public API or when the team does not yet know what the public boundary should
be.

## Recommendation

My recommendation is Option 1: publish one public facade crate, not the current
set of internal workspace crates.

Reasoning:

- You were right to be concerned that people will not want to pull in all the
  crates.
- A single public crate matches the likely adoption model much better.
- Optional integrations like Guardiandb should become separate add-on crates
  later, not part of the core public dependency story.
- The current workspace can still stay modular internally, which preserves the
  engineering value of the split without forcing users to consume it.

## What Needs To Happen For The Recommendation

1. Create a real public facade crate, likely named `pureflow`.
2. Decide which code belongs in that public API and which code remains internal.
3. Keep the current `pureflow-*` crates private or fold them into modules.
4. Add optional integration crates only after the core public crate is stable.
5. Publish the facade crate only after the public API is stable enough for
   semver.

## Decision Rule

Use this rule of thumb:

- if the code is core workflow shape or runtime behavior, it belongs in the
  facade
- if the code is optional integration logic, it belongs in a separate plugin or
  add-on crate
- if the code is only helping internal organization, keep it private

## Final Position

The repo should not be published to crates.io as a collection of many public
workspace crates.

It should be published as one public crate with optional add-ons later, if and
when the add-ons prove valuable.
