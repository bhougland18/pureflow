# Pureflow Handoff - 2026-04-24 (Late)

## Current State

The current working copy contains the runtime-bootstrap continuation work:

- `cdt-rtb.1` - runtime lifecycle observation seam
- `cdt-rtb.2` - asupersync runtime skeleton

The most recent JJ description is:

- `cdt-rtb.1+2: lifecycle seam and asupersync runtime skeleton`

The repository is in a healthy state after the latest validation runs, with the usual known `dylint_driver` prelude still appearing before the real checks complete.

## Important Docs

Read these first when resuming:

- [docs/AGENTS.md](/home/ben/code/pureflow/docs/AGENTS.md)
- [docs/pureflow_proposal.md](/home/ben/code/pureflow/docs/pureflow_proposal.md)
- [docs/archive/handoff_2026-04-24_evening.md](/home/ben/code/pureflow/docs/archive/handoff_2026-04-24_evening.md)
- [docs/audits/Audit_scope.md](/home/ben/code/pureflow/docs/audits/Audit_scope.md)
- [docs/audits/Audit_4_23.md](/home/ben/code/pureflow/docs/audits/Audit_4_23.md)
- [docs/epics/epic-1-foundation.md](/home/ben/code/pureflow/docs/epics/epic-1-foundation.md)

## Topics To Discuss

1. Should node capabilities grow to include explicit logging and tracing permissions, or should those remain part of the runtime and metadata layers instead of node capability descriptors?
2. Should metadata stay split across context, message, and lifecycle types, or do we want a first-class metadata collection/sink API next?
3. Should the runtime surface a dedicated introspection API for node contracts, capabilities, and execution metadata before the next major runtime bead?
4. Should `asupersync` remain only a thin bootstrap wrapper for now, or should the next runtime bead start shaping task-tree orchestration more directly?
5. Should the current effect taxonomy be expanded to cover observability concerns, or should observability stay outside `EffectCapability` altogether?

## Resolved Direction

Decision after resuming:

- Logging, tracing, and routine runtime telemetry stay outside `EffectCapability`.
- Metadata remains split across context, message, and lifecycle source types.
- A first-class metadata sink API is the collection boundary, not a replacement metadata model.
- `asupersync` remains a thin bootstrap wrapper until lifecycle and metadata observation are stable.
- Introspection should come before deeper task-tree orchestration.
- `asupersync` task context, channel, permit, and join types should stay behind Pureflow-owned adapters unless a future bead explicitly changes the public FBP boundary.

## Suggested Resume Point

1. Review the important docs above.
2. Review the metadata sink and lifecycle observer wiring in `pureflow-core` and `pureflow-runtime`.
3. Continue with `cdt-rtb.4` for bounded async port handles, or `cdt-rtb.8` for deterministic runtime tests.
