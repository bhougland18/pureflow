# Contracts Core Questions

## Assumptions Made

- `NodeContract` uses `NodeId` directly instead of a separate contract-id wrapper.
- `SchemaRef` is an opaque string handle for now.
- Schema compatibility is treated as exact equality when both sides declare a schema.
- Capability descriptors stay in `pureflow-core`; `pureflow-contract` validates against them instead of redefining them.

## Open Follow-Ups

- Should schema compatibility stay exact-equality, or should we add a compatibility policy object before workflow-format parsing lands?
- Should contract validation require every node to have a capability descriptor, or is missing capability metadata acceptable for some native-only workflows?
