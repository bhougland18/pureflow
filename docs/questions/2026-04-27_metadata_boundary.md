# Metadata Boundary Questions

Date: 2026-04-27

Open design points for `cdt-rpk.1`:

- Should message-boundary records distinguish send vs receive explicitly, or is a single message record with route metadata enough?
- Should `PortsIn` and `PortsOut` own an optional metadata sink, or should the runtime attach a temporary thread-local seam before invoking a node?
- Should message observations record payload size now, or defer that until the tiered payload bead?
- Should receive-side metadata be emitted only when a packet is delivered, or also when a port is observed as drained/disconnected?

Current leaning:

- Use a typed message-observation record with explicit boundary kind.
- Prefer sink attachment on the port handles over a thread-local if it can be done without changing `NodeExecutor`.
- Keep payload size out of the first bead unless it falls out naturally from the envelope shape.
