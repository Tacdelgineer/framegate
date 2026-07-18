# Content Factory

Monorepo for the VPS news pipeline and the Tailscale job queue used by its DGX
media worker.

## Layout

- `pipeline/` — crash-resumable news-to-video pipeline.
- `queue/` — FastAPI and SQLite coordination service plus its Python client.
- `worker-dgx/` — synchronization point for the separately managed DGX worker.
- `deploy/systemd/` — deployed user-service unit files.
- `docs/ARCHITECTURE.md` — machines, network boundaries, data flow, and models.

The queue and Hermes Web UI listen only on the VPS Tailscale address. See the
architecture document before changing binds, ports, or firewall policy.
