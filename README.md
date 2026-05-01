# Goldspade

Enterprise-grade compliant RPA scaffold for workflow orchestration and reliable task dispatch.

Current version: `1.0.2`

## Layout

- `docs/` - architecture and PRD notes
- `python/` - workflow orchestrator and rules hot reload
- `go/` - reliable execution node
- `shared/` - SQLite schema and shared definitions
- `scripts/` - local startup and deployment helpers

## Next steps

- Wire the Python orchestrator to Redis
- Wire the Go executor to the task queue
- Add domain-specific rules to `rules.json`
- Deploy the first server-side release
