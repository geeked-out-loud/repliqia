# Repliqia: Vector-Clock-Based Distributed Key-Value Store

A Dynamo-inspired distributed storage system implementing causal ordering and conflict detection using vector clocks.

## Overview

Repliqia is a prototype distributed system that demonstrates:
- **Causal Ordering**: Using vector clocks to track event causality
- **Conflict Detection**: Multi-version storage for concurrent updates  
- **Eventual Consistency**: AP from CAP theorem for high availability
- **Replication**: Peer-to-peer synchronization without centralized coordination

## Architecture

```
┌─────────────────────────────────────────────┐
│              REST API (Flask)               │
├─────────────────────────────────────────────┤
│         Replication & Sync Engine           │
├─────────────────────────────────────────────┤
│    Core Node Service & Merge Strategy       │
├─────────────────────────────────────────────┤
│         Local Storage (SQLite/JSON)         │
└─────────────────────────────────────────────┘
    └── Vector Clock Engine (Foundation)
```

## Quick Start

```bash
# Setup
uv sync

# Run tests
uv run pytest

# Start a node
uv run python -m repliqia.api.server --node A --port 5000
```

## Modules

- **clock**: Vector clock implementation & comparison logic
- **storage**: Local key-value storage backends
- **core**: Node service, replication logic, merge strategies
- **replication**: Peer synchronization and conflict resolution
- **api**: Flask REST endpoints
- **utils**: Helper functions

## References

- Lamport (1978): "Time, Clocks, and the Ordering of Events in a Distributed System"
- Fidge/Mattern (1988): Vector clocks
- DeCandia et al. (2007): "Dynamo: Amazon's Highly Available Key-Value Store"

## Status

🚧 **Under Development** (v0.1.0)

- [x] Project structure & dependencies
- [x] Module 1: Vector Clock Engine
- [x] Module 2: Storage Layer (JSON + SQLite)
- [x] Module 3: Core Node Service
- [x] Module 4: Replication Engine
- [x] Module 5: REST API (conflicts, sync, quorum metadata)
- [x] Module 6: Test Suite (unit + integration)
- [ ] Multi-node runtime/demo scripts hardening
- [ ] GUI dashboard (intentionally deferred)
