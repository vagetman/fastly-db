# Fastly Compute SQL Database

An edge SQL database running on Fastly Compute. Accepts SQL queries via HTTP POST, persists state to Fastly KV Store, and serves responses from in-memory [GlueSQL](https://gluesql.org/) across all PoPs worldwide.

## Setup

This service requires a Fastly KV Store named `sql_data` linked to the Compute service. All database state (snapshots and delta keys) is stored there.

```bash
# Create the KV store
fastly kv-store create --name sql_data

# Link it to your service (use the store ID from the previous command)
fastly resource-link create --resource-id <STORE_ID> --version latest --autoclone
```

For local development, the store is seeded from `kv-seed.json` (configured in `fastly.toml`).

## Features

- Execute SQL queries (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.) over HTTP
- Persistent storage via Fastly KV Store with automatic compaction
- Dual-snapshot redundancy for safe cross-PoP replication
- In-memory GlueSQL engine for fast query execution
- Concurrent multi-PoP coordination via CAS writes

## Architecture

### Request sandboxing

Each Fastly Compute sandbox is a short-lived WASM instance that handles up to 100 requests (configured via `with_max_requests`) with a 15-second idle timeout. The database is loaded into memory on the first request and reused across subsequent requests within the same sandbox. Between requests, the sandbox incrementally refreshes by checking for new delta keys written by other sandboxes (on other PoPs or concurrently on the same PoP). When the sandbox shuts down, it performs a final compaction to fold any remaining deltas into a snapshot.

Multiple sandboxes can run concurrently across different Fastly PoPs worldwide. Each maintains its own in-memory copy of the database and coordinates through the shared KV store using CAS writes for compaction and generation checks for cache invalidation.

### Snapshot architecture

The database is persisted to Fastly KV Store using a journal of delta keys (`__sql_<UUIDv7>`) plus compacted snapshots.

Two alternating snapshot slots (`__meta_snapshot_a` and `__meta_snapshot_b`) are maintained so that at any point in time there are two copies of the data. While one snapshot is being written or propagating across PoPs, the other is guaranteed to be fully available. On compaction, the **stale** slot (older boundary) is overwritten via CAS; on load, the **newer** slot is used and its deltas replayed. Delta keys are only deleted once both snapshots have folded them in.

### Data lifecycle & compaction

Every mutating SQL statement is appended to the KV store as a new delta key (`__sql_<UUIDv7>`). On read, the database is reconstructed by deserializing the latest snapshot and replaying any delta keys written after it.

**When compaction runs:**

- **Automatically** after a mutation, if either condition is met:
  - 50+ delta keys have accumulated, or
  - the oldest uncompacted delta is older than 5 minutes.
- **Unconditionally** when a sandbox shuts down (after serving up to 100 requests or hitting the 15 s idle timeout), to fold any remaining deltas.

**How compaction works:**

1. The full in-memory database is serialized (bincode) and written to the **stale** snapshot slot (the one with the older boundary) using a CAS (`if_generation_match`) write. This ensures exactly one sandbox wins in case of concurrent compaction across regions.
2. The written slot's metadata is updated with the boundary — the key of the newest delta folded in.
3. Delta keys older than the **older** of the two snapshot boundaries are deleted. Since both snapshots have incorporated those deltas, they are safe to remove even if one snapshot is still propagating.

**Consistency guarantees:**

- CAS writes prevent lost-update conflicts between concurrent compactors.
- The dual-snapshot design ensures at least one fully-propagated snapshot is always available, even during writes or cross-PoP replication delays.
- Sandboxes detect when another sandbox has compacted (via generation checks) and perform a full reload rather than replaying stale deltas.

## Load Testing

The `loadtest/` directory contains [wrk2](https://github.com/giltene/wrk2) scripts for benchmarking the deployed service.

### Prerequisites

```bash
brew install wrk2   # recommended — constant-rate with coordinated omission correction
# or: brew install wrk
```

### Quick start

```bash
# Seed a test table (only needed once)
./loadtest/run.sh --setup https://your-service.edgecompute.app

# Run read + write benchmarks (30s each, 50 req/s, 10 connections)
./loadtest/run.sh https://your-service.edgecompute.app

# Custom parameters
./loadtest/run.sh -d 60s -c 20 -R 100 https://your-service.edgecompute.app
```

### Individual scripts

```bash
# Read-only benchmark
wrk2 -t2 -c10 -d30s -R50 -s loadtest/read.lua https://your-service.edgecompute.app

# Write benchmark
wrk2 -t2 -c10 -d30s -R50 -s loadtest/write.lua https://your-service.edgecompute.app
```

> **Note:** Write tests append to the KV journal. Long write runs will increase cold-start times since `load_db()` replays the full journal on each request.

## Security issues

Please see [SECURITY.md](SECURITY.md) for guidance on reporting security-related issues.
