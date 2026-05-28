# Key-value store example

*Demonstrates a complete working cluster with gRPC transport, file storage and simple interactive CLI client.*

## Quick start

### 1. Build (optional)

```bash
cd examples/kv-store
cargo build
```

### 2. Start a 3-node cluster

Terminal 1:
```bash
cargo run --bin kv_node -- 1 6000
```

Terminal 2:
```bash
cargo run --bin kv_node -- 2 6000
```

Terminal 3:
```bash
cargo run --bin kv_node -- 3 6000
```

*1–3 are Node IDs. 6000 is a base port — actual ports will be 6001, 6002, 6003. Change if those are taken.*

### 3. Start a client

Terminal 4:
```bash
cargo run --bin kv_client
```

*Available commands:*
- **put <key> <value>**
- **get <key>**
- **exit**


# Testing

## Integration tests spawn real nodes as separate processes, test via gRPC, and verify:

- **test_put_get**: Basic write/read
- **test_leader_crash**: Failover and data persistence
- **test_snapshot_recovery**: Log compaction and snapshot streaming

## How to run tests:
```bash
cd examples/kv-store
cargo test --test integration -- --nocapture
```

# Limitations
- **Fixed 3-node topology:** no dynamic membership, but can be changed to any number before starting a cluster
- **No TLS or authentication for simplicity**
- **Storage is file-based and unoptimized**
- **Graceful shutdown**: Implemented in core (`RaftEvent::Shutdown`), but not wired to OS signals (SIGINT/SIGTERM) in the example. The example terminates via Ctrl+C without cleanup.
