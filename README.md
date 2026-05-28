# Raft Consensus Core

Async Raft consensus implementation in Rust. Event-driven architecture with tokio, supporting log replication, leader election, snapshots, and client deduplication.

## Architecture

- **State machine replication**: User-defined `StateMachine` trait for application logic
- **Storage**: `Storage` trait for WAL, snapshots, and metadata
- **Network**: `Transport` trait for any RPC implementation
- **Async**: Single-threaded event loop per node using `tokio::select!`


## Features

- [x] Leader election with randomized timeouts
- [x] Log replication and commit
- [x] Snapshot creation and streaming to lagging followers
- [x] Client request deduplication, based on sequential numbers
- [x] Graceful shutdown handling
- [x] Persistent storage interface (term, log, snapshots, client state)


## Known Limitations

- **Membership changes**: Dynamic cluster reconfiguration (adding/removing nodes) is not implemented. This requires Joint Consensus and is significantly more complex.
- **Read optimization**: All reads go through Raft log (no lease reads or quorum reads).
- **Exactly-once semantics**: Best-effort. If leader crashes before `save_client_state`, duplicate requests may be re-executed on retry.


## Usage

```rust
use raft::{Config, RaftNode, RaftTypeConfig};

struct MyConfig;

impl RaftTypeConfig for MyConfig {
    type StateMachine = MyStateMachine;
    type Storage = MyStorage;
    type Transport = MyTransport;
}

// Also create state_machine, storage and transport...
let (event_tx, event_rx) = tokio::sync::mpsc::channel(1024);

let mut node: RaftNode<MyConfig> = RaftNode::new(Config::default(), my_state_machine, my_storage, my_transport, event_tx.clone(), event_rx).await?;
node.run().await;
```

### For detailed example, see examples/kv-store.


## Safety Notes

This is a learning/educational project. While the core algorithm is implemented correctly, it has not been formally verified or tested under Jepsen-style fault injection. Use in production at your own risk.

## Contributing

This project is primarily for learning and demonstration. If you want to extend it, here are natural next steps:

- **Membership changes**: Implement Joint Consensus for dynamic cluster reconfiguration
- **Read optimization**: Add lease-based or quorum reads to reduce log contention
- **Deterministic simulation**: Integrate [turmoil](https://github.com/tokio-rs/turmoil) for network fault testing
- **Metrics**: Replace `println!` with `tracing` and add Prometheus endpoints

Pull requests for bug fixes and documentation improvements are welcome. For major features, please open an issue first to discuss design.
