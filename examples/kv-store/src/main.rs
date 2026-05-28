use raft::traits::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use raft::{raft::RaftNode, Config, NodeId};
use tonic::transport::Server;

pub mod network;
pub mod store;

use network::{proto, ClientServerImpl, GrpcTransport, RaftServerImpl};
use store::{FileStorage, KVStateMachine};

pub struct KVClusterConfig;

// Impl RaftTypeConfig using implemented traits to create RaftNode later.
impl RaftTypeConfig for KVClusterConfig {
    type StateMachine = KVStateMachine;
    type Transport = GrpcTransport;
    type Storage = FileStorage;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run --bin -- kv_node <node_id> [base_port]");
        std::process::exit(1);
    }

    let current_node_id: NodeId = args[1].parse().expect("Node ID must be a valid u64");
    let base_port: u64 = args
        .get(2)
        .map(|s| s.parse().expect("base_port must be > 0"))
        .unwrap_or(5000);

    // There's expected to by exactly 3 nodes with ids 1, 2, 3
    let mut cluster_topology = HashMap::new();
    for i in 1..=3 {
        cluster_topology.insert(i, format!("http://127.0.0.1:{}", base_port + i));
    }

    let current_addr_str = cluster_topology
        .get(&current_node_id)
        .expect("Node ID not found in topology");

    let socket_addr: SocketAddr = current_addr_str.replace("http://", "").parse()?;

    let peers: Vec<NodeId> = cluster_topology
        .keys()
        .cloned()
        .filter(|&id| id != current_node_id)
        .collect();

    println!("Starting Raft Node {} on {}", current_node_id, socket_addr);

    // Init Node config.
    // Recommended to use same timeout and heartbeat values on each node
    // otherwise cluster might not work correctly.
    let config = Config {
        node_id: current_node_id,
        peers: peers.clone(),
        election_timeout_min: 500,  // ms
        election_timeout_max: 1000, // ms
        heartbeat_interval: 300,    // ms
        snapshot_threshold: 5,
        listen_addr: current_addr_str.clone(),
    };

    // Create channel for communication with Leader node.
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(1024);

    // Log will be saved in raft_data/...
    let storage = FileStorage::new(current_node_id, &format!("./raft_data_{}", base_port));

    let state_machine = KVStateMachine::new();
    let transport = Arc::new(GrpcTransport::new(cluster_topology));

    // Create a new node.
    let mut raft_node: RaftNode<KVClusterConfig> = RaftNode::new(
        config,
        state_machine,
        storage,
        transport,
        event_tx.clone(), // clone so we can send events later. Otherwise node will be unreachable.
        event_rx,
    )
    .await?;

    // Create gRPC network using `event_tx`
    let raft_network_service = RaftServerImpl::new(event_tx.clone());
    let client_network_service = ClientServerImpl::new(event_tx.clone());

    let raft_server = proto::raft_network_server::RaftNetworkServer::new(raft_network_service);
    let client_server =
        proto::client_network_server::ClientNetworkServer::new(client_network_service);

    // Start gRPC server on node's address
    tokio::spawn(async move {
        println!("gRPC Server listening on {}", socket_addr);
        Server::builder()
            .add_service(raft_server)
            .add_service(client_server)
            .serve(socket_addr)
            .await
            .expect("gRPC server failed to start");
    });

    println!("Raft Core started.");
    raft_node.run().await;

    Ok(())
}
