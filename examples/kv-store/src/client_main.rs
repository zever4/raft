use raft::client::RaftClient;
use raft::NodeId;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;

mod network;
mod store;

use network::GrpcTransport;
use store::KVCommand;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== KV RAFT INITIALIZATION ===");

    let mut cluster_topology = HashMap::new();
    cluster_topology.insert(1, "http://127.0.0.1:5001".to_string());
    cluster_topology.insert(2, "http://127.0.0.1:5002".to_string());
    cluster_topology.insert(3, "http://127.0.0.1:5003".to_string());

    let peers: Vec<NodeId> = cluster_topology.keys().cloned().collect();

    let transport = Arc::new(GrpcTransport::new(cluster_topology));

    // Start from node 1, if it's not a leader Redirect will happen.
    let mut raft_client = RaftClient::new(1, peers, transport);

    println!("Connecting to cluster...");
    if let Err(e) = raft_client.init_session().await {
        eprintln!("Session init error: {}", e);
        std::process::exit(1);
    }
    println!("Connected successfully!");

    println!("\nAvailable commands:");
    println!("  put <key> <value> - Insert value");
    println!("  get <key>         - Get value");
    println!("  exit              - Exit");
    println!("--------------------------------------");

    let bincode_cfg = bincode_next::config::standard();
    let stdin = io::stdin();
    let mut input = String::new();

    loop {
        print!("kv_raft> ");
        io::stdout().flush()?;
        input.clear();

        if stdin.read_line(&mut input)? == 0 {
            break; // Ctrl+D
        }

        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "exit" => break,

            "put" => {
                if parts.len() < 3 {
                    println!(" Error: Expected syntax: put <key> <value>");
                    continue;
                }
                let key = parts[1].to_string();
                let value = parts[2..].join(" ");

                let kv_cmd = KVCommand::Put { key, value };
                let cmd_bytes = bincode_next::encode_to_vec(&kv_cmd, bincode_cfg)?;

                // send to Raft server
                println!("Sending PUT command...");
                match raft_client.send_command(cmd_bytes).await {
                    Ok(_) => println!(" Success: Value saved and replicated!"),
                    Err(e) => println!(" Execution error: {}", e),
                }
            }

            "get" => {
                if parts.len() < 2 {
                    println!(" Error: Expected syntax: get <key>");
                    continue;
                }
                let key = parts[1].to_string();

                let kv_cmd = KVCommand::Get { key };
                let cmd_bytes = bincode_next::encode_to_vec(&kv_cmd, bincode_cfg)?;

                println!("Sending GET command...");
                match raft_client.send_command(cmd_bytes).await {
                    Ok(response_bytes) => {
                        let value = String::from_utf8(response_bytes)
                            .unwrap_or_else(|_| "<Invalid UTF-8>".to_string());

                        if value.is_empty() {
                            println!(" Result: key not found");
                        } else {
                            println!(" Result: {} = {}", parts[1], value);
                        }
                    }
                    Err(e) => println!(" Execution error: {}", e),
                }
            }

            _ => {
                println!(" Unknown command.");
            }
        }
    }

    Ok(())
}
