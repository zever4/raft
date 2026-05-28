use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::{Child, Command};

use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio::time::sleep;

use kv_store::{GrpcTransport, KVCommand};
use raft::client::RaftClient;
use raft::NodeId;

struct NodeProcess {
    id: NodeId,
    child: Child,
    _data_dir: TempDir,
    stdout_lines: Arc<Mutex<Vec<String>>>,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

pub struct TestCluster {
    nodes: Vec<NodeProcess>,
    addrs: HashMap<NodeId, String>,
}

impl TestCluster {
    /// Creates cluster from `count` of nodes
    pub async fn new(count: usize) -> Self {
        assert!(count >= 3, "Need at least 3 nodes for Raft majority");

        let base_port = 6000u16 + (rand::random::<u16>() % 900); // random port for each test.
        let mut addrs = HashMap::new();

        for i in 1..=count {
            addrs.insert(
                i as NodeId,
                format!("http://127.0.0.1:{}", base_port + i as u16),
            );
        }

        let mut nodes = vec![];

        for i in 1..=count {
            let node_id = i as NodeId;

            let data_dir = TempDir::new().unwrap();
            // peers and node port are created in main.rs

            let mut child = Command::new("cargo")
                .args([
                    "run",
                    "--quiet",
                    "--bin",
                    "kv_node",
                    "--",
                    &node_id.to_string(),
                    &base_port.to_string(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn kv_node");

            let stdout_lines = Arc::new(Mutex::new(Vec::new()));

            if let Some(stdout) = child.stdout.take() {
                let lines = stdout_lines.clone();
                tokio::spawn(async move {
                    let reader = BufReader::new(stdout);
                    let mut lines_stream = reader.lines();
                    while let Ok(Some(line)) = lines_stream.next_line().await {
                        let mut buf = lines.lock().await;
                        buf.push(line);
                        if buf.len() > 1000 {
                            buf.remove(0);
                        }
                    }
                });
            }

            nodes.push(NodeProcess {
                id: node_id,
                child,
                _data_dir: data_dir,
                stdout_lines: stdout_lines,
            });
        }

        sleep(Duration::from_millis(500)).await;

        let cluster = Self { nodes, addrs };

        cluster.wait_for_leader().await;

        cluster
    }

    /// Wait until leader appears in logs.
    pub async fn wait_for_leader(&self) -> NodeId {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

        while tokio::time::Instant::now() < deadline {
            for node in &self.nodes {
                let lines = node.stdout_lines.lock().await;
                for line in lines.iter().rev().take(20) {
                    // Look for "👑👑👑 [NODE X] MAJORITY VOTES"
                    if line.contains("MAJORITY VOTES GRANTED") {
                        if let Some(start) = line.find("[NODE ") {
                            if let Some(end) = line[start..].find(']') {
                                let id_str = &line[start + 6..start + end];
                                if let Ok(id) = id_str.parse::<NodeId>() {
                                    return id;
                                }
                            }
                        }
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }

        panic!(
            "No leader elected within 10 seconds. Logs:\n{}",
            self.logs_summary().await
        );
    }

    pub fn client(&self) -> RaftClient<GrpcTransport> {
        let transport = Arc::new(GrpcTransport::new(self.addrs.clone()));
        let peers: Vec<NodeId> = self.addrs.keys().cloned().collect();
        RaftClient::new(1, peers, transport)
    }

    pub async fn kill_leader(&mut self) -> NodeId {
        let leader_id = self.current_leader().await;
        self.kill_node(leader_id).await;
        leader_id
    }

    pub async fn kill_node(&mut self, id: NodeId) {
        let idx = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .expect(&format!("Node {} not found", id));

        let mut node = self.nodes.remove(idx);
        let _ = node.child.kill();
        let _ = node.child.wait();

        sleep(Duration::from_millis(200)).await;
    }

    pub async fn current_leader(&self) -> NodeId {
        for node in &self.nodes {
            let lines = node.stdout_lines.lock().await;
            for line in lines.iter().rev().take(50) {
                if line.contains("[LEADER") {
                    if let Some(start) = line.find("[LEADER ") {
                        if let Some(end) = line[start..].find(']') {
                            let id_str = &line[start + 8..start + end];
                            if let Ok(id) = id_str.parse::<NodeId>() {
                                return id;
                            }
                        }
                    }
                }
            }
        }
        self.wait_for_leader().await
    }

    pub fn alive_count(&self) -> usize {
        self.nodes.len()
    }

    pub async fn logs_summary(&self) -> String {
        let mut result = String::new();
        for node in &self.nodes {
            let lines = node.stdout_lines.lock().await;
            result.push_str(&format!("\n=== NODE {} (last 20 lines) ===\n", node.id));
            for line in lines.iter().rev().take(20) {
                result.push_str(line);
                result.push('\n');
            }
        }
        result
    }

    async fn cleanup(&mut self) {
        for node in &mut self.nodes {
            let _ = node.child.start_kill();
            let _ = node.child.wait().await;
        }
        self.nodes.clear();
    }
}

#[allow(async_fn_in_trait)]
pub trait KVClientExt {
    async fn put(&mut self, key: &str, value: &str) -> Result<(), String>;
    async fn get(&mut self, key: &str) -> Result<String, String>;
}

impl KVClientExt for RaftClient<GrpcTransport> {
    async fn put(&mut self, key: &str, value: &str) -> Result<(), String> {
        let cmd = KVCommand::Put {
            key: key.to_string(),
            value: value.to_string(),
        };
        let cfg = bincode_next::config::standard();
        let bytes = bincode_next::encode_to_vec(&cmd, cfg).map_err(|e| e.to_string())?;

        match self.send_command(bytes).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn get(&mut self, key: &str) -> Result<String, String> {
        let cmd = KVCommand::Get {
            key: key.to_string(),
        };
        let cfg = bincode_next::config::standard();
        let bytes = bincode_next::encode_to_vec(&cmd, cfg).map_err(|e| e.to_string())?;

        match self.send_command(bytes).await {
            Ok(result) => {
                String::from_utf8(result).map_err(|_| "Invalid UTF-8 in response".to_string())
            }
            Err(e) => Err(e),
        }
    }
}

// =============== TESTS ===============

#[tokio::test]
async fn test_put_get() {
    let mut cluster = TestCluster::new(3).await;

    let mut client = cluster.client();
    client.init_session().await.unwrap();

    client.put("k1", "v1").await.unwrap();
    let result = client.get("k1").await.unwrap();

    assert_eq!(result, "v1");
    cluster.cleanup().await;
}

#[tokio::test]
async fn test_leader_crash() {
    let mut cluster = TestCluster::new(3).await;

    let mut client = cluster.client();
    client.init_session().await.unwrap();

    client.put("k1", "v1").await.unwrap();

    let old_leader = cluster.kill_leader().await;
    println!("Killed leader {}", old_leader);

    sleep(Duration::from_secs(3)).await;

    let result = client.get("k1").await.unwrap();
    assert_eq!(result, "v1");

    client.put("k2", "v2").await.unwrap();
    assert_eq!(client.get("k2").await.unwrap(), "v2");
    cluster.cleanup().await;
}

#[tokio::test]
async fn test_snapshot_recovery() {
    let mut cluster = TestCluster::new(3).await;

    let mut client = cluster.client();
    client.init_session().await.unwrap();

    // threshold = 5, write 15 cmds → 3 snapshots
    for i in 0..15 {
        client
            .put(&format!("key{}", i), &format!("value{}", i))
            .await
            .unwrap();
    }

    let curr_leader = cluster.current_leader().await;
    let follower_id = cluster
        .nodes
        .iter()
        .find(|n| n.id != curr_leader)
        .unwrap()
        .id;

    cluster.kill_node(follower_id).await;
    println!("Killed follower {}", follower_id);

    sleep(Duration::from_secs(2)).await;

    assert_eq!(client.get("key14").await.unwrap(), "value14");
    cluster.cleanup().await;
}
