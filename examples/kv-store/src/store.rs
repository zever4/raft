use async_trait::async_trait;
use bincode_next::{Decode, Encode};
use raft::log::LogEntry;
use raft::traits::{StateMachine, Storage};
use raft::{ClientId, SeqNum};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Serialize, Deserialize, Decode, Encode)]
pub enum KVCommand {
    Put { key: String, value: String },
    Get { key: String },
}

#[allow(dead_code)]
pub struct KVStateMachine {
    db: HashMap<String, String>,
}

#[allow(dead_code)]
impl KVStateMachine {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }
}

impl StateMachine for KVStateMachine {
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, String> {
        let cfg = bincode_next::config::standard();
        let (cmd, _): (KVCommand, usize) = bincode_next::decode_from_slice(command, cfg)
            .map_err(|e| format!("StateMachine failed to decode command: {}", e))?;

        match cmd {
            KVCommand::Put { key, value } => {
                self.db.insert(key, value);
                Ok(vec![])
            }
            KVCommand::Get { key } => {
                let value = self.db.get(&key).cloned().unwrap_or_default();
                Ok(value.into_bytes())
            }
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, String> {
        let cfg = bincode_next::config::standard();
        let data = bincode_next::encode_to_vec(&self.db, cfg)
            .map_err(|e| format!("StateMachine failed to encode snapshot: {}", e))?;
        Ok(data)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), String> {
        let cfg = bincode_next::config::standard();
        let (data, _): (HashMap<String, String>, _) =
            bincode_next::decode_from_slice(snapshot, cfg)
                .map_err(|e| format!("StateMachine failed to decode snapshot: {}", e))?;
        self.db = data;
        Ok(())
    }
}

// Simple file storage for example. Not recommended for actual projects.
#[allow(dead_code)]
#[derive(Clone)]
pub struct FileStorage {
    base_dir: PathBuf,
    state_path: PathBuf,
    log_path: PathBuf,
    snapshot_path: PathBuf,
    client_state_path: PathBuf,
}

#[allow(dead_code)]
impl FileStorage {
    pub fn new(node_id: u64, base_dir: &str) -> Self {
        let base = PathBuf::from(base_dir);
        std::fs::create_dir_all(&base).unwrap();

        Self {
            base_dir: base.clone(),
            state_path: base.join(format!("node_{}_state.json", node_id)),
            log_path: base.join(format!("node_{}_log.bin", node_id)),
            snapshot_path: base.join(format!("node_{}_snapshot.bin", node_id)),
            client_state_path: base.join(format!("node_{}_clients.json", node_id)),
        }
    }

    async fn atomic_write(&self, path: &PathBuf, data: &[u8]) -> Result<(), String> {
        let temp = path.with_extension("tmp");
        let mut file = File::create(&temp)
            .await
            .map_err(|e| format!("Create temp file: {}", e))?;
        file.write_all(data)
            .await
            .map_err(|e| format!("Write temp file: {}", e))?;
        file.flush()
            .await
            .map_err(|e| format!("Flush temp file: {}", e))?;
        drop(file);

        tokio::fs::rename(&temp, path)
            .await
            .map_err(|e| format!("Atomic rename: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for FileStorage {
    // --- term / voted_for ---

    async fn save_term(&mut self, term: u64, voted_for: Option<u64>) -> Result<(), String> {
        let json = serde_json::json!({
            "term": term,
            "voted_for": voted_for,
        });
        let bytes = serde_json::to_vec(&json).map_err(|e| e.to_string())?;
        self.atomic_write(&self.state_path, &bytes).await
    }

    async fn load_term(&self) -> Result<(u64, Option<u64>), String> {
        if !self.state_path.exists() {
            return Ok((0, None));
        }
        let bytes = tokio::fs::read(&self.state_path)
            .await
            .map_err(|e| e.to_string())?;
        let json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;

        let term = json["term"].as_u64().unwrap_or(0);
        let voted_for = json["voted_for"].as_u64();

        Ok((term, voted_for))
    }

    // --- log ---

    async fn append_log(&mut self, entries: &[LogEntry]) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .await
            .map_err(|e| e.to_string())?;

        let cfg = bincode_next::config::standard();

        for entry in entries {
            let bytes = bincode_next::encode_to_vec(entry, cfg)
                .map_err(|e| format!("Bincode encode error: {}", e))?;
            file.write_u32(bytes.len() as u32)
                .await
                .map_err(|e| e.to_string())?;
            file.write_all(&bytes).await.map_err(|e| e.to_string())?;
        }

        file.flush().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn load_log(&self) -> Result<Vec<LogEntry>, String> {
        if !self.log_path.exists() {
            return Ok(vec![]);
        }

        let mut file = File::open(&self.log_path)
            .await
            .map_err(|e| e.to_string())?;
        let mut entries = Vec::new();
        let cfg = bincode_next::config::standard();

        loop {
            match file.read_u32().await {
                Ok(len) => {
                    let mut buf = vec![0u8; len as usize];
                    file.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
                    let (entry, _): (LogEntry, usize) = bincode_next::decode_from_slice(&buf, cfg)
                        .map_err(|e| format!("Bincode decode error: {}", e))?;
                    entries.push(entry);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.to_string()),
            }
        }

        Ok(entries)
    }

    async fn truncate_log(&mut self, index: u64) -> Result<(), String> {
        let entries = self.load_log().await?;
        let filtered: Vec<_> = entries.into_iter().filter(|e| e.index > index).collect();

        // Rewrite the whole file
        let mut file = File::create(&self.log_path)
            .await
            .map_err(|e| e.to_string())?;
        let cfg = bincode_next::config::standard();

        for entry in &filtered {
            let bytes = bincode_next::encode_to_vec(entry, cfg)
                .map_err(|e| format!("Bincode encode error: {}", e))?;
            file.write_u32(bytes.len() as u32)
                .await
                .map_err(|e| e.to_string())?;
            file.write_all(&bytes).await.map_err(|e| e.to_string())?;
        }

        file.flush().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    // --- client state ---

    async fn save_client_state(
        &mut self,
        last_client_id: ClientId,
        last_seq_nums: &HashMap<ClientId, SeqNum>,
    ) -> Result<(), String> {
        let map: HashMap<u64, u64> = last_seq_nums
            .iter()
            .map(|(cid, seq)| (cid.to_u64(), seq.to_u64()))
            .collect();

        let json = serde_json::json!({
            "last_client_id": last_client_id.to_u64(),
            "seq_nums": map,
        });

        let bytes = serde_json::to_vec(&json).map_err(|e| e.to_string())?;
        self.atomic_write(&self.client_state_path, &bytes).await
    }

    async fn load_client_state(&self) -> Result<(ClientId, HashMap<ClientId, SeqNum>), String> {
        if !self.client_state_path.exists() {
            return Ok((ClientId::new(0), HashMap::new()));
        }

        let bytes = tokio::fs::read(&self.client_state_path)
            .await
            .map_err(|e| e.to_string())?;
        let json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;

        let last_client_id = ClientId::new(json["last_client_id"].as_u64().unwrap_or(0));

        let mut seq_nums = HashMap::new();
        if let Some(map) = json["seq_nums"].as_object() {
            for (k, v) in map {
                let cid = ClientId::new(k.parse::<u64>().map_err(|e| e.to_string())?);
                let seq = SeqNum::new(v.as_u64().unwrap_or(0));
                seq_nums.insert(cid, seq);
            }
        }

        Ok((last_client_id, seq_nums))
    }

    // --- snapshot ---

    async fn save_snapshot(&mut self, index: u64, term: u64, data: &[u8]) -> Result<(), String> {
        let mut buf = Vec::with_capacity(24 + data.len());
        buf.extend_from_slice(&index.to_le_bytes());
        buf.extend_from_slice(&term.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
        buf.extend_from_slice(data);
        self.atomic_write(&self.snapshot_path, &buf).await
    }

    async fn load_snapshot(&self) -> Result<Option<(u64, u64, Vec<u8>)>, String> {
        if !self.snapshot_path.exists() {
            return Ok(None);
        }
        let bytes = tokio::fs::read(&self.snapshot_path)
            .await
            .map_err(|e| e.to_string())?;
        if bytes.len() < 24 {
            return Ok(None);
        }
        let index = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let term = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let data_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let data = bytes[24..24 + data_len].to_vec();
        Ok(Some((index, term, data)))
    }
}
