pub mod client;
pub mod log;
pub mod messages;
pub mod raft;
pub mod traits;

use bincode_next::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    raft::{RaftEvent, RaftNode, State},
    traits::RaftTypeConfig,
};

pub type NodeId = u64;

/// Information about request.
///
/// `client_id` is received from register_client() function
/// `seq_num` is auto-incremented after every `RaftClient::send_command()`
/// Every entry with `seq_num` < `last_received_seq` will be deleted from cache.
#[derive(Debug)]
pub struct ClientRequest {
    pub client_id: ClientId,
    pub seq_num: SeqNum,
    pub last_received_seq: SeqNum,
    pub command: RaftCommand,
    pub response_tx: oneshot::Sender<CommandResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandResult {
    Success(Vec<u8>),
    Error(String),
    Redirect(NodeId),
}

#[derive(Debug, Clone, Serialize, Deserialize, Decode, Encode)]
pub enum RaftCommand {
    RegisterClient,
    ClientCommand(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout_min: u64, // ms
    pub election_timeout_max: u64, // ms
    pub heartbeat_interval: u64,   // ms
    /// amount of entries after which snapshot will be requested.
    pub snapshot_threshold: u64,
    pub listen_addr: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: Vec::new(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            snapshot_threshold: 500,
            listen_addr: "127.0.0.1:50051".to_string(),
        }
    }
}

impl<C: RaftTypeConfig> RaftNode<C> {
    pub async fn register_client(&self) -> CommandResult {
        if self.state != State::Leader {
            return match self.current_leader {
                Some(leader_id) => CommandResult::Redirect(leader_id),
                None => CommandResult::Error(
                    "Node is not a leader and current leader is unknown".to_string(),
                ),
            };
        }

        let (tx, rx) = oneshot::channel();

        let event = RaftEvent::ClientRequest(ClientRequest {
            client_id: ClientId::from_u64(0), // fake id for registration. Actual NodeId is > 0
            seq_num: SeqNum::from_u64(0),
            last_received_seq: SeqNum::from_u64(0),
            command: RaftCommand::RegisterClient,
            response_tx: tx,
        });

        if self.event_tx.send(event).await.is_ok() {
            match rx.await {
                Ok(result) => return result,
                Err(_) => {
                    return CommandResult::Error("Raft core dropped response channel".to_string());
                }
            }
        }

        CommandResult::Error("Node internal error or shutdown".to_string())
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Decode, Encode,
)]
pub struct ClientId(u64);

impl ClientId {
    /// Creates ClientId from u64.
    /// Use this strictly for network code.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }

    pub(crate) fn from_u64(id: u64) -> Self {
        Self(id)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Decode, Encode,
)]
pub struct SeqNum(u64);

impl SeqNum {
    /// Creates SeqNum for u64.
    /// Use this strictly for network code.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }

    pub(crate) fn from_u64(seq_num: u64) -> Self {
        Self(seq_num)
    }
}
