use crate::messages::ClientCommandRequest;
use crate::traits::Transport;
use crate::*;
use std::sync::Arc;

pub struct RaftClient<T: Transport> {
    client_id: Option<ClientId>,

    next_seq_num: SeqNum,

    last_received_seq: SeqNum,

    current_leader: NodeId,

    peers: Vec<NodeId>,

    transport: Arc<T>,
}

impl<T: Transport> RaftClient<T> {
    pub fn new(initial_node: NodeId, peers: Vec<NodeId>, transport: Arc<T>) -> Self {
        Self {
            client_id: None,
            next_seq_num: SeqNum::from_u64(1),
            last_received_seq: SeqNum::from_u64(0),
            current_leader: initial_node,
            peers,
            transport,
        }
    }

    pub async fn init_session(&mut self) -> Result<(), String> {
        let mut attempts = 0;
        let max_attempts = self.peers.len() + 1;

        while attempts < max_attempts {
            match self
                .transport
                .send_register_request(self.current_leader)
                .await
            {
                CommandResult::Success(bytes) => {
                    // Parse ClientId from Little-Endian bytes
                    if bytes.len() == 8 {
                        let id_raw = u64::from_le_bytes(bytes.try_into().unwrap());
                        self.client_id = Some(ClientId::from_u64(id_raw));
                        self.next_seq_num = SeqNum::from_u64(1);
                        self.last_received_seq = SeqNum::from_u64(0);
                        return Ok(());
                    }
                    return Err("Invalid ClientId bytes length from leader".to_string());
                }
                CommandResult::Redirect(new_leader_id) => {
                    // send request to `new_leader_id` in next iteration
                    self.current_leader = new_leader_id;
                    attempts += 1;
                }
                CommandResult::Error(_) => {
                    self.current_leader = self.pick_next_peer();
                    attempts += 1;
                }
            }
        }
        Err("Failed to register client: Cluster unavailable".to_string())
    }

    pub async fn send_command(&mut self, cmd_bytes: Vec<u8>) -> Result<Vec<u8>, String> {
        let cid = self
            .client_id
            .ok_or("Client session is not initialized. Call init_session first.")?;

        let current_seq = self.next_seq_num;
        let mut attempts = 0;
        let max_attempts = self.peers.len() + 1;

        while attempts < max_attempts {
            let request = ClientCommandRequest {
                client_id: cid,
                seq_num: current_seq,
                last_received_seq: self.last_received_seq,
                command: cmd_bytes.clone(),
            };

            match self
                .transport
                .send_client_command(self.current_leader, request)
                .await
            {
                CommandResult::Success(response_payload) => {
                    self.last_received_seq = current_seq;
                    self.next_seq_num = SeqNum::from_u64(current_seq.0 + 1);
                    return Ok(response_payload);
                }
                CommandResult::Redirect(new_leader_id) => {
                    self.current_leader = new_leader_id;
                    attempts += 1;
                }
                CommandResult::Error(_) => {
                    self.current_leader = self.pick_next_peer();
                    attempts += 1;
                }
            }
        }
        Err("Command execution failed: Cluster leader timeout or split-brain".to_string())
    }

    fn pick_next_peer(&self) -> NodeId {
        let current_idx = self
            .peers
            .iter()
            .position(|&id| id == self.current_leader)
            .unwrap_or(0);
        let next_idx = (current_idx + 1) % self.peers.len();
        self.peers[next_idx]
    }
}
