use std::collections::HashMap;

use crate::log::LogEntry;
use crate::{ClientId, SeqNum, messages::*};
use crate::{CommandResult, NodeId};
use async_trait::async_trait;

pub trait RaftTypeConfig: Send + Sync + 'static {
    type StateMachine: StateMachine + Send + Sync + 'static;
    type Storage: Storage + Send + Sync + 'static;
    type Transport: Transport + Send + Sync + 'static;
}

/// WAL + term/voted_for + snapshots
#[async_trait]
pub trait Storage: Clone + Send + Sync + 'static {
    /// Сохранить текущий term и за кого голосовали
    async fn save_term(&mut self, term: u64, voted_for: Option<NodeId>) -> Result<(), String>;

    /// Загрузить term и voted_for
    async fn load_term(&self) -> Result<(u64, Option<NodeId>), String>;

    /// Дописать записи в лог (append-only)
    async fn append_log(&mut self, entries: &[LogEntry]) -> Result<(), String>;

    /// Load whole log at node's start.
    /// Must return entries only after last snapshot.
    async fn load_log(&self) -> Result<Vec<LogEntry>, String>;

    /// Truncate log up to `index`
    async fn truncate_log(&mut self, index: u64) -> Result<(), String>;

    /// Save biggest existing ClientId and last SeqNum for each existing client.
    async fn save_client_state(
        &mut self,
        last_client_id: ClientId,
        last_seq_nums: &HashMap<ClientId, SeqNum>,
    ) -> Result<(), String>;

    /// Load client state. If no data is available, return (ClientId::new(0), empty map)
    async fn load_client_state(&self) -> Result<(ClientId, HashMap<ClientId, SeqNum>), String>;

    /// Save snapshot with index and term of last entry in this snapshot.
    async fn save_snapshot(&mut self, index: u64, term: u64, data: &[u8]) -> Result<(), String>;

    // Option<(last_included_index, last_included_term, data)>
    async fn load_snapshot(&self) -> Result<Option<(u64, u64, Vec<u8>)>, String>;
}

/// Must be implemented by user
pub trait StateMachine: Send + Sync + 'static {
    /// Apply log entry. Executed when entry is committed.
    ///
    ///
    /// * `command` - bytes from LogEntry.command
    ///
    /// Returns Result of command
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, String>;

    /// Serialize current state machine
    fn snapshot(&self) -> Result<Vec<u8>, String>;

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), String>;
}

// TODO: Documentation about what exaclty these funcs must do

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send_client_command(&self, target: NodeId, req: ClientCommandRequest)
    -> CommandResult;

    async fn send_register_request(&self, target: NodeId) -> CommandResult;

    async fn send_request_vote(
        &self,
        target: NodeId,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse;

    async fn send_append_entries(
        &self,
        target: NodeId,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse;

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse;
}
