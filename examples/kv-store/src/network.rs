use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use raft::messages::{self as core_msg, InstallSnapshotRequest, InstallSnapshotResponse};
use raft::raft::RaftEvent;
use raft::traits::Transport;
use raft::{CommandResult, NodeId};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("raft_kv");
}

use proto::raft_network_client::RaftNetworkClient;

use crate::network::proto::client_network_client::ClientNetworkClient;
use crate::network::proto::raft_network_server::RaftNetwork;
use crate::network::proto::{
    GAppendEntriesRequest, GAppendEntriesResponse, GInstallSnapshotRequest,
    GInstallSnapshotResponse, GRequestVoteRequest, GRequestVoteResponse,
};

pub struct GrpcTransport {
    /// Adressess of peer nodes in cluster: NodeId -> "http://127.0.0.1:5001"
    peer_addresses: std::collections::HashMap<NodeId, String>,
    peer_channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
    client_channels: Arc<RwLock<HashMap<NodeId, Channel>>>,
}

impl GrpcTransport {
    pub fn new(addresses: HashMap<NodeId, String>) -> Self {
        Self {
            peer_addresses: addresses,
            peer_channels: Arc::new(RwLock::new(HashMap::new())),
            client_channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_peer_channel(&self, target: NodeId) -> Result<Channel, String> {
        {
            let channels = self.peer_channels.read().unwrap();
            if let Some(ch) = channels.get(&target) {
                return Ok(ch.clone());
            }
        }

        // First connection
        let addr = self
            .peer_addresses
            .get(&target)
            .ok_or_else(|| format!("Unknown target node address for ID {:?}", target))?;

        let channel = Channel::from_shared(addr.clone())
            .map_err(|e| format!("Invalid URI for node {}: {}", target, e))?
            .connect()
            .await
            .map_err(|e| format!("gRPC connection failed to {}: {}", target, e))?;

        let mut channels = self.peer_channels.write().unwrap();
        channels.insert(target, channel.clone());
        Ok(channel)
    }

    async fn get_client_channel(&self, target: NodeId) -> Result<Channel, String> {
        {
            let channels = self.client_channels.read().unwrap();
            if let Some(ch) = channels.get(&target) {
                return Ok(ch.clone());
            }
        }

        let addr = self
            .peer_addresses
            .get(&target)
            .ok_or_else(|| format!("Unknown target node address for ID {}", target))?;

        let channel = Channel::from_shared(addr.clone())
            .map_err(|e| format!("Invalid URI for node {}: {}", target, e))?
            .connect()
            .await
            .map_err(|e| format!("gRPC client connection failed to {}: {}", target, e))?;

        let mut channels = self.client_channels.write().unwrap();
        channels.insert(target, channel.clone());
        Ok(channel)
    }

    async fn connect_to_peer(&self, target: NodeId) -> Result<RaftNetworkClient<Channel>, String> {
        let channel = self.get_peer_channel(target).await?;
        Ok(RaftNetworkClient::new(channel))
    }

    async fn connect_to_client_interface(
        &self,
        target: NodeId,
    ) -> Result<ClientNetworkClient<Channel>, String> {
        let channel = self.get_client_channel(target).await?;
        Ok(ClientNetworkClient::new(channel))
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn send_request_vote(
        &self,
        target: NodeId,
        req: core_msg::RequestVoteRequest,
    ) -> core_msg::RequestVoteResponse {
        let mut client = match self.connect_to_peer(target).await {
            Ok(c) => c,
            Err(_) => {
                // if couldnt connect to node, send `false`
                return core_msg::RequestVoteResponse {
                    term: req.term,
                    vote_granted: false,
                };
            }
        };

        let grpc_req = proto::GRequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        match client.request_vote(grpc_req).await {
            Ok(grpc_resp) => {
                let resp = grpc_resp.into_inner();
                core_msg::RequestVoteResponse {
                    term: resp.term,
                    vote_granted: resp.vote_granted,
                }
            }
            // if network error happened also send `false`
            Err(_) => core_msg::RequestVoteResponse {
                term: req.term,
                vote_granted: false,
            },
        }
    }

    async fn send_append_entries(
        &self,
        target: NodeId,
        req: core_msg::AppendEntriesRequest,
    ) -> core_msg::AppendEntriesResponse {
        let mut client = match self.connect_to_peer(target).await {
            Ok(c) => c,
            Err(_) => {
                return core_msg::AppendEntriesResponse {
                    term: req.term,
                    success: false,
                    conflict_index: 0,
                    last_log_index: 0,
                }
            }
        };

        let cfg = bincode_next::config::standard();
        match bincode_next::encode_to_vec(&req.entries, cfg) {
            Ok(serialized_entries) => {
                let grpc_req = proto::GAppendEntriesRequest {
                    term: req.term,
                    leader_id: req.leader_id,
                    prev_log_index: req.prev_log_index,
                    prev_log_term: req.prev_log_term,
                    serialized_entries,
                    leader_commit: req.leader_commit,
                };

                match client.append_entries(grpc_req).await {
                    Ok(grpc_resp) => {
                        let resp = grpc_resp.into_inner();
                        core_msg::AppendEntriesResponse {
                            term: resp.term,
                            success: resp.success,
                            conflict_index: resp.conflict_index,
                            last_log_index: resp.last_log_index,
                        }
                    }
                    Err(_) => core_msg::AppendEntriesResponse {
                        term: req.term,
                        success: false,
                        conflict_index: 0,
                        last_log_index: 0,
                    },
                }
            }
            Err(_) => {
                return core_msg::AppendEntriesResponse {
                    term: req.term,
                    success: false,
                    conflict_index: 0,
                    last_log_index: 0,
                };
            }
        }
    }

    async fn send_register_request(&self, target: NodeId) -> CommandResult {
        let mut client = match self.connect_to_client_interface(target).await {
            Ok(c) => c,
            Err(e) => return CommandResult::Error(format!("Network unreachable: {}", e)),
        };

        let grpc_req = proto::GRegisterClientRequest {};

        match client.register_client(grpc_req).await {
            Ok(grpc_resp) => {
                let reply = grpc_resp.into_inner();
                match reply.result {
                    Some(proto::g_client_reply::Result::SuccessData(bytes)) => {
                        CommandResult::Success(bytes)
                    }
                    Some(proto::g_client_reply::Result::RedirectLeaderId(leader_id)) => {
                        CommandResult::Redirect(leader_id)
                    }
                    Some(proto::g_client_reply::Result::ErrorMsg(err)) => CommandResult::Error(err),
                    None => {
                        CommandResult::Error("Received empty response from cluster node".into())
                    }
                }
            }
            Err(status) => CommandResult::Error(format!("gRPC error: {}", status.message())),
        }
    }

    async fn send_client_command(
        &self,
        target: NodeId,
        req: core_msg::ClientCommandRequest,
    ) -> CommandResult {
        let mut client = match self.connect_to_client_interface(target).await {
            Ok(c) => c,
            Err(e) => return CommandResult::Error(format!("Network unreachable: {}", e)),
        };

        let grpc_req = proto::GClientCommandRequest {
            client_id: req.client_id.to_u64(),
            seq_num: req.seq_num.to_u64(),
            last_received_seq: req.last_received_seq.to_u64(),
            command_payload: req.command,
        };

        match client.send_command(grpc_req).await {
            Ok(grpc_resp) => {
                let reply = grpc_resp.into_inner();
                match reply.result {
                    Some(proto::g_client_reply::Result::SuccessData(bytes)) => {
                        CommandResult::Success(bytes)
                    }
                    Some(proto::g_client_reply::Result::RedirectLeaderId(leader_id)) => {
                        CommandResult::Redirect(leader_id)
                    }
                    Some(proto::g_client_reply::Result::ErrorMsg(err)) => CommandResult::Error(err),
                    None => {
                        CommandResult::Error("Received empty response from cluster node".into())
                    }
                }
            }
            Err(status) => CommandResult::Error(format!("gRPC error: {}", status.message())),
        }
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let mut client = match self.connect_to_peer(target).await {
            Ok(c) => c,
            Err(_) => {
                return InstallSnapshotResponse {
                    term: req.term,
                    success: false,
                }
            }
        };

        let grpc_req = proto::GInstallSnapshotRequest {
            term: req.term,
            leader_id: req.leader_id,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data: req.data,
        };

        match tokio::time::timeout(Duration::from_secs(30), client.install_snapshot(grpc_req)).await
        {
            Ok(Ok(grpc_resp)) => {
                let reply = grpc_resp.into_inner();
                return InstallSnapshotResponse {
                    term: reply.term,
                    success: reply.success,
                };
            }
            Ok(Err(e)) => {
                eprintln!("send_install_snapshot error: {}", e);
                return InstallSnapshotResponse {
                    term: req.term,
                    success: false,
                };
            }
            Err(e) => {
                eprintln!("send_install_snapshot timeout: {}", e);
                return InstallSnapshotResponse {
                    term: req.term,
                    success: false,
                };
            }
        }
    }
}

pub struct RaftServerImpl {
    event_tx: mpsc::Sender<RaftEvent>,
}

#[allow(dead_code)]
impl RaftServerImpl {
    pub fn new(event_tx: mpsc::Sender<RaftEvent>) -> Self {
        Self { event_tx }
    }
}

#[tonic::async_trait]
impl RaftNetwork for RaftServerImpl {
    async fn request_vote(
        &self,
        request: Request<GRequestVoteRequest>,
    ) -> Result<Response<GRequestVoteResponse>, Status> {
        let req = request.into_inner();

        let core_req = core_msg::RequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let event = RaftEvent::VoteRequestReceived(core_req, tx);
        if self.event_tx.send(event).await.is_err() {
            return Err(Status::internal("Raft leader is not running"));
        }

        match rx.await {
            Ok(RaftEvent::VoteResponse(core_resp)) => Ok(Response::new(GRequestVoteResponse {
                term: core_resp.term,
                vote_granted: core_resp.vote_granted,
            })),
            _ => Err(Status::internal("Raft core failed to process vote request")),
        }
    }

    async fn append_entries(
        &self,
        request: Request<GAppendEntriesRequest>,
    ) -> Result<Response<GAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        // decode log entries
        let cfg = bincode_next::config::standard();
        let (core_entries, _len): (Vec<raft::log::LogEntry>, usize) =
            bincode_next::decode_from_slice(&req.serialized_entries, cfg)
                .map_err(|e| Status::invalid_argument(format!("Bincode decode failed: {}", e)))?;

        let core_req = core_msg::AppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: core_entries,
            leader_commit: req.leader_commit,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let event = RaftEvent::AppendEntriesReceived(core_req, tx);
        if self.event_tx.send(event).await.is_err() {
            return Err(Status::internal("Raft leader is not running"));
        }

        match rx.await {
            Ok(core_resp) => Ok(Response::new(GAppendEntriesResponse {
                term: core_resp.term,
                success: core_resp.success,
                conflict_index: core_resp.conflict_index,
                last_log_index: core_resp.last_log_index,
            })),
            Err(_) => Err(Status::internal(
                "Raft core failed to process append entries",
            )),
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<GInstallSnapshotRequest>,
    ) -> Result<Response<GInstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let core_req = InstallSnapshotRequest {
            term: req.term,
            leader_id: req.leader_id,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data: req.data,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let event = RaftEvent::InstallSnapshotReceived(core_req, tx);
        if self.event_tx.send(event).await.is_err() {
            return Err(Status::internal("Receiver node is not running."));
        }

        match rx.await {
            Ok(core_resp) => Ok(Response::new(GInstallSnapshotResponse {
                term: core_resp.term,
                success: core_resp.success,
            })),
            Err(_) => Err(Status::internal("Raft core failed to install snapshot.")),
        }
    }
}

use proto::client_network_server::ClientNetwork;
use proto::{GClientCommandRequest, GClientReply, GRegisterClientRequest};

pub struct ClientServerImpl {
    event_tx: mpsc::Sender<RaftEvent>,
}

#[allow(dead_code)]
impl ClientServerImpl {
    pub fn new(event_tx: mpsc::Sender<RaftEvent>) -> Self {
        Self { event_tx }
    }
}

#[tonic::async_trait]
impl ClientNetwork for ClientServerImpl {
    async fn register_client(
        &self,
        _request: Request<GRegisterClientRequest>,
    ) -> Result<Response<GClientReply>, Status> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let event = RaftEvent::ClientRequest(raft::ClientRequest {
            client_id: raft::ClientId::new(0),
            seq_num: raft::SeqNum::new(0),
            last_received_seq: raft::SeqNum::new(0),
            command: raft::RaftCommand::RegisterClient,
            response_tx: tx,
        });

        if self.event_tx.send(event).await.is_err() {
            return Err(Status::internal("Raft core is offline"));
        }

        match rx.await {
            Ok(core_res) => match core_res {
                CommandResult::Success(bytes) => Ok(Response::new(GClientReply {
                    result: Some(proto::g_client_reply::Result::SuccessData(bytes)),
                })),
                CommandResult::Redirect(leader_id) => Ok(Response::new(GClientReply {
                    result: Some(proto::g_client_reply::Result::RedirectLeaderId(leader_id)),
                })),
                CommandResult::Error(err) => Ok(Response::new(GClientReply {
                    result: Some(proto::g_client_reply::Result::ErrorMsg(err)),
                })),
            },
            Err(_) => Err(Status::internal("Internal channel dropped")),
        }
    }

    async fn send_command(
        &self,
        request: Request<GClientCommandRequest>,
    ) -> Result<Response<GClientReply>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::oneshot::channel();

        let event = RaftEvent::ClientRequest(raft::ClientRequest {
            client_id: raft::ClientId::new(req.client_id),
            seq_num: raft::SeqNum::new(req.seq_num),
            last_received_seq: raft::SeqNum::new(req.last_received_seq),
            command: raft::RaftCommand::ClientCommand(req.command_payload),
            response_tx: tx,
        });

        if self.event_tx.send(event).await.is_err() {
            return Err(Status::internal("Raft core is offline"));
        }

        match rx.await {
            Ok(core_res) => match core_res {
                CommandResult::Success(bytes) => Ok(Response::new(GClientReply {
                    result: Some(proto::g_client_reply::Result::SuccessData(bytes)),
                })),
                CommandResult::Redirect(leader_id) => Ok(Response::new(GClientReply {
                    result: Some(proto::g_client_reply::Result::RedirectLeaderId(leader_id)),
                })),
                CommandResult::Error(err) => Ok(Response::new(GClientReply {
                    result: Some(proto::g_client_reply::Result::ErrorMsg(err)),
                })),
            },
            Err(_) => Err(Status::internal("Internal channel dropped")),
        }
    }
}
