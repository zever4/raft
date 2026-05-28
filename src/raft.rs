use crate::log::{Log, LogEntry};
use crate::messages::*;
use crate::traits::*;
use crate::{ClientId, ClientRequest, CommandResult, Config, NodeId, RaftCommand, SeqNum};
use core::panic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct LeaderState {
    /// For each follower: next index for send
    pub next_index: HashMap<NodeId, u64>,

    /// For each follower: last replicated index
    pub match_index: HashMap<NodeId, u64>,

    // NOTE:
    // pending_clients and client_results hashmaps may grow infinitely
    // might be a good idea to delete old records from them
    // e.g. if seq_num = 10, then we can delete all records with seq_num < 10 for this client
    //
    /// Requests that haven't finished yet.
    pub pending_clients: HashMap<(ClientId, SeqNum), oneshot::Sender<CommandResult>>,

    /// Client requests deduplication: (`client_id`, `seq_num`) -> `CommandResult`
    pub client_results: HashMap<(ClientId, SeqNum), CommandResult>,

    // NOTE:
    // If it'd be allowed to send new requests before last one was received by client,
    // deleting earlier results will be incorrect
    //
    /// Last `seq_num` of each client.
    /// All results with `seq_num` less than this one are considered
    /// to be received by client.
    pub last_seq_nums: HashMap<ClientId, SeqNum>,

    /// Incremented after new client was registered.
    /// It's impossible to have 2 clients with the same id
    /// because only leader can register clients
    pub last_client_id: ClientId,
}

pub struct RaftNode<C: RaftTypeConfig> {
    pub config: Config,
    pub state: State,
    pub current_term: u64,
    pub voted_for: Option<NodeId>, // `None` if didn't vote in `current_term`
    pub log: Log,

    /// Index of  last Entry, committed to the StateMachine
    pub commit_index: u64,

    /// Index of last Entry, applied to the StateMachine
    pub last_applied: u64,

    /// None if node is not a leader
    pub leader_state: Option<LeaderState>,
    pub current_leader: Option<NodeId>,

    pub state_machine: Arc<std::sync::RwLock<C::StateMachine>>,
    pub storage: C::Storage,
    pub transport: Arc<C::Transport>,

    pub event_tx: mpsc::Sender<RaftEvent>,
    pub event_rx: mpsc::Receiver<RaftEvent>,
}

#[derive(Debug)]
pub enum RaftEvent {
    ClientRequest(ClientRequest),

    VoteRequestReceived(RequestVoteRequest, oneshot::Sender<RaftEvent>),
    VoteResponse(RequestVoteResponse),

    AppendEntriesResponse(AppendEntriesResponse, NodeId, u64),
    AppendEntriesReceived(AppendEntriesRequest, oneshot::Sender<AppendEntriesResponse>),

    InstallSnapshotReceived(
        InstallSnapshotRequest,
        oneshot::Sender<InstallSnapshotResponse>,
    ),
    // (last_included_index, last_included_term)
    SnapshotCompleted(u64, u64),
    // Response from follower. u64 - `last_included_index` in snapshot
    InstallSnapshotResponseReceived(InstallSnapshotResponse, NodeId, u64),

    // Signal to shutdown the node.
    // Could be called by sending RaftEvent::Shutdown to node.
    // There might be any reason to send it: SIGTERM, SIGINT(ctrl+c), etc.
    Shutdown,
}

impl<C: RaftTypeConfig> RaftNode<C> {
    pub async fn new(
        config: Config,
        mut state_machine: C::StateMachine,
        storage: C::Storage,
        transport: Arc<C::Transport>,
        event_tx: mpsc::Sender<RaftEvent>,
        event_rx: mpsc::Receiver<RaftEvent>,
    ) -> Result<Self, String> {
        // Load persisted state
        let (term, voted_for) = storage.load_term().await?;

        let mut log = Log::new();
        let mut commit_index = 0u64;
        let mut last_applied = 0u64;
        let mut snapshot_loaded = false;

        let snapshot = storage.load_snapshot().await?;
        if let Some((s_index, s_term, data)) = snapshot {
            if s_index > 0 {
                state_machine
                    .restore(&data)
                    .map_err(|e| format!("StateMachine restore failed: {}", e))?;

                log.set_snapshot(s_index, s_term);
                commit_index = s_index;
                last_applied = s_index;
                snapshot_loaded = true;
            }
        }

        let log_entries = storage.load_log().await?;
        if !log_entries.is_empty() {
            // Storage might return entries already included in snapshot
            let filtered: Vec<_> = log_entries
                .into_iter()
                .filter(|e| e.index > log.snapshot_index())
                .collect();

            if !filtered.is_empty() {
                log.append(filtered);
            }
        }

        if log.first_index() > 1 && !snapshot_loaded {
            return Err("FATAL: Log is truncated, but no snapshot found. Data might be corrupted. Manual recovery required.".into());
        }

        if !log.is_empty() && log.first_index() != log.snapshot_index() + 1 {
            return Err(format!(
                "FATAL: Log is not consistent: snapshot_index = {}, first_log_index = {}",
                log.snapshot_index(),
                log.first_index()
            ));
        }

        // Arc<RwLock<...>> is needed because making a snapshot might be a long operation, that
        // will completely block leader cycle if executed in same thread
        let state_machine = Arc::new(std::sync::RwLock::new(state_machine));

        Ok(Self {
            config,
            state: State::Follower,
            current_term: term,
            voted_for,
            log,
            commit_index,
            last_applied,
            leader_state: None,
            current_leader: None,
            state_machine,
            storage,
            transport,
            event_tx,
            event_rx,
        })
    }

    pub async fn run(&mut self) {
        loop {
            let should_continue = match self.state {
                State::Follower => self.run_follower().await,
                State::Candidate => self.run_candidate().await,
                State::Leader => self.run_leader().await,
            };

            if !should_continue {
                break;
            }
        }
    }

    async fn run_follower(&mut self) -> bool {
        let mut election_deadline = Instant::now() + self.random_election_timeout();

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(election_deadline) => {
                    self.current_leader = None;
                    self.state = State::Candidate;
                    return true;
                }

                Some(event) = self.event_rx.recv() => {
                    match event {
                        RaftEvent::Shutdown => {
                            self.shutdown().await;
                            return false;
                        }
                        RaftEvent::AppendEntriesReceived(req, resp_tx) => {
                            let resp = self.handle_append_entries(req).await;

                            if resp.term >= self.current_term {
                                election_deadline = Instant::now() + self.random_election_timeout();
                            }

                            let _ = resp_tx.send(resp);
                        }
                        RaftEvent::VoteRequestReceived(req, resp_tx) => {
                            let resp = self.handle_request_vote(req).await;
                            if resp.vote_granted {
                            election_deadline = Instant::now() + self.random_election_timeout();
                            }
                            let _ = resp_tx.send(RaftEvent::VoteResponse(resp));
                        }
                        RaftEvent::ClientRequest(req) => {
                            let res = match self.current_leader {
                                Some(id) => CommandResult::Redirect(id),
                                None => CommandResult::Error("Leader is currrently unknown. Try again later.".to_string()),
                            };
                            let _ = req.response_tx.send(res);
                        }

                        RaftEvent::InstallSnapshotReceived(req, resp_tx) => {
                            let resp = self.handle_install_snapshot(req).await;

                            if resp.term >= self.current_term {
                                election_deadline = Instant::now() + self.random_election_timeout();
                            }
                            let _ = resp_tx.send(resp);
                        }

                        _ => {
                            // Followers should ignore other events.
                        }
                    }
                }
            }
        }
    }

    async fn run_candidate(&mut self) -> bool {
        // vote for itself
        let mut tasks = JoinSet::new();

        self.current_leader = None;
        self.start_new_election(&mut tasks);
        if let Err(e) = self
            .storage
            .save_term(self.current_term, self.voted_for)
            .await
        {
            self.fatal_storage_error("save_term", e);
        }

        // this node should not be included in peers
        let total_nodes = self.config.peers.len() + 1;
        let majority = total_nodes / 2 + 1;
        let mut votes_granted = 1;

        let mut election_deadline = Instant::now() + self.random_election_timeout();

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(election_deadline) => {
                    // election timeout - start all over again
                    self.start_new_election(&mut tasks);
                    if let Err(e) = self
                        .storage
                        .save_term(self.current_term, self.voted_for)
                        .await {
                            self.fatal_storage_error("save_term", e)
                    }

                    election_deadline = Instant::now() + self.random_election_timeout();
                    votes_granted = 1;
                }

                Some(response) = tasks.join_next() => {
                    match response {
                        Ok(resp) => {
                            if resp.term > self.current_term {
                            self.become_follower(resp.term).await;
                            return true;
                            }
                            if resp.vote_granted && self.state == State::Candidate {
                                votes_granted += 1;
                                if votes_granted >= majority {
                                    self.become_leader().await;
                                    return true;
                                }
                            }
                        }
                        Err(_) => {} // ignore task errors. In worst scenario node just will wait until
                                     // next election_deadline.
                    }
                }
                Some(event) = self.event_rx.recv() => {
                    match event {
                        RaftEvent::Shutdown => {
                            self.shutdown().await;
                            return false;
                        }
                        RaftEvent::AppendEntriesReceived(req, resp_tx) => {
                            if req.term >= self.current_term {
                                // leader appeared
                                self.become_follower(req.term).await;
                                self.current_leader = Some(req.leader_id);

                                let resp = self.handle_append_entries(req).await;
                                let _ = resp_tx.send(resp);
                                return true // become follower
                            } else {
                                let resp = AppendEntriesResponse {
                                    term: self.current_term,
                                    success: false,
                                    conflict_index: 0,
                                    last_log_index: self.log.last_index(),
                                };
                                let _ = resp_tx.send(resp);
                            }
                        }

                        RaftEvent::VoteRequestReceived(req, resp_tx) => {
                            let resp = self.handle_request_vote(req).await;
                            let _ = resp_tx.send(RaftEvent::VoteResponse(resp));

                            // handle_request_vote turns into follower if req.term > self.term
                            if self.state == State::Follower {
                                self.current_leader = None;
                                return true
                            }
                        }

                        RaftEvent::ClientRequest(req) => {
                            let _ = req.response_tx.send(CommandResult::Error("No leader elected yet. Node is currently a candidate".to_string()));
                        }

                        RaftEvent::InstallSnapshotReceived(req, resp_tx) => {
                            if req.term >= self.current_term {
                                self.become_follower(req.term).await;
                                self.current_leader = Some(req.leader_id);

                                let resp = self.handle_install_snapshot(req).await;
                                let _ = resp_tx.send(resp);
                                return true
                            } else {
                                let resp = InstallSnapshotResponse {
                                    term: self.current_term,
                                    success: false,
                                };
                                let _ = resp_tx.send(resp);
                            }
                        }

                        RaftEvent::SnapshotCompleted(last_index, last_term) => {
                            self.log.compact(last_index, last_term);
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    async fn run_leader(&mut self) -> bool {
        let mut heartbeat_timer =
            tokio::time::interval(Duration::from_millis(self.config.heartbeat_interval));
        heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = heartbeat_timer.tick() => {

                    // ================ DEBUG ===========================
                    println!("💓 [LEADER {}] Heartbeat timer ticked. Broadcasting AppendEntries...", self.config.node_id);

                    for &peer in &self.config.peers {
                        self.send_append_entries_to_follower(peer);
                    }
                }

                Some(event) = self.event_rx.recv() => {
                    match event {
                        RaftEvent::Shutdown => {
                            self.shutdown().await; // save current state
                            return false;
                        }
                        RaftEvent::ClientRequest(req) => {
                            self.handle_client_request(req).await;
                        }

                        RaftEvent::AppendEntriesResponse(resp, follower, match_target) => {
                            if resp.term > self.current_term {
                                self.become_follower(resp.term).await;
                                return true
                            }

                            let leader_state = self.leader_state.as_mut().expect("node is not a leader");
                            if !resp.success {
                                let next_idx = leader_state.next_index.entry(follower).or_insert(1);

                                // if next_idx > match_target then then this might be a response
                                // to old request. Newer request could make next_idx > match_target
                                // so we should ignore this request.
                                //
                                // next_idx <= match_target means everything goes as expected.
                                // Decrement next_idx and send entries again.
                                if *next_idx <= match_target {
                                    if resp.conflict_index > 0 && resp.conflict_index < *next_idx {
                                        *next_idx = resp.conflict_index;
                                    } else {
                                        *next_idx = next_idx.saturating_sub(1);
                                    }

                                    if *next_idx == 0 { *next_idx = 1; }

                                    self.send_append_entries_to_follower(follower);
                                }
                            } else {


                                // =========================== DEBUG =========================================

                                println!("📬 [LEADER {}] Received successful AppendEntriesResponse from Follower {}. Match target: {}", self.config.node_id, follower, match_target);



                                let match_idx = leader_state.match_index.entry(follower).or_insert(0);
                                *match_idx = std::cmp::max(*match_idx, match_target);

                                let next_idx = leader_state.next_index.entry(follower).or_insert(1);
                                *next_idx = *match_idx + 1;

                                // Check if we can increase commit_index
                                let total_nodes = self.config.peers.len() + 1;
                                let majority = total_nodes / 2 + 1;
                                let mut new_commit = self.commit_index;

                                for idx in ((self.commit_index + 1)..=self.log.last_index()).rev() {
                                    let entry = self.log.get(idx).unwrap();

                                    // Leader cannot commit entries from past terms
                                    if entry.term == self.current_term {
                                        let count = leader_state.match_index.values().filter(|&&v| v >= idx).count() + 1;
                                        if count >= majority {
                                            new_commit = idx;
                                            break; // latest commit found -> all earlier entries
                                                   // are also committed
                                        }
                                    }
                                }
                                if new_commit > self.commit_index {
                                    self.commit_index = new_commit;
                                    // Apply all new committed entries
                                    while self.last_applied < self.commit_index {
                                        self.last_applied += 1;
                                        let entry = self.log.get(self.last_applied).unwrap();

                                        let result = match &entry.command {
                                            RaftCommand::RegisterClient => {
                                                CommandResult::Success(entry.client_id.0.to_le_bytes().to_vec())
                                            }
                                            RaftCommand::ClientCommand(cmd) => {
                                                let resp = self.state_machine.write().unwrap().apply(cmd);
                                                match resp {
                                                    Ok(bytes) => CommandResult::Success(bytes),
                                                    Err(e) => CommandResult::Error(e),
                                                }
                                            }
                                        };

                                        leader_state.client_results.insert((entry.client_id, entry.seq_num), result.clone());
                                        if entry.client_id > ClientId(0) {
                                            leader_state.last_seq_nums.insert(entry.client_id, entry.seq_num);

                                        }
                                        // Entry is committed, so we remove it from
                                        // pending_clients. Same result can be found in
                                        // client_results until client sends
                                        // `last_received_seq`, that >= than seq_num of this entry
                                        if let Some(tx) = leader_state.pending_clients.remove(&(entry.client_id, entry.seq_num)) {
                                            let _ = tx.send(result);
                                        }
                                    }

                                    // NOTE: If leader crashes before `save_client_state`, then deduplication state is lost.
                                    // If client retries the same `seq_num`, command may be executed again.
                                    // Clients must not repeat requests with the same seq_num,
                                    // especially after receiving result for this request.
                                    if !leader_state.last_seq_nums.is_empty() {
                                        // TODO: Might be better to save_client_state in
                                        // tokio::spawn, If this function ever becomes a
                                        // bottleneck.
                                        if let Err(e) = self.storage.save_client_state(leader_state.last_client_id, &leader_state.last_seq_nums).await {
                                            self.fatal_storage_error("save_client_state", e);
                                        }
                                    }

                                    // Check if snapshot is required.
                                    // First snapshot happens here.
                                    // After it happens, leader will send snapshot request for
                                    // every follower, if leader no longer has entry with their
                                    // last index. (if it was deleted after a snapshot)
                                    if self.last_applied - self.log.snapshot_index() >= self.config.snapshot_threshold {
                                        if let Some(last_applied_entry) = self.log.get(self.last_applied) {

                                            let sm = self.state_machine.clone();
                                            let mut st = self.storage.clone();
                                            let event_tx_clone = self.event_tx.clone();
                                            let node_id = self.config.node_id;
                                            let snapshot_index = last_applied_entry.index;
                                            let snapshot_term = last_applied_entry.term;

                                            tokio::spawn(async move {

                                                let snapshot_result = tokio::task::spawn_blocking(move || {
                                                    let guard = sm.read().unwrap();
                                                    guard.snapshot()
                                                }).await;

                                                match snapshot_result {
                                                    Ok(Ok(bytes)) => {
                                                        println!("💾 [LEADER {}] Background task: Writing snapshot up to index {} to disk...", node_id, snapshot_index);

                                                        match st.save_snapshot(snapshot_index, snapshot_term, &bytes).await {
                                                            Ok(_) => {
                                                                let _ = event_tx_clone.send(RaftEvent::SnapshotCompleted(snapshot_index, snapshot_term)).await;
                                                            }
                                                            Err(e) => {
                                                                eprintln!("❌ [LEADER {}] FATAL: Background snapshot write failed: {}. Shutting down node to prevent data corruption.", node_id, e);
                                                                std::process::exit(1);
                                                            }
                                                        }
                                                    }
                                                    Ok(Err(e)) => {
                                                        eprintln!("❌ [LEADER {}] StateMachine::snapshot() failed: {}", node_id, e);
                                                        std::process::exit(1);
                                                    }

                                                    Err(join_err) => {
                                                        eprintln!("❌ [LEADER {}] StateMachine::snapshot() task panicked: {}", node_id, join_err);
                                                        std::process::exit(1);
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }
                            }
                        }

                        // Leader made it's own snapshot
                        RaftEvent::SnapshotCompleted(last_index, last_term) => {
                            if last_index > self.log.snapshot_index() {
                                // ======================= DEBUG ==============================
                                println!("🧹 [LEADER {}] Compacting log up to index {}", self.config.node_id, last_index);

                                self.log.compact(last_index, last_term);

                                if let Err(e) = self.storage.truncate_log(last_index).await {
                                    self.fatal_storage_error("truncate_log", e);
                                }
                            }
                        }

                        RaftEvent::InstallSnapshotResponseReceived(resp, follower, last_included_index) => {
                            if resp.term > self.current_term {
                                self.become_follower(resp.term).await;
                                return true
                            }

                            if resp.success {
                                let leader_state = self.leader_state.as_mut().expect("node is not a leader");

                                let match_idx = leader_state.match_index.entry(follower).or_insert(0);
                                *match_idx = std::cmp::max(*match_idx, last_included_index);

                                let next_idx = leader_state.next_index.entry(follower).or_insert(1);
                                *next_idx = *match_idx + 1;

                                // ================= DEBUG ========================
                                println!("📊 [LEADER {}] Follower {} advanced to snapshot index {}", self.config.node_id, follower, last_included_index);

                                self.send_append_entries_to_follower(follower);
                            }
                        }

                        RaftEvent::VoteRequestReceived(req, resp_tx) => {
                            let resp = self.handle_request_vote(req).await;

                            let _ = resp_tx.send(RaftEvent::VoteResponse(resp));

                            // handle_request_vote could turn node into follower if vote was
                            // granted
                            if self.state == State::Follower {
                                return true
                            }
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    // Updates candidate's term and sends vote request to each peer node
    fn start_new_election(&mut self, tasks: &mut JoinSet<RequestVoteResponse>) {
        //  ============================================ DEBUG ==============================================
        println!(
            "🚨 [NODE {}] Election timeout triggered! Incrementing term to {} and starting new election round...",
            self.config.node_id,
            self.current_term + 1
        );

        self.current_term += 1;
        self.voted_for = Some(self.config.node_id);
        self.state = State::Candidate;

        tasks.abort_all();

        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.config.node_id,
            last_log_term: self.log.last_term(),
            last_log_index: self.log.last_index(),
        };

        for &peer in &self.config.peers {
            let req_clone = req.clone();
            let transport = self.transport.clone();
            tasks.spawn(async move { transport.send_request_vote(peer, req_clone).await });
        }
    }

    async fn become_follower(&mut self, term: u64) {
        //  ============================================ DEBUG ==============================================
        println!(
            "📉 [NODE {}] Stepping down to FOLLOWER for term {}",
            self.config.node_id, term
        );

        self.current_term = term;
        self.state = State::Follower;
        self.voted_for = None;
        self.leader_state = None;
        self.current_leader = None;
        if let Err(e) = self
            .storage
            .save_term(self.current_term, self.voted_for)
            .await
        {
            self.fatal_storage_error("save_term", e);
        }
    }

    async fn become_leader(&mut self) {
        //  ============================================ DEBUG ==============================================
        println!(
            "👑👑👑 [NODE {}] MAJORITY VOTES GRANTED! Becoming LEADER for term {} 👑👑👑",
            self.config.node_id, self.current_term
        );
        self.state = State::Leader;
        self.current_leader = Some(self.config.node_id);
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        let last_index = self.log.last_index();
        for &peer in &self.config.peers {
            next_index.insert(peer, last_index + 1);
            match_index.insert(peer, 0);
        }

        let (last_client_id, last_seq_nums) = self
            .storage
            .load_client_state()
            .await
            .unwrap_or((ClientId(0), HashMap::new()));

        self.leader_state = Some(LeaderState {
            next_index,
            match_index,
            pending_clients: HashMap::new(),
            client_results: HashMap::new(),
            last_client_id,
            last_seq_nums,
        });
    }

    /// Generate random election timeout in [min, max] range from `Config`
    fn random_election_timeout(&self) -> Duration {
        let timeout_ms =
            rand::random_range(self.config.election_timeout_min..=self.config.election_timeout_max);
        Duration::from_millis(timeout_ms)
    }

    /// Called only by leader. Sends AppendEntriesRequest to `follower`.
    ///
    /// # Panics:
    /// Panics if storage log is corrupted.
    /// For details see `send_install_snapshot_to_follower()`
    fn send_append_entries_to_follower(&self, follower: NodeId) {
        let leader_state = self.leader_state.as_ref().expect("node is not a leader");

        let next_idx = *leader_state
            .next_index
            .get(&follower)
            .unwrap_or(&(self.log.last_index() + 1));

        // follower's previous log index
        let prev_log_index = if next_idx > 0 { next_idx - 1 } else { 0 };

        if prev_log_index < self.log.snapshot_index() {
            println!(
                "⚠️ [LEADER {}] Follower {} next_index ({}) is behind leader snapshot_index ({}). Triggering InstallSnapshot!",
                self.config.node_id,
                follower,
                next_idx,
                self.log.snapshot_index()
            );
            self.send_install_snapshot_to_follower(follower);
            return;
        }

        let prev_log_term = if prev_log_index == 0 {
            0
        } else if prev_log_index == self.log.snapshot_index() {
            self.log.snapshot_term()
        } else {
            self.log
                .get(prev_log_index)
                .expect("next_index - 1 must exist in live log entries")
                .term
        };

        let entries = if next_idx <= self.log.last_index() {
            self.log.get_range(next_idx, self.log.last_index() + 1)
        } else {
            vec![]
        };

        let match_target = prev_log_index + entries.len() as u64;

        let request = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.config.node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };

        let transport = self.transport.clone();
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            let response = transport.send_append_entries(follower, request).await;
            let _ = event_tx
                .send(RaftEvent::AppendEntriesResponse(
                    response,
                    follower,
                    match_target,
                ))
                .await;
        });
    }

    /// Called only by leader. Sends a request to install snapshot.
    ///
    /// # Panics:
    /// Panics if any data corruptions happen.
    /// In this case node has to be restarted with empty log.
    fn send_install_snapshot_to_follower(&self, follower: NodeId) {
        let transport = self.transport.clone();
        let storage = self.storage.clone();
        let event_tx = self.event_tx.clone();

        // Loading a snapshot might be a long procedure.
        // We have to save current state for valid checks later.
        let last_included_index = self.log.snapshot_index();
        let last_included_term = self.log.snapshot_term();
        let current_term = self.current_term;
        let leader_id = self.config.node_id;

        tokio::spawn(async move {
            match storage.load_snapshot().await {
                Ok(Some((storage_index, storage_term, snapshot_data))) => {
                    if storage_index != last_included_index || storage_term != last_included_term {
                        panic!(
                            "❌ [LEADER {}] FATAL: Snapshot metadata mismatch! Memory: (idx={}, term={}), Storage: (idx={}, term={}). Data corruption suspected.",
                            leader_id,
                            last_included_index,
                            last_included_term,
                            storage_index,
                            storage_term
                        );
                    }

                    let request = InstallSnapshotRequest {
                        term: current_term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        data: snapshot_data,
                    };

                    // ====================== DEBUG ================================
                    println!(
                        "🚀 [LEADER {}] Sending InstallSnapshot RPC to Follower {}. Last Index: {}",
                        leader_id, follower, last_included_index
                    );

                    let response = transport.send_install_snapshot(follower, request).await;

                    let _ = event_tx
                        .send(RaftEvent::InstallSnapshotResponseReceived(
                            response,
                            follower,
                            last_included_index,
                        ))
                        .await;
                }
                Ok(None) => {
                    panic!(
                        "❌ [LEADER {}] Critical storage error: Log is compacted to index {}, but snapshot data is missing in storage!",
                        leader_id, last_included_index
                    );
                }
                Err(e) => {
                    // Dont know exactly if data corruption happened or not.
                    // Next time leader sends AppendEntries to this node, it will try to load
                    // snapshot again.
                    eprintln!(
                        "❌ [LEADER {}] Failed to load snapshot from storage for follower {}: {}",
                        leader_id, follower, e
                    );
                }
            }
        });
    }

    async fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: 0,
                last_log_index: self.log.last_index(),
            };
        }

        if req.term > self.current_term {
            self.become_follower(req.term).await;
        }
        // become_follower resets `self.current_leader`
        // so we set possibly new `leader_id` as `current_leader`
        if req.term >= self.current_term {
            self.current_leader = Some(req.leader_id);
        }

        self.current_leader = Some(req.leader_id);

        let prev_index = req.prev_log_index;

        if prev_index < self.log.snapshot_index() {
            return AppendEntriesResponse {
                term: self.current_term,
                success: true, // Log is consistent up to snapshot index. Success
                conflict_index: 0,
                last_log_index: self.log.last_index(),
            };
        }

        if prev_index > 0 {
            let current_prev_term = if prev_index == self.log.snapshot_index() {
                self.log.snapshot_term()
            } else if let Some(entry) = self.log.get(prev_index) {
                entry.term
            } else {
                0 // No entry at all
            };

            if current_prev_term != req.prev_log_term {
                return AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    conflict_index: prev_index,
                    last_log_index: self.log.last_index(),
                };
            }
        }

        if !req.entries.is_empty() {
            let new_entries = req.entries;

            // prev_index + 1 > snapshot_index, guaranteed by checks above.
            self.log.truncate_from(prev_index + 1);
            self.log.append(new_entries);

            if let Err(e) = self
                .storage
                .append_log(
                    &self
                        .log
                        .get_range(prev_index + 1, self.log.last_index() + 1),
                )
                .await
            {
                self.fatal_storage_error("append_log", e);
            }
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(req.leader_commit, self.log.last_index());

            while self.last_applied < self.commit_index {
                self.last_applied += 1;

                if self.last_applied <= self.log.snapshot_index() {
                    continue;
                }

                let entry = self
                    .log
                    .get(self.last_applied)
                    .expect("applied entries must be in log");

                match &entry.command {
                    RaftCommand::RegisterClient => {}
                    RaftCommand::ClientCommand(cmd) => {
                        let _ = self.state_machine.write().unwrap().apply(cmd);
                    }
                }
            }
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            conflict_index: 0,
            last_log_index: self.log.last_index(),
        }
    }

    async fn handle_request_vote(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        if req.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if req.term > self.current_term {
            self.become_follower(req.term).await;
        }

        let vote_granted =
            if self.voted_for.is_none() || self.voted_for == Some(req.candidate_id as NodeId) {
                let our_last_log_index = self.log.last_index();
                let our_last_log_term = self.log.last_term();
                if req.last_log_term > our_last_log_term
                    || (req.last_log_term == our_last_log_term
                        && req.last_log_index >= our_last_log_index)
                {
                    self.voted_for = Some(req.candidate_id as NodeId);

                    if let Err(e) = self
                        .storage
                        .save_term(self.current_term, self.voted_for)
                        .await
                    {
                        self.fatal_storage_error("save_term", e);
                    }

                    true
                } else {
                    false
                }
            } else {
                false
            };

        RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    /// Executes requests from clients (both configuration and state mutations).
    async fn handle_client_request(&mut self, req: ClientRequest) {
        // Redirect if leadership was lost during request routing
        let leader_state = match self.leader_state.as_mut() {
            Some(state) => state,
            None => {
                let res = match self.current_leader {
                    Some(leader_id) => CommandResult::Redirect(leader_id),
                    None => CommandResult::Error("Node is not a leader".into()),
                };
                let _ = req.response_tx.send(res);
                return;
            }
        };

        // Clean already received results from cache
        leader_state
            .client_results
            .retain(|(cid, seq), _| !(*cid == req.client_id && seq.0 <= req.last_received_seq.0));

        if let Some(last_seq) = leader_state.last_seq_nums.get(&req.client_id) {
            if req.seq_num <= *last_seq {
                // Result was lost after re-election or snapshot.
                // Client should've received this result, or `last_seq_nums` wouldn't have
                // incremented. Can't really do anything but send empty response.
                let _ = req.response_tx.send(CommandResult::Success(vec![]));
                return;
            }
        }

        // Check if request is a duplicate or already in process
        let key = (req.client_id, req.seq_num);
        if let Some(result) = leader_state.client_results.get(&key) {
            let _ = req.response_tx.send(result.clone());
            return;
        }
        if leader_state.pending_clients.contains_key(&key) {
            let _ = req.response_tx.send(CommandResult::Error(
                "Duplicate request is already in progress".into(),
            ));
            return;
        }

        let mut target_client_id = req.client_id;
        let mut target_seq_num = req.seq_num;

        if let RaftCommand::RegisterClient = req.command {
            leader_state.last_client_id = ClientId::from_u64(leader_state.last_client_id.0 + 1);

            target_client_id = leader_state.last_client_id;
            target_seq_num = SeqNum::from_u64(0);
            leader_state
                .last_seq_nums
                .insert(target_client_id, target_seq_num);
        }

        let entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            command: req.command,
            client_id: target_client_id,
            seq_num: target_seq_num,
        };

        self.log.append(vec![entry.clone()]);
        if let Err(e) = self.storage.append_log(&[entry]).await {
            self.fatal_storage_error("append_log", e);
        }

        let target_key = (target_client_id, target_seq_num);
        leader_state
            .pending_clients
            .insert(target_key, req.response_tx);

        for &peer in &self.config.peers {
            self.send_append_entries_to_follower(peer);
        }
    }

    async fn handle_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        if req.term < self.current_term {
            return InstallSnapshotResponse {
                term: self.current_term,
                success: false,
            };
        }

        if req.term > self.current_term {
            self.become_follower(req.term).await;
        }
        // become_follower resets `self.current_leader`
        // so we set possibly new `leader_id` as `current_leader`
        if req.term >= self.current_term {
            self.current_leader = Some(req.leader_id);
        }

        // ============================== DEBUG =================================
        println!(
            "📦 [NODE {}] Installing snapshot from leader. Last included index: {}",
            self.config.node_id, req.last_included_index
        );

        if let Err(e) = self.state_machine.write().unwrap().restore(&req.data) {
            eprintln!(
                "📦 [NODE {}] StateMachine restore failed: {}",
                self.config.node_id, e
            );
            return InstallSnapshotResponse {
                term: self.current_term,
                success: false,
            };
        }

        if let Err(e) = self
            .storage
            .save_snapshot(req.last_included_index, req.last_included_term, &req.data)
            .await
        {
            eprintln!(
                "📦 [NODE {}] Failed to save physical snapshot: {}",
                self.config.node_id, e
            );
            return InstallSnapshotResponse {
                term: self.current_term,
                success: false,
            };
        }

        self.log
            .compact(req.last_included_index, req.last_included_term);
        if let Err(e) = self.storage.truncate_log(req.last_included_index).await {
            self.fatal_storage_error("truncate_log", e);
        }

        // every entry in snapshot is considered applied to StateMachine
        self.commit_index = std::cmp::max(self.commit_index, req.last_included_index);
        self.last_applied = std::cmp::max(self.last_applied, req.last_included_index);
        self.current_leader = Some(req.leader_id); // in case leader gets changed

        InstallSnapshotResponse {
            term: self.current_term,
            success: true,
        }
    }

    fn fatal_storage_error(&self, context: &str, e: String) -> ! {
        eprintln!(
            "❌ [NODE {}] FATAL STORAGE ERROR in {}: {}. Data directory may be corrupted. Node is shutting down.",
            self.config.node_id, context, e
        );
        std::process::exit(1);
    }

    // Shuts node down gracefully.
    // If node was a leader, it saves current state before exiting.
    async fn shutdown(&mut self) {
        println!(
            "🛑 [NODE {}] Shutting down gracefully...",
            self.config.node_id
        );

        if let Some(leader_state) = &self.leader_state {
            if !leader_state.last_seq_nums.is_empty() {
                let _ = self
                    .storage
                    .save_client_state(leader_state.last_client_id, &leader_state.last_seq_nums)
                    .await;
            }
        }

        let _ = self
            .storage
            .save_term(self.current_term, self.voted_for)
            .await;

        if let Some(leader_state) = &mut self.leader_state {
            for (_, tx) in leader_state.pending_clients.drain() {
                let _ = tx.send(CommandResult::Error("Node is shutting down".into()));
            }
        }
    }
}
