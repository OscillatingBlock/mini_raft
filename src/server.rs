use crate::{
    codec::{AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse},
    config::ServerConfig,
};
use rand::RngExt;
use std::{sync::Arc, time::Duration};

use anyhow::Context;
use tokio::{
    sync::mpsc::{self, error::SendError},
    time::Instant,
};
use tracing::{debug, field::debug, info, instrument, warn};

use crate::state_machine::StateMachine;

#[allow(dead_code)]
pub struct Server {
    pub id: NodeId,
    state: ServerState,
    config: ServerConfig,
    to_network: mpsc::Sender<Outgoing>,
    from_network: tokio::sync::Mutex<mpsc::Receiver<Message>>,
    log: Vec<LogEntry>,
}

#[allow(dead_code)]
pub struct ServerState {
    state: RaftState,
    state_machine: Arc<dyn StateMachine>,

    current_term: u32,
    voted_for: Option<NodeId>,
    // log: Vec<String>,
    //
    commit_index: u32,
    last_applied: u32,

    next_index: Vec<u64>,
    match_index: Vec<u64>,

    last_heartbeat: Instant,

    votes_received: u32,
}

impl ServerState {
    pub fn new(state_machine: Arc<dyn StateMachine>) -> Self {
        Self {
            state: RaftState::Follower,
            state_machine,
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],
            last_heartbeat: Instant::now(),
            votes_received: 0,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct NodeId(u32);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NodeId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub leader_id: u32,
    pub server_id: u32,
    pub term: u32,
    pub command: LogCommand,
    pub data: u32,
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogCommand {
    Noop,
    Set,
    Delete,
}

#[allow(dead_code)]
enum Destination {
    Leader,
    Node(NodeId),
    Broadcast,
}

#[allow(dead_code)]
struct Outgoing {
    dest: Destination,
    msg: Message,
}

#[allow(dead_code)]
impl Server {
    fn new(
        id: NodeId,
        config: ServerConfig,
        state_machine: Arc<dyn StateMachine>,
        to_network: mpsc::Sender<Outgoing>,
        from_network: mpsc::Receiver<Message>,
    ) -> Server {
        info!("initializing new server node");
        let state = ServerState::new(state_machine);
        let from_network = tokio::sync::Mutex::new(from_network);
        Self {
            id,
            config,
            state,
            to_network,
            from_network,
            log: vec![],
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    fn run(&self) {
        info!("starting server main loop");
        loop {
            match self.state.state {
                RaftState::Follower => {}
                RaftState::Candidate => {}
                RaftState::Leader => {}
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn follower(&mut self) {
        info!("entering follower state");
        self.apply_latest_commited_logs().await;
        let timeout_duration = self.config.election_timeout;

        loop {
            tokio::select! {
                _ = self.recieve_rpc() => {
                    debug!("follower received rpc");
                    break
                }
                _ = set_timer(timeout_duration.clone()) => {
                    self.state.state = RaftState::Candidate;
                    warn!("election timeout expired, transitioning from follower to candidate");
                    break
                }
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn candidate(&mut self) {
        self.state.current_term += 1;
        self.state.last_heartbeat = Instant::now();
        self.state.voted_for = Some(self.id.clone());
        info!(
            term = self.state.current_term,
            "starting new election as candidate"
        );

        let mut timeout_duration = self.config.election_timeout;
        let mut rng = rand::rng();

        loop {
            // send request_vote rpcs to all other server
            debug("sending request_vote rpc to peers");
            self.send_request_vote_rpc_to_peers().await;

            tokio::select! {
                _ = self.recieve_rpc() => {
                    // break the candidate loop
                    // either we recieved majority votes and became Leader
                    // or we discovered Leader and became follower
                    if self.state.state != RaftState::Candidate {
                        info!(new_state = ?self.state.state, "state changed during candidate phase, exiting loop");
                        break
                    }
                }
                _ = set_timer(timeout_duration.clone()) => {
                    warn!("candidate election timeout expired, restarting election");
                    timeout_duration += Duration::from_micros(rng.random_range(0..500));
                }
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn send_request_vote_rpc_to_peers(&mut self) {
        debug!("broadcasting request vote rpc to peers");
        let request_vote_msg = Message::RequestVoteType(RequestVote {
            term: self.state.current_term,
            candidate_id: self.id.clone(),
            last_log_index: if self.log.is_empty() {
                0
            } else {
                self.log.len() as u32 - 1
            },
            last_log_term: if self.log.is_empty() {
                0
            } else {
                self.log[self.log.len() - 1].term
            },
        });
        let outgoing_msg = Outgoing {
            dest: Destination::Broadcast,
            msg: request_vote_msg,
        };
        _ = self.to_network.send(outgoing_msg).await;
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn leader(&mut self) {
        info!("entering leader state");
        self.apply_latest_commited_logs().await;

        // upon election send first heartbeat to all peers
        self.send_heartbeat().await;

        loop {
            self.manage_follower_logs().await;
            self.handle_commit_index().await;

            tokio::select! {
                _ = set_timer(self.config.heartbeat_interval) => {
                    debug!("heartbeat interval reached, sending heartbeat");
                    self.send_heartbeat().await;
                }

                _ = self.recieve_rpc() => {
                    // if we discovered that we are no longer leader break leader loop
                    if self.state.state != RaftState::Leader {
                        warn!(new_state = ?self.state.state, "no longer leader, stepping down");
                        break;
                    }
                }
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn send_heartbeat(&mut self) {
        debug!("sending heartbeat to peers");
        let heartbeat_msg = Message::AppendEntriesType(AppendEntries {
            term: self.state.current_term,
            leader_id: self.id.clone(),
            prev_log_index: if self.log.is_empty() {
                0
            } else {
                self.log.len() as u32 - 1
            },
            prev_log_term: if self.log.is_empty() {
                0
            } else {
                self.log[self.log.len() - 1].term
            },
            entries: vec![],
            leader_commit: self.state.commit_index,
        });
        let outgoing_msg = Outgoing {
            dest: Destination::Broadcast,
            msg: heartbeat_msg,
        };
        self.to_network.send(outgoing_msg).await.unwrap();
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn recieve_rpc(&mut self) -> anyhow::Result<()> {
        let msg = self
            .from_network
            .lock()
            .await
            .recv()
            .await
            .context("channel closed")?;

        debug!(
            message_type = msg.message_type(),
            "received rpc message from network"
        );

        match msg {
            Message::AppendEntriesType(append_entries_msg) => {
                self.handle_append_entries(append_entries_msg).await?;
            }

            Message::AppendEntriesResponseType(_) => {
                debug!("received append entries response");
            }

            Message::RequestVoteType(request_vote_msg) => {
                self.handle_request_vote_rpc(request_vote_msg).await?;
            }

            Message::RequestVoteResponseType(request_vote_response_msg) => {
                self.handle_request_vote_response_rpc(request_vote_response_msg)
                    .await;
            }
        }
        Ok(())
    }

    #[instrument(skip(self, msg), fields(node_id = %self.id, term = msg.term))]
    async fn handle_append_entries(&mut self, msg: AppendEntries) -> anyhow::Result<()> {
        debug!("handling append entries message");
        let response = Message::AppendEntriesResponseType(AppendEntriesResponse {
            term: self.state.current_term,
            success: false,
        });
        let reply = Outgoing {
            dest: Destination::Leader,
            msg: response,
        };
        info!(term = self.state.current_term);
        // Reply false if term < currentTerm
        if msg.term < self.state.current_term {
            self.reply_false_if_lower_term(Message::AppendEntriesType(msg.clone()), reply)
                .await?;

            return Ok(());
        }

        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        let matched = self.reply_false_if_prev_log_does_not_match(&msg).await?;
        if !matched {
            return Ok(());
        }

        // if we are leader and msg term higher than our current term, then become follower
        self.change_state_if_higher_term(&msg);

        if msg.entries.is_empty() {
            debug!("received valid empty heartbeat from leader");
            return Ok(()); // heartbeat
        };

        // If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it
        self.delete_conflicting_and_append_new_entries(&msg);

        // If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if msg.leader_commit > self.state.commit_index {
            let last_new_entry_index = msg.entries[msg.entries.len() - 1].index;
            self.state.commit_index = std::cmp::min(msg.leader_commit, last_new_entry_index as u32);
            debug!(
                commit_index = self.state.commit_index,
                "updated local commit index"
            );
        }

        let response = Message::AppendEntriesResponseType(AppendEntriesResponse {
            term: self.state.current_term,
            success: true,
        });
        let outgoing_msg = Outgoing {
            dest: Destination::Leader,
            msg: response,
        };
        self.to_network.send(outgoing_msg).await?;

        Ok(())
    }

    async fn reply_false_if_lower_term(
        &mut self,
        msg: Message,
        reply: Outgoing,
    ) -> anyhow::Result<()> {
        let term = match msg {
            Message::AppendEntriesType(msg) => msg.term,
            Message::RequestVoteType(msg) => msg.term,
            _ => {
                unreachable!()
            }
        };
        if term < self.state.current_term {
            match reply.msg {
                Message::AppendEntriesResponseType(_) => {
                    warn!(
                        leader_term = term,
                        current_term = self.state.current_term,
                        "rejecting append entries: leader term is lower than local term"
                    );
                }
                Message::RequestVoteResponseType(_) => {
                    debug!("denying vote: candidate term is lower than current term");
                }
                _ => {}
            }

            return self
                .to_network
                .send(reply)
                .await
                .context("failed to send append entries");
        }
        Ok(())
    }

    async fn reply_false_if_prev_log_does_not_match(
        &mut self,
        msg: &AppendEntries,
    ) -> anyhow::Result<bool> {
        let mut prev_log_index_matching = true;

        match self.log.get(msg.prev_log_index as usize) {
            Some(log_entry) => {
                if log_entry.term != msg.prev_log_term {
                    prev_log_index_matching = false;
                }
            }
            None => {
                // if self.log.len == 0, its the first msg from leader , accept it
                if self.log.len() != 0 {
                    prev_log_index_matching = false;
                }
            }
        }

        if !prev_log_index_matching {
            let response = Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: self.state.current_term,
                success: false,
            });
            let outgoing_msg = Outgoing {
                dest: Destination::Leader,
                msg: response,
            };
            warn!(
                prev_log_index = msg.prev_log_index,
                "rejecting append entries: previous log index/term does not match"
            );
            self.to_network
                .send(outgoing_msg)
                .await
                .context("failed to send append entries")?;

            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn change_state_if_higher_term(&mut self, msg: &AppendEntries) {
        if msg.term > self.state.current_term {
            info!(
                old_term = self.state.current_term,
                new_term = msg.term,
                "discovered higher term, stepping down to follower"
            );
            self.state.current_term = msg.term;
            self.state.state = RaftState::Follower;
        } else if msg.term == self.state.current_term && self.state.state == RaftState::Candidate {
            //if we are candidate, and recieved heartbeat or new entries from leader with higher term, then
            //become follower
            info!(
                old_term = self.state.current_term,
                new_term = msg.term,
                "candidate discovered leader, stepping down to follower"
            );
            self.state.state = RaftState::Follower;
        }
    }

    fn delete_conflicting_and_append_new_entries(&mut self, msg: &AppendEntries) {
        let mut new_entries = vec![];
        for entry in msg.entries.iter() {
            let ith_log_entry = self.log.get(entry.index as usize);
            match ith_log_entry {
                Some(log_entry) => {
                    if log_entry.term != entry.term {
                        warn!(
                            index = entry.index,
                            "log conflict detected, truncating conflicting entries"
                        );
                        self.log.truncate(entry.index as usize);
                        new_entries.push(entry.clone());
                    }
                }
                None => {
                    new_entries.push(entry.clone());
                }
            }
        }

        // Append any new entries not already in the log
        for entry in new_entries.iter() {
            debug!(index = entry.index, "appending new entry to local log");
            self.log.insert(entry.index as usize, entry.clone());
        }
    }

    #[instrument(skip(self, msg), fields(node_id = %self.id, candidate_id = %msg.candidate_id))]
    async fn handle_request_vote_rpc(&mut self, msg: RequestVote) -> anyhow::Result<()> {
        debug!("handling request vote rpc");
        let no_vote_response_msg = Message::RequestVoteResponseType(RequestVoteResponse {
            term: self.state.current_term,
            vote_granted: false,
        });
        let reply = Outgoing {
            dest: Destination::Node(msg.candidate_id.clone()),
            msg: no_vote_response_msg.clone(),
        };

        //Reply false if term < currentTerm
        if msg.term < self.state.current_term {
            self.reply_false_if_lower_term(Message::RequestVoteType(msg.clone()), reply)
                .await?;

            return Ok(());
        }

        match self.state.voted_for {
            Some(ref voted_for) => {
                if voted_for != &msg.candidate_id
                    || msg.last_log_index < self.log[self.log.len() - 1].index
                    || msg.last_log_term != self.log[self.log.len() - 1].term
                {
                    debug!(
                        "denying vote: already voted for another candidate or log is less up-to-date"
                    );
                    self.to_network.send(reply).await?;
                    return Ok(());
                }
            }
            None => {}
        }

        info!(candidate_id = %msg.candidate_id, "granting vote to candidate");
        let vote_response = RequestVoteResponse {
            term: self.state.current_term,
            vote_granted: true,
        };
        let vote_response_msg = Message::RequestVoteResponseType(vote_response);
        let outgoing_msg = Outgoing {
            dest: Destination::Node(msg.candidate_id),
            msg: vote_response_msg.clone(),
        };
        self.to_network.send(outgoing_msg).await?;

        Ok(())
    }

    #[instrument(skip(self, msg), fields(node_id = %self.id, term = msg.term))]
    async fn handle_request_vote_response_rpc(&mut self, msg: RequestVoteResponse) {
        debug!("handling request vote response rpc");
        if msg.term > self.state.current_term {
            info!(
                old_term = self.state.current_term,
                new_term = msg.term,
                "discovered higher term in vote response, becoming follower"
            );
            self.state.current_term = msg.term;
            self.state.state = RaftState::Follower;
            self.state.voted_for = None;
            self.state.votes_received = 0;
            return;
        }

        if msg.vote_granted {
            // count votes
            self.state.votes_received += 1;
            debug!(
                votes_received = self.state.votes_received,
                "vote granted, tallying votes"
            );
            // if majority votes, become leader
            if self.state.votes_received >= (self.config.total_nodes / 2) + 1 {
                self.state.state = RaftState::Leader;
                info!("candidate received majority votes, transitioning to leader");
            }
            // else stay candidate
        } else {
            debug!("vote denied by peer");
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn manage_follower_logs(&self) {
        for (_i, next_index) in self.state.next_index.iter().enumerate() {
            let last_log_index = self.state.state_machine.get_last_log_index();
            if last_log_index > *next_index {
                debug!(next_index = *next_index, "managing follower logs");
                // send AppendEntries RPC with log entries starting at nextIndex
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn handle_commit_index(&self) {}

    // applies latest commited logs to state machine
    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn apply_latest_commited_logs(&mut self) {
        if self.state.commit_index > self.state.last_applied {
            self.state.last_applied += 1;
            debug!(
                last_applied = self.state.last_applied,
                "applying committed log to state machine"
            );
            let log = self.log[self.state.last_applied as usize].clone();
            self.state.state_machine.apply_log(log);
        }
    }
}

async fn set_timer(timeout_duration: tokio::time::Duration) {
    tokio::time::sleep(timeout_duration).await;
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::state_machine::MockStateMachine;
    use std::sync::Arc;

    fn create_new_server_and_mock_msg_sender()
    -> (Server, mpsc::Sender<Message>, mpsc::Receiver<Outgoing>) {
        println!("Creating new server and mock message sender");
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let node_id = NodeId(1);
        let config = ServerConfig {
            election_timeout: tokio::time::Duration::from_millis(200),
            heartbeat_interval: tokio::time::Duration::from_millis(50),
            address: "localhost:8080".to_string(),
            total_nodes: 5,
        };
        let state_machine = MockStateMachine::new();
        let server_state_machine = ServerState::new(Arc::new(state_machine));
        let (node_tx, adapter_rx) = mpsc::channel::<Outgoing>(32); // Node → adapter
        let (adapter_tx, node_rx) = mpsc::channel::<Message>(32); // adapter → Node

        let server = Server::new(
            node_id,
            config,
            server_state_machine.state_machine.clone(),
            node_tx,
            node_rx,
        );
        (server, adapter_tx, adapter_rx)
    }

    async fn populate_follower_log(mock_adapter_tx: &mpsc::Sender<Message>) {
        mock_adapter_tx
            .send(Message::AppendEntriesType(AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    leader_id: 1,
                    server_id: 1,
                    term: 1,
                    command: LogCommand::Set,
                    data: 42,
                    index: 0,
                }],
                leader_commit: 0,
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_follower_timeout_triggers_candidate() {
        let (mut server, _do_not_remove_this_unused_variable, _this_also) =
            create_new_server_and_mock_msg_sender();
        server.follower().await;

        let current_state = server.state.state;
        assert_eq!(current_state, RaftState::Candidate);
    }

    #[tokio::test]
    async fn test_leader_heartbeat_prevents_follower_timeout() {
        let (mut server, mock_adapter_tx, _) = create_new_server_and_mock_msg_sender();
        mock_adapter_tx
            .send(Message::AppendEntriesType(AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }))
            .await
            .unwrap();

        server.follower().await;
        let current_state = server.state.state;
        assert_eq!(current_state, RaftState::Follower);
    }

    #[tokio::test]
    async fn test_follower_rejects_incosistent_log_append_entries() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        //send one valid log entry to follower first
        populate_follower_log(&mock_adapter_tx).await;

        server.follower().await;
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(first_response) = first_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(first_response.success, true));

        //send an AppendEntries RPC with a conflicting log entry
        mock_adapter_tx
            .send(Message::AppendEntriesType(AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 1,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    leader_id: 1,
                    server_id: 1,
                    term: 1,
                    command: LogCommand::Set,
                    data: 43,
                    index: 0,
                }],
                leader_commit: 0,
            }))
            .await
            .unwrap();

        server.follower().await;

        let second_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(second_response) = second_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(second_response.success, false));
    }

    #[tokio::test]
    async fn test_follower_accepts_valid_append_entries() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        populate_follower_log(&mock_adapter_tx).await;

        server.follower().await;
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(first_response) = first_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(first_response.success, true));

        //send 2 valid AppendEntries RPC with  new log entry
        let first_log_entry = LogEntry {
            leader_id: 1,
            server_id: 1,
            term: 1,
            command: LogCommand::Set,
            data: 43,
            index: 1,
        };
        let second_log_entry = LogEntry {
            leader_id: 1,
            server_id: 1,
            term: 1,
            command: LogCommand::Set,
            data: 44,
            index: 2,
        };
        mock_adapter_tx
            .send(Message::AppendEntriesType(AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: 0,
                prev_log_term: 1,
                entries: vec![first_log_entry.clone(), second_log_entry.clone()],
                leader_commit: 0,
            }))
            .await
            .unwrap();
        server.follower().await;

        let response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(response) = response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(response.success, true));

        let server_first_log_entry = server.log.get(1).unwrap();
        let server_second_log_entry = server.log.get(2).unwrap();

        assert_eq!(server_first_log_entry, &first_log_entry);
        assert_eq!(server_second_log_entry, &second_log_entry);
    }

    #[tokio::test]
    async fn test_follower_grants_vote_to_valid_candidate() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        populate_follower_log(&mock_adapter_tx).await;

        server.follower().await;
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(first_response) = first_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(first_response.success, true));

        mock_adapter_tx
            .send(Message::RequestVoteType(RequestVote {
                term: 2,
                candidate_id: NodeId(2),
                last_log_index: 0,
                last_log_term: 1,
            }))
            .await
            .unwrap();
        server.follower().await;

        let vote_response = mock_adapter_rx.recv().await.unwrap();
        let Message::RequestVoteResponseType(vote_response) = vote_response.msg else {
            panic!("Expected RequestVoteResponseType");
        };
        assert!(vote_response.vote_granted);
    }

    #[tokio::test]
    async fn test_new_candidate_sends_request_vote_rpc_to_peers() {
        tokio::time::pause();
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        populate_follower_log(&mock_adapter_tx).await;
        server.follower().await;

        //receive the append entries response from the follower state
        let _ = mock_adapter_rx.recv().await.unwrap();

        //already send an heartbeat so candidate loop ends in the next step when we start it
        let heartbeat = Message::AppendEntriesType(AppendEntries {
            term: server.state.current_term,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        });
        mock_adapter_tx.send(heartbeat).await.unwrap();

        //candidate method should send request vote rpc to all peers
        server.candidate().await;

        let request_vote_msg = mock_adapter_rx.recv().await.unwrap();
        let Message::RequestVoteType(req) = request_vote_msg.msg else {
            println!("Expected RequestVoteType, got {:?}", request_vote_msg.msg);
            panic!("Expected RequestVoteType")
        };
        assert_eq!(req.term, server.state.current_term);
        assert_eq!(req.candidate_id, server.id);
        assert_eq!(req.last_log_index, server.log.len() as u32 - 1);
        assert_eq!(req.last_log_term, server.log[server.log.len() - 1].term);
    }

    #[tokio::test]
    async fn test_candidate_reverts_to_follower_on_higher_term() {
        let (mut server, mock_adapter_tx, mut _mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        //make follower timeout and become candidate
        server.follower().await;

        //already send an heartbeat so candidate loop ends in the next step when we start it
        let heartbeat = Message::AppendEntriesType(AppendEntries {
            term: server.state.current_term + 1,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        });
        mock_adapter_tx.send(heartbeat).await.unwrap();

        server.candidate().await;

        assert_eq!(server.state.state, RaftState::Follower);
    }

    #[tokio::test]
    async fn test_candidate_becomes_leader_on_majority_votes() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        // follower -> candidate -> leader (if recieves majority vote)
        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;
        assert_eq!(server.state.state, RaftState::Leader);
    }

    #[tokio::test]
    async fn test_leader_becomes_follower_on_higher_term() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        //send a higher term message to leader, so leader can recieve it when it starts
        let higher_term_msg = Message::AppendEntriesType(AppendEntries {
            term: server.state.current_term + 1,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        });
        mock_adapter_tx.send(higher_term_msg).await.unwrap();

        //start leader loop
        server.leader().await;

        assert_eq!(server.state.state, RaftState::Follower);
    }

    #[tokio::test]
    async fn test_leader_sends_periodic_heartbeats() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        // spawn a task , which sends a heartbeat after 3 heartbeat intervals to current leader
        // telling current leader that its not leader anymore ,
        // do this to stop the leader loop
        tokio::task::spawn(async move {
            tokio::time::sleep(server.config.heartbeat_interval * 3).await;
            let higher_term_msg = Message::AppendEntriesType(AppendEntries {
                term: server.state.current_term + 1,
                leader_id: NodeId(4),
                prev_log_index: 0,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            });

            mock_adapter_tx.send(higher_term_msg).await.unwrap();
        });
        server.leader().await;

        //check we recieved atleast 2 heartbeat in this interval
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesType(append_entries) = first_response.msg else {
            panic!("Expected heartbeat message got {:?}", first_response.msg);
        };
        assert_eq!(append_entries.entries.len(), 0);
        println!("{:?}", append_entries);

        let second_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesType(append_entries) = second_response.msg else {
            panic!("Expected heartbeat message got {:?}", second_response.msg);
        };
        assert_eq!(append_entries.entries.len(), 0);
    }

    async fn simulate_follower_becomes_leader(
        server: &mut Server,
        mock_adapter_tx: &mpsc::Sender<Message>,
        mock_adapter_rx: &mut mpsc::Receiver<Outgoing>,
    ) {
        //first make follower transition to candidate state by not sending any msg during its
        //election_timeout
        server.follower().await;

        //send majority votes to candidate
        for _ in 0..(server.config.total_nodes / 2) + 1 {
            mock_adapter_tx
                .send(Message::RequestVoteResponseType(RequestVoteResponse {
                    term: server.state.current_term,
                    vote_granted: true,
                }))
                .await
                .unwrap();
        }

        server.candidate().await;

        //drain the request vote rpc request sent by candidate which it sent before entering the loop
        for _ in 0..(server.config.total_nodes / 2) + 1 {
            _ = mock_adapter_rx.recv().await.unwrap();
        }
    }
}
