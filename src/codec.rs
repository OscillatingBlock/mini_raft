use crate::server::{LogEntry, NodeId};

pub struct MessageCodec {}

#[derive(Debug, Clone)]
pub enum Message {
    AppendEntriesType(AppendEntries),
    AppendEntriesResponseType(AppendEntriesResponse),
    RequestVoteType(RequestVote),
    RequestVoteResponseType(RequestVoteResponse),
}

impl Message {
    pub fn message_type(&self) -> String {
        match self {
            Message::AppendEntriesType(_) => "AppendEntriesType".to_string(),
            Message::AppendEntriesResponseType(_) => "AppendEntriesResponseType".to_string(),
            Message::RequestVoteType(_) => "RequestVoteType".to_string(),
            Message::RequestVoteResponseType(_) => "RequestVoteResponseType".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u32,
    pub leader_id: NodeId,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub entries: Vec<LogEntry>, // vec![] for heartbeat
    pub leader_commit: u32,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u32,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u32,
    pub candidate_id: NodeId,
    pub last_log_index: u32,
    pub last_log_term: u32,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u32,
    pub vote_granted: bool,
}

// pub enum MessageType {
//
// }
pub trait Decoder {
    type Item;
    type Error;

    // Transforms raw bytes into a high-level Message
    fn decode(&mut self, src: &mut bytes::Bytes) -> Result<Option<Self::Item>, Self::Error>;
}

pub trait Encoder {
    type Error;
    fn encode(&mut self) -> Result<Option<bytes::Bytes>, Self::Error>;
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = anyhow::Error;
    fn decode(&mut self, src: &mut bytes::Bytes) -> Result<Option<Message>, Self::Error> {
        !unimplemented!()
    }
}
