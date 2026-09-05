use crate::{
    codec::{AppendEntries, Message, MessageCodec},
    server::NodeId,
};

use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, msg: Message) -> anyhow::Result<()>;
    async fn recv(&self) -> anyhow::Result<Message>;
}

#[async_trait::async_trait]
pub trait InboundTransport: Send + Sync {}

pub struct MessagePipeline {
    codec: MessageCodec,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
}

impl MessagePipeline {
    pub fn new(
        codec: crate::codec::MessageCodec,
        tx: mpsc::Sender<Message>,
        rx: mpsc::Receiver<Message>,
    ) -> Self {
        Self { codec, tx, rx }
    }
}
pub struct MockMessagePipeline {
    tx: mpsc::Sender<Message>,
    rx: tokio::sync::Mutex<mpsc::Receiver<Message>>,
}

impl MockMessagePipeline {
    pub fn new(tx: mpsc::Sender<Message>, rx: mpsc::Receiver<Message>) -> Self {
        let rx = Mutex::new(rx);
        Self { tx, rx }
    }

    pub fn recv_mock_heartbeat(&self) -> Message {
        Message::AppendEntriesType(AppendEntries {
            term: 0,
            leader_id: NodeId::new(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        })
    }
}

#[async_trait::async_trait]
impl Transport for MockMessagePipeline {
    async fn send(&self, msg: Message) -> anyhow::Result<()> {
        self.tx
            .send(msg)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }
    async fn recv(&self) -> anyhow::Result<Message> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Channel closed"))
    }
}
