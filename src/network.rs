use futures::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, mpsc},
    time::{Duration, timeout},
};
use tokio_util::codec::Framed;

use crate::{
    codec::{Codec, Hello, Message},
    server::{ClientId, Destination, Id, Incoming, NodeId, Outgoing},
};

pub struct NetworkManager {
    connections_map: ConnectionRegistry,
    codec: Codec,
    //channel to send Incoming messages from network to server
    to_server: Arc<mpsc::Sender<Incoming>>,
    id: NodeId,
}

// thread safe mapping of client and peer id to their writer tasks
// receiving end are used by writer task
struct ConnectionRegistry {
    peer_registry: Arc<Mutex<HashMap<NodeId, mpsc::Sender<Message>>>>,
    client_registry: Arc<Mutex<HashMap<ClientId, mpsc::Sender<Message>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionRole {
    Inbound,
    Outbound,
}

impl ConnectionRegistry {
    fn new() -> Self {
        let peer_registry_map: HashMap<NodeId, mpsc::Sender<Message>> = HashMap::new();
        let client_registry_map: HashMap<ClientId, mpsc::Sender<Message>> = HashMap::new();
        Self {
            peer_registry: Arc::new(Mutex::new(peer_registry_map)),
            client_registry: Arc::new(Mutex::new(client_registry_map)),
        }
    }

    async fn register(&self, id: Id) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel::<Message>(32);
        match id {
            Id::Client(client_id) => {
                self.client_registry.lock().await.insert(client_id, tx);
                debug!(target: "network", ?client_id, "registered client sender channel");
            }
            Id::Peer(peer_id) => {
                self.peer_registry.lock().await.insert(peer_id, tx);
                debug!(target: "network", ?peer_id, "registered peer sender channel");
            }
        }
        rx
    }

    async fn send(&self, id: &Id, msg: Message) -> anyhow::Result<()> {
        let tx = match id {
            Id::Client(client_id) => self.client_registry.lock().await.get(client_id).cloned(),
            Id::Peer(peer_id) => self.peer_registry.lock().await.get(peer_id).cloned(),
        };

        let Some(tx) = tx else {
            anyhow::bail!("writer channel not available for id: {:?}", id);
        };

        tx.send(msg).await?;
        Ok(())
    }

    async fn get_all_peers(&self) -> Vec<NodeId> {
        self.peer_registry.lock().await.keys().cloned().collect()
    }
}

use tracing::{debug, error, info, instrument, warn};

impl NetworkManager {
    pub fn new(to_server: mpsc::Sender<Incoming>, codec: Codec, id: NodeId) -> Self {
        let to_server = Arc::new(to_server);
        Self {
            connections_map: ConnectionRegistry::new(),
            codec,
            to_server,
            id,
        }
    }

    #[instrument(skip(self, listener), fields(node_id = ?self.id))]
    /// Listens for incoming connections from client or peers
    pub async fn listen(&self, listener: TcpListener) -> anyhow::Result<()> {
        info!(target: "network", "starting inbound TCP listener");
        loop {
            let (conn, addr) = match listener.accept().await {
                Ok(val) => val,
                Err(e) => {
                    error!(target: "network", error = %e, "failed to accept incoming TCP connection");
                    continue;
                }
            };

            debug!(target: "network", ?addr, "accepted new raw TCP stream");
            let framed = Framed::new(conn, self.codec.clone());
            self.handle_conn(framed, addr.to_string(), ConnectionRole::Inbound)
                .await;
        }
    }

    /// Generic connection handler supporting both inbound and outbound roles with unique logs.
    async fn handle_conn(
        &self,
        mut framed: Framed<TcpStream, Codec>,
        addr: String,
        role: ConnectionRole,
    ) {
        let id = match self.perform_handshake(&mut framed).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                match role {
                    ConnectionRole::Inbound => {
                        warn!(target: "network", ?addr, "inbound connection closed gracefully during handshake without identification");
                    }
                    ConnectionRole::Outbound => {
                        warn!(target: "network", ?addr, "outbound connection closed gracefully during handshake without identification");
                    }
                }
                return;
            }
            Err(e) => {
                match role {
                    ConnectionRole::Inbound => {
                        error!(target: "network", ?addr, error = %e, "handshake process failed for incoming connection");
                    }
                    ConnectionRole::Outbound => {
                        error!(target: "network", ?addr, error = %e, "handshake process failed for outbound connection");
                    }
                }
                return;
            }
        };

        match role {
            ConnectionRole::Inbound => {
                info!(target: "network", ?id, ?addr, "inbound handshake successful, spawning connection handlers");
            }
            ConnectionRole::Outbound => {
                info!(target: "network", ?id, ?addr, "outbound peer dial and handshake completed successfully");
            }
        }

        let rx = self.connections_map.register(id.clone()).await;
        self.spawn_reader_writer_tasks(framed, id, rx).await;
    }

    #[instrument(skip(self, conn), fields(node_id = ?self.id))]
    async fn perform_handshake(
        &self,
        conn: &mut Framed<TcpStream, Codec>,
    ) -> anyhow::Result<Option<Id>> {
        debug!(target: "network", "sending hello message for handshake initialization");
        conn.send(Message::HelloType(Hello { id: self.id })).await?;

        let msg = self.read_hello_response(conn).await?;

        let id = match msg {
            Some(Message::HelloType(hello)) => {
                info!(target: "network", peer_id = ?hello.id, "identified remote connection as peer node");
                Id::Peer(hello.id)
            }
            Some(Message::HelloClientType(hello)) => {
                info!(target: "network", client_id = ?hello.id, "identified remote connection as client");
                Id::Client(hello.id)
            }
            Some(other) => {
                warn!(target: "network", message_type = ?other, "received unexpected message type during handshake");
                anyhow::bail!("unexpected message type in handshake")
            }
            None => {
                return Ok(None);
            }
        };

        Ok(Some(id))
    }

    async fn read_hello_response(
        &self,
        conn: &mut Framed<TcpStream, Codec>,
    ) -> anyhow::Result<Option<Message>> {
        let handshake_timeout = Duration::from_secs(5);
        let msg_future = timeout(handshake_timeout, conn.next());

        match msg_future.await {
            Ok(Some(Ok(m))) => Ok(Some(m)),
            Ok(Some(Err(e))) => {
                error!(target: "network", error = %e, "codec decoding error during handshake response");
                anyhow::bail!("failed to receive handshake message: {e}")
            }
            Ok(None) => {
                warn!(target: "network", "peer closed connection prematurely during handshake");
                return Ok(None);
            }
            Err(_) => {
                warn!(target: "network", timeout_secs = handshake_timeout.as_secs(), "handshake timed out");
                anyhow::bail!(
                    "handshake timed out after {} seconds",
                    handshake_timeout.as_secs()
                )
            }
        }
    }

    #[instrument(skip(self, peer_addresses), fields(node_id = ?current_node_id))]
    pub async fn dial_peers(
        &self,
        peer_addresses: HashMap<NodeId, String>,
        current_node_id: NodeId,
    ) {
        for (peer_id, addr) in peer_addresses {
            if peer_id > current_node_id {
                debug!(target: "network", peer_id = ?peer_id, %addr, "attempting to dial remote peer");
                // Retry: peers may not be listening yet at boot (sequential startup).
                let Some(conn) = self.connect_with_retry(addr.clone(), peer_id).await else {
                    continue;
                };

                let framed = Framed::new(conn, self.codec.clone());
                self.handle_conn(framed, addr, ConnectionRole::Outbound)
                    .await;
            }
        }
    }

    async fn connect_with_retry(&self, addr: String, peer_id: NodeId) -> Option<TcpStream> {
        let mut conn_opt = None;
        for attempt in 1..=6 {
            match TcpStream::connect(&addr).await {
                Ok(c) => {
                    conn_opt = Some(c);
                    break;
                }
                Err(e) => {
                    error!(target: "network", peer_id = ?peer_id, %addr, error = %e, attempt, "failed to establish TCP connection with peer, retrying");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        let Some(c) = conn_opt else {
            error!(target: "network", peer_id = ?peer_id, %addr, "giving up dialing peer after retries");
            return None;
        };

        return Some(c);
    }

    #[instrument(skip(self, conn), fields(node_id = ?self.id, ?id))]
    async fn spawn_reader_writer_tasks(
        &self,
        conn: Framed<TcpStream, Codec>,
        id: Id,
        rx: mpsc::Receiver<Message>,
    ) {
        let (sink, stream) = conn.split();
        let to_server = self.to_server.clone();

        tokio::spawn(async move {
            debug!(target: "network", ?id, "spawned framed_reader background task");
            framed_reader(stream, id, to_server.clone()).await;
            debug!(target: "network", ?id, "framed_reader task terminated");
        });

        tokio::spawn(async move {
            debug!(target: "network", ?id, "spawned framed_writer background task");
            framed_writer(sink, rx).await;
            debug!(target: "network", ?id, "framed_writer task terminated");
        });
    }
}

#[instrument(skip(framed_stream, to_server), fields(?id))]
async fn framed_reader(
    mut framed_stream: SplitStream<Framed<TcpStream, Codec>>,
    id: Id,
    to_server: Arc<mpsc::Sender<Incoming>>,
) {
    while let Some(msg) = framed_stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                error!(target: "network", ?id, error = %e, "error decoding frame from stream, skipping frame");
                continue;
            }
        };

        let incoming_msg = Incoming { from: id, msg };
        debug!(target: "network", ?id, "forwarding incoming message to server pipeline");

        if let Err(e) = to_server.send(incoming_msg).await {
            error!(target: "network", ?id, error = %e, "server input channel closed, terminating reader loop");
            return;
        }
    }
    warn!(target: "network", ?id, "remote stream ended (EOF)");
}

#[instrument(skip(framed_sink, from_dispatcher))]
async fn framed_writer(
    mut framed_sink: SplitSink<Framed<TcpStream, Codec>, Message>,
    mut from_dispatcher: mpsc::Receiver<Message>,
) {
    while let Some(msg) = from_dispatcher.recv().await {
        debug!(target: "network", "sending message frame over TCP stream");
        if let Err(e) = framed_sink.send(msg).await {
            error!(target: "network", error = %e, "failed to send message frame over TCP sink, writer exiting");
            break;
        }
    }
    debug!(target: "network", "server-to-connection channel dropped, writer terminating");
}

//forwards incoming messages from server to writer task of a connection
pub async fn dispatch_server_msg(
    mut from_server: mpsc::Receiver<Outgoing>,
    network_manager: Arc<NetworkManager>,
) {
    while let Some(msg) = from_server.recv().await {
        match msg.dest {
            Destination::Node(node_id) => {
                let id = Id::Peer(node_id);
                if let Err(e) = network_manager.connections_map.send(&id, msg.msg).await {
                    error!(target: "network", ?id, error = %e, "failed to dispatch message to peer");
                }
            }
            Destination::Client(client_id) => {
                let id = Id::Client(client_id);
                if let Err(e) = network_manager.connections_map.send(&id, msg.msg).await {
                    error!(target: "network", ?id, error = %e, "failed to dispatch message to client");
                }
            }
            Destination::Broadcast => {
                let nodes = network_manager.connections_map.get_all_peers().await;
                info!(target: "network", peer_count = nodes.len(), "broadcasting message to all connected peers");
                for node_id in nodes {
                    let id = Id::Peer(node_id);
                    if let Err(e) = network_manager
                        .connections_map
                        .send(&id, msg.msg.clone())
                        .await
                    {
                        error!(target: "network", ?id, error = %e, "failed to broadcast message to peer");
                    }
                }
            }
        }
    }
}
