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
    to_server: Arc<Mutex<mpsc::Sender<Incoming>>>,
    from_server: mpsc::Receiver<Outgoing>,
    id: NodeId,
}

pub struct ConnectionRegistry {
    peer_registry: Arc<Mutex<HashMap<NodeId, mpsc::Sender<Message>>>>,
    client_registry: Arc<Mutex<HashMap<ClientId, mpsc::Sender<Message>>>>,
}

use tracing::{debug, error, info, instrument, warn};

impl NetworkManager {
    #[instrument(skip(self))]
    async fn dispatch_msg_from_server(&mut self) {
        while let Some(msg) = self.from_server.recv().await {
            match msg.dest {
                Destination::Node(node_id) => {
                    let id = Id::Peer(node_id);
                    debug!(target: "network", node_id = ?node_id, "dispatching server message to peer node");
                    self.dispatch_msg(id, msg.msg).await;
                }
                Destination::Client(client_id) => {
                    let id = Id::Client(client_id);
                    debug!(target: "network", client_id = ?client_id, "dispatching server message to client");
                    self.dispatch_msg(id, msg.msg).await;
                }
                Destination::Broadcast => {
                    let guard = self.connections_map.peer_registry.lock().await;
                    let nodes: Vec<_> = guard.keys().cloned().collect();
                    drop(guard);
                    info!(target: "network", peer_count = nodes.len(), "broadcasting message to all connected peers");
                    for node_id in nodes {
                        self.dispatch_msg(Id::Peer(node_id), msg.msg.clone()).await;
                    }
                }
            }
        }
    }

    #[instrument(skip(self, msg), fields(node_id = ?self.id))]
    async fn dispatch_msg(&self, id: Id, msg: Message) {
        let client_guard = self.connections_map.client_registry.lock().await;
        let peer_guard = self.connections_map.peer_registry.lock().await;

        let tx = match id {
            Id::Client(client_id) => client_guard.get(&client_id),
            Id::Peer(peer_id) => peer_guard.get(&peer_id),
        };
        let Some(tx) = tx else {
            warn!(target: "network", ?id, "failed to dispatch message: writer channel not available");
            return;
        };

        if let Err(e) = tx.send(msg).await {
            error!(target: "network", ?id, error = %e, "connection handler exited and dropped write channel end");
        }
    }

    #[instrument(skip(self, listener), fields(node_id = ?self.id))]
    async fn listener(&self, listener: TcpListener) -> anyhow::Result<()> {
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
            let mut framed = Framed::new(conn, self.codec.clone());

            let id = match self.perform_handshake(&mut framed).await {
                Ok(Some(id)) => id,
                Ok(None) => {
                    warn!(target: "network", ?addr, "connection closed gracefully during handshake without identification");
                    continue;
                }
                Err(e) => {
                    error!(target: "network", ?addr, error = %e, "handshake process failed for incoming connection");
                    continue;
                }
            };

            info!(target: "network", ?id, ?addr, "handshake successful, spawning connection handlers");
            self.spawn_connection_handling_tasks(framed, id).await;
        }
    }

    #[instrument(skip(self, conn), fields(node_id = ?self.id))]
    async fn perform_handshake(
        &self,
        conn: &mut Framed<TcpStream, Codec>,
    ) -> anyhow::Result<Option<Id>> {
        debug!(target: "network", "sending hello message for handshake initialization");
        conn.send(Message::HelloType(Hello { id: self.id })).await?;

        let handshake_timeout = Duration::from_secs(5);
        let msg_future = timeout(handshake_timeout, conn.next());

        let msg = match msg_future.await {
            Ok(Some(Ok(m))) => m,
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
        };

        let id = match msg {
            Message::HelloType(hello) => {
                info!(target: "network", peer_id = ?hello.id, "identified remote connection as peer node");
                Id::Peer(hello.id)
            }
            Message::HelloClientType(hello) => {
                info!(target: "network", client_id = ?hello.id, "identified remote connection as client");
                Id::Client(hello.id)
            }
            other => {
                warn!(target: "network", message_type = ?other, "received unexpected message type during handshake");
                anyhow::bail!("unexpected message type in handshake")
            }
        };

        Ok(Some(id))
    }

    #[instrument(skip(self, peer_addresses), fields(node_id = ?current_node_id))]
    async fn dial_peers(&self, peer_addresses: HashMap<NodeId, String>, current_node_id: NodeId) {
        for (peer_id, addr) in peer_addresses {
            if peer_id > current_node_id {
                debug!(target: "network", peer_id = ?peer_id, %addr, "attempting to dial remote peer");
                let conn = match TcpStream::connect(&addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(target: "network", peer_id = ?peer_id, %addr, error = %e, "failed to establish TCP connection with peer");
                        continue;
                    }
                };

                let mut framed = Framed::new(conn, self.codec.clone());
                match self.perform_handshake(&mut framed).await {
                    Ok(Some(peer_node_id)) => {
                        info!(target: "network", peer_id = ?peer_node_id, "peer dial and handshake completed successfully");
                        self.spawn_connection_handling_tasks(framed, peer_node_id)
                            .await;
                    }
                    Ok(None) => {
                        warn!(target: "network", peer_id = ?peer_id, "peer closed connection during outbound handshake");
                    }
                    Err(e) => {
                        error!(target: "network", peer_id = ?peer_id, error = %e, "outbound handshake failed");
                    }
                }
            }
        }
    }

    #[instrument(skip(self, conn), fields(node_id = ?self.id, ?id))]
    async fn spawn_connection_handling_tasks(&self, conn: Framed<TcpStream, Codec>, id: Id) {
        let (tx, rx) = mpsc::channel::<Message>(32);
        match id {
            Id::Client(client_id) => {
                self.connections_map
                    .client_registry
                    .lock()
                    .await
                    .insert(client_id, tx);
                debug!(target: "network", ?client_id, "registered client sender channel");
            }
            Id::Peer(peer_id) => {
                self.connections_map
                    .peer_registry
                    .lock()
                    .await
                    .insert(peer_id, tx);
                debug!(target: "network", ?peer_id, "registered peer sender channel");
            }
        }

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
    to_server: Arc<Mutex<mpsc::Sender<Incoming>>>,
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

        if let Err(e) = to_server.lock().await.send(incoming_msg).await {
            error!(target: "network", ?id, error = %e, "server input channel closed, terminating reader loop");
            return;
        }
    }
    warn!(target: "network", ?id, "remote stream ended (EOF)");
}

#[instrument(skip(framed_sink, from_server))]
async fn framed_writer(
    mut framed_sink: SplitSink<Framed<TcpStream, Codec>, Message>,
    mut from_server: mpsc::Receiver<Message>,
) {
    while let Some(msg) = from_server.recv().await {
        debug!(target: "network", "sending message frame over TCP stream");
        if let Err(e) = framed_sink.send(msg).await {
            error!(target: "network", error = %e, "failed to send message frame over TCP sink, writer exiting");
            break;
        }
    }
    debug!(target: "network", "server-to-connection channel dropped, writer terminating");
}
