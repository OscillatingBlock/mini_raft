use std::sync::Arc;

use anyhow::Context;
use mini_raft::{
    codec::Codec,
    config,
    network::NetworkManager,
    server::{Incoming, Outgoing, Server},
    state_machine::SimpleStateMachine,
};
use tokio::{net::TcpListener, sync::mpsc};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,mini_raft=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(target: "main", "starting server initialization and configuration loading");
    let config_path = resolve_config_path();
    info!(target: "main", %config_path, "resolved config file path");
    let config = config::ServerConfig::load_from(&config_path).context("failed to load config")?;
    let node_id = config.node_id();
    let peer_listener_address = config.peer_listener_address.clone();
    let client_listener_address = config.client_listener_address.clone();
    let peer_addresses = config.peer_node_addresses();
    let state_machine = SimpleStateMachine::new();

    let (to_network, from_server) = mpsc::channel::<Outgoing>(32);
    let (to_server, from_network) = mpsc::channel::<Incoming>(32);

    info!(target: "main", node_id = ?node_id, "instantiating core server runtime loop");
    let mut server = Server::new(
        node_id,
        config,
        Arc::new(state_machine),
        to_network,
        from_network,
    );
    tokio::spawn(async move {
        info!(target: "main", "spawning core server background task");
        server.run().await;
    });

    let codec = Codec::new();
    let network_manager = NetworkManager::new(to_server, codec, node_id);

    let nm_clone = Arc::new(network_manager);
    let nm_clone1 = nm_clone.clone();
    tokio::spawn(async move {
        info!(target: "main", "spawning server message dispatch listener background task");
        mini_raft::network::dispatch_server_msg(from_server, nm_clone1).await;
    });

    info!(target: "main", %client_listener_address, %peer_listener_address, "binding network listeners");
    let client_listener = TcpListener::bind(client_listener_address).await?;
    let peer_listener = TcpListener::bind(peer_listener_address).await?;

    let nm_clone2 = Arc::clone(&nm_clone);
    let client_listener_handle = tokio::spawn(async move {
        info!(target: "main", "starting client TCP listener task loop");
        _ = nm_clone2.listen(client_listener).await;
    });

    let nm_clone3 = nm_clone.clone();
    let peer_listener_handle = tokio::spawn(async move {
        info!(target: "main", "starting peer TCP listener task loop");
        _ = nm_clone3.listen(peer_listener).await;
    });

    let nm_clone4 = nm_clone.clone();
    info!(target: "main", "initiating outbound peer dial sequence");
    nm_clone4.dial_peers(peer_addresses, node_id).await;

    client_listener_handle.await?;
    peer_listener_handle.await?;

    Ok(())
}

fn resolve_config_path() -> String {
    // Precedence: `--config <path>` / `--config=<path>`, then MINI_RAFT_CONFIG env, else "config".
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            return args[i + 1].clone();
        }
        if let Some(rest) = args[i].strip_prefix("--config=") {
            return rest.to_string();
        }
    }
    std::env::var("MINI_RAFT_CONFIG").unwrap_or_else(|_| "config".to_string())
}
