use std::collections::HashMap;

use tokio::time::Duration;

use crate::server::NodeId;

pub struct ServerConfig {
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub address: String,
    pub total_nodes: u32,
    pub peer_addresses: HashMap<NodeId, String>,
}
