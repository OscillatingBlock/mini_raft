use tokio::time::Duration;

pub struct ServerConfig {
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub address: String,
    pub total_nodes: u32,
}
