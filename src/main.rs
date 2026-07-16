mod bootstrap;
mod config;
mod ingress;

use std::{
    str::FromStr,
    sync::{Arc, Weak},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, bail};
use bootstrap::BeaconBootstrap;
use config::Config;
use eth2_network_config::Eth2NetworkConfig;
use futures::channel::mpsc;
use ingress::IngressPublisher;
use lighthouse_network::{
    Context as NetworkContext, Enr, MessageAcceptance, NetworkConfig, NetworkEvent, PeerId,
    Response, SyncInfo, SyncStatus,
    rpc::{RequestType, methods::StatusMessage},
    service::{Network, api_types::AppRequestId},
};
use tokio::{
    runtime::Runtime,
    sync::watch,
    time::{Duration, MissedTickBehavior},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use types::{ChainSpec, ForkContext, MainnetEthSpec, Slot};

fn main() -> Result<()> {
    let config = Config::load()?;
    init_tracing(&config)?;
    let runtime = Arc::new(Runtime::new()?);
    let runtime_ref = Arc::clone(&runtime);
    runtime.block_on(run(config, Arc::downgrade(&runtime_ref)))
}

async fn run(config: Config, runtime: Weak<Runtime>) -> Result<()> {
    let network_definition = Eth2NetworkConfig::constant("mainnet")
        .map_err(anyhow::Error::msg)?
        .context("Lighthouse was built without Ethereum Mainnet constants")?;
    let spec = Arc::new(
        network_definition
            .chain_spec::<MainnetEthSpec>()
            .map_err(anyhow::Error::msg)?,
    );
    let genesis_root = network_definition
        .genesis_validators_root::<MainnetEthSpec>()
        .map_err(anyhow::Error::msg)?
        .context("Mainnet genesis validators root is unavailable")?;
    let genesis_time = network_definition
        .genesis_time::<MainnetEthSpec>()
        .map_err(anyhow::Error::msg)?
        .context("Mainnet genesis time is unavailable")?;
    let current_slot = current_slot(genesis_time, &spec)?;

    let bootstrap = BeaconBootstrap::connect(config.beacon_api_url.clone(), genesis_root).await?;
    let initial_status = bootstrap.status(&spec).await?;
    let (status_tx, status_rx) = watch::channel(initial_status);
    tokio::spawn(refresh_status(bootstrap, Arc::clone(&spec), status_tx));

    let mut network_config = NetworkConfig::default();
    network_config.network_dir = config.data_dir.clone();
    network_config.set_ipv4_listening_address(
        config.listen_address,
        config.tcp_port,
        config.discovery_port,
        config.quic_port,
    );
    network_config.enr_address = (config.enr_address, None);
    network_config.target_peers = config.target_peers;
    network_config.client_version =
        concat!("forkt-lighthouse/", env!("CARGO_PKG_VERSION")).to_owned();
    network_config.upnp_enabled = false;
    network_config.topics = vec![lighthouse_network::types::GossipKind::BeaconBlock];
    network_config.boot_nodes_enr = network_definition.boot_enr.unwrap_or_default();
    for enr in config.extra_boot_enrs {
        network_config
            .boot_nodes_enr
            .push(Enr::from_str(&enr).map_err(anyhow::Error::msg)?);
    }
    let network_config = Arc::new(network_config);
    let local_keypair = lighthouse_network::load_private_key(&network_config);

    let (executor_signal, executor_exit) = async_channel::bounded(1);
    let (shutdown_tx, _) = mpsc::channel(1);
    let executor = task_executor::TaskExecutor::new(runtime, executor_exit, shutdown_tx);
    let fork_context = Arc::new(ForkContext::new::<MainnetEthSpec>(
        current_slot,
        genesis_root,
        &spec,
    ));
    let network_context = NetworkContext {
        config: Arc::clone(&network_config),
        enr_fork_id: spec.enr_fork_id::<MainnetEthSpec>(current_slot, genesis_root),
        fork_context,
        chain_spec: Arc::clone(&spec),
        libp2p_registry: None,
    };
    let (mut network, globals) = Network::<MainnetEthSpec>::new(
        executor,
        network_context,
        spec.custody_requirement,
        local_keypair,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    let ingress = IngressPublisher::new(config.ingress_socket)?;

    info!(
        local_peer_id = %globals.local_peer_id(),
        target_peers = config.target_peers,
        "Forkt consensus sentry started"
    );
    let _executor_signal = executor_signal;

    loop {
        tokio::select! {
            _ = shutdown_signal() => {
                info!("shutdown signal received");
                return Ok(());
            }
            event = network.next_event() => {
                handle_network_event(
                    event,
                    &mut network,
                    &globals,
                    &status_rx,
                    &ingress,
                ).await;
            }
        }
    }
}

async fn handle_network_event(
    event: NetworkEvent<MainnetEthSpec>,
    network: &mut Network<MainnetEthSpec>,
    globals: &Arc<lighthouse_network::NetworkGlobals<MainnetEthSpec>>,
    status: &watch::Receiver<StatusMessage>,
    ingress: &IngressPublisher,
) {
    match event {
        NetworkEvent::PubsubMessage {
            id,
            source,
            topic,
            message,
        } => {
            let observed_at = ingress.now();
            let gossip_message_id = id.to_string();
            let gossip_topic = topic.to_string();
            network.report_message_validation_result(&source, id, MessageAcceptance::Ignore);
            if let lighthouse_network::PubsubMessage::BeaconBlock(block) = message {
                let client = globals.client(&source).to_string();
                ingress
                    .publish_block(
                        observed_at,
                        gossip_message_id,
                        gossip_topic,
                        source,
                        Some(client),
                        &block,
                    )
                    .await;
            }
        }
        NetworkEvent::StatusPeer(peer_id) => {
            send_status(network, peer_id, status.borrow().clone());
        }
        NetworkEvent::RequestReceived {
            peer_id,
            inbound_request_id,
            request_type: RequestType::Status(remote),
        } => {
            update_peer_status(globals, peer_id, &remote, &status.borrow());
            network.send_response(
                peer_id,
                inbound_request_id,
                Response::<MainnetEthSpec>::Status(status.borrow().clone()),
            );
        }
        NetworkEvent::ResponseReceived {
            peer_id,
            response: Response::Status(remote),
            ..
        } => {
            update_peer_status(globals, peer_id, &remote, &status.borrow());
        }
        NetworkEvent::PeerConnectedIncoming(peer_id)
        | NetworkEvent::PeerConnectedOutgoing(peer_id) => {
            info!(%peer_id, connected_peers = globals.connected_peers(), "peer connected");
        }
        NetworkEvent::PeerDisconnected(peer_id) => {
            info!(%peer_id, connected_peers = globals.connected_peers(), "peer disconnected");
        }
        NetworkEvent::RequestReceived {
            peer_id,
            request_type,
            ..
        } => {
            warn!(%peer_id, ?request_type, "unsupported RPC request ignored");
        }
        NetworkEvent::RPCFailed {
            peer_id,
            error: rpc_error,
            ..
        } => {
            warn!(%peer_id, error = ?rpc_error, "peer RPC failed");
        }
        NetworkEvent::NewListenAddr(address) => info!(%address, "consensus listener active"),
        NetworkEvent::ZeroListeners => error!("consensus network has no active listeners"),
        NetworkEvent::ResponseReceived { .. }
        | NetworkEvent::PartialDataColumnSidecar { .. }
        | NetworkEvent::PeerUpdatedCustodyGroupCount(_) => {}
    }
}

fn update_peer_status(
    globals: &Arc<lighthouse_network::NetworkGlobals<MainnetEthSpec>>,
    peer_id: PeerId,
    remote: &StatusMessage,
    local: &StatusMessage,
) {
    if remote.fork_digest() != local.fork_digest() {
        warn!(%peer_id, "peer status has a different fork digest");
        return;
    }
    let info = SyncInfo {
        head_slot: *remote.head_slot(),
        head_root: *remote.head_root(),
        finalized_epoch: *remote.finalized_epoch(),
        finalized_root: *remote.finalized_root(),
        earliest_available_slot: remote.earliest_available_slot().ok().copied(),
    };
    globals
        .peers
        .write()
        .update_sync_status(&peer_id, SyncStatus::Synced { info });
}

fn send_status(network: &mut Network<MainnetEthSpec>, peer_id: PeerId, status: StatusMessage) {
    if let Err((_, error)) =
        network.send_request(peer_id, AppRequestId::Router, RequestType::Status(status))
    {
        warn!(%peer_id, ?error, "failed to send status handshake");
    }
}

async fn refresh_status(
    bootstrap: BeaconBootstrap,
    spec: Arc<ChainSpec>,
    status: watch::Sender<StatusMessage>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(6));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        match bootstrap.status(&spec).await {
            Ok(next) => {
                status.send_replace(next);
            }
            Err(error) => warn!(%error, "failed to refresh beacon status"),
        }
    }
}

fn current_slot(genesis_time: u64, spec: &ChainSpec) -> Result<Slot> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if now < genesis_time {
        bail!("system clock predates Ethereum Mainnet beacon genesis");
    }
    Ok(Slot::new(
        (now - genesis_time) / spec.get_slot_duration().as_secs(),
    ))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

fn init_tracing(config: &Config) -> Result<()> {
    let filter = EnvFilter::try_new(&config.log_filter)?;
    if config.log_json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
    }
}
