use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use lighthouse_network::PeerId;
use serde::Serialize;
use tokio::net::UnixDatagram;
use tracing::error;
use types::{ExecPayload, MainnetEthSpec, SignedBeaconBlock};

pub const INGRESS_SCHEMA_VERSION: u16 = 1;

#[derive(Debug)]
pub struct IngressPublisher {
    socket: UnixDatagram,
    destination: PathBuf,
    started: Instant,
}

impl IngressPublisher {
    pub fn new(destination: PathBuf) -> Result<Self> {
        Ok(Self {
            socket: UnixDatagram::unbound().context("failed to create ingress datagram socket")?,
            destination,
            started: Instant::now(),
        })
    }

    pub fn now(&self) -> IngressTimestamp {
        IngressTimestamp {
            wall_clock: Utc::now(),
            monotonic_ns: u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        }
    }

    pub async fn publish_block(
        &self,
        observed_at: IngressTimestamp,
        gossip_message_id: String,
        gossip_topic: String,
        source: PeerId,
        client_version: Option<String>,
        block: &SignedBeaconBlock<MainnetEthSpec>,
    ) {
        let message = block.message();
        let execution = message
            .execution_payload()
            .ok()
            .map(|payload| ExecutionPayload {
                block_hash: payload.block_hash().into_root().to_string(),
                parent_hash: payload.parent_hash().into_root().to_string(),
                block_number: payload.block_number(),
                timestamp: payload.timestamp(),
            });
        let event = ConsensusIngress {
            schema_version: INGRESS_SCHEMA_VERSION,
            wall_clock: observed_at.wall_clock,
            monotonic_ns: observed_at.monotonic_ns,
            source_peer_id: source.to_string(),
            source_client: client_version,
            gossip_message_id,
            gossip_topic,
            beacon_block_root: block.canonical_root().to_string(),
            beacon_parent_root: block.parent_root().to_string(),
            beacon_state_root: block.state_root().to_string(),
            slot: block.slot().as_u64(),
            proposer_index: block.message().proposer_index(),
            execution,
        };

        let payload = match serde_json::to_vec(&event) {
            Ok(payload) => payload,
            Err(error) => {
                error!(%error, "failed to serialize consensus ingress event");
                return;
            }
        };
        if let Err(error) = self.socket.send_to(&payload, &self.destination).await {
            // Gossip handling must never block on downstream persistence. A
            // local socket failure is explicit evidence loss and must page.
            error!(
                %error,
                socket = %self.destination.display(),
                "failed to deliver consensus ingress event"
            );
        }
    }
}

#[derive(Debug)]
pub struct IngressTimestamp {
    wall_clock: chrono::DateTime<Utc>,
    monotonic_ns: u64,
}

#[derive(Debug, Serialize)]
struct ConsensusIngress {
    schema_version: u16,
    wall_clock: chrono::DateTime<Utc>,
    monotonic_ns: u64,
    source_peer_id: String,
    source_client: Option<String>,
    gossip_message_id: String,
    gossip_topic: String,
    beacon_block_root: String,
    beacon_parent_root: String,
    beacon_state_root: String,
    slot: u64,
    proposer_index: u64,
    execution: Option<ExecutionPayload>,
}

#[derive(Debug, Serialize)]
struct ExecutionPayload {
    block_hash: String,
    parent_hash: String,
    block_number: u64,
    timestamp: u64,
}
