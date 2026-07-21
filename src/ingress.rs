use std::{
    io::ErrorKind,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use anyhow::{Context, Result};
use chrono::Utc;
use lighthouse_network::PeerId;
use serde::Serialize;
use tokio::net::UnixDatagram;
use tracing::error;
use types::{ExecPayload, MainnetEthSpec, SignedAggregateAndProof, SignedBeaconBlock};

pub const INGRESS_SCHEMA_VERSION: u16 = 2;

static DROPPED_EVIDENCE: AtomicU64 = AtomicU64::new(0);

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

    pub fn publish_block(
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
            kind: "beacon_block",
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
        self.send(&event);
    }

    pub fn publish_aggregate(
        &self,
        observed_at: IngressTimestamp,
        gossip_message_id: String,
        gossip_topic: String,
        source: PeerId,
        client_version: Option<String>,
        aggregate: &SignedAggregateAndProof<MainnetEthSpec>,
    ) {
        let message = aggregate.message();
        let attestation = message.aggregate();
        let data = attestation.data();
        let event = ConsensusIngressAggregate {
            kind: "aggregate_attestation",
            schema_version: INGRESS_SCHEMA_VERSION,
            wall_clock: observed_at.wall_clock,
            monotonic_ns: observed_at.monotonic_ns,
            source_peer_id: source.to_string(),
            source_client: client_version,
            gossip_message_id,
            gossip_topic,
            slot: data.slot.as_u64(),
            beacon_block_root: data.beacon_block_root.to_string(),
            target_epoch: data.target.epoch.as_u64(),
            target_root: data.target.root.to_string(),
            committee_index: attestation.committee_index(),
            aggregator_index: message.aggregator_index(),
            attester_count: attestation.num_set_aggregation_bits() as u64,
        };
        self.send(&event);
    }

    fn send<T: Serialize>(&self, event: &T) {
        let payload = match serde_json::to_vec(event) {
            Ok(payload) => payload,
            Err(error) => {
                error!(%error, "failed to serialize consensus ingress event");
                return;
            }
        };
        match self.socket.try_send_to(&payload, &self.destination) {
            Ok(_sent) => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                let dropped = DROPPED_EVIDENCE.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped == 1 || dropped.is_multiple_of(100) {
                    error!(
                        dropped,
                        socket = %self.destination.display(),
                        "dropped consensus ingress event because sensor socket queue is full"
                    );
                }
            }
            Err(error) => {
                error!(
                    %error,
                    socket = %self.destination.display(),
                    "failed to deliver consensus ingress event"
                );
            }
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
    kind: &'static str,
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
struct ConsensusIngressAggregate {
    kind: &'static str,
    schema_version: u16,
    wall_clock: chrono::DateTime<Utc>,
    monotonic_ns: u64,
    source_peer_id: String,
    source_client: Option<String>,
    gossip_message_id: String,
    gossip_topic: String,
    slot: u64,
    beacon_block_root: String,
    target_epoch: u64,
    target_root: String,
    committee_index: Option<u64>,
    aggregator_index: u64,
    attester_count: u64,
}

#[derive(Debug, Serialize)]
struct ExecutionPayload {
    block_hash: String,
    parent_hash: String,
    block_number: u64,
    timestamp: u64,
}
