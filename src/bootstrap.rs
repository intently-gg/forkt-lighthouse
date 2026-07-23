use std::str::FromStr;

use anyhow::{Context, Result, bail};
use lighthouse_network::rpc::methods::{StatusMessage, StatusMessageV2};
use reqwest::Client;
use serde::Deserialize;
use types::{ChainSpec, Epoch, EthSpec, Hash256, MainnetEthSpec, Slot};
use url::Url;

#[derive(Clone, Debug)]
pub struct BeaconBootstrap {
    client: Client,
    base_url: Url,
    genesis_validators_root: Hash256,
}

/// Cached Beacon API fields that change slowly relative to slot time.
#[derive(Clone, Debug)]
pub struct StatusCheckpoints {
    pub finalized_epoch: Epoch,
    pub finalized_root: Hash256,
    pub head_root: Hash256,
}

impl BeaconBootstrap {
    pub async fn connect(base_url: Url, expected_genesis_root: Hash256) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent(concat!("forkt-lighthouse/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let bootstrap = Self {
            client,
            base_url,
            genesis_validators_root: expected_genesis_root,
        };
        let genesis: ApiResponse<GenesisData> = bootstrap.get("eth/v1/beacon/genesis").await?;
        let observed = parse_hash(&genesis.data.genesis_validators_root)?;
        if observed != expected_genesis_root {
            bail!("beacon API genesis validators root is not Ethereum Mainnet");
        }
        Ok(bootstrap)
    }

    /// Fetch finalized checkpoint + current head root from the Beacon API.
    /// Call infrequently; pair with a wall-clock `head_slot` via [`build_status`].
    pub async fn fetch_checkpoints(&self) -> Result<StatusCheckpoints> {
        let (head, finality): (ApiResponse<HeaderData>, ApiResponse<FinalityData>) =
            tokio::try_join!(
                self.get("eth/v1/beacon/headers/head"),
                self.get("eth/v1/beacon/states/head/finality_checkpoints"),
            )?;
        Ok(StatusCheckpoints {
            finalized_epoch: Epoch::new(parse_u64(&finality.data.finalized.epoch)?),
            finalized_root: parse_hash(&finality.data.finalized.root)?,
            head_root: parse_hash(&head.data.root)?,
        })
    }

    pub fn build_status(
        &self,
        spec: &ChainSpec,
        head_slot: Slot,
        checkpoints: &StatusCheckpoints,
    ) -> StatusMessage {
        let fork_digest = spec.compute_fork_digest(
            self.genesis_validators_root,
            head_slot.epoch(MainnetEthSpec::slots_per_epoch()),
        );
        StatusMessage::V2(StatusMessageV2 {
            fork_digest,
            finalized_root: checkpoints.finalized_root,
            finalized_epoch: checkpoints.finalized_epoch,
            head_root: checkpoints.head_root,
            head_slot,
            // Network-only sentry: advertise no historical blocks available.
            earliest_available_slot: Slot::new(head_slot.as_u64().saturating_add(1)),
        })
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.base_url.join(path)?;
        self.client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("beacon API returned malformed JSON")
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct GenesisData {
    genesis_validators_root: String,
}

#[derive(Debug, Deserialize)]
struct HeaderData {
    root: String,
}

#[derive(Debug, Deserialize)]
struct FinalityData {
    finalized: Checkpoint,
}

#[derive(Debug, Deserialize)]
struct Checkpoint {
    epoch: String,
    root: String,
}

fn parse_hash(value: &str) -> Result<Hash256> {
    Hash256::from_str(value)
        .map_err(|error| anyhow::anyhow!("invalid hash from beacon API: {error}"))
}

fn parse_u64(value: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid integer from beacon API: {error}"))
}
