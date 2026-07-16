use std::{env, net::Ipv4Addr, path::PathBuf};

use anyhow::{Context, Result, bail};
use url::Url;

#[derive(Clone, Debug)]
pub struct Config {
    pub beacon_api_url: Url,
    pub ingress_socket: PathBuf,
    pub data_dir: PathBuf,
    pub listen_address: Ipv4Addr,
    pub enr_address: Option<Ipv4Addr>,
    pub tcp_port: u16,
    pub discovery_port: u16,
    pub quic_port: u16,
    pub target_peers: usize,
    pub extra_boot_enrs: Vec<String>,
    pub log_filter: String,
    pub log_json: bool,
}

impl Config {
    pub fn load() -> Result<Self> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(dotenvy::Error::Io(ref error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("failed to load .env"),
        }
        let beacon_api_url: Url = required("FORKT_BEACON_API_URL")?
            .parse()
            .context("FORKT_BEACON_API_URL is not a valid URL")?;
        if !matches!(beacon_api_url.scheme(), "http" | "https") {
            bail!("FORKT_BEACON_API_URL must use http or https");
        }

        let listen_address = parse("FORKT_CONSENSUS_LISTEN_ADDRESS", "0.0.0.0")?;
        let enr_address = env::var("FORKT_CONSENSUS_ENR_ADDRESS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.parse())
            .transpose()
            .context("FORKT_CONSENSUS_ENR_ADDRESS is not a valid IPv4 address")?;

        let config = Self {
            beacon_api_url,
            ingress_socket: env::var("FORKT_INGRESS_SOCKET")
                .unwrap_or_else(|_| "/run/forkt/consensus.sock".to_owned())
                .into(),
            data_dir: env::var("FORKT_CONSENSUS_DATA_DIR")
                .unwrap_or_else(|_| "./data/consensus".to_owned())
                .into(),
            listen_address,
            enr_address,
            tcp_port: parse("FORKT_CONSENSUS_TCP_PORT", "9000")?,
            discovery_port: parse("FORKT_CONSENSUS_DISCOVERY_PORT", "9000")?,
            quic_port: parse("FORKT_CONSENSUS_QUIC_PORT", "9001")?,
            target_peers: parse("FORKT_CONSENSUS_PEER_TARGET", "100")?,
            extra_boot_enrs: split_csv(&env::var("FORKT_CONSENSUS_BOOT_ENRS").unwrap_or_default()),
            log_filter: env::var("FORKT_LOG")
                .unwrap_or_else(|_| "info,lighthouse_network=info".to_owned()),
            log_json: parse("FORKT_LOG_JSON", "false")?,
        };
        if !(1..=500).contains(&config.target_peers) {
            bail!("FORKT_CONSENSUS_PEER_TARGET must be between 1 and 500");
        }
        if config.ingress_socket.as_os_str().is_empty() {
            bail!("FORKT_INGRESS_SOCKET must not be empty");
        }
        Ok(config)
    }
}

fn required(key: &'static str) -> Result<String> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{key} is required"))
}

fn parse<T>(key: &'static str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env::var(key)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .map_err(|error| anyhow::anyhow!("{key} is invalid: {error}"))
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
