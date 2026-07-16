# Forkt Lighthouse sentry

Standalone Ethereum Mainnet consensus-gossip ingress for Forkt. It pins
Lighthouse networking to v8.2.0 and does not run a beacon database, fork choice,
validator duties, or execution client.

The sentry:

1. verifies a configured Beacon API against the Mainnet genesis validators root;
2. uses that API only to maintain status-handshake head/finality fields;
3. joins Lighthouse discv5/libp2p consensus networking;
4. subscribes to `beacon_block` gossip;
5. records decoded-gossip delivery time and the first forwarding peer;
6. SSZ-decodes beacon and execution references;
7. sends a versioned JSON datagram to `forkt-sensor`.

Messages are observed as `ssz_decoded`, not proposer-signature validated, and
are not relayed. The timestamp is captured before Forkt processing or consensus
validation, but after libp2p framing, Snappy decompression, duplicate filtering,
and SSZ decoding.

## Run

Start `forkt-sensor` first so the ingress socket exists.

```bash
cp .env.example .env
# Set FORKT_BEACON_API_URL.
cargo run --release
```

Expose TCP/UDP 9000 and UDP 9001 for best peer connectivity. Persist
`FORKT_CONSENSUS_DATA_DIR` to retain the libp2p identity.

## Checks

```bash
cargo fmt --check
cargo check
cargo test
```
