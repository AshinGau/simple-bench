use alloy::primitives::U256;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub faucet: FaucetConfig,
    pub rpc: RpcConfig,
    pub bench: BenchConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FaucetConfig {
    pub private_key: String,
    #[serde(deserialize_with = "from_eth_to_u256")]
    pub faucet_eth_balance: U256,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchConfig {
    pub num_accounts: usize,
    pub rpc_concurrency: usize,
    pub num_inflight_senders: usize,
    pub transfer_type: TransferType,
    pub max_fee_per_gas: u64,
    pub max_priority_fee_per_gas: u64,
    #[serde(default)]
    pub num_tokens: usize,
    pub max_pool_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Native,
    Erc20,
}

/// Parse ETH amount (number or string) to wei.
fn from_eth_to_u256<'de, D>(deserializer: D) -> Result<U256, D::Error>
where
    D: Deserializer<'de>,
{
    struct EthVisitor;

    impl<'de> de::Visitor<'de> for EthVisitor {
        type Value = U256;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("ETH amount as number or string")
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<U256, E> {
            Ok(U256::from(v as u128 * 1_000_000_000_000_000_000u128))
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<U256, E> {
            Ok(U256::from(v as u128 * 1_000_000_000_000_000_000u128))
        }

        fn visit_f64<E: de::Error>(self, v: f64) -> Result<U256, E> {
            Ok(U256::from((v * 1e18) as u128))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<U256, E> {
            let eth: f64 = v.parse().map_err(de::Error::custom)?;
            Ok(U256::from((eth * 1e18) as u128))
        }
    }

    deserializer.deserialize_any(EthVisitor)
}

/// Derive num_accounts deterministic private keys from faucet key.
pub fn derive_worker_keys(faucet_key: &str, num_accounts: usize) -> Vec<String> {
    let base = faucet_key.trim_start_matches("0x");
    let base_bytes = hex::decode(base).expect("invalid faucet private key");
    let mut keys = Vec::with_capacity(num_accounts);
    for i in 0..num_accounts {
        let mut key_bytes = base_bytes.clone();
        // XOR last 4 bytes with index to create deterministic but unique keys
        let idx = (i as u32).to_be_bytes();
        let len = key_bytes.len();
        for j in 0..4 {
            key_bytes[len - 4 + j] ^= idx[j];
        }
        keys.push(format!("0x{}", hex::encode(&key_bytes)));
    }
    keys
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let config: Self = toml::from_str(&content)?;
        if config.bench.rpc_concurrency == 0 {
            anyhow::bail!("bench.rpc_concurrency must be greater than 0");
        }
        if config.bench.num_inflight_senders == 0 {
            anyhow::bail!("bench.num_inflight_senders must be greater than 0");
        }
        Ok(config)
    }
}
