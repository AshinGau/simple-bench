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
    #[serde(default = "default_rpc_batch_size")]
    pub rpc_batch_size: usize,
    #[serde(default = "default_faucet_level")]
    pub faucet_level: usize,
    #[serde(default, deserialize_with = "from_opt_eth_to_u256")]
    pub faucet_eth_per_level: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Native,
    Erc20,
}

fn default_rpc_batch_size() -> usize {
    64
}

fn default_faucet_level() -> usize {
    10
}

pub const NATIVE_TRANSFER_GAS_LIMIT: u64 = 21_000;
pub const ERC20_TRANSFER_GAS_LIMIT: u64 = 100_000;
pub const ERC20_DEPLOY_GAS_LIMIT: u64 = 1_500_000;

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

fn from_opt_eth_to_u256<'de, D>(deserializer: D) -> Result<Option<U256>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<f64> = Option::deserialize(deserializer)?;
    Ok(v.map(|eth| U256::from((eth * 1e18) as u128)))
}

/// Derive num_accounts deterministic private keys from faucet key.
pub fn derive_worker_keys(faucet_key: &str, num_accounts: usize) -> Vec<String> {
    derive_xored_keys(faucet_key, num_accounts, false)
}

/// Derive faucet_level intermediate account keys (XOR first 4 bytes, orthogonal to worker keys).
pub fn derive_intermediate_keys(faucet_key: &str, level: usize) -> Vec<String> {
    derive_xored_keys(faucet_key, level, true)
}

fn derive_xored_keys(faucet_key: &str, count: usize, xor_prefix: bool) -> Vec<String> {
    let base = faucet_key.trim_start_matches("0x");
    let base_bytes = hex::decode(base).expect("invalid faucet private key");
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let mut key_bytes = base_bytes.clone();
        let idx = key_derivation_index(i).to_be_bytes();
        let start = if xor_prefix { 0 } else { key_bytes.len() - 4 };
        for j in 0..4 {
            key_bytes[start + j] ^= idx[j];
        }
        keys.push(format!("0x{}", hex::encode(&key_bytes)));
    }
    keys
}

fn key_derivation_index(i: usize) -> u32 {
    if i == 0 {
        u32::MAX
    } else {
        i as u32
    }
}

impl BenchConfig {
    pub fn faucet_eth_per_level_or_default(&self, faucet_eth_balance: U256) -> U256 {
        self.faucet_eth_per_level
            .unwrap_or_else(|| faucet_eth_balance / U256::from(self.faucet_level))
    }

    pub fn clamped_faucet_level(&self) -> usize {
        self.faucet_level.min(self.num_accounts)
    }

    pub fn transfer_gas_limit(&self) -> u64 {
        match self.transfer_type {
            TransferType::Native => NATIVE_TRANSFER_GAS_LIMIT,
            TransferType::Erc20 => ERC20_TRANSFER_GAS_LIMIT,
        }
    }

    pub fn transfer_amount(&self) -> U256 {
        U256::from(1)
    }

    pub fn transfer_native_value(&self) -> U256 {
        match self.transfer_type {
            TransferType::Native => self.transfer_amount(),
            TransferType::Erc20 => U256::ZERO,
        }
    }
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
        if config.bench.transfer_type == TransferType::Erc20 && config.bench.num_tokens == 0 {
            anyhow::bail!("bench.num_tokens must be greater than 0 when transfer_type=erc20");
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{derive_intermediate_keys, derive_worker_keys};

    const FAUCET_KEY: &str = "0xa276f0bd98df14e4f795e38f25fd5424f07b7f57d0cadb09a7203b5fca723bdc";

    #[test]
    fn worker_keys_do_not_include_faucet_key_and_are_unique() {
        let keys = derive_worker_keys(FAUCET_KEY, 8);
        assert_eq!(keys.len(), 8);
        assert_ne!(keys[0], FAUCET_KEY);
        assert!(!keys.iter().any(|key| key == FAUCET_KEY));

        let unique: HashSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len());
    }

    #[test]
    fn intermediate_keys_do_not_include_faucet_key_and_are_unique() {
        let keys = derive_intermediate_keys(FAUCET_KEY, 8);
        assert_eq!(keys.len(), 8);
        assert_ne!(keys[0], FAUCET_KEY);
        assert!(!keys.iter().any(|key| key == FAUCET_KEY));

        let unique: HashSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len());
    }
}
