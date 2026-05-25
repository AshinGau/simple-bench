use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;

use crate::rpc::RpcClient;

#[derive(Clone)]
pub struct Account {
    pub key: String,
    pub address: Address,
    pub nonce: u64,
}

impl Account {
    pub fn from_key(key: &str) -> Result<Self> {
        let key = key.trim_start_matches("0x");
        let signer: PrivateKeySigner = key.parse()?;
        Ok(Self {
            key: key.to_string(),
            address: signer.address(),
            nonce: 0,
        })
    }

    pub async fn balance(&self, rpc: &RpcClient) -> Result<U256> {
        rpc.get_balance(self.address).await
    }

    pub fn signer(&self) -> Result<PrivateKeySigner> {
        Ok(self.key.trim_start_matches("0x").parse()?)
    }
}

pub fn faucet_address(key: &str) -> Result<Address> {
    let key = key.trim_start_matches("0x");
    let signer: PrivateKeySigner = key.parse()?;
    Ok(signer.address())
}
