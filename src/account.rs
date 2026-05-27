use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;

use crate::monitor::TxReceipt;
use crate::rpc::RpcClient;

#[derive(Clone)]
pub struct Account {
    pub key: String,
    pub address: Address,
    pub nonce: u64,
    pub balance: U256,
}

impl Account {
    pub fn from_key(key: &str) -> Result<Self> {
        let key = key.trim_start_matches("0x");
        let signer: PrivateKeySigner = key.parse()?;
        Ok(Self {
            key: key.to_string(),
            address: signer.address(),
            nonce: 0,
            balance: U256::ZERO,
        })
    }

    pub async fn balance(&self, rpc: &RpcClient) -> Result<U256> {
        rpc.get_balance(self.address).await
    }

    pub fn signer(&self) -> Result<PrivateKeySigner> {
        Ok(self.key.trim_start_matches("0x").parse()?)
    }

    /// 估算一笔 tx 的 gas 开销（用于发送前余额充足检查）
    pub fn estimated_gas_cost(max_fee_per_gas: u64, gas_limit: u64) -> U256 {
        U256::from(max_fee_per_gas) * U256::from(gas_limit) * U256::from(1_000_000_000u64)
    }

    /// 用 receipt 更新本地 nonce 和 balance
    pub fn apply_receipt(&mut self, receipt: &TxReceipt, value: U256) {
        self.nonce += 1;
        let gas_cost = U256::from(receipt.gas_used) * U256::from(receipt.effective_gas_price);
        self.balance = self.balance.saturating_sub(gas_cost + value);
    }
}

pub fn faucet_address(key: &str) -> Result<Address> {
    let key = key.trim_start_matches("0x");
    let signer: PrivateKeySigner = key.parse()?;
    Ok(signer.address())
}
