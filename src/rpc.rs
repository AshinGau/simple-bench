use alloy::primitives::{Address, Bytes, B256, U256};
use anyhow::Result;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct RpcClient {
    pub url: String,
    pub chain_id: u64,
    client: reqwest::Client,
}

impl RpcClient {
    pub fn new(url: &str, chain_id: u64) -> Self {
        Self {
            url: url.to_string(),
            chain_id,
            client: reqwest::Client::new(),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let resp: Value = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = resp.get("error") {
            anyhow::bail!("RPC error: {}", err);
        }
        Ok(resp["result"].clone())
    }

    pub async fn txpool_status(&self) -> Result<(u64, u64)> {
        let r = self.call("txpool_status", json!([])).await?;
        let pending = u64::from_str_radix(r["pending"].as_str().unwrap_or("0x0").trim_start_matches("0x"), 16)?;
        let queued = u64::from_str_radix(r["queued"].as_str().unwrap_or("0x0").trim_start_matches("0x"), 16)?;
        Ok((pending, queued))
    }

    pub async fn block_number(&self) -> Result<u64> {
        let r = self.call("eth_blockNumber", json!([])).await?;
        Ok(u64::from_str_radix(
            r.as_str().unwrap_or("0x0").trim_start_matches("0x"),
            16,
        )?)
    }

    pub async fn get_block_tx_hashes(&self, block_num: u64) -> Result<Vec<B256>> {
        let r = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{:x}", block_num), false]),
            )
            .await?;
        let txs = r["transactions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut hashes = Vec::with_capacity(txs.len());
        for tx in txs {
            let hash_str = tx.as_str().unwrap_or("0x");
            let hash_bytes: [u8; 32] = hex::decode(hash_str.trim_start_matches("0x"))?
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid tx hash length"))?;
            hashes.push(B256::from(hash_bytes));
        }
        Ok(hashes)
    }

    pub async fn get_transaction_receipt(&self, hash: B256) -> Result<Option<Value>> {
        let r = self
            .call(
                "eth_getTransactionReceipt",
                json!([format!("0x{}", hex::encode(hash))]),
            )
            .await?;
        if r.is_null() {
            Ok(None)
        } else {
            Ok(Some(r))
        }
    }

    pub async fn batch_get_receipts(&self, hashes: &[B256]) -> Result<Vec<Option<Value>>> {
        if hashes.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| {
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionReceipt",
                    "params": [format!("0x{}", hex::encode(h))],
                    "id": i + 1
                })
            })
            .collect();
        let resp: Vec<Value> = self.client.post(&self.url).json(&calls).send().await?.json().await?;
        let mut results = Vec::with_capacity(hashes.len());
        for r in resp {
            let result = &r["result"];
            if result.is_null() {
                results.push(None);
            } else {
                results.push(Some(result.clone()));
            }
        }
        Ok(results)
    }

    pub async fn get_balance(&self, addr: Address) -> Result<U256> {
        let r = self
            .call("eth_getBalance", json!([format!("0x{:x}", addr), "latest"]))
            .await?;
        let s = r.as_str().unwrap_or("0x0");
        Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
    }

    pub async fn get_nonce(&self, addr: Address) -> Result<u64> {
        let r = self
            .call(
                "eth_getTransactionCount",
                json!([format!("0x{:x}", addr), "latest"]),
            )
            .await?;
        Ok(u64::from_str_radix(
            r.as_str().unwrap_or("0x0").trim_start_matches("0x"),
            16,
        )?)
    }

    pub async fn send_raw_tx(&self, raw: &Bytes) -> Result<B256> {
        let r = self
            .call(
                "eth_sendRawTransaction",
                json!([format!("0x{}", hex::encode(raw))]),
            )
            .await?;
        let hash_str = r.as_str().unwrap_or("0x");
        let hash_bytes: [u8; 32] = hex::decode(hash_str.trim_start_matches("0x"))?
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid tx hash"))?;
        Ok(B256::from(hash_bytes))
    }
}
