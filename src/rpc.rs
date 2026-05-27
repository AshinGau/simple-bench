use alloy::primitives::{Address, Bytes, B256, U256};
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct RpcClient {
    pub url: String,
    client: reqwest::Client,
    rpc_permits: Arc<tokio::sync::Semaphore>,
}

impl RpcClient {
    pub fn new(url: &str, _chain_id: u64, rpc_concurrency: usize) -> Self {
        Self {
            url: url.to_string(),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            rpc_permits: Arc::new(tokio::sync::Semaphore::new(rpc_concurrency)),
        }
    }

    async fn post_json<T, R>(&self, body: &T) -> Result<R>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let _permit = self.rpc_permits.acquire().await?;
        Ok(self
            .client
            .post(&self.url)
            .json(body)
            .send()
            .await?
            .json::<R>()
            .await?)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({"jsonrpc":"2.0","method":method,"params":params,"id":1});
        let resp: Value = self.post_json(&body).await?;
        if let Some(err) = resp.get("error") {
            anyhow::bail!("RPC error: {}", err);
        }
        Ok(resp["result"].clone())
    }

    pub async fn txpool_status(&self) -> Result<(u64, u64)> {
        let r = self.call("txpool_status", json!([])).await?;
        let pending = u64::from_str_radix(
            r["pending"]
                .as_str()
                .unwrap_or("0x0")
                .trim_start_matches("0x"),
            16,
        )?;
        let queued = u64::from_str_radix(
            r["queued"]
                .as_str()
                .unwrap_or("0x0")
                .trim_start_matches("0x"),
            16,
        )?;
        Ok((pending, queued))
    }

    pub async fn block_number(&self) -> Result<u64> {
        let r = self.call("eth_blockNumber", json!([])).await?;
        Ok(u64::from_str_radix(
            r.as_str().unwrap_or("0x0").trim_start_matches("0x"),
            16,
        )?)
    }

    pub async fn batch_get_block_tx_hashes(&self, block_nums: &[u64]) -> Result<Vec<Vec<B256>>> {
        if block_nums.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = block_nums
            .iter()
            .enumerate()
            .map(|(i, block_num)| {
                json!({"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[format!("0x{:x}", block_num), false],"id":i+1})
            })
            .collect();
        let resp: Vec<Value> = self.post_json(&calls).await?;
        let ordered = order_batch_responses(resp, block_nums.len())?;
        ordered
            .iter()
            .enumerate()
            .map(|(i, r)| parse_block_tx_hashes(block_nums[i], &r["result"]))
            .collect()
    }

    pub async fn batch_get_receipts(&self, hashes: &[B256]) -> Result<Vec<Option<Value>>> {
        if hashes.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = hashes.iter().enumerate().map(|(i, h)| {
            json!({"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":[format!("0x{}", hex::encode(h))],"id":i+1})
        }).collect();
        let resp: Vec<Value> = self.post_json(&calls).await?;
        let ordered = order_batch_responses(resp, hashes.len())?;
        Ok(ordered
            .iter()
            .map(|r| {
                if r["result"].is_null() {
                    None
                } else {
                    Some(r["result"].clone())
                }
            })
            .collect())
    }

    pub async fn get_balance(&self, addr: Address) -> Result<U256> {
        let r = self
            .call("eth_getBalance", json!([format!("0x{:x}", addr), "latest"]))
            .await?;
        Ok(U256::from_str_radix(
            r.as_str().unwrap_or("0x0").trim_start_matches("0x"),
            16,
        )?)
    }

    pub async fn batch_has_code(&self, addrs: &[Address]) -> Result<Vec<bool>> {
        if addrs.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                json!({"jsonrpc":"2.0","method":"eth_getCode","params":[format!("0x{:x}", addr), "latest"],"id":i+1})
            })
            .collect();
        let resp: Vec<Value> = self.post_json(&calls).await?;
        let ordered = order_batch_responses(resp, addrs.len())?;
        Ok(ordered
            .iter()
            .map(|r| {
                !r["result"]
                    .as_str()
                    .unwrap_or("0x")
                    .trim_start_matches("0x")
                    .is_empty()
            })
            .collect())
    }

    pub async fn get_nonce(&self, addr: Address) -> Result<u64> {
        let r = self
            .call(
                "eth_getTransactionCount",
                json!([format!("0x{:x}", addr), "pending"]),
            )
            .await?;
        Ok(u64::from_str_radix(
            r.as_str().unwrap_or("0x0").trim_start_matches("0x"),
            16,
        )?)
    }

    pub async fn batch_send_raw_txs(&self, raws: &[Bytes]) -> Result<Vec<B256>> {
        if raws.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = raws.iter().enumerate().map(|(i, raw)| {
            json!({"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":[format!("0x{}", hex::encode(raw))],"id":i+1})
        }).collect();
        let resp: Vec<Value> = self.post_json(&calls).await?;
        let ordered = order_batch_responses(resp, raws.len())?;
        ordered.iter().map(|r| parse_b256(&r["result"])).collect()
    }

    pub async fn batch_get_nonces(&self, addrs: &[Address]) -> Result<Vec<u64>> {
        if addrs.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = addrs.iter().enumerate().map(|(i, addr)| {
            json!({"jsonrpc":"2.0","method":"eth_getTransactionCount","params":[format!("0x{:x}", addr), "pending"],"id":i+1})
        }).collect();
        let resp: Vec<Value> = self.post_json(&calls).await?;
        let ordered = order_batch_responses(resp, addrs.len())?;
        ordered
            .iter()
            .map(|r| {
                let s = r["result"].as_str().unwrap_or("0x0");
                Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
            })
            .collect::<Result<Vec<_>>>()
    }

    pub async fn batch_get_balances(&self, addrs: &[Address]) -> Result<Vec<U256>> {
        if addrs.is_empty() {
            return Ok(vec![]);
        }
        let calls: Vec<Value> = addrs.iter().enumerate().map(|(i, addr)| {
            json!({"jsonrpc":"2.0","method":"eth_getBalance","params":[format!("0x{:x}", addr), "latest"],"id":i+1})
        }).collect();
        let resp: Vec<Value> = self.post_json(&calls).await?;
        let ordered = order_batch_responses(resp, addrs.len())?;
        ordered
            .iter()
            .map(|r| {
                let s = r["result"].as_str().unwrap_or("0x0");
                Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
            })
            .collect::<Result<Vec<_>>>()
    }
}

fn parse_block_tx_hashes(block_num: u64, block: &Value) -> Result<Vec<B256>> {
    if block.is_null() {
        anyhow::bail!("block {} not found", block_num);
    }
    let txs = block["transactions"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("block {} has no transactions array", block_num))?;
    let mut hashes = Vec::with_capacity(txs.len());
    for tx in txs {
        hashes.push(parse_b256(tx)?);
    }
    Ok(hashes)
}

fn parse_b256(value: &Value) -> Result<B256> {
    let hash_str = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("expected hash string"))?;
    let bytes: [u8; 32] = hex::decode(hash_str.trim_start_matches("0x"))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid tx hash"))?;
    Ok(B256::from(bytes))
}

fn order_batch_responses(resp: Vec<Value>, expected: usize) -> Result<Vec<Value>> {
    let mut ordered: Vec<Option<Value>> = (0..expected).map(|_| None).collect();
    for item in resp {
        if let Some(err) = item.get("error") {
            anyhow::bail!("RPC error: {}", err);
        }
        let id = item
            .get("id")
            .and_then(|id| id.as_u64())
            .ok_or_else(|| anyhow::anyhow!("missing id in batch response"))?;
        if id < 1 || id as usize > expected {
            anyhow::bail!("unexpected batch response id: {}", id);
        }
        ordered[(id - 1) as usize] = Some(item);
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            v.ok_or_else(|| anyhow::anyhow!("missing response for batch index {}", i + 1))
        })
        .collect()
}
