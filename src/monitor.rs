use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use alloy::primitives::B256;
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

use crate::rpc::RpcClient;

pub enum MonitorCommand {
    Register(RegisterTx),
}

pub struct RegisterTx {
    pub hash: B256,
    pub reply: oneshot::Sender<TxReceipt>,
    pub registered: oneshot::Sender<()>,
}

#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub success: bool,
    pub gas_used: u64,
    pub effective_gas_price: u128,
}

pub struct BlockMonitor {
    rpc: RpcClient,
    pending: HashMap<B256, oneshot::Sender<TxReceipt>>,
    pub pool_size: Arc<AtomicU64>,
    register_rx: mpsc::Receiver<MonitorCommand>,
    rpc_batch_size: usize,
}

impl BlockMonitor {
    pub fn new(
        rpc: RpcClient,
        register_rx: mpsc::Receiver<MonitorCommand>,
        rpc_batch_size: usize,
    ) -> Self {
        Self {
            rpc,
            pending: HashMap::new(),
            pool_size: Arc::new(AtomicU64::new(0)),
            register_rx,
            rpc_batch_size,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut last_block = self.rpc.block_number().await?;
        let mut pool_check_count = 0u64;
        log::info!("[monitor] started at block={}", last_block);

        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            self.drain_registrations();

            pool_check_count += 1;
            if pool_check_count == 10 {
                pool_check_count = 0;
                if let Ok((pending, queued)) = self.rpc.txpool_status().await {
                    self.pool_size.store(pending, Ordering::Relaxed);
                    if !self.pending.is_empty() {
                        log::debug!(
                            "[monitor] txpool pending={} queued={} tracked_pending={}",
                            pending,
                            queued,
                            self.pending.len()
                        );
                    }
                }
            }

            let current_block = match self.rpc.block_number().await {
                Ok(b) => b,
                Err(_) => continue,
            };

            self.drain_registrations();

            if current_block <= last_block {
                continue;
            }

            if self.pending.is_empty() {
                log::debug!(
                    "[monitor] idle advance blocks={}..={} tracked_pending=0",
                    last_block + 1,
                    current_block
                );
                last_block = current_block;
                continue;
            }

            log::debug!(
                "[monitor] new_blocks={}..={} tracked_pending={}",
                last_block + 1,
                current_block,
                self.pending.len()
            );
            let mut processed_to = last_block;
            let mut block_num = last_block + 1;
            while block_num <= current_block {
                // Block queries are heavy (many tx hashes per block), cap at 32
                let block_batch = self.rpc_batch_size.min(32);
                let chunk_end = (block_num + block_batch as u64 - 1).min(current_block);
                match self.process_block_batch(block_num, chunk_end).await {
                    Ok(()) => processed_to = chunk_end,
                    Err(e) => {
                        log::warn!(
                            "[monitor] process_block_batch({}..={}) failed: {}",
                            block_num,
                            chunk_end,
                            e
                        );
                        break;
                    }
                }
                self.drain_registrations();
                block_num = chunk_end + 1;
            }

            last_block = processed_to;
        }
    }

    fn drain_registrations(&mut self) {
        let mut registered = 0;
        while let Ok(cmd) = self.register_rx.try_recv() {
            match cmd {
                MonitorCommand::Register(reg) => {
                    self.pending.insert(reg.hash, reg.reply);
                    let _ = reg.registered.send(());
                    registered += 1;
                }
            }
        }
        if registered > 0 {
            log::debug!(
                "[monitor] registered={} pending={}",
                registered,
                self.pending.len()
            );
        }
    }

    async fn process_block_batch(&mut self, start: u64, end: u64) -> Result<()> {
        let block_nums: Vec<u64> = (start..=end).collect();
        let blocks = self.rpc.batch_get_block_tx_hashes(&block_nums).await?;
        for (block_num, hashes) in block_nums.into_iter().zip(blocks.into_iter()) {
            self.process_block_hashes(block_num, hashes).await?;
        }
        Ok(())
    }

    async fn process_block_hashes(&mut self, block_num: u64, hashes: Vec<B256>) -> Result<()> {
        let matched: Vec<B256> = hashes
            .iter()
            .filter(|h| self.pending.contains_key(*h))
            .copied()
            .collect();

        if matched.is_empty() {
            return Ok(());
        }

        log::debug!(
            "[monitor] block={} confirming matched={}/{} pending={}",
            block_num,
            matched.len(),
            hashes.len(),
            self.pending.len()
        );

        // Always fetch receipts
        for chunk in matched.chunks(self.rpc_batch_size) {
            let receipts = self.rpc.batch_get_receipts(chunk).await?;
            for (i, receipt) in receipts.iter().enumerate() {
                let tx_hash = chunk[i];
                if let Some(reply) = self.pending.remove(&tx_hash) {
                    let tx_receipt = match receipt {
                        Some(r) => TxReceipt {
                            success: r["status"].as_str() == Some("0x1"),
                            gas_used: parse_hex_u64(&r["gasUsed"]),
                            effective_gas_price: parse_hex_u128(&r["effectiveGasPrice"]),
                        },
                        None => TxReceipt {
                            success: false,
                            gas_used: 0,
                            effective_gas_price: 0,
                        },
                    };
                    let _ = reply.send(tx_receipt);
                }
            }
        }
        Ok(())
    }
}

fn parse_hex_u64(value: &serde_json::Value) -> u64 {
    value
        .as_str()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0)
}

fn parse_hex_u128(value: &serde_json::Value) -> u128 {
    value
        .as_str()
        .and_then(|s| u128::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0)
}
