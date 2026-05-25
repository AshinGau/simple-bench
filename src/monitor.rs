use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use alloy::primitives::B256;
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

use crate::rpc::RpcClient;

/// Sent from worker to monitor to register a pending tx.
pub struct RegisterTx {
    pub hash: B256,
    pub reply: oneshot::Sender<bool>, // true = success, false = failure/timeout
}

pub struct BlockMonitor {
    rpc: RpcClient,
    pending: HashMap<B256, oneshot::Sender<bool>>,
    pub pool_size: Arc<AtomicU64>,
    register_rx: mpsc::Receiver<RegisterTx>,
    fetch_receipt: bool,
}

impl BlockMonitor {
    pub fn new(
        rpc: RpcClient,
        register_rx: mpsc::Receiver<RegisterTx>,
        fetch_receipt: bool,
    ) -> Self {
        Self {
            rpc,
            pending: HashMap::new(),
            pool_size: Arc::new(AtomicU64::new(0)),
            register_rx,
            fetch_receipt,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut last_block = self.rpc.block_number().await?;
        log::info!(
            "[monitor] 从区块 {} 开始监控, receipt={}",
            last_block,
            self.fetch_receipt
        );

        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Drain registration channel
            while let Ok(reg) = self.register_rx.try_recv() {
                self.pending.insert(reg.hash, reg.reply);
            }

            // Update pool size
            if let Ok((pending, _)) = self.rpc.txpool_status().await {
                self.pool_size.store(pending, Ordering::Relaxed);
            }

            if self.pending.is_empty() {
                continue;
            }

            // Check for new blocks
            let current_block = match self.rpc.block_number().await {
                Ok(b) => b,
                Err(_) => continue,
            };

            if current_block <= last_block {
                continue;
            }

            for block_num in (last_block + 1)..=current_block {
                self.process_block(block_num).await;
            }

            last_block = current_block;
        }
    }

    async fn process_block(&mut self, block_num: u64) {
        let hashes = match self.rpc.get_block_tx_hashes(block_num).await {
            Ok(h) => h,
            Err(_) => return,
        };

        // Find matched pending txs
        let matched: Vec<B256> = hashes
            .iter()
            .filter(|h| self.pending.contains_key(*h))
            .copied()
            .collect();

        if matched.is_empty() {
            return;
        }

        if self.fetch_receipt {
            // Batch fetch receipts, then notify with success/fail
            let receipts = match self.rpc.batch_get_receipts(&matched).await {
                Ok(r) => r,
                Err(_) => return,
            };
            for (i, receipt) in receipts.iter().enumerate() {
                let tx_hash = matched[i];
                if let Some(reply) = self.pending.remove(&tx_hash) {
                    let success = match receipt {
                        Some(r) => r["status"].as_str() == Some("0x1"),
                        None => false,
                    };
                    let _ = reply.send(success);
                }
            }
        } else {
            // Just notify "已上链", no receipt check
            for hash in &matched {
                if let Some(reply) = self.pending.remove(hash) {
                    let _ = reply.send(true);
                }
            }
        }
    }
}
