use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use alloy::primitives::B256;
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

use crate::rpc::RpcClient;

pub enum MonitorCommand {
    Register(RegisterTx),
    Cancel(B256),
}

pub struct RegisterTx {
    pub hash: B256,
    pub reply: oneshot::Sender<bool>,
    pub registered: oneshot::Sender<()>,
}

pub struct BlockMonitor {
    rpc: RpcClient,
    pending: HashMap<B256, oneshot::Sender<bool>>,
    pub pool_size: Arc<AtomicU64>,
    register_rx: mpsc::Receiver<MonitorCommand>,
    fetch_receipt: bool,
}

impl BlockMonitor {
    pub fn new(
        rpc: RpcClient,
        register_rx: mpsc::Receiver<MonitorCommand>,
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
        let mut pool_check_count = 0u64;
        log::info!(
            "[monitor] started at block={} receipt={}",
            last_block,
            self.fetch_receipt
        );

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
                let chunk_end = (block_num + 31).min(current_block);
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
        let mut canceled = 0;
        while let Ok(cmd) = self.register_rx.try_recv() {
            match cmd {
                MonitorCommand::Register(reg) => {
                    self.pending.insert(reg.hash, reg.reply);
                    let _ = reg.registered.send(());
                    registered += 1;
                }
                MonitorCommand::Cancel(hash) => {
                    if self.pending.remove(&hash).is_some() {
                        canceled += 1;
                    }
                }
            }
        }
        if registered > 0 || canceled > 0 {
            log::debug!(
                "[monitor] registered={} canceled={} pending={}",
                registered,
                canceled,
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

        if self.fetch_receipt {
            let receipts = self.rpc.batch_get_receipts(&matched).await?;
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
            for hash in &matched {
                if let Some(reply) = self.pending.remove(hash) {
                    let _ = reply.send(true);
                }
            }
        }
        Ok(())
    }
}
