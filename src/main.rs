mod account;
mod config;
mod monitor;
mod rpc;
mod stats;
mod tx;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use alloy::primitives::{Address, Bytes, B256, U256};
use anyhow::Result;
use clap::Parser;
use tokio::sync::{mpsc, oneshot};

use account::Account;
use config::{
    BenchConfig, Config, TransferType, ERC20_TRANSFER_GAS_LIMIT, NATIVE_TRANSFER_GAS_LIMIT,
};
use monitor::{MonitorCommand, RegisterTx, TxReceipt};
use rpc::RpcClient;
use stats::Stats;

const MAX_INIT_SENDER_INFLIGHT_TXS: usize = 60;

// ── CLI ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "simple_bench")]
enum Cli {
    /// 分发 native ETH + 部署 ERC20 + 分发 token
    Faucet {
        #[arg(short, long)]
        config: Option<String>,
    },
    /// 压测（假设账户已有资金）
    Bench {
        #[arg(short, long)]
        config: Option<String>,
    },
    /// 回收剩余资金到 faucet
    Recover {
        #[arg(short, long)]
        config: Option<String>,
    },
}

// ── main ───────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    match cli {
        Cli::Faucet { config } => {
            let config_path = config.unwrap_or_else(|| "bench.toml".to_string());
            let config = Config::load(&config_path)?;
            let chain_id = detect_chain_id(&config.rpc.url).await?;
            let rpc = RpcClient::new(&config.rpc.url, chain_id, config.bench.rpc_concurrency);
            let (register_tx, register_rx) = mpsc::channel::<MonitorCommand>(100_000);
            let mut monitor =
                monitor::BlockMonitor::new(rpc.clone(), register_rx, config.bench.rpc_batch_size);
            let _pool_size = monitor.pool_size.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor.run().await {
                    log::error!("[monitor] exited: {}", e);
                }
            });
            run_faucet(&config, &rpc, chain_id, register_tx).await?;
        }
        Cli::Bench { config } => {
            let config_path = config.unwrap_or_else(|| "bench.toml".to_string());
            let config = Config::load(&config_path)?;
            let chain_id = detect_chain_id(&config.rpc.url).await?;
            let rpc = RpcClient::new(&config.rpc.url, chain_id, config.bench.rpc_concurrency);
            let (register_tx, register_rx) = mpsc::channel::<MonitorCommand>(100_000);
            let mut monitor =
                monitor::BlockMonitor::new(rpc.clone(), register_rx, config.bench.rpc_batch_size);
            let pool_size = monitor.pool_size.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor.run().await {
                    log::error!("[monitor] exited: {}", e);
                }
            });
            run_bench(&config, &rpc, chain_id, register_tx, pool_size).await?;
        }
        Cli::Recover { config } => {
            let config_path = config.unwrap_or_else(|| "bench.toml".to_string());
            let config = Config::load(&config_path)?;
            let chain_id = detect_chain_id(&config.rpc.url).await?;
            let rpc = RpcClient::new(&config.rpc.url, chain_id, config.bench.rpc_concurrency);
            let (register_tx, register_rx) = mpsc::channel::<MonitorCommand>(100_000);
            let mut monitor =
                monitor::BlockMonitor::new(rpc.clone(), register_rx, config.bench.rpc_batch_size);
            let _pool_size = monitor.pool_size.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor.run().await {
                    log::error!("[monitor] exited: {}", e);
                }
            });
            run_recover(&config, &rpc, chain_id, register_tx).await?;
        }
    }
    Ok(())
}

// ── helpers ────────────────────────────────────────────────────────

async fn register_confirmations(
    register_tx: &mpsc::Sender<MonitorCommand>,
    hashes: &[B256],
) -> Result<Vec<oneshot::Receiver<TxReceipt>>> {
    let mut confirmation_rxs = Vec::with_capacity(hashes.len());
    let mut registered_rxs = Vec::with_capacity(hashes.len());
    for &hash in hashes {
        let (reply, confirmation_rx) = oneshot::channel();
        let (registered_tx, registered_rx) = oneshot::channel();
        register_tx
            .send(MonitorCommand::Register(RegisterTx {
                hash,
                reply,
                registered: registered_tx,
            }))
            .await?;
        confirmation_rxs.push(confirmation_rx);
        registered_rxs.push(registered_rx);
    }
    for rx in registered_rxs {
        rx.await?;
    }
    Ok(confirmation_rxs)
}

async fn wait_confirmations(confirmations: Vec<oneshot::Receiver<TxReceipt>>) -> Result<()> {
    for rx in confirmations {
        let receipt = rx.await?;
        if !receipt.success {
            anyhow::bail!("tx reverted");
        }
    }
    Ok(())
}

fn ensure_hashes_match(expected: &[B256], actual: &[B256]) -> Result<()> {
    if expected.len() != actual.len() {
        anyhow::bail!(
            "batch returned {} hashes for {} txs",
            actual.len(),
            expected.len()
        );
    }
    for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
        if e != a {
            anyhow::bail!(
                "batch tx hash mismatch at {}: expected 0x{}, got 0x{}",
                i,
                hex::encode(e.as_slice()),
                hex::encode(a.as_slice()),
            );
        }
    }
    Ok(())
}

async fn detect_chain_id(url: &str) -> Result<u64> {
    let body = serde_json::json!({"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1});
    let client = reqwest::Client::builder().no_proxy().build()?;
    let resp: serde_json::Value = client.post(url).json(&body).send().await?.json().await?;
    Ok(u64::from_str_radix(
        resp["result"]
            .as_str()
            .unwrap_or("0x0")
            .trim_start_matches("0x"),
        16,
    )?)
}

fn derive_bench_recipient(account: &Account) -> Address {
    let mut input = [0u8; 28];
    input[..20].copy_from_slice(account.address.as_slice());
    input[20..].copy_from_slice(&account.nonce.to_be_bytes());
    let hash = alloy::primitives::keccak256(input);
    Address::from_slice(&hash[12..])
}

fn validate_derived_accounts(
    faucet: Address,
    workers: &[Account],
    intermediates: &[Account],
) -> Result<()> {
    if workers.iter().any(|account| account.address == faucet) {
        anyhow::bail!("derived worker set contains faucet address 0x{:x}", faucet);
    }
    if intermediates
        .iter()
        .any(|account| account.address == faucet)
    {
        anyhow::bail!(
            "derived intermediate set contains faucet address 0x{:x}",
            faucet
        );
    }

    let worker_set: HashSet<_> = workers.iter().map(|account| account.address).collect();
    if worker_set.len() != workers.len() {
        anyhow::bail!("derived worker set contains duplicate addresses");
    }
    let intermediate_set: HashSet<_> = intermediates
        .iter()
        .map(|account| account.address)
        .collect();
    if intermediate_set.len() != intermediates.len() {
        anyhow::bail!("derived intermediate set contains duplicate addresses");
    }
    if !worker_set.is_disjoint(&intermediate_set) {
        anyhow::bail!("worker and intermediate sets overlap");
    }
    Ok(())
}

fn intermediate_gas_reserve(bench: &BenchConfig, worker_count: usize) -> U256 {
    let worker_count = U256::from(worker_count);
    let native_reserve =
        Account::estimated_gas_cost(bench.max_fee_per_gas, NATIVE_TRANSFER_GAS_LIMIT)
            * worker_count;
    let erc20_reserve = if bench.transfer_type == TransferType::Erc20 {
        Account::estimated_gas_cost(bench.max_fee_per_gas, ERC20_TRANSFER_GAS_LIMIT)
            * worker_count
            * U256::from(bench.num_tokens)
    } else {
        U256::ZERO
    };
    native_reserve + erc20_reserve
}

fn worker_subset(workers: &[Account], chunk_size: usize, idx: usize) -> Vec<Account> {
    workers.chunks(chunk_size).nth(idx).unwrap_or(&[]).to_vec()
}

async fn discover_erc20_token_addresses(
    config: &Config,
    rpc: &RpcClient,
) -> Result<Arc<[Address]>> {
    let faucet = Account::from_key(&config.faucet.private_key)?;
    let faucet_nonce = rpc.get_nonce(faucet.address).await?;
    let level = config.bench.clamped_faucet_level() as u64;
    let deploy_span = (config.bench.num_tokens as u64)
        .checked_mul(level + 1)
        .ok_or_else(|| anyhow::anyhow!("erc20 token recovery span overflow"))?;
    let deploy_start_nonce = faucet_nonce.checked_sub(deploy_span).ok_or_else(|| {
        anyhow::anyhow!(
            "faucet nonce {} is too small to recover {} ERC20 deployments; run faucet first with matching config",
            faucet_nonce,
            config.bench.num_tokens
        )
    })?;

    let token_addresses: Vec<Address> = (0..config.bench.num_tokens)
        .map(|i| tx::create_address(faucet.address, deploy_start_nonce + i as u64))
        .collect();
    let has_code = rpc.batch_has_code(&token_addresses).await?;
    for (token, has_code) in token_addresses.iter().zip(has_code) {
        if !has_code {
            anyhow::bail!(
                "no contract code at derived token address 0x{:x}; run faucet first with matching erc20 config",
                token
            );
        }
    }
    Ok(Arc::<[Address]>::from(token_addresses))
}

// ── submit_raws_async ──────────────────────────────────────────────

/// Batch 发送 raws，注册 monitor，返回 confirmation receivers。
async fn submit_raws_async(
    rpc: &RpcClient,
    register_tx: &mpsc::Sender<MonitorCommand>,
    raws: &[Bytes],
    rpc_batch_size: usize,
) -> Result<(Vec<B256>, Vec<oneshot::Receiver<TxReceipt>>)> {
    let mut hashes = Vec::with_capacity(raws.len());
    let mut all_confirmations = Vec::with_capacity(raws.len());
    for chunk in raws.chunks(rpc_batch_size) {
        let expected: Vec<B256> = chunk.iter().map(tx::raw_tx_hash).collect();
        let confirmations = register_confirmations(register_tx, &expected).await?;
        let returned = rpc.batch_send_raw_txs(chunk).await?;
        ensure_hashes_match(&expected, &returned)?;
        hashes.extend_from_slice(&expected);
        all_confirmations.extend(confirmations);
    }
    Ok((hashes, all_confirmations))
}

/// 按 MAX_INIT_SENDER_INFLIGHT_TXS 分批，每批 batch 发送 + 等确认。
async fn submit_with_inflight_limit(
    rpc: &RpcClient,
    register_tx: &mpsc::Sender<MonitorCommand>,
    raws: &[Bytes],
    rpc_batch_size: usize,
) -> Result<()> {
    for chunk in raws.chunks(MAX_INIT_SENDER_INFLIGHT_TXS) {
        let (_, c) = submit_raws_async(rpc, register_tx, chunk, rpc_batch_size).await?;
        wait_confirmations(c).await?;
    }
    Ok(())
}

// ── run_faucet ─────────────────────────────────────────────────────

async fn run_faucet(
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
) -> Result<()> {
    let faucet = Account::from_key(&config.faucet.private_key)?;
    let level = config.bench.clamped_faucet_level();
    let eth_per_inter = config
        .bench
        .faucet_eth_per_level_or_default(config.faucet.faucet_eth_balance);
    let rpc_batch = config.bench.rpc_batch_size;

    let worker_keys =
        config::derive_worker_keys(&config.faucet.private_key, config.bench.num_accounts);
    let workers: Vec<Account> = worker_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    let inter_keys = config::derive_intermediate_keys(&config.faucet.private_key, level);
    let intermediates: Vec<Account> = inter_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    validate_derived_accounts(faucet.address, &workers, &intermediates)?;

    log::info!(
        "[faucet] {} workers, faucet_level={}, eth_per_inter={} wei",
        workers.len(),
        level,
        eth_per_inter,
    );

    // === Phase 1: Native ETH ===
    // 1a: master → intermediates
    let master_nonce = rpc.get_nonce(faucet.address).await?;
    let mut raws_1a = Vec::with_capacity(level);
    for (i, inter) in intermediates.iter().enumerate() {
        let mut s = faucet.clone();
        s.nonce = master_nonce + i as u64;
        let signed =
            tx::build_native_tx(&s, inter.address, eth_per_inter, &config.bench, chain_id).await?;
        raws_1a.push(signed.raw);
    }
    submit_with_inflight_limit(rpc, &register_tx, &raws_1a, rpc_batch).await?;
    log::info!("[faucet] Phase 1a done: master → {} intermediates", level);

    // 1b: intermediates → workers (parallel)
    let inter_addrs: Vec<Address> = intermediates.iter().map(|a| a.address).collect();
    let inter_nonces = rpc.batch_get_nonces(&inter_addrs).await?;
    let chunk_size = workers.len().div_ceil(level);
    let mut handles = Vec::new();
    for (idx, inter) in intermediates.iter().enumerate() {
        let subset = worker_subset(&workers, chunk_size, idx);
        if subset.is_empty() {
            continue;
        }
        let gas_reserve = intermediate_gas_reserve(&config.bench, subset.len());
        if eth_per_inter <= gas_reserve {
            anyhow::bail!(
                "intermediate allocation {} wei does not cover required gas reserve {} wei for {} workers",
                eth_per_inter,
                gas_reserve,
                subset.len()
            );
        }
        let distributable = eth_per_inter - gas_reserve;
        let eth_per_worker = distributable / U256::from(subset.len());
        if eth_per_worker.is_zero() {
            anyhow::bail!(
                "intermediate allocation {} wei leaves zero worker funding after reserving {} wei gas",
                eth_per_inter,
                gas_reserve
            );
        }
        let base_nonce = inter_nonces[idx];
        let mut s_inter = inter.clone();
        let rpc = rpc.clone();
        let register_tx = register_tx.clone();
        let bench = config.bench.clone();
        handles.push(tokio::spawn(async move {
            let mut raws = Vec::with_capacity(subset.len());
            for (j, w) in subset.iter().enumerate() {
                s_inter.nonce = base_nonce + j as u64;
                let signed =
                    tx::build_native_tx(&s_inter, w.address, eth_per_worker, &bench, chain_id)
                        .await?;
                raws.push(signed.raw);
            }
            submit_with_inflight_limit(&rpc, &register_tx, &raws, rpc_batch).await
        }));
    }
    for h in handles {
        h.await??;
    }
    log::info!("[faucet] Phase 1b done: intermediates → workers");

    // === Phase 2: ERC20 ===
    if config.bench.transfer_type == TransferType::Erc20 {
        let bytecode = tx::simple_token_bytecode();
        let supply = U256::from(1_000_000_000u64) * U256::from(10u64.pow(18));
        let token_per_worker = U256::from(1_000_000u64) * U256::from(10u64.pow(18));

        // 2a: master deploy + transfer to intermediates
        let master_nonce = rpc.get_nonce(faucet.address).await?;
        let mut raws_2a = Vec::new();
        let mut token_addresses = Vec::with_capacity(config.bench.num_tokens);

        // Deploy tokens
        for i in 0..config.bench.num_tokens {
            let mut s = faucet.clone();
            s.nonce = master_nonce + i as u64;
            token_addresses.push(tx::create_address(s.address, s.nonce));
            let signed =
                tx::build_deploy_tx(&s, &bytecode, supply, &config.bench, chain_id).await?;
            raws_2a.push(signed.raw);
        }
        let token_addresses = Arc::<[Address]>::from(token_addresses);

        // Transfer tokens to intermediates
        let token_per_inter = supply / U256::from(level);
        let base_nonce = master_nonce + config.bench.num_tokens as u64;
        let mut nonce_offset = 0u64;
        for token_addr in token_addresses.iter() {
            for inter in &intermediates {
                let mut s = faucet.clone();
                s.nonce = base_nonce + nonce_offset;
                nonce_offset += 1;
                let signed = tx::build_erc20_tx(
                    &s,
                    *token_addr,
                    inter.address,
                    token_per_inter,
                    &config.bench,
                    chain_id,
                )
                .await?;
                raws_2a.push(signed.raw);
            }
        }

        let (_, c) = submit_raws_async(rpc, &register_tx, &raws_2a, rpc_batch).await?;
        wait_confirmations(c).await?;
        log::info!(
            "[faucet] Phase 2a done: {} tokens deployed, transferred to intermediates",
            config.bench.num_tokens
        );

        // 2b: intermediates → workers (parallel)
        let inter_nonces = rpc.batch_get_nonces(&inter_addrs).await?;
        let mut handles2 = Vec::new();
        for (idx, inter) in intermediates.iter().enumerate() {
            let subset: Vec<Account> = workers
                .chunks(chunk_size)
                .nth(idx)
                .unwrap_or(&[])
                .iter()
                .cloned()
                .collect();
            if subset.is_empty() {
                continue;
            }
            let base_nonce = inter_nonces[idx];
            let mut s_inter = inter.clone();
            let rpc = rpc.clone();
            let register_tx = register_tx.clone();
            let bench = config.bench.clone();
            let tokens = token_addresses.clone();
            handles2.push(tokio::spawn(async move {
                let mut raws = Vec::new();
                let mut nonce = base_nonce;
                for token_addr in tokens.iter() {
                    for w in &subset {
                        s_inter.nonce = nonce;
                        nonce += 1;
                        let signed = tx::build_erc20_tx(
                            &s_inter,
                            *token_addr,
                            w.address,
                            token_per_worker,
                            &bench,
                            chain_id,
                        )
                        .await?;
                        raws.push(signed.raw);
                    }
                }
                submit_with_inflight_limit(&rpc, &register_tx, &raws, rpc_batch).await
            }));
        }
        for h in handles2 {
            h.await??;
        }
        log::info!("[faucet] Phase 2b done: intermediates → workers (ERC20)");
    }

    log::info!("[faucet] complete");
    Ok(())
}

// ── BatchSender ────────────────────────────────────────────────────

struct BatchTx {
    raw: Bytes,
    expected_hash: B256,
    reply: oneshot::Sender<Result<B256>>,
}

async fn spawn_batch_sender(
    rpc: RpcClient,
    mut rx: mpsc::Receiver<BatchTx>,
    stop: Arc<AtomicBool>,
    rpc_batch_size: usize,
) {
    let flush_interval = tokio::time::Duration::from_millis(5);
    let mut buf: Vec<BatchTx> = Vec::new();
    loop {
        let should_flush = loop {
            if buf.len() >= rpc_batch_size {
                break true;
            }
            if stop.load(Ordering::Relaxed) && !buf.is_empty() {
                break true;
            }
            if buf.is_empty() {
                match rx.recv().await {
                    Some(tx) => buf.push(tx),
                    None => break false,
                }
                continue;
            }
            match tokio::time::timeout(flush_interval, rx.recv()).await {
                Ok(Some(tx)) => buf.push(tx),
                Ok(None) => break true,
                Err(_) => break true,
            }
        };

        if !should_flush {
            break;
        }

        let batch: Vec<Bytes> = buf.iter().map(|t| t.raw.clone()).collect();
        match rpc.batch_send_raw_txs(&batch).await {
            Ok(returned) => {
                for (i, bt) in buf.drain(..).enumerate() {
                    if i < returned.len() && returned[i] == bt.expected_hash {
                        let _ = bt.reply.send(Ok(returned[i]));
                    } else {
                        let _ = bt.reply.send(Err(anyhow::anyhow!("hash mismatch")));
                    }
                }
            }
            Err(e) => {
                for bt in buf.drain(..) {
                    let _ = bt
                        .reply
                        .send(Err(anyhow::anyhow!("batch_send failed: {}", e)));
                }
                break;
            }
        }

        if stop.load(Ordering::Relaxed) {
            while let Ok(bt) = rx.try_recv() {
                let _ = bt.reply.send(Err(anyhow::anyhow!("stopped")));
            }
            break;
        }
    }
}

// ── run_bench ──────────────────────────────────────────────────────

#[derive(Clone)]
enum BenchWorkload {
    Native,
    Erc20 { token_addresses: Arc<[Address]> },
}

#[derive(Clone)]
struct BenchCfg {
    workload: BenchWorkload,
    fee_config: tx::TxFeeConfig,
    max_pool_size: u64,
    amount: U256,
    receipt_value: U256,
    gas_limit: u64,
}

impl BenchCfg {
    fn from_config(config: &BenchConfig, token_addresses: Arc<[Address]>) -> Self {
        let workload = match config.transfer_type {
            TransferType::Native => BenchWorkload::Native,
            TransferType::Erc20 => BenchWorkload::Erc20 { token_addresses },
        };
        Self {
            workload,
            fee_config: tx::TxFeeConfig::from(config),
            max_pool_size: config.max_pool_size,
            amount: config.transfer_amount(),
            receipt_value: config.transfer_native_value(),
            gas_limit: config.transfer_gas_limit(),
        }
    }

    fn estimated_gas(&self) -> U256 {
        Account::estimated_gas_cost(self.fee_config.max_fee_per_gas, self.gas_limit)
    }

    async fn build_tx(&self, account: &Account, chain_id: u64) -> Result<tx::SignedTx> {
        let to = derive_bench_recipient(account);
        match &self.workload {
            BenchWorkload::Native => {
                tx::build_native_transfer(
                    account,
                    to,
                    self.amount,
                    self.fee_config,
                    chain_id,
                    self.gas_limit,
                )
                .await
            }
            BenchWorkload::Erc20 { token_addresses } => {
                let token = token_addresses[account.nonce as usize % token_addresses.len()];
                tx::build_erc20_transfer(
                    account,
                    token,
                    to,
                    self.amount,
                    self.fee_config,
                    chain_id,
                    self.gas_limit,
                )
                .await
            }
        }
    }
}

async fn run_bench(
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
    pool_size: Arc<AtomicU64>,
) -> Result<()> {
    let worker_keys =
        config::derive_worker_keys(&config.faucet.private_key, config.bench.num_accounts);
    let mut accounts: Vec<Account> = worker_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    // Initialize nonces and balances
    let addrs: Vec<Address> = accounts.iter().map(|a| a.address).collect();
    let nonces = rpc.batch_get_nonces(&addrs).await?;
    let balances = rpc.batch_get_balances(&addrs).await?;
    for (i, account) in accounts.iter_mut().enumerate() {
        account.nonce = nonces[i];
        account.balance = balances[i];
    }

    let token_addresses = if config.bench.transfer_type == TransferType::Erc20 {
        let tokens = discover_erc20_token_addresses(config, rpc).await?;
        log::info!("[bench] recovered {} ERC20 token addresses", tokens.len());
        tokens
    } else {
        Arc::<[Address]>::from(Vec::new())
    };

    log::info!(
        "[bench] {} workers, chain_id={}, type={:?}",
        accounts.len(),
        chain_id,
        config.bench.transfer_type,
    );

    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let active_count = Arc::new(AtomicUsize::new(accounts.len()));
    let num_accounts = config.bench.num_accounts;

    // Batch sender
    let (batch_tx, batch_rx) = mpsc::channel::<BatchTx>(100_000);
    let sender_stop = stop.clone();
    let sender_rpc = rpc.clone();
    let sender_batch = config.bench.rpc_batch_size;
    tokio::spawn(async move {
        spawn_batch_sender(sender_rpc, batch_rx, sender_stop, sender_batch).await;
    });

    // Stats logger + stop checker
    {
        let stats = stats.clone();
        let stop = stop.clone();
        let active = active_count.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                if active.load(Ordering::Relaxed) > 0 {
                    stats.log_summary(active.load(Ordering::Relaxed));
                }
            }
        });
    }

    let bench_cfg = BenchCfg::from_config(&config.bench, token_addresses);

    let mut handles = Vec::new();
    for (idx, account) in accounts.into_iter().enumerate() {
        handles.push(tokio::spawn(run_bench_worker(
            idx,
            account,
            chain_id,
            bench_cfg.clone(),
            stats.clone(),
            pool_size.clone(),
            register_tx.clone(),
            batch_tx.clone(),
            active_count.clone(),
            stop.clone(),
            num_accounts,
        )));
    }

    for h in handles {
        let _ = h.await;
    }

    stop.store(true, Ordering::Relaxed);
    stats.log_final();
    Ok(())
}

async fn run_bench_worker(
    idx: usize,
    mut account: Account,
    chain_id: u64,
    bench_cfg: BenchCfg,
    stats: Arc<Stats>,
    pool_size: Arc<AtomicU64>,
    register_tx: mpsc::Sender<MonitorCommand>,
    batch_tx: mpsc::Sender<BatchTx>,
    active_count: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    num_accounts: usize,
) {
    let estimated_gas = bench_cfg.estimated_gas();

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        while pool_size.load(Ordering::Relaxed) > bench_cfg.max_pool_size {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if account.balance < estimated_gas {
            break;
        }

        let signed = match bench_cfg.build_tx(&account, chain_id).await {
            Ok(signed) => signed,
            Err(e) => {
                log::error!("[worker#{}] build_tx: {}", idx, e);
                break;
            }
        };

        let tx_hash = tx::raw_tx_hash(&signed.raw);

        // Send via BatchSender
        let (reply_tx, reply_rx) = oneshot::channel();
        if batch_tx
            .send(BatchTx {
                raw: signed.raw.clone(),
                expected_hash: tx_hash,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            break;
        }
        match reply_rx.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                log::error!("[worker#{}] batch_sender: {}", idx, e);
                break;
            }
            Err(_) => break,
        }

        stats.inc_sent();

        // Register and wait for confirmation
        let (confirm_tx, confirm_rx) = oneshot::channel();
        let (registered_tx, registered_rx) = oneshot::channel();
        if register_tx
            .send(MonitorCommand::Register(RegisterTx {
                hash: tx_hash,
                reply: confirm_tx,
                registered: registered_tx,
            }))
            .await
            .is_err()
        {
            break;
        }
        if registered_rx.await.is_err() {
            break;
        }

        let receipt = match confirm_rx.await {
            Ok(r) => r,
            Err(_) => break,
        };

        account.apply_receipt(&receipt, bench_cfg.receipt_value);

        if receipt.success {
            stats.inc_confirmed();
        } else {
            stats.inc_failed();
            break;
        }
    }

    active_count.fetch_sub(1, Ordering::Relaxed);
    if active_count.load(Ordering::Relaxed) <= num_accounts / 2 {
        stop.store(true, Ordering::Relaxed);
    }
}

// ── run_recover ────────────────────────────────────────────────────

async fn run_recover(
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
) -> Result<()> {
    let faucet = account::faucet_address(&config.faucet.private_key)?;
    let gas = Account::estimated_gas_cost(config.bench.max_fee_per_gas, NATIVE_TRANSFER_GAS_LIMIT);

    let worker_keys =
        config::derive_worker_keys(&config.faucet.private_key, config.bench.num_accounts);
    let workers: Vec<Account> = worker_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    let level = config.bench.clamped_faucet_level();
    let inter_keys = config::derive_intermediate_keys(&config.faucet.private_key, level);
    let intermediates: Vec<Account> = inter_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    let all_accounts: Vec<Account> = workers
        .into_iter()
        .chain(intermediates.into_iter())
        .collect();
    log::info!(
        "[recover] scanning {} accounts gas_reserve={} wei",
        all_accounts.len(),
        gas,
    );

    // Parallel balance checks + tx building
    let mut tasks: tokio::task::JoinSet<Result<Option<Bytes>>> = tokio::task::JoinSet::new();
    for account in all_accounts {
        let rpc = rpc.clone();
        let bench = config.bench.clone();
        tasks.spawn(async move {
            let balance = account.balance(&rpc).await?;
            if balance <= gas {
                return Ok(None);
            }
            let nonce = rpc.get_nonce(account.address).await?;
            let mut acc = account;
            acc.nonce = nonce;
            let signed = tx::build_native_tx(&acc, faucet, balance - gas, &bench, chain_id).await?;
            Ok(Some(signed.raw))
        });
    }

    let mut raws = Vec::new();
    let mut skipped = 0usize;
    let mut errors = 0usize;
    let mut completed = 0usize;
    while let Some(joined) = tasks.join_next().await {
        completed += 1;
        match joined? {
            Ok(Some(raw)) => raws.push(raw),
            Ok(None) => skipped += 1,
            Err(e) => {
                errors += 1;
                log::warn!("[recover] error: {}", e);
            }
        }
        if completed % 10 == 0 {
            log::debug!(
                "[recover] scanned={} prepared={} skipped={} errors={}",
                completed,
                raws.len(),
                skipped,
                errors,
            );
        }
    }

    log::info!(
        "[recover] scan done prepared={} skipped={} errors={}",
        raws.len(),
        skipped,
        errors,
    );
    if raws.is_empty() {
        log::info!("[recover] no accounts with balance above gas reserve");
        return Ok(());
    }
    log::info!("[recover] submitting {} recovery txs", raws.len());

    let (_, c) = submit_raws_async(rpc, &register_tx, &raws, config.bench.rpc_batch_size).await?;
    wait_confirmations(c).await?;
    log::info!("[recover] {} accounts → faucet", raws.len());
    Ok(())
}
