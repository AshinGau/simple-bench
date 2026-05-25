mod account;
mod config;
mod monitor;
mod rpc;
mod stats;
mod tx;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use anyhow::Result;
use clap::Parser;

use account::Account;
use config::{Config, TransferType};
use monitor::RegisterTx;
use rpc::RpcClient;
use stats::Stats;

#[derive(Parser)]
#[command(name = "simple_bench")]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,
    #[arg(long)]
    recover: bool,
    #[arg(long)]
    receipt: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(|| "bench.toml".to_string());
    let config = Config::load(&config_path)?;
    let chain_id = detect_chain_id(&config.rpc.url).await?;
    let rpc = RpcClient::new(&config.rpc.url, chain_id);

    // Derive accounts
    let worker_keys = config::derive_worker_keys(&config.faucet.private_key, config.bench.num_accounts);
    let mut accounts: Vec<Account> = worker_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    if cli.recover {
        recover(&mut accounts, &config, &rpc, chain_id).await?;
        return Ok(());
    }

    // Start block monitor
    let (register_tx, register_rx) = tokio::sync::mpsc::channel::<RegisterTx>(100000);
    let mut block_monitor = monitor::BlockMonitor::new(rpc.clone(), register_rx, cli.receipt);

    // Distribute funds
    distribute_funds(&accounts, &config, &rpc, chain_id).await?;

    // Deploy ERC20 tokens if needed
    let token_addresses: Vec<Address> = if config.bench.transfer_type == TransferType::Erc20 {
        deploy_tokens(&accounts[0], &config, &rpc, chain_id).await?
    } else {
        vec![]
    };

    // Init nonces
    for acc in accounts.iter_mut() {
        acc.init_nonce(&rpc).await?;
    }

    log::info!(
        "[启动] {} accounts, 每账户 {} ETH, chain_id={}",
        accounts.len(),
        config.faucet.faucet_eth_balance / U256::from(accounts.len() as u64),
        chain_id
    );

    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let active_count = Arc::new(AtomicUsize::new(accounts.len()));

    // Stats logging
    let stats_clone = stats.clone();
    let stop_clone = stop.clone();
    let active_clone = active_count.clone();
    let stats_handle = tokio::spawn(async move {
        while !stop_clone.load(Ordering::Relaxed) {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let active = active_clone.load(Ordering::Relaxed);
            if active > 0 {
                stats_clone.log_summary(active);
            }
        }
    });

    // Spawn workers
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.bench.num_senders));
    let mut handles = Vec::new();

    for (idx, account) in accounts.into_iter().enumerate() {
        let rpc = rpc.clone();
        let config = config.clone();
        let stats = stats.clone();
        let sem = semaphore.clone();
        let pool = block_monitor.pool_size.clone();
        let active = active_count.clone();
        let tokens = token_addresses.clone();
        let reg_tx = register_tx.clone();

        let handle = tokio::spawn(async move {
            let result = run_worker(idx, account, &config, &rpc, &stats, &sem, &pool, &tokens, reg_tx).await;
            active.fetch_sub(1, Ordering::Relaxed);
            match result {
                Ok(()) => log::info!("[结束] 账户 #{} 余额不足, 已移除", idx),
                Err(e) => log::error!("[错误] 账户 #{}: {}", idx, e),
            }
        });

        handles.push(handle);
    }

    // Start monitor (must be after workers so register_tx is created first)
    let monitor_handle = tokio::spawn(async move {
        if let Err(e) = block_monitor.run().await {
            log::error!("[monitor] error: {}", e);
        }
    });

    // Wait for all workers
    for h in handles {
        let _ = h.await;
    }

    stop.store(true, Ordering::Relaxed);
    let _ = stats_handle.await;
    stats.log_final();
    monitor_handle.abort();

    Ok(())
}

async fn run_worker(
    idx: usize,
    mut account: Account,
    config: &Config,
    rpc: &RpcClient,
    stats: &Stats,
    sem: &Arc<tokio::sync::Semaphore>,
    pool_size: &Arc<std::sync::atomic::AtomicU64>,
    token_addresses: &[Address],
    register_tx: tokio::sync::mpsc::Sender<RegisterTx>,
) -> Result<()> {
    let token_idx_base = if token_addresses.is_empty() {
        0
    } else {
        idx % token_addresses.len()
    };

    loop {
        // Flow control
        while pool_size.load(Ordering::Relaxed) > config.bench.max_pool_size {
            log::warn!(
                "[流控] mempool pending={} > max={}, 暂停发送...",
                pool_size.load(Ordering::Relaxed),
                config.bench.max_pool_size
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let _permit = sem.acquire().await?;

        // Check balance
        let balance = account.balance(rpc).await?;
        let gas_wei = U256::from(config.bench.max_fee_per_gas) * U256::from(21000u64) * U256::from(1_000_000_000u64);
        if balance < gas_wei {
            break;
        }

        // Build transaction
        let signed = if config.bench.transfer_type == TransferType::Native {
            let hash = alloy::primitives::keccak256(
                [account.address.as_slice(), &account.nonce.to_be_bytes()].concat(),
            );
            let to = Address::from_slice(&hash[12..]);
            tx::build_native_tx(&account, to, U256::from(1u64), &config.bench, rpc.chain_id).await?
        } else {
            let token = token_addresses[token_idx_base];
            tx::build_erc20_tx(&account, token, account.address, U256::from(1u64), &config.bench, rpc.chain_id).await?
        };

        // Send transaction
        let tx_hash = rpc.send_raw_tx(&signed.raw).await?;
        stats.inc_sent();

        // Register with monitor and wait for block-driven receipt
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        register_tx.send(RegisterTx { hash: tx_hash, reply: reply_tx }).await?;

        match reply_rx.await {
            Ok(true) => {
                stats.inc_confirmed();
            }
            Ok(false) => {
                stats.inc_failed();
                log::warn!("[失败] 账户 #{} tx {} reverted", idx, tx_hash);
                break;
            }
            Err(_) => {
                stats.inc_failed();
                log::warn!("[错误] 账户 #{} tx {} monitor关闭", idx, tx_hash);
                break;
            }
        }

        account.nonce += 1;
        drop(_permit);
    }

    Ok(())
}

async fn distribute_funds(
    accounts: &[Account],
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
) -> Result<()> {
    let mut faucet = Account::from_key(&config.faucet.private_key)?;
    faucet.init_nonce(rpc).await?;

    let per_account = config.faucet.faucet_eth_balance / U256::from(accounts.len() as u64);
    log::info!("[启动] Faucet → {} accounts, 每账户 {} wei", accounts.len(), per_account);

    let mut tx_hashes = Vec::with_capacity(accounts.len());
    for acc in accounts.iter() {
        let signed = tx::build_native_tx(&faucet, acc.address, per_account, &config.bench, chain_id).await?;
        let hash = rpc.send_raw_tx(&signed.raw).await?;
        tx_hashes.push(hash);
        faucet.nonce += 1;
    }

    for hash in &tx_hashes {
        loop {
            if let Some(receipt) = rpc.get_transaction_receipt(*hash).await? {
                if receipt["status"].as_str() != Some("0x1") {
                    log::error!("[启动] 分发交易 {} 失败", hash);
                }
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    log::info!("[启动] 分发完成");
    Ok(())
}

async fn deploy_tokens(
    deployer_account: &Account,
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
) -> Result<Vec<Address>> {
    let bytecode = tx::simple_token_bytecode();
    let mut deployer = deployer_account.clone();
    deployer.init_nonce(rpc).await?;

    let initial_supply = U256::from(1_000_000_000u64) * U256::from(10u64).pow(U256::from(18));

    let mut addresses = Vec::new();
    for i in 0..config.bench.num_tokens {
        let signed = tx::build_deploy_tx(&deployer, &bytecode, initial_supply, &config.bench, chain_id).await?;
        let hash = rpc.send_raw_tx(&signed.raw).await?;

        loop {
            if let Some(receipt) = rpc.get_transaction_receipt(hash).await? {
                let addr_str = receipt["contractAddress"].as_str().unwrap_or("0x");
                let addr_bytes: [u8; 20] = hex::decode(addr_str.trim_start_matches("0x"))?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid contract address"))?;
                let addr = Address::from(addr_bytes);
                log::info!("[启动] ERC20 #{} 部署完成: 0x{:x}", i, addr);
                addresses.push(addr);
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        deployer.nonce += 1;
    }

    Ok(addresses)
}

async fn recover(
    accounts: &mut [Account],
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
) -> Result<()> {
    let faucet_addr = account::faucet_address(&config.faucet.private_key)?;
    let gas_wei = U256::from(config.bench.max_fee_per_gas) * U256::from(21000u64) * U256::from(1_000_000_000u64);

    for (idx, acc) in accounts.iter_mut().enumerate() {
        acc.init_nonce(rpc).await?;
        let balance = acc.balance(rpc).await?;
        if balance <= gas_wei {
            if balance > U256::ZERO {
                log::info!("[恢复] 账户 #{}: 余额 {} 不足以支付gas, 跳过", idx, balance);
            }
            continue;
        }

        let recover_amount = balance - gas_wei;
        let signed = tx::build_native_tx(acc, faucet_addr, recover_amount, &config.bench, chain_id).await?;
        let hash = rpc.send_raw_tx(&signed.raw).await?;

        loop {
            if rpc.get_transaction_receipt(hash).await?.is_some() {
                log::info!("[恢复] 账户 #{}: 回收 {} wei → faucet", idx, recover_amount);
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    Ok(())
}

async fn detect_chain_id(url: &str) -> Result<u64> {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_chainId",
        "params": [],
        "id": 1
    });
    let resp: serde_json::Value = client.post(url).json(&body).send().await?.json().await?;
    let hex_str = resp["result"].as_str().unwrap_or("0x0");
    Ok(u64::from_str_radix(hex_str.trim_start_matches("0x"), 16)?)
}
