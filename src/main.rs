mod account;
mod config;
mod monitor;
mod rpc;
mod stats;
mod tx;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use alloy::primitives::{Address, Bytes, B256, U256};
use anyhow::Result;
use clap::Parser;
use tokio::sync::{mpsc, oneshot};

use account::Account;
use config::{Config, TransferType};
use monitor::{MonitorCommand, RegisterTx};
use rpc::RpcClient;
use stats::Stats;

const MAX_INIT_SENDER_INFLIGHT_TXS: usize = 60;

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
    let rpc = RpcClient::new(&config.rpc.url, chain_id, config.bench.rpc_concurrency);

    let worker_keys =
        config::derive_worker_keys(&config.faucet.private_key, config.bench.num_accounts);
    let mut accounts: Vec<Account> = worker_keys
        .iter()
        .map(|k| Account::from_key(k))
        .collect::<Result<_>>()?;

    let (register_tx, register_rx) = mpsc::channel::<MonitorCommand>(100000);
    let mut block_monitor = monitor::BlockMonitor::new(rpc.clone(), register_rx, cli.receipt);
    let pool_size = block_monitor.pool_size.clone();
    tokio::spawn(async move {
        if let Err(e) = block_monitor.run().await {
            log::error!("[monitor] exited: {}", e);
        }
    });

    if cli.recover {
        recover(&mut accounts, &config, &rpc, chain_id, register_tx.clone()).await?;
        return Ok(());
    }

    distribute_funds(&accounts, &config, &rpc, chain_id, register_tx.clone()).await?;

    let token_addresses: Vec<Address> = if config.bench.transfer_type == TransferType::Erc20 {
        let addrs =
            deploy_tokens(&accounts[0], &config, &rpc, chain_id, register_tx.clone()).await?;
        distribute_tokens(
            &accounts[0],
            &accounts,
            &addrs,
            &config,
            &rpc,
            chain_id,
            register_tx.clone(),
        )
        .await?;
        addrs
    } else {
        vec![]
    };

    log::info!(
        "[init] {} workers, chain_id={}, type={:?}, rpc_concurrency={}, num_inflight_senders={}",
        accounts.len(),
        chain_id,
        config.bench.transfer_type,
        config.bench.rpc_concurrency,
        config.bench.num_inflight_senders
    );

    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let active_count = Arc::new(AtomicUsize::new(accounts.len()));
    let inflight_sender_sem = Arc::new(tokio::sync::Semaphore::new(
        config.bench.num_inflight_senders,
    ));

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

    let mut handles = Vec::new();
    for (idx, account) in accounts.into_iter().enumerate() {
        let active = active_count.clone();
        let ctx = WorkerCtx {
            config: config.clone(),
            rpc: rpc.clone(),
            stats: stats.clone(),
            inflight_sender_sem: inflight_sender_sem.clone(),
            pool_size: pool_size.clone(),
            token_addresses: token_addresses.clone(),
            register_tx: register_tx.clone(),
        };

        handles.push(tokio::spawn(async move {
            let result = run_worker(idx, account, ctx).await;
            active.fetch_sub(1, Ordering::Relaxed);
            match result {
                Ok(()) => log::debug!("[worker#{}] stopped", idx),
                Err(e) => log::error!("[worker#{}] error: {}", idx, e),
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    stop.store(true, Ordering::Relaxed);
    stats.log_final();
    Ok(())
}

struct WorkerCtx {
    config: Config,
    rpc: RpcClient,
    stats: Arc<Stats>,
    inflight_sender_sem: Arc<tokio::sync::Semaphore>,
    pool_size: Arc<AtomicU64>,
    token_addresses: Vec<Address>,
    register_tx: mpsc::Sender<MonitorCommand>,
}

async fn run_worker(idx: usize, mut account: Account, ctx: WorkerCtx) -> Result<()> {
    let token_idx = idx % ctx.token_addresses.len().max(1);

    loop {
        while ctx.pool_size.load(Ordering::Relaxed) > ctx.config.bench.max_pool_size {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        account.nonce = ctx
            .rpc
            .get_nonce(account.address)
            .await
            .unwrap_or(account.nonce);

        let balance = account.balance(&ctx.rpc).await?;
        let gas_limit = if ctx.config.bench.transfer_type == TransferType::Native {
            21000
        } else {
            100000
        };
        let gas_wei = U256::from(ctx.config.bench.max_fee_per_gas)
            * U256::from(gas_limit)
            * U256::from(1_000_000_000u64);
        if balance < gas_wei {
            break;
        }

        let signed = if ctx.config.bench.transfer_type == TransferType::Native {
            let to = Address::from_slice(
                &alloy::primitives::keccak256(
                    [account.address.as_slice(), &account.nonce.to_be_bytes()].concat(),
                )[12..],
            );
            tx::build_native_tx(
                &account,
                to,
                U256::from(1),
                &ctx.config.bench,
                ctx.rpc.chain_id,
            )
            .await?
        } else {
            tx::build_erc20_tx(
                &account,
                ctx.token_addresses[token_idx],
                account.address,
                U256::from(1),
                &ctx.config.bench,
                ctx.rpc.chain_id,
            )
            .await?
        };

        let _permit = ctx.inflight_sender_sem.acquire().await?;
        let hash = tx::raw_tx_hash(&signed.raw);
        let (reply, rx) = oneshot::channel();
        let (registered_tx, registered_rx) = oneshot::channel();
        ctx.register_tx
            .send(MonitorCommand::Register(RegisterTx {
                hash,
                reply,
                registered: registered_tx,
            }))
            .await?;
        registered_rx.await?;

        match ctx.rpc.send_raw_tx(&signed.raw).await {
            Ok(returned_hash) => {
                if returned_hash != hash {
                    let _ = ctx.register_tx.send(MonitorCommand::Cancel(hash)).await;
                    ctx.stats.inc_failed();
                    log::error!(
                        "[worker#{}] send_raw_tx hash mismatch: expected 0x{}, got 0x{}",
                        idx,
                        hex::encode(hash.as_slice()),
                        hex::encode(returned_hash.as_slice())
                    );
                    break;
                }
                ctx.stats.inc_sent();
                match rx.await {
                    Ok(true) => ctx.stats.inc_confirmed(),
                    _ => {
                        ctx.stats.inc_failed();
                        break;
                    }
                }
                account.nonce += 1;
            }
            Err(e) => {
                let _ = ctx.register_tx.send(MonitorCommand::Cancel(hash)).await;
                log::error!("[worker#{}] send_raw_tx: {}", idx, e);
                break;
            }
        }
        drop(_permit);
    }
    Ok(())
}

async fn distribute_funds(
    accounts: &[Account],
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
) -> Result<()> {
    let faucet = Account::from_key(&config.faucet.private_key)?;
    let amount = config.faucet.faucet_eth_balance / U256::from(accounts.len() as u64);
    log::info!(
        "[init] sending {} wei to {} accounts",
        amount,
        accounts.len()
    );

    let base_nonce = rpc.get_nonce(faucet.address).await?;
    let mut raws = Vec::with_capacity(accounts.len());
    for (i, acc) in accounts.iter().enumerate() {
        let mut sender = faucet.clone();
        sender.nonce = base_nonce + i as u64;
        let signed =
            tx::build_native_tx(&sender, acc.address, amount, &config.bench, chain_id).await?;
        raws.push(signed.raw);
    }

    let hashes = submit_sender_limited_batches(rpc, &register_tx, raws).await?;
    log::info!("[init] ETH sent to {} accounts", hashes.len());
    Ok(())
}

async fn deploy_tokens(
    deployer: &Account,
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
) -> Result<Vec<Address>> {
    let code = tx::simple_token_bytecode();
    let supply = U256::from(1_000_000_000u64) * U256::from(10u64.pow(18));
    let base_nonce = rpc.get_nonce(deployer.address).await?;
    let mut raws = Vec::with_capacity(config.bench.num_tokens);
    let mut addresses = Vec::with_capacity(config.bench.num_tokens);

    for i in 0..config.bench.num_tokens {
        let nonce = base_nonce + i as u64;
        let mut sender = deployer.clone();
        sender.nonce = nonce;
        addresses.push(tx::create_address(sender.address, nonce));
        let signed = tx::build_deploy_tx(&sender, &code, supply, &config.bench, chain_id).await?;
        raws.push(signed.raw);
    }

    submit_sender_limited_batches(rpc, &register_tx, raws).await?;
    for (i, address) in addresses.iter().enumerate() {
        log::info!("[init] ERC20#{} deployed at 0x{:x}", i, address);
    }
    Ok(addresses)
}

async fn distribute_tokens(
    deployer: &Account,
    accounts: &[Account],
    tokens: &[Address],
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
) -> Result<()> {
    let amount = U256::from(1_000_000u64) * U256::from(10u64.pow(18));
    let targets: Vec<&Account> = accounts
        .iter()
        .filter(|a| a.address != deployer.address)
        .collect();
    if targets.is_empty() || tokens.is_empty() {
        return Ok(());
    }

    let base_nonce = rpc.get_nonce(deployer.address).await?;
    let mut raws = Vec::with_capacity(tokens.len() * targets.len());

    for (ti, token) in tokens.iter().enumerate() {
        for (ai, acc) in targets.iter().enumerate() {
            let mut sender = deployer.clone();
            sender.nonce = base_nonce + (ti * targets.len() + ai) as u64;
            let signed = tx::build_erc20_tx(
                &sender,
                *token,
                acc.address,
                amount,
                &config.bench,
                chain_id,
            )
            .await?;
            raws.push(signed.raw);
        }
    }

    let hashes = submit_sender_limited_batches(rpc, &register_tx, raws).await?;
    log::info!("[init] tokens distributed: {} txs", hashes.len());
    Ok(())
}

async fn recover(
    accounts: &mut [Account],
    config: &Config,
    rpc: &RpcClient,
    chain_id: u64,
    register_tx: mpsc::Sender<MonitorCommand>,
) -> Result<()> {
    let faucet = account::faucet_address(&config.faucet.private_key)?;
    let gas =
        U256::from(config.bench.max_fee_per_gas) * U256::from(21000) * U256::from(1_000_000_000u64);

    log::info!(
        "[recover] scanning {} accounts gas_reserve={} wei",
        accounts.len(),
        gas
    );

    let mut tasks = tokio::task::JoinSet::new();
    for (i, account) in accounts.iter().cloned().enumerate() {
        let rpc = rpc.clone();
        let bench = config.bench.clone();
        let address = account.address;
        tasks.spawn(async move {
            let result = build_recover_tx(account, rpc, bench, faucet, gas, chain_id).await;
            (i, address, result)
        });
    }

    let total = accounts.len();
    let mut completed = 0usize;
    let mut raws = Vec::new();
    let mut skipped = 0usize;
    let mut errors = 0usize;
    while let Some(joined) = tasks.join_next().await {
        let (i, address, result) = joined?;
        completed += 1;
        match result {
            Ok(Some(raw)) => raws.push(raw),
            Ok(None) => skipped += 1,
            Err(e) => {
                errors += 1;
                log::warn!("[recover] account#{} addr=0x{:x}: {}", i, address, e);
            }
        }
        if completed.is_multiple_of(10) || completed == total {
            log::debug!(
                "[recover] scanned={} prepared={} skipped={} errors={}",
                completed,
                raws.len(),
                skipped,
                errors
            );
        }
    }

    log::info!(
        "[recover] scan done prepared={} skipped={} errors={}",
        raws.len(),
        skipped,
        errors
    );
    if raws.is_empty() {
        log::info!("[recover] no accounts with balance above gas reserve");
        return Ok(());
    }
    log::info!("[recover] submitting {} recovery txs", raws.len());
    let hashes = submit_batch_and_confirm(rpc, &register_tx, raws).await?;
    log::info!("[recover] {} accounts -> faucet", hashes.len());
    Ok(())
}

async fn build_recover_tx(
    mut account: Account,
    rpc: RpcClient,
    bench: config::BenchConfig,
    faucet: Address,
    gas: U256,
    chain_id: u64,
) -> Result<Option<Bytes>> {
    let balance = account.balance(&rpc).await?;
    if balance <= gas {
        return Ok(None);
    }
    account.nonce = rpc.get_nonce(account.address).await?;
    let signed = tx::build_native_tx(&account, faucet, balance - gas, &bench, chain_id).await?;
    Ok(Some(signed.raw))
}

async fn submit_sender_limited_batches(
    rpc: &RpcClient,
    register_tx: &mpsc::Sender<MonitorCommand>,
    raws: Vec<Bytes>,
) -> Result<Vec<B256>> {
    if raws.len() <= MAX_INIT_SENDER_INFLIGHT_TXS {
        return submit_batch_and_confirm(rpc, register_tx, raws).await;
    }

    log::info!(
        "[init] limiting single-sender inflight: {} txs in batches of {}",
        raws.len(),
        MAX_INIT_SENDER_INFLIGHT_TXS
    );

    let total_batches = raws.len().div_ceil(MAX_INIT_SENDER_INFLIGHT_TXS);
    let mut hashes = Vec::with_capacity(raws.len());
    for (idx, chunk) in raws.chunks(MAX_INIT_SENDER_INFLIGHT_TXS).enumerate() {
        log::debug!(
            "[init] submitting sender batch {}/{} txs={}",
            idx + 1,
            total_batches,
            chunk.len()
        );
        hashes.extend(submit_batch_and_confirm(rpc, register_tx, chunk.to_vec()).await?);
    }
    Ok(hashes)
}

async fn submit_batch_and_confirm(
    rpc: &RpcClient,
    register_tx: &mpsc::Sender<MonitorCommand>,
    raws: Vec<Bytes>,
) -> Result<Vec<B256>> {
    let expected_hashes: Vec<B256> = raws.iter().map(tx::raw_tx_hash).collect();
    let confirmations = register_confirmations(register_tx, &expected_hashes).await?;
    let hashes = rpc.batch_send_raw_txs(&raws).await?;
    ensure_hashes_match(&expected_hashes, &hashes)?;
    wait_confirmations(confirmations).await?;
    Ok(hashes)
}

async fn register_confirmations(
    register_tx: &mpsc::Sender<MonitorCommand>,
    hashes: &[B256],
) -> Result<Vec<(B256, oneshot::Receiver<bool>)>> {
    let mut confirmations = Vec::with_capacity(hashes.len());
    let mut registered_receivers = Vec::with_capacity(hashes.len());
    for &hash in hashes {
        let (reply, rx) = oneshot::channel();
        let (registered_tx, registered_rx) = oneshot::channel();
        register_tx
            .send(MonitorCommand::Register(RegisterTx {
                hash,
                reply,
                registered: registered_tx,
            }))
            .await?;
        registered_receivers.push(registered_rx);
        confirmations.push((hash, rx));
    }
    for registered_rx in registered_receivers {
        registered_rx.await?;
    }
    Ok(confirmations)
}

async fn wait_confirmations(confirmations: Vec<(B256, oneshot::Receiver<bool>)>) -> Result<()> {
    for (hash, rx) in confirmations {
        match rx.await {
            Ok(true) => {}
            Ok(false) => anyhow::bail!("tx 0x{} failed", hex::encode(hash.as_slice())),
            Err(_) => anyhow::bail!(
                "confirmation channel closed for tx 0x{}",
                hex::encode(hash.as_slice())
            ),
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
    for (i, (expected, actual)) in expected.iter().zip(actual.iter()).enumerate() {
        if expected != actual {
            anyhow::bail!(
                "batch tx hash mismatch at {}: expected 0x{}, got 0x{}",
                i,
                hex::encode(expected.as_slice()),
                hex::encode(actual.as_slice())
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
