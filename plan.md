# simple_bench 重构计划

## 1. 概述

将当前 `simple_bench` 拆分为三个独立子命令，并对压测阶段进行深度优化：去掉不必要的 RPC 调用、引入跨 worker batch 发送、本地余额维护、以及基于 receipt 的精确 gas 扣费。

**核心优化：**
- Nonce 本地维护（去掉 `get_nonce` RPC）
- Balance 本地维护（去掉 `get_balance` RPC）
- 多 worker 的 `send_raw_tx` 合并为 JSON-RPC batch 调用
- Receipt 模式返回实际 gas 消耗，精确本地余额
- 单个 sender 失败即退出，50% sender 退出后全局停止

---

## 2. CLI 变更

### 现有
```
simple_bench [-c config] [--recover] [--receipt]
```

### 改为
```
simple_bench faucet   [-c config]
simple_bench bench    [-c config]
simple_bench recover  [-c config]
```

| 子命令 | 功能 |
|--------|------|
| `faucet` | 分发 native ETH + 部署 ERC20 + 分发 token |
| `bench` | 压测（假设账户已有资金） |
| `recover` | 回收剩余资金到 faucet |

**所有 confirm 统一走 receipt**（`batch_get_receipts`），不再有非 receipt 模式。这是唯一能精确识别 revert + 精确追踪余额的途径。

### 配置文件

`bench.toml` 结构保持不变。不同子命令只关注自己需要的字段：

| 字段 | faucet | bench | recover |
|------|--------|-------|---------|
| `faucet.*` | ✅ | — | ✅ |
| `rpc.*` | ✅ | ✅ | ✅ |
| `bench.transfer_type` | ✅ | ✅ | — |
| `bench.num_accounts` | ✅ | ✅ | ✅ |
| `bench.num_tokens` | ✅ | ✅ | — |
| `bench.rpc_concurrency` | ✅ | ✅ | ✅ |
| `bench.num_inflight_senders` | — | ✅ | — |
| `bench.max_fee_per_gas` | ✅ | ✅ | ✅ |
| `bench.max_priority_fee_per_gas` | ✅ | ✅ | ✅ |
| `bench.max_pool_size` | — | ✅ | — |
| `bench.faucet_eth_balance` | ✅ | — | — |
| `bench.faucet_level` | ✅ | — | ✅ |
| `bench.faucet_eth_per_level` | ✅ | — | — |
| `bench.rpc_batch_size` | ✅ | ✅ | ✅ |

`num_inflight_senders` 在 bench 中的语义改为**同时进行中的 sender 数量上限**（与 `rpc_concurrency` 解耦）。

`faucet_level`（默认 10）控制 fan-out：master faucet 先分发到 `faucet_level` 个中间账户，这些中间账户再**并行**分发到 `num_accounts` 个测试账户。中间账户不参与 bench 压测，仅在 recover 时回收。

`faucet_eth_per_level`（默认 `faucet_eth_balance / faucet_level`）：master 发给每个中间账户的 ETH 数量。中间账户用这笔钱做两件事：① 向自己的 worker 子集分发 native ETH，② 向自己的 worker 子集分发 ERC20 token 时支付 gas。

`rpc_batch_size`（默认 64）：**所有** JSON-RPC batch 调用的单次请求上限，统一控制 HTTP body 大小。作用于 `batch_send_raw_txs`、`batch_get_block_tx_hashes`、`batch_get_receipts`、`batch_get_nonces`、`batch_get_balances`。

---

## 3. 架构变更

### 3.1 新增组件：BatchSender

```
                         ┌──────────────┐
     Worker 1 ──────────→│              │────→ batch_send_raw_txs (1次 RPC)
     Worker 2 ──────────→│ BatchSender  │────→ 分发结果给各 worker
     Worker N ──────────→│              │
                         └──────────────┘
```

- 每个 worker 构建好 tx 后，通过 `mpsc::Sender` 将 `(raw_tx, reply_oneshot)` 发送给 BatchSender。worker 本地预计算 `tx_hash = keccak256(raw_tx)`，BatchSender 返回 RPC 返回的 hash 供校验
- BatchSender 收集 tx，满足以下任一条件时 flush：
  - 累积数量达到 `rpc_batch_size`
  - 收到全局 stop 信号（flush 剩余 tx 后退出）
- 只消耗 1 个 `rpc_concurrency` permit

### 3.2 Monitor 回传类型扩展

```rust
// 当前
oneshot::Sender<bool>  // 仅 success/fail

// 改为
struct TxReceipt {
    pub success: bool,
    pub gas_used: u64,
    pub effective_gas_price: u128,
}
oneshot::Sender<TxReceipt>
```

Monitor 始终拉取 receipt，`TxReceipt` 的三个字段始终填充实际值。不再有"非 receipt 模式"。

### 3.3 Worker 本地状态维护

`Account` 结构体增加 `balance` 字段（详见 4.6 节），worker 在本地维护 `nonce` 和 `balance`：

- `nonce`：确认后 `+= 1`，本地单调递增
- `balance`：确认后扣减 `gas_cost + value`（`saturating_sub`）

**启动时**：通过 `batch_get_nonces` + `batch_get_balances` 一次性 JSON-RPC batch 调用获取所有 worker 的初始状态（仅 bench 子命令需要，faucet/recover 不需要）。这 2 次 batch 调用各消耗 1 个 `rpc_concurrency` permit。

**每轮循环**：
1. 检查 `pool_size`（与当前一致）
2. 检查本地 `balance >= estimated_gas_cost`
3. 构建 tx
4. 发送给 BatchSender，等待返回 `tx_hash`
5. 注册到 Monitor，等待 `TxReceipt`
6. 用 receipt 更新 `nonce += 1`，`balance -= gas_cost + value`
7. 若 receipt 显示失败（`success == false`），退出该 sender

### 3.4 50% 退出条件

```rust
let active_count = Arc<new AtomicUsize(num_accounts)>;
let stop = Arc<new AtomicBool(false)>;

// 在 stats 日志打印时检查
fn check_stop() {
    if active_count.load() <= num_accounts / 2 {
        stop.store(true);
    }
}
```

stop 信号触发后：
- BatchSender flush 剩余 tx 并退出
- 剩余 worker 在 `pool_size` 检查或 `stop` 检查后逐步退出
- 主线程等所有 handle join 后输出最终统计

---

## 4. 文件级变更

### 4.1 `src/main.rs` — 主要重构

**CLI 改为子命令：**
```rust
#[derive(Parser)]
enum Cli {
    Faucet { #[arg(short, long)] config: Option<String> },
    Bench  { #[arg(short, long)] config: Option<String> },
    Recover { #[arg(short, long)] config: Option<String> },
}
```

**保留常量：**
- `MAX_INIT_SENDER_INFLIGHT_TXS = 60`：faucet 阶段每个 sender 的 inflight tx 上限（包括 master 和所有中间账户）

**删除：**
- `--receipt` / `--recover` CLI flags
- `submit_sender_limited_batches` 函数
- `WorkerCtx` 中的 `inflight_sender_sem`（bench 阶段 batch sender 替代）

**新增函数：**
- `run_faucet()` — native fan-out + ERC20 deploy + token fan-out（两阶段串行，每 sender 限 `MAX_INIT_SENDER_INFLIGHT_TXS`）
- `run_bench()` — 启动 BatchSender + Monitor + Workers
- `run_recover()` — 扫描 worker + 中间账户，回收至 master
- `spawn_batch_sender()` — 启动 BatchSender task
- `spawn_worker_batched()` — 使用 BatchSender 的 worker
- `submit_raws_async()` — 统一 batch 发送 + 异步注册 monitor，返回 confirmation receivers
- `submit_sender_chunks()` — faucent 单 sender 分批发送 + 等确认（替代旧 `submit_sender_limited_batches`，语义相同但走 receipt）

### 4.2 `src/monitor.rs` — 回传类型扩展

**新增：**
```rust
pub struct TxReceipt {
    pub success: bool,
    pub gas_used: u64,
    pub effective_gas_price: u128,
}
```

**修改：**
- 删除 `fetch_receipt: bool` 字段（始终拉 receipt）
- `pending: HashMap<B256, oneshot::Sender<bool>>` → `HashMap<B256, oneshot::Sender<TxReceipt>>`
- `RegisterTx.reply: oneshot::Sender<bool>` → `oneshot::Sender<TxReceipt>`
- `process_block_hashes`：匹配到 tx 后 batch 拉 receipt，解析 `gasUsed` 和 `effectiveGasPrice` 填入 `TxReceipt`
- `process_block_batch`：chunk 大小从硬编码 32 改为 `rpc_batch_size`

### 4.3 `src/rpc.rs` — 微小调整

现有 batch 方法保持不变。新增两个方法（已在步骤中列出）：
- `batch_get_nonces(&self, addrs: &[Address]) -> Result<Vec<u64>>`
- `batch_get_balances(&self, addrs: &[Address]) -> Result<Vec<U256>>`

所有 batch 方法由调用方按 `rpc_batch_size` 分块传入（`rpc.rs` 不负责分块，保持纯粹）。

### 4.4 `src/config.rs`

`BenchConfig` 新增字段：
```rust
pub rpc_batch_size: usize,        // 默认 64，所有 JSON-RPC batch 调用上限
pub faucet_level: usize,          // 默认 10
pub faucet_eth_per_level: U256,   // 默认 = faucet_eth_balance / faucet_level
```

其中 `faucet_eth_per_level` 未配置时自动计算。`faucet_level` 最大 clamp 为 `num_accounts`。

### 4.5 `src/stats.rs` — 不变

当前统计字段足够。

### 4.6 `src/account.rs` — 新增字段和方法

Account 增加 `balance` 字段：

```rust
#[derive(Clone)]
pub struct Account {
    pub key: String,
    pub address: Address,
    pub nonce: u64,
    pub balance: U256,   // 新增：本地维护的余额
}
```

新增方法：

```rust
impl Account {
    /// 估算一笔 tx 的 gas 开销（用于发送前余额充足检查）
    pub fn estimated_gas_cost(max_fee_per_gas: u64, gas_limit: u64) -> U256 {
        U256::from(max_fee_per_gas) * U256::from(gas_limit) * U256::from(1_000_000_000u64)
    }

    /// 用 receipt 更新本地 nonce 和 balance（receipt 始终有实际值）
    pub fn apply_receipt(
        &mut self,
        receipt: &TxReceipt,
        value: U256,
    ) {
        self.nonce += 1;
        let gas_cost = U256::from(receipt.gas_used) * U256::from(receipt.effective_gas_price);
        self.balance = self.balance.saturating_sub(gas_cost + value);
    }
}
```

### 4.7 `src/tx.rs` — 不变

### 4.8 `bench.toml.template` — 新增字段

```toml
[bench]
# ... 现有字段 ...

# 新增
rpc_batch_size = 64
faucet_level = 10
# faucet_eth_per_level 默认 = faucet_eth_balance / faucet_level
# 需要手动覆盖时取消注释：
# faucet_eth_per_level = 50
```

---

## 5. Bench 阶段数据流

```
┌──────────────────────────────────────────────────────────────────┐
│  main()                                                          │
│                                                                  │
│  spawn_batch_sender() ──→ tx_rx ←── mpsc channel                │
│  spawn_monitor() ────→ register_tx ←── mpsc channel              │
│                                                                  │
│  for each account:                                               │
│    spawn_worker(                                                   │
│      account,                                                     │
│      tx_tx: batch_sender 的 sender,                              │
│      register_tx: monitor 的 sender,                             │
│      active_count, stop, stats, pool_size, config                │
│    )                                                              │
│                                                                  │
│  wait all workers join                                           │
│  stop signal → batch_sender flush & exit                         │
│  log final stats                                                 │
└──────────────────────────────────────────────────────────────────┘

Worker loop:
┌─────────────────────────────────────────────────────┐
│ loop {                                                │
│   if stop || pool_size > max { sleep; continue }      │
│   if balance < estimated_gas { break }                │
│                                                       │
│   tx = build_tx(nonce_local)                          │
│   tx_hash = keccak256(raw_tx)                          │  ← 本地预计算
│   batch_sender.send(raw_tx).await?;                   │  ← 1次 mpsc，RPC 返回后校验 hash 一致
│   monitor.register(tx_hash)                           │  ← 1次 mpsc 交互
│   receipt = monitor.wait(tx_hash).await               │  ← 等待区块确认
│                                                       │
│   account.apply_receipt(receipt)                      │  ← 本地更新 nonce+balance
│   if !receipt.success { break }                       │
│   stats.inc_confirmed()                               │
│ }                                                     │
│ active_count.fetch_sub(1)                             │
│ if active_count <= num_accounts/2 { stop = true }     │
└─────────────────────────────────────────────────────┘

BatchSender loop:
┌──────────────────────────────────────────────┐
│ buf = []                                      │
│ loop {                                        │
│   select {                                    │
│     tx = tx_rx.recv() → buf.push(tx)         │
│     stop → flush & break                      │
│   }                                           │
│   if buf.len() >= rpc_batch_size { flush }    │
│ }                                             │
│                                                │
│ flush:                                          │
│   returned = rpc.batch_send_raw_txs(buf).await │
│   for each (worker, expected_hash, returned_hash): │
│     verify match → reply Ok / Err              │
│   buf.clear()                                  │
└──────────────────────────────────────────────┘
```

---

## 6. Faucet 阶段优化

核心改动：`submit_sender_limited_batches`（单 sender 串行等确认）→ `submit_sender_chunks`（按 `MAX_INIT_SENDER_INFLIGHT_TXS` 分批，每批 batch 发送后等 receipt 确认）+ 中间账户 fan-out。

底层 batch 发送由 `submit_raws_async` 统一处理：

```rust
/// 将 raws 按 rpc_batch_size 分批 JSON-RPC batch 发送，注册 monitor，
/// 返回所有 confirmation receivers 供上层统一等待。
async fn submit_raws_async(
    rpc: &RpcClient,
    register_tx: &mpsc::Sender<MonitorCommand>,
    raws: &[Bytes],
    batch_size: usize,
) -> Result<(Vec<B256>, Vec<oneshot::Receiver<TxReceipt>>)> {
    let mut hashes = Vec::with_capacity(raws.len());
    let mut all_confirmations = Vec::with_capacity(raws.len());
    for chunk in raws.chunks(batch_size) {
        let expected: Vec<B256> = chunk.iter().map(tx::raw_tx_hash).collect();
        let confirmations = register_confirmations(register_tx, &expected).await?;
        let returned = rpc.batch_send_raw_txs(chunk).await?;
        ensure_hashes_match(&expected, &returned)?;
        hashes.extend_from_slice(&expected);
        all_confirmations.extend(confirmations);
    }
    Ok((hashes, all_confirmations))
}
```

### 6.1 Faucet Fan-out（`faucet_level > 1`）

单 sender 受 nonce 顺序 + mempool 打包限制，分发慢。引入中间层实现并行分发。

Native 和 ERC20 分两阶段串行执行，与当前模式一致。每阶段内部使用 fan-out。

#### Phase 1: Native ETH 分发

```
1a. master → L 个中间账户 (L 笔 tx，单 sender，batch 发送)
  master ─→ inter_0  (faucet_eth_per_level wei)
  ...
  等待确认

1b. L 个中间账户各分发自己的 worker 子集（并行，L 个 sender）
  inter_0 ─→ worker_0, 1, ...
  inter_1 ─→ worker_k, k+1, ...
  ...
  等待确认
```

#### Phase 2: ERC20 部署 + 分发（`transfer_type = "erc20"`）

```
2a. master 部署 M 个 ERC20 合约 + 转移 token 给中间账户
  master ─→ deploy ERC20_0, ERC20_1, ...
  master ─→ transfer(inter_0, token_amount_per_inter) × L × M 笔 tx
  等待确认

2b. L 个中间账户并行分发 token 给各自的 worker 子集
  inter_0 ─→ transfer(worker_0, token_amount) × (workers_per_inter × M)
  ...
  等待确认
```

**Inflight 限制**：每个 sender（包括 master 和所有中间账户）同时未确认的 tx 不超过 `MAX_INIT_SENDER_INFLIGHT_TXS`（60）。L 个中间账户并行推进，每个内部串行分批（每批 ≤60，等待确认后再发下一批），L 路之间互不阻塞。理论最大并发 inflight = `faucet_level × 60`。

```rust
fn run_faucet() {
    // === Phase 1: Native ETH ===
    // 1a: master → intermediates（单 sender）
    submit_sender_chunks(rpc, register_tx, master, &intermediates,
        |s, inter| build_native_tx(s, inter.addr, faucet_eth_per_level),
    ).await?;

    // 1b: intermediates → workers（L 路并行，每路限 MAX_INIT_SENDER_INFLIGHT_TXS）
    let inter_nonces = rpc.batch_get_nonces(&inter_addrs).await?;
    let mut handles = vec![];
    for (idx, inter) in intermediates.iter().enumerate() {
        let mut inter = inter.clone();
        inter.nonce = inter_nonces[idx];
        let workers = worker_subset(idx).to_vec();
        handles.push(tokio::spawn(submit_sender_chunks(
            rpc.clone(), register_tx.clone(), inter, &workers,
            |s, w| build_native_tx(s, w.addr, eth_per_worker),
        )));
    }
    for h in handles { h.await??; }

    // === Phase 2: ERC20 (if transfer_type == Erc20) ===
    // 2a: master deploy + transfer to intermediates（单 sender）
    submit_sender_chunks(rpc, register_tx, master, &deploy_targets,
        |s, (i, bytecode, supply)| build_deploy_tx(s, bytecode, supply),
    ).await?;
    submit_sender_chunks(rpc, register_tx, master, &token_dist_targets,
        |s, (inter, token, amt)| build_erc20_tx(s, *token, inter.addr, *amt),
    ).await?;

    // 2b: intermediates → workers（L 路并行，每路限流）
    inter_nonces = rpc.batch_get_nonces(&inter_addrs).await?;
    handles.clear();
    for (idx, inter) in intermediates.iter().enumerate() {
        let mut inter = inter.clone();
        inter.nonce = inter_nonces[idx];
        let mut items = vec![];
        for token in &tokens {
            for w in &worker_subset(idx) {
                items.push((*token, *w, token_per_worker));
            }
        }
        handles.push(tokio::spawn(submit_sender_chunks(
            rpc.clone(), register_tx.clone(), inter, &items,
            |s, (token, w, amt)| build_erc20_tx(s, *token, w.addr, *amt),
        )));
    }
    for h in handles { h.await??; }
}
```

`submit_sender_chunks` 的核心逻辑：

```rust
/// 单个 sender 分批发送 target_items，每批 ≤ MAX_INIT_SENDER_INFLIGHT_TXS
async fn submit_sender_chunks<T, F>(
    rpc: RpcClient,
    register_tx: mpsc::Sender<MonitorCommand>,
    mut sender: Account,          // sender 账户（含 nonce 初值）
    target_items: &[T],           // 发送目标列表
    build_tx: F,                  // Fn(&Account, &T) -> Result<Bytes>
) -> Result<()>
where F: Fn(&Account, &T) -> Result<Bytes>
{
    for (chunk_idx, chunk) in target_items.chunks(MAX_INIT_SENDER_INFLIGHT_TXS).enumerate() {
        let mut raws = Vec::with_capacity(chunk.len());
        for (j, item) in chunk.iter().enumerate() {
            let mut s = sender.clone();
            s.nonce = sender.nonce + (chunk_idx * MAX_INIT_SENDER_INFLIGHT_TXS + j) as u64;
            raws.push(build_tx(&s, item)?);
        }
        let (_, c) = submit_raws_async(&rpc, &register_tx, &raws, rpc_batch_size).await?;
        wait_all_confirmations(c).await;
    }
    Ok(())
}
```

阶段之间串行（先 native 全部确认，再 ERC20）。每个中间账户内部的分批也是串行（等上一批确认再发下一批），防止同一 sender 的 nonce gap。

**中间账户 key 派生**（XOR 前 4 bytes，与 worker key 的 XOR 后 4 bytes 正交）：
```rust
fn derive_intermediate_keys(faucet_key: &str, level: usize) -> Vec<String> {
    let base = hex::decode(faucet_key.trim_start_matches("0x")).unwrap();
    (0..level).map(|i| {
        let mut kb = base.clone();
        let idx = (i as u32).to_be_bytes();
        for j in 0..4 { kb[j] ^= idx[j]; }
        format!("0x{}", hex::encode(&kb))
    }).collect()
}
```

**中间账户不参与 bench**：`run_bench` 只启动 `num_accounts` 个 worker。中间账户余额留在链上，等 recover 回收。

**Recover 阶段**：扫描 `num_accounts` 个 worker + `faucet_level` 个中间账户，全部回收至 master faucet。

---

## 7. Recover 阶段变更

当前 `recover` 已经使用 `submit_batch_and_confirm`（batch 发送 + 等确认），逻辑基本不变。

**变更：**
- 扫描范围 = `num_accounts` 个 worker + `faucet_level` 个中间账户
- 统一走 receipt confirm（与 faucet/bench 一致）
- nonce 从链上 `batch_get_nonces` 一次性获取（账户可能很久没动过，本地 nonce 不可信）

---

## 8. 边界情况

| 场景 | 处理 |
|------|------|
| 所有 worker 余额为 0 | 正常退出 |
| RPC 返回 hash 不匹配 | 与现有逻辑一致，cancel monitor 注册 |
| Batch send 部分失败 | JSON-RPC batch 可能部分成功，需要逐条检查返回 |
| 网络断开 | 与现有逻辑一致，worker 报错退出 |
| 50% 退出时 BatchSender 还有缓存 tx | flush 后退出，这些 tx 仍会被 monitor 跟踪 |
| receipt 为 null（tx 上链但未及时生成 receipt） | 视为失败，worker 退出 |
| nonce 跳跃（之前 tx 未被打包就退出了） | 重启时从链上获取 nonce 重新初始化 |
| faucet_level 不能整除 num_accounts | 最后几个中间账户多承担 1 个 worker |
| faucet_eth_per_level 不足覆盖分发 | 与当前逻辑一致：余额不足时退出 |
| 中间账户 key 与 worker key 碰撞 | 派生方法保证正交（XOR 位置不同），配置加载时校验 |
| faucet_level > num_accounts | 自动 clamp 为 num_accounts（每个中间账户对应 1 个 worker） |
| 中间账户 worker 数量 > MAX_INIT_SENDER_INFLIGHT_TXS | `submit_sender_chunks` 自动分批，每批等确认后再发下一批 |
| Phase 2 某中间账户 send 失败 | 独立 tokio task panic → JoinError，不影响其他中间账户，最终 join 时报错 |

---

## 9. 实现步骤（建议顺序）

1. **Monitor 类型扩展**：`bool` → `TxReceipt`，不影响现有逻辑
2. **Account 本地状态**：`Account` 增加 `balance` 字段 + `apply_receipt` 方法
3. **RPC batch 补充**：新增 `batch_get_nonces`、`batch_get_balances`（`rpc.rs`）
4. **BatchSender**：独立组件，实现 collect/flush/reply 逻辑（仅 bench 阶段使用）
5. **Faucet 统一**：`submit_sender_chunks` + `submit_raws_async` 替换 `submit_sender_limited_batches`
6. **Faucet fan-out**：中间账户派生 + L 路并行分发（每路限 `MAX_INIT_SENDER_INFLIGHT_TXS`）
7. **Worker 改造**：接入 BatchSender，去掉 `get_nonce`/`get_balance`，本地维护
8. **CLI 拆分**：三步子命令 `faucet` / `bench` / `recover`
9. **50% 退出条件**：全局 `stop` 信号
10. **Recover 扩展**：扫描 worker + 中间账户
11. **配置更新**：`bench.toml` 新字段 + `bench.toml.template`
12. **测试验证**

---

## 10. 未解决 / 待讨论

- [ ] `rpc_batch_size = 64` 是否适合 Gravity RPC 的单次 body 大小限制，可能需要实际测试确认。
- [ ] `--receipt` 已删除，confirm 统一走 receipt。✅ 已决策。
- [ ] faucet 阶段发送量巨大时（如 10000 账户 + ERC20），native 和 ERC20 分阶段串行，每阶段内部 batch 发送，mempool 压力可控。✅ 已决策。
