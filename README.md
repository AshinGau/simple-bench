# simple_bench

Gravity EVM 极简压测工具。支持多种交易类型，用于链上吞吐量和延迟测试。

## 快速开始

```bash
# 1. 配置文件
cp bench.toml.template bench.toml
# 编辑：填 faucet private_key、RPC url、选择 transfer_type

# 2. 初始化资金 + 部署合约
cargo run -- faucet

# 3. 压测
cargo run -- bench

# 4. 回收剩余资金（可选）
cargo run -- recover
```

## 命令

```
simple_bench faucet    初始化：分发 ETH、部署合约、分发 token
simple_bench bench     压测（假设 faucet 已完成）
simple_bench recover   回收所有账户剩余资金到 faucet
```

每个命令支持 `-c <config>` 指定配置文件（默认 `bench.toml`），`--fast` 跳过 receipt 拉取。

## 工作原理

```
┌─────────┐     ┌──────────┐     ┌───────────┐
│ faucet  │ ──▶ │  worker  │ ──▶ │   chain   │
│ 分发资金 │     │ 循环发交易 │     │ (Gravity) │
│ 部署合约 │     └──────────┘     └───────────┘
└─────────┘           │                │
                      ▼                ▼
               ┌──────────┐    ┌───────────┐
               │BatchSender│    │  Monitor  │
               │ 批量发送   │    │ 追踪确认   │
               └──────────┘    └───────────┘
```

1. **faucet**: 从 faucet 私钥派生出 N 个 worker 账户，分层分发 ETH；根据 transfer_type 部署 ERC20 / Delegate 合约并分发 token
2. **bench**: 每个 worker 循环发送交易 → BatchSender 批量提交 → Monitor 追踪上链 → 更新 nonce/余额
3. **flow control**: Monitor 定期检查 mempool size，超过 `max_pool_size` 暂停发送；RPC 请求受 `rpc_concurrency` 上限约束
4. **结束**: worker 余额低于 gas 预估时退出；超过半数 worker 退出后全局停止

### faucet 分阶段执行

```
Phase 1: Native ETH
  1a: faucet → intermediate（分层分发）
  1b: intermediate → workers（并行，按 workers/level 分片）

Phase 2: ERC20（仅 transfer_type=erc20 或 mix）
  2a: faucet 部署 N 个 ERC20 合约 → 向 intermediate 分发 token
  2b: intermediate → workers（并行分发 token）

Phase 3: EIP-7702 Delegate（仅 transfer_type=eip7702 或 mix）
  3:  faucet 部署 Delegate 合约
```

## 配置说明 (bench.toml)

### [faucet]

| 参数 | 类型 | 说明 |
|---|---|---|
| `private_key` | hex | faucet 私钥，资金和合约从该地址发出 |
| `faucet_eth_balance` | ETH | 分发总 ETH 量，均分到 `faucet_level` 个 intermediate |

### [rpc]

| 参数 | 类型 | 说明 |
|---|---|---|
| `url` | string | Gravity RPC 端点 |

### [bench]

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `num_accounts` | int | — | worker 账户总数 |
| `rpc_concurrency` | int | — | 全局限流：同时进行的 JSON-RPC 请求数上限 |
| `transfer_type` | string | — | 交易类型，见下方 |
| `max_fee_per_gas` | int | — | EIP-1559 maxFeePerGas（Gwei） |
| `max_priority_fee_per_gas` | int | — | EIP-1559 maxPriorityFeePerGas（Gwei） |
| `max_pool_size` | int | — | mempool pending tx 上限，超过暂停发送 |
| `num_tokens` | int | `0` | ERC20 合约数（erc20 / mix 时生效） |
| `rpc_batch_size` | int | `64` | 每批 JSON-RPC batch 请求的 tx 数 |
| `faucet_level` | int | `10` | intermediate 层数，worker 被均分成 level 组并行分发 |
| `faucet_eth_per_level` | ETH | 均分 | 每个 intermediate 收到的 ETH（默认 `faucet_eth_balance / faucet_level`） |

### transfer_type

| 值 | 交易内容 | 消耗 gas/笔 | 需要 faucet |
|---|---|---|---|
| `native` | EIP-1559 ETH 转账，value=1 wei | 21,000 | ETH 分发 |
| `erc20` | ERC20 transfer | 100,000 | ETH 分发 + 部署 ERC20 + 分发 token |
| `eip2935` | 调用 `HISTORY_STORAGE`（EIP-2935）查询区块哈希 | 50,000 | ETH 分发 |
| `eip7702` | EIP-7702 SetCode 委托交易 | 100,000 | ETH 分发 + 部署 Delegate 合约 |
| `mix` | 四种类型轮转（native→erc20→eip2935→eip7702） | 按类型 | 全部（ETH + ERC20 + Delegate） |

## 依赖

- Rust 2021+
- alloy 2.0（EVM 签名/编码/类型）
- reqwest（HTTP JSON-RPC）
