# simple_bench

极简 EVM 压测工具，用于 Gravity 网络性能测试。

## 快速开始

```bash
# 1. 创建配置文件
cp bench.toml.template bench.toml
# 编辑 bench.toml，填入 faucet private_key

# 2. 运行压测
cargo run

# 3. 回收资金
cargo run -- --recover
```

## 命令行参数

```
simple_bench [OPTIONS]

OPTIONS:
  -c, --config <PATH>   配置文件路径 (默认: bench.toml)
      --recover         回收所有 worker 剩余资金到 faucet
      --receipt         监控 receipt，区分 success/fail (默认只确认上链)
  -h, --help            帮助
```

## 配置说明 (bench.toml)

```toml
[faucet]
private_key = "0x..."
faucet_eth_balance = 5000    # 分发总额(ETH), 平均分配给 num_accounts

[rpc]
url = "https://mainnet-rpc.gravity.xyz"

[bench]
num_accounts = 100           # 测试账户数
rpc_concurrency = 32         # 全局 JSON-RPC HTTP 请求并发上限
num_inflight_senders = 50    # 最大未上链确认 sender 数
transfer_type = "native"     # "native" 或 "erc20"
max_fee_per_gas = 50         # Gwei
max_priority_fee_per_gas = 1 # Gwei
num_tokens = 2               # ERC20 合约数 (transfer_type=erc20 时生效)
max_pool_size = 40000        # mempool pending 上限，超过则暂停发送
```

## 工作原理

1. **启动**: faucet 批量/并发提交初始化交易，然后通过区块监控确认上链
2. **压测**: 每个 worker 循环发交易，最多 `num_inflight_senders` 个 sender 同时等待上链确认
3. **RPC 流控**: 所有 JSON-RPC 请求共享 `rpc_concurrency` 并发上限，收到 RPC 回复后释放额度
4. **Mempool 流控**: monitor 定期检查 mempool size，超过 `max_pool_size` 则暂停发送
5. **结束**: 所有 worker 余额不足时自动结束，输出统计

### Receipt 监控

- 默认模式: 通过区块 tx hash 匹配确认上链，不查 receipt
- `--receipt` 模式: 对匹配到的 tx hash 额外 batch 拉取 receipt，区分 success/fail/revert

两种模式下，nonce 均在 tx 被打包进区块后递增（无论成功或 revert）。

## 依赖

- Rust 2021+
- alloy 2.0 (EVM 签名/编码)
- reqwest (HTTP RPC)
