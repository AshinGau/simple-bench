use alloy::consensus::{SignableTransaction, TxEip1559};
use alloy::eips::Encodable2718;
use alloy::primitives::{keccak256, Address, Bytes, B256, U256};
use alloy::signers::Signer;
use anyhow::Result;

use crate::account::Account;
use crate::config::BenchConfig;

pub struct SignedTx {
    pub raw: Bytes,
}

fn gas_prices(config: &BenchConfig) -> (u128, u128) {
    (
        config.max_priority_fee_per_gas as u128 * 1_000_000_000,
        config.max_fee_per_gas as u128 * 1_000_000_000,
    )
}

async fn sign_and_encode(account: &Account, tx: TxEip1559) -> Result<SignedTx> {
    let signer = account.signer()?;
    let hash = tx.signature_hash();
    let sig = signer.sign_hash(&hash).await?;
    let signed = tx.into_signed(sig);
    Ok(SignedTx {
        raw: Bytes::from(signed.encoded_2718()),
    })
}

/// Build and sign a native transfer.
pub async fn build_native_tx(
    account: &Account,
    to: Address,
    value: U256,
    config: &BenchConfig,
    chain_id: u64,
) -> Result<SignedTx> {
    let (max_prio_wei, max_fee_wei) = gas_prices(config);
    let tx = TxEip1559 {
        chain_id,
        nonce: account.nonce,
        max_priority_fee_per_gas: max_prio_wei,
        max_fee_per_gas: max_fee_wei,
        gas_limit: 21000,
        to: alloy::primitives::TxKind::Call(to),
        value,
        access_list: Default::default(),
        input: Bytes::new(),
    };
    sign_and_encode(account, tx).await
}

/// Build and sign an ERC20 transfer(address to, uint256 amount).
pub async fn build_erc20_tx(
    account: &Account,
    token: Address,
    to: Address,
    amount: U256,
    config: &BenchConfig,
    chain_id: u64,
) -> Result<SignedTx> {
    // transfer(address,uint256) selector = 0xa9059cbb
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&[0xa9, 0x05, 0x9c, 0xbb]);
    let mut addr_padded = [0u8; 32];
    addr_padded[12..].copy_from_slice(to.as_slice());
    data.extend_from_slice(&addr_padded);
    data.extend_from_slice(&amount.to_be_bytes::<32>());

    let (max_prio_wei, max_fee_wei) = gas_prices(config);
    let tx = TxEip1559 {
        chain_id,
        nonce: account.nonce,
        max_priority_fee_per_gas: max_prio_wei,
        max_fee_per_gas: max_fee_wei,
        gas_limit: 100000,
        to: alloy::primitives::TxKind::Call(token),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::from(data),
    };
    sign_and_encode(account, tx).await
}

/// Build a deploy transaction for a minimal ERC20 contract.
pub async fn build_deploy_tx(
    account: &Account,
    bytecode: &[u8],
    initial_supply: U256,
    config: &BenchConfig,
    chain_id: u64,
) -> Result<SignedTx> {
    let mut deploy_data = bytecode.to_vec();
    deploy_data.extend_from_slice(&initial_supply.to_be_bytes::<32>());

    let (max_prio_wei, max_fee_wei) = gas_prices(config);
    let tx = TxEip1559 {
        chain_id,
        nonce: account.nonce,
        max_priority_fee_per_gas: max_prio_wei,
        max_fee_per_gas: max_fee_wei,
        gas_limit: 1500000,
        to: alloy::primitives::TxKind::Create,
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::from(deploy_data),
    };
    sign_and_encode(account, tx).await
}

/// Minimal ERC20 bytecode (SimpleToken).
pub fn simple_token_bytecode() -> Vec<u8> {
    let hex_str = include_str!("erc20_bytecode.txt").trim();
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    hex::decode(hex_str).expect("invalid ERC20 bytecode in erc20_bytecode.txt")
}

pub fn raw_tx_hash(raw: &Bytes) -> B256 {
    keccak256(raw.as_ref())
}

pub fn create_address(sender: Address, nonce: u64) -> Address {
    let mut payload = Vec::with_capacity(32);
    encode_rlp_bytes(&mut payload, sender.as_slice());
    encode_rlp_u64(&mut payload, nonce);

    let mut encoded = Vec::with_capacity(payload.len() + 1);
    encode_rlp_list(&mut encoded, &payload);
    let hash = keccak256(&encoded);
    Address::from_slice(&hash[12..])
}

fn encode_rlp_list(out: &mut Vec<u8>, payload: &[u8]) {
    encode_rlp_header(out, 0xc0, 0xf7, payload.len());
    out.extend_from_slice(payload);
}

fn encode_rlp_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.len() == 1 && bytes[0] < 0x80 {
        out.push(bytes[0]);
        return;
    }
    encode_rlp_header(out, 0x80, 0xb7, bytes.len());
    out.extend_from_slice(bytes);
}

fn encode_rlp_u64(out: &mut Vec<u8>, value: u64) {
    if value == 0 {
        out.push(0x80);
        return;
    }
    let bytes = value.to_be_bytes();
    let first = bytes
        .iter()
        .position(|b| *b != 0)
        .unwrap_or(bytes.len() - 1);
    encode_rlp_bytes(out, &bytes[first..]);
}

fn encode_rlp_header(out: &mut Vec<u8>, short_base: u8, long_base: u8, len: usize) {
    if len <= 55 {
        out.push(short_base + len as u8);
        return;
    }
    let len_bytes = usize_to_be_bytes(len);
    out.push(long_base + len_bytes.len() as u8);
    out.extend_from_slice(&len_bytes);
}

fn usize_to_be_bytes(value: usize) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let first = bytes
        .iter()
        .position(|b| *b != 0)
        .unwrap_or(bytes.len() - 1);
    bytes[first..].to_vec()
}
