use alloy::consensus::{SignableTransaction, TxEip1559};
use alloy::eips::Encodable2718;
use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::signers::Signer;
use anyhow::Result;

use crate::account::Account;
use crate::config::BenchConfig;

pub struct SignedTx {
    pub raw: Bytes,
    pub hash: TxHash,
}

/// Build and sign a native transfer.
pub async fn build_native_tx(
    account: &Account,
    to: Address,
    value: U256,
    config: &BenchConfig,
    chain_id: u64,
) -> Result<SignedTx> {
    let signer = account.signer()?;

    let tx = TxEip1559 {
        chain_id,
        nonce: account.nonce,
        max_priority_fee_per_gas: config.max_priority_fee_per_gas as u128,
        max_fee_per_gas: config.max_fee_per_gas as u128,
        gas_limit: 21000,
        to: alloy::primitives::TxKind::Call(to),
        value,
        access_list: Default::default(),
        input: Bytes::new(),
    };

    let hash = tx.signature_hash();
    let sig = signer.sign_hash(&hash).await?;
    let signed = tx.into_signed(sig);
    let tx_hash = *signed.hash();
    let raw = signed.encoded_2718();

    Ok(SignedTx {
        raw: Bytes::from(raw),
        hash: tx_hash,
    })
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
    let signer = account.signer()?;

    // transfer(address,uint256) selector = 0xa9059cbb
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&[0xa9, 0x05, 0x9c, 0xbb]);
    let mut addr_padded = [0u8; 32];
    addr_padded[12..].copy_from_slice(to.as_slice());
    data.extend_from_slice(&addr_padded);
    let mut amount_bytes = [0u8; 32];
    let amount_vec = amount.to_be_bytes_vec();
    let start = 32 - amount_vec.len();
    amount_bytes[start..].copy_from_slice(&amount_vec);
    data.extend_from_slice(&amount_bytes);

    let tx = TxEip1559 {
        chain_id,
        nonce: account.nonce,
        max_priority_fee_per_gas: config.max_priority_fee_per_gas as u128,
        max_fee_per_gas: config.max_fee_per_gas as u128,
        gas_limit: 100000,
        to: alloy::primitives::TxKind::Call(token),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::from(data),
    };

    let hash = tx.signature_hash();
    let sig = signer.sign_hash(&hash).await?;
    let signed = tx.into_signed(sig);
    let tx_hash = *signed.hash();
    let raw = signed.encoded_2718();

    Ok(SignedTx {
        raw: Bytes::from(raw),
        hash: tx_hash,
    })
}

/// Build a deploy transaction for a minimal ERC20 contract.
pub async fn build_deploy_tx(
    account: &Account,
    bytecode: &[u8],
    initial_supply: U256,
    config: &BenchConfig,
    chain_id: u64,
) -> Result<SignedTx> {
    let signer = account.signer()?;

    let mut deploy_data = bytecode.to_vec();
    let mut supply_bytes = [0u8; 32];
    let supply_vec = initial_supply.to_be_bytes_vec();
    let start = 32 - supply_vec.len();
    supply_bytes[start..].copy_from_slice(&supply_vec);
    deploy_data.extend_from_slice(&supply_bytes);

    let tx = TxEip1559 {
        chain_id,
        nonce: account.nonce,
        max_priority_fee_per_gas: config.max_priority_fee_per_gas as u128,
        max_fee_per_gas: config.max_fee_per_gas as u128,
        gas_limit: 1500000,
        to: alloy::primitives::TxKind::Create,
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::from(deploy_data),
    };

    let hash = tx.signature_hash();
    let sig = signer.sign_hash(&hash).await?;
    let signed = tx.into_signed(sig);
    let tx_hash = *signed.hash();
    let raw = signed.encoded_2718();

    Ok(SignedTx {
        raw: Bytes::from(raw),
        hash: tx_hash,
    })
}

/// Minimal ERC20 bytecode (SimpleToken).
pub fn simple_token_bytecode() -> Vec<u8> {
    let hex_str = include_str!("erc20_bytecode.txt").trim();
    hex::decode(hex_str).expect("invalid ERC20 bytecode in erc20_bytecode.txt")
}
