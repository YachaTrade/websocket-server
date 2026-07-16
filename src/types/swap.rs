use serde::{Deserialize, Serialize};

use crate::types::{AccountInfo, SwapInfo};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSwap {
    pub account_info: AccountInfo,
    pub swap_info: SwapInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSwapMessage {
    #[serde(skip_serializing)]
    pub token_id: String,
    pub swaps: Vec<TokenSwap>,
    pub total_count: i64,
}
