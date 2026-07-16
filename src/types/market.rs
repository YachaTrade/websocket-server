use serde::{Deserialize, Serialize};

use crate::types::MarketInfo;

/// Market 정보를 WebSocket으로 전송하기 위한 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenMarketMessage {
    #[serde(skip_serializing)]
    pub token_id: String,
    pub market_info: MarketInfo,
}
