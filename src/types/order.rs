use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::types::{MarketInfo, TokenInfo};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TokenOrderType {
    CreationTime, // created_at
    LatestTrade,  // market latest_trade_at
}

impl TokenOrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TokenOrderType::CreationTime => "creation_time",
            TokenOrderType::LatestTrade => "latest_trade",
        }
    }
}

impl FromStr for TokenOrderType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "creation_time" => Ok(TokenOrderType::CreationTime),
            "latest_trade" => Ok(TokenOrderType::LatestTrade),
            _ => Err(format!("Invalid TokenOrderType: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderToken {
    pub token_info: TokenInfo,
    pub market_info: MarketInfo,
    pub percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderMessage {
    pub order_type: TokenOrderType,
    pub tokens: Vec<OrderToken>,
    pub total_count: i64,
}
