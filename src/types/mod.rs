pub mod chart;
pub mod market;
pub mod metrics;
pub mod order;
pub mod stream;
pub mod swap;

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
//Token

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TokenInfo {
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub image_uri: String,
    #[serde(default)]
    pub description: Option<String>,
    pub is_graduated: bool,
    pub is_nsfw: bool,
    #[serde(default)]
    pub twitter: Option<String>,
    #[serde(default)]
    pub telegram: Option<String>,
    #[serde(default)]
    pub website: Option<String>,
    pub created_at: i64,
    pub creator: AccountInfo,
    pub is_cto: bool,
    pub version: TokenVersion,
}

/// Token version enum: V2 migration에서 token.version 컬럼 대응
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq)]
#[sqlx(type_name = "VARCHAR")]
pub enum TokenVersion {
    V1,
    V2,
}

/// Account information
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AccountInfo {
    pub account_id: String,
    pub nickname: String,
    pub bio: String,
    pub image_uri: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, Default)]
#[sqlx(type_name = "VARCHAR")]
#[sqlx(rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketType {
    #[default]
    Curve, // "CURVE"
    Dex,   // "DEX"
}
/// Quote token 메타데이터 (multi-quote 지원)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuoteInfo {
    pub quote_id: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub image_uri: String,
}

/// Fee 설정 정보 (V2 전용)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeInfo {
    /// 크리에이터 수수료율 (basis points)
    pub creator_fee_rate: i16,
    /// 커브 프로토콜 수수료율 (basis points)
    pub curve_protocol_fee_rate: i16,
    /// DEX 프로토콜 수수료율 (basis points)
    pub dex_protocol_fee_rate: i16,
}

/// Market information with pricing data
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct MarketInfo {
    pub market_type: MarketType,
    pub token_id: String,
    /// Quote token 정보 (V1: WMON, V2: quoteToken - WMON이 아닐 수 있음)
    pub quote_info: QuoteInfo,
    pub market_id: String,
    pub reserve_native: String,
    /// Quote reserve (V2 schema alias; V1에서는 reserve_native와 동일 값)
    #[serde(default)]
    pub reserve_quote: String,
    pub reserve_token: String,
    /// Token/USD price
    pub token_price: String,
    /// MON/USD price
    pub native_price: String,
    /// Quote/USD price (V2 schema alias; V1에서는 native_price와 동일 값)
    #[serde(default)]
    pub quote_price: String,
    /// MON/Token price
    pub price: String,
    /// USD/Token price
    pub price_usd: String,
    /// MON/Token price
    pub price_native: String,
    /// Quote/Token price (V2 schema alias; V1에서는 price_native와 동일 값)
    #[serde(default)]
    pub price_quote: String,
    /// Total supply (used for market cap calculation in bonding curve)
    pub total_supply: String,
    /// Volume (used for tokne total volume)
    pub volume: String,
    // Ath price(USD)
    pub ath_price: String,
    // Ath price(USD)
    pub ath_price_usd: String,
    //Ath price(Native)
    pub ath_price_native: String,
    /// ATH price (Quote) — V2 alias; V1에서는 ath_price_native와 동일 값
    #[serde(default)]
    pub ath_price_quote: String,
    /// Holder count (used for tokne total holder count)
    pub holder_count: i64,
    /// Last stats update timestamp (holder_count, total_supply) - not serialized
    #[serde(skip_serializing, default)]
    pub last_stats_update: i64,
    /// Fee 설정 정보 (V2 전용, V1은 null)
    pub fee_info: Option<FeeInfo>,
}

/// Swap event type enum
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "VARCHAR")]
#[sqlx(rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SwapType {
    Buy,
    Sell,
}

/// Swap transaction information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapInfo {
    pub event_type: SwapType,
    pub native_amount: String,
    /// Quote amount (V2 schema alias; V1에서는 native_amount와 동일 값)
    #[serde(default)]
    pub quote_amount: String,
    pub token_amount: String,
    pub native_price: String,
    /// Quote token의 USD 가격 (현재는 native_price와 동일, 향후 다른 quote token 지원 시 분리)
    pub quote_price: String,
    pub value: String,
    pub transaction_hash: String,
    pub created_at: i64,
}

#[cfg(test)]
mod market_type_tests {
    use super::MarketType;

    // giwa: market_type wire 값은 CURVE/DEX 두 개뿐 (V2 prefix 제거).
    // observer giwa 브랜치가 DB market.market_type에 쓰는 값과 일치해야 한다.
    #[test]
    fn market_type_serializes_to_curve_and_dex_only() {
        assert_eq!(
            serde_json::to_string(&MarketType::Curve).unwrap(),
            "\"CURVE\""
        );
        assert_eq!(serde_json::to_string(&MarketType::Dex).unwrap(), "\"DEX\"");
    }

    #[test]
    fn market_type_rejects_v2_wire_values() {
        assert_eq!(
            serde_json::from_str::<MarketType>("\"CURVE\"").unwrap(),
            MarketType::Curve
        );
        assert_eq!(
            serde_json::from_str::<MarketType>("\"DEX\"").unwrap(),
            MarketType::Dex
        );
        // V2 값은 giwa에서 더 이상 유효한 wire 값이 아니다
        assert!(serde_json::from_str::<MarketType>("\"V2_CURVE\"").is_err());
        assert!(serde_json::from_str::<MarketType>("\"V2_DEX\"").is_err());
    }
}
