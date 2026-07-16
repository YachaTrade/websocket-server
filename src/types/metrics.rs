use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
// use utoipa::ToSchema; // TODO: utoipa가 필요하면 Cargo.toml에 추가

/// TimeFrame enum for metrics calculation
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TimeFrame {
    #[serde(rename = "30")]
    ThirtyMinutes,
    #[serde(rename = "60")]
    OneHour,
    #[serde(rename = "240")]
    FourHours,
    #[serde(rename = "1D")]
    OneDay,
}

impl TimeFrame {
    /// TimeFrame을 초 단위로 변환
    pub fn to_seconds(&self) -> i64 {
        match self {
            TimeFrame::ThirtyMinutes => 1800,
            TimeFrame::OneHour => 3600,
            TimeFrame::FourHours => 14400,
            TimeFrame::OneDay => 86400,
        }
    }

    /// TimeFrame을 문자열로 변환
    pub fn to_string(&self) -> &'static str {
        match self {
            TimeFrame::ThirtyMinutes => "30",
            TimeFrame::OneHour => "60",
            TimeFrame::FourHours => "240",
            TimeFrame::OneDay => "1D",
        }
    }

    /// 표시용 문자열로 변환
    pub fn to_display_string(&self) -> &'static str {
        match self {
            TimeFrame::ThirtyMinutes => "30m",
            TimeFrame::OneHour => "60m",
            TimeFrame::FourHours => "240m",
            TimeFrame::OneDay => "1d",
        }
    }

    /// 차트 인터벌로 변환
    pub fn to_chart_interval(&self) -> &'static str {
        match self {
            TimeFrame::ThirtyMinutes => "30",
            TimeFrame::OneHour => "60",
            TimeFrame::FourHours => "240",
            TimeFrame::OneDay => "60", // Use 60min interval for 24h metrics
        }
    }
}

/// Swap snapshot for metrics aggregation (Redis Sorted Set member)
/// value = price * native_amount (스왑의 실제 가치)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapSnapshot {
    pub timestamp: i64,
    pub is_buy: bool,
    pub value: BigDecimal,
    pub account_id: String,
}

/// Price snapshot for metrics aggregation (Redis Sorted Set member)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceSnapshot {
    pub timestamp: i64,
    pub block_number: i64,
    pub price: BigDecimal,
    pub tx_index: i32,
    pub log_index: i64,
}

/// Transaction count by type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionCount {
    pub buy: i64,
    pub sell: i64,
    pub total: i64,
}

/// Volume amount by type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeAmount {
    pub buy: String,
    pub sell: String,
    pub total: String,
}

/// Maker count by type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerCount {
    pub buy: i64,
    pub sell: i64,
    pub total: i64,
}

/// Single metric item for a specific timeframe
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricItem {
    pub timeframe: String,
    pub percent: f64,
    pub transactions: TransactionCount,
    pub volume: VolumeAmount,
    pub makers: MakerCount,
}

/// Batch response for multiple timeframes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsBatchResponse {
    pub metrics: Vec<MetricItem>,
}

/// Metrics message for WebSocket subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenMetricsMessage {
    #[serde(skip_serializing)]
    pub token_id: String,
    pub metrics: Vec<MetricItem>,
}
