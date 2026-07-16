use anyhow::Result;
use bigdecimal::{BigDecimal, RoundingMode};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;

// BigDecimal을 소수점 10자리로 제한하여 직렬화
fn serialize_price_10_decimal<S>(value: &BigDecimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let rounded = value.with_scale_round(10, RoundingMode::Up);
    serializer.serialize_str(&rounded.normalized().to_plain_string())
}

// BigDecimal을 plain string으로 직렬화 (소수점 제한 없음)
fn serialize_plain_string<S>(value: &BigDecimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.normalized().to_plain_string())
}

/// 차트 업데이트 파라미터 (too_many_arguments 경고 해결용)
#[derive(Debug, Clone)]
pub struct ChartUpdateParams<'a> {
    /// 토큰 ID
    pub token_id: &'a str,
    /// 차트 인터벌 (예: "1", "5", "1H")
    pub interval: &'a str,
    /// 캔들 시작 타임스탬프
    pub timestamp: i64,
    /// MON/TOKEN 가격
    pub price: &'a BigDecimal,
    /// 거래량 (MON)
    pub volume: Option<&'a BigDecimal>,
    /// MON/USD 가격 (USD 계산용)
    pub native_price: &'a BigDecimal,
    /// 토큰 총 공급량
    pub total_supply: &'a BigDecimal,
    /// 체인 이벤트 순서. 같은 캔들 안에서 비동기 처리 순서가 뒤집혀도
    /// open/close를 실제 이벤트 순서 기준으로 유지하는 데 사용합니다.
    pub order: Option<ChartUpdateOrder>,
}

/// 차트 업데이트 컨텍스트 (interval 제외한 공통 파라미터)
#[derive(Debug, Clone)]
pub struct ChartUpdateContext<'a> {
    /// 토큰 ID
    pub token_id: &'a str,
    /// 타임스탬프
    pub timestamp: i64,
    /// MON/TOKEN 가격
    pub price: &'a BigDecimal,
    /// 거래량 (MON)
    pub volume: Option<&'a BigDecimal>,
    /// MON/USD 가격 (USD 계산용)
    pub native_price: &'a BigDecimal,
    /// 토큰 총 공급량
    pub total_supply: &'a BigDecimal,
    /// 체인 이벤트 순서
    pub order: Option<ChartUpdateOrder>,
}

impl<'a> ChartUpdateContext<'a> {
    /// ChartUpdateParams로 변환 (interval 추가)
    pub fn to_params(&self, interval: &'a str, candle_start: i64) -> ChartUpdateParams<'a> {
        ChartUpdateParams {
            token_id: self.token_id,
            interval,
            timestamp: candle_start,
            price: self.price,
            volume: self.volume,
            native_price: self.native_price,
            total_supply: self.total_supply,
            order: self.order,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct ChartUpdateOrder {
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
}

/// 차트 가격 타입
/// 1. Price: MON/TOKEN 가격 (기본)
/// 2. PriceUsd: TOKEN의 USD 가격 (Price * latest_price)
/// 3. MarketCap: 시가총액 (MON) (Price * total_supply)
/// 4. MarketCapUsd: 시가총액 (USD) (Price * total_supply * latest_price)
#[derive(Debug, Clone, Copy, Default, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum PriceType {
    #[serde(rename = "price")]
    #[default]
    Price,
    #[serde(rename = "price_usd")]
    PriceUsd,
    #[serde(rename = "market_cap")]
    MarketCap,
    #[serde(rename = "market_cap_usd")]
    MarketCapUsd,
}

impl PriceType {
    /// price type을 문자열로 반환 (&str로 반환하여 불필요한 할당 제거)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Price => "price",
            Self::PriceUsd => "price_usd",
            Self::MarketCap => "market_cap",
            Self::MarketCapUsd => "market_cap_usd",
        }
    }
}

impl fmt::Display for PriceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for PriceType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "price" => Ok(Self::Price),
            "price_usd" => Ok(Self::PriceUsd),
            "market_cap" => Ok(Self::MarketCap),
            "market_cap_usd" => Ok(Self::MarketCapUsd),
            _ => Err(anyhow::anyhow!("Invalid price type: {}", value)),
        }
    }
}

impl TryFrom<String> for PriceType {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::try_from(value.as_str())
    }
}

/// Chart 상태 상수
pub const CHART_STATUS_OK: &str = "ok";

/// 차트 응답용 구조체 (WebSocket으로 내려줄 때 사용)
/// price_type에 따라 o, h, l, c, v에 적절한 값이 들어감
/// - Price: MON/TOKEN 가격
/// - PriceUsd: USD 가격
/// - MarketCap: MON 기준 시가총액
/// - MarketCapUsd: USD 기준 시가총액
///
/// token_id / interval / k(price_type) 는 라우팅 식별자.
/// 한 소켓에 여러 chart 구독이 떠있을 때 client가 응답이 어떤 구독에 속하는지
/// 판별할 수 있도록 페이로드에 동봉한다. 서버에서도 receive-side에서 mismatch
/// 검증용 (regression 방어).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartResponse {
    pub token_id: String,
    pub interval: String,
    pub k: String,
    pub s: String,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub o: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub h: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub l: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub c: BigDecimal,
    #[serde(serialize_with = "serialize_plain_string")]
    pub v: BigDecimal,
    pub t: i64,
}

/// 차트 캔들 데이터 (내부 저장용)
/// MON/TOKEN 가격, USD 가격, total_supply를 모두 저장하여 모든 price_type 계산 지원
/// - Price: o, h, l, c, v 사용
/// - PriceUsd: usd_o, usd_h, usd_l, usd_c, usd_v 사용
/// - MarketCap: o * total_supply, ...
/// - MarketCapUsd: usd_o * total_supply, ...
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Chart {
    pub s: String,
    /// MON/TOKEN OHLCV
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub o: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub c: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub h: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub l: BigDecimal,
    #[serde(serialize_with = "serialize_plain_string")]
    pub v: BigDecimal,
    pub t: i64,
    /// USD OHLCV (해당 시점의 native_price로 계산된 정확한 USD 가격)
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub usd_o: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub usd_c: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub usd_h: BigDecimal,
    #[serde(serialize_with = "serialize_price_10_decimal")]
    pub usd_l: BigDecimal,
    #[serde(serialize_with = "serialize_plain_string")]
    pub usd_v: BigDecimal,
    /// 토큰 총 공급량 (Market Cap 계산용)
    #[serde(serialize_with = "serialize_plain_string")]
    pub total_supply: BigDecimal,
}

impl Chart {
    /// price_type에 따라 ChartResponse로 변환
    /// - Price: MON/TOKEN 가격
    /// - PriceUsd: USD 가격
    /// - MarketCap: MON 기준 시가총액 (price * total_supply)
    /// - MarketCapUsd: USD 기준 시가총액 (usd_price * total_supply)
    ///
    /// token_id / interval 은 cross-token 라우팅 식별자로 페이로드에 동봉된다.
    pub fn to_response(
        &self,
        token_id: &str,
        interval: &str,
        price_type: PriceType,
    ) -> ChartResponse {
        // k 필드에 price_type 문자열 저장
        let k = price_type.as_str().to_string();
        let token_id = token_id.to_string();
        let interval = interval.to_string();

        match price_type {
            PriceType::Price => ChartResponse {
                token_id,
                interval,
                k,
                s: CHART_STATUS_OK.to_string(),
                o: self.o.clone(),
                h: self.h.clone(),
                l: self.l.clone(),
                c: self.c.clone(),
                v: self.v.clone(),
                t: self.t,
            },
            PriceType::PriceUsd => ChartResponse {
                token_id,
                interval,
                k,
                s: CHART_STATUS_OK.to_string(),
                o: self.usd_o.clone(),
                h: self.usd_h.clone(),
                l: self.usd_l.clone(),
                c: self.usd_c.clone(),
                v: self.usd_v.clone(),
                t: self.t,
            },
            PriceType::MarketCap => {
                let ts = &self.total_supply;
                ChartResponse {
                    token_id,
                    interval,
                    k,
                    s: CHART_STATUS_OK.to_string(),
                    o: &self.o * ts,
                    h: &self.h * ts,
                    l: &self.l * ts,
                    c: &self.c * ts,
                    v: self.v.clone(),
                    t: self.t,
                }
            }
            PriceType::MarketCapUsd => {
                let ts = &self.total_supply;
                ChartResponse {
                    token_id,
                    interval,
                    k,
                    s: CHART_STATUS_OK.to_string(),
                    o: &self.usd_o * ts,
                    h: &self.usd_h * ts,
                    l: &self.usd_l * ts,
                    c: &self.usd_c * ts,
                    v: self.usd_v.clone(),
                    t: self.t,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChartInterval {
    Minute1,
    Minute5,
    Minute15,
    Minute30,
    Hour1,
    Hour4,
    Day1,
    Week1,
    Month1,
}

use chrono::Datelike;

impl ChartInterval {
    pub fn to_seconds(&self) -> i64 {
        match self {
            Self::Minute1 => 60,
            Self::Minute5 => 300,
            Self::Minute15 => 900,
            Self::Minute30 => 1800,
            Self::Hour1 => 3600,
            Self::Hour4 => 14400,
            Self::Day1 => 86400,
            Self::Week1 => 604800,
            Self::Month1 => 2592000, // 30일 기준
        }
    }

    pub fn get_candle_start(&self, timestamp: i64) -> i64 {
        match self {
            Self::Week1 => {
                // 주간 캔들은 월요일 00:00:00 UTC로 정규화
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp, 0)
                    .unwrap_or_else(chrono::Utc::now);
                let weekday = dt.weekday().num_days_from_monday();
                let monday = dt.date_naive() - chrono::Duration::days(weekday as i64);
                monday.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp()
            }
            Self::Month1 => {
                // 월간 캔들은 해당 월 1일 00:00:00 UTC로 정규화
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp, 0)
                    .unwrap_or_else(chrono::Utc::now);
                let month_start = dt.date_naive().with_day(1).unwrap();
                month_start
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp()
            }
            _ => {
                // 나머지는 interval로 나누기
                let interval = self.to_seconds();
                (timestamp / interval) * interval
            }
        }
    }

    pub fn previous_candle_start(&self, current_start: i64) -> i64 {
        match self {
            Self::Month1 => {
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(current_start, 0)
                    .unwrap_or_else(chrono::Utc::now);
                let date = dt.date_naive();
                let (year, month) = if date.month() == 1 {
                    (date.year() - 1, 12)
                } else {
                    (date.year(), date.month() - 1)
                };
                chrono::NaiveDate::from_ymd_opt(year, month, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp()
            }
            _ => current_start - self.to_seconds(),
        }
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::Minute1,
            Self::Minute5,
            Self::Minute15,
            Self::Minute30,
            Self::Hour1,
            Self::Hour4,
            Self::Day1,
            Self::Week1,
            Self::Month1,
        ]
    }

    /// interval을 문자열로 반환 (&str로 반환하여 불필요한 할당 제거)
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Minute1 => "1",
            Self::Minute5 => "5",
            Self::Minute15 => "15",
            Self::Minute30 => "30",
            Self::Hour1 => "1H",
            Self::Hour4 => "4H",
            Self::Day1 => "D",
            Self::Week1 => "W",
            Self::Month1 => "M",
        }
    }
}

impl fmt::Display for ChartInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ChartInterval {
    pub fn get_cache_expiration(&self) -> u64 {
        match self {
            Self::Minute1 => 120 * 1000,    // 2분
            Self::Minute5 => 300 * 1000,    // 5분
            Self::Minute15 => 900 * 1000,   // 15분
            Self::Minute30 => 1800 * 1000,  // 30분
            Self::Hour1 => 3600 * 1000,     // 1시간
            Self::Hour4 => 14400 * 1000,    // 4시간
            Self::Day1 => 86400 * 1000,     // 1일
            Self::Week1 => 604800 * 1000,   // 7일
            Self::Month1 => 2592000 * 1000, // 30일
        }
    }
}

impl TryFrom<&str> for ChartInterval {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "1" => Ok(Self::Minute1),
            "5" => Ok(Self::Minute5),
            "15" => Ok(Self::Minute15),
            "30" => Ok(Self::Minute30),
            "1H" => Ok(Self::Hour1),
            "4H" => Ok(Self::Hour4),
            "D" => Ok(Self::Day1),
            "W" => Ok(Self::Week1),
            "M" => Ok(Self::Month1),
            _ => Err(anyhow::anyhow!("Invalid interval type: {}", value)),
        }
    }
}

impl TryFrom<String> for ChartInterval {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::try_from(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_candle_starts_at_calendar_month_start() {
        // 2024-01-15 14:34:56 UTC
        let timestamp = 1705330496;

        assert_eq!(
            ChartInterval::Month1.get_candle_start(timestamp),
            1704067200
        );
    }

    #[test]
    fn previous_month_candle_uses_calendar_month_start() {
        // 2024-03-01 00:00:00 UTC
        let current_start = 1709251200;

        assert_eq!(
            ChartInterval::Month1.previous_candle_start(current_start),
            1706745600
        );
    }
}
