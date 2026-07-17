use std::str::FromStr;

use chrono::{Datelike, Utc};
use sqlx::types::BigDecimal;

#[cfg(test)]
mod test;

pub mod retry;

/// 거래 quote 측 금액이 order_latest_trade(LatestTrade) push 최소 금액 이상인지 판단한다.
///
/// 최소 금액 = quote 토큰 1개의 10%(0.1) = `0.1 × 10^quote_decimals` (raw 기준).
/// 예) 18 decimals → 1e17, 6 decimals → 1e5. 임계값을 quote_decimals로 스케일하므로
/// WETH(18)이 아닌 quote 토큰 마켓에서도 정상 거래가 잘못 차단되지 않는다.
/// buy 는 amount_in, sell 은 amount_out 이 quote 측 금액이다.
pub fn meets_order_latest_trade_min_amount(amount: &BigDecimal, quote_decimals: i32) -> bool {
    // 0.1 × 10^decimals == 10^(decimals - 1)
    let min_amount = BigDecimal::from_str(&format!("1e{}", quote_decimals - 1))
        .expect("power-of-ten min amount is always a valid BigDecimal");
    amount >= &min_amount
}

pub fn convert_chart_timestamp(timestamp: i64, interval: &str) -> i64 {
    let total_minutes = timestamp / 60;

    let rounded_minutes = match interval {
        "1m" => total_minutes,
        "5m" => (total_minutes / 5) * 5,
        "15m" => (total_minutes / 15) * 15,
        "30m" => (total_minutes / 30) * 30,
        "1h" => (total_minutes / 60) * 60,
        "1d" => (total_minutes / 1440) * 1440,
        "1w" => {
            let dt = chrono::DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap();
            let weekday = dt.weekday().num_days_from_monday();
            let monday = dt.date_naive() - chrono::Duration::days(weekday as i64);
            let monday_start = monday.and_hms_opt(0, 0, 0).unwrap();
            monday_start.and_utc().timestamp() / 60
        }
        "1M" => {
            let dt = chrono::DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap();
            let month_start = dt.date_naive().with_day(1).unwrap();
            let month_start_time = month_start.and_hms_opt(0, 0, 0).unwrap();
            month_start_time.and_utc().timestamp() / 60
        }
        _ => total_minutes, // 기본적으로 1분 단위로 처리
    };

    rounded_minutes * 60
}

pub fn to_big_decimal<T: ToString>(value: T) -> BigDecimal {
    BigDecimal::from_str(&value.to_string()).unwrap_or_default()
}

/// 현재 Unix 타임스탬프 (초 단위)를 반환합니다.
pub fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// 시작 가격과 현재 가격을 비교하여 변화율(%)을 계산합니다.
/// 소수점 3자리까지 반올림합니다.
pub fn calculate_price_change_percent(start_price: &str, current_price: &str) -> Option<f64> {
    let start: f64 = start_price.parse().ok()?;
    let current: f64 = current_price.parse().ok()?;

    if (start - 0.0).abs() < f64::EPSILON {
        return None;
    }

    let change_percent = ((current - start) / start) * 100.0;
    // Round to 3 decimal places
    Some((change_percent * 1000.0).round() / 1000.0)
}

/// BigDecimal을 사용한 정밀한 percent 계산
/// String 변환 없이 BigDecimal로 직접 계산하여 정밀도 유지
pub fn calculate_price_change_percent_precise(
    start_price: &bigdecimal::BigDecimal,
    current_price: &bigdecimal::BigDecimal,
) -> Option<f64> {
    use bigdecimal::Zero;

    if start_price.is_zero() {
        return None;
    }

    // BigDecimal로 계산: ((current - start) / start) * 100
    let diff = current_price - start_price;
    let percent_decimal = (&diff / start_price) * bigdecimal::BigDecimal::from(100);

    // BigDecimal을 f64로 변환하고 소수점 3자리로 반올림
    let percent_f64 = percent_decimal.to_string().parse::<f64>().ok()?;
    Some((percent_f64 * 1000.0).round() / 1000.0)
}
