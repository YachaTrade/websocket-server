use crate::utils::{convert_chart_timestamp, meets_order_latest_trade_min_amount, to_big_decimal};
use sqlx::types::BigDecimal;
use std::str::FromStr;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_convert_chart_timestamp_1m() {
        // 2024-01-01 12:54:56 UTC
        let timestamp = 1704113696;

        // 1분 단위는 그대로 유지 (초만 0으로)
        let result = convert_chart_timestamp(timestamp, "1m");
        let expected = 1704113640; // 2024-01-01 12:54:00

        assert_eq!(result, expected, "1분 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_5m() {
        // 2024-01-01 12:54:56 UTC
        let timestamp = 1704113696;

        // 5분 단위로 내림 (12:50:00)
        let result = convert_chart_timestamp(timestamp, "5m");
        let expected = 1704113400; // 2024-01-01 12:50:00

        assert_eq!(result, expected, "5분 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_15m() {
        // 2024-01-01 12:54:56 UTC
        let timestamp = 1704113696;

        // 15분 단위로 내림 (12:45:00)
        let result = convert_chart_timestamp(timestamp, "15m");
        let expected = 1704113100; // 2024-01-01 12:45:00

        assert_eq!(result, expected, "15분 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_30m() {
        // 2024-01-01 12:54:56 UTC
        let timestamp = 1704113696;

        // 30분 단위로 내림 (12:30:00)
        let result = convert_chart_timestamp(timestamp, "30m");
        let expected = 1704112200; // 2024-01-01 12:30:00

        assert_eq!(result, expected, "30분 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_1h() {
        // 2024-01-01 12:54:56 UTC
        let timestamp = 1704113696;

        // 1시간 단위로 내림 (12:00:00)
        let result = convert_chart_timestamp(timestamp, "1h");
        let expected = 1704110400; // 2024-01-01 12:00:00

        assert_eq!(result, expected, "1시간 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_1_m() {
        // 2024-01-15 14:34:56 UTC
        let timestamp = 1705330496;

        // 1개월 단위로 월 시작 시각으로 내림 (2024-01-01 00:00:00)
        let result = convert_chart_timestamp(timestamp, "1M");
        let expected = 1704067200; // 2024-01-01 00:00:00

        assert_eq!(result, expected, "1개월 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_1d() {
        // 2024-01-01 14:34:56 UTC
        let timestamp = 1704120896;

        // 1일 단위로 내림 (00:00:00)
        let result = convert_chart_timestamp(timestamp, "1d");
        let expected = 1704067200; // 2024-01-01 00:00:00

        assert_eq!(result, expected, "1일 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_1w() {
        // 2024-01-03 14:34:56 UTC (수요일)
        let timestamp = 1704293696;

        // 1주 단위로 월요일 00:00:00으로 내림
        let result = convert_chart_timestamp(timestamp, "1w");
        let expected = 1704067200; // 2024-01-01 00:00:00 (월요일)

        assert_eq!(result, expected, "1주 단위 변환 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_invalid_interval() {
        // 잘못된 interval은 1분 단위로 처리
        let timestamp = 1704113696;

        let result = convert_chart_timestamp(timestamp, "invalid");
        let expected = 1704113640; // 1분 단위와 동일

        assert_eq!(result, expected, "잘못된 interval 처리 실패");
    }

    #[test]
    fn test_convert_chart_timestamp_edge_cases() {
        // 정확히 분 단위 경계인 경우
        let timestamp = 1704110400; // 2024-01-01 12:00:00

        // 모든 interval에서 그대로 유지되어야 함
        assert_eq!(convert_chart_timestamp(timestamp, "1m"), 1704110400);
        assert_eq!(convert_chart_timestamp(timestamp, "5m"), 1704110400);
        assert_eq!(convert_chart_timestamp(timestamp, "15m"), 1704110400);
        assert_eq!(convert_chart_timestamp(timestamp, "30m"), 1704110400);
        assert_eq!(convert_chart_timestamp(timestamp, "1h"), 1704110400);

        // 월 시작 시간 테스트
        let month_start = 1704067200; // 2024-01-01 00:00:00
        assert_eq!(convert_chart_timestamp(month_start, "1M"), 1704067200);
    }

    #[test]
    fn test_to_big_decimal_from_string() {
        // 문자열에서 BigDecimal 변환
        let value = "123.456";
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("123.456").unwrap();

        assert_eq!(result, expected, "문자열 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_from_integer() {
        // 정수에서 BigDecimal 변환
        let value = 12345;
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("12345").unwrap();

        assert_eq!(result, expected, "정수 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_from_float() {
        // 부동소수점에서 BigDecimal 변환
        let value = 123.456f64;
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("123.456").unwrap();

        assert_eq!(result, expected, "부동소수점 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_from_u128() {
        // u128에서 BigDecimal 변환
        let value: u128 = 340282366920938463463374607431768211455; // u128::MAX
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("340282366920938463463374607431768211455").unwrap();

        assert_eq!(result, expected, "u128 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_scientific_notation() {
        // 과학적 표기법 문자열
        let value = "1.23e10";
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("1.23e10").unwrap();

        assert_eq!(result, expected, "과학적 표기법 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_invalid_string() {
        // 잘못된 문자열은 기본값(0) 반환
        let value = "not_a_number";
        let result = to_big_decimal(value);
        let expected = BigDecimal::default(); // 0

        assert_eq!(result, expected, "잘못된 문자열 처리 실패");
    }

    #[test]
    fn test_to_big_decimal_zero() {
        // 0 변환
        let value = 0;
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("0").unwrap();

        assert_eq!(result, expected, "0 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_negative() {
        // 음수 변환
        let value = -12345;
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str("-12345").unwrap();

        assert_eq!(result, expected, "음수 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_very_large_number() {
        // 매우 큰 수 변환
        let value = "999999999999999999999999999999999999999999999999";
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str(value).unwrap();

        assert_eq!(result, expected, "큰 수 변환 실패");
    }

    #[test]
    fn test_to_big_decimal_precision() {
        // 고정밀도 소수
        let value = "0.00000000000000000001";
        let result = to_big_decimal(value);
        let expected = BigDecimal::from_str(value).unwrap();

        assert_eq!(result, expected, "고정밀도 소수 변환 실패");
    }

    #[test]
    fn test_chart_timestamp_different_timezones() {
        // 다양한 시간대 테스트
        let timestamps = vec![
            0,          // 1970-01-01 00:00:00
            86400,      // 1970-01-02 00:00:00
            31536000,   // 1971-01-01 00:00:00
            1704067200, // 2024-01-01 00:00:00
        ];

        for &ts in &timestamps {
            let result_1m = convert_chart_timestamp(ts, "1m");
            let result_1h = convert_chart_timestamp(ts, "1h");
            let result_1d = convert_chart_timestamp(ts, "1d");

            // 정확한 시간 경계에서는 모든 결과가 동일해야 함
            assert_eq!(result_1m, ts);
            assert_eq!(result_1h, ts);
            assert_eq!(result_1d, ts);
        }
    }

    // order_latest_trade 최소 금액 필터: quote 토큰 1개의 10%(0.1) = 0.1 × 10^quote_decimals raw 이상일 때만 push.
    // 임계값을 quote_decimals로 스케일하여 non-18-decimal quote 마켓도 올바르게 동작해야 한다.
    #[test]
    fn meets_min_amount_18_decimals_below_threshold() {
        // 18 decimals → 임계값 1e17. 1e17 - 1 은 push 안 함
        let amount = BigDecimal::from_str("99999999999999999").unwrap();
        assert!(
            !meets_order_latest_trade_min_amount(&amount, 18),
            "18 decimals 임계값 미만은 push 대상이 아니어야 함"
        );
    }

    #[test]
    fn meets_min_amount_18_decimals_at_threshold() {
        // 정확히 1e17 (0.1 토큰) → 경계 포함(이상)이므로 push
        let amount = BigDecimal::from_str("100000000000000000").unwrap();
        assert!(
            meets_order_latest_trade_min_amount(&amount, 18),
            "18 decimals 임계값과 같으면 push 대상이어야 함"
        );
    }

    #[test]
    fn meets_min_amount_18_decimals_above_threshold() {
        // 1e18 (1 토큰) → push
        let amount = BigDecimal::from_str("1000000000000000000").unwrap();
        assert!(
            meets_order_latest_trade_min_amount(&amount, 18),
            "18 decimals 임계값 초과는 push 대상이어야 함"
        );
    }

    #[test]
    fn meets_min_amount_returns_false_for_zero() {
        let amount = BigDecimal::from_str("0").unwrap();
        assert!(
            !meets_order_latest_trade_min_amount(&amount, 18),
            "0 금액은 push 대상이 아니어야 함"
        );
    }

    #[test]
    fn meets_min_amount_6_decimals_scales_threshold() {
        // 6 decimals quote → 임계값 = 0.1 × 10^6 = 100000 (0.1 토큰)
        // 1 토큰(1_000_000) → push
        let one_token = BigDecimal::from_str("1000000").unwrap();
        assert!(
            meets_order_latest_trade_min_amount(&one_token, 6),
            "6 decimals 1 토큰은 push 대상이어야 함"
        );
        // 정확히 0.1 토큰(100000) → 경계 포함이므로 push
        let at_threshold = BigDecimal::from_str("100000").unwrap();
        assert!(
            meets_order_latest_trade_min_amount(&at_threshold, 6),
            "6 decimals 0.1 토큰(경계)은 push 대상이어야 함"
        );
        // 0.1 토큰 미만(99999) → push 안 함. (하드코딩 1e17 이었다면 잘못 차단되던 케이스)
        let below = BigDecimal::from_str("99999").unwrap();
        assert!(
            !meets_order_latest_trade_min_amount(&below, 6),
            "6 decimals 0.1 토큰 미만은 push 대상이 아니어야 함"
        );
    }
}
