# Utils 모듈 테스트 케이스

## 테스트 목적
Utils 모듈의 유틸리티 함수들이 올바르게 동작하는지 검증합니다.

## 테스트 시나리오

### convert_chart_timestamp 함수 테스트

#### 1. `test_convert_chart_timestamp_1m`
- **목적**: 1분 단위 타임스탬프 변환 검증
- **시나리오**: 12:34:56 → 12:34:00
- **예상 결과**: 초 단위만 0으로 변환

#### 2. `test_convert_chart_timestamp_5m`
- **목적**: 5분 단위 타임스탬프 변환 검증
- **시나리오**: 12:34:56 → 12:30:00
- **예상 결과**: 5분 단위로 내림

#### 3. `test_convert_chart_timestamp_15m`
- **목적**: 15분 단위 타임스탬프 변환 검증
- **시나리오**: 12:34:56 → 12:30:00
- **예상 결과**: 15분 단위로 내림

#### 4. `test_convert_chart_timestamp_30m`
- **목적**: 30분 단위 타임스탬프 변환 검증
- **시나리오**: 12:34:56 → 12:30:00
- **예상 결과**: 30분 단위로 내림

#### 5. `test_convert_chart_timestamp_1h`
- **목적**: 1시간 단위 타임스탬프 변환 검증
- **시나리오**: 12:34:56 → 12:00:00
- **예상 결과**: 시간 단위로 내림

#### 6. `test_convert_chart_timestamp_4h`
- **목적**: 4시간 단위 타임스탬프 변환 검증
- **시나리오**: 14:34:56 → 12:00:00
- **예상 결과**: 4시간 단위로 내림

#### 7. `test_convert_chart_timestamp_1d`
- **목적**: 1일 단위 타임스탬프 변환 검증
- **시나리오**: 14:34:56 → 00:00:00
- **예상 결과**: 일 단위로 내림

#### 8. `test_convert_chart_timestamp_1w`
- **목적**: 1주 단위 타임스탬프 변환 검증
- **시나리오**: 수요일 → 월요일 00:00:00
- **예상 결과**: 주의 시작(월요일)으로 내림

#### 9. `test_convert_chart_timestamp_invalid_interval`
- **목적**: 잘못된 interval 처리 검증
- **시나리오**: "invalid" interval 입력
- **예상 결과**: 기본값(1분 단위) 적용

#### 10. `test_convert_chart_timestamp_edge_cases`
- **목적**: 경계값 처리 검증
- **시나리오**: 정확한 분/시간 경계 입력
- **예상 결과**: 모든 interval에서 동일한 값 유지

### to_big_decimal 함수 테스트

#### 11. `test_to_big_decimal_from_string`
- **목적**: 문자열에서 BigDecimal 변환 검증
- **시나리오**: "123.456" → BigDecimal
- **예상 결과**: 정확한 변환

#### 12. `test_to_big_decimal_from_integer`
- **목적**: 정수에서 BigDecimal 변환 검증
- **시나리오**: 12345 → BigDecimal
- **예상 결과**: 정확한 변환

#### 13. `test_to_big_decimal_from_float`
- **목적**: 부동소수점에서 BigDecimal 변환 검증
- **시나리오**: 123.456f64 → BigDecimal
- **예상 결과**: 정확한 변환

#### 14. `test_to_big_decimal_from_u128`
- **목적**: u128 최대값 변환 검증
- **시나리오**: u128::MAX → BigDecimal
- **예상 결과**: 정확한 변환

#### 15. `test_to_big_decimal_scientific_notation`
- **목적**: 과학적 표기법 변환 검증
- **시나리오**: "1.23e10" → BigDecimal
- **예상 결과**: 정확한 변환

#### 16. `test_to_big_decimal_invalid_string`
- **목적**: 잘못된 문자열 처리 검증
- **시나리오**: "not_a_number" → BigDecimal
- **예상 결과**: 기본값(0) 반환

#### 17. `test_to_big_decimal_zero`
- **목적**: 0 변환 검증
- **시나리오**: 0 → BigDecimal
- **예상 결과**: BigDecimal 0

#### 18. `test_to_big_decimal_negative`
- **목적**: 음수 변환 검증
- **시나리오**: -12345 → BigDecimal
- **예상 결과**: 음수 BigDecimal

#### 19. `test_to_big_decimal_very_large_number`
- **목적**: 매우 큰 수 변환 검증
- **시나리오**: 48자리 숫자 → BigDecimal
- **예상 결과**: 정확한 변환

#### 20. `test_to_big_decimal_precision`
- **목적**: 고정밀도 소수 변환 검증
- **시나리오**: "0.00000000000000000001" → BigDecimal
- **예상 결과**: 정밀도 유지

#### 21. `test_chart_timestamp_different_timezones`
- **목적**: 다양한 시간대 경계값 처리 검증
- **시나리오**: 여러 시간대의 00:00:00 타임스탬프
- **예상 결과**: 정확한 시간 경계에서는 변환 없음

## 엣지 케이스

### 1. 타임스탬프 변환
- Unix epoch (0)
- 미래의 타임스탬프
- 음수 타임스탬프 (1970년 이전)
- 정확한 interval 경계값

### 2. BigDecimal 변환
- 오버플로우 가능성이 있는 매우 큰 수
- 언더플로우 가능성이 있는 매우 작은 수
- 특수 문자가 포함된 문자열
- 빈 문자열

### 3. 주간 변환 특수 케이스
- 월요일이 아닌 다른 요일
- 연도 경계에서의 주 변환
- 윤년 처리

## 테스트 실행 방법

```bash
# utils 테스트만 실행
cargo test utils::test

# 특정 함수 테스트만 실행
cargo test test_convert_chart_timestamp

# 상세 출력과 함께 실행
cargo test utils::test -- --nocapture
```

## 주의사항
- 타임스탬프는 UTC 기준으로 처리됩니다
- BigDecimal 변환 시 잘못된 입력은 기본값(0)을 반환합니다
- 주간 변환은 월요일을 주의 시작으로 간주합니다
- 모든 시간 변환은 내림(floor) 방식을 사용합니다