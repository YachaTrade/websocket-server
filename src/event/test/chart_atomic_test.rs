/// Chart 원자성 테스트
///
/// 이 테스트는 동시에 여러 거래 이벤트가 발생했을 때
/// Chart 데이터(OHLCV)가 정확하게 업데이트되는지 확인합니다.
///
/// 테스트 시나리오:
/// 1. Atomic 테스트: update_chart_atomic 사용 (Lua Script)
/// 2. Non-Atomic 테스트: GET -> UPDATE -> SET 방식 (race condition 재현)
/// 3. Chart Event Flow 통합 테스트: CurveChartUpdate -> Chart 업데이트 검증
/// 4. 다중 Interval 테스트: 1m, 5m, 1H 등 여러 interval 동시 업데이트

use crate::db::cache::CacheManager;
use crate::event::chart::CHART_EVENT_PRODUCER;
use crate::types::chart::{Chart, ChartInterval};
use crate::types::stream::{CurveChartUpdate, CurveSync, CurveTrade, Buy};
use bigdecimal::BigDecimal;
use std::str::FromStr;
use tokio;

/// 테스트용 환경 초기화
async fn init_test_env() {
    // .env 파일 로드
    dotenv::dotenv().ok();

    // Redis와 PostgreSQL 초기화
    if let Err(e) = crate::db::redis::RedisDatabase::init().await {
        eprintln!("Redis init failed: {}", e);
        panic!("Redis must be running for tests. Make sure REDIS_URL is set in .env");
    }

    if let Err(e) = crate::db::postgres::PostgresDatabase::init().await {
        eprintln!("Postgres init failed: {}", e);
        panic!("PostgreSQL must be running for tests. Make sure DATABASE_URL is set in .env");
    }

    // CacheManager 초기화
    if let Err(e) = CacheManager::init().await {
        eprintln!("CacheManager init failed: {}", e);
    }
}

#[tokio::test]
async fn test_concurrent_chart_updates_atomic() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_CHART_ATOMIC_TOKEN";
    let interval = ChartInterval::Minute1;

    println!("\n🧪 Testing ATOMIC Chart Update (using Lua Script)");

    // 현재 시간을 기준으로 캔들 시작 시간 계산
    let now = chrono::Utc::now().timestamp();
    let candle_start = interval.get_candle_start(now);

    println!("✅ Candle start time: {} ({})", candle_start, interval.to_string());

    // 1. 동시에 100개의 거래 발생 (가격과 거래량 업데이트)
    let num_tasks = 100;
    let base_price = BigDecimal::from_str("0.001").unwrap();
    let volume_per_trade = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH

    let mut handles = vec![];
    for i in 0..num_tasks {
        let cm = cache_manager.clone();
        let token = test_token.to_string();
        let interval_str = interval.to_string();

        // 가격을 약간씩 변동 (0.001 ~ 0.002)
        let price_variation = BigDecimal::from(i) / BigDecimal::from(100000);
        let price = &base_price + &price_variation;
        let volume = volume_per_trade.clone();

        handles.push(tokio::spawn(async move {
            cm.update_chart_atomic(
                &token,
                &interval_str,
                candle_start,
                &price,
                Some(&volume),
            )
            .await
            .expect("Failed to update chart");

            if i % 20 == 0 {
                println!("  ✓ Task {} completed (price: {})", i, price);
            }
        }));
    }

    // 모든 태스크 완료 대기
    for h in handles {
        h.await.unwrap();
    }

    println!("✅ All {} concurrent chart updates completed", num_tasks);

    // 2. 최종 Chart 확인 (Redis에서 직접 조회 - Atomic 동작 검증)
    let final_chart = cache_manager
        .redis
        .get_chart(test_token, &interval.to_string(), candle_start)
        .await
        .expect("Failed to get chart from Redis");

    let expected_volume = &volume_per_trade * BigDecimal::from(num_tasks);

    println!("\n📊 ATOMIC Chart Update Results:");
    println!("  Symbol:         {}", final_chart.s);
    println!("  Open:           {}", final_chart.o);
    println!("  High:           {}", final_chart.h);
    println!("  Low:            {}", final_chart.l);
    println!("  Close:          {}", final_chart.c);
    println!("  Volume:         {} ETH", final_chart.v);
    println!("  Expected Vol:   {} ETH", expected_volume);
    println!("  Timestamp:      {}", final_chart.t);

    // 검증
    assert_eq!(final_chart.s, format!("{}:{}", test_token, interval.to_string()));
    assert_eq!(final_chart.t, candle_start);

    // Volume 검증 (Atomic 방식은 정확해야 함)
    let volume_diff = (&final_chart.v - &expected_volume).abs();
    assert!(
        volume_diff < BigDecimal::from_str("0.000001").unwrap(),
        "Volume should match exactly! Expected: {}, Got: {}, Diff: {}",
        expected_volume,
        final_chart.v,
        volume_diff
    );

    // OHLC 검증
    assert!(final_chart.o >= base_price, "Open should be >= base price");
    assert!(final_chart.h >= final_chart.o, "High should be >= Open");
    assert!(final_chart.l <= final_chart.c, "Low should be <= Close");
    assert!(final_chart.c >= base_price, "Close should be >= base price");

    println!("✅ ATOMIC Chart Update test PASSED\n");
}

#[tokio::test]
async fn test_concurrent_chart_updates_non_atomic() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_CHART_NON_ATOMIC_TOKEN";
    let interval = ChartInterval::Minute1;

    println!("\n🧪 Testing NON-ATOMIC Chart Update (GET -> UPDATE -> SET)");

    let now = chrono::Utc::now().timestamp();
    let candle_start = interval.get_candle_start(now);

    println!("✅ Candle start time: {} ({})", candle_start, interval.to_string());

    // 1. 초기 Chart 생성
    let initial_price = BigDecimal::from_str("0.001").unwrap();
    let native_price = BigDecimal::from_str("10").unwrap(); // 테스트용 MON/USD 가격
    let total_supply = BigDecimal::from(1_000_000_000_000_000_000_000_000_000_i128);
    let usd_price = &initial_price * &native_price;

    let initial_chart = Chart {
        s: format!("{}:{}", test_token, interval.to_string()),
        o: initial_price.clone(),
        h: initial_price.clone(),
        l: initial_price.clone(),
        c: initial_price.clone(),
        v: BigDecimal::from(0),
        t: candle_start,
        usd_o: usd_price.clone(),
        usd_h: usd_price.clone(),
        usd_l: usd_price.clone(),
        usd_c: usd_price.clone(),
        usd_v: BigDecimal::from(0),
        total_supply: total_supply.clone(),
    };

    cache_manager
        .redis
        .set_chart(test_token, &interval.to_string(), candle_start, &initial_chart)
        .await
        .expect("Failed to set initial chart");

    println!("✅ Initial chart set");

    // 2. Non-atomic 방식으로 동시 업데이트
    let num_tasks = 100;
    let base_price = BigDecimal::from_str("0.001").unwrap();
    let volume_per_trade = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH

    let mut handles = vec![];
    for i in 0..num_tasks {
        let cm = cache_manager.clone();
        let token = test_token.to_string();
        let interval_str = interval.to_string();

        let price_variation = BigDecimal::from(i) / BigDecimal::from(100000);
        let price = &base_price + &price_variation;
        let volume = volume_per_trade.clone();

        handles.push(tokio::spawn(async move {
            // NON-ATOMIC: Read -> Modify -> Write
            match cm.redis.get_chart(&token, &interval_str, candle_start).await {
                Ok(mut chart) => {
                    // Update OHLC
                    if price > chart.h {
                        chart.h = price.clone();
                    }
                    if price < chart.l {
                        chart.l = price.clone();
                    }
                    chart.c = price;

                    // Update volume
                    chart.v = chart.v + volume;

                    // 약간의 지연 추가 (race condition 발생 확률 증가)
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;

                    // Write back
                    let _ = cm
                        .redis
                        .set_chart(&token, &interval_str, candle_start, &chart)
                        .await;

                    if i % 20 == 0 {
                        println!("  ✓ Task {} completed", i);
                    }
                }
                Err(e) => {
                    eprintln!("  ✗ Task {} failed: {}", i, e);
                }
            }
        }));
    }

    // 모든 태스크 완료 대기
    for h in handles {
        h.await.unwrap();
    }

    println!("✅ All {} concurrent chart updates completed", num_tasks);

    // 3. 최종 Chart 확인
    let final_chart = cache_manager
        .redis
        .get_chart(test_token, &interval.to_string(), candle_start)
        .await
        .expect("Failed to get chart");

    let expected_volume = &volume_per_trade * BigDecimal::from(num_tasks);

    println!("\n📊 NON-ATOMIC Chart Update Results:");
    println!("  Expected Vol:   {} ETH", expected_volume);
    println!("  Actual Vol:     {} ETH", final_chart.v);
    println!("  Match:          {}", final_chart.v == expected_volume);
    println!("  Lost Vol:       {} ETH", (&expected_volume - &final_chart.v).abs());

    // Non-atomic은 race condition으로 인해 실패할 가능성이 높음
    if final_chart.v < expected_volume {
        let lost = &expected_volume - &final_chart.v;
        let lost_percent = (&lost / &expected_volume) * BigDecimal::from(100);
        println!("\n⚠️  Race condition detected!");
        println!("  Lost {} ETH ({:.2}% of total) due to concurrent writes", lost, lost_percent);
        println!("✅ This proves we NEED atomic operations for chart updates!");
    } else {
        println!("\n⚠️  By chance, race condition didn't occur this time");
        println!("  Run the test multiple times to see race conditions");
    }

    println!("✅ NON-ATOMIC Chart Update test completed\n");
}

#[tokio::test]
async fn test_chart_multi_interval_atomic() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_CHART_MULTI_INTERVAL";

    println!("\n🧪 Testing Multi-Interval Chart Update (Atomic)");

    let now = chrono::Utc::now().timestamp();
    let price = BigDecimal::from_str("0.001").unwrap();
    let volume = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH

    // 모든 interval에 대해 동시 업데이트
    let intervals = ChartInterval::all();
    let mut handles = vec![];

    for interval in intervals.clone() {
        let cm = cache_manager.clone();
        let token = test_token.to_string();
        let p = price.clone();
        let v = volume.clone();

        handles.push(tokio::spawn(async move {
            let candle_start = interval.get_candle_start(now);

            let chart = cm
                .update_chart_atomic(
                    &token,
                    &interval.to_string(),
                    candle_start,
                    &p,
                    Some(&v),
                )
                .await
                .expect("Failed to update chart");

            println!("  ✓ {} interval updated (t={})", interval.to_string(), chart.t);
            (interval, chart)
        }));
    }

    // 모든 interval 업데이트 완료 대기
    let mut results = vec![];
    for h in handles {
        let (interval, chart) = h.await.unwrap();
        results.push((interval, chart));
    }

    println!("✅ All {} intervals updated", results.len());

    // 각 interval별로 검증
    println!("\n📊 Multi-Interval Update Results:");
    for (interval, chart) in results {
        let expected_candle_start = interval.get_candle_start(now);

        println!(
            "  {} - Volume: {}, Timestamp: {}, Match: {}",
            interval.to_string(),
            chart.v,
            chart.t,
            chart.t == expected_candle_start
        );

        assert_eq!(chart.t, expected_candle_start);
        assert_eq!(chart.v, volume);
        assert_eq!(chart.o, price);
        assert_eq!(chart.c, price);
    }

    println!("✅ Multi-Interval Chart Update test PASSED\n");
}

#[tokio::test]
async fn test_chart_event_producer_integration() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_CHART_EVENT_INTEGRATION";

    println!("\n🧪 Testing Chart Event Producer Integration");

    // ChartEventProducer 초기화
    if CHART_EVENT_PRODUCER.get().is_none() {
        crate::event::chart::ChartEventProducer::init()
            .expect("Failed to init ChartEventProducer");
    }

    let producer = CHART_EVENT_PRODUCER
        .get()
        .expect("ChartEventProducer should be initialized")
        .clone();

    // 1. WebSocket 구독 생성 (1분봉)
    let chart_key = crate::event::chart::ChartKey {
        token_id: test_token.to_string(),
        interval: ChartInterval::Minute1.to_string(),
        price_type: crate::types::chart::PriceType::Price,
    };

    let mut receiver = producer
        .get_event_receiver(chart_key.clone())
        .expect("Failed to get event receiver");

    println!("✅ WebSocket receiver created for {:?}", chart_key);

    // 2. CurveChartUpdate 이벤트 생성 및 전송
    let now = chrono::Utc::now().timestamp() as u64;

    let sync = CurveSync {
        token: test_token.to_string(),
        virtual_token_amount: BigDecimal::from_str("1000000000000000000000").unwrap(), // 1000 tokens
        virtual_native_amount: BigDecimal::from_str("5000000000000000000").unwrap(),   // 5 ETH
        block_number: 12345,
        block_timestamp: now,
        tx_hash: "0xTEST_TX_HASH".to_string(),
    };

    let buy = Buy {
        user: "0xUSER".to_string(),
        token: test_token.to_string(),
        amount_in: BigDecimal::from_str("1000000000000000000").unwrap(), // 1 ETH
        amount_out: BigDecimal::from_str("200000000000000000000").unwrap(), // 200 tokens
        block_number: 12345,
        block_timestamp: now,
        tx_hash: "0xTEST_TX_HASH".to_string(),
    };

    let trade = CurveTrade::Buy(buy);

    let chart_update = CurveChartUpdate {
        sync: sync.clone(),
        trade: Some(trade),
    };

    let boxed_event: Box<dyn crate::types::stream::CurveEvent> = Box::new(chart_update);

    producer
        .curve_event_sender
        .send(boxed_event)
        .await
        .expect("Failed to send curve event");

    println!("✅ CurveChartUpdate event sent");

    // 3. Chart 메시지 수신 확인 (타임아웃 설정)
    match tokio::time::timeout(tokio::time::Duration::from_secs(3), receiver.recv()).await {
        Ok(Ok(chart)) => {
            println!("✅ Chart message received:");
            println!("  Symbol: {}", chart.s);
            println!("  OHLC: O={}, H={}, L={}, C={}", chart.o, chart.h, chart.l, chart.c);
            println!("  Volume: {}", chart.v);
            println!("  Timestamp: {}", chart.t);

            // 검증
            assert!(chart.s.contains(test_token));
            assert!(chart.v > BigDecimal::from(0));

            // 가격 계산 검증 (virtual_native / virtual_token)
            let expected_price = &sync.virtual_native_amount / &sync.virtual_token_amount;
            println!("  Expected price: {}", expected_price);

            // OHLC가 모두 같은 값이어야 함 (첫 거래)
            assert_eq!(chart.o, chart.h);
            assert_eq!(chart.o, chart.l);
            assert_eq!(chart.o, chart.c);
        }
        Ok(Err(e)) => {
            eprintln!("❌ Failed to receive chart message: {}", e);
            panic!("Chart message not received");
        }
        Err(_) => {
            eprintln!("❌ Timeout waiting for chart message");
            panic!("Chart message receive timeout");
        }
    }

    println!("✅ Chart Event Producer Integration test PASSED\n");
}

#[tokio::test]
async fn test_chart_ohlc_correctness() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_CHART_OHLC";
    let interval = ChartInterval::Minute1;

    println!("\n🧪 Testing Chart OHLC Correctness");

    let now = chrono::Utc::now().timestamp();
    let candle_start = interval.get_candle_start(now);

    println!("✅ Candle start time: {}", candle_start);

    // 1. 순차적으로 가격 변동 (0.001 -> 0.002 -> 0.0015)
    let prices = vec![
        BigDecimal::from_str("0.001").unwrap(),   // Open
        BigDecimal::from_str("0.002").unwrap(),   // High
        BigDecimal::from_str("0.0015").unwrap(),  // Close
        BigDecimal::from_str("0.0008").unwrap(),  // Low
    ];

    let volume = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH

    for (i, price) in prices.iter().enumerate() {
        cache_manager
            .update_chart_atomic(
                test_token,
                &interval.to_string(),
                candle_start,
                price,
                Some(&volume),
            )
            .await
            .expect("Failed to update chart");

        println!("  ✓ Price update {}: {}", i + 1, price);
    }

    // 2. 최종 Chart 확인
    let chart = cache_manager
        .get_chart(test_token, &interval.to_string(), candle_start)
        .await
        .expect("Failed to get chart");

    println!("\n📊 OHLC Results:");
    println!("  Open:   {}", chart.o);
    println!("  High:   {}", chart.h);
    println!("  Low:    {}", chart.l);
    println!("  Close:  {}", chart.c);
    println!("  Volume: {}", chart.v);

    // 검증
    assert_eq!(chart.o, BigDecimal::from_str("0.001").unwrap(), "Open should be first price");
    assert_eq!(chart.h, BigDecimal::from_str("0.002").unwrap(), "High should be max price");
    assert_eq!(chart.l, BigDecimal::from_str("0.0008").unwrap(), "Low should be min price");
    assert_eq!(chart.c, BigDecimal::from_str("0.0008").unwrap(), "Close should be last price");

    let expected_volume = &volume * BigDecimal::from(4); // 4 trades
    let volume_diff = (&chart.v - &expected_volume).abs();
    assert!(
        volume_diff < BigDecimal::from_str("0.000001").unwrap(),
        "Volume should be sum of all trades"
    );

    // OHLC 관계 검증
    assert!(chart.h >= chart.o, "High >= Open");
    assert!(chart.h >= chart.c, "High >= Close");
    assert!(chart.l <= chart.o, "Low <= Open");
    assert!(chart.l <= chart.c, "Low <= Close");

    println!("✅ Chart OHLC Correctness test PASSED\n");
}

#[tokio::test]
async fn test_chart_candle_boundary() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_CHART_BOUNDARY";
    let interval = ChartInterval::Minute1;

    println!("\n🧪 Testing Chart Candle Boundary");

    // 1분 간격의 경계 테스트
    let base_time = 1700000000i64; // 임의의 시작 시간
    let candle1_start = interval.get_candle_start(base_time);
    let candle2_start = interval.get_candle_start(base_time + 60); // 1분 후

    println!("✅ Candle 1 start: {}", candle1_start);
    println!("✅ Candle 2 start: {}", candle2_start);

    assert_eq!(candle2_start - candle1_start, 60, "Candles should be 60 seconds apart");

    // Candle 1 업데이트
    let price1 = BigDecimal::from_str("0.001").unwrap();
    let volume1 = BigDecimal::from_str("1000000000000000000").unwrap();

    cache_manager
        .update_chart_atomic(
            test_token,
            &interval.to_string(),
            candle1_start,
            &price1,
            Some(&volume1),
        )
        .await
        .expect("Failed to update candle 1");

    println!("✅ Candle 1 updated");

    // Candle 2 업데이트
    let price2 = BigDecimal::from_str("0.002").unwrap();
    let volume2 = BigDecimal::from_str("2000000000000000000").unwrap();

    cache_manager
        .update_chart_atomic(
            test_token,
            &interval.to_string(),
            candle2_start,
            &price2,
            Some(&volume2),
        )
        .await
        .expect("Failed to update candle 2");

    println!("✅ Candle 2 updated");

    // 두 캔들 조회 및 검증 (Redis에서 직접 조회)
    let chart1 = cache_manager
        .redis
        .get_chart(test_token, &interval.to_string(), candle1_start)
        .await
        .expect("Failed to get candle 1 from Redis");

    let chart2 = cache_manager
        .redis
        .get_chart(test_token, &interval.to_string(), candle2_start)
        .await
        .expect("Failed to get candle 2 from Redis");

    println!("\n📊 Candle Boundary Results:");
    println!("  Candle 1: price={}, volume={}, t={}", chart1.c, chart1.v, chart1.t);
    println!("  Candle 2: price={}, volume={}, t={}", chart2.c, chart2.v, chart2.t);

    // 검증
    assert_eq!(chart1.t, candle1_start);
    assert_eq!(chart2.t, candle2_start);
    assert_eq!(chart1.c, price1);
    assert_eq!(chart2.c, price2);
    assert_ne!(chart1.v, chart2.v, "Candles should have different volumes");

    println!("✅ Chart Candle Boundary test PASSED\n");
}
