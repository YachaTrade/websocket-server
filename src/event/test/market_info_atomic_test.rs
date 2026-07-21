/// Market Info 원자성 테스트
///
/// 이 테스트는 동시에 여러 이벤트가 발생했을 때
/// market_info의 volume이 정확하게 계산되는지 확인합니다.
///
/// 테스트 시나리오:
/// 1. Atomic 테스트: increment_market_volume 사용 (Lua Script)
/// 2. Non-Atomic 테스트: GET -> ADD -> SET 방식 (race condition 재현)
/// 3. Market Event Flow 통합 테스트: CurveSync -> MarketInfo 업데이트 검증

use crate::db::cache::CacheManager;
use crate::event::market::MARKET_EVENT_PRODUCER;
use crate::types::stream::{CurveEvent, CurveSync};
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
async fn test_concurrent_market_volume_updates_atomic() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_MARKET_ATOMIC_TOKEN";

    println!("\n🧪 Testing ATOMIC Market Volume Update (using Lua Script)");

    // 1. 테스트용 market_info 초기화 (volume = 0)
    let initial_market_info = crate::types::MarketInfo {
        market_type: crate::types::MarketType::Curve,
        token_id: test_token.to_string(),
        market_id: "0xMARKET_TEST".to_string(),
        token_price: "0.001".to_string(),
        native_price: "1".to_string(),
        price: "0.001".to_string(),
        total_supply: "1000000000000000000000000".to_string(), // 1M tokens
        liquidity: "10000000000000000000".to_string(),         // 10 ETH
        volume: "0".to_string(),
        ath_price: "0.001".to_string(),
        holder_count: 100,
        last_stats_update: 0,
    };

    cache_manager
        .set_market_info(test_token, &initial_market_info)
        .await
        .expect("Failed to set initial market info");

    println!("✅ Initial market_info set: volume = 0");

    // 2. 동시에 100개의 volume 증가 요청 (ATOMIC - Lua Script 사용)
    let num_tasks = 100;
    let increment_per_task = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH in Wei

    let mut handles = vec![];
    for i in 0..num_tasks {
        let cm = cache_manager.clone();
        let token = test_token.to_string();
        let inc = increment_per_task.clone();

        handles.push(tokio::spawn(async move {
            cm.increment_market_volume(&token, &inc)
                .await
                .expect("Failed to increment volume");
            if i % 20 == 0 {
                println!("  ✓ Task {} completed", i);
            }
        }));
    }

    // 모든 태스크 완료 대기
    for h in handles {
        h.await.unwrap();
    }

    println!("✅ All {} concurrent tasks completed", num_tasks);

    // 3. 결과 확인 (Redis에서 직접 조회 - Atomic 동작 검증)
    let final_info = cache_manager
        .redis
        .get_market_info(test_token)
        .await
        .expect("Failed to get market info from Redis");

    let final_volume = BigDecimal::from_str(&final_info.volume)
        .expect("Failed to parse volume");

    let expected = &increment_per_task * BigDecimal::from(num_tasks);

    println!("\n📊 ATOMIC Market Volume Test Results:");
    println!("  Expected volume: {} wei ({} ETH)", expected, &expected / BigDecimal::from(10u64.pow(18)));
    println!("  Actual volume:   {} wei ({} ETH)", final_volume, &final_volume / BigDecimal::from(10u64.pow(18)));
    println!("  Match:           {}", final_volume == expected);
    println!("  Difference:      {} wei", (&expected - &final_volume).abs());

    // Atomic 방식은 항상 정확해야 함
    assert_eq!(
        final_volume, expected,
        "Atomic version should always be correct! Expected: {}, Got: {}",
        expected, final_volume
    );

    // 정리
    cache_manager
        .redis
        .delete_market_info(test_token)
        .await
        .expect("Failed to cleanup");

    println!("✅ ATOMIC Market Volume test PASSED\n");
}

#[tokio::test]
async fn test_concurrent_market_volume_updates_non_atomic() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_MARKET_NON_ATOMIC_TOKEN";

    println!("\n🧪 Testing NON-ATOMIC Market Volume Update (GET -> ADD -> SET)");

    // 1. 초기화
    let initial_market_info = crate::types::MarketInfo {
        market_type: crate::types::MarketType::Curve,
        token_id: test_token.to_string(),
        market_id: "0xMARKET_TEST".to_string(),
        token_price: "0.001".to_string(),
        native_price: "1".to_string(),
        price: "0.001".to_string(),
        total_supply: "1000000000000000000000000".to_string(),
        liquidity: "10000000000000000000".to_string(),
        volume: "0".to_string(),
        ath_price: "0.001".to_string(),
        holder_count: 100,
        last_stats_update: 0,
    };

    cache_manager
        .set_market_info(test_token, &initial_market_info)
        .await
        .expect("Failed to set initial market info");

    println!("✅ Initial market_info set: volume = 0");

    // 2. Non-atomic 방식으로 동시 증가 (GET -> calculate -> SET)
    let num_tasks = 100;
    let increment_per_task = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH

    let mut handles = vec![];
    for i in 0..num_tasks {
        let cm = cache_manager.clone();
        let token = test_token.to_string();
        let inc = increment_per_task.clone();

        handles.push(tokio::spawn(async move {
            // NON-ATOMIC: Read -> Modify -> Write
            match cm.redis.get_market_info(&token).await {
                Ok(current) => {
                    let current_volume =
                        BigDecimal::from_str(&current.volume).unwrap_or(BigDecimal::from(0));
                    let new_volume = current_volume + inc;

                    // 약간의 지연 추가 (race condition 발생 확률 증가)
                    tokio::time::sleep(tokio::time::Duration::from_micros(50)).await;

                    // volume을 10^18으로 나눠서 Ether 단위로 저장
                    let scaled_volume = new_volume / &*crate::config::DECIMALS;
                    let _ = cm
                        .redis
                        .update_market_field(&token, "volume", &scaled_volume.to_string())
                        .await;

                    if i % 20 == 0 {
                        println!("  ✓ Task {} completed", i);
                    }
                }
                Err(e) => {
                    eprintln!("  ✗ Task {} failed to read: {}", i, e);
                }
            }
        }));
    }

    // 모든 태스크 완료 대기
    for h in handles {
        h.await.unwrap();
    }

    println!("✅ All {} concurrent tasks completed", num_tasks);

    // 3. 결과 확인 (Redis에서 직접 조회)
    let final_info = cache_manager
        .redis
        .get_market_info(test_token)
        .await
        .expect("Failed to get market info");

    let final_volume = BigDecimal::from_str(&final_info.volume)
        .expect("Failed to parse volume");

    let expected = &increment_per_task * BigDecimal::from(num_tasks);

    println!("\n📊 NON-ATOMIC Market Volume Test Results:");
    println!("  Expected volume: {} wei ({} ETH)", expected, &expected / BigDecimal::from(10u64.pow(18)));
    println!("  Actual volume:   {} wei ({} ETH)", final_volume, &final_volume / BigDecimal::from(10u64.pow(18)));
    println!("  Match:           {}", final_volume == expected);
    println!("  Lost volume:     {} wei ({} ETH)",
        (&expected - &final_volume).abs(),
        (&expected - &final_volume).abs() / BigDecimal::from(10u64.pow(18))
    );

    // Non-atomic은 race condition으로 인해 실패할 가능성이 높음
    if final_volume < expected {
        let lost = &expected - &final_volume;
        let lost_percent = (&lost / &expected) * BigDecimal::from(100);
        println!("\n⚠️  Race condition detected!");
        println!("  Lost {} wei ({:.2}% of total) due to concurrent writes", lost, lost_percent);
        println!("✅ This proves we NEED atomic operations for market volume!");
    } else {
        println!("\n⚠️  By chance, race condition didn't occur this time");
        println!("  Run the test multiple times to see race conditions");
    }

    // 정리
    cache_manager
        .redis
        .delete_market_info(test_token)
        .await
        .expect("Failed to cleanup");

    println!("✅ NON-ATOMIC Market Volume test completed\n");
}

#[tokio::test]
async fn test_market_event_producer_integration() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_MARKET_EVENT_INTEGRATION";

    println!("\n🧪 Testing Market Event Producer Integration");

    // MarketEventProducer 초기화
    if MARKET_EVENT_PRODUCER.get().is_none() {
        crate::event::market::MarketEventProducer::init()
            .expect("Failed to init MarketEventProducer");
    }

    let producer = MARKET_EVENT_PRODUCER
        .get()
        .expect("MarketEventProducer should be initialized")
        .clone();

    // 1. 초기 MarketInfo 설정
    let initial_market_info = crate::types::MarketInfo {
        market_type: crate::types::MarketType::Curve,
        token_id: test_token.to_string(),
        market_id: "0xMARKET_TEST".to_string(),
        token_price: "0.001".to_string(),
        native_price: "1".to_string(),
        price: "0.001".to_string(),
        total_supply: "1000000000000000000000000".to_string(),
        liquidity: "10000000000000000000".to_string(),
        volume: "0".to_string(),
        ath_price: "0.001".to_string(),
        holder_count: 100,
        last_stats_update: 0,
    };

    cache_manager
        .set_market_info(test_token, &initial_market_info)
        .await
        .expect("Failed to set initial market info");

    println!("✅ Initial market_info set");

    // 2. WebSocket 구독 생성
    let mut receiver = producer
        .get_event_receiver(test_token.to_string())
        .await
        .expect("Failed to get event receiver");

    println!("✅ WebSocket receiver created");

    // 3. CurveSync 이벤트 전송
    let sync_event = CurveSync {
        token: test_token.to_string(),
        virtual_token_amount: BigDecimal::from_str("1000000000000000000000").unwrap(),
        virtual_native_amount: BigDecimal::from_str("5000000000000000000").unwrap(),
        block_number: 12345,
        block_timestamp: chrono::Utc::now().timestamp() as u64,
        tx_hash: "0xTEST_TX_HASH".to_string(),
    };

    let boxed_event: Box<dyn CurveEvent> = Box::new(sync_event.clone());

    producer
        .curve_event_sender
        .send(boxed_event)
        .await
        .expect("Failed to send curve event");

    println!("✅ CurveSync event sent");

    // 4. 이벤트 수신 확인 (타임아웃 설정)
    match tokio::time::timeout(tokio::time::Duration::from_secs(2), receiver.recv()).await {
        Ok(Ok(market_message)) => {
            println!("✅ Market message received:");
            println!("  Token ID: {}", market_message.token_id);
            println!("  Volume: {}", market_message.market_info.volume);
            println!("  Price: {}", market_message.market_info.price);

            assert_eq!(market_message.token_id, test_token);
        }
        Ok(Err(e)) => {
            eprintln!("❌ Failed to receive market message: {}", e);
            panic!("Market message not received");
        }
        Err(_) => {
            eprintln!("❌ Timeout waiting for market message");
            panic!("Market message receive timeout");
        }
    }

    // 정리
    cache_manager
        .redis
        .delete_market_info(test_token)
        .await
        .expect("Failed to cleanup");

    println!("✅ Market Event Producer Integration test PASSED\n");
}

#[tokio::test]
async fn test_market_info_consistency_across_cache() {
    // 테스트 환경 초기화
    init_test_env().await;

    let cache_manager = CacheManager::instance().expect("CacheManager should be initialized");
    let test_token = "0xTEST_MARKET_CONSISTENCY";

    println!("\n🧪 Testing Market Info Consistency (Redis ↔ PostgreSQL)");

    // 1. MarketInfo 생성 및 저장
    let market_info = crate::types::MarketInfo {
        market_type: crate::types::MarketType::Curve,
        token_id: test_token.to_string(),
        market_id: "0xMARKET_TEST".to_string(),
        token_price: "0.001234567890123456".to_string(),
        native_price: "1.5".to_string(),
        price: "0.001234567890123456".to_string(),
        total_supply: "1000000000000000000000000".to_string(),
        liquidity: "10000000000000000000".to_string(),
        volume: "5000000000000000000000".to_string(), // 5000 ETH
        ath_price: "0.002".to_string(),
        holder_count: 150,
        last_stats_update: chrono::Utc::now().timestamp() as u64,
    };

    cache_manager
        .set_market_info(test_token, &market_info)
        .await
        .expect("Failed to set market info");

    println!("✅ Market info saved to cache");

    // 2. Redis에서 조회
    let redis_info = cache_manager
        .redis
        .get_market_info(test_token)
        .await
        .expect("Failed to get from Redis");

    println!("✅ Retrieved from Redis");

    // 3. Volume 증가
    let increment = BigDecimal::from_str("1000000000000000000").unwrap(); // 1 ETH
    cache_manager
        .increment_market_volume(test_token, &increment)
        .await
        .expect("Failed to increment volume");

    println!("✅ Volume incremented by 1 ETH");

    // 4. 다시 조회하여 volume 확인
    let updated_info = cache_manager
        .get_market_info(test_token)
        .await
        .expect("Failed to get updated market info");

    let original_volume = BigDecimal::from_str(&market_info.volume).unwrap();
    let updated_volume = BigDecimal::from_str(&updated_info.volume).unwrap();
    let expected_volume = original_volume + increment;

    println!("\n📊 Volume Update Results:");
    println!("  Original:  {} wei", market_info.volume);
    println!("  Increment: {} wei", increment);
    println!("  Expected:  {} wei", expected_volume);
    println!("  Actual:    {} wei", updated_volume);
    println!("  Match:     {}", updated_volume == expected_volume);

    assert_eq!(
        updated_volume, expected_volume,
        "Volume should be correctly updated"
    );

    // 5. 다른 필드들이 변경되지 않았는지 확인
    assert_eq!(updated_info.token_price, market_info.token_price);
    assert_eq!(updated_info.liquidity, market_info.liquidity);
    assert_eq!(updated_info.holder_count, market_info.holder_count);

    println!("✅ Other fields remain unchanged");

    // 정리
    cache_manager
        .redis
        .delete_market_info(test_token)
        .await
        .expect("Failed to cleanup");

    println!("✅ Market Info Consistency test PASSED\n");
}
