use std::{error::Error, sync::Arc, time::Duration};

use alloy::{
    primitives::Address,
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::{Context, Result};

use bigdecimal::RoundingMode;
use dashmap::DashMap;
use tokio::time::{self, sleep};
use tokio_stream::StreamExt;

use crate::{
    error_log,
    types::{MarketInfo, MarketType, TokenInfo},
};
use tracing::{info, instrument, warn};

use crate::{
    client::RpcClient,
    config::V2_BONDING_CURVE_ADDRESS,
    db::cache::CacheManager,
    types::stream::{
        Buy, CreateCurve, CurveChartUpdate, CurveEventType, CurveSync, EventType, Graduate, Sell,
        TokenMetadata,
    },
    utils::to_big_decimal,
};

use super::receive::receive_v2_curve_event;

/// 트랜잭션별 이벤트 버퍼 키
/// 같은 tx 안에서 trade보다 앞선 가장 가까운 Sync를 찾아 chart update를 생성한다.
///
/// V2 BondingCurve는 단일 컨트랙트가 모든 V2 토큰을 호스트하므로 한 트랜잭션이
/// 두 토큰을 건드릴 수 있다(router/aggregator multi-hop 등). 토큰을 키에 포함하지
/// 않으면 token A의 Sell이 token B의 Sync와 페어돼 B의 chart에 A의 거래량이
/// 박히는 cross-token 오염이 발생한다. token을 키에 포함해 토큰별로 버퍼 entry를
/// 분리한다.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct EventBufferKey {
    transaction_hash: String,
    transaction_index: u64,
    token: String,
}

/// 이벤트 버퍼
/// Buy/Sell과 Sync를 묶어서 처리하기 위한 임시 저장소
#[derive(Debug)]
struct TransactionEventBuffer {
    trades: Vec<CurveEventType>,
    syncs: Vec<CurveSync>,
    timestamp: time::Instant,
}

/// V2 Curve 이벤트 버퍼 (transaction_hash + log_index -> events)
/// V1과 독립적인 싱글톤으로 전역 관리
static EVENT_BUFFER: once_cell::sync::Lazy<Arc<DashMap<EventBufferKey, TransactionEventBuffer>>> =
    once_cell::sync::Lazy::new(|| Arc::new(DashMap::new()));

// V2 BondingCurve ABI 바인딩
sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    IV2BondingCurve,
    "abi/v2/BondingCurve.json"
}

/// V2 BondingCurve 이벤트 스트림
/// 수신 이벤트: Create, Buy, Sell, Sync, Graduate
/// V1과 동일한 패턴이지만 V2 ABI 및 V2_BONDING_CURVE_ADDRESS 사용
#[instrument(skip(_event_type))]
pub async fn stream_v2_curve_events(_event_type: EventType) -> Result<()> {
    let client = match RpcClient::instance() {
        Ok(client) => client,
        Err(e) => {
            panic!("Failed to get RpcClient instance: {}", e);
        }
    };

    // V2 BondingCurve 주소로 필터 생성
    // SnipingPenalty 이벤트는 구독하지 않음
    let filter = Filter::new()
        .address(V2_BONDING_CURVE_ADDRESS.parse::<Address>().unwrap())
        .events(vec![
            IV2BondingCurve::Create::SIGNATURE,
            IV2BondingCurve::Buy::SIGNATURE,
            IV2BondingCurve::Sell::SIGNATURE,
            IV2BondingCurve::Sync::SIGNATURE,
            IV2BondingCurve::Graduate::SIGNATURE,
        ]);

    let stream_timeout = Duration::from_millis(*crate::config::STREAM_TIMEOUT);
    let reconnect_delay = Duration::from_secs(1);
    info!(
        "V2 Curve stream timeout set to {}ms",
        *crate::config::STREAM_TIMEOUT
    );

    // 블록 타임스탬프 로컬 캐시 - (timestamp, 저장 시각)
    // Redis 대신 로컬 메모리 사용으로 성능 향상 (10초 TTL)
    let block_timestamp_cache: Arc<DashMap<u64, (u64, time::Instant)>> = Arc::new(DashMap::new());

    // 백그라운드 태스크: 10초 이상 지난 캐시 항목 자동 정리 (5초마다 실행)
    {
        let block_cache = block_timestamp_cache.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                let now = time::Instant::now();
                block_cache.retain(|_, (_, timestamp)| {
                    now.duration_since(*timestamp) < Duration::from_secs(10)
                });
            }
        });
    }

    // 백그라운드 태스크: 2초 이상 지난 버퍼 항목 자동 정리 및 강제 플러시 (1초마다 실행)
    {
        let buffer = EVENT_BUFFER.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                let now = time::Instant::now();
                let mut to_remove = Vec::new();

                // 2초 이상 지난 항목은 강제로 플러시 (Sync 없이 Trade만 있는 경우)
                for entry in buffer.iter() {
                    if now.duration_since(entry.timestamp) > Duration::from_secs(2) {
                        to_remove.push(entry.key().clone());
                    }
                }

                for key in to_remove {
                    if let Some((_, buf)) = buffer.remove(&key) {
                        warn!(
                            "Flushing incomplete V2 transaction buffer after timeout: tx={}, tx_idx={}, trades={}, syncs={}",
                            key.transaction_hash,
                            key.transaction_index,
                            buf.trades.len(),
                            buf.syncs.len()
                        );
                    }
                }
            }
        });
    }

    // CacheManager 인스턴스를 외부에서 한 번만 획득하여 재사용
    let cache_manager = match CacheManager::instance() {
        Ok(cm) => cm,
        Err(e) => {
            panic!("Failed to get CacheManager instance: {}", e);
        }
    };

    // 무한 루프로 지속적인 스트림 유지
    loop {
        let current_provider_index = client.get_current_provider_index().await;

        // 새로운 스트림 생성 시도 (표준 eth_subscribe("logs"))
        let mut stream = match client.get_stream(&filter).await {
            Ok(stream) => stream,
            Err(e) => {
                error_log!("Failed to create V2 curve stream, retrying in 1s: {}", e);
                client.update_best_provider().await;
                tokio::time::sleep(reconnect_delay).await;
                continue;
            }
        };

        info!("V2 Curve log stream started successfully");

        // 스트림 이벤트 처리 루프 - provider 변경 감지
        loop {
            // Provider 변경 감지
            let new_provider_index = client.get_current_provider_index().await;
            if new_provider_index != current_provider_index {
                info!(
                    "Provider changed from {} to {}, reconnecting V2 Curve stream",
                    current_provider_index, new_provider_index
                );
                // 명시적으로 stream을 drop하여 subscription 정리
                drop(stream);
                info!("V2 Curve stream subscription closed due to provider change");
                break; // 외부 루프로 돌아가서 새 스트림 생성
            }

            match tokio::time::timeout(stream_timeout, stream.next()).await {
                Ok(Some(log)) => {
                    // Reorg로 되돌려진 로그(removed=true)는 무시 — 표준 logs 구독은
                    // reorg 시 같은 로그를 removed 플래그와 함께 재전달한다.
                    if log.removed {
                        continue;
                    }

                    // V2 curve 스트림에서 로그를 받았으므로 스트림이 살아있음을 기록
                    crate::metrics::METRICS.stream.record_curve_event();

                    let cache_mgr = cache_manager.clone();
                    let block_cache = block_timestamp_cache.clone();
                    tokio::spawn(async move {
                        let event = parse_log(log, client, cache_mgr, block_cache).await;
                        match event {
                            Ok(event) => {
                                info!("V2 CURVE Stream Event: {event:?}");
                                if let Err(e) = handle_v2_curve_event(event).await {
                                    error_log!("Failed to handle V2 curve event: {}", e);
                                }
                            }
                            Err(e) => {
                                if !e.to_string().contains("Not a white list token") {
                                    error_log!("Failed to parse V2 curve log - error: {}", e);
                                }
                            }
                        }
                    });
                }
                Ok(None) => {
                    warn!(
                        "V2 Curve stream ended (subscription error or WebSocket closed), reconnecting..."
                    );
                    // 명시적으로 stream을 drop하여 subscription 정리
                    drop(stream);
                    info!("V2 Curve stream subscription closed due to stream end");

                    // WebSocket 에러로 인한 종료일 수 있으므로 provider 재연결
                    if let Err(e) = client.reconnect_current_provider().await {
                        warn!(
                            "Failed to reconnect provider: {}, trying fallback provider on next loop",
                            e
                        );
                        // 재연결 실패 시 fallback provider로 전환
                        client.update_best_provider().await;
                    }

                    break;
                }
                Err(_) => {
                    warn!(
                        "V2 Curve stream timed out after {}ms, reconnecting provider to cleanup subscriptions...",
                        stream_timeout.as_millis()
                    );
                    // 명시적으로 stream을 drop하여 subscription 정리
                    drop(stream);
                    info!("V2 Curve stream subscription closed due to timeout");

                    // Provider를 재연결하여 WebSocket connection과 모든 subscription 정리
                    if let Err(e) = client.reconnect_current_provider().await {
                        warn!(
                            "Failed to reconnect provider: {}, trying fallback provider on next loop",
                            e
                        );
                        // 재연결 실패 시 fallback provider로 전환
                        client.update_best_provider().await;
                    }

                    break;
                }
            }
        }

        // Stream이 종료된 후 재연결 전 짧은 대기
        // reconnect_current_provider()가 이미 200ms 대기하므로 추가 대기는 최소화
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// V2 BondingCurve 로그를 파싱하여 CurveEventType으로 변환
/// V2 ABI의 이벤트 시그니처를 사용하여 Create, Buy, Sell, Sync, Graduate 이벤트를 파싱
use crate::stream::{is_valid_nadfun_token_address, validate_curve_token};

async fn parse_log(
    log: Log,
    client: &RpcClient,
    cache_manager: Arc<CacheManager>,
    block_timestamp_cache: Arc<DashMap<u64, (u64, time::Instant)>>,
) -> Result<CurveEventType> {
    let transaction_hash = match log.transaction_hash {
        Some(hash) => hash.to_string(),
        None => {
            error_log!("No transaction hash found in V2 curve log: {:?}", log);
            return Err(anyhow::anyhow!("No transaction hash"));
        }
    };

    let block_number = match log.block_number {
        Some(number) => number,
        None => match client.get_latest_block_number().await {
            Ok(block_number) => block_number,
            Err(e) => {
                error_log!("Failed to get latest block number: {}", e);
                return Err(anyhow::anyhow!(
                    "Fail to V2 Curve get block number {e:?}\nLog{log:?}"
                ));
            }
        },
    };

    let block_timestamp = match log.block_timestamp {
        Some(timestamp) => timestamp,
        None => {
            match get_cached_block_timestamp(client, block_number, &block_timestamp_cache).await {
                Ok(timestamp) => timestamp,
                Err(e) => {
                    error_log!(
                        "Fail to V2 Curve get block timestamp for block {block_number}: {e:?}\nLog: {log:?}"
                    );
                    return Err(anyhow::anyhow!(
                        "Fail to get block timestamp for block {block_number}: {e:?}\nLog: {log:?}"
                    ));
                }
            }
        }
    };
    let log_index = log.log_index.unwrap_or(u64::MAX);
    let transaction_index = log.transaction_index.unwrap_or(u64::MAX);

    match log.topic0() {
        // V2 Create 이벤트 처리
        // V1의 CurveCreate와 달리 pair, quoteToken 필드가 추가됨
        Some(&IV2BondingCurve::Create::SIGNATURE_HASH) => {
            let IV2BondingCurve::Create {
                creator,
                token,
                pair,
                quoteToken,
                name,
                symbol,
                tokenURI,
                virtualQuoteReserve,
                virtualTokenReserve,
                minTokenReserve: _,
            } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode V2 Create log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode V2 Create log: {}", e));
                }
            };

            // V2 nad.fun 토큰은 마지막 4자가 "7777" — 컨트랙트가 vanity 강제하지만 본 서버에서도 defensive check.
            // 통과 못 하면 whitelist 등록 자체를 안 해 downstream Buy/Sell에서도 자동 거부.
            let token_str_check = token.to_string();
            if !is_valid_nadfun_token_address(&token_str_check) {
                return Err(anyhow::anyhow!(
                    "Not a white list token (suffix mismatch): {}",
                    token_str_check
                ));
            }

            // 토큰 메타데이터 fetch (15초 타임아웃)
            let token_metadata = match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                fetch_token_metadata(&tokenURI),
            )
            .await
            {
                Ok(result) => match result {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        error_log!("Failed to fetch token metadata: {}", e);
                        return Err(anyhow::anyhow!("Failed to fetch token metadata: {}", e));
                    }
                },
                Err(_) => {
                    error_log!("Fail to fetch token metadata");
                    return Err(anyhow::anyhow!("Fail to fetch token metadata"));
                }
            };

            {
                let creator = creator.to_string();
                let (account_info, latest_native_price_result) = tokio::join!(
                    cache_manager.get_account_info(&creator),
                    cache_manager.get_latest_price()
                );

                let latest_native_price = match latest_native_price_result {
                    Ok(price) => price,
                    Err(e) => {
                        error_log!("Failed to get latest native price: {}", e);
                        return Err(anyhow::anyhow!("Failed to get latest native price: {}", e));
                    }
                };

                // 토큰 정보 및 예약 정보 준비
                let token_info = TokenInfo {
                    token_id: token.to_string(),
                    name: name.to_string(),
                    symbol: symbol.to_string(),
                    image_uri: token_metadata.image_uri.clone(),
                    description: token_metadata.description.clone(),
                    is_graduated: false,
                    is_nsfw: token_metadata.is_nsfw,
                    twitter: token_metadata.twitter.clone(),
                    telegram: token_metadata.telegram.clone(),
                    website: token_metadata.website.clone(),
                    created_at: block_timestamp as i64,
                    creator: account_info,
                    is_cto: false,
                    version: crate::types::TokenVersion::V2,
                };

                // Price = virtual_quote / virtual_token (1 token을 사는데 필요한 quote 양)
                // CurveSync와 동일한 계산 방식 사용
                // BigDecimal → String 변환은 전부 .normalized().to_plain_string() 로 통일
                let price = (to_big_decimal(virtualQuoteReserve)
                    / to_big_decimal(virtualTokenReserve))
                .with_scale_round(10, RoundingMode::Up);
                let token_price = (&price * &to_big_decimal(&latest_native_price))
                    .normalized()
                    .to_plain_string();
                let price = price.normalized().to_plain_string();
                let reserve_quote = to_big_decimal(virtualQuoteReserve)
                    .normalized()
                    .to_plain_string();
                let reserve_token = to_big_decimal(virtualTokenReserve)
                    .normalized()
                    .to_plain_string();

                // Curve: MarketType::Curve, market_id = V2_BONDING_CURVE_ADDRESS
                // V2는 quoteToken이 WMON이 아닐 수 있으므로 DB에서 QuoteInfo 조회
                // fee_config도 함께 조회 (V2 전용)
                let quote_token_str = quoteToken.to_string();
                let token_str_for_fee = token.to_string();
                let (quote_info, fee_info) = tokio::join!(
                    cache_manager.get_quote_info(&quote_token_str),
                    cache_manager.get_fee_info(&token_str_for_fee)
                );

                let market_info = MarketInfo {
                    market_type: MarketType::Curve,
                    token_id: token.to_string(),
                    quote_info,
                    market_id: V2_BONDING_CURVE_ADDRESS.clone(),
                    token_price: token_price.clone(),
                    native_price: latest_native_price.clone(),
                    quote_price: latest_native_price,
                    price: price.clone(),
                    price_usd: token_price, // USD/Token = price * native_price
                    price_native: price.clone(), // MON/Token = price
                    price_quote: price.clone(), // Quote/Token = price
                    ath_price: price.clone(), // 기존 호환성 유지
                    ath_price_usd: price.clone(), // 초기 ATH (USD) - CreateCurve 시점에는 price랑 동일
                    ath_price_native: price.clone(), // 초기 ATH (Native) = price
                    ath_price_quote: price,       // 초기 ATH (Quote) = price
                    total_supply: "1_000_000_000_000_000_000_000_000_000".to_string(),
                    reserve_native: reserve_quote.clone(),
                    reserve_quote,
                    reserve_token,
                    volume: "0".to_string(),
                    holder_count: 0,      // 초기 생성자 1명
                    last_stats_update: 0, // 초기 생성 시 0
                    fee_info,             // V2 fee 설정
                };

                // String 값들을 미리 생성
                let token_str = token.to_string();
                let pair_str = pair.to_string();

                // 모든 캐시 작업을 병렬로 실행
                // V2 Create에서는 pair 화이트리스트도 함께 등록
                // V2 Create: quoteToken과 token으로 token0/token1 결정
                // V2에서는 quoteToken이 WMON이 아닐 수도 있으므로 quoteToken 주소로 정렬
                let (pool_token0, pool_token1) =
                    if quoteToken.to_string().to_lowercase() < token_str.to_lowercase() {
                        (quoteToken.to_string(), token_str.clone())
                    } else {
                        (token_str.clone(), quoteToken.to_string())
                    };

                let (result1, result2, result3, result4, result5) = tokio::join!(
                    cache_manager.insert_white_list_token(&token_str, true),
                    cache_manager.insert_white_list_pool(&pair_str, true),
                    cache_manager.set_token_info(&token_str, &token_info),
                    cache_manager.set_market_info(&token_str, &market_info),
                    // V2에서는 Create 시점에 pair가 존재하므로 pool_pair 등록
                    // Swap 이벤트에서 get_pool_pair로 token0/token1 조회 가능하게 함
                    cache_manager.insert_pool_pair(&pair_str, &pool_token0, &pool_token1),
                );

                // 모든 결과 확인
                if let Err(e) = result1 {
                    error_log!("Failed to insert white list token: {}", e);
                    return Err(anyhow::anyhow!("Failed to insert white list token: {}", e));
                }
                if let Err(e) = result2 {
                    error_log!("Failed to insert white list pool: {}", e);
                    return Err(anyhow::anyhow!("Failed to insert white list pool: {}", e));
                }
                if let Err(e) = result3 {
                    error_log!("Failed to set token info: {}", e);
                    return Err(anyhow::anyhow!("Failed to set token info: {}", e));
                }
                if let Err(e) = result4 {
                    error_log!("Failed to set market info: {}", e);
                    return Err(anyhow::anyhow!("Failed to set market info: {}", e));
                }
                if let Err(e) = result5 {
                    error_log!("Failed to insert pair white list pool: {}", e);
                    return Err(anyhow::anyhow!(
                        "Failed to insert pair white list pool: {}",
                        e
                    ));
                }
            }

            // V2 Create: quoteToken 사용, pair 포함
            let create_curve = CreateCurve {
                creator: creator.to_string(),
                token: token.to_string(),
                virtual_token: to_big_decimal(virtualTokenReserve),
                virtual_native: to_big_decimal(virtualQuoteReserve),
                token_metadata,
                name,
                symbol,
                transaction_hash,
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
                quote_token: quoteToken.to_string(),
                pair: Some(pair.to_string()),
                version: crate::types::TokenVersion::V2,
            };

            info!(
                "🟢 V2 CURVE CREATE 이벤트 생성: token={}, creator={}, name={}",
                create_curve.token, create_curve.creator, create_curve.name
            );
            Ok(CurveEventType::CreateCurve(create_curve))
        }

        // V2 Buy 이벤트 처리
        // V1과 달리 (token, buyer, quoteIn, tokenOut) 필드 사용
        Some(&IV2BondingCurve::Buy::SIGNATURE_HASH) => {
            let curve = log.address().to_string();
            info!("V2 Buy log decoded: curve={}", curve);
            let IV2BondingCurve::Buy {
                token,
                buyer,
                quoteIn,
                tokenOut,
            } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode V2 Buy log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode V2 Buy log: {}", e));
                }
            };
            info!(
                "V2 Buy log decoded: token={}, buyer={}, quoteIn={}, tokenOut={}",
                token, buyer, quoteIn, tokenOut
            );

            let token = token.to_string();

            // 토큰 검증 — 7777 suffix + whitelist (Create와 같은 tx race 대비 최대 3초 polling)
            validate_curve_token(&cache_manager, &token).await?;

            // buyer로 actor 해석 (EIP-7702 지원)
            // V2 Curve Buy/Sell은 swap_to 동등 필드가 없어 None 전달.
            let account_id = cache_manager
                .resolve_actor(&transaction_hash, &buyer.to_string(), &token, true, None)
                .await
                .unwrap_or_else(|e| {
                    warn!("[V2 CURVE] Failed to resolve actor for Buy: {}", e);
                    buyer.to_string()
                });
            // V2: quoteIn = amountIn, tokenOut = amountOut
            let amount_in = to_big_decimal(quoteIn);
            let amount_out = to_big_decimal(tokenOut);

            let buy = Buy {
                account_id,
                to: None,
                amount_in,
                amount_out,
                token,
                market: curve.to_string(),
                market_type: MarketType::Curve,
                transaction_hash,
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            };

            info!(
                "🔵 V2 CURVE BUY 이벤트 생성: token={}, amount_in={}, amount_out={}",
                buy.token, buy.amount_in, buy.amount_out
            );

            // NOTE: metrics/market 업데이트는 event handler에서 순차 처리됨
            // (race condition 방지를 위해 stream에서 제거)

            Ok(CurveEventType::Buy(buy))
        }

        // V2 Sell 이벤트 처리
        // V1과 달리 (token, seller, tokenIn, quoteOut) 필드 사용
        Some(&IV2BondingCurve::Sell::SIGNATURE_HASH) => {
            let curve = log.address().to_string();

            let IV2BondingCurve::Sell {
                token,
                seller,
                tokenIn,
                quoteOut,
            } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode V2 Sell log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode V2 Sell log: {}", e));
                }
            };

            let token = token.to_string();

            // 토큰 검증 — 7777 suffix + whitelist (Create와 같은 tx race 대비 최대 3초 polling)
            validate_curve_token(&cache_manager, &token).await?;

            // seller로 actor 해석 (EIP-7702 지원)
            let account_id = cache_manager
                .resolve_actor(&transaction_hash, &seller.to_string(), &token, false, None)
                .await
                .unwrap_or_else(|e| {
                    warn!("[V2 CURVE] Failed to resolve actor for Sell: {}", e);
                    seller.to_string()
                });
            // V2: tokenIn = amountIn, quoteOut = amountOut
            let amount_in = to_big_decimal(tokenIn);
            let amount_out = to_big_decimal(quoteOut);

            let sell = Sell {
                account_id,
                to: None,
                amount_in,
                amount_out,
                token,
                market: curve,
                market_type: MarketType::Curve,
                transaction_hash,
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            };

            info!(
                "🔴 V2 CURVE SELL 이벤트 생성: token={}, amount_in={}, amount_out={}",
                sell.token, sell.amount_in, sell.amount_out
            );

            // NOTE: metrics/market 업데이트는 event handler에서 순차 처리됨
            // (race condition 방지를 위해 stream에서 제거)

            Ok(CurveEventType::Sell(sell))
        }

        // V2 Sync 이벤트 처리
        // V2: realQuoteReserve, realTokenReserve, virtualQuoteReserve, virtualTokenReserve
        Some(&IV2BondingCurve::Sync::SIGNATURE_HASH) => {
            let IV2BondingCurve::Sync {
                token,
                realQuoteReserve,
                realTokenReserve,
                virtualQuoteReserve,
                virtualTokenReserve,
            } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode V2 Sync log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode V2 Sync log: {}", e));
                }
            };

            let token = token.to_string();

            // V2: quote는 반드시 native(WMON)가 아닐 수 있음
            // CurveSync 필드명은 V1 호환을 위해 `native`로 유지하지만, 의미상 quote 리저브
            let virtual_quote = to_big_decimal(virtualQuoteReserve);
            let virtual_token = to_big_decimal(virtualTokenReserve);

            // Price = virtual_quote / virtual_token (1 token을 사는데 필요한 quote 양)
            let price = (&virtual_quote / &virtual_token)
                .with_scale_round(10, bigdecimal::RoundingMode::Up);

            let sync = CurveSync {
                token: token.to_string(),
                reserve_native_amount: to_big_decimal(realQuoteReserve), // V2: quote reserve
                reserve_token_amount: to_big_decimal(realTokenReserve),
                virtual_native_amount: virtual_quote, // V2: quote reserve
                virtual_token_amount: virtual_token,
                price,
                transaction_hash,
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
            };

            info!(
                "🔄 V2 CURVE SYNC 이벤트 생성: token={}, reserve_token_amount={}",
                sync.token, sync.reserve_token_amount
            );

            // NOTE: metrics/market 업데이트는 event handler에서 순차 처리됨
            // (race condition 방지를 위해 stream에서 제거)

            Ok(CurveEventType::CurveSync(sync))
        }

        // V2 Graduate 이벤트 처리
        // V2: (token, pair) -> pair를 pool로 매핑
        Some(&IV2BondingCurve::Graduate::SIGNATURE_HASH) => {
            let IV2BondingCurve::Graduate { token, pair } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode V2 Graduate log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode V2 Graduate log: {}", e));
                }
            };

            let token = token.to_string();
            let pool = pair.to_string();

            // V2에서는 quoteToken이 WMON이 아닐 수 있으므로 market_info에서 quote_id 조회.
            // 조회 실패 시 WMON으로 fallback하지 않음 — non-WMON quote 토큰의 경우
            // pool_pair가 (WMON, token)으로 잘못 등록되어 on-chain pool의 실제
            // (token0, token1)과 어긋남. 이후 모든 PAIR 이벤트의 reserve/amount
            // 해석이 뒤집힘.
            //
            // 다만 CreateCurve와 Graduate가 같은 트랜잭션/직후 블록에서 연달아
            // 발생하면 indexer가 PostgreSQL `market` 행을 아직 commit 못 한 짧은
            // 레이스가 생긴다. 이때 그냥 graduate를 폐기하면 local cache가 Curve
            // 로 영구 stuck 되어 socket payload가 계속 Curve로 내려간다(리스타트
            // 해야 회복). 폐기 전에 짧은 지수 백오프로 재시도해 indexer가 따라오는
            // 윈도우를 흡수한다. 5회 × (200/400/800/1600 ms) = 총 3s sleep.
            const GRADUATE_MARKET_INFO_MAX_ATTEMPTS: u32 = 5;
            const GRADUATE_MARKET_INFO_INITIAL_DELAY_MS: u64 = 200;
            let market_lookup = crate::utils::retry::retry_async(
                || cache_manager.get_market_info(&token),
                GRADUATE_MARKET_INFO_MAX_ATTEMPTS,
                GRADUATE_MARKET_INFO_INITIAL_DELAY_MS,
                |attempt, err, delay| {
                    warn!(
                        "V2 Graduate: market_info 조회 실패 ({}) — 재시도 {}/{} ({}ms 후): {}",
                        token,
                        attempt,
                        GRADUATE_MARKET_INFO_MAX_ATTEMPTS,
                        delay.as_millis(),
                        err
                    );
                },
            )
            .await;
            let quote_id = match market_lookup {
                Ok(market) => market.quote_info.quote_id,
                Err(e) => {
                    error_log!(
                        "V2 Graduate: market_info 조회 실패 ({}) — quote_id 결정 불가 ({}회 재시도 후 포기), graduate 중단: {}",
                        token,
                        GRADUATE_MARKET_INFO_MAX_ATTEMPTS,
                        e
                    );
                    return Err(anyhow::anyhow!(
                        "V2 Graduate: market_info 조회 실패 ({}) after {} retries: {}",
                        token,
                        GRADUATE_MARKET_INFO_MAX_ATTEMPTS,
                        e
                    ));
                }
            };

            // quote_id와 token을 주소 정렬하여 token0, token1 결정
            let (token0, token1) = if quote_id.to_lowercase() < token.to_lowercase() {
                (quote_id.clone(), token.clone())
            } else {
                (token.clone(), quote_id.clone())
            };
            {
                // 두 캐시 작업을 병렬로 실행
                let (result1, result2) = tokio::join!(
                    cache_manager.insert_pool_pair(&pool, &token0, &token1),
                    cache_manager.insert_white_list_pool(&pool, true)
                );

                // 결과 확인
                if let Err(e) = result1 {
                    error_log!("Failed to insert pool pair: {}", e);
                    return Err(anyhow::anyhow!("Failed to insert pool pair: {}", e));
                }
                if let Err(e) = result2 {
                    error_log!("Failed to insert white list pool: {}", e);
                    return Err(anyhow::anyhow!("Failed to insert white list pool: {}", e));
                }
            }

            let graduate = Graduate {
                token: token.to_string(),
                pool: pool.to_string(),
                transaction_hash,
                block_timestamp,
                block_number,
                log_index,
                transaction_index,
            };

            // Redis 캐시 업데이트: TokenInfo와 MarketInfo 최신화
            // graduated 발생 시 is_graduated=true, market_id=pool로 업데이트
            if let Err(e) = cache_manager.update_cache_from_graduate(&graduate).await {
                // 캐시 업데이트 실패는 치명적이지 않으므로 warning만 로깅하고 계속 진행
                warn!(
                    "Failed to update cache from V2 Graduate for token {}: {}",
                    graduate.token, e
                );
            }

            info!(
                "🎓 V2 Graduate 이벤트 생성: token={}, pool={}",
                graduate.token, graduate.pool
            );
            Ok(CurveEventType::Graduate(graduate))
        }
        _ => Err(anyhow::anyhow!("Unknown V2 curve event type")),
    }
}

/// 블록 타임스탬프 로컬 캐시 조회
/// Redis 대신 로컬 메모리 사용으로 성능 향상 (10초 TTL)
async fn get_cached_block_timestamp(
    client: &RpcClient,
    block_number: u64,
    cache: &DashMap<u64, (u64, time::Instant)>,
) -> Result<u64> {
    // 1. 로컬 캐시 확인
    if let Some(entry) = cache.get(&block_number) {
        return Ok(entry.0);
    }

    // 2. RPC에서 조회
    let block_timestamp = match client.get_block_timestamp(block_number).await {
        Ok(timestamp) => timestamp,
        Err(e) => {
            error_log!(
                "Failed to get block timestamp for block {}: {}",
                block_number,
                e
            );
            return Err(anyhow::anyhow!(
                "Failed to get block timestamp for block {}: {}",
                block_number,
                e
            ));
        }
    };

    // 3. 캐시에 저장 (10초 TTL)
    cache.insert(block_number, (block_timestamp, time::Instant::now()));

    Ok(block_timestamp)
}

/// V2 Curve 이벤트를 처리하고 Buy/Sell + Sync를 묶어서 ChartUpdate 생성
async fn handle_v2_curve_event(event: CurveEventType) -> Result<()> {
    let buffer = EVENT_BUFFER.clone();

    // 패턴 매칭으로 이벤트 타입 확인 (enum의 장점 활용)
    match &event {
        CurveEventType::Buy(buy) => {
            let key = EventBufferKey {
                transaction_hash: buy.transaction_hash.clone(),
                transaction_index: buy.transaction_index,
                token: buy.token.clone(),
            };
            info!(
                "📥 V2 Buy event: tx={}, tx_idx={}, log_idx={}, token={}",
                buy.transaction_hash, buy.transaction_index, buy.log_index, buy.token
            );
            process_trade_event(event.clone(), key, buffer).await?;
            receive_v2_curve_event(event).await
        }
        CurveEventType::Sell(sell) => {
            let key = EventBufferKey {
                transaction_hash: sell.transaction_hash.clone(),
                transaction_index: sell.transaction_index,
                token: sell.token.clone(),
            };
            info!(
                "📥 V2 Sell event: tx={}, tx_idx={}, log_idx={}, token={}",
                sell.transaction_hash, sell.transaction_index, sell.log_index, sell.token
            );
            process_trade_event(event.clone(), key, buffer).await?;
            receive_v2_curve_event(event).await
        }
        CurveEventType::CurveSync(sync) => {
            let key = EventBufferKey {
                transaction_hash: sync.transaction_hash.clone(),
                transaction_index: sync.transaction_index,
                token: sync.token.clone(),
            };
            info!(
                "📥 V2 Sync event: tx={}, tx_idx={}, log_idx={}, token={}",
                sync.transaction_hash, sync.transaction_index, sync.log_index, sync.token
            );
            process_sync_event(event.clone(), sync.clone(), key, buffer).await?;
            receive_v2_curve_event(event).await
        }
        // CreateCurve, Graduate 등 다른 이벤트는 그대로 전송
        _ => receive_v2_curve_event(event).await,
    }
}

/// Buy/Sell 이벤트를 버퍼에 추가하고 두 개 모였는지 확인
async fn process_trade_event(
    event: CurveEventType,
    key: EventBufferKey,
    buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>>,
) -> Result<()> {
    let _block_number = event.get_block_number();

    // 버퍼에 Trade 추가
    buffer
        .entry(key.clone())
        .or_insert(TransactionEventBuffer {
            trades: Vec::new(),
            syncs: Vec::new(),
            timestamp: time::Instant::now(),
        })
        .trades
        .push(event);

    drain_ready_chart_updates(key, buffer).await
}

/// Sync 이벤트를 버퍼에 추가하고 두 개 모였는지 확인
async fn process_sync_event(
    event: CurveEventType,
    sync: CurveSync,
    key: EventBufferKey,
    buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>>,
) -> Result<()> {
    let _block_number = event.get_block_number();

    // 버퍼에 Sync 추가
    buffer
        .entry(key.clone())
        .or_insert(TransactionEventBuffer {
            trades: Vec::new(),
            syncs: Vec::new(),
            timestamp: time::Instant::now(),
        })
        .syncs
        .push(sync);

    drain_ready_chart_updates(key, buffer).await
}

/// 같은 tx 안에서 trade보다 앞선 가장 가까운 Sync를 찾아 ChartUpdate 생성 및 전송
async fn drain_ready_chart_updates(
    key: EventBufferKey,
    buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>>,
) -> Result<()> {
    let mut chart_updates = Vec::new();
    let mut remove_empty_buffer = false;

    if let Some(mut entry) = buffer.get_mut(&key) {
        loop {
            let Some((trade_idx, sync_idx)) = find_ready_curve_pair(&entry) else {
                break;
            };

            let trade = entry.trades.remove(trade_idx);
            let sync = entry.syncs.remove(sync_idx);
            chart_updates.push(CurveChartUpdate {
                trade: Some(Box::new(trade)),
                block_number: sync.block_number,
                transaction_hash: sync.transaction_hash.clone(),
                sync,
            });
        }

        info!(
            "🔍 V2 Buffer check: tx={}, tx_idx={}, trades={}, syncs={}, ready_updates={}",
            key.transaction_hash,
            key.transaction_index,
            entry.trades.len(),
            entry.syncs.len(),
            chart_updates.len()
        );

        remove_empty_buffer = entry.trades.is_empty() && entry.syncs.is_empty();
    } else {
        info!(
            "⚠️  V2 Buffer miss: tx={}, tx_idx={} not found in buffer",
            key.transaction_hash, key.transaction_index
        );
    }

    if remove_empty_buffer {
        buffer.remove(&key);
    }

    for chart_update in chart_updates {
        info!(
            "🎯 V2 CurveChartUpdate created: tx={}, token={}",
            chart_update.transaction_hash, chart_update.sync.token
        );

        use crate::stream::v2::curve::receive::handle_v2_chart_update_event;
        handle_v2_chart_update_event(chart_update).await?;
    }

    Ok(())
}

fn find_ready_curve_pair(entry: &TransactionEventBuffer) -> Option<(usize, usize)> {
    entry
        .trades
        .iter()
        .enumerate()
        .find_map(|(trade_idx, trade)| {
            let trade_log_index = trade.get_log_index();
            entry
                .syncs
                .iter()
                .enumerate()
                .filter(|(_, sync)| sync.log_index < trade_log_index)
                .max_by_key(|(_, sync)| sync.log_index)
                .map(|(sync_idx, _)| (trade_idx, sync_idx))
        })
}

const REQUEST_TIMEOUT_SECS: u64 = 10;

pub async fn fetch_token_metadata(token_uri: &str) -> Result<TokenMetadata> {
    let url = if !token_uri.starts_with("http://") && !token_uri.starts_with("https://") {
        format!("https://{}", token_uri)
    } else {
        token_uri.to_string()
    };

    // URI 검증

    if !(url.starts_with("https://storage.nadapp.net/") && url.ends_with(".json")) {
        return Err(anyhow::anyhow!("Invalid token URI: {}", url));
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()
        .context("HTTP 클라이언트 생성 실패")?;

    // 첫 번째 시도 시간 측정
    let fetch_start = std::time::Instant::now();
    match fetch_metadata(&client, &url).await {
        Ok(metadata) => {
            let fetch_duration = fetch_start.elapsed();
            info!(
                "🕒 fetch_metadata 성공: {}ms (URL: {})",
                fetch_duration.as_millis(),
                url
            );
            Ok(metadata)
        }
        Err(err) => {
            let fetch_duration = fetch_start.elapsed();
            // 타임아웃 오류일 경우만 재시도
            if err.to_string().to_lowercase().contains("timeout") {
                warn!(
                    "🕒 fetch_metadata 타임아웃 발생: {}ms, 재시도 중: {}",
                    fetch_duration.as_millis(),
                    url
                );
                time::sleep(Duration::from_millis(500)).await;

                // 재시도 시간 측정
                let retry_start = std::time::Instant::now();
                match fetch_metadata(&client, &url).await {
                    Ok(metadata) => {
                        let retry_duration = retry_start.elapsed();
                        info!(
                            "🕒 fetch_metadata 재시도 성공: {}ms (URL: {})",
                            retry_duration.as_millis(),
                            url
                        );
                        Ok(metadata)
                    }
                    Err(retry_err) => {
                        let retry_duration = retry_start.elapsed();
                        warn!(
                            "🕒 fetch_metadata 재시도 실패: {}ms (URL: {})",
                            retry_duration.as_millis(),
                            url
                        );
                        Err(retry_err)
                    }
                }
            } else {
                warn!(
                    "🕒 fetch_metadata 실패: {}ms (URL: {})",
                    fetch_duration.as_millis(),
                    url
                );
                Err(err)
            }
        }
    }
}

async fn fetch_metadata(client: &reqwest::Client, url: &str) -> Result<TokenMetadata> {
    let mut metadata = TokenMetadata::default();
    let max_retries = 5;
    let mut retries = 0;
    loop {
        let response = match client
            .get(url)
            .header("Accept", "application/json")
            .header("User-Agent", "Nad-Observer/1.0") // User-Agent 추가
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                sleep(Duration::from_millis(300)).await;
                warn!(
                    "Failed to fetch metadata: error sending request for url ({}) err :{:?}",
                    url,
                    err.source()
                );
                retries += 1;
                if retries >= max_retries {
                    return Err(anyhow::anyhow!(
                        "Failed to fetch metadata after {} retries, err : {}",
                        retries,
                        err
                    ));
                }
                continue;
            }
        };

        if !response.status().is_success() {
            let err_msg = format!("Invalid HTTP status: {}: {}", response.status(), url);
            if response.status().as_u16() == 404 {
                error_log!("{}", err_msg);
                return Err(anyhow::anyhow!(err_msg));
            }

            continue;
        }

        let text = response
            .text()
            .await
            .context(format!("응답 바디 읽기 실패: {}", url))?;

        if text.trim().is_empty() {
            let err_msg = format!("빈 응답: {}", url);
            error_log!("{}", err_msg);
            return Err(anyhow::anyhow!(err_msg));
        }

        let json = serde_json::from_str::<serde_json::Value>(&text)
            .context(format!("Invalid Json: {}", url))?;

        let image_uri = json
            .get("image_uri")
            .and_then(|v| v.as_str())
            .or_else(|| json.get("image").and_then(|v| v.as_str()))
            .ok_or_else(|| anyhow::anyhow!("메타데이터에서 이미지 URI를 찾을 수 없음"))?;

        if image_uri.is_empty() {
            let err_msg = "메타데이터에서 이미지 URI가 비어 있음";
            error_log!("{}", err_msg);
            return Err(anyhow::anyhow!(err_msg));
        }

        metadata.image_uri = image_uri.to_string();

        for (field, target) in [
            ("description", &mut metadata.description),
            ("website", &mut metadata.website),
            ("twitter", &mut metadata.twitter),
            ("telegram", &mut metadata.telegram),
        ] {
            if let Some(value) = json.get(field).and_then(|v| v.as_str()) {
                *target = Some(value.to_string());
            }
        }
        break;
    }

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;

    fn buy_at(log_index: u64) -> CurveEventType {
        CurveEventType::Buy(Buy {
            account_id: "account".to_string(),
            to: None,
            amount_in: BigDecimal::from(1),
            amount_out: BigDecimal::from(1),
            token: "token".to_string(),
            market: "market".to_string(),
            market_type: MarketType::Curve,
            transaction_hash: "tx".to_string(),
            block_number: 1,
            block_timestamp: 1,
            log_index,
            transaction_index: 7,
        })
    }

    fn sync_at(log_index: u64) -> CurveSync {
        CurveSync {
            token: "token".to_string(),
            reserve_native_amount: BigDecimal::from(1),
            reserve_token_amount: BigDecimal::from(1),
            virtual_native_amount: BigDecimal::from(1),
            virtual_token_amount: BigDecimal::from(1),
            price: BigDecimal::from(1),
            block_number: 1,
            block_timestamp: 1,
            transaction_hash: "tx".to_string(),
            log_index,
            transaction_index: 7,
        }
    }

    #[test]
    fn v2_curve_pair_uses_closest_previous_sync() {
        let entry = TransactionEventBuffer {
            trades: vec![buy_at(15)],
            syncs: vec![sync_at(10), sync_at(14), sync_at(16)],
            timestamp: time::Instant::now(),
        };

        assert_eq!(find_ready_curve_pair(&entry), Some((0, 1)));
    }

    #[test]
    fn v2_curve_pair_ignores_future_sync() {
        let entry = TransactionEventBuffer {
            trades: vec![buy_at(15)],
            syncs: vec![sync_at(16)],
            timestamp: time::Instant::now(),
        };

        assert_eq!(find_ready_curve_pair(&entry), None);
    }

    /// EventBufferKey가 token을 키에 포함하므로 같은 (tx_hash, tx_idx) 라도
    /// 토큰이 다르면 서로 다른 entry로 분리돼야 한다.
    #[test]
    fn event_buffer_key_separates_tokens_in_same_tx() {
        use std::collections::HashMap;

        let key_a = EventBufferKey {
            transaction_hash: "0xabc".to_string(),
            transaction_index: 5,
            token: "0xTokenA".to_string(),
        };
        let key_b = EventBufferKey {
            transaction_hash: "0xabc".to_string(),
            transaction_index: 5,
            token: "0xTokenB".to_string(),
        };

        assert_ne!(key_a, key_b);

        let mut map: HashMap<EventBufferKey, &str> = HashMap::new();
        map.insert(key_a.clone(), "A's bucket");
        map.insert(key_b.clone(), "B's bucket");

        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&key_a), Some(&"A's bucket"));
        assert_eq!(map.get(&key_b), Some(&"B's bucket"));
    }

    /// 같은 tx의 buffer에 token A의 Sell과 token B의 Sync가 들어와도
    /// 서로 다른 entry로 분리되어 cross-token 페어링이 일어나지 않아야 한다.
    /// 회귀: 키에 token이 없던 시절엔 Sell_A + Sync_B 로 페어돼 B의 chart에
    /// A의 거래량이 박혔다.
    #[tokio::test]
    async fn cross_token_events_dont_cross_pair() {
        use dashmap::DashMap;
        use std::sync::Arc;

        let buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>> =
            Arc::new(DashMap::new());

        // token B의 Sync (log_idx 5) — token A의 Sell이 들어와도 페어 가능했었음
        let sync_b = CurveSync {
            token: "0xTokenB".to_string(),
            reserve_native_amount: BigDecimal::from(1),
            reserve_token_amount: BigDecimal::from(1),
            virtual_native_amount: BigDecimal::from(1),
            virtual_token_amount: BigDecimal::from(1),
            price: BigDecimal::from(1),
            block_number: 1,
            block_timestamp: 1,
            transaction_hash: "0xabc".to_string(),
            log_index: 5,
            transaction_index: 5,
        };
        let sync_b_key = EventBufferKey {
            transaction_hash: "0xabc".to_string(),
            transaction_index: 5,
            token: "0xTokenB".to_string(),
        };
        process_sync_event(
            CurveEventType::CurveSync(sync_b.clone()),
            sync_b,
            sync_b_key.clone(),
            buffer.clone(),
        )
        .await
        .unwrap();

        // token A의 Sell (log_idx 6) — Sync_B(5)보다 늦으니 log_idx 거리상으론
        // 매칭 후보였음. 키에 token이 들어가면 entry가 분리돼 매칭 불가.
        let sell_a = CurveEventType::Sell(Sell {
            account_id: "a-actor".to_string(),
            to: None,
            amount_in: BigDecimal::from(1000),
            amount_out: BigDecimal::from(500),
            token: "0xTokenA".to_string(),
            market: "curve".to_string(),
            market_type: MarketType::Curve,
            transaction_hash: "0xabc".to_string(),
            block_number: 1,
            block_timestamp: 1,
            log_index: 6,
            transaction_index: 5,
        });
        let sell_a_key = EventBufferKey {
            transaction_hash: "0xabc".to_string(),
            transaction_index: 5,
            token: "0xTokenA".to_string(),
        };
        process_trade_event(sell_a, sell_a_key.clone(), buffer.clone())
            .await
            .unwrap();

        // 두 토큰이 서로 다른 entry에 들어가야 함
        assert_eq!(
            buffer.len(),
            2,
            "토큰별로 별도 entry가 만들어져야 한다 (실제 {}개)",
            buffer.len()
        );

        // A entry: Sell만 있고 Sync가 없으니 페어 매칭 안 됨 → entry 유지
        let a_entry = buffer
            .get(&sell_a_key)
            .expect("A의 entry가 살아있어야 한다 (페어 매칭 안 됐으니까)");
        assert_eq!(a_entry.trades.len(), 1);
        assert_eq!(a_entry.syncs.len(), 0);
        drop(a_entry);

        // B entry: Sync만 있고 Trade 없음 → entry 유지
        let b_entry = buffer
            .get(&sync_b_key)
            .expect("B의 entry가 살아있어야 한다 (페어 매칭 안 됐으니까)");
        assert_eq!(b_entry.trades.len(), 0);
        assert_eq!(b_entry.syncs.len(), 1);
        assert_eq!(b_entry.syncs[0].token, "0xTokenB");
        drop(b_entry);
    }
}
