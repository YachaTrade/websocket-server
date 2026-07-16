use std::time::{Duration, Instant};

use crate::{
    event::{
        chart::CHART_EVENT_PRODUCER, market::MARKET_EVENT_PRODUCER,
        metrics::METRICS_EVENT_PRODUCER, order::ORDER_EVENT_PRODUCER, swap::SWAP_EVENT_PRODUCER,
    },
    types::stream::{Buy, DexBurn, DexChartUpdate, DexEventType, DexMint, DexSync, Sell},
};

use anyhow::Result;

use crate::error_log;
use tracing::{info, instrument};

/// DEX 이벤트 수신 및 처리 (enum으로 최적화)
#[instrument(skip(event))]
pub async fn receive_dex_event(event: DexEventType) -> Result<()> {
    let start_time = Instant::now();

    // 패턴 매칭으로 이벤트 처리 (HashMap 간접 호출 제거)
    match event {
        DexEventType::Buy(e) => handle_buy_event(e).await?,
        DexEventType::Sell(e) => handle_sell_event(e).await?,
        DexEventType::DexSync(e) => handle_sync_event(e).await?,
        DexEventType::DexMint(e) => handle_mint_event(e).await?,
        DexEventType::DexBurn(e) => handle_burn_event(e).await?,
        DexEventType::DexChartUpdate(e) => handle_chart_update_event(e).await?,
    }

    tracing::debug!("Single dex event processed in {:?}", start_time.elapsed());
    Ok(())
}

#[instrument(skip(buy))]
pub async fn handle_buy_event(buy: Buy) -> Result<()> {
    let time = Instant::now();

    // 이벤트 프로듀서 초기화 - 에러 처리로 변경
    let order_event_producer = match ORDER_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("ORDER_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("ORDER_EVENT_PRODUCER not initialized"));
        }
    };

    let swap_event_producer = match SWAP_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("TOKEN_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("TOKEN_EVENT_PRODUCER not initialized"));
        }
    };

    let chart_event_producer = match CHART_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("CHART_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("CHART_EVENT_PRODUCER not initialized"));
        }
    };

    let metrics_event_producer = match METRICS_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("METRICS_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("METRICS_EVENT_PRODUCER not initialized"));
        }
    };

    let market_event_producer = match MARKET_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("MARKET_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("MARKET_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 한 번만 감싸서 zero-copy 공유 (clone 대신 포인터만 복사)
    let buy_event = std::sync::Arc::new(DexEventType::Buy(buy));

    // 병렬로 논블로킹 이벤트 전송 (Arc::clone은 포인터만 복사)
    let (order_result, swap, chart_result, metrics_result, market_result) = tokio::join!(
        async {
            match order_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => {
                    tracing::debug!("[Channel] Order-DEX: message sent ✅");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("[Channel] ⚠️ Order-DEX channel full, skipping dex buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("[Channel] ❌ Order-DEX channel closed!");
                    Err(anyhow::anyhow!("Order producer channel closed"))
                }
            }
        },
        async {
            match swap_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => {
                    tracing::debug!("[Channel] Token-DEX: message sent ✅");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("[Channel] ⚠️ Token-DEX channel full, skipping dex buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::error!("[Channel] ❌ Token-DEX channel closed!");
                    Err(anyhow::anyhow!("Token producer channel closed"))
                }
            }
        },
        async {
            match chart_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Chart producer channel full, skipping dex buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Chart producer channel closed"))
                }
            }
        },
        async {
            match metrics_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Metrics producer channel full, skipping dex buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Metrics producer channel closed"))
                }
            }
        },
        async {
            match market_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Market producer channel full, skipping dex buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Market producer channel closed"))
                }
            }
        }
    );

    // 채널 닫힘 에러만 체크 (가득참은 무시)
    if let Err(e) = order_result {
        error_log!("Order producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = swap {
        error_log!("Token producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = chart_result {
        error_log!("Chart producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = metrics_result {
        error_log!("Metrics producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = market_result {
        error_log!("Market producer channel error: {}", e);
        return Err(e);
    }

    // Arc 안의 데이터 참조
    if let DexEventType::Buy(ref buy_ref) = *buy_event {
        info!(
            "Dex Buy event handled successfully: token={} in {:?} ms",
            buy_ref.token,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(sell))]
pub async fn handle_sell_event(sell: Sell) -> Result<()> {
    let time = Instant::now();

    // 이벤트 프로듀서 초기화 - 에러 처리로 변경
    let order_event_producer = match ORDER_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("ORDER_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("ORDER_EVENT_PRODUCER not initialized"));
        }
    };

    let swap_event_producer = match SWAP_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("SWAP_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("SWAP_EVENT_PRODUCER not initialized"));
        }
    };

    let chart_event_producer = match CHART_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("CHART_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("CHART_EVENT_PRODUCER not initialized"));
        }
    };

    let metrics_event_producer = match METRICS_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("METRICS_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("METRICS_EVENT_PRODUCER not initialized"));
        }
    };

    let market_event_producer = match MARKET_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("MARKET_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("MARKET_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 한 번만 감싸서 zero-copy 공유
    let sell_event = std::sync::Arc::new(DexEventType::Sell(sell));

    // 병렬로 논블로킹 이벤트 전송 (Arc::clone은 포인터만 복사)
    let (order_result, swap, chart_result, metrics_result, market_result) = tokio::join!(
        async {
            match order_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Order producer channel full, skipping dex sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Order producer channel closed"))
                }
            }
        },
        async {
            match swap_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Token producer channel full, skipping dex sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Token producer channel closed"))
                }
            }
        },
        async {
            match chart_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Chart producer channel full, skipping dex sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Chart producer channel closed"))
                }
            }
        },
        async {
            match metrics_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Metrics producer channel full, skipping dex sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Metrics producer channel closed"))
                }
            }
        },
        async {
            match market_event_producer
                .dex_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Market producer channel full, skipping dex sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Market producer channel closed"))
                }
            }
        }
    );

    // 채널 닫힘 에러만 체크 (가득참은 무시)
    if let Err(e) = order_result {
        error_log!("Order producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = swap {
        error_log!("Swap producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = chart_result {
        error_log!("Chart producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = metrics_result {
        error_log!("Metrics producer channel error: {}", e);
        return Err(e);
    }
    if let Err(e) = market_result {
        error_log!("Market producer channel error: {}", e);
        return Err(e);
    }

    // Arc 안의 데이터 참조
    if let DexEventType::Sell(ref sell_ref) = *sell_event {
        info!(
            "Dex Sell event handled successfully: token={} in {:?} ms",
            sell_ref.token,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(sync))]
async fn handle_sync_event(sync: DexSync) -> Result<()> {
    // Create all controllers at once
    let time = Instant::now();

    // 이벤트 프로듀서 초기화 - 에러 처리로 변경
    let chart_event_producer = match CHART_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("CHART_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("CHART_EVENT_PRODUCER not initialized"));
        }
    };

    let market_event_producer = match MARKET_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("MARKET_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("MARKET_EVENT_PRODUCER not initialized"));
        }
    };

    let metrics_event_producer = match METRICS_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("METRICS_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("METRICS_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 한 번만 감싸서 zero-copy 공유
    let sync_event = std::sync::Arc::new(DexEventType::DexSync(sync));

    // 채널 에러 처리 (500ms 타임아웃 적용)
    let channel_timeout = Duration::from_millis(500);
    let chart_task = tokio::time::timeout(
        channel_timeout,
        chart_event_producer
            .dex_event_sender
            .send(std::sync::Arc::clone(&sync_event)),
    );

    let market_task = tokio::time::timeout(
        channel_timeout,
        market_event_producer
            .dex_event_sender
            .send(std::sync::Arc::clone(&sync_event)),
    );

    let metrics_task = tokio::time::timeout(
        channel_timeout,
        metrics_event_producer
            .dex_event_sender
            .send(std::sync::Arc::clone(&sync_event)),
    );

    // 병렬 전송
    let (chart_result, market_result, metrics_result) =
        tokio::join!(chart_task, market_task, metrics_task);

    match chart_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send sync event to chart producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send sync event to chart producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending sync event to chart producer");
            return Err(anyhow::anyhow!(
                "Timeout sending sync event to chart producer"
            ));
        }
    }

    match market_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send sync event to market producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send sync event to market producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending sync event to market producer");
            return Err(anyhow::anyhow!(
                "Timeout sending sync event to market producer"
            ));
        }
    }

    match metrics_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send sync event to metrics producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send sync event to metrics producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending sync event to metrics producer");
            return Err(anyhow::anyhow!(
                "Timeout sending sync event to metrics producer"
            ));
        }
    }

    // Arc 안의 데이터 참조
    if let DexEventType::DexSync(ref sync_ref) = *sync_event {
        info!(
            "Dex sync event handled successfully: token={} in {:?} ms",
            sync_ref.token,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(mint))]
async fn handle_mint_event(mint: DexMint) -> Result<()> {
    // Create all controllers at once
    let time = Instant::now();

    // 이벤트 프로듀서 초기화 - 에러 처리로 변경
    let swap_event_producer = match SWAP_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("TOKEN_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("TOKEN_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 zero-copy 공유
    let mint_event = std::sync::Arc::new(DexEventType::DexMint(mint));

    // 채널 에러 처리 (500ms 타임아웃 적용)
    let channel_timeout = Duration::from_millis(500);
    match tokio::time::timeout(
        channel_timeout,
        swap_event_producer.dex_event_sender.send(mint_event),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send mint event to token producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send mint event to token producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending mint event to token producer");
            return Err(anyhow::anyhow!(
                "Timeout sending mint event to token producer"
            ));
        }
    }

    // Arc에서 참조 추출 (이미 send로 move됨)
    info!(
        "Dex mint event handled successfully in {:?} ms",
        time.elapsed()
    );
    Ok(())
}

#[instrument(skip(burn))]
async fn handle_burn_event(burn: DexBurn) -> Result<()> {
    // Create all controllers at once
    let time = Instant::now();

    // 이벤트 프로듀서 초기화 - 에러 처리로 변경
    let swap_event_producer = match SWAP_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("SWAP_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("SWAP_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 zero-copy 공유
    let burn_event = std::sync::Arc::new(DexEventType::DexBurn(burn));

    // 채널 에러 처리 (500ms 타임아웃 적용)
    let channel_timeout = Duration::from_millis(500);
    match tokio::time::timeout(
        channel_timeout,
        swap_event_producer.dex_event_sender.send(burn_event),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send burn event to token producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send burn event to token producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending burn event to token producer");
            return Err(anyhow::anyhow!(
                "Timeout sending burn event to token producer"
            ));
        }
    }

    // Arc에서 참조 추출 (이미 send로 move됨)
    info!(
        "Dex burn event handled successfully in {:?} ms",
        time.elapsed()
    );
    Ok(())
}

/// DexChartUpdate 이벤트 처리 - chart producer에만 전송
#[instrument(skip(chart_update))]
pub async fn handle_chart_update_event(chart_update: DexChartUpdate) -> Result<()> {
    let time = Instant::now();

    // Chart producer에만 전송
    let chart_event_producer = match CHART_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("CHART_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("CHART_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 zero-copy 공유
    let chart_update_event = std::sync::Arc::new(DexEventType::DexChartUpdate(chart_update));

    // Chart producer에 타임아웃과 함께 전송
    let chart_task = tokio::time::timeout(
        Duration::from_millis(500),
        chart_event_producer
            .dex_event_sender
            .send(chart_update_event),
    );

    match chart_task.await {
        Ok(Ok(_)) => {
            info!(
                "DexChartUpdate sent to chart producer in {:?}",
                time.elapsed()
            );
        }
        Ok(Err(e)) => {
            error_log!("Failed to send DexChartUpdate to chart producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send DexChartUpdate to chart producer"
            ));
        }
        Err(_) => {
            error_log!("Timeout sending DexChartUpdate to chart producer");
            return Err(anyhow::anyhow!(
                "Timeout sending DexChartUpdate to chart producer"
            ));
        }
    }

    Ok(())
}
