use std::time::{Duration, Instant};

use crate::{
    event::{
        chart::CHART_EVENT_PRODUCER, market::MARKET_EVENT_PRODUCER,
        metrics::METRICS_EVENT_PRODUCER, order::ORDER_EVENT_PRODUCER, swap::SWAP_EVENT_PRODUCER,
    },
    types::stream::{
        Buy, CreateCurve, CurveChartUpdate, CurveEventType, CurveSync, Graduate, Sell,
    },
};

use anyhow::Result;

use crate::error_log;
use tracing::{info, instrument};

/// Curve 이벤트 수신 및 처리 (enum으로 최적화)
#[instrument(skip(event))]
pub async fn receive_curve_event(event: CurveEventType) -> Result<()> {
    let start_time = Instant::now();

    // 패턴 매칭으로 이벤트 처리 (HashMap 간접 호출 제거)
    match event {
        CurveEventType::CreateCurve(e) => handle_create_curve_event(e).await?,
        CurveEventType::Buy(e) => handle_buy_event(e).await?,
        CurveEventType::Sell(e) => handle_sell_event(e).await?,
        CurveEventType::CurveSync(e) => handle_sync_event(e).await?,
        CurveEventType::Graduate(e) => handle_graduate_event(e).await?,
        CurveEventType::CurveChartUpdate(e) => handle_chart_update_event(e).await?,
    }

    tracing::debug!("Single Curve event processed in {:?}", start_time.elapsed());
    Ok(())
}

#[instrument(skip(create_curve))]
pub async fn handle_create_curve_event(create_curve: CreateCurve) -> Result<()> {
    let time = Instant::now();

    // 모든 이벤트 프로듀서 초기화 - 에러 처리로 변경
    let order_event_producer = match ORDER_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("ORDER_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("ORDER_EVENT_PRODUCER not initialized"));
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

    // Arc로 zero-copy 공유
    let create_event = std::sync::Arc::new(CurveEventType::CreateCurve(create_curve));

    // 병렬로 논블로킹 이벤트 전송 (Arc::clone은 포인터만 복사)
    let (order_result, chart_result, metrics_result, market_result) = tokio::join!(
        async {
            match order_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&create_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Order producer channel full, skipping Curve create curve event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Order producer channel closed"))
                }
            }
        },
        async {
            match chart_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&create_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Chart producer channel full, skipping Curve create curve event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Chart producer channel closed"))
                }
            }
        },
        async {
            match metrics_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&create_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Metrics producer channel full, skipping Curve create curve event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Metrics producer channel closed"))
                }
            }
        },
        async {
            match market_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&create_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Market producer channel full, skipping Curve create curve event");
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
    if let CurveEventType::CreateCurve(ref create_curve_ref) = *create_event {
        info!(
            "Curve Create Curve event handled successfully: token={} in {:?} ms",
            create_curve_ref.token,
            time.elapsed()
        );
    }

    Ok(())
}

#[instrument(skip(buy))]
pub async fn handle_buy_event(buy: Buy) -> Result<()> {
    let time = Instant::now();

    // Arc로 zero-copy 공유
    let buy_event = std::sync::Arc::new(CurveEventType::Buy(buy));

    // 모든 이벤트 프로듀서 초기화 - 에러 처리로 변경
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

    // 병렬로 논블로킹 이벤트 전송 (Chart는 stream에서 CurveChartUpdate로 처리)
    let (order_result, swap_result, metrics_result, market_result) = tokio::join!(
        async {
            match order_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Order producer channel full, skipping Curve buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Order producer channel closed"))
                }
            }
        },
        async {
            match swap_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("SWAP producer channel full, skipping Curve buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("SWAP producer channel closed"))
                }
            }
        },
        async {
            match metrics_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Metrics producer channel full, skipping Curve buy event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Metrics producer channel closed"))
                }
            }
        },
        async {
            match market_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&buy_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Market producer channel full, skipping Curve buy event");
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
    if let Err(e) = swap_result {
        error_log!("Token producer channel error: {}", e);
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
    if let CurveEventType::Buy(ref buy_ref) = *buy_event {
        info!(
            "Curve Buy event handled successfully: token={} in {:?} ms",
            buy_ref.token,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(sell))]
pub async fn handle_sell_event(sell: Sell) -> Result<()> {
    let time = Instant::now();

    // Arc로 zero-copy 공유
    let sell_event = std::sync::Arc::new(CurveEventType::Sell(sell));

    // 모든 이벤트 프로듀서 초기화 - 에러 처리로 변경
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

    // 병렬로 논블로킹 이벤트 전송 (Chart는 stream에서 CurveChartUpdate로 처리)
    let (order_result, swap_result, metrics_result, market_result) = tokio::join!(
        async {
            match order_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Order producer channel full, skipping Curve sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Order producer channel closed"))
                }
            }
        },
        async {
            match swap_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Token producer channel full, skipping Curve sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Token producer channel closed"))
                }
            }
        },
        async {
            match metrics_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Metrics producer channel full, skipping Curve sell event");
                    Ok(())
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(anyhow::anyhow!("Metrics producer channel closed"))
                }
            }
        },
        async {
            match market_event_producer
                .curve_event_sender
                .try_send(std::sync::Arc::clone(&sell_event))
            {
                Ok(_) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("Market producer channel full, skipping Curve sell event");
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
    if let Err(e) = swap_result {
        error_log!("Token producer channel error: {}", e);
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
    if let CurveEventType::Sell(ref sell_ref) = *sell_event {
        info!(
            "Curve Sell event handled successfully: token={} in {:?} ms",
            sell_ref.token,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(sync))]
pub async fn handle_sync_event(sync: CurveSync) -> Result<()> {
    let time = Instant::now();

    // 모든 이벤트 프로듀서 초기화 - 에러 처리로 변경 (Chart는 stream에서 CurveChartUpdate로 처리)
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

    // Arc로 zero-copy 공유
    let sync_event = std::sync::Arc::new(CurveEventType::CurveSync(sync));

    // 기본 이벤트 전송 작업 생성 (500ms 타임아웃 적용)
    let channel_timeout = Duration::from_millis(500);
    let metrics_task = tokio::time::timeout(
        channel_timeout,
        metrics_event_producer
            .curve_event_sender
            .send(std::sync::Arc::clone(&sync_event)), // clone 제거
    );

    let market_task = tokio::time::timeout(
        channel_timeout,
        market_event_producer
            .curve_event_sender
            .send(std::sync::Arc::clone(&sync_event)), // clone 제거
    );

    // 병렬 전송
    let (metrics_result, market_result) = tokio::join!(metrics_task, market_task);

    match metrics_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send Curve sync event to metrics producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send Curve sync event to metrics producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending Curve sync event to metrics producer");
            return Err(anyhow::anyhow!(
                "Timeout sending Curve sync event to metrics producer"
            ));
        }
    }

    match market_result {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send Curve sync event to market producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send Curve sync event to market producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending Curve sync event to market producer");
            return Err(anyhow::anyhow!(
                "Timeout sending Curve sync event to market producer"
            ));
        }
    }

    // Arc 안의 데이터 참조
    if let CurveEventType::CurveSync(ref sync_ref) = *sync_event {
        info!(
            "Curve Sync event handled successfully: {:?} in {:?} ms",
            sync_ref,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(chart_update))]
pub async fn handle_chart_update_event(chart_update: CurveChartUpdate) -> Result<()> {
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
    let chart_event = std::sync::Arc::new(CurveEventType::CurveChartUpdate(chart_update));

    let channel_timeout = Duration::from_millis(500);
    let chart_task = tokio::time::timeout(
        channel_timeout,
        chart_event_producer
            .curve_event_sender
            .send(chart_event.clone()), // Arc clone (포인터만 복사)
    );

    match chart_task.await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            error_log!("Failed to send Curve chart update to chart producer: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to send Curve chart update to chart producer: channel closed"
            ));
        }
        Err(_) => {
            error_log!("Timeout (500ms) sending Curve chart update to chart producer");
            return Err(anyhow::anyhow!(
                "Timeout sending Curve chart update to chart producer"
            ));
        }
    }

    // Arc 안의 데이터 참조
    if let CurveEventType::CurveChartUpdate(ref chart_ref) = *chart_event {
        info!(
            "Curve CurveChartUpdate handled successfully: tx={}, token={} in {:?} ms",
            chart_ref.transaction_hash,
            chart_ref.sync.token,
            time.elapsed()
        );
    }
    Ok(())
}

#[instrument(skip(graduate))]
pub async fn handle_graduate_event(graduate: Graduate) -> Result<()> {
    let time = Instant::now();

    // Market Producer에 Graduate 이벤트 전송
    use crate::event::market::MARKET_EVENT_PRODUCER;

    let market_event_producer = match MARKET_EVENT_PRODUCER.get() {
        Some(producer) => producer,
        None => {
            error_log!("MARKET_EVENT_PRODUCER not initialized");
            return Err(anyhow::anyhow!("MARKET_EVENT_PRODUCER not initialized"));
        }
    };

    // Arc로 zero-copy 공유
    let graduate_event = std::sync::Arc::new(CurveEventType::Graduate(graduate));

    // Market Producer로 전송 (try_send 사용)
    match market_event_producer
        .curve_event_sender
        .try_send(graduate_event.clone())  // Arc clone (포인터만 복사)
    {
        Ok(_) => {
            if let CurveEventType::Graduate(ref grad_ref) = *graduate_event {
                info!(
                    "Curve Graduate event sent to market producer: token={}, pool={}",
                    grad_ref.token, grad_ref.pool
                );
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            if let CurveEventType::Graduate(ref grad_ref) = *graduate_event {
                tracing::warn!(
                    "Market producer channel full, skipping Curve graduate event for token {}",
                    grad_ref.token
                );
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            return Err(anyhow::anyhow!("Market producer channel closed"));
        }
    }

    if let CurveEventType::Graduate(ref grad_ref) = *graduate_event {
        info!(
            "Curve Graduate event handled successfully: token={}, pool={} in {:?}",
            grad_ref.token,
            grad_ref.pool,
            time.elapsed()
        );
    }
    Ok(())
}
