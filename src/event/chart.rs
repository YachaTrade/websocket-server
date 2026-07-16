use crate::{
    error_log,
    types::chart::{
        Chart, ChartInterval, ChartResponse, ChartUpdateContext, ChartUpdateOrder, PriceType,
    },
};
use anyhow::Result;
use bigdecimal::{BigDecimal, RoundingMode};
use dashmap::DashMap;
use futures_util::future::try_join_all;
use std::{fmt, str::FromStr, sync::Arc};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info};

use crate::{
    config::CHANNEL_SIZE,
    db::cache::CacheManager,
    metrics::{
        monitored_broadcast_channel, monitored_channel, MonitoredBroadcastReceiver,
        MonitoredBroadcastSender, MonitoredReceiver, MonitoredSender,
    },
    types::stream::{CreateCurve, CurveEventType, DexEventType},
};

pub static CHART_EVENT_PRODUCER: OnceCell<Arc<ChartEventProducer>> = OnceCell::const_new();

/// 차트 키 (token_id + interval + price_type로 구성)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ChartKey {
    pub token_id: String,
    pub interval: String,
    pub price_type: PriceType,
}

pub struct ChartEventProducer {
    /// Sender channel for DEX events (Arc로 zero-copy 공유)
    pub dex_event_sender: MonitoredSender<Arc<DexEventType>>,

    /// Receiver channel for DEX events (Arc<Mutex>로 감싸서 thread-safe하게 만듦)
    pub dex_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<DexEventType>>>>,

    /// Sender channel for Curve events (Arc로 zero-copy 공유)
    pub curve_event_sender: MonitoredSender<Arc<CurveEventType>>,

    /// Receiver channel for Curve events (Arc<Mutex>로 감싸서 thread-safe하게 만듦)
    pub curve_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<CurveEventType>>>>,

    /// WebSocket 이벤트 전송용 (ChartResponse로 내려줌)
    pub event_sender: Arc<DashMap<ChartKey, MonitoredBroadcastSender<ChartResponse>>>,
}

impl fmt::Debug for ChartEventProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChartEventProducer").finish()
    }
}

impl ChartEventProducer {
    /// 단일 interval에 대한 차트 업데이트 헬퍼 함수 (tokio::join! 사용 위함)
    ///
    /// # Arguments
    /// * `cache_manager` - 캐시 매니저
    /// * `ctx` - 차트 업데이트 컨텍스트 (공통 파라미터)
    /// * `interval` - 차트 인터벌
    async fn update_chart_for_interval(
        cache_manager: &CacheManager,
        ctx: &ChartUpdateContext<'_>,
        interval: ChartInterval,
    ) -> Result<(ChartInterval, Chart)> {
        let candle_start = interval.get_candle_start(ctx.timestamp);
        let params = ctx.to_params(interval.as_str(), candle_start);
        let chart = cache_manager.update_chart_atomic(&params).await?;
        Ok((interval, chart))
    }

    async fn update_all_intervals(
        cache_manager: &CacheManager,
        ctx: &ChartUpdateContext<'_>,
    ) -> Result<Vec<(ChartInterval, Chart)>> {
        let updates = ChartInterval::all()
            .into_iter()
            .map(|interval| Self::update_chart_for_interval(cache_manager, ctx, interval));

        try_join_all(updates).await
    }

    pub fn init() -> Result<()> {
        let (dex_event_sender, dex_event_receiver) = monitored_channel("Chart-DEX", *CHANNEL_SIZE);
        let (curve_event_sender, curve_event_receiver) =
            monitored_channel("Chart-Curve", *CHANNEL_SIZE);
        let event_sender = Arc::new(DashMap::new());

        let producer = ChartEventProducer {
            dex_event_sender,
            dex_event_receiver: Arc::new(Mutex::new(dex_event_receiver)),
            curve_event_sender,
            curve_event_receiver: Arc::new(Mutex::new(curve_event_receiver)),
            event_sender,
        };

        CHART_EVENT_PRODUCER
            .set(Arc::new(producer))
            .expect("Failed to set CHART_EVENT_PRODUCER");

        // 주기적 정리 태스크 시작
        Self::start_cleanup_task()?;

        Ok(())
    }

    pub async fn main() -> Result<()> {
        // 프로듀서 인스턴스를 가져옴
        let producer = CHART_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("CHART_EVENT_PRODUCER not initialized"))?
            .clone();

        // Curve 이벤트 수신 태스크
        let producer_curve = producer.clone();
        tokio::spawn(async move {
            if let Err(e) = producer_curve.receive_curve_event().await {
                error_log!("Error in receive_curve_event: {}", e);
            }
        });

        // DEX 이벤트 수신 태스크
        tokio::spawn(async move {
            if let Err(e) = producer.receive_dex_event().await {
                error_log!("Error in receive_dex_event: {}", e);
            }
        });

        tracing::info!(
            "ChartEventProducer main started: receive_curve_event and receive_dex_event tasks spawned"
        );

        Ok(())
    }

    // Curve 이벤트 수신 및 처리
    pub async fn receive_curve_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        while let Some(event) = self.curve_event_receiver.lock().await.recv().await {
            info!("ChartEvent receive curve event: {:?}", event);

            // CurveChartUpdate: Sync 먼저 처리 → Trade 처리
            if let Some(chart_update) = event.is_chart_update() {
                if let Err(e) = self
                    .process_chart_update_event(chart_update, &cache_manager)
                    .await
                {
                    error_log!("Error processing chart update event: {}", e);
                }
            }
            // CreateCurve: 초기 가격 설정
            else if let Some(create_curve) = event.is_create_curve() {
                if let Err(e) = self
                    .process_create_curve_event(create_curve, &cache_manager)
                    .await
                {
                    error_log!("Error processing create curve event: {}", e);
                }
            }
            // 그 외 이벤트는 무시 (Buy/Sell/Sync는 CurveChartUpdate로 처리됨)
            else {
                debug!("ChartEventProducer: Ignoring non-chart event type");
            }
        }
        Ok(())
    }

    // DEX 이벤트 수신 및 처리
    pub async fn receive_dex_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        while let Some(event) = self.dex_event_receiver.lock().await.recv().await {
            // DexChartUpdate만 처리 (Buy/Sell/Sync는 DexChartUpdate로 번들되어 들어옴)
            if let Some(chart_update) = event.is_dex_chart_update() {
                if let Err(e) = self
                    .process_dex_chart_update_event(chart_update, &cache_manager)
                    .await
                {
                    error_log!("Error processing dex chart update event: {}", e);
                }
            } else {
                debug!("ChartEventProducer receive_dex_event: Ignoring non-chart event");
            }
        }
        Ok(())
    }

    // CurveChartUpdate 이벤트 처리: Atomic 업데이트
    async fn process_chart_update_event(
        &self,
        chart_update: &crate::types::stream::CurveChartUpdate,
        cache_manager: &CacheManager,
    ) -> Result<()> {
        tracing::info!(
            "🎯 Processing CurveChartUpdate - Token: {}, has_trade: {}, Timestamp: {}",
            chart_update.sync.token,
            chart_update.trade.is_some(),
            chart_update.sync.block_timestamp
        );

        // 가격 계산
        let price =
            &chart_update.sync.virtual_native_amount / &chart_update.sync.virtual_token_amount;

        // Trade에서 거래량 추출 (ChartUpdate는 항상 Trade 포함)
        let trade = chart_update
            .trade
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ChartUpdate must contain Trade"))?;

        let volume = match trade.as_ref() {
            crate::types::stream::CurveEventType::Buy(buy) => &buy.amount_in,
            crate::types::stream::CurveEventType::Sell(sell) => &sell.amount_out,
            _ => return Err(anyhow::anyhow!("Trade must be Buy or Sell")),
        };

        let timestamp = chart_update.sync.block_timestamp as i64;
        let block_number = chart_update.sync.block_number as i64;

        // quote_price (quote/USD) 조회 — market의 quote_id 기준, block_number 기반
        let quote_id = cache_manager.get_market_quote_id(&chart_update.sync.token);
        let native_price = cache_manager
            .get_quote_usd_price(&quote_id, block_number)
            .await;

        // total_supply 조회
        let total_supply = cache_manager
            .get_token_total_supply(&chart_update.sync.token)
            .await
            .map(|s| BigDecimal::from_str(&s).unwrap_or_else(|_| BigDecimal::from(0)))
            .unwrap_or_else(|_| BigDecimal::from(0));

        // 공통 파라미터를 컨텍스트로 묶음
        let ctx = ChartUpdateContext {
            token_id: &chart_update.sync.token,
            timestamp,
            price: &price,
            volume: Some(volume),
            native_price: &native_price,
            total_supply: &total_supply,
            order: Some(ChartUpdateOrder {
                block_number: chart_update.sync.block_number,
                transaction_index: chart_update.sync.transaction_index,
                log_index: chart_update.sync.log_index,
            }),
        };

        let charts = Self::update_all_intervals(cache_manager, &ctx).await?;

        // WebSocket으로 Chart 전송
        self.send_charts_to_websocket(&chart_update.sync.token, charts, cache_manager)
            .await?;

        Ok(())
    }

    // CreateCurve 이벤트 처리 (가격만 설정, 채널 전송 안함)
    async fn process_create_curve_event(
        &self,
        create_curve: &CreateCurve,
        cache_manager: &CacheManager,
    ) -> Result<()> {
        let price = (create_curve.virtual_native.clone() / create_curve.virtual_token.clone())
            .with_scale_round(10, RoundingMode::Up);
        tracing::info!(
            "Processing CreateCurve event - Token: {}, Price: {}, Timestamp: {}",
            create_curve.token,
            price,
            create_curve.block_timestamp
        );

        let timestamp = create_curve.block_timestamp as i64;
        let block_number = create_curve.block_number as i64;

        // quote_price (quote/USD) 조회 — quote_token 기준, block_number 기반
        let native_price = cache_manager
            .get_quote_usd_price(&create_curve.quote_token, block_number)
            .await;

        // CreateCurve에서는 초기 total_supply 사용 (1조 개)
        let total_supply = BigDecimal::from(1_000_000_000_000_000_000_000_000_000_i128);

        // 공통 파라미터를 컨텍스트로 묶음 (거래량 없음)
        let ctx = ChartUpdateContext {
            token_id: &create_curve.token,
            timestamp,
            price: &price,
            volume: None,
            native_price: &native_price,
            total_supply: &total_supply,
            order: Some(ChartUpdateOrder {
                block_number: create_curve.block_number,
                transaction_index: create_curve.transaction_index,
                log_index: create_curve.log_index,
            }),
        };

        let charts = Self::update_all_intervals(cache_manager, &ctx).await?;

        // WebSocket으로 Chart 전송
        self.send_charts_to_websocket(&create_curve.token, charts, cache_manager)
            .await?;

        Ok(())
    }

    // DexChartUpdate 이벤트 처리: Atomic 업데이트
    async fn process_dex_chart_update_event(
        &self,
        chart_update: &crate::types::stream::DexChartUpdate,
        cache_manager: &CacheManager,
    ) -> Result<()> {
        tracing::info!(
            "🎯 Processing DexChartUpdate - Token: {}, has_trade: {}, Timestamp: {}",
            chart_update.sync.token,
            chart_update.trade.is_some(),
            chart_update.sync.block_timestamp
        );

        let price = &chart_update.sync.price;

        // Trade에서 거래량 추출 (DexChartUpdate는 항상 Trade 포함)
        let trade = chart_update
            .trade
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("DexChartUpdate must contain Trade"))?;

        let volume = if let Some(buy) = trade.is_buy() {
            &buy.amount_in
        } else if let Some(sell) = trade.is_sell() {
            &sell.amount_out
        } else {
            return Err(anyhow::anyhow!("Trade must be Buy or Sell"));
        };

        let timestamp = chart_update.sync.block_timestamp as i64;
        let block_number = chart_update.sync.block_number as i64;

        // quote_price (quote/USD) 조회 — market의 quote_id 기준, block_number 기반
        let quote_id = cache_manager.get_market_quote_id(&chart_update.sync.token);
        let native_price = cache_manager
            .get_quote_usd_price(&quote_id, block_number)
            .await;

        // total_supply 조회
        let total_supply = cache_manager
            .get_token_total_supply(&chart_update.sync.token)
            .await
            .map(|s| BigDecimal::from_str(&s).unwrap_or_else(|_| BigDecimal::from(0)))
            .unwrap_or_else(|_| BigDecimal::from(0));

        // 공통 파라미터를 컨텍스트로 묶음
        let ctx = ChartUpdateContext {
            token_id: &chart_update.sync.token,
            timestamp,
            price,
            volume: Some(volume),
            native_price: &native_price,
            total_supply: &total_supply,
            order: Some(ChartUpdateOrder {
                block_number: chart_update.sync.block_number,
                transaction_index: chart_update.sync.transaction_index,
                log_index: chart_update.sync.log_index,
            }),
        };

        let charts = Self::update_all_intervals(cache_manager, &ctx).await?;

        // WebSocket으로 Chart 전송
        self.send_charts_to_websocket(&chart_update.sync.token, charts, cache_manager)
            .await?;

        Ok(())
    }

    pub fn get_event_receiver(
        &self,
        chart_key: ChartKey,
    ) -> Result<MonitoredBroadcastReceiver<ChartResponse>> {
        let sender = self
            .event_sender
            .entry(chart_key.clone())
            .or_insert_with(|| {
                let channel_name = format!(
                    "Chart-{}-{}-{}",
                    chart_key.token_id.replace(':', "-"),
                    chart_key.interval,
                    chart_key.price_type
                );
                let (sender, _receiver) =
                    monitored_broadcast_channel::<ChartResponse>(&channel_name, *CHANNEL_SIZE);
                sender
            })
            .clone();

        Ok(sender.subscribe())
    }

    // WebSocket으로 Charts 전송 (interval별로 받은 Chart를 price_type별로 ChartResponse로 변환해서 전송)
    async fn send_charts_to_websocket(
        &self,
        token_id: &str,
        charts: Vec<(ChartInterval, Chart)>,
        _cache_manager: &CacheManager,
    ) -> Result<()> {
        // 각 interval의 Chart에 대해 price_type별로 변환해서 전송
        for (interval, chart) in charts {
            for price_type in [
                PriceType::Price,
                PriceType::PriceUsd,
                PriceType::MarketCap,
                PriceType::MarketCapUsd,
            ] {
                let chart_key = ChartKey {
                    token_id: token_id.to_string(),
                    interval: interval.as_str().to_string(),
                    price_type,
                };

                if let Some(sender) = self.event_sender.get(&chart_key) {
                    // Chart.to_response()로 price_type에 맞는 ChartResponse 생성
                    // (token_id/interval은 페이로드에 동봉 — receive-side 방어용)
                    let response = chart.to_response(token_id, interval.as_str(), price_type);

                    match sender.send(response) {
                        Ok(_) => {
                            tracing::info!(
                                "Chart sent to WebSocket - Token: {}, Interval: {}, PriceType: {:?}",
                                token_id,
                                interval.as_str(),
                                price_type
                            );
                        }
                        Err(err) => {
                            tracing::debug!(
                                "No active receivers - Token: {}, Interval: {}, PriceType: {:?}, err: {}",
                                token_id,
                                interval.as_str(),
                                price_type,
                                err
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 사용되지 않는 차트 이벤트 정리 태스크 시작
    fn start_cleanup_task() -> Result<()> {
        let producer = CHART_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("CHART_EVENT_PRODUCER not initialized"))?;

        let event_sender = Arc::clone(&producer.event_sender);

        tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(tokio::time::Duration::from_secs(300));

            loop {
                cleanup_interval.tick().await;

                let mut removed_count = 0;

                event_sender.retain(|chart_key, sender| {
                    if sender.receiver_count() == 0 {
                        tracing::info!("정리 중: 사용되지 않는 차트 {:?}", chart_key);
                        removed_count += 1;
                        false
                    } else {
                        true
                    }
                });

                if removed_count > 0 {
                    tracing::info!(
                        "차트 정리 완료: {}개 차트 제거, 남은 차트: {}개",
                        removed_count,
                        event_sender.len()
                    );
                }
            }
        });

        Ok(())
    }
}
