use crate::{
    error_log,
    types::order::{OrderMessage, OrderToken, TokenOrderType},
    utils::{calculate_price_change_percent, meets_order_latest_trade_min_amount},
};
use anyhow::Result;
use dashmap::DashMap;
use std::{fmt, sync::Arc};
use tokio::sync::{Mutex, OnceCell};
use tracing::info;

use crate::{
    config::CHANNEL_SIZE,
    db::cache::CacheManager,
    metrics::{
        monitored_broadcast_channel, monitored_channel, MonitoredBroadcastReceiver,
        MonitoredBroadcastSender, MonitoredReceiver, MonitoredSender,
    },
    types::stream::{CurveEventType, DexEventType},
};

pub static ORDER_EVENT_PRODUCER: OnceCell<Arc<OrderEventProducer>> = OnceCell::const_new();
pub struct OrderEventProducer {
    /// Sender channel for DEX events (Arc로 zero-copy 공유)
    pub dex_event_sender: MonitoredSender<Arc<DexEventType>>,

    /// Receiver channel for DEX events
    pub dex_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<DexEventType>>>>,

    /// Sender channel for Curve events (Arc로 zero-copy 공유)
    pub curve_event_sender: MonitoredSender<Arc<CurveEventType>>,

    /// Receiver channel for Curve events
    pub curve_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<CurveEventType>>>>,

    pub event_sender: Arc<DashMap<TokenOrderType, MonitoredBroadcastSender<OrderMessage>>>,
}

impl fmt::Debug for OrderEventProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderEventProducer").finish()
    }
}

impl OrderEventProducer {
    pub fn init() -> Result<()> {
        let (dex_event_sender, dex_event_receiver) = monitored_channel("Order-DEX", *CHANNEL_SIZE);
        let (curve_event_sender, curve_event_receiver) =
            monitored_channel("Order-Curve", *CHANNEL_SIZE);
        let event_sender = Arc::new(DashMap::new());
        let producer = OrderEventProducer {
            dex_event_sender,
            dex_event_receiver: Arc::new(Mutex::new(dex_event_receiver)),
            curve_event_sender,
            curve_event_receiver: Arc::new(Mutex::new(curve_event_receiver)),
            event_sender,
        };
        ORDER_EVENT_PRODUCER
            .set(Arc::new(producer))
            .expect("Failed to set ORDER_EVENT_PRODUCER");
        Ok(())
    }

    pub async fn main() -> Result<()> {
        // 프로듀서 인스턴스를 가져옴
        let producer = ORDER_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("ORDER_EVENT_PRODUCER not initialized"))?
            .clone();

        // Curve 이벤트 수신 태스크
        let producer_curve = producer.clone();
        tokio::spawn(async move {
            if let Err(e) = producer_curve.receive_curve_event().await {
                error_log!("Error in receive_curve_event: {:?}", e);
            }
        });

        // DEX 이벤트 수신 태스크
        tokio::spawn(async move {
            if let Err(e) = producer.receive_dex_event().await {
                error_log!("Error in receive_dex_event: {:?}", e);
            }
        });

        tracing::info!(
            "OrderEventProducer main started: receive_curve_event and receive_dex_event tasks spawned"
        );

        Ok(())
    }

    pub async fn receive_curve_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.curve_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("OrderEventProducer receive curve event: {:?}", event);
            let message: OrderMessage =
                match (event.is_create_curve(), event.is_buy(), event.is_sell()) {
                    (Some(create_curve), _, _) => {
                        // 병렬로 모든 데이터 조회
                        let (market_info_result, token_result, price_24h_ago_result) = tokio::join!(
                            cache_manager.get_market_info(&create_curve.token),
                            cache_manager.get_token_info(&create_curve.token),
                            cache_manager.get_price_24h_ago(&create_curve.token),
                        );

                        // 에러 시 로그만 찍고 스킵
                        let (market_info, token_info) = match (market_info_result, token_result) {
                            (Ok(r), Ok(t)) => (r, t),
                            _ => {
                                error_log!("DB query failed for create_curve event, skipping");
                                continue;
                            }
                        };

                        // NSFW 토큰은 메시지 전송하지 않음
                        if token_info.is_nsfw {
                            info!(
                                "NSFW token detected, skipping message: {}",
                                token_info.token_id
                            );
                            continue;
                        }

                        // percent 계산
                        let percent = match price_24h_ago_result {
                            Ok(Some(price_24h_ago)) => {
                                calculate_price_change_percent(&price_24h_ago, &market_info.price)
                                    .unwrap_or(0.0)
                            }
                            _ => 0.0,
                        };

                        OrderMessage {
                            order_type: TokenOrderType::CreationTime,
                            tokens: vec![OrderToken {
                                token_info,
                                market_info,
                                percent,
                            }],
                            total_count: 1,
                        }
                    }
                    (_, Some(buy), _) => {
                        // 병렬로 모든 데이터 조회
                        let (market_info_result, token_result, price_24h_ago_result) = tokio::join!(
                            cache_manager.get_market_info(&buy.token),
                            cache_manager.get_token_info(&buy.token),
                            cache_manager.get_price_24h_ago(&buy.token),
                        );

                        // 에러 시 로그만 찍고 스킵
                        let (market_info, token_info) = match (market_info_result, token_result) {
                            (Ok(r), Ok(t)) => (r, t),
                            _ => {
                                error_log!("DB query failed for buy event, skipping");
                                continue;
                            }
                        };

                        // NSFW 토큰은 메시지 전송하지 않음
                        if token_info.is_nsfw {
                            info!(
                                "NSFW token detected, skipping message: {}",
                                token_info.token_id
                            );
                            continue;
                        }

                        // native(quote) 금액이 최소 금액 미만이면 latest_trade push 하지 않음
                        if !meets_order_latest_trade_min_amount(
                            &buy.amount_in,
                            market_info.quote_info.decimals,
                        ) {
                            info!(
                                "latest_trade below min amount, skip: token={}, amount_in={}",
                                buy.token, buy.amount_in
                            );
                            continue;
                        }

                        // percent 계산
                        let percent = match price_24h_ago_result {
                            Ok(Some(price_24h_ago)) => {
                                calculate_price_change_percent(&price_24h_ago, &market_info.price)
                                    .unwrap_or(0.0)
                            }
                            _ => 0.0,
                        };

                        OrderMessage {
                            order_type: TokenOrderType::LatestTrade,
                            tokens: vec![OrderToken {
                                token_info,
                                market_info,
                                percent,
                            }],
                            total_count: 1,
                        }
                    }
                    (_, _, Some(sell)) => {
                        // 병렬로 모든 데이터 조회
                        let (market_info_result, token_result, price_24h_ago_result) = tokio::join!(
                            cache_manager.get_market_info(&sell.token),
                            cache_manager.get_token_info(&sell.token),
                            cache_manager.get_price_24h_ago(&sell.token),
                        );

                        // 에러 시 로그만 찍고 스킵
                        let (market_info, token_info) = match (market_info_result, token_result) {
                            (Ok(r), Ok(t)) => (r, t),
                            _ => {
                                error_log!("DB query failed for sell event, skipping");
                                continue;
                            }
                        };

                        // NSFW 토큰은 메시지 전송하지 않음
                        if token_info.is_nsfw {
                            info!(
                                "NSFW token detected, skipping message: {}",
                                token_info.token_id
                            );
                            continue;
                        }

                        // native(quote) 금액이 최소 금액 미만이면 latest_trade push 하지 않음
                        if !meets_order_latest_trade_min_amount(
                            &sell.amount_out,
                            market_info.quote_info.decimals,
                        ) {
                            info!(
                                "latest_trade below min amount, skip: token={}, amount_out={}",
                                sell.token, sell.amount_out
                            );
                            continue;
                        }

                        // percent 계산
                        let percent = match price_24h_ago_result {
                            Ok(Some(price_24h_ago)) => {
                                calculate_price_change_percent(&price_24h_ago, &market_info.price)
                                    .unwrap_or(0.0)
                            }
                            _ => 0.0,
                        };

                        OrderMessage {
                            order_type: TokenOrderType::LatestTrade,
                            tokens: vec![OrderToken {
                                token_info,
                                market_info,
                                percent,
                            }],
                            total_count: 1,
                        }
                    }
                    _ => {
                        error_log!("OrderEventProducer receive_curve_event Unknown event type");
                        continue;
                    }
                };
            let _ = self.send_message(message).await;
        }
        Ok(())
    }

    pub async fn receive_dex_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.dex_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("OrderEventProducer receive dex event: {:?}", event);

            let message: OrderMessage = match (event.is_buy(), event.is_sell()) {
                (Some(buy), _) => {
                    // 병렬로 모든 데이터 조회
                    let (market_info_result, token_result, price_24h_ago_result) = tokio::join!(
                        cache_manager.get_market_info(&buy.token),
                        cache_manager.get_token_info(&buy.token),
                        cache_manager.get_price_24h_ago(&buy.token),
                    );

                    // 에러 시 로그만 찍고 스킵
                    let (market_info, token_info) = match (market_info_result, token_result) {
                        (Ok(r), Ok(t)) => (r, t),
                        _ => {
                            error_log!("DB query failed for buy event, skipping");
                            continue;
                        }
                    };

                    // NSFW 토큰은 메시지 전송하지 않음
                    if token_info.is_nsfw {
                        info!(
                            "NSFW token detected, skipping message: {}",
                            token_info.token_id
                        );
                        continue;
                    }

                    // native(quote) 금액이 최소 금액 미만이면 latest_trade push 하지 않음
                    if !meets_order_latest_trade_min_amount(
                        &buy.amount_in,
                        market_info.quote_info.decimals,
                    ) {
                        info!(
                            "latest_trade below min amount, skip: token={}, amount_in={}",
                            buy.token, buy.amount_in
                        );
                        continue;
                    }

                    // percent 계산
                    let percent = match price_24h_ago_result {
                        Ok(Some(price_24h_ago)) => {
                            calculate_price_change_percent(&price_24h_ago, &market_info.price)
                                .unwrap_or(0.0)
                        }
                        _ => 0.0,
                    };

                    OrderMessage {
                        order_type: TokenOrderType::LatestTrade,
                        tokens: vec![OrderToken {
                            token_info,
                            market_info,
                            percent,
                        }],
                        total_count: 1,
                    }
                }
                (_, Some(sell)) => {
                    // 병렬로 모든 데이터 조회
                    let (market_info_result, token_result, price_24h_ago_result) = tokio::join!(
                        cache_manager.get_market_info(&sell.token),
                        cache_manager.get_token_info(&sell.token),
                        cache_manager.get_price_24h_ago(&sell.token),
                    );

                    // 에러 시 로그만 찍고 스킵
                    let (market_info, token_info) = match (market_info_result, token_result) {
                        (Ok(r), Ok(t)) => (r, t),
                        _ => {
                            error_log!("DB query failed for sell event, skipping");
                            continue;
                        }
                    };

                    // NSFW 토큰은 메시지 전송하지 않음
                    if token_info.is_nsfw {
                        info!(
                            "NSFW token detected, skipping message: {}",
                            token_info.token_id
                        );
                        continue;
                    }

                    // native(quote) 금액이 최소 금액 미만이면 latest_trade push 하지 않음
                    if !meets_order_latest_trade_min_amount(
                        &sell.amount_out,
                        market_info.quote_info.decimals,
                    ) {
                        info!(
                            "latest_trade below min amount, skip: token={}, amount_out={}",
                            sell.token, sell.amount_out
                        );
                        continue;
                    }

                    // percent 계산
                    let percent = match price_24h_ago_result {
                        Ok(Some(price_24h_ago)) => {
                            calculate_price_change_percent(&price_24h_ago, &market_info.price)
                                .unwrap_or(0.0)
                        }
                        _ => 0.0,
                    };

                    OrderMessage {
                        order_type: TokenOrderType::LatestTrade,
                        tokens: vec![OrderToken {
                            token_info,
                            market_info,
                            percent,
                        }],
                        total_count: 1,
                    }
                }
                _ => {
                    error_log!("OrderEventProducer receive_dex_event Unknown event type");
                    continue;
                }
            };
            let _ = self.send_message(message).await;
        }
        Ok(())
    }

    async fn send_message(&self, message: OrderMessage) -> Result<()> {
        let order_type = message.order_type;

        {
            // 다른 타입은 해당 order_type에만 전송
            if let Some(sender) = self.event_sender.get(&order_type) {
                let receiver_count = sender.receiver_count();
                info!(
                    "📡 Order type {:?} has {} receivers",
                    order_type, receiver_count
                );

                // receiver가 있는지 확인
                if receiver_count > 0 {
                    if let Err(e) = sender.send(message) {
                        error_log!(
                            "Failed to send message for order_type {:?}: {:?}",
                            order_type,
                            e
                        );
                    } else {
                        info!(
                            "✅ Successfully sent message for order_type {:?}",
                            order_type
                        );
                    }
                } else {
                    // receiver가 없으면 메시지를 보내지 않음
                    info!(
                        "⚠️ No receivers for OrderMessage with order_type {:?}, skipping message",
                        order_type
                    );
                }
            } else {
                error_log!("❌ No sender found for order_type: {:?}", order_type);
            }
        }
        Ok(())
    }
    pub fn get_event_receiver(
        &self,
        order_type: TokenOrderType,
    ) -> Result<MonitoredBroadcastReceiver<OrderMessage>> {
        // or_insert_with_entry를 사용하여 한 번의 락킹으로 처리
        let receiver = self
            .event_sender
            .entry(order_type)
            .or_insert_with(|| {
                let channel_name = format!("Order-{:?}", order_type);
                let (sender, _receiver) =
                    monitored_broadcast_channel::<OrderMessage>(&channel_name, *CHANNEL_SIZE);
                sender
            })
            .to_owned();

        let receiver = receiver.subscribe();
        Ok(receiver)
    }
}
