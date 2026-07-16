pub mod client;
pub mod config;
pub mod db;
pub mod event;
pub mod metrics;
pub mod server;
pub mod stream;
pub mod types;
pub mod utils;

// 모든 에러 로그에 "ERROR : " 접두사를 추가하는 매크로
#[macro_export]
macro_rules! error_log {
    ($($arg:tt)*) => {
        tracing::error!("ERROR : {}", format!($($arg)*))
    };
}
