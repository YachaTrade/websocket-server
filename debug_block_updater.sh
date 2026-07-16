#!/bin/bash

# 블록 업데이터 디버깅 스크립트
echo "🔍 블록 업데이터 디버깅 시작..."
echo "현재 시간: $(date)"
echo "================================="

# 환경변수 설정
export RUST_LOG=info,websocket_server::client=debug
export BLOCK_UPDATE_INTERVAL=1000  # 1초마다 체크

echo "환경변수:"
echo "- RUST_LOG=$RUST_LOG"
echo "- BLOCK_UPDATE_INTERVAL=$BLOCK_UPDATE_INTERVAL ms"
echo "================================="

# 서버 실행 및 블록 업데이트 로그만 필터링
cargo run 2>&1 | grep -E "Block updater|Block updated|Block unchanged|Successfully fetched|Attempting to fetch|RPC error|Request timeout|Failed to update"