#!/usr/bin/env bash
# Nads.fun WebSocket Server에 wscat으로 접속.
# 연결 후 들어오는 모든 메시지를 stdout에 그대로 stream.
# 사용자가 입력한 텍스트는 그대로 ws로 send (wscat 인터랙티브 모드).
#
# 사용:
#   ./connect.sh              # dev 서버
#   WS_URL=ws://localhost:8001/wss ./connect.sh
#
# 종료: Ctrl+C

set -euo pipefail
WS_URL="${WS_URL:-wss://dev-wss.nadapp.net/wss}"

if ! command -v wscat >/dev/null 2>&1; then
  echo "wscat 미설치: npm install -g wscat" >&2
  exit 1
fi

echo "[connect] $WS_URL" >&2
exec wscat -c "$WS_URL"
