#!/usr/bin/env bash
# v2 wsserver systemd 서비스 + bash alias 설치 스크립트.
# 기존 wsserver(production)와 별도로 wsserver-v2를 등록한다.
#
# 포트/IP/REDIS_KEY_PREFIX 등 모든 런타임 설정은 `${WORK_DIR}/.env`가 단일 source.
# systemd unit은 PORT를 명시하지 않아 .env 값이 그대로 적용된다.
#
# 사용:
#   ./scripts/deploy.sh
#
# 환경변수로 override 가능:
#   SERVICE_NAME    기본 wsserver-v2
#   WORK_DIR        기본 /home/ubuntu/websocket-server-v2
#   ALIAS_PREFIX    기본 ws2
#   BASHRC          기본 ~/.bashrc
#
# 본 스크립트는 build/clone은 하지 않는다. 사전에:
#   git clone -b v2 <repo> websocket-server-v2
#   cd websocket-server-v2
#   cp ../websocket-server/.env .env       # PORT, REDIS_KEY_PREFIX 등 수정
#   cargo build --release
# 가 끝난 상태를 가정.

set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-wsserver-v2}"
WORK_DIR="${WORK_DIR:-/home/ubuntu/websocket-server-v2}"
ALIAS_PREFIX="${ALIAS_PREFIX:-ws2}"
BASHRC="${BASHRC:-$HOME/.bashrc}"

SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
BIN_PATH="${WORK_DIR}/target/release/websocket-server"

# alias 블록 idempotent하게 갈아끼우기 위한 marker
ALIAS_MARKER_BEGIN="# >>> ${SERVICE_NAME} aliases (deploy.sh) >>>"
ALIAS_MARKER_END="# <<< ${SERVICE_NAME} aliases <<<"

echo "[deploy] service:  $SERVICE_NAME"
echo "[deploy] workdir:  $WORK_DIR"
echo "[deploy] binary:   $BIN_PATH"
echo "[deploy] env file: ${WORK_DIR}/.env (포트/Redis 등은 여기서 관리)"
echo "[deploy] alias:    ${ALIAS_PREFIX}{start,stop,restart,log,status}"
echo "[deploy] bashrc:   $BASHRC"
echo

# .env 존재 확인 (없으면 wsserver가 PORT 등을 못 읽어 panic할 가능성)
if [[ ! -f "${WORK_DIR}/.env" ]]; then
  echo "[deploy] WARN: ${WORK_DIR}/.env 가 없습니다. wsserver가 환경변수를 못 읽고 실패할 수 있습니다."
fi

# 1) systemd unit 파일 작성 (sudo)
# - PORT/IP 등은 명시하지 않음 → WorkingDirectory의 .env를 dotenv가 읽음
# - 운영자가 .env 한 곳만 보면 되도록 단일 source-of-truth 유지
echo "[deploy] writing $SERVICE_PATH ..."
sudo tee "$SERVICE_PATH" > /dev/null << EOF
[Unit]
Description=Websocket Service (${SERVICE_NAME})
After=network.target

[Service]
User=ubuntu
Group=ubuntu
WorkingDirectory=${WORK_DIR}
ExecStart=${BIN_PATH}
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
EOF

# 2) daemon-reload + enable
echo "[deploy] systemctl daemon-reload && enable ..."
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}.service"

# 3) alias 갱신 (marker 사이를 통째로 갈아끼움 → idempotent)
echo "[deploy] updating aliases in $BASHRC ..."
touch "$BASHRC"
if grep -qF "$ALIAS_MARKER_BEGIN" "$BASHRC"; then
  # 기존 블록 제거
  sed -i.bak "/^${ALIAS_MARKER_BEGIN}\$/,/^${ALIAS_MARKER_END}\$/d" "$BASHRC"
fi

cat << EOF >> "$BASHRC"
${ALIAS_MARKER_BEGIN}
alias ${ALIAS_PREFIX}restart='sudo systemctl restart ${SERVICE_NAME} && journalctl -u ${SERVICE_NAME} -f -o cat'
alias ${ALIAS_PREFIX}start='sudo systemctl start ${SERVICE_NAME}'
alias ${ALIAS_PREFIX}stop='sudo systemctl stop ${SERVICE_NAME}'
alias ${ALIAS_PREFIX}log='journalctl -u ${SERVICE_NAME} -f -o cat'
alias ${ALIAS_PREFIX}status='sudo systemctl status ${SERVICE_NAME}'
${ALIAS_MARKER_END}
EOF

echo
echo "[deploy] done."
echo
echo "다음 단계:"
echo "  ${WORK_DIR}/.env 의 PORT, IP, REDIS_KEY_PREFIX 등 확인"
echo "  source $BASHRC"
echo "  ${ALIAS_PREFIX}start"
echo "  ${ALIAS_PREFIX}log"
echo "  ss -tlnp | grep :\$(grep -E '^PORT=' ${WORK_DIR}/.env | sed 's/.*=\"*\\([0-9]*\\).*/\\1/')"
