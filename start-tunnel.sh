#!/bin/bash
# Cloudflare Quick Tunnel로 외부 공개 실행 스크립트
#
# 사용법:
#   ./start-tunnel.sh        # Streamlit + Cloudflare Quick Tunnel (임시 URL 발급)
#   ./start-tunnel.sh --stop # 터널·Streamlit 종료

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/.venv/bin/python"
PORT=8501
cd "$SCRIPT_DIR"

if [ "$1" = "--stop" ]; then
    echo "[종료] Cloudflare Tunnel·Streamlit 종료..."
    pkill -f "cloudflared tunnel" 2>/dev/null || true
    pkill -f "streamlit run app.py" 2>/dev/null || true
    echo "완료"
    exit 0
fi

# Streamlit 시작 (이미 실행 중이면 스킵)
if lsof -i :$PORT -sTCP:LISTEN &>/dev/null; then
    echo "[1/2] Streamlit 이미 실행 중 (port $PORT)"
else
    echo "[1/2] Streamlit 시작 (port $PORT)..."
    "$VENV" -m streamlit run app.py --server.port $PORT --server.headless true &
    sleep 4
fi

# Cloudflare Quick Tunnel 시작 (계정 불필요, 재시작 시 URL 변경)
# 주의: --config /dev/null 은 다른 프로젝트(emarket)의 ~/.cloudflared/config.yml 을
#       무시하기 위함이다. 이 config 에는 catch-all `http_status:404` 규칙이 있어,
#       무시하지 않으면 quick tunnel 의 trycloudflare 주소가 모두 404 가 된다.
echo "[2/2] Cloudflare Quick Tunnel 시작..."
echo "  → 공개 URL은 아래 로그의 https://...trycloudflare.com 주소"
echo ""
cloudflared tunnel --config /dev/null --url "http://localhost:$PORT" --no-autoupdate
