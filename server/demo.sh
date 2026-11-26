#!/bin/sh
# 실시간 데모 서버를 켜고 끄는 스크립트
#
#   server/demo.sh run       쓰는 동안만 켠다. 이 창에서 실행되고 Ctrl+C 나 접속 종료로 꺼진다.
#   server/demo.sh start     백그라운드로 켠다. 로그아웃해도 계속 실행된다.
#   server/demo.sh stop      끈다.
#   server/demo.sh restart   껐다 켠다.
#   server/demo.sh status    켜져 있는지, 모델이 응답하는지 보여 준다.
#   server/demo.sh link      페이지 주소를 보여 준다.
#
# 기본은 접근 토큰 없이 쓴다: 서버가 켜져 있으면 연결된 사람은 누구나 쓰고, 끄면 아무도 못 쓴다.
# 토큰을 쓰려면 demo.env 에 ICD_DEMO_REQUIRE_TOKEN=1 을 적는다(그때는 link 가 토큰이 든 링크를 보여 준다).
#
# 설정은 server/demo.env 에 적는다(demo.env.example 참고).
set -eu
cd "$(dirname "$0")"
umask 077

PY="${PYTHON:-python3}"
LOG=icd_demo_server.log
export ICD_DEMO_PIDFILE="$PWD/icd_demo.pid"

# 설정 파일. 다른 파일을 쓰려면 ICD_DEMO_ENV_FILE 로 경로를 준다.
ENV_FILE="${ICD_DEMO_ENV_FILE:-./demo.env}"
if [ -f "$ENV_FILE" ]; then
  set -a
  . "$ENV_FILE"
  set +a
fi

is_running() {
  [ -s "$ICD_DEMO_PIDFILE" ] && kill -0 "$(cat "$ICD_DEMO_PIDFILE")" 2>/dev/null
}

make_token() {
  if [ "${ICD_DEMO_REQUIRE_TOKEN:-}" = "1" ] && [ -z "${ICD_DEMO_TOKEN:-}" ] && [ ! -s .access_token ]; then
    "$PY" -c 'import secrets; print(secrets.token_urlsafe(24))' > .access_token
    echo "접근 토큰을 새로 만들었습니다: server/.access_token"
  fi
}

# 페이지는 토큰과 주소를 화면에서 묻지 않는다. 이 링크로 한 번 열면 브라우저가 토큰을 기억한다.
page_link() {
  origin="$(printf '%s' "${ICD_DEMO_ORIGINS:-https://icd-coding.netlify.app}" | cut -d, -f1 | sed 's|/*$||')"
  if [ -n "${ICD_DEMO_TOKEN:-}" ]; then
    tok="$ICD_DEMO_TOKEN"
  elif [ "${ICD_DEMO_REQUIRE_TOKEN:-}" = "1" ] && [ -s .access_token ]; then
    tok="$(cat .access_token)"
  else
    tok=""
  fi
  port="${ICD_DEMO_PORT:-8765}"
  link="$origin/demo"
  sep="#"
  if [ -n "$tok" ]; then link="$link${sep}token=$tok"; sep="&"; fi
  if [ "$port" != "8765" ]; then link="$link${sep}api=http://127.0.0.1:$port"; fi
  echo "$link"
}
show_link() {
  echo "페이지 주소:"
  echo "  $(page_link)"
}

# 모델 서비스를 같이 켜고 끄고 싶으면 demo.env 에 명령을 적는다. 비어 있으면 아무것도 하지 않는다.
# 명령의 출력은 파일로 받았다가 보여 준다. 터미널이 끊긴 뒤에도(창을 닫았을 때) 명령이 끝까지 돌게 하기 위해서다.
HOOK_OUT="$PWD/icd_demo_hook.out"
model_start() {
  if [ -n "${ICD_DEMO_MODEL_START_CMD:-}" ]; then
    sh -c "$ICD_DEMO_MODEL_START_CMD" < /dev/null > "$HOOK_OUT" 2>&1 || true
    cat "$HOOK_OUT" 2>/dev/null || true
  fi
}
model_stop() {
  if [ -n "${ICD_DEMO_MODEL_STOP_CMD:-}" ]; then
    sh -c "$ICD_DEMO_MODEL_STOP_CMD" < /dev/null > "$HOOK_OUT" 2>&1 || true
    cat "$HOOK_OUT" 2>/dev/null || true
  fi
}

health() {
  "$PY" - <<'PYEOF'
import json, os, urllib.request
port = os.environ.get("ICD_DEMO_PORT") or "8765"
token = os.environ.get("ICD_DEMO_TOKEN", "").strip()
if not token and os.path.isfile(".access_token"):
    token = open(".access_token").read().strip()
request = urllib.request.Request("http://127.0.0.1:%s/api/health" % port)
if token:
    request.add_header("Authorization", "Bearer " + token)
try:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    data = json.loads(opener.open(request, timeout=8).read().decode("utf-8"))
except Exception:
    print("서버가 응답하지 않습니다.")
else:
    print("방식: %s" % data.get("pipeline"))
    if data.get("llm_ready"):
        print("생성 모델: %s (응답함)" % data.get("model"))
    else:
        print("생성 모델: 응답 없음 (꺼져 있거나 아직 올라오는 중)")
    if "generate" in (data.get("stages") or []) and len(data.get("stages") or []) > 1:
        print("재정렬 모델: %s" % ("응답함" if data.get("rerank_ready") else "응답 없음 (꺼져 있거나 아직 올라오는 중)"))
    if data.get("idle_exit_min"):
        print("자동 종료: 판독문 요청이 없으면 약 %d분 뒤" % (int(data.get("idle_exit_in_s", 0)) // 60))
PYEOF
}

start() (
  exec 9>> .icd_demo_lifecycle.lock
  if ! flock -n 9; then
    echo "다른 데모 실행이 진행 중입니다."
    return 1
  fi
  if is_running; then
    echo "이미 켜져 있습니다 (pid $(cat "$ICD_DEMO_PIDFILE"))."
    return 0
  fi
  rm -f "$ICD_DEMO_PIDFILE"
  make_token
  model_start
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup "$PY" -u icd_demo_server.py >> "$LOG" 2>&1 < /dev/null 9>&- &
  else
    nohup "$PY" -u icd_demo_server.py >> "$LOG" 2>&1 < /dev/null 9>&- &
  fi
  i=0
  while [ $i -lt 20 ]; do
    if is_running; then
      echo "켰습니다 (pid $(cat "$ICD_DEMO_PIDFILE")). 끄려면: sh server/demo.sh stop"
      show_link
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  echo "켜지 못했습니다. 마지막 로그:"
  tail -n 5 "$LOG" || true
  return 1
)

stop() {
  if is_running; then
    pid="$(cat "$ICD_DEMO_PIDFILE")"
    kill "$pid"
    i=0
    while kill -0 "$pid" 2>/dev/null && [ $i -lt 40 ]; do
      i=$((i + 1))
      sleep 0.25
    done
    echo "껐습니다."
  else
    echo "켜져 있지 않습니다."
  fi
  rm -f "$ICD_DEMO_PIDFILE"
  model_stop
}

run_foreground() {
  if is_running; then
    echo "이미 백그라운드로 켜져 있습니다 (pid $(cat "$ICD_DEMO_PIDFILE")). 먼저 끄려면: sh server/demo.sh stop"
    return 1
  fi
  make_token
  echo "이 창을 열어 둔 동안만 켜져 있습니다. Ctrl+C 또는 연결 종료로 끕니다."
  show_link
  exec "$PY" -u demo_foreground.py
}

case "${1:-}" in
  run) run_foreground ;;
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status)
    if is_running; then
      echo "켜져 있음 (pid $(cat "$ICD_DEMO_PIDFILE"))"
      health
    else
      echo "꺼져 있음"
    fi ;;
  link)
    make_token > /dev/null
    page_link ;;
  token)
    make_token > /dev/null
    if [ -n "${ICD_DEMO_TOKEN:-}" ]; then
      echo "$ICD_DEMO_TOKEN"
    elif [ "${ICD_DEMO_REQUIRE_TOKEN:-}" = "1" ]; then
      cat .access_token
    else
      echo "접근 토큰을 쓰지 않는 설정입니다."
    fi ;;
  *) echo "사용: sh server/demo.sh run|start|stop|restart|status|link|token"; exit 2 ;;
esac
