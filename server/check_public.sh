#!/bin/sh
# 공개 저장소에 올리기 전에 돌리는 검사.
# 서버 주소(IP), 토큰, 서버별 설정 파일이 올라갈 파일에 섞여 있으면 알려 주고 실패한다.
# 사용: sh server/check_public.sh        (저장소 맨 위 폴더에서)
set -eu
cd "$(dirname "$0")/.."
bad=0

# git 저장소 안이면 올라갈 파일(추적 중 + 새 파일, .gitignore 제외)만, 아니면 폴더 전체를 본다.
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  files=$(git ls-files --cached --others --exclude-standard -- .)
else
  files=$(find . -type f ! -name '.access_token' ! -name 'demo.env' ! -name '*.log' ! -name '*.pid' ! -path '*/__pycache__/*' | sed 's|^\./||')
fi

# 1) 127.0.0.1 과 0.0.0.0 이 아닌 IPv4 주소
hits=$(printf '%s\n' "$files" | while IFS= read -r f; do
  [ -f "$f" ] || continue
  grep -n -I -o -E '(^|[^0-9.])([0-9]{1,3}\.){3}[0-9]{1,3}([^0-9.]|$)' "$f" 2>/dev/null \
    | grep -v -E '(^|[^0-9.])(127\.0\.0\.1|0\.0\.0\.0)([^0-9.]|$)' \
    | sed "s|^|$f:|" || true
done)
if [ -n "$hits" ]; then
  echo "[문제] IP 주소로 보이는 값이 있습니다:"
  printf '%s\n' "$hits" | cut -c1-160
  bad=1
fi

# 2) 올라가면 안 되는 파일
for name in server/.access_token server/demo.env; do
  if printf '%s\n' "$files" | grep -q -x "$name"; then
    echo "[문제] $name 이 올라갈 파일에 들어 있습니다."
    bad=1
  fi
done

# 3) 접속 명령에 실제 계정이나 서버 이름이 적혀 있는지 (자리표시자 <...> 만 허용)
hits=$(printf '%s\n' "$files" | while IFS= read -r f; do
  [ -f "$f" ] || continue
  [ "$f" = "server/check_public.sh" ] && continue
  grep -n -I -E 'ssh +[^|]*[A-Za-z0-9._-]+@[A-Za-z0-9._-]+' "$f" 2>/dev/null | sed "s|^|$f:|" || true
done)
if [ -n "$hits" ]; then
  echo "[문제] 실제 계정@서버가 적힌 접속 명령이 있습니다:"
  printf '%s\n' "$hits" | cut -c1-160
  bad=1
fi

if [ "$bad" = 0 ]; then
  echo "문제 없음: 올라갈 파일에 서버 주소, 토큰, 서버별 설정이 없습니다."
fi
exit "$bad"
