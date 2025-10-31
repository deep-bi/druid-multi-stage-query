#!/usr/bin/env bash
set -euo pipefail

VER="${1:?druid_version required}"
JARS_ROOT="${2:?jars_root required}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRUID_URL="http://localhost:8888"
WORK="$ROOT/druid"
HOME="$WORK/apache-druid-${VER}"
TAR="$WORK/apache-druid-${VER}-bin.tar.gz"
QUERY_PATH="$ROOT/.github/resources/queries"
RESULTS_PATH="$ROOT/.github/resources/results"

require() {
  command -v "$1" >/dev/null || { echo "ERROR: missing $1"; exit 1; }
}

require curl; require jq
curl_ok() { curl -fsS "$@"; }

check_health() {
  local name="$1" port="$2"
  for _ in {1..120}; do
    if curl -fsS "http://localhost:${port}/status/health" >/dev/null 2>&1; then
      echo "$name healthy on port $port"
      return
    fi
    echo "waiting for $name..."
    sleep 3
  done
  echo "ERROR: $name failed health check"; exit 1
}

poll_query_state() {
  local qid="$1" t="$2" start=$(date +%s)
  while :; do
    local s
    s=$(curl_ok "${DRUID_URL}/druid/v2/native/statements/${qid}/" |
        jq -r '.state')
    s="${s:-UNKNOWN}"
    [[ "$s" =~ ^(SUCCESS|FAILED|CANCELED)$ ]] && { echo "$s"; return; }
    (( $(date +%s) - start > t )) && { echo "$s"; return; }
    sleep 2
  done
}

fetch_results() {
  local qid="$1"
  local url="${DRUID_URL}/druid/v2/native/statements/${qid}/results/"
  echo "fetching results for ${qid}..." >&2
  echo " -> GET ${url}" >&2

  local out
  if ! out=$(curl_ok "$url" 2>&1); then
    echo "ERROR: failed to fetch results for ${qid}" >&2
    echo "$out" >&2
    exit 1
  fi

  echo " -> parsing JSON" >&2
  jq -S . <<<"$out"
  echo " -> results fetched successfully" >&2
}

compare_exact() {
  local actual="$1" expected="$2"
  echo "comparing $actual to $expected..."
  diff -u <(jq -S . "$actual") <(jq -S . "$expected")
}

log() { printf "\n==> %s\n" "$*"; }
info() { printf " -> %s\n" "$*"; }
fail() { printf " !! %s\n" "$*" >&2; exit 1; }

download_tar() {
  mkdir -p "$WORK"; cd "$WORK"
  local U1="https://downloads.apache.org/druid/${VER}/apache-druid-${VER}-bin.tar.gz"
  local U2="https://archive.apache.org/dist/druid/${VER}/apache-druid-${VER}-bin.tar.gz"
  local TAR="apache-druid-${VER}-bin.tar.gz"

  echo "downloading Apache Druid $VER..."
  if ! curl -fsSLO "$U1"; then
    echo "primary URL failed, trying archive..."
    curl -fsSLO "$U2"
  fi

  echo "extracting $TAR..."
  tar -xzf "$TAR"
}


overlay_jars() {
  echo "overlaying jars..."
  rm -f "$HOME/lib/web-console-$VER.jar" || true
  cp -v "$JARS_ROOT/web-console-$VER.jar" "$HOME/lib/"
  rm -f "$HOME/extensions/druid-multi-stage-query/druid-multi-stage-query-$VER.jar" || true
  cp -v "$JARS_ROOT/druid-multi-stage-query-$VER.jar" "$HOME/extensions/druid-multi-stage-query"
}

start_micro() {
  echo "starting micro-quickstart..."
  cd "$HOME"
  nohup bash -lc "./bin/start-micro-quickstart" > "$ROOT/quickstart.log" 2>&1 &
  check_health "Router" 8888
  check_health "Coordinator" 8081
  check_health "Broker" 8082
  check_health "Historical" 8083
}

ingest_wikipedia() {
  log "Submitting Wikipedia ingestion task"
  cd "$HOME"
  local task
  task=$(curl -fsS -X POST -H 'Content-Type:application/json' \
         -d @quickstart/tutorial/wikipedia-index.json \
         http://localhost:8081/druid/indexer/v1/task | jq -r .task)
  info "task id: $task"

  for _ in {1..120}; do
    local st
    st=$(curl -fsS "${DRUID_URL}/druid/indexer/v1/task/${task}/status" | jq -r .status.status)
    [[ "$st" == "SUCCESS" ]] && { info "ingestion succeeded"; return; }
    [[ "$st" == "FAILED"  ]] && fail "ingestion failed"
    info "ingestion running..."
    sleep 3
  done
  fail "ingestion timed out"
}

native_tests() {
  mkdir -p "$ROOT/collected-results"
  run_query() {
    local qid qfile="$QUERY_PATH/$1.json"
    qid="query-$(jq -r '.context.queryId' "$qfile")"
    curl_ok -X POST -H 'Content-Type: application/json' "${DRUID_URL}/druid/v2/native/statements/" --data-binary @"$qfile" >/dev/null
    local st; st=$(poll_query_state "$qid" 180)
    [[ "$st" == "SUCCESS" ]] || { echo "::error::$1=$st"; exit 1; }
    fetch_results "$qid" > "$ROOT/collected-results/$1-actual.json"
    compare_exact "$ROOT/collected-results/$1-actual.json" "$RESULTS_PATH/$1.json" || { echo "::error::$1 mismatch"; exit 1; }
  }
  run_query groupby
  run_query scan
  local qid="query-$(jq -r '.context.queryId' "$QUERY_PATH/long.json")"
  curl_ok -X POST -H 'Content-Type: application/json' "${DRUID_URL}/druid/v2/native/statements/" --data-binary @"$QUERY_PATH/long.json" >/dev/null
  for _ in {1..60}; do
    local st
    st=$(curl_ok "${DRUID_URL}/druid/v2/native/statements/${qid}/" | jq -r '.state')
    [[ "$st" =~ ^(RUNNING)$ ]] && break || sleep 2
  done
  curl_ok -X DELETE "${DRUID_URL}/druid/v2/native/statements/${qid}/" >/dev/null
  for _ in {1..120}; do
    local state msg
    state=$(curl_ok "${DRUID_URL}/druid/v2/native/statements/${qid}/" | jq -r '.state')
    msg=$(curl_ok "${DRUID_URL}/druid/v2/native/statements/${qid}/" | jq -r '.errorDetails.errorMessage // empty')

    if [[ "$state" == "FAILED" && "$msg" == "Shutdown request from user" ]]; then
      echo "query $qid canceled by user"
      return
    fi

    [[ "$state" =~ ^(SUCCESS|FAILED|CANCELED)$ ]] && break
    sleep 2
  done

  echo "ERROR: query $qid did not report user-cancel failure"; exit 1
}

selenium_check() {
  local qid="query-$(jq -r '.context.queryId' "$QUERY_PATH/groupby.json")"
  local args=(-q -f "$ROOT/selenium-msq-test/pom.xml" test
              -DdruidBaseUrl="$DRUID_URL"
              -DtaskId="$qid"
              -Dheadless=true
              -DtimeoutSec=120
              -DexpectedResultsFilePath="$RESULTS_PATH/groupby.json")

  echo "starting Selenium UI check..."
  echo " -> query id: $qid"
  echo " -> base URL: $DRUID_URL"
  echo " -> running: mvn ${args[*]}"

  if mvn "${args[@]}"; then
    echo "Selenium UI check succeeded"
  else
    echo "ERROR: Selenium UI check failed"; exit 1
  fi
}

log "Download Druid"
download_tar

log "Overlay jars"
overlay_jars

log "Start micro-quickstart"
start_micro

log "Ingest wikipedia"
ingest_wikipedia

log "Native statements tests"
native_tests

log "Selenium UI check"
selenium_check

echo "tar=$TAR" >> "$GITHUB_OUTPUT"
echo "dir=$HOME" >> "$GITHUB_OUTPUT"
