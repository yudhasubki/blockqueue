#!/bin/zsh

set -euo pipefail

backend="${PERSIST_BACKEND:-sqlite}"
expected_messages="${EXPECTED_MESSAGES:-}"
expected_deliveries="${EXPECTED_DELIVERIES:-$expected_messages}"
wait_seconds="${PERSIST_WAIT_SECONDS:-60}"
postgres_url="${POSTGRES_VERIFY_URL:-}"

if [[ -n "$expected_messages" ]]; then
  if [[ "$expected_messages" != <-> || "$expected_deliveries" != <-> || "$wait_seconds" != <-> ]]; then
    print -u2 "EXPECTED_MESSAGES, EXPECTED_DELIVERIES, and PERSIST_WAIT_SECONDS must be non-negative integers"
    exit 2
  fi

  case "$backend" in
    sqlite)
      ;;
    postgres|postgresql|pgsql)
      if [[ -z "$postgres_url" ]]; then
        print -u2 "POSTGRES_VERIFY_URL is required when PERSIST_BACKEND=postgres"
        exit 2
      fi
      database=$(psql "$postgres_url" --no-psqlrc --tuples-only --no-align \
        --set ON_ERROR_STOP=1 --command 'SELECT current_database()')
      if [[ "$database" != *_bench ]]; then
        print -u2 "refusing PostgreSQL verification for database '$database': name must end with _bench"
        exit 2
      fi
      backend="postgres"
      ;;
    *)
      print -u2 "unsupported PERSIST_BACKEND '$backend'"
      exit 2
      ;;
  esac
fi

read_count() {
  local table="$1"
  if [[ "$backend" == "postgres" ]]; then
    psql "$postgres_url" --no-psqlrc --tuples-only --no-align \
      --set ON_ERROR_STOP=1 --command "SELECT COUNT(*) FROM $table;" 2>/dev/null || print 0
    return
  fi
  local database="${SQLITE_DB:-/private/tmp/blockqueue-k6.db}"
  sqlite3 "$database" "SELECT COUNT(*) FROM $table;" 2>/dev/null || print 0
}

ulimit -n "${K6_NOFILE:-4096}"
k6 "$@"

if [[ -n "$expected_messages" ]]; then
  deadline=$((SECONDS + wait_seconds))
  actual_messages=0
  actual_deliveries=0
  while (( SECONDS < deadline )); do
    actual_messages=$(read_count messages)
    actual_deliveries=$(read_count message_deliveries)
    if [[ "$actual_messages" == "$expected_messages" && "$actual_deliveries" == "$expected_deliveries" ]]; then
      print "verified persisted rows: messages=$actual_messages deliveries=$actual_deliveries"
      exit 0
    fi
    sleep 1
  done
  print -u2 "persisted row mismatch: expected_messages=$expected_messages expected_deliveries=$expected_deliveries messages=$actual_messages deliveries=$actual_deliveries"
  exit 1
fi
