#!/bin/zsh

set -euo pipefail

postgres_url="${BLOCKQUEUE_BENCH_POSTGRES_URL:-}"
if [[ -z "$postgres_url" ]]; then
  print -u2 "BLOCKQUEUE_BENCH_POSTGRES_URL is required"
  exit 2
fi

database=$(psql "$postgres_url" --no-psqlrc --tuples-only --no-align \
  --set ON_ERROR_STOP=1 --command 'SELECT current_database()')
if [[ "$database" != *_bench ]]; then
  print -u2 "refusing to reset PostgreSQL database '$database': name must end with _bench"
  exit 2
fi

psql "$postgres_url" --no-psqlrc --quiet --set ON_ERROR_STOP=1 \
  --command 'BEGIN; DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; COMMIT;'
print "reset PostgreSQL benchmark database: $database"
