# Benchmarking BlockQueue

Use fresh databases and report the median of at least five trials. Async
admission throughput and durable commit latency are different guarantees and
must be reported separately.

## In-process Go benchmarks

```bash
go test -run '^$' \
  -bench 'BenchmarkPublishAsync|BenchmarkPublishDurable|BenchmarkBatchPublishDurable100|BenchmarkClaim100|BenchmarkClaimAndAck|BenchmarkBatchAck100|BenchmarkBatchNack100|BenchmarkNackFailure' \
  -benchmem -count=5 .
```

The benchmark setup uses the public queue API, strict durability, one topic,
and one subscriber. SQLite runs by default. To run the identical cases against
an isolated PostgreSQL schema, point the harness at a dedicated database whose
name ends in `_bench`:

```bash
BLOCKQUEUE_BENCH_POSTGRES_URL='postgres://postgres:postgres@127.0.0.1:5432/blockqueue_bench?sslmode=disable' \
  go test -run '^$' \
  -bench 'BenchmarkPublishAsync|BenchmarkPublishDurable|BenchmarkBatchPublishDurable100|BenchmarkClaim100|BenchmarkClaimAndAck|BenchmarkBatchAck100|BenchmarkBatchNack100|BenchmarkNackFailure' \
  -benchmem -benchtime=10x -count=5 .
```

Each PostgreSQL benchmark creates and drops a random schema. The harness refuses
database names without the `_bench` suffix and verifies exact canonical-message
and delivery row counts after every case. Async timing stops before the writer
barrier; cleanup still drains the admitted backlog.
Use a fixed `-benchtime=Nx` for PostgreSQL multi-case runs: adaptive calibration
can admit a very large async backlog, whose required post-timing durable drain
then dominates total suite time.

The release comparison includes publish, claim, set-based batch ACK/NACK, and
the single-NACK failure path (including `delivery_errors`). Compare each case
to the same backend and durability baseline; the median of five fresh trials
may regress by at most 5%.

v0.2 hardening seed baseline (Apple M1, strict durability, `-benchtime=10x`,
five fresh isolated trials):

| Operation | SQLite median | PostgreSQL median | Unit |
|---|---:|---:|---|
| Claim100 | 2.528 ms | 8.071 ms | one 100-message claim |
| BatchAck100 | 2.979 ms | 11.103 ms | one 100-message transaction |
| BatchNack100 | 4.791 ms | 4.679 ms | one 100-message transaction plus error history |
| NackFailure | 0.186 ms | 0.779 ms | one NACK plus error-history insert |

Every trial verified exact canonical-message and delivery counts; NACK trials
also verified one `delivery_errors` row per operation. `BatchNack100` was added
after the seed run and its value is the first five-trial baseline for that path.
These numbers establish the first comparable baseline for the new paths, so the
5% regression gate applies to subsequent revisions rather than being inferred
against a different pre-v0.2 workload.

The 2026-07-12 v0.3 release-candidate run used Go 1.25.12, PostgreSQL 16.9,
strict durability, `-benchtime=10x`, and five fresh databases/schemas on the
same Apple M1 host:

| Operation | SQLite median | PostgreSQL median | Unit |
| --- | ---:| ---:| --- |
| PublishDurable | 0.201 ms | 0.985 ms | one committed message |
| PublishAsync | 1.554 us | 1.638 us | one process-local admission |
| BatchPublishDurable100 | 1.884 ms | 3.666 ms | one committed 100-message batch |
| Claim100 | 2.342 ms | 8.091 ms | one 100-message claim |
| BatchAck100 | 2.925 ms | 10.895 ms | one 100-message transaction |
| BatchNack100 | 4.804 ms | 4.276 ms | one 100-message transaction plus error history |
| NackFailure | 0.183 ms | 0.773 ms | one NACK plus error-history insert |

Every case verified its exact canonical-message and delivery counts. Against
the seed table, the largest regression was SQLite `BatchNack100` at 0.27%, so
the release-candidate run passed the 5% gate. A less noisy five-trial
`-benchtime=100x` diagnostic measured claim plus ACK at 0.496 ms on SQLite and
0.778 ms on PostgreSQL.

Caller-owned SQLite transactions intentionally hold its single writer lock.
The diagnostic benchmark measures how directly that hold time appears in a
queued durable writer barrier:

```bash
go test -run '^$' -bench BenchmarkSQLiteCallerTransactionContention \
  -benchtime=1x -count=5 .
```

It runs 10 ms, 100 ms, and 1 s hold cases. This is a documented contention cost,
not a throughput gate: application callbacks should contain only short local
database work and must not perform network I/O.

To verify that historical scheduler growth never returns to the claim hot path,
run the opt-in one-million-row fixture with fixed trials:

```bash
BLOCKQUEUE_BENCH_LARGE_HISTORY=1 \
  go test -run '^$' -bench '^BenchmarkClaim100WithMillionScheduleRuns/sqlite$' \
  -benchtime=100x -count=5 .

BLOCKQUEUE_BENCH_LARGE_HISTORY=1 \
BLOCKQUEUE_BENCH_POSTGRES_URL='postgres://.../blockqueue_bench?sslmode=disable' \
  go test -run '^$' -bench '^BenchmarkClaim100WithMillionScheduleRuns/postgres$' \
  -benchtime=100x -count=5 .
```

Compare the five-trial median and tail spread with `BenchmarkClaim100` from the
same machine. The median regression gate is 5%; retain every raw trial because
local PostgreSQL tail variance can be wider than the median.

The 2026-07-11 Apple M1 validation used Go 1.25.12 and five paired 100-iteration
trials after normalizing fixture WAL and statistics. SQLite measured 6.25 ms/op
without history and 5.79 ms/op with 1,000,000 runs (`-7.4%`). PostgreSQL
measured 55.70 ms/op without history and 52.50 ms/op with 1,000,000 runs
(`-5.7%`). Historical growth therefore caused no regression. PostgreSQL
absolute latency changes after repeatedly creating and dropping multi-million-
row schemas, so never compare a warmed pre-fixture trial against a post-fixture
trial.

The same audit recorded fixed-count before/after hot-path medians. SQLite
`Claim100` improved from 6.28 to 6.02 ms (`-4.1%`), single claim+ACK from 0.62
to 0.50 ms (`-18.8%`), and `BatchAck100` stayed flat at 19.08/19.10 ms.
PostgreSQL stayed within noise: 27.82/27.91 ms, 0.90/0.91 ms, and 56.60/56.30
ms respectively. Idle durable publish with a 50 ms configured flush interval
fell from 50.2 ms to 0.18 ms because durable waiters trigger an immediate
drain-to-empty flush; async-only traffic retains the batching window.

## HTTP/k6 benchmark: SQLite

Run from the repository root with a fresh database:

```bash
rm -f /private/tmp/blockqueue-k6.db /private/tmp/blockqueue-k6.db-shm /private/tmp/blockqueue-k6.db-wal
go run ./cmd/blockqueue migrate -config benchmark/config-sqlite.yaml
go run ./cmd/blockqueue http -config benchmark/config-sqlite.yaml
```

Single-message closed-loop trial against strict durability:

```bash
EXECUTOR=constant-vus VUS=100 DURATION=10s \
  zsh benchmark/run-k6.sh run \
  --summary-trend-stats='med,p(95),p(99.9)' benchmark/publish.js
```

`config-single.yaml` explicitly uses balanced SQLite durability to match the
historical `synchronous=NORMAL` baseline. Start both `migrate` and `http` with
that file when reproducing the historical numbers below. Production defaults,
`config-sqlite.yaml`, and the in-process benchmarks use strict durability, so do not
compare those result sets.

Historical SQLite balanced HTTP acceptance run (Apple M1, darwin/arm64,
100 VUs, five fresh 10-second trials):

| Trial | req/s | median | p95 | p99.9 | persisted messages/deliveries |
|---|---:|---:|---:|---:|---:|
| 1 | 39,839 | 1.18 ms | 6.55 ms | 26.65 ms | 398,640 / 398,640 |
| 2 | 40,516 | 1.25 ms | 6.32 ms | 26.09 ms | 405,688 / 405,688 |
| 3 | 41,905 | 1.17 ms | 6.37 ms | 25.51 ms | 419,309 / 419,309 |
| 4 | 38,144 | 1.18 ms | 6.88 ms | 27.92 ms | 382,214 / 382,214 |
| 5 | 41,429 | 1.25 ms | 6.16 ms | 24.77 ms | 414,714 / 414,714 |
| **Median** | **40,516** | **1.18 ms** | **6.37 ms** | **26.09 ms** | **exact in every trial** |

The comparable baseline is 41,128 req/s. The median regression is 1.49%, which
passes the 5% gate. Every request succeeded and shutdown drained all admitted
messages before row counts were checked.

The 2026-07-11 audit changes were also measured against an archived `HEAD`
snapshot on the same busy host, alternating fresh databases to remove machine
state as a confounder:

| Build | five-trial req/s | median req/s | median latency | median p95 | median p99.9 |
|---|---|---:|---:|---:|---:|
| archived `HEAD` | 36,522 / 36,993 / 31,504 / 37,718 / 38,288 | 36,993 | 1.18 ms | 6.56 ms | 32.26 ms |
| current | 36,844 / 33,881 / 37,983 / 38,356 / 37,932 | 37,932 | 1.17 ms | 6.36 ms | 30.78 ms |

Current throughput is 2.54% higher, so the adaptive durable flush change passes
the no-more-than-5% regression gate while preserving the async group-commit
window. A separate current verification trial shut down with exactly 280,510
canonical messages and 280,510 delivery rows for 280,510 successful responses.

The 2026-07-12 v0.3 release-candidate rerun used the same balanced profile,
k6 0.36.0, 100 VUs, and five fresh 10-second cycles:

| Trial | req/s | median | p95 | p99.9 | persisted messages/deliveries |
| --- | ---:| ---:| ---:| ---:| ---:|
| 1 | 36,725 | 1.24 ms | 6.64 ms | 30.11 ms | 367,658 / 367,658 |
| 2 | 35,069 | 1.27 ms | 7.00 ms | 40.15 ms | 350,817 / 350,817 |
| 3 | 36,410 | 1.25 ms | 6.73 ms | 31.83 ms | 364,330 / 364,330 |
| 4 | 37,332 | 1.20 ms | 6.68 ms | 29.02 ms | 373,582 / 373,582 |
| 5 | 37,095 | 1.21 ms | 6.61 ms | 28.93 ms | 371,168 / 371,168 |
| **Median** | **36,725** | **1.24 ms** | **6.68 ms** | **30.11 ms** | **exact in every trial** |

The full spread is retained. The median is 3.18% below the latest
comparable 37,932 req/s result and therefore remains inside the 5% gate.

Batch burst:

```bash
RATE=1000 PUBLISH_BATCH_SIZE=100 DURATION=10s \
  PRE_ALLOCATED_VUS=50 MAX_VUS=200 \
  zsh benchmark/run-k6.sh run benchmark/publish.js
```

Sustained SQLite publish must cross the adaptive checkpoint's two-minute maximum
defer and verify exact canonical and delivery row counts. This example expects
130,000 messages from 100 iterations/second × 10 messages × 130 seconds:

```bash
RATE=100 PUBLISH_BATCH_SIZE=10 DURATION=130s \
  PRE_ALLOCATED_VUS=50 MAX_VUS=200 EXPECTED_MESSAGES=130000 \
  zsh benchmark/run-k6.sh run benchmark/publish.js
```

The verification waits for the admitted writer backlog to commit, then compares
both `messages` and `message_deliveries`. It fails if k6 drops iterations, the
writer loses data, or the database was not fresh.

Consume and batch ACK with receipt tokens:

```bash
VUS=20 DURATION=15s zsh benchmark/run-k6.sh run benchmark/consume.js
```

## HTTP/k6 benchmark: PostgreSQL

Use a dedicated database named with the `_bench` suffix. The reset script is
destructive only inside that guarded database and refuses any other name. Stop
the benchmark server before resetting it.

```bash
createdb blockqueue_bench
export BLOCKQUEUE_BENCH_POSTGRES_URL='postgres://postgres:postgres@127.0.0.1:5432/blockqueue_bench?sslmode=disable'
export BLOCKQUEUE_BENCH_POSTGRES_HOST='127.0.0.1'
export BLOCKQUEUE_BENCH_POSTGRES_USER='postgres'
export BLOCKQUEUE_BENCH_POSTGRES_PASSWORD='postgres'

zsh benchmark/reset-postgres.sh
go run ./cmd/blockqueue migrate -config benchmark/config-postgres.yaml.example
go run ./cmd/blockqueue http -config benchmark/config-postgres.yaml.example
```

Committed single-message trial:

```bash
EXECUTOR=constant-vus VUS=100 DURATION=10s WAIT_FOR_COMMIT=1 \
  zsh benchmark/run-k6.sh run \
  --summary-trend-stats='med,p(95),p(99.9)' benchmark/publish.js
```

The PostgreSQL benchmark config uses a 100-message writer batch and 1 ms
latency fallback. A durable closed-loop test cannot have more outstanding
messages than its VU count, so a batch much larger than 100 would never fill
and would add the fallback delay to nearly every commit.

Latest committed-publish run (PostgreSQL 16.9 through pgx, native UUID keys,
Apple M1, strict durability, 100 VUs, one subscriber, five fresh 10-second
trials):

| Trial | committed msg/s | median | p95 | p99.9 | persisted messages/deliveries |
|---|---:|---:|---:|---:|---:|
| 1 | 10,711 | 7.55 ms | 18.07 ms | 119.57 ms | 107,268 / 107,268 |
| 2 | 13,690 | 6.53 ms | 12.05 ms | 49.42 ms | 137,094 / 137,094 |
| 3 | 13,716 | 6.62 ms | 11.93 ms | 35.57 ms | 137,402 / 137,402 |
| 4 | 6,755 | 8.73 ms | 38.38 ms | 732.65 ms | 67,844 / 67,844 |
| 5 | 8,111 | 8.07 ms | 34.00 ms | 178.38 ms | 81,273 / 81,273 |
| **Median** | **10,711** | **7.55 ms** | **18.07 ms** | **119.57 ms** | **exact in every trial** |

All 530,881 publishes returned after commit with zero HTTP failures. The median
throughput is 1.36% below the previous 10,859 msg/s baseline and remains inside
the 5% release gate. The wide trial range is retained deliberately: the load
generator, server, and PostgreSQL shared one development host, so this is a
local regression baseline rather than a general PostgreSQL capacity claim.

The 2026-07-12 v0.3 release-candidate rerun used the same strict committed
profile and five fresh database cycles:

| Trial | committed msg/s | median | p95 | p99.9 | persisted messages/deliveries |
| --- | ---:| ---:| ---:| ---:| ---:|
| 1 | 14,160 | 6.40 ms | 10.85 ms | 48.74 ms | 141,806 / 141,806 |
| 2 | 14,977 | 6.15 ms | 9.68 ms | 37.67 ms | 149,910 / 149,910 |
| 3 | 13,677 | 6.17 ms | 12.74 ms | 49.61 ms | 136,936 / 136,936 |
| 4 | 9,658 | 6.55 ms | 34.02 ms | 91.51 ms | 96,751 / 96,751 |
| 5 | 14,188 | 6.32 ms | 11.32 ms | 48.19 ms | 142,089 / 142,089 |
| **Median** | **14,160** | **6.32 ms** | **11.32 ms** | **48.74 ms** | **exact in every trial** |

The noisy fourth trial is retained. Median committed throughput is 32.20%
above the previous 10,711 msg/s baseline, with no HTTP failures.

Run a sustained async trial and verify both tables through a separate PostgreSQL
connection. Keep the verifier URL in an environment variable so credentials are
not passed as command-line flags or committed to the repository.

```bash
RATE=100 PUBLISH_BATCH_SIZE=10 DURATION=130s \
  PRE_ALLOCATED_VUS=50 MAX_VUS=200 EXPECTED_MESSAGES=130000 \
  PERSIST_BACKEND=postgres POSTGRES_VERIFY_URL="$BLOCKQUEUE_BENCH_POSTGRES_URL" \
  zsh benchmark/run-k6.sh run benchmark/publish.js
```

`run-k6.sh` also refuses PostgreSQL verifier databases without the `_bench`
suffix. Run at least five fresh reset/migrate/server/trial cycles and report the
median; SQLite and PostgreSQL results are separate baselines.

## Environment variables

- `BASE_URL` defaults to `http://127.0.0.1:8090`.
- `API_PREFIX` defaults to `/v1`.
- `TOPIC` and `SUBSCRIBER` select the queue resources.
- Publish: `RATE`, `DURATION`, `PRE_ALLOCATED_VUS`, `MAX_VUS`, `VUS`,
  `EXECUTOR`, `PUBLISH_BATCH_SIZE`, and `WAIT_FOR_COMMIT`. Set
  `WAIT_FOR_COMMIT=1` to measure committed publish latency/throughput instead
  of the default process-local async admission path.
- Consume: `VUS`, `DURATION`, and `LONG_POLL_TIMEOUT`.
- Persistence verification: `EXPECTED_MESSAGES`, `EXPECTED_DELIVERIES`,
  `PERSIST_BACKEND`, `SQLITE_DB`, `POSTGRES_VERIFY_URL`, and
  `PERSIST_WAIT_SECONDS`. `EXPECTED_DELIVERIES` defaults to
  `EXPECTED_MESSAGES`, which assumes one subscriber.
- `K6_NOFILE` controls the load generator descriptor limit.

Keep CPU frequency, durability mode, subscriber count, payload size, and
checkpoint interval identical when comparing revisions. A release passes the
single-message gate when the median of five fresh comparable trials regresses by
no more than 5%.
