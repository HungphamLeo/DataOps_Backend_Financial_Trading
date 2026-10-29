# ADR-001: Kafka KRaft Production Hardening (Quorum + Idempotent Producer + Fencing Token)

- **Status:** Accepted (implementation partially complete — see "Implementation status")
- **Date:** 2026-10
- **Plan reference:** `PLAN_REPO03_Trading_production.md` — ST01
- **Scope:** `infra_dataops_finance_trading/`, `dataops_stream_producer/`, `dataops_backend_api/`

## Context

The platform runs a 3-broker Kafka cluster in KRaft mode (no ZooKeeper) carrying real-time
financial trade and order-book data.

Main risks:

1. **Data loss on broker failure** — default Kafka settings (`acks=1`, `min.insync.replicas=1`,
   unclean leader election) can acknowledge a write that exists on only one replica.
2. **Duplicates on retry** — a producer that retries after a leader failover can write the same
   message twice.
3. **Zombie Leader** — a node paused by a long Stop-The-World GC (e.g. 15 s) is replaced by a new
   leader, then resumes still believing it is leader and may write stale data.

For financial data we choose **consistency over availability** (CP in CAP terms): it is better to
reject writes temporarily than to accept data that can be lost or diverge.

## Decision

### 1. Quorum configuration (broker side) — `docker-compose.yml`

| Setting | Value | Reason |
|---|---|---|
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | 3 | Every topic is replicated on all 3 brokers |
| `KAFKA_MIN_INSYNC_REPLICAS` | 2 | Quorum: W + R > N with N=3, W=2, R=2 |
| `KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE` | `false` | Never elect an out-of-sync replica; prefer unavailable over inconsistent |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | 3 | `__consumer_offsets` survives one broker loss |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | 3 | `__transaction_state` is required by idempotent/transactional producers |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | 2 | Quorum write for the transaction log |
| `CLUSTER_ID` | fixed, identical on all brokers | Stable KRaft metadata identity; a new ID means a fresh cluster and lost offsets |

### 2. Producer (client side) — `dataops_stream_producer/pkg/producer/producer.go`

- `Producer.RequiredAcks = sarama.WaitForAll` — wait for all in-sync replicas.
- `Producer.Idempotent = true` — broker de-duplicates by (producer id, partition, sequence).
- `Net.MaxOpenRequests = 1` — required by sarama together with idempotence; keeps strict ordering.
- `Producer.Partitioner = NewBinancePartitioner` (`transactional_producer.go`):
  `BTC*` → partition 0, `ETH*` → partition 1, all other symbols hashed with FNV-32a into
  partitions `2..N-1`. The same key always maps to the same partition (per-symbol ordering).
  Altcoins do **not** get dedicated partitions; several symbols can share one.

`acks=all` only gives the intended guarantee together with `min.insync.replicas=2`; with
`min.insync.replicas=1` the ISR could shrink to one replica and `acks=all` would degrade to `acks=1`.

### 3. Fencing Token — `dataops_backend_api`

- Table `cluster_configs` (GORM model `models.ClusterConfig`), singleton row `id=1`, holding
  `cluster_id`, `current_token` (monotonic `BIGINT`), `last_leader_id`, `updated_at`.
  Created by `AutoMigrate` and seeded in `repository.InitializeDatabase`.
- `ClusterConfigRepository.IncrementToken(newLeaderID)` — increments the token inside a
  transaction using `SELECT ... FOR UPDATE`, so concurrent callers cannot lose an increment.
- `ClusterConfigRepository.ValidateFencingToken(token)` — rejects any token lower than
  `current_token`; the caller should answer HTTP 409 and not retry.
- Rationale for enforcing at the storage layer: neither producer nor broker holds shared global
  state; the database is the single place that can order tokens.

### 4. Topic provisioning

- `kafka-init` in `docker-compose.yml` creates topics at stack start-up.
- `infra_dataops_finance_trading/scripts/create_topics.sh` is the standalone, idempotent
  equivalent (`--if-not-exists`, supports `DRY_RUN=1`) for staging/production or re-creation.
  Topic names must match `dataops_stream_producer/constants/topics.go`.
- Auto-creation of topics is avoided on purpose: a mistyped topic name would silently create a
  topic with replication factor 1.

## Implementation status (verified against the repository)

| Item | State |
|---|---|
| Broker quorum settings (section 1) | Done in all three brokers |
| Producer settings (section 2) | Done |
| `ClusterConfig` model, repository, migration + seed | Done; files are syntactically valid (`gofmt -e`). Not compiled: the `dataops_backend_api` module currently fails to build because of pre-existing git merge-conflict markers (see Follow-ups) |
| `scripts/create_topics.sh` | Done; `bash -n` passes; topic list aligned with `constants/topics.go` |
| **Partition count of `binance.trade.ticks` / `binance.orderbook.snapshot` in `kafka-init`** | **Still 6.** `create_topics.sh` uses 12. Plan ST01 asks for 12. Because both use `--if-not-exists`, whichever runs first wins; an existing topic must be altered with `kafka-topics --alter --partitions 12` |
| **Fencing token enforcement** | **Not wired.** `ValidateFencingToken` / `IncrementToken` / `NewClusterConfigRepo` exist but are not called from any handler or election listener yet |

## Consequences

**Positive**

- No acknowledged data loss when one of three brokers fails (two in-sync replicas remain).
- Duplicates caused by producer retries after failover are suppressed.
- Once wired, a zombie writer carrying an old token is rejected deterministically.

**Costs and trade-offs**

- Write latency increases (roughly a few ms) because each write waits for two replicas.
- With `min.insync.replicas=2`, losing **two** brokers makes partitions reject writes
  (`NotEnoughReplicas`). This is the intended CP behaviour, but producers must handle the error
  (retry window, DLQ — see `dlq.go`).
- `MaxOpenRequests=1` caps per-connection throughput; acceptable at current volume.
- Partition count can only be increased, never decreased, and increasing it changes the
  key → partition mapping for hashed symbols (ordering is only preserved within a partition
  from that point on). Do it during a quiet window.
- The fencing check depends on MySQL. Behaviour when MySQL is unreachable (fail-open vs
  fail-closed) is **not yet decided**; it must be chosen explicitly when the check is wired in.

## Follow-ups

1. Align `kafka-init` partitions with the plan (12 for the two high-volume topics), or document
   why 6 is kept.
2. Wire `ValidateFencingToken` / `IncrementToken` into the write path and election handling, and
   decide fail-open vs fail-closed.
3. Add unit tests for `clusterConfigRepo` (token monotonicity, concurrent `IncrementToken`).
4. Resolve the unresolved merge-conflict markers (`<<<<<<< HEAD` ... `>>>>>>> 20261010_data_processing`)
   in `dataops_backend_api` (`configuration/configuration.go`, `models/tcbs_account.go`,
   `services/*`, `api/rest/*`). They block `go build` for the whole module, so the fencing-token
   code cannot be compiled or tested until this is done.
5. ST02 (static membership) and ST03 (DLQ circuit breaker) build on this configuration.
