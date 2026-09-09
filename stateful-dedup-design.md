# Stateful transaction deduplication design

Status: design proposal for later implementation and validation.

## Context

This representative bank workload retains approximately 24 crore transaction IDs per year, equivalent to 240 million IDs. A transaction ID is 24 characters long.

The existing deduplication design uses an AP `IMap` backed by PostgreSQL through an asynchronous write-behind MapStore. A new transaction is accepted using `IMap.putIfAbsent()`, while an existing ID is rejected as a duplicate.

The additional requirement is that a transaction can be unique when first received but fail during later business processing. The corrected transaction may then arrive again with the same ID and must be allowed to retry. Consequently, presence or absence of an ID is no longer sufficient; the deduplication entry must retain processing state.

## Important concurrency constraint

A retry must not be accepted merely because the current state is `PROCESSING`. That state cannot distinguish between an original attempt that is still running and an original attempt that has stopped making progress. Allowing another attempt whenever `PROCESSING` is observed could allow the original and retry attempts to execute concurrently.

A retry should instead require either:

- An explicit transition from the application to `RETRYABLE_FAILED`.
- An expired processing lease, used to recover an attempt whose processor has failed without reporting its outcome.

Every accepted attempt must have a monotonically increasing attempt token. Completion and failure operations must include this token and may update the entry only when the token still matches. This fences a late result from an older attempt after a newer retry has already started.

## Proposed state value

Use a small immutable value with explicit Compact serialization:

```java
final class DedupState {
    byte status;          // PROCESSING, RETRYABLE_FAILED, RETRYING, PROCESSED
    int attempt;
    long attemptToken;    // Fencing token/version
    long updatedAt;
    long leaseUntil;
}
```

Avoid storing textual processor names or failure messages in every entry. If a failure category is needed, store a small numeric reason code. The one-year expiration time can continue to use Hazelcast entry metadata and the PostgreSQL `expirationTime` column.

## State transitions

```mermaid
stateDiagram-v2
    [*] --> PROCESSING: ID absent; accept new attempt
    PROCESSING --> PROCESSED: Complete with matching token
    PROCESSING --> RETRYABLE_FAILED: Fail with matching token
    PROCESSING --> RETRYING: Lease expired; increment token
    RETRYABLE_FAILED --> RETRYING: Explicit retry; increment token
    RETRYING --> PROCESSED: Complete with matching token
    RETRYING --> RETRYABLE_FAILED: Fail with matching token
    PROCESSING --> PROCESSING: Live lease; reject in-flight duplicate
    RETRYING --> RETRYING: Live lease; reject in-flight duplicate
    PROCESSED --> PROCESSED: Reject completed duplicate
```

The claim result should distinguish at least:

- `ACCEPT_NEW`
- `ACCEPT_RETRY`
- `DUPLICATE_IN_FLIGHT`
- `DUPLICATE_COMPLETED`

## Atomic implementation

The claim operation should use an `EntryProcessor` rather than a client-side `get()` followed by `replace()`. The EntryProcessor runs against the entry on the partition owner and makes the state-dependent decision and update atomically in one map invocation. Hazelcast documents Entry Processors as a way to execute read/update logic on the member that owns an entry and avoid separate get and update network hops: [Hazelcast distributed computing documentation](https://docs.hazelcast.com/hazelcast/5.7/computing/distributed-computing).

A normal successful transaction has two calls:

1. `claim(id)` returns `ACCEPT_NEW` and the attempt token.
2. `complete(id, token)` moves the matching attempt to `PROCESSED`.

A transaction that fails and is later corrected has additional transitions:

1. `claim(id)` returns `ACCEPT_NEW`.
2. `fail(id, token)` moves the entry to `RETRYABLE_FAILED`.
3. A later `claim(id)` returns `ACCEPT_RETRY`, increments the attempt number and issues a new token.
4. `complete(id, newToken)` moves the retry to `PROCESSED`.

`complete()` and `fail()` must reject stale attempt tokens without changing the stored entry.

## AP consistency boundary

An `IMap` operation is atomic for a key on the current partition owner under normal cluster operation and member failure recovery. The AP map is not a linearizable CP data structure across a network split. If the requirement is that two attempts must never be accepted even during a split-brain scenario, AP `IMap` plus asynchronous PostgreSQL is not sufficient by itself. That stronger requirement would need a CP design, synchronous database arbitration, or an explicitly accepted availability-versus-consistency policy.

## MapStore and PostgreSQL design

Retain the one-second asynchronous write-behind MapStore for performance.

With `write-coalescing=true`, a quick `PROCESSING` to `PROCESSED` transition will generally cause only the latest value to be written to PostgreSQL within the write-delay window. Hazelcast defines write coalescing as retaining only the latest store operation for a key within the delay window: [Hazelcast MapStore configuration](https://docs.hazelcast.com/hazelcast/5.7/mapstore/configuration-guide).

This has the following durability implications:

- A member failure is protected by the IMap backup.
- A complete cluster loss before a transient state reaches PostgreSQL could lose that transient state.
- If every intermediate state must survive complete cluster loss, write coalescing is insufficient. That requirement would need synchronous persistence, disabled coalescing, or a separate durable attempt journal.

Use typed PostgreSQL columns rather than JSONB for this small, frequently updated state:

```sql
CREATE TABLE transaction_dedup_state (
    id              VARCHAR(24) PRIMARY KEY,
    status          SMALLINT NOT NULL,
    attempt         INTEGER NOT NULL,
    attempt_token   BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL,
    lease_until     BIGINT,
    expiration_time BIGINT NOT NULL
);
```

The MapStore should batch typed upserts and preserve the current one-year expiration behaviour.

## Simulator test design

Add a dedicated `StatefulDeduplicationTest` with independently configurable percentages for:

- New transaction claims.
- Completed duplicates.
- Concurrent in-flight duplicates.
- Retryable business failures.
- Redelivery of retryable failures.
- Expired-lease recovery.
- Successful completion versus retryable failure.
- Processing duration.
- Target TPS and thread count.

The test should maintain four efficient key pools rather than generating or caching the entire production domain in clients:

1. Pre-existing `PROCESSED` keys for ordinary duplicate attempts.
2. New unique keys generated cheaply per worker and thread.
3. A small hot-key set deliberately shared across workers to create concurrent claim races.
4. Keys deliberately moved into `RETRYABLE_FAILED` for retry testing.

Record separate latency probes for:

- `claimNew`
- `claimRetry`
- `duplicateProcessed`
- `duplicateInFlight`
- `complete`
- `fail`

The full test should include a warmup and a fixed-rate measured phase. The annual volume determines retained capacity, while the measured rate must be based on the bank's peak TPS rather than the annual average.

## Verification design

The global verification phase should flush the MapStore and verify all of the following:

- Exactly one attempt token is accepted for a transaction at a time.
- A stale token cannot complete or fail a newer attempt.
- Every accepted successful attempt finishes in `PROCESSED`.
- Every intentionally failed attempt is `RETRYABLE_FAILED` or was subsequently accepted as a retry.
- `PROCESSED` and live in-flight duplicates are rejected with the correct decision.
- IMap state counts match PostgreSQL counts after `map.flush()`.
- PostgreSQL and IMap agree on status, attempt and token for a deterministic verification sample.
- No accepted state is lost during a Hazelcast member failure.

For large-scale population, PostgreSQL should be populated by a Kubernetes job inside the VPC and Hazelcast should load from PostgreSQL. The 240 million records must not be generated or transferred from external Simulator load generators.

## Capacity estimate for 24 crore IDs

Twenty-four crore IDs is 240 million retained entries.

### Raw data

| Component | Calculation | Decimal size | Binary size |
| --- | ---: | ---: | ---: |
| 24-byte ID | 240M × 24 bytes | 5.76 GB | 5.36 GiB |
| Integer state only | 240M × 4 bytes | 0.96 GB | 0.89 GiB |
| Raw ID plus integer | 240M × 28 bytes | 6.72 GB | 6.26 GiB |

The raw 6.26 GiB figure is not a Hazelcast capacity figure. It excludes serialized-data headers, the additional state fields, map record metadata, native allocator block rounding, fragmentation and backup copies.

### Hazelcast estimate

Until the Compact value and actual map entry cost are measured, budget approximately 160 to 224 bytes per logical entry:

| Capacity layer | Estimated size |
| --- | ---: |
| Primary logical dataset | 35.8–50.1 GiB |
| Primary plus one synchronous backup | 71.5–100.1 GiB cluster-wide |
| Normal distribution across three members | 23.8–33.4 GiB per member |
| Distribution across two surviving members after one member fails | 35.8–50.1 GiB per surviving member |
| With approximately 30% failure and fragmentation headroom | 46.5–65.1 GiB per member |

The estimate is intentionally conservative because the POOLED native-memory allocator uses power-of-two block sizes and is subject to internal and per-thread fragmentation: [Hazelcast POOLED allocator documentation](https://docs.hazelcast.com/hazelcast/5.7-snapshot/storage/pooled-native-memory-allocator).

The current 20 GiB native-memory allocation per Hazelcast member is not sufficient. A reasonable initial configuration for validation is:

- Three Hazelcast members on a machine class with approximately 120 GB memory, such as the `c2-standard-30` class.
- Approximately 12–16 GiB heap per member.
- Approximately 72–80 GiB native memory per member.
- One synchronous IMap backup.

This is a starting point, not a final production size. First load one million state entries, measure Hazelcast primary and backup entry-memory cost, `memory.usedNative`, and `memory.usedMetadata`, and extrapolate the observed bytes per entry. Hazelcast exposes map primary/backup memory-cost and native-memory metrics for this purpose: [Hazelcast metrics documentation](https://docs.hazelcast.com/hazelcast/5.7/list-of-metrics).

### PostgreSQL estimate

For the narrow typed table, primary-key index and expiration support, expect approximately 40–55 GiB at 240 million rows before operational headroom.

- A 100 GiB PostgreSQL volume is too tight.
- A 200 GiB `premium-rwo` volume is a reasonable minimum.
- A 300 GiB volume is safer if retry/update churn, WAL retention, vacuum activity or bulk expiry cleanup is significant.

### Average versus peak traffic

240 million new IDs per year averages approximately 7.6 new IDs per second. With two state transitions for a normally successful transaction, that is approximately 15.2 state operations per second before duplicates and retries. These averages are useful for annual storage calculations but are not appropriate performance targets; Simulator must use the bank's peak transaction rate and realistic duplicate/retry bursts.

## Open workload questions

The following must be confirmed before implementation:

1. What is the peak TPS and expected burst duration?
2. What percentages of arrivals are completed duplicates, concurrent duplicates and genuine retries?
3. Does the application explicitly report `RETRYABLE_FAILED`, or must retry eligibility be inferred only through a processing lease timeout?
4. What processing lease duration is safe for the longest valid transaction?
5. Must intermediate states survive complete cluster loss, or is protection against an individual member failure sufficient?
6. Is strict duplicate prevention required during network partitions, or is AP availability with documented split-brain behaviour acceptable?

## Planned validation sequence

1. Finalize bank workload semantics and the state value.
2. Implement the Compact serializer and assert its exact serialized size.
3. Implement atomic claim, complete and fail EntryProcessors with token fencing.
4. Implement the typed MapStore and PostgreSQL schema.
5. Build a one-million-entry sizing test and measure actual bytes per entry.
6. Adjust native memory and PostgreSQL disk sizing from the measurement.
7. Run the fixed-rate functional/performance workload.
8. Add member-failure and delayed-network chaos scenarios.
9. Decide separately whether split-brain correctness requires a CP or synchronous database design.
