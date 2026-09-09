# Simulate Bank Deduplication Use Cases

I want to try provide some experimental results to banks for their deduplication use cases. Deduplication is basically the deduplication of payments typically via their payment id.

## Goal

Create GCP cluster and run experiments on Hazelcast IMap + Postgres deduplication, Hazelcast IMap + Jet + Postgres deduplication, CP Map deduplication, and the Bank UPI merchant-profile workload. Use Hazelcast Simulator to execute tests and generate comparable results. Also do some tests with errors added by Chaos Mesh. With Chaos Mesh we can simulate a three-DC stretched cluster and introduce latency with jitter to model inter-AZ or inter-DC networks.

## Setup

### GKE (Done)

I have a project called /Users/raj/src/simu-chaos I have often used to build the base. Copy the content of the project here. Remove unnecessary bits. We can always pull stuff from the original project. Idea is that the Ansible script should be able to build the GKE project. Make sure we use latest Hz and other dependencies. Postgres needs to be installed too. I have postgres installation mechanism in /Users/raj/src/streaming-payments. To run Hz and postgres we may need 4 VMs ensuring that Hz gets at least 8 vcpu per pod and maybe 24GB RAM per instance. Stick to the zone and other choices as per the simu-chaos project.

### Simulator

Simulator must also gets deployed in the same cluster. The existing simu-chaos project deploys the simulator clients in VMs in the same VPC and the main simulator code is running on the local laptop. We will keep the same pattern here too.

Simulator has the ability to test plain IMap and CPMap tests but it does not have built in capability to test Jet. We have to think on how to test just dedup setup.

The Simulator project is at /Users/raj/src/hazelcast-simulator for reference.

### Number of payments

For a representative bank, assume 40 million payments per day across all payment rails, including 30 million UPI payments. Use 2,000 UPI payments per second as an illustrative busy-period rate. These are synthetic sizing assumptions, not figures attributed to a specific bank.

Deduplication is scoped to a single payment-id domain. The same DB table and IMap should not store payment IDs from more than one payment rail or deduplication domain. In practice, each domain would have its own table and IMap, and likely its own database and Hazelcast deployment. Therefore this project models one deduplication domain at a time and uses plain payment IDs as keys, with no domain discriminator.

For sizing we will simulate peak TPS by looking at a busy domain such as UPI. Actual TPS may be lower, but we will use 10K TPS for the dedupe test.

### Deduplication via IMap + Postgres

Create an IMap called `deduplicate` and have it backed by a MapStore. Ensure that we are loading only one year of data. In streaming-payments repo there is an IMap called `cdttrftx-txdedup` which loads only last 1 year (or so of ids). We can follow the same pattern. `cdttrftx-txdedup` is used by Jet for the purpose but we will have similar IMap for non Jet and Jet based dedup.

#### Data Load (Done for the 1M baseline)

For the current experiment, test against 1,000,000 existing 24-digit payment IDs. A one-shot Kubernetes Job bulk-generates this deterministic baseline inside PostgreSQL and verifies it before Hazelcast starts its EAGER MapStore load. The benchmark can choose a configured percentage from this existing range and generates the remainder as unique 24-digit IDs from a separate range. This avoids sending the seed data through Simulator and does not require a GCS artifact for a small synthetic dataset. Revisit a versioned object or database snapshot when the experiment moves to a much larger or non-synthetic corpus.

#### 10K TPS no Chaos tests

Once data load is done, we should have 5 min tests putIfAbsent. As of now no chaos, just simple tests. Throughput should be limited to 10K

The AP maps use Hazelcast native memory. The current three-member sizing is an 8 GiB JVM heap plus a 20 GiB pooled native-memory region inside a 36 GiB member container on `c2-standard-16` nodes. The larger sizing supports the separate million-entry, exact-10-KiB merchant-profile workload with one backup and a temporary member outage; the original deduplication baseline is much smaller.

### Bank UPI merchant profiles (implemented; cluster run pending)

The separate `merchant-profile` IMap is backed by PostgreSQL with a one-second asynchronous write-behind MapStore. In `dedup_test_mode=merchant_profile`, a Kubernetes Job generates one million 24-digit merchant keys and their large JSONB values inside the GKE VPC. The test calls `loadAll(true)` before warmup so neither seed generation nor initial loading is measured and no large dataset is uploaded from outside the VPC.

Each Compact value is exactly 10,240 bytes excluding its key, enforced by a unit test. Simulator clients create a small cache of immutable sample values during setup; measured writes choose from this cache so object construction is excluded, while Compact serialization and network transfer remain included. The workload runs at 5K aggregate TPS with separate 70% read and 30% update timesteps, a 60-second warmup, and a five-minute measurement. Global verification checks the one-million-entry IMap size, flushes write-behind, and compares distinct current-run updates in the IMap and PostgreSQL. An optional Chaos Mesh manifest fails one member pod for 30 seconds during the measured interval.

### Deduplication via CPMap (initial no-chaos test done)

CP is enabled with persistence on the same Hazelcast deployment used by AP and Jet; there is no separate CP deployment playbook. Each member has a 50 GiB persistent volume. AP and CP deduplication data are not tested concurrently.

The custom CPMap Simulator test populates its own 1M existing 24-digit IDs in parallel during Prepare, then runs the same 10K TPS, five-minute `putIfAbsent` mix. `cpGroupCount` controls the number of CPMap shards. Population and test operations both choose the shard with `floorMod(key.hashCode(), cpGroupCount)`, so a payment ID always reaches the same CP group. Three groups are used by default, keeping the estimated logical data comfortably below the default 100 MB CPMap limit per shard. Existing hot keys and group indexes are precomputed; new 24-digit keys use thread-local buffers so key generation is not a material part of timed execution.

### Deduplication via IMap + PostgreSQL + Jet (implemented; cluster run pending)

The same `deduplicate` IMap, PostgreSQL table, 1M baseline, MapStore, and native memory configuration are reused. Simulator writes uniquely keyed request envelopes to a transient IMap whose event journal is the Jet source. The Jet job performs asynchronous `putIfAbsent`, flushes the deduplicate IMap on snapshots, and writes an `ACCEPTED`, `DUPLICATE`, or `RETRY` decision to a result IMap.

The job is configured for AT_LEAST_ONCE processing, a 10-second snapshot interval, and an initial snapshot before processing. Each operation has a separate 24-digit correlation ID so a replay of the same operation is a retry, while a different operation with an already-seen 24-digit payment ID is a true duplicate.

The request, result, predicate, verification aggregator, and member-side database verification task use explicit Compact serializers registered on both members and Simulator clients.

Simulator measures input-map acknowledgement in the normal timestep and records end-to-end latency when the corresponding decision first appears in the result IMap. The result listener is only the latency signal. Final local verification drains outstanding results, and global `@Verify` compares the input and result IMap counts for the run and fails on any incorrect classification. It then flushes the MapStore-backed deduplication IMap and verifies that the active PostgreSQL row count for this run's new-payment prefix equals `ACCEPTED + RETRY`; duplicates must not add rows.

## Execution

I may list many things below. You can create agents and ensure that goal is met. You should execute in stages and use agents if parallel work can be done. Do not add unnecessary abstractions. Keep it simple and straight forward. I will be available to answer questions and provide guidance. You can also use the original projects for reference.

1. Make sure no licenses or keys are present in this repo
1. Do not create unnecessary abstractions.
1. Build the smallest working code but it should produce correct results.
1. We can add abstractions and complexity when we add business complexity
1. Just code, let me run all the commands on the cluster.
