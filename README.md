# Simulator for Bank Deduplication Experiments

This repo is bootstrapped for GKE-based Hazelcast Simulator experiments. The current base deploys:

- GKE in `asia-south1-a`
- 4 Hazelcast/Postgres worker nodes using `c2-standard-16`
- 2 client VMs in the same VPC
- Hazelcast Platform `5.7.0`
- Hazelcast Management Center `5.11.0`
- Hazelcast Enterprise Helm chart `5.15.0`
- Postgres `18.4`
- Chaos Mesh `2.8.3`

The deduplication model is intentionally scoped to one payment-id domain per deployment. A bank would create a separate IMap/table, and often a separate database or Hazelcast cluster, for each payment rail or deduplication domain. The `deduplicate` IMap therefore stores plain payment IDs only; there is no domain discriminator in the key or table schema.

## Prerequisites

- Ansible with `google.cloud`, `kubernetes.core`, `community.crypto`, and `community.general` collections.
- `gcloud`, `kubectl`, and `helm`.
- GCP project ID in `GCP_PROJECT_ID` (for example, `export GCP_PROJECT_ID=your-gcp-project-id`), or pass `-e project_id=your-gcp-project-id` to Ansible.
- Service account email in `GCP_SERVICE_ACCOUNT_EMAIL` for Chaos Mesh access, or pass `-e user_name=your-access-identity`.
- GCP credentials at `~/gcp/credentials.json`, or update `k8s/roles/gke/vars/main.yml`.
- A public SSH key at `~/.ssh/id_ed25519.pub`, or set `SIMU_DEDUP_SSH_PUB_KEY`.
- Postgres password in `SIMU_DEDUP_POSTGRES_PASSWORD` for every deployment.
- Hazelcast Enterprise license at `~/hazelcast/demo.license`, or set `HAZELCAST_LICENSE_KEY`.
- Hazelcast Simulator at `~/src/hazelcast-simulator`.

## Build Test Jar

```bash
cd ~/src/simu-dedup
mvn -DskipTests package
scripts/install-simulator-user-lib
```

When test Java changes while load-generator VMs already exist, run `inventory install simulator --hosts loadgenerators` after installing the JAR locally. Simulator copies `user-lib` during that installation; changing only a test-suite YAML file does not require reinstalling it.

## Deploy

The merchant-profile workload uses representative bank data and synthetic merchant records. Its mode is `merchant_profile`, with map `merchant-profile` and PostgreSQL table `merchant_profile`. Use a fresh deployment when switching from an older workload naming scheme: map/table names and Compact serializer type names have changed, and existing data is not migrated automatically.

The same Helm release provisions Hazelcast, Management Center, PostgreSQL, and all data structures needed by the AP, CP, Jet, and merchant-profile tests. The Hazelcast maps, MapStores, event journal, Compact serializers, JDBC data connection, and CP subsystem are all defined in the member YAML rendered by `k8s/roles/hz/templates/hazelcast-values.yml.j2`; there are no Hazelcast Operator custom resources or mode-dependent data-structure sections in that template. Before each member starts, an init container reads the PostgreSQL password from the `postgres-auth` Kubernetes Secret and writes it as a YAML-quoted value into the member-only configuration volume. Deployment fails if a password or CP leadership placeholder remains unresolved. `dedup_test_mode` identifies the intended workload and controls whether the long-running Jet job is submitted:

```bash
cd ~/src/simu-dedup

# IMap + Postgres
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=ap \
  --tags="gke,client,hz,postgres,chaos"

# CPMap test; PostgreSQL and the AP/Jet map definitions remain available
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=cp \
  --tags="gke,client,hz,postgres,chaos"

# IMap + PostgreSQL + Jet
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=jet \
  --tags="gke,client,hz,postgres,chaos"

# 10 KiB UPI merchant-profile IMap + PostgreSQL
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=merchant_profile \
  --tags="gke,client,hz,postgres,chaos"
```

The four workload nodes use `c2-standard-16` to retain the C2 CPU family while increasing each node to 16 vCPU and 64 GiB. Each of the three Hazelcast members requests 36 GiB, with an 8 GiB heap and a 20 GiB pooled native-memory region. The AP maps use `NATIVE` storage, PostgreSQL uses a 100 GiB `premium-rwo` volume, and CP persistence uses a 50 GiB `premium-rwo` volume per member. Do not keep the original deduplication data and the 10 KiB merchant-profile data loaded concurrently; use a fresh cluster or destroy the unused map before changing workloads.

After Postgres becomes Ready, a one-shot Kubernetes Job bulk-generates the original million-row deduplication baseline directly inside PostgreSQL. When `dedup_test_mode=merchant_profile`, the same in-cluster Job also generates the million-row merchant-profile baseline. The original deduplication IDs range from `100000000000000000000000` through `100000000000000000999999`; the merchant-profile merchant IDs range from `300000000000000000000000` through `300000000000000000999999`, and each merchant-profile row contains the JSONB representation of the fixed merchant profile used by the Compact serializer. The Job generates all large payload bytes inside the GKE VPC, so the data is never uploaded through a Simulator client or staged in GCS. The playbook allows up to one hour for generation and verifies the configured ranges before creating Hazelcast.

To change the baseline, override `dedup_seed_record_count`, `dedup_seed_start_id`, or `dedup_seed_id_prefix` as Ansible extra variables. The Simulator test's `existingKeyDomain` and prefix must describe the same key range. The Job is safe to rerun against a retained Postgres volume; it refreshes the configured seed range without removing other records.

After the playbook creates the client VMs:

1. Get the public IPs for VMs named `raj-dedup-client-*` from GCP.
2. Add those IPs under `loadgenerators.hosts` in `inventory.yaml`.
3. Confirm that `client-hazelcast.xml` contains the three private addresses reported by `kubectl get svc hz-primary-0 hz-primary-1 hz-primary-2`. The deployment renders these addresses automatically after the three internal per-member load balancers become ready.

The client configuration explicitly uses `cluster-routing mode="ALL_MEMBERS"` and lists every per-member internal load-balancer address. The Simulator VMs are in the same VPC and region, so they retain direct partition-owner routing without public Hazelcast endpoints.

## Run Simulator

```bash
cd ~/src/simu-dedup
source ../hazelcast-simulator/.venv/bin/activate
inventory install java --hosts 'loadgenerators'
inventory install simulator --hosts 'loadgenerators'
perftest run chaos_tests.yaml
```

For IMap + Postgres deduplication:

```bash
perftest run imap_postgres_tests.yaml
```

The scenario uses 4 clients at 2,500 operations/second each, for 10,000 operations/second in aggregate. `existingKeyPercentage` controls how many operations use an existing key. `existingKeySampleSize` controls the size of that duplicate hot set; its keys are evenly sampled from the seeded 1M-key domain. The remainder use unique 24-digit IDs beginning with `2`. Increment `newKeyRunId` before rerunning against the same cluster so the new-key subset does not overlap an earlier run. Each Simulator client builds only the configured existing-key sample during setup, and the timed path uses a random array lookup. New keys use a private character buffer per test thread and do not use shared counters or formatting utilities.

The final global verification flushes the MapStore-backed IMap and compares its active key count with PostgreSQL for the current run prefix (`newIdPrefix + newKeyRunId`). This excludes the seeded baseline and data from other runs. Tune `databaseConnectionName`, `databaseTableName`, and `databaseVerificationTimeoutSeconds` in `imap_postgres_tests.yaml` if the deployment names or expected flush time change.

For the 10 KiB UPI merchant-profile workload:

```bash
perftest run merchant_profile_postgres_tests.yaml
```

This scenario runs a 60-second warmup followed by a five-minute measured interval at 5,000 aggregate operations/second. Two separate timesteps report read and write latency: 70% call `IMap.get`, while 30% update existing keys with `IMap.set`. The map uses one-second asynchronous MapStore write-behind with write coalescing. Each client creates 256 exact-size sample values during setup and the timed write path only performs an array lookup, so object construction is excluded while Compact serialization and network transfer remain part of the measured operation.

The merchant-profile map uses LAZY MapStore initialization so it does not consume native memory during AP, CP, or Jet runs. Its global `Prepare` calls `loadAll(true)`, loading the million PostgreSQL rows into Hazelcast entirely within the VPC before warmup. The exact-size unit test requires the complete Compact `Data` value, excluding its IMap key, to be exactly 10,240 bytes. Change `updateRunId` before reusing retained merchant-profile data.

Final verification requires the IMap to remain at one million entries, flushes the write-behind queue, and compares the distinct keys carrying the current `updateRunId` in Hazelcast with rows carrying that marker in PostgreSQL. To exercise a recoverable member/pod failure, apply `chaos/merchant-profile-member-failure.yaml` during the measured interval; it fails `hz-primary-2` for 30 seconds. This is a member-process/pod test on a live node, not a whole-node-loss rescheduling guarantee.

For CPMap deduplication:

```bash
perftest run cpmap_tests.yaml
```

The custom test clears its CPMaps, then all Simulator clients populate the 1M existing IDs in parallel during `Prepare`. A CP-backed claim cursor prevents overlapping work. `cpGroupCount` creates that many CPMaps in distinct CP groups, and both population and timed traffic route a key with `floorMod(key.hashCode(), cpGroupCount)`. The default is three groups. Treat the group count as part of the data layout: changing it requires repopulation.

`existingKeySampleSize` and `existingKeyPercentage` have the same meaning as in the AP test. Existing keys and their group indexes are precomputed as a small hot set. Each new key is generated from a thread-local 24-character buffer and only its hash is calculated on the timed path. Increase `newKeyRunId` for every retained-state run, especially after an interrupted population.

For IMap + PostgreSQL + Jet deduplication:

```bash
perftest run jet_postgres_tests.yaml
```

The deployment inspects the cluster and submits the long-running `simu-dedup-jet` job with `hz-cli` from `hz-primary-0` whenever a running job with that name is absent. This check is independent of whether Helm reports a configuration change, so rerunning the Jet deployment repairs a missing job. Set `jet_job_redeploy: true` to cancel and replace an already-running job. Simulator writes uniquely keyed requests to `deduplicate-jet-input`; its event journal feeds Jet, which runs asynchronous `putIfAbsent` operations against the same MapStore-backed `deduplicate` IMap used by the AP benchmark. Jet writes one decision per operation ID to `deduplicate-jet-results`.

Payment IDs and operation IDs are separate 24-digit values. The payment ID is the business deduplication key. The operation ID correlates a submission with its result and lets AT_LEAST_ONCE replay be distinguished from a true duplicate:

- no previous value: `ACCEPTED`;
- previous value equals this operation ID: `RETRY`;
- previous value belongs to another operation: `DUPLICATE`.

Requests, decisions, and the distributed verification helpers use explicit Compact serializers registered in both the member and Simulator client configuration. Keep those registrations symmetric when adding or evolving fields.

The normal `submit` timestep measures input-map acknowledgement latency and TPS. A filtered result-map listener completes the `jetEndToEnd` probe when the decision becomes visible. Listener delivery is used only for latency: local verification falls back to batched result reads for lost listener events, and the final global `@Verify` compares this run's distributed input and result IMap counts and checks all result classifications. Fallback results are not added to the latency histogram.

The same global verification calls `flush()` on the MapStore-backed `deduplicate` IMap and then runs a count query on a Hazelcast member through the configured JDBC data connection. It counts active PostgreSQL rows under the current run's new-payment prefix and requires that number to equal `ACCEPTED + RETRY`; duplicate decisions must not create database rows. The prefix scope deliberately excludes the 1M baseline and data retained from other runs. Tune `dedupMapName`, `databaseConnectionName`, `databaseTableName`, and `databaseVerificationTimeoutSeconds` in `jet_postgres_tests.yaml` if the deployment names or expected flush time change.

The job uses `AT_LEAST_ONCE`, a 10-second snapshot interval, `requireSnapshotBeforeProcessing`, and the Enterprise map-flush sink. The result-map decision is the end-to-end latency boundary; PostgreSQL durability is completed independently by the snapshot-driven flush. The default 3M-entry event journal provides about 300 seconds of history at 10K TPS. Capacity is shared across all 271 partitions, so each partition receives roughly 11,070 slots. Increase `jet_event_journal_capacity` before testing a longer recovery interval.

Keep `resetTransientMaps` disabled when running multiple Jet suites against the same long-running job. Operation IDs contain `newKeyRunId`, and verification scans only that run's prefix, so retained input and result entries from earlier runs do not affect correctness. Clearing a completed 1.2M-operation input map generates enough journal removal traffic to consume the recovery window. Increment `newKeyRunId` for every retained-data run; the test fails during preparation if that run ID already exists.

## Data Sizing

Hazelcast 5.7 serialization produced a 36-byte serialized 24-digit String key and a 25-byte serialized timestamp String value in a local sizing check. These figures exclude record metadata and allocator overhead, so the deployment uses conservative headroom:

| Workload state | Estimated data size |
| --- | ---: |
| AP, 1M baseline, one backup | less than about 100 MiB per member |
| AP after 5 minutes at 10K TPS and 50% new keys | less than about 300 MiB per member |
| Jet input + result + dedup data after the default run | about 0.8 GiB/member serialized, before allocator/metadata overhead |
| merchant-profile, 1M exact 10 KiB values, one backup | 19.07 GiB serialized cluster-wide; about 10.67 GiB/member after 10 KiB values round to 16 KiB POOLED blocks |
| merchant-profile with one member unavailable | about 16 GiB of value blocks per surviving member, before keys and metadata |
| CP, 1M baseline over three groups | about 19.4 MiB per CPMap |
| CP after the default workload, about 2.5M total keys | about 48.5 MiB per CPMap |

The 20 GiB native-memory region is sized for the million-entry merchant-profile map with one synchronous backup and a temporary single-member outage. Hazelcast POOLED uses a buddy allocator, so a 10 KiB serialized value occupies the next power-of-two block, 16 KiB. The deployment exposes the supported `min-block-size`, `page-size`, and `metadata-space-percentage` settings but deliberately keeps the Hazelcast defaults of 16 bytes, 4 MiB, and 12.5%; changing the first two does not eliminate the 10 KiB-to-16 KiB internal fragmentation. Monitor `memory.usedNative`, `memory.usedMetadata`, and native fragmentation during the first run before reducing headroom. CPMap defaults to a 100 MB maximum and has an absolute supported maximum of 2,000 MB, so the three-group default remains below the conservative 100 MB setting without raising it. On this three-member cluster every three-member CP group is replicated to the same three members; more groups split the CPMap size and distribute Raft leadership/work, but do not reduce aggregate per-member storage.

CP leader auto step-down is controlled by `cp_auto_step_down_enabled` in `k8s/roles/hz/vars/main.yml` and is disabled by default. When enabled, the Helm chart starts an init container that copies the common member YAML to a writable volume and sets `auto-step-down-when-leader: true` only for the member selected by `cp_auto_step_down_member_ordinal`, which defaults to `hz-primary-2`; the other members use `false`. The selected member remains a voting CP member but transfers ordinary CP group leadership away after an election. Hazelcast permits this setting on only a minority of CP members, so a three-member CP cluster can mark at most one member this way. The setting does not apply to the METADATA CP group.

The CP Raft snapshot threshold is controlled by `cp_raft_snapshot_commit_interval` and defaults to 100,000 commits. With 10K aggregate TPS distributed evenly over three CP groups, this produces a snapshot approximately every 30 seconds per group instead of approximately every three seconds with Hazelcast's 10,000-commit default. Increasing the threshold reduces snapshot frequency and persistence I/O at the cost of retaining more Raft log entries in heap between snapshots.

Relevant Hazelcast guidance:

- [Hazelcast Enterprise Helm chart](https://docs.hazelcast.com/hazelcast/5.7/kubernetes/helm-hazelcast-enterprise-chart)
- [CP Subsystem configuration and CP groups](https://docs.hazelcast.com/hazelcast/5.7/cp-subsystem/configuration)
- [CPMap configuration and size limits](https://docs.hazelcast.com/hazelcast/5.7/data-structures/cpmap)
- [Native in-memory format](https://docs.hazelcast.com/hazelcast/5.7/data-structures/setting-data-format)
- [High-density memory configuration](https://docs.hazelcast.com/hazelcast/5.7/storage/high-density-memory)
- [POOLED native-memory allocator](https://docs.hazelcast.com/hazelcast/5.7-snapshot/storage/pooled-native-memory-allocator)
- [Java client ALL_MEMBERS routing](https://docs.hazelcast.com/hazelcast/5.7/clients/java#client-cluster-routing-modes)
- [GKE internal LoadBalancer services](https://cloud.google.com/kubernetes-engine/docs/how-to/internal-load-balancing)
- [Compact serialization](https://docs.hazelcast.com/hazelcast/5.7/serialization/compact-serialization)
- [Map journal source and snapshot-driven map flush](https://docs.hazelcast.com/hazelcast/5.7/integrate/map-connector)
- [Jet job processing guarantees](https://docs.hazelcast.com/hazelcast/5.7/pipelines/configuring-jobs)

## Chaos Experiments

Apply a chaos manifest while a test is running:

```bash
cd ~/src/simu-dedup/chaos
kubectl apply -f pod-failure.yaml
```

Available starter manifests:

- `chaos/pod-failure.yaml`
- `chaos/pod-kill.yaml`
- `chaos/network-partition.yaml`

## Cleanup

```bash
ansible-playbook k8s/undeploy.yaml --tags="undeploy,gke"
```

The undeploy playbook removes Kubernetes workloads, Helm releases, PVCs, client VMs, the GKE cluster, firewall rule, subnet, and VPC network.
