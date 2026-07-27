# Simulator for Bank Deduplication Experiments

This repo is bootstrapped for GKE-based Hazelcast Simulator experiments. The current base deploys:

- GKE in `asia-south1-a`
- 4 Hazelcast/Postgres worker nodes using `c2-standard-8`
- 2 client VMs in the same VPC
- Hazelcast Platform `5.7.0`
- Hazelcast Management Center `5.11.0`
- Hazelcast Platform Operator chart `5.18.0`
- Postgres `18.4`
- Chaos Mesh `2.8.3`

The deduplication model is intentionally scoped to one payment-id domain per deployment. A bank would create a separate IMap/table, and often a separate database or Hazelcast cluster, for each payment rail or deduplication domain. The `deduplicate` IMap therefore stores plain payment IDs only; there is no domain discriminator in the key or table schema.

## Prerequisites

- Ansible with `google.cloud`, `kubernetes.core`, `community.crypto`, and `community.general` collections.
- `gcloud`, `kubectl`, and `helm`.
- GCP credentials at `~/gcp/credentials.json`, or update `k8s/roles/gke/vars/main.yml`.
- A public SSH key at `~/.ssh/id_ed25519.pub`, or set `SIMU_DEDUP_SSH_PUB_KEY`.
- Postgres password in `SIMU_DEDUP_POSTGRES_PASSWORD` for AP/Jet deployments.
- Hazelcast Enterprise license at `~/hazelcast/demo.license`, or set `HAZELCAST_LICENSE_KEY`.
- Hazelcast Simulator at `~/src/hazelcast-simulator`.

## Build Test Jar

```bash
cd ~/src/simu-dedup
mvn -DskipTests package
scripts/install-simulator-user-lib
```

## Deploy

The same playbook provisions the cluster for AP, CP, and Jet tests. CP is enabled with persistence in every mode so switching workloads does not require a second cluster definition. Select the data path with `dedup_test_mode`:

```bash
cd ~/src/simu-dedup

# IMap + Postgres
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=ap \
  --tags="gke,client,hz,postgres,chaos"

# CPMap only: no Postgres or MapStore resource
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=cp \
  --tags="gke,client,hz,chaos"

# IMap + PostgreSQL + Jet
ansible-playbook k8s/deploy.yaml \
  -e dedup_test_mode=jet \
  --tags="gke,client,hz,postgres,chaos"
```

The three Hazelcast members each request 16 GiB, with an 8 GiB heap and a 4 GiB pooled native-memory region. The AP `deduplicate` IMap uses `NATIVE` storage. CP persistence uses a 50 GiB `premium-rwo` volume per member. Do not run the AP and CP deduplication scenarios concurrently; the CP test destroys an already-created AP IMap before it starts its CP population.

After Postgres becomes Ready, a one-shot Kubernetes Job bulk-generates 1,000,000 fixed-width, 24-digit baseline IDs (`100000000000000000000000` through `100000000000000000999999`) directly inside Postgres. Their first-seen values are distributed over the previous 365 days and expire 366 days after first-seen. The playbook waits for the Job to verify every seed ID before creating Hazelcast, so its EAGER MapStore cannot start against a partial baseline.

To change the baseline, override `dedup_seed_record_count`, `dedup_seed_start_id`, or `dedup_seed_id_prefix` as Ansible extra variables. The Simulator test's `existingKeyDomain` and prefix must describe the same key range. The Job is safe to rerun against a retained Postgres volume; it refreshes the configured seed range without removing other records.

After the playbook creates the client VMs:

1. Get the public IPs for VMs named `raj-dedup-client-*` from GCP.
2. Add those IPs under `loadgenerators.hosts` in `inventory.yaml`.
3. Run `kubectl get svc hz-primary` and copy the `EXTERNAL-IP`.
4. Replace `127.0.0.1` in `client-hazelcast.xml` with that `EXTERNAL-IP`.

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

The deployment starts the long-running `simu-dedup-jet` job before Simulator. Simulator writes uniquely keyed requests to `deduplicate-jet-input`; its event journal feeds Jet, which runs asynchronous `putIfAbsent` operations against the same MapStore-backed `deduplicate` IMap used by the AP benchmark. Jet writes one decision per operation ID to `deduplicate-jet-results`.

Payment IDs and operation IDs are separate 24-digit values. The payment ID is the business deduplication key. The operation ID correlates a submission with its result and lets AT_LEAST_ONCE replay be distinguished from a true duplicate:

- no previous value: `ACCEPTED`;
- previous value equals this operation ID: `RETRY`;
- previous value belongs to another operation: `DUPLICATE`.

Requests, decisions, and the distributed verification helpers use explicit Compact serializers registered in both the member and Simulator client configuration. Keep those registrations symmetric when adding or evolving fields.

The normal `submit` timestep measures input-map acknowledgement latency and TPS. A filtered result-map listener completes the `jetEndToEnd` probe when the decision becomes visible. Listener delivery is used only for latency: local verification falls back to batched result reads for lost listener events, and the final global `@Verify` compares this run's distributed input and result IMap counts and checks all result classifications. Fallback results are not added to the latency histogram.

The same global verification calls `flush()` on the MapStore-backed `deduplicate` IMap and then runs a count query on a Hazelcast member through the configured JDBC data connection. It counts active PostgreSQL rows under the current run's new-payment prefix and requires that number to equal `ACCEPTED + RETRY`; duplicate decisions must not create database rows. The prefix scope deliberately excludes the 1M baseline and data retained from other runs. Tune `dedupMapName`, `databaseConnectionName`, `databaseTableName`, and `databaseVerificationTimeoutSeconds` in `jet_postgres_tests.yaml` if the deployment names or expected flush time change.

The job uses `AT_LEAST_ONCE`, a 10-second snapshot interval, `requireSnapshotBeforeProcessing`, and the Enterprise map-flush sink. The result-map decision is the end-to-end latency boundary; PostgreSQL durability is completed independently by the snapshot-driven flush. The default 1M-entry event journal provides about 100 seconds of history at 10K TPS; increase `jet_event_journal_capacity` before testing a longer recovery interval.

## Data Sizing

Hazelcast 5.7 serialization produced a 36-byte serialized 24-digit String key and a 25-byte serialized timestamp String value in a local sizing check. These figures exclude record metadata and allocator overhead, so the deployment uses conservative headroom:

| Workload state | Estimated data size |
| --- | ---: |
| AP, 1M baseline, one backup | less than about 100 MiB per member |
| AP after 5 minutes at 10K TPS and 50% new keys | less than about 300 MiB per member |
| Jet input + result + dedup data after the default run | about 0.8 GiB/member serialized, before allocator/metadata overhead |
| CP, 1M baseline over three groups | about 19.4 MiB per CPMap |
| CP after the default workload, about 2.5M total keys | about 48.5 MiB per CPMap |

The 4 GiB native-memory region therefore has substantial room for the 1M AP baseline and the default workload. CPMap defaults to a 100 MB maximum and has an absolute supported maximum of 2,000 MB, so the three-group default remains below the conservative 100 MB setting without raising it. On this three-member cluster every three-member CP group is replicated to the same three members; more groups split the CPMap size and distribute Raft leadership/work, but do not reduce aggregate per-member storage. Validate these estimates with member native-memory, heap, and CP persistence metrics during the first real run.

Relevant Hazelcast guidance:

- [CP Subsystem with the Platform Operator](https://docs.hazelcast.com/operator/5.18/cp-subsystem)
- [CP Subsystem configuration and CP groups](https://docs.hazelcast.com/hazelcast/5.7/cp-subsystem/configuration)
- [CPMap configuration and size limits](https://docs.hazelcast.com/hazelcast/5.7/data-structures/cpmap)
- [Native in-memory format](https://docs.hazelcast.com/hazelcast/5.7/data-structures/setting-data-format)
- [High-density memory configuration](https://docs.hazelcast.com/hazelcast/5.7/storage/high-density-memory)
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
