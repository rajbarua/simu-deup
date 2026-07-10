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

The deduplication model is intentionally scoped to one payment-id domain per
deployment. A bank would create a separate IMap/table, and often a separate
database or Hazelcast cluster, for each payment rail or deduplication domain.
The `deduplicate` IMap therefore stores plain payment IDs only; there is no
domain discriminator in the key or table schema.

## Prerequisites

- Ansible with `google.cloud`, `kubernetes.core`, `community.crypto`, and `community.general` collections.
- `gcloud`, `kubectl`, and `helm`.
- GCP credentials at `~/gcp/credentials.json`, or update `k8s/roles/gke/vars/main.yml`.
- A public SSH key at `~/.ssh/id_ed25519.pub`, or set `SIMU_DEDUP_SSH_PUB_KEY`.
- Postgres password in `SIMU_DEDUP_POSTGRES_PASSWORD`.
- Hazelcast Enterprise license at `~/hazelcast/demo.license`, or set `HAZELCAST_LICENSE_KEY`.
- Hazelcast Simulator at `/Users/raj/src/hazelcast-simulator`.

## Build Test Jar

```bash
cd ~/src/simu-dedup
mvn -DskipTests package
scripts/install-simulator-user-lib
```

## Deploy

```bash
cd ~/src/simu-dedup
ansible-playbook k8s/deploy.yaml --tags="gke,client,hz,postgres,chaos"
```

After Postgres becomes Ready, a one-shot Kubernetes Job bulk-generates
1,000,000 fixed-width, 24-digit baseline IDs
(`100000000000000000000000` through `100000000000000000999999`)
directly inside Postgres. Their first-seen values are distributed over the
previous 365 days and expire 366 days after first-seen. The playbook waits for
the Job to verify every seed ID before creating Hazelcast, so its EAGER
MapStore cannot start against a partial baseline. No GCS dataset is used for
this deterministic 1M-row seed.

To change the baseline, override `dedup_seed_record_count`,
`dedup_seed_start_id`, or `dedup_seed_id_prefix` as Ansible extra variables.
The Simulator test's `existingKeyDomain` and prefix must describe the same
key range. The Job is safe to rerun against a retained Postgres volume; it
refreshes the configured seed range without removing other records.

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

The scenario uses two clients at 5,000 operations/second each, for 10,000
operations/second in aggregate. `existingKeyPercentage` controls how many
operations use an existing key. `existingKeySampleSize` controls the size of
that duplicate hot set; its keys are evenly sampled from the seeded 1M-key
domain. The remainder use unique 24-digit IDs beginning with `2`. Increment
`newKeyRunId` before rerunning against the same cluster so the new-key subset
does not overlap an earlier run. Each Simulator client builds only the
configured existing-key sample during setup, and the timed path uses a random
array lookup. New keys use a private character buffer per test thread and do
not use shared counters or formatting utilities.

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

The undeploy playbook removes Kubernetes workloads, Helm releases, PVCs, client VMs, the
GKE cluster, firewall rule, subnet, and VPC network.
