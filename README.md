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
