# Simulate Bank Deduplication Use Cases

I want to try provide some experimental results to banks for their deduplication use cases. Deduplication is basically the deduplication of payments typically via their payment id. You can find more on this in [AXIS_Bank_Hazelcast_Proposal.pptx](AXIS_Bank_Hazelcast_Proposal.pptx).

## Goal

Create GCP cluster and run experiments on three types of deduplication architectures - Hazelcast IMap + Postgres, Hazelcast IMap + Jet + Postgres and CP Map. Using Hazelcast Simulator to execute tests and generate the results so that we can compare. Also do some more tests with adding errors using chaos mesh. With chaos mesh we can simulate 3 DC stretched cluster setup and introduce latency with some jitter too to simulate inter AZ or inter DC type network.

## Setup

### GKE (Done)

I have a project called /Users/raj/src/simu-chaos I have often used to build the base. Copy the content of the project here. Remove unnecessary bits. We can always pull stuff from the original project. Idea is that the Ansible script should be able to build the GKE project. Make sure we use latest Hz and other dependencies. Postgres needs to be installed too. I have postgres installation mechanism in /Users/raj/src/streaming-payments. To run Hz and postgres we may need 4 VMs ensuring that Hz gets at least 8 vcpu per pod and maybe 24GB RAM per instance. Stick to the zone and other choices as per the simu-chaos project.

### Simulator

Simulator must also gets deployed in the same cluster. The existing simu-chaos project deploys the simulator clients in VMs in the same VPC and the main simulator code is running on the local laptop. We will keep the same pattern here too.

Simulator has the ability to test plain IMap and CPMap tests but it does not have built in capability to test Jet. We have to think on how to test just dedup setup.

The Simulator project is at /Users/raj/src/hazelcast-simulator for reference.

### Number of payments

As per google search, AXIS bank does about 40 million payments per day across all payment rails. UPI itself accounts for about 30 million payments per day. Credit cards may be around 3 million, high-value payments about 1 million, and others about 0.5 million. We can assume 40m in total per day. UPI does about 2k payments per second.

Deduplication is scoped to a single payment-id domain. The same DB table and IMap should not store payment IDs from more than one payment rail or deduplication domain. In practice, each domain would have its own table and IMap, and likely its own database and Hazelcast deployment. Therefore this project models one deduplication domain at a time and uses plain payment IDs as keys, with no domain discriminator.

For sizing we will simulate peak TPS by looking at a busy domain such as UPI. Actual TPS may be lower, but we will use 10K TPS for the dedupe test.

### Deduplication via IMap + Postgres

Create an IMap called `deduplicate` and have it backed by a MapStore. Ensure that we are loading only one year of data. In streaming-payments repo there is an IMap called `cdttrftx-txdedup` which loads only last 1 year (or so of ids). We can follow the same pattern. `cdttrftx-txdedup` is used by Jet for the purpose but we will have similar IMap for non Jet and Jet based dedup.

#### Data Load (Done for the 1M baseline)

For the current experiment, test against 1,000,000 existing 24-digit payment IDs. A one-shot Kubernetes Job bulk-generates this deterministic baseline inside PostgreSQL and verifies it before Hazelcast starts its EAGER MapStore load. The benchmark can choose a configured percentage from this existing range and generates the remainder as unique 24-digit IDs from a separate range. This avoids sending the seed data through Simulator and does not require a GCS artifact for a small synthetic dataset. Revisit a versioned object or database snapshot when the experiment moves to a much larger or non-synthetic corpus.

#### 10K TPS no Chaos tests

Once data load is done, we should have 5 min tests putIfAbsent. As of now no chaos, just simple tests. Throughput should be limited to 10K

## Execution

I may list many things below. You can create agents and ensure that goal is met. You should execute in stages and use agents if parallel work can be done. Do not add unnecessary abstractions. Keep it simple and straight forward. I will be available to answer questions and provide guidance. You can also use the original projects for reference.

1. Make sure no licenses or keys are present in this repo
1. Do not create unnecessary abstractions.
1. Build the smallest working code but it should produce correct results.
1. We can add abstractions and complexity when we add business complexity
1. Just code, let me run all the commands on the cluster.
