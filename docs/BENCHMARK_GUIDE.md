# Step-by-step MongoDB benchmark guide

This guide compares a **one-shard sharded cluster (`s1`)** with a separate **two-shard sharded cluster (`s2`)**, using the same Locust image, dataset configuration, workload settings, and GKE worker resources.

Both targets are independent clusters and must be accessed through their own `mongos` endpoint. The same database name can be used on each because they are separate clusters. The setup shards the profile and daily-metrics collections by hashed `player_id` (and the optional identity collection by hashed `lookup_key`) on both targets. The `s1` data therefore resides on its only shard; `s2` setup verifies that mongos sees two shards and requests four initial chunks per empty hashed collection so loading can distribute data across both. The config `shards` value records the expected shard count and controls this setup behavior; `MONGO_URI` selects the actual cluster.

## Where results are saved

- Local benchmark runs save artifacts on the computer running Locust, in **`./result/`** by default. Each run gets its own directory, e.g. `result/<run-id>/`.
- GKE runs initially save artifacts in **`/results` inside the master pod**. They are **not uploaded to GCS by default**.
- GCS upload is recommended for GKE and enabled when `--gcs-prefix` is passed to the manifest renderer (Step 8). Download the uploaded artifacts for local reporting.
- Without GCS or persistent storage, files in a terminated container are generally not accessible using `kubectl cp`.
- The repository `.gitignore` excludes `result/` so benchmark output is not committed accidentally.

## 1. Install and run smoke checks

From the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m unittest discover -s tests -v
python -m tests.check_distributed
```

Use the smoke dataset first to validate the workflow. It has 10,000 profiles and approximately 360,000 daily gaming rows.

## 2. Prepare MongoDB targets

Have these targets available:

| Target | Topology |
|---|---|
| `s1` | Sharded cluster with one shard |
| `s2` | Sharded cluster with two shards |

Record each target's `mongos` connection URI. Use the same MongoDB version, replica count per shard, read/write concerns, index definitions, and comparable per-shard resources. This framework deploys load generators; it does not provision MongoDB.

## 3. Build the benchmark image once

```bash
export PROJECT=your-gcp-project
export REGION=asia-east2
export IMAGE="$REGION-docker.pkg.dev/$PROJECT/bench/mongo-locust:v1"

# Authenticate Docker to Artifact Registry, then build locally and push directly.
gcloud auth configure-docker "$REGION-docker.pkg.dev"
docker build --tag "$IMAGE" .
docker push "$IMAGE"
```

The `bench` Docker repository must already exist in Artifact Registry in `$REGION`. Your account needs Artifact Registry Writer access to push, and the GKE node service account needs Artifact Registry Reader access to pull. The Docker image is built for your machine's default platform. Use this same image for every target.

## 4. Generate identical target/scenario configurations

```bash
export BASE_CONFIG="${BASE_CONFIG:-config/production.yaml}"
python - <<'PY'
from copy import deepcopy
import os
from pathlib import Path
import yaml

base = yaml.safe_load(Path(os.environ["BASE_CONFIG"]).read_text())
out = Path("/tmp/mongodb-bench-configs")
out.mkdir(parents=True, exist_ok=True)

for label, shard_count in [("s1", 1), ("s2", 2)]:
    for scenario in ["profile", "adt"]:
        config = deepcopy(base)
        config["label"] = f"{label}-{scenario}"
        config["shards"] = shard_count
        config["scenario"] = scenario
        (out / f"{label}-{scenario}.yaml").write_text(
            yaml.safe_dump(config, sort_keys=False)
        )
PY
```

This creates four workload configs, not four datasets. To start with smoke settings, run `export BASE_CONFIG=config/smoke.yaml` before the snippet. Because the MongoDB targets are separate clusters, all configs can use the same database name and dataset settings. Load the dataset once into each cluster, then reuse it for that cluster's `profile` and `adt` runs. The production example uses 3 million patrons and up to 180 days of history—about 108 million daily documents at its configured activity density.

## 5. Connect GKE to a MongoDB target

```bash
kubectl apply -f deploy/namespace.yaml

export MONGO_URI='mongodb://USER:PASSWORD@YOUR-MONGOS/?authSource=admin'
kubectl -n mongo-bench create secret generic mongodb \
  --from-literal=uri="$MONGO_URI" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Before preparing data or running a workload on a target, set `MONGO_URI` to that cluster's `mongos` endpoint and apply the Secret. Repeat when switching targets. Jobs read the Secret when they start; the config's `shards` field does not select the cluster, but setup checks it against the actual shard count for the two-shard target.

## 6. Set up and load data once per cluster

Do the following once for `s1`, then repeat for `s2` with its URI and config. For each cluster, use the profile config for setup and load; both scenarios have identical data settings.

```bash
export TARGET=s1
export SCENARIO=profile
export CONFIG="/tmp/mongodb-bench-configs/$TARGET-$SCENARIO.yaml"
export OUT="result/deploy-$TARGET-prepare"
export MONGO_URI='mongodb://USER:PASSWORD@S1-MONGOS/?authSource=admin'

kubectl -n mongo-bench create secret generic mongodb \
  --from-literal=uri="$MONGO_URI" \
  --dry-run=client -o yaml | kubectl apply -f -

python -m benchmark.deploy \
  --image "$IMAGE" \
  --config "$CONFIG" \
  --workers 8 \
  --users 200 \
  --spawn-rate 20 \
  --duration 30m \
  --out "$OUT"

kubectl apply -f "$OUT/config.yaml"
kubectl apply -f "$OUT/setup.yaml"
kubectl -n mongo-bench wait \
  --for=condition=complete job/benchmark-setup --timeout=10m

kubectl apply -f "$OUT/loader.yaml"
kubectl -n mongo-bench wait \
  --for=condition=complete job/benchmark-loader --timeout=24h
```

The setup uses hashed sharding and creates indexes on both clusters. For `s2`, run setup on empty collections so the four initial hashed chunks can be distributed before data loading; rerunning setup does not repartition collections that are already sharded. The loader uses eight deterministic partitions by default. Data loading is outside the timed benchmark. When repeating the commands for `s2`, change `TARGET`, `CONFIG`, `OUT`, and `MONGO_URI` to the two-shard cluster. The data generator is deterministic, so both clusters receive equivalent datasets.

Before preparing the second cluster, remove completed preparation jobs so Kubernetes can create them again with the new config:

```bash
kubectl -n mongo-bench delete job benchmark-setup benchmark-loader --ignore-not-found
```

Validate the sample data and indexes from an environment with network access to MongoDB:

```bash
export MONGO_URI='mongodb://USER:PASSWORD@YOUR-MONGOS/?authSource=admin'
python -m benchmark.data validate --config "$CONFIG" --samples 100
python -m benchmark.data inspect --config "$CONFIG"
```

Run validation/inspection against each cluster using its matching config and URI. Wait for balancing to settle. Confirm the `s1` collections are on its only shard and the `s2` collections have data distributed across both shards before measurement, using the cluster's sharding-status or monitoring tools.

## 7. Render and run each benchmark scenario

Run `profile` and `adt` separately on each target, reusing the dataset loaded above. Before each target's run, update the Kubernetes Secret to that target's `mongos` URI. Use a unique output directory for each target/scenario. Since the Kubernetes resources have fixed names, delete the prior run's master Job and worker Deployment before applying manifests for the next run:

```bash
kubectl -n mongo-bench delete job locust-master --ignore-not-found
kubectl -n mongo-bench delete deployment locust-worker --ignore-not-found
# Wait until all previous worker pods disappear; press Ctrl+C when none remain.
kubectl -n mongo-bench get pods -l app=locust-worker -w
```

Repeat the Secret update and deployment render/apply commands for each target/scenario, using the matching `MONGO_URI`, `TARGET`, and `SCENARIO` values.

```bash
export TARGET=s1
export SCENARIO=profile
export MONGO_URI='mongodb://USER:PASSWORD@S1-MONGOS/?authSource=admin'
export CONFIG="/tmp/mongodb-bench-configs/$TARGET-$SCENARIO.yaml"
export OUT="result/deploy-$TARGET-$SCENARIO"

kubectl -n mongo-bench create secret generic mongodb \
  --from-literal=uri="$MONGO_URI" \
  --dry-run=client -o yaml | kubectl apply -f -

python -m benchmark.deploy \
  --image "$IMAGE" \
  --config "$CONFIG" \
  --workers 8 \
  --users 200 \
  --spawn-rate 20 \
  --duration 30m \
  --gcs-prefix "gs://YOUR_BUCKET/$TARGET/$SCENARIO" \
  --out "$OUT"

kubectl apply -f "$OUT/config.yaml"
kubectl apply -f "$OUT/workers.yaml"
kubectl apply -f "$OUT/master.yaml"
kubectl -n mongo-bench logs -f job/locust-master
```

The master waits for the configured number of workers. Do not change worker membership during a measured run. GKE Autopilot starts the load-generation pods; it does not automatically increase the Locust user count or workload rate.

### Which scenario to run

Render the target config matching the desired scenario, then apply its config, worker, and master manifests as shown above.

- **`profile`:** member-profile reads, updates, lookups, and bounded host lists.
- **`adt`:** finite ADT computation and profile writeback across the configured patron population. The run stops after the batch completes or the run-time limit is reached.

Run profile and ADT as separate scenarios. Allow enough time for ADT to finish; an incomplete batch produces an invalid run.

For all four runs (`s1-profile`, `s1-adt`, `s2-profile`, `s2-adt`), use the same image, worker count, CPU/memory settings, offered load, and timing. Repeat important tests at least three times and alternate target order. Restore comparable mutable data state between repeats; profile updates and ADT writeback modify documents, so reload or otherwise reset the affected data outside the timed interval when needed. Do not run competing ADT writeback tests on the same collections concurrently.

## 8. Save GKE artifacts to GCS

By default, GKE does **not** upload artifacts to GCS. For durable results, render the deployment with a GCS prefix before starting the run:

```bash
python -m benchmark.deploy \
  --image "$IMAGE" \
  --config "$CONFIG" \
  --workers 8 \
  --users 200 \
  --spawn-rate 20 \
  --duration 30m \
  --gcs-prefix "gs://YOUR_BUCKET/$TARGET/$SCENARIO" \
  --out "$OUT"
```

The bucket must exist and the `mongo-bench/benchmark` Kubernetes service account needs object-write permission through Workload Identity. For this cluster (`niko-feng`, project number `785680100598`), ask a bucket administrator to grant the service-account principal the bucket-scoped Object Creator role:

```bash
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET \
  --member="principal://iam.googleapis.com/projects/785680100598/locations/global/workloadIdentityPools/niko-feng.svc.id.goog/subject/ns/mongo-bench/sa/benchmark" \
  --role="roles/storage.objectCreator"
```

Replace `YOUR_BUCKET` with the bucket used by `--gcs-prefix`. For another project or cluster, change the project number and Workload Identity pool to match that cluster. `roles/storage.objectCreator` permits creating new objects but not overwriting them, so use a fresh run ID when retrying. The renderer appends a run ID. After Locust exits, the master uploads `/results` even if Locust returned an error; check its logs for `Uploaded ... artifact(s)` and verify the objects. If the pod is killed or upload fails, these files may be lost without persistent storage.

Download the run directory (replace `RUN_ID` with the renderer's output):

```bash
mkdir -p "result/$TARGET/$SCENARIO"
gcloud storage cp --recursive "gs://YOUR_BUCKET/$TARGET/$SCENARIO/RUN_ID/*" "result/$TARGET/$SCENARIO/"
```

Repeat for every run. Local Locust runs already write directly beneath `result/`.

## 9. Summarize local artifacts

Once the GKE artifacts have been copied to `result/`:

```bash
python -m benchmark.report \
  result/*/*/run-*.json \
  --out result/summary.csv
```

For only one target/scenario:

```bash
python -m benchmark.report \
  "result/$TARGET/$SCENARIO"/run-*.json \
  --out "result/$TARGET/$SCENARIO/summary.csv"
```

The JSON artifacts contain the config, Locust operation stats, ADT job results, timing snapshots, and counters. The report summarizes throughput and batch duration across runs.

## 10. Compare results and supporting metrics

Compare:

1. Successful profile throughput at the same latency/error SLO.
2. Profile p95/p99 across the measured profile workload.
3. ADT wall-clock completion time.
4. Per-shard CPU, cache, storage latency, replication lag, and workload distribution.
5. GKE worker CPU/throttling, memory, network, and restarts, to ensure Locust is not the bottleneck.
6. Total resources and cost, because adding equivalent shards adds aggregate capacity.

Useful speedup calculations:

```text
Throughput speedup(N) = sustainable throughput(N) / throughput(1)
Batch speedup(N)      = ADT duration(1) / ADT duration(N)
Scaling efficiency   = speedup(N) / N
```

The strongest evidence for these use cases is increased sustainable profile throughput at the customer's latency/error objective, or shorter ADT completion time, on the two-shard target. Compare total resources and cost as well as performance.
