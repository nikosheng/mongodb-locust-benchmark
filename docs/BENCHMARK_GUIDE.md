# Step-by-step MongoDB benchmark guide

This guide compares **1, 2, and 4 MongoDB shards** with the same Locust image, dataset configuration, workload settings, and GKE worker resources.

The recommended one-shard baseline is a sharded cluster containing one shard, with all targets accessed through `mongos`. This keeps the router path consistent. A standalone replica-set baseline is possible, but should be reported as a separate topology.

## Where results are saved

- Local benchmark runs save artifacts on the computer running Locust, in **`./result/`** by default. Each run gets its own directory, e.g. `result/<run-id>/`.
- GKE runs initially save artifacts in **`/results` inside the master pod**. They are **not uploaded to GCS by default**.
- After a GKE run completes, copy artifacts to this computer's `result/` folder with `kubectl cp` (Step 10). Keep the master pod until the copy succeeds.
- GCS upload is optional and enabled only when `--gcs-prefix` is passed to the manifest renderer.
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
| `s4` | Sharded cluster with four shards |

Record each target's `mongos` connection URI. Use the same MongoDB version, replica count per shard, read/write concerns, index definitions, and comparable per-shard resources. This framework deploys load generators; it does not provision MongoDB.

## 3. Build the benchmark image once

```bash
export PROJECT=your-gcp-project
export REGION=asia-east2
export IMAGE="$REGION-docker.pkg.dev/$PROJECT/bench/mongo-locust:v1"

gcloud builds submit --tag "$IMAGE" .
```

Use this same image for every target.

## 4. Generate identical target/scenario configurations

```bash
python - <<'PY'
from copy import deepcopy
from pathlib import Path
import yaml

base = yaml.safe_load(Path("config/production.yaml").read_text())
out = Path("/tmp/mongodb-bench-configs")
out.mkdir(parents=True, exist_ok=True)

for label, shard_count in [("s1", 1), ("s2", 2), ("s4", 4)]:
    for scenario in ["profile", "adt", "combined"]:
        config = deepcopy(base)
        config["label"] = label
        config["shards"] = shard_count
        config["scenario"] = scenario
        (out / f"{label}-{scenario}.yaml").write_text(
            yaml.safe_dump(config, sort_keys=False)
        )
PY
```

For your first run, replace `config/production.yaml` with `config/smoke.yaml`. The production example uses 3 million patrons and up to 180 days of history—about 108 million daily documents at its configured activity density.

## 5. Connect GKE to a MongoDB target

```bash
kubectl apply -f deploy/namespace.yaml

export MONGO_URI='mongodb://USER:PASSWORD@YOUR-MONGOS/?authSource=admin'
kubectl -n mongo-bench create secret generic mongodb \
  --from-literal=uri="$MONGO_URI" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Repeat for each target, setting its URI before preparing or running it. The URI is stored in a Kubernetes Secret, not the benchmark configuration.

## 6. Render a deployment for data preparation

Example for the `s1` profile dataset:

```bash
export TARGET=s1
export SCENARIO=profile
export CONFIG="/tmp/mongodb-bench-configs/$TARGET-$SCENARIO.yaml"
export OUT="result/deploy-$TARGET-setup"

python -m benchmark.deploy \
  --image "$IMAGE" \
  --config "$CONFIG" \
  --workers 8 \
  --users 200 \
  --spawn-rate 20 \
  --duration 30m \
  --out "$OUT"

kubectl apply -f "$OUT/config.yaml"
```

## 7. Set up and load MongoDB data

```bash
kubectl apply -f "$OUT/setup.yaml"
kubectl -n mongo-bench wait \
  --for=condition=complete job/benchmark-setup --timeout=10m

kubectl apply -f "$OUT/loader.yaml"
kubectl -n mongo-bench wait \
  --for=condition=complete job/benchmark-loader --timeout=24h
```

The setup uses hashed sharding and creates indexes. The loader uses eight deterministic partitions by default. Data loading is outside the timed benchmark.

Validate the sample data and indexes from an environment with network access to MongoDB:

```bash
export MONGO_URI='mongodb://USER:PASSWORD@YOUR-MONGOS/?authSource=admin'
python -m benchmark.data validate --config "$CONFIG" --samples 100
python -m benchmark.data inspect --config "$CONFIG"
```

Wait for shard balancing to settle before measuring.

## 8. Start the GKE workers and master

Use a unique artifact directory for each target/scenario. GKE results will be copied there after the run.

```bash
kubectl apply -f "$OUT/workers.yaml"
kubectl apply -f "$OUT/master.yaml"
kubectl -n mongo-bench logs -f job/locust-master
```

The master waits for the configured number of workers. Do not change worker membership during a measured run. GKE Autopilot starts the load-generation pods; it does not automatically increase the Locust user count or workload rate.

### Which scenario to run

Render the target config matching the desired scenario, then apply its config, worker, and master manifests as shown above.

- **`profile`:** member-profile reads, updates, lookups, and bounded host lists.
- **`adt`:** finite ADT computation and profile writeback across the configured patron population. The run stops after the batch completes or the run-time limit is reached.
- **`combined`:** profile traffic before and during the ADT batch. This is the headline business scenario: compare profile latency while ADT runs and total ADT completion time.

Allow enough time for ADT to finish in the `adt` and `combined` scenarios. An incomplete batch produces an invalid run.

## 9. Repeat across shard counts and scenarios

For each target (`s1`, `s2`, `s4`):

1. Update `MONGO_URI` to that target's `mongos` endpoint, then update the Kubernetes Secret before launching that target's jobs:

   ```bash
   kubectl -n mongo-bench create secret generic mongodb \\
     --from-literal=uri="$MONGO_URI" \\
     --dry-run=client -o yaml | kubectl apply -f -
   ```

2. Set `TARGET` and the matching `CONFIG` file.
3. Prepare and validate the same dataset settings.
4. Run `profile`, `adt`, and `combined` scenarios.
5. Use the same image, worker count, CPU/memory settings, offered load, and timing for comparable runs.
6. Repeat important tests at least three times, alternating target order.
7. Restore comparable mutable data state between repeats; rerun the deterministic loader outside the timed interval when appropriate.

Use separate benchmark databases or collections for each topology if targets may contain data for multiple configurations. Do not run competing ADT writeback tests on the same collections concurrently.

## 10. Copy GKE artifacts to your computer

After the run has finished, copy the master pod's `/results` directory to local `./result/`. Run this on your computer from the repository root, and set `TARGET` and `SCENARIO` to the completed run:

```bash
export TARGET=s1
export SCENARIO=combined
mkdir -p "result/$TARGET/$SCENARIO"

MASTER_POD=$(kubectl -n mongo-bench get pods -l app=locust-master \
  -o jsonpath='{.items[0].metadata.name}')

kubectl -n mongo-bench cp \
  "$MASTER_POD:/results/." \
  "result/$TARGET/$SCENARIO/"
```

Confirm the copy before deleting the completed master Job/pod:

```bash
ls -la "result/$TARGET/$SCENARIO"
```

Repeat for every target and scenario. Local Locust runs already write directly beneath `result/`; they do not need `kubectl cp`.

### Optional GCS backup

By default, GKE does **not** upload artifacts to GCS. To also upload a backup, render the deployment with a GCS prefix:

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

The GKE service account must have permission to write objects to the bucket. GCS is optional; the local `kubectl cp` procedure works without it.

## 11. Summarize local artifacts

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

## 12. Compare results and supporting metrics

Compare:

1. Successful profile throughput at the same latency/error SLO.
2. Profile p95/p99 before and during ADT processing.
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

The strongest evidence for these use cases is increased sustainable profile throughput or shorter ADT completion time while preserving the same online latency/error objective during batch execution.
