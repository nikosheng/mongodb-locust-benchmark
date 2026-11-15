# MongoDB distributed Locust benchmark

Benchmark **one shard versus multiple shards** using casino-shaped workloads:

1. **Patron ADT precompute:** full-population finite jobs and targeted incremental refreshes.
2. **Member profile single view:** targeted reads/updates, identity lookup journeys, and non-shard-key controls.

One Locust master coordinates horizontally scalable workers on **GKE Autopilot**. Workers connect directly to MongoDB with PyMongo. The data generator reuses the profile schema builder in `generate_member_profile.py`. ADT aggregation and precompute workloads are implemented in this framework under `benchmark/`; they do not depend on a separate `adt_precompute.py` script.

**Versions:** Python 3.12; **PyMongo 4.20.0**, verified as the latest stable PyPI release on 2026-11-15; Locust 2.47.0. PyMongo and Locust are pinned in `requirements.txt`. Pinning makes runs reproducible; check PyPI before deliberately upgrading a future image.

## Architecture

```text
                         GKE Autopilot
     ┌────────────────────────────────────────────────┐
     │ Master Job: users, statistics, finite job ledger│
     │                  │ ClusterIP :5557             │
     │ Worker Deployment: 2 → 4 → 8 → 16 → 32 pods     │
     │                  │ PyMongo                     │
     │ Separate setup and indexed data-loader Jobs    │
     └──────────────────┼─────────────────────────────┘
                        │ private connection
                 MongoDB mongos endpoints
                        │
                    1 / 2 replica-set shards

     Master artifacts → local result/ or optional GCS
     MongoDB/Atlas + GKE monitoring → external metrics
```

The framework deploys the **load generators**, not MongoDB itself or the GKE cluster. Connect it to existing Atlas or self-managed targets. Keep the master and all workers on the same image and benchmark configuration.

For an operator-oriented runbook—including copying GKE artifacts to your computer—see [`docs/BENCHMARK_GUIDE.md`](docs/BENCHMARK_GUIDE.md).

## Quick start: local smoke test

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Optional local MongoDB; Docker must be running.
docker compose up -d
export MONGO_URI='mongodb://localhost:27017'

# Preview synthetic BSON-shaped data and the ADT pipeline without connecting.
python -m benchmark.data preview --config config/smoke.yaml

# Standalone/unsharded setup for functional smoke testing.
python -m benchmark.data setup --config config/smoke.yaml --identities
python -m benchmark.data load --config config/smoke.yaml --identities
python -m benchmark.data validate --config config/smoke.yaml

python -m benchmark.run --config config/smoke.yaml \
  --users 20 --spawn-rate 10 --duration 2m
python -m benchmark.report result/*/run-*.json
```

The smoke dataset contains 10,000 profiles and approximately 360,000 daily records. Every patron-day is generated independently of loader partition count. All values are synthetic.

Use a fresh benchmark database for each dataset specification. The loader uses idempotent replacement/upserts, so interrupted partitions can be rerun. Loading also resets mutated profile state; rerunning does not remove older documents outside a changed specification. To change patron count, dates, or history, use a new database or explicitly clear your dedicated benchmark collections first.

## Configuration

Start with `config/smoke.yaml` or `config/production.yaml`. Missing keys inherit defaults in `benchmark/config.py`.

| Setting | Meaning |
|---|---|
| `label`, `shards` | Target label and declared shard count used in reporting; shard count does not provision MongoDB |
| `scenario` | `profile`, `adt`, `incremental`, or `ingest` |
| `players`, `history_days`, `active_day_probability` | Dataset scale and patron activity density |
| `seed`, `as_of` | Reproducible source data and fixed ADT cutoff |
| `windows` | ADT windows, e.g. `[7, 30, 90, 180]` |
| `denominator` | `active` gaming days or all `window` days |
| `warmup_seconds` | Initial traffic excluded by resetting Locust statistics |
| `distribution` | `uniform`, `skewed`, or `hotspot` |
| `hot_fraction`, `hot_probability` | Hot population size and probability of selecting it |
| `profile_weights` | Relative operation weights; set unwanted default operations to `0` |
| `batch.size` | Patrons per finite job / aggregation cursor batch size |
| `batch.slots_per_worker` | Concurrent finite jobs per worker, separate from online users |
| `batch.mode` | `ids`: bounded `$in`; `range`: original range-based fan-out control |
| `batch.writeback` | Include profile writes, or compute only |
| `batch.lease_seconds`, `max_attempts` | Heartbeated job lease and bounded retries |
| `load.mode` | `concurrency` or scheduled `arrival` |
| `load.rate` | Aggregate scheduled arrivals/second across a fixed worker set |
| `load.max_inflight_per_worker` | Bounded scheduled concurrency; additional arrivals are counted as dropped |
| `mongo` | Pool size, operation timeout, read preference, read/write concerns |
| `slo` | Optional `p99_ms`, `max_error_rate`, and `batch_seconds` report thresholds |

Credentials are supplied only through **`MONGO_URI`**. Other environment variables:

```text
BENCH_CONFIG       YAML path; defaults to config/smoke.yaml
BENCH_LABEL        Override target label (must match on master/workers)
BENCH_SCENARIO     Override scenario (must match on master/workers)
BENCH_RUN_ID       Unique identifier; otherwise generated by controller
BENCH_RESULTS      Artifact directory; defaults to result locally, /results in image
BENCH_GCS_PREFIX   Optional gs://bucket/prefix for artifact wrapper
```

`read_preference` supports `primary` and `secondaryPreferred`; use primary for the initial comparison. Read concern defaults to `local`, write concern to `majority`. URI options also affect behavior; keep them consistent between targets.

### Dataset scale

The production example uses 3 million patrons, 180 history days and 20% active-day density: approximately **108 million daily documents**, plus profiles and optional identity mappings. Size the dataset using measured BSON size, indexes, compression, and cache capacity. This is intentionally a substantial dataset; calibrate first with smoke settings.

Generation preserves nested profile structures including `pinfo`, contacts, cards, hosts, tags, tier, `bucket_bal` with Decimal128 amounts, and ODS metadata. Daily records contain:

```javascript
{
  player_id: NumberLong(...),
  gaming_date: ISODate(...),
  gaming: { total: {
    theo_win: Decimal128(...), actual_win: Decimal128(...), turnover: Decimal128(...)
  } },
  _ODS_modified_dtm: ISODate(...)
}
```

Daily rows are unique by deterministic patron-day ID, with one row per active patron/day. Production datasets with multiple casino/game rows per day need daily consolidation before using row counts as active-day counts.

## MongoDB preparation

The benchmark comparison uses two independent clusters: a one-shard sharded cluster (`s1`) and a two-shard sharded cluster (`s2`). Connect to each through its own `mongos` endpoint. Shard the profile and daily collections by hashed `player_id`; shard the optional identity collection by hashed `lookup_key`:

```bash
export MONGO_URI='mongodb+srv://USER:PASSWORD@TARGET/?retryWrites=true'
python -m benchmark.data setup --config config/production.yaml --sharded --identities
python -m benchmark.data load --config config/production.yaml --identities
python -m benchmark.data validate --config config/production.yaml --samples 100
python -m benchmark.data inspect --config config/production.yaml
```

Run setup and load once on each cluster with identical data settings. The deterministic loader creates equivalent datasets, while the clusters remain isolated from each other's resource use. Reuse each cluster's dataset for its `profile` and `adt` runs. The config `shards` value is reporting metadata; the `MONGO_URI` chooses the actual cluster. `setup` requires collection/index creation and sharding privileges. `inspect` needs access to storage statistics and explain commands. Workers need read/write access to the workload collections. Supply an appropriately privileged credential to setup rather than using worker access for administration.

For the complete GKE workflow, including generation of the four `s1`/`s2` profile/ADT configs and switching the Kubernetes MongoDB Secret between the independent clusters, follow [`docs/BENCHMARK_GUIDE.md`](docs/BENCHMARK_GUIDE.md). Each dataset is prepared and loaded once per cluster, not once per scenario.

### Shard keys and indexes

- `member_profile`: `{player_id: "hashed"}`.
- `player_daily_metrics`: `{player_id: "hashed"}`, plus `{player_id: 1, gaming_date: 1}`.
- Optional `member_identity`: `{lookup_key: "hashed"}`.
- Profiles retain the reference generator's representative secondary indexes and add `{primary_host_num: 1, is_vip: 1, player_id: 1}` for bounded host lists.
- Both comparison targets use sharded collections; `s1` has one shard and `s2` has two. Confirm the `s2` collections are balanced across both shards before measurement.

Wait for balancing/index builds to finish before measurements. Inspect document/byte distribution, not just chunk counts. `inspect` prints storage data and representative profile/ADT explains. Check non-shard-key and range controls separately with representative explains before interpreting them. Empty-collection distribution and administrative behavior vary by MongoDB version.

Patron equality can target one shard; numeric ranges on a hashed key generally fan out. A bounded `$in` job can target multiple or all shards. Matching shard keys in different collections **does not guarantee physical co-location**.

### Parallel local loading

Run disjoint partitions in separate terminals/processes; use the same total partition count:

```bash
python -m benchmark.data load --config config/production.yaml --partition 0 --partitions 8 --identities
python -m benchmark.data load --config config/production.yaml --partition 1 --partitions 8 --identities
# ... partitions 2 through 7
```

The indexed GKE Job automates this partitioning. Setup/index creation happens once, before loaders start. Indexes are maintained during loading; loading time is excluded from workload results.

## Workloads

### Member profile

The default mix is 65% full reads, 15% contact updates, 10% read/update journeys, 5% member-number lookups, and 5% bounded host/VIP lists.

| Operation | Behavior |
|---|---|
| `read_full` | Full profile by patron equality |
| `read_projected` | Patron ID, tier, and ADT projection |
| `update_contact` | Updates nickname and ODS modification time |
| `update_bucket` | Updates group 1 Decimal128 bucket balance using an array filter |
| `read_update` | Complete read-then-update journey, recorded once |
| `member_lookup` | Non-shard-key lookup by member number |
| `card_lookup` | Direct card-array lookup; schema intentionally permits patrons without cards |
| `host_list` | Host/VIP filter, ordered by patron ID, projection, maximum 50 records |
| `identity_journey` | Hashed member-number identity mapping, followed by profile lookup |

Generate optional identities using `--identities` before enabling `identity_journey`. This variant uses member numbers for complete population coverage; the reference profile generator has about 5% cardless patrons. Missing profiles, failed updates, and missing mappings are recorded as failures; expected card misses are valid responses.

### Finite ADT batch

```bash
BENCH_SCENARIO=adt python -m benchmark.run --config config/smoke.yaml \
  --users 1 --duration 20m
```

The controller partitions the entire configured patron ID population into bounded jobs. A worker aggregates active source rows for all windows, fully consumes the cursor, and performs acknowledged unordered profile writeback. Patrons with no activity in the largest window are not updated. The test stops early after all logical jobs terminate; exceeding `--duration` with unfinished jobs makes the artifact invalid and returns nonzero.

Job execution is **at least once**. Assignments have leases, heartbeats, attempt tokens, bounded retries, and stale-acknowledgment rejection. ADT values are idempotent for a fixed source dataset and `as_of`; retry timestamps can differ. The ledger counts each completed logical job once. Diagnostic Locust timings include retry attempts, while ledger totals represent logical completed work. Do not run competing ADT runs against the same profile collection concurrently.

`adt.compute_batch` includes cursor consumption and decoding. `adt.writeback_batch` includes write acknowledgment. `BUSINESS adt.job_e2e` includes both. Do not add these request counts together as business throughput. `daily_rows` is the number of source rows represented by the result, **not MongoDB documents examined**.

After completion, use the run ID from the artifact to verify sampled source data and writeback against an independent Python Decimal calculation:

```bash
python -m benchmark.data validate --config config/smoke.yaml --samples 100 --run-id RUN_ID
```

### Incremental refresh and ingestion

```bash
BENCH_SCENARIO=incremental python -m benchmark.run --config config/smoke.yaml --duration 5m
BENCH_SCENARIO=ingest python -m benchmark.run --config config/smoke.yaml --duration 5m
```

Incremental tasks aggregate one patron and write ADT, producing `BUSINESS adt.refresh_patron_e2e`. Ingestion upserts a patron-day at **`as_of + 1 day`**, outside the immutable ADT calculation window. This bounds dataset growth and exercises insert-then-update behavior; it is not an unlimited append-only event ingestion test. Reruns after the next-day records exist are update-dominant. Restore equivalent target state for fair comparisons.

### Concurrency versus scheduled arrival rate

- **Concurrency:** `--users` is the total online user population, distributed across workers. More workers provide generator capacity, not automatically more offered load.
- **Arrival:** set `load.mode: arrival`, `load.rate`, and the inflight cap. The framework allocates a fixed global schedule across workers. Keep at least one Locust user to start the lifecycle; users do not generate additional operations in this mode.
- Arrivals that exceed the inflight cap are counted as **dropped**, not silently delayed. Start-lag events record generator scheduling delay. Counts include scheduled/started/completed/failed/dropped. This is bounded shedding, not an unbounded durable queue or real event-freshness measurement.
- Distributed arrival counters are reported periodically; the final sample can omit last in-flight completions. Preserve this limitation when interpreting near-boundary totals.
- Worker membership changes during measured time invalidate a run. Restart a run after scaling workers.

## Distributed execution without Kubernetes

Use the same config and environment overrides in every terminal. Start workers first; they will wait for a master:

```bash
export BENCH_CONFIG=config/smoke.yaml
export MONGO_URI='mongodb://YOUR_MONGOS'
locust -f locustfile.py --worker --master-host 127.0.0.1
# Start another worker in a second terminal with the same environment.
```

Master terminal:

```bash
python -m benchmark.run --config config/smoke.yaml --workers 2 \
  --users 100 --spawn-rate 20 --duration 5m
```

Restart workers for each distributed run. `benchmark.run --repeats N` supports local runs; distributed repeats require a fresh master and worker cohort for each run. Config hashes are checked during worker initialization.

## Deploy to GKE Autopilot

Prerequisites: an existing Autopilot cluster, Artifact Registry repository, MongoDB network connectivity, `gcloud`, Docker, and `kubectl`. Place generators in the same region as the database and keep routing comparable across targets. Use private connectivity where available.

### 1. Build an immutable image

```bash
export PROJECT=your-project
export REGION=asia-east2
export IMAGE="$REGION-docker.pkg.dev/$PROJECT/bench/mongo-locust:v1"

# Authenticate Docker to Artifact Registry, then build locally and push directly.
gcloud auth configure-docker "$REGION-docker.pkg.dev"
docker build --tag "$IMAGE" .
docker push "$IMAGE"
```

The `bench` Docker repository must already exist in Artifact Registry in `$REGION`. Your account needs Artifact Registry Writer access to push, and the GKE node service account needs Artifact Registry Reader access to pull. The Docker image is built for your machine's default platform. The image uses Python 3.12, installs pinned driver/Locust versions, runs as a non-root user, and creates `/results`. For long-term reproducibility, preserve an image digest and the installed dependency inventory with each campaign.

### 2. Namespace, connection secret, and config

```bash
kubectl apply -f deploy/namespace.yaml

export TARGET=s1                 # s1 or s2
export SCENARIO=profile           # profile or adt
export CONFIG="/tmp/mongodb-bench-configs/$TARGET-$SCENARIO.yaml"
export OUT="result/deploy-$TARGET-$SCENARIO"
export MONGO_URI='mongodb://USER:PASSWORD@TARGET-MONGOS/?authSource=admin'

kubectl -n mongo-bench create secret generic mongodb \
  --from-literal=uri="$MONGO_URI" \
  --dry-run=client -o yaml | kubectl apply -f -

python -m benchmark.deploy --image "$IMAGE" --config "$CONFIG" \
  --workers 8 --users 200 --spawn-rate 20 --duration 30m --out "$OUT"
kubectl apply -f "$OUT/config.yaml"
```

The renderer writes concrete manifests, unique run ID, worker count, image, loader partitions, and master arguments. Follow [`docs/BENCHMARK_GUIDE.md`](docs/BENCHMARK_GUIDE.md) to generate the four configs, set up and load each cluster once, and run all four target/scenario combinations. The default setup shards collections on both clusters; do not pass `--unsharded`. Switch `MONGO_URI` and update the Secret whenever changing clusters. The config `shards` value is reporting metadata; the Secret selects the target.

### 3. Prepare the data

```bash
kubectl apply -f "$OUT/setup.yaml"
kubectl -n mongo-bench wait --for=condition=complete job/benchmark-setup --timeout=10m
kubectl apply -f "$OUT/loader.yaml"
kubectl -n mongo-bench logs -f job/benchmark-loader
kubectl -n mongo-bench wait --for=condition=complete job/benchmark-loader --timeout=24h
```

The loader defaults to eight indexed partitions. Change `--partitions` in the renderer to change both Kubernetes completions and generator partitioning together. Run setup/load once on `s1`, then switch the Secret to the independent `s2` cluster, delete the completed setup/loader Jobs, and run the same steps there. Run `validate` and `inspect` against each cluster before measuring; wait for balancing to settle and confirm the `s2` collections are distributed across both shards.

### 4. Start and scale workers

```bash
# Remove the previous run's resources before applying the next run.
kubectl -n mongo-bench delete job locust-master --ignore-not-found
kubectl -n mongo-bench delete deployment locust-worker --ignore-not-found
kubectl apply -f "$OUT/workers.yaml"
kubectl apply -f "$OUT/master.yaml"
kubectl -n mongo-bench logs -f job/locust-master
```

Repeat the manifest render/apply for each of the four target/scenario configs. Before changing targets, update the MongoDB Secret to the new cluster's `mongos` URI. The master waits up to 600 seconds for the configured worker count. Worker pods may be running while waiting for the master service; Locust registration, rather than Kubernetes pod readiness alone, determines test readiness.

For a new 16-worker run, render a new run directory with `--workers 16`, delete the previous master Job and worker Deployment, apply the new config/workers/master manifests. Jobs have immutable pod templates; create fresh Jobs between runs. Keep dataset preparation Jobs unless reloading is intended.

For exploratory scaling before measurement:

```bash
kubectl -n mongo-bench scale deployment locust-worker --replicas=16
```

Also update the next master's `--expect-workers` count. **Do not scale during a measured run.** Autopilot provisions node capacity for the pods; HPA is deliberately excluded from controlled benchmark runs.

Pods initially request/limit 1 vCPU and 2 GiB RAM. Each worker runs one Python process. Calibrate worker CPU, event-loop lag, pool pressure, and network throughput; increase worker replicas before concluding the database is saturated. Monitor throttling and record effective resource requests after Autopilot admission. The master has a ClusterIP service on 5557 and 8089; headless mode does not start the UI. For interactive exploration remove `--headless`/headless-only arguments from a separate manifest and port-forward port 8089.

### 5. Save GKE results to your local disk

The default workflow stores results in `/results` inside the master pod. It does **not** upload to GCS unless you pass `--gcs-prefix`. After the benchmark finishes, copy the files to your computer before deleting the Job or pod. Run these commands on your local machine in the repository directory:

```bash
mkdir -p "result/$TARGET/$SCENARIO"
MASTER_POD=$(kubectl -n mongo-bench get pods -l app=locust-master \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n mongo-bench cp "$MASTER_POD:/results/." "result/$TARGET/$SCENARIO/"
```

The files are now on your local disk under `result/<target>/<scenario>/`. Keep the completed master Job/pod until the copy succeeds. The `result/` folder is git-ignored to avoid committing benchmark artifacts. If you also want a cloud backup, optionally render the deployment with `--gcs-prefix gs://YOUR_BUCKET/PREFIX`; the GKE service account must have GCS write access.

## Comparison procedure

1. Prepare independent 1- and 2-shard targets with equal **per-shard** resources, replica count, MongoDB version, indexes, and concerns. Route all through `mongos`.
2. Keep the exact dataset specification constant. Verify distribution and query targeting.
3. Run the same production-shaped profile workload against each target, sweeping offered load to find sustainable throughput at the customer's latency/error SLO.
4. Run the finite ADT scenario separately against each target and compare completion time.
5. Run incremental ADT separately if it is representative of the customer's workload.
6. Sweep users or scheduled rate while preserving generator headroom. Fix worker count within each run.
7. Repeat key points at least three times, alternate target order, and restore comparable mutable state.
8. Collect MongoDB/Atlas CPU, cache eviction, storage latency/IOPS, replication lag, router CPU, and per-shard activity. Collect GKE CPU/throttling, memory, network, restarts, and effective resource allocations. These external metrics are not scraped automatically by this repository.
9. Compare successful throughput **at the same SLO** and ADT completion time. Include total resources and cost.

Multiple equivalent shards add total hardware resources. An optional equal-budget scale-up comparison answers a separate economic question. Low-load point latency and single-document hotspot contention may not improve with sharding. Preserve range/card/host controls in the report to make routing tradeoffs visible.

## Results and reporting

Each runner invocation writes:

```text
result/RUN_ID/
  run-RUN_ID.json          config/hash, versions, timings, operation histograms,
                          finite-job ledger, arrival counters, cumulative snapshots
  locust_stats.csv
  locust_stats_history.csv
  locust_failures.csv
  locust_exceptions.csv
  locust.html
```

Local runs save beneath `./result/` by default. GKE runs initially save beneath `/results` in the master pod; copy them to your local `result/<target>/<scenario>/` directory with `kubectl cp` after completion. GCS upload is optional and requires `--gcs-prefix`. Both `result/` and legacy `results/` output directories are git-ignored.

```bash
python -m benchmark.report result/*/run-*.json --out result/summary.csv
```

For GKE runs, point the report command at the local copies, for example:

```bash
python -m benchmark.report result/*/*/run-*.json --out result/summary.csv
```

The report emits per-run CSV plus repeated-run median/min/max successful throughput and median ADT duration. It separates profile operations, incremental E2E operations, scheduler events, and component timings; it does not treat the sum of Locust request counts as business throughput. Worst per-operation p99 is reported rather than averaging percentiles. SLO evaluation applies the configured whole-measurement thresholds.

`valid` means the measurement completed without framework/incomplete-batch errors. `slo_pass` separately evaluates errors, configured latency/batch limits, and arrival shedding/completion. Arrival counters are periodic in distributed mode, so examine boundary samples before making strict completion claims. A valid run can fail its SLO.

Suggested presentation charts:

- Sustainable successful profile ops/s versus shard count.
- Profile p99 versus offered load.
- ADT wall-clock duration versus shard count.
- Per-shard CPU/cache/I/O and throughput per dollar.

```text
Throughput speedup(N) = sustainable throughput(N) / throughput(1)
Batch speedup(N)      = ADT duration(1) / ADT duration(N)
Scaling efficiency   = speedup(N) / N
```

## Verification

```bash
python -m unittest discover -s tests -v
python -m tests.check_distributed
locust -f locustfile.py --list
```

The core tests check partition-independent data generation, ADT date boundaries/rounding, expired-lease retries, stale acknowledgments, duplicate completion, terminal failures, and reporting semantics. The distributed check runs a real Locust master and two real worker runners over sockets with in-memory workload execution, verifying complete finite-job coverage and result artifacts. It also checks that the bounded arrival scheduler records overload shedding. Live MongoDB/GKE testing is still needed for routing, server aggregation, storage metrics, and performance claims.

## Repository map

```text
benchmark/config.py       Configuration/defaults/validation
benchmark/mongo.py        PyMongo connections and concerns
benchmark/workloads.py    Instrumented database operations
benchmark/adt.py          ADT pipeline and Decimal reference
benchmark/jobs.py         Lease-based logical job ledger
benchmark/runtime.py      Locust distributed coordination and arrivals
benchmark/data.py         Setup/load/validate/inspect/preview CLI
benchmark/run.py          Repeated local or distributed-master runner
benchmark/deploy.py       Kubernetes manifest renderer
benchmark/artifacts.py    Subprocess execution and GCS upload
benchmark/report.py       Per-run and repeated-run summaries
locustfile.py             Gevent patching, events, online user
config/                   Smoke and production-shaped examples
deploy/                   GKE Autopilot manifests
tests/                    Correctness and distributed coordination checks
```
