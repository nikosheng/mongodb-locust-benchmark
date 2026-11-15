"""Render repeatable Kubernetes manifests from checked-in templates."""
import argparse
import json
from pathlib import Path
import uuid

import yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Use an immutable image tag or digest")
    parser.add_argument("--config", default="config/smoke.yaml")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--spawn-rate", type=float, default=20)
    parser.add_argument("--duration", default="20m")
    parser.add_argument("--gcs-prefix", help="gs://bucket/prefix; run ID is appended")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--unsharded", action="store_true", help="Setup collections without sharding")
    parser.add_argument("--out", default="results/deploy")
    args = parser.parse_args()
    if min(args.workers, args.users, args.partitions, args.spawn_rate) <= 0:
        parser.error("Workers, users, partitions, and spawn-rate must be positive")
    run_id = args.run_id or uuid.uuid4().hex[:12]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for template in Path("deploy").glob("*.yaml"):
        documents = list(yaml.safe_load_all(template.read_text()))
        for doc in documents:
            if doc["kind"] not in {"Job", "Deployment"}:
                continue
            spec = doc["spec"]
            pod = spec["template"]["spec"]
            container = pod["containers"][0]
            container["image"] = args.image
            if template.stem == "workers":
                spec["replicas"] = args.workers
            elif template.stem == "loader":
                spec["completions"] = spec["parallelism"] = args.partitions
                container["args"][container["args"].index("--partitions") + 1] = str(args.partitions)
            elif template.stem == "setup" and args.unsharded:
                container["args"].remove("--sharded")
            elif template.stem == "master":
                values = {"--expect-workers": args.workers, "-u": args.users,
                          "-r": args.spawn_rate, "--run-time": args.duration}
                for flag, value in values.items():
                    container["args"][container["args"].index(flag) + 1] = str(value)
                for env in container["env"]:
                    if env["name"] == "BENCH_RUN_ID":
                        env["value"] = run_id
                    elif env["name"] == "BENCH_GCS_PREFIX":
                        env["value"] = f"{args.gcs_prefix.rstrip('/')}/{run_id}" if args.gcs_prefix else ""
        (out / template.name).write_text(yaml.safe_dump_all(documents, sort_keys=False))
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {
        "name": "benchmark-config", "namespace": "mongo-bench"},
        "data": {"benchmark.yaml": Path(args.config).read_text()}}
    (out / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
    print(json.dumps({"run_id": run_id, "manifests": str(out), "workers": args.workers}))


if __name__ == "__main__":
    main()
