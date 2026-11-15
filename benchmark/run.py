"""Repeat local/distributed-master runs while preserving subprocess failure status."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/smoke.yaml")
    parser.add_argument("--users", type=int, default=20)
    parser.add_argument("--spawn-rate", type=float, default=10)
    parser.add_argument("--duration", default="5m", help="Includes warmup; ADT stops early on completion")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--workers", type=int, help="Run master; workers must already be running with identical configuration")
    args = parser.parse_args()
    if args.repeats < 1 or args.users < 1 or args.spawn_rate <= 0:
        parser.error("repeats, users, and spawn-rate must be positive")
    if args.workers and args.repeats != 1:
        parser.error("Distributed repeats need fresh worker processes for each run; use --repeats 1")
    root = Path(os.getenv("BENCH_RESULTS", "result"))
    root.mkdir(parents=True, exist_ok=True)
    for _ in range(args.repeats):
        run_id = uuid.uuid4().hex[:12]
        directory = root / run_id
        directory.mkdir()
        env = {**os.environ, "BENCH_CONFIG": args.config, "BENCH_RUN_ID": run_id,
               "BENCH_RESULTS": str(directory)}
        command = [sys.executable, "-m", "locust", "-f", "locustfile.py", "--headless",
                   "-u", str(args.users), "-r", str(args.spawn_rate), "--run-time", args.duration,
                   "--stop-timeout", "30", "--csv", str(directory / "locust"), "--csv-full-history",
                   "--html", str(directory / "locust.html")]
        if args.workers:
            command += ["--master", "--expect-workers", str(args.workers), "--expect-workers-max-wait", "600"]
        result = subprocess.run(command, env=env, check=False)
        if result.returncode:
            sys.exit(result.returncode)


if __name__ == "__main__":
    main()
