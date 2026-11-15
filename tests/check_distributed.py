"""Exercise real Locust master/worker messaging with deterministic in-memory work.

Run separately so gevent patching happens before driver imports.
"""
from gevent import monkey
monkey.patch_all()

import copy
import json
import os
import socket
import tempfile
from pathlib import Path
from unittest.mock import patch

import gevent
from locust.env import Environment

from benchmark.config import DEFAULTS
from benchmark.runtime import Runtime


class Client:
    admin = None

    def __init__(self):
        self.admin = self

    def command(self, name):
        return {"ok": 1}

    def close(self):
        pass


class Work:
    def __init__(self, *args):
        pass

    def measured(self, name, function, request_type="MONGO"):
        return function()

    def compute(self, lo, hi):
        gevent.sleep(0.01)
        return {"patrons": hi - lo, "daily_rows": (hi - lo) * 5, "profiles_written": hi - lo}

    def online(self):
        gevent.sleep(0.03)


def main():
    cfg = copy.deepcopy(DEFAULTS)
    cfg.update(scenario="combined", players=23, warmup_seconds=0.3)
    cfg["batch"].update(size=5, slots_per_worker=2)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    master_env = Environment()
    master = master_env.create_master_runner("127.0.0.1", port)
    worker_envs = [Environment() for _ in range(2)]
    workers = [env.create_worker_runner("127.0.0.1", port) for env in worker_envs]
    runtimes = [Runtime(master_env, cfg)] + [Runtime(env, cfg) for env in worker_envs]
    with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, BENCH_RESULTS=directory), \
            patch("benchmark.runtime.connect", return_value=(Client(), None)), \
            patch("benchmark.runtime.Workloads", Work):
        gevent.sleep(0.2)
        assert len(master.clients) == 2
        for runtime in runtimes:
            runtime.start()
        with gevent.Timeout(10):
            while not runtimes[0].batch_ended:
                gevent.sleep(0.05)
        ledger = runtimes[0].ledger
        assert ledger.done and not ledger.failed
        assert ledger.totals == {"patrons": 23, "daily_rows": 115, "profiles_written": 23}
        assert len(ledger.completed) == 5
        for runtime in reversed(runtimes):
            runtime.stop()
        result = json.loads(next(Path(directory).glob("run-*.json")).read_text())
        assert result["valid"] and result["worker_count"] == 2
        assert result["batch"]["completed_jobs"] == 5
    for worker in workers:
        worker.quit()
    master.quit()
    print("PASS: two real Locust workers completed five disjoint finite jobs; totals and artifact verified")

    # Arrival scheduler must expose overload rather than hide it in closed-loop throughput.
    env = Environment()
    runner = env.create_local_runner()
    cfg.update(scenario="profile", warmup_seconds=0)
    cfg["load"].update(mode="arrival", rate=500, max_inflight_per_worker=1)
    runtime = Runtime(env, cfg)
    with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, BENCH_RESULTS=directory), \
            patch("benchmark.runtime.connect", return_value=(Client(), None)), \
            patch("benchmark.runtime.Workloads", Work):
        runtime.start()
        gevent.sleep(0.5)
        runtime.stop()
        assert runtime.counts["scheduled"] > 100
        assert runtime.counts["dropped"] > 50
        assert runtime.counts["completed"] > 0
        assert runtime.counts["started"] <= 20
    runner.quit()
    print("PASS: bounded arrival scheduler records completed work and dropped overload")


if __name__ == "__main__":
    main()
