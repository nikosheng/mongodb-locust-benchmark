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
from locust import User, task
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


class IdleUser(User):
    @task
    def idle(self):
        gevent.sleep(0.05)


def main():
    cfg = copy.deepcopy(DEFAULTS)
    cfg.update(scenario="adt", players=23, warmup_seconds=0.3)
    cfg["batch"].update(size=5, slots_per_worker=2)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    master_env = Environment(user_classes=[IdleUser])
    master = master_env.create_master_runner("127.0.0.1", port)
    worker_envs = [Environment(user_classes=[IdleUser]) for _ in range(2)]
    workers = [env.create_worker_runner("127.0.0.1", port) for env in worker_envs]
    runtimes = [Runtime(master_env, cfg)] + [Runtime(env, cfg) for env in worker_envs]
    for env, runtime in zip([master_env] + worker_envs, runtimes):
        env.events.test_start.add_listener(lambda environment, runtime=runtime, **kw: runtime.start())
        env.events.test_stop.add_listener(lambda environment, runtime=runtime, **kw: runtime.stop())
        if runtime.worker:
            env.events.report_to_master.add_listener(runtime.report)

            def final_event(environment, runtime=runtime, **kw):
                # This sample exists only after the worker has stopped.
                runtime.counts["completed"] = 42
                environment.events.request.fire(request_type="MONGO", name="final.sample",
                                                response_time=1, response_length=0, exception=None)

            env.events.test_stop.add_listener(final_event)
    master_env.events.worker_report.add_listener(runtimes[0].receive_report)
    with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, BENCH_RESULTS=directory), \
            patch("benchmark.runtime.connect", return_value=(Client(), None)), \
            patch("benchmark.runtime.Workloads", Work):
        gevent.sleep(0.2)
        assert len(master.clients) == 2
        master.start(2, 20)
        with gevent.Timeout(10):
            while not list(Path(directory).glob("run-*.json")):
                gevent.sleep(0.05)
        ledger = runtimes[0].ledger
        assert ledger.done and not ledger.failed
        assert ledger.totals == {"patrons": 23, "daily_rows": 115, "profiles_written": 23}
        assert len(ledger.completed) == 5
        result = json.loads(next(Path(directory).glob("run-*.json")).read_text())
        assert result["valid"] and result["worker_count"] == 2
        assert result["missing_final_workers"] == []
        assert result["final_worker_reports_complete"]
        assert result["interrupted_operations"] == 0
        assert result["arrivals"]["completed"] == 84
        assert next(op for op in result["operations"] if op["name"] == "final.sample")["requests"] == 2
        assert result["batch"]["completed_jobs"] == 5
    for worker in workers:
        worker.quit()
    master.quit()
    print("PASS: final worker stats and counters are included in the distributed artifact")

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
