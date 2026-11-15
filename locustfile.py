"""Run with Locust, never import PyMongo ahead of monkey patching."""
from gevent import monkey
monkey.patch_all()

import gevent
import time
from locust import User, events, task
from locust.runners import MasterRunner

from benchmark.config import load_config
from benchmark.runtime import Runtime


@events.init.add_listener
def initialize(environment, **kwargs):
    environment.benchmark = Runtime(environment, load_config())


@events.test_start.add_listener
def start(environment, **kwargs):
    environment.benchmark.start()
    runtime = environment.benchmark
    if not isinstance(environment.runner, MasterRunner):
        def reset_worker():
            while runtime.measurement_at is None:
                gevent.sleep(0.05)
            gevent.sleep(max(0, runtime.measurement_at - time.time()))
            environment.stats.reset_all()
        runtime.greenlets.append(gevent.spawn(reset_worker))


@events.test_stop.add_listener
def stop(environment, **kwargs):
    environment.benchmark.stop()


@events.report_to_master.add_listener
def worker_report(client_id, data, **kwargs):
    # Event has no environment parameter; obtain the active runtime via User's environment below.
    if _runtime is not None:
        _runtime.report(client_id, data)


_runtime = None


@events.init.add_listener
def capture_runtime(environment, **kwargs):
    global _runtime
    _runtime = environment.benchmark
    environment.events.worker_report.add_listener(_runtime.receive_report)


class MongoUser(User):
    """One business operation per task; use -u for concurrency mode only."""

    @task
    def execute(self):
        runtime = self.environment.benchmark
        if runtime.work is None or runtime.cfg["scenario"] == "adt" or runtime.cfg["load"]["mode"] == "arrival":
            gevent.sleep(0.1)
            return
        runtime.online()
        gevent.sleep(0)  # Keep the control plane responsive even for very fast operations.
