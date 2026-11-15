"""Locust lifecycle, finite-batch coordination, and bounded scheduled arrivals."""
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import version

import gevent
from gevent.pool import Pool
from gevent.queue import Queue
from locust.runners import MasterRunner, WorkerRunner

from benchmark.config import config_hash, output_dir
from benchmark.jobs import JobLedger
from benchmark.mongo import connect
from benchmark.workloads import Workloads

LOG = logging.getLogger(__name__)


class Runtime:
    def __init__(self, environment, cfg):
        self.env, self.cfg = environment, cfg
        self.runner = environment.runner
        self.master = isinstance(self.runner, MasterRunner)
        self.worker = isinstance(self.runner, WorkerRunner)
        self.controller = not self.worker
        self.owner = self.runner.client_id if self.worker else "local"
        self.greenlets = []
        self.client = self.work = None
        self.stopping = False
        self.run_id = None
        self.slots = {}
        self.counts = {"scheduled": 0, "started": 0, "completed": 0, "failed": 0, "dropped": 0}
        self.worker_counts = {}
        self.assignment = {"rank": 0, "workers": 1}
        self.started_at = self.measurement_at = self.batch_started = self.batch_ended = None
        self.ledger = None
        self.finished_successfully = False
        self.snapshots = []
        self.arrival_pool = Pool(cfg["load"]["max_inflight_per_worker"])
        for name, handler in (("bench_hello", self.on_hello), ("bench_init", self.on_init),
                              ("bench_claim", self.on_claim), ("bench_job", self.on_job),
                              ("bench_heartbeat", self.on_heartbeat), ("bench_done", self.on_done)):
            self.runner.register_message(name, handler)

    def send(self, name, data, target=None):
        if self.worker:
            self.runner.send_message(name, data)
        elif self.master:
            self.runner.send_message(name, data, client_id=target)
        else:
            handlers = {"bench_hello": self.on_hello, "bench_init": self.on_init,
                        "bench_claim": self.on_claim, "bench_job": self.on_job,
                        "bench_heartbeat": self.on_heartbeat, "bench_done": self.on_done}
            from types import SimpleNamespace
            handlers[name](self.env, SimpleNamespace(data=data, node_id="local"))

    def start(self):
        self.stopping = False
        if self.controller:
            self.run_id = os.getenv("BENCH_RUN_ID") or uuid.uuid4().hex[:12]
            self.started_at = time.time()
            self.measurement_at = self.started_at + self.cfg["warmup_seconds"]
            if self.cfg["scenario"] in {"adt", "combined"}:
                b = self.cfg["batch"]
                self.ledger = JobLedger(self.cfg["players"], self.cfg["player_id_base"], b["size"],
                                        b["lease_seconds"], b["max_attempts"])
            self.greenlets.append(gevent.spawn(self.coordinate))
        if not self.master:
            self.greenlets.append(gevent.spawn(self.start_worker))

    def on_hello(self, environment, msg, **kwargs):
        if not self.controller or not self.run_id:
            return
        if msg.data["config_hash"] != config_hash(self.cfg):
            LOG.error("Worker configuration differs from master")
            self.env.process_exit_code = 2
            gevent.spawn(self.runner.quit)
            return
        clients = sorted(self.runner.clients.keys()) if self.master else ["local"]
        self.send("bench_init", {"run_id": self.run_id, "measurement_at": self.measurement_at,
                                 "rank": clients.index(msg.node_id), "workers": len(clients)}, msg.node_id)

    def on_init(self, environment, msg, **kwargs):
        self.run_id = msg.data["run_id"]
        self.measurement_at = msg.data["measurement_at"]
        self.assignment = {k: msg.data[k] for k in ("rank", "workers")}

    def start_worker(self):
        try:
            while not self.measurement_at or not self.run_id:
                self.send("bench_hello", {"config_hash": config_hash(self.cfg)})
                gevent.sleep(0.2)
            # Local mode already has metadata, but still needs no network handshake.
            self.client, db = connect(self.cfg)
            self.client.admin.command("ping")
            seed = self.cfg["seed"] + self.assignment["rank"]
            self.work = Workloads(self.cfg, db, self.env.events, seed, self.run_id)
            if self.cfg["scenario"] in {"adt", "combined"}:
                for slot in range(self.cfg["batch"]["slots_per_worker"]):
                    self.slots[slot] = Queue()
                    self.greenlets.append(gevent.spawn(self.batch_slot, slot))
            if self.cfg["scenario"] != "adt" and self.cfg["load"]["mode"] == "arrival":
                self.greenlets.append(gevent.spawn(self.arrivals))
        except Exception:
            LOG.exception("Worker initialization failed")
            self.env.process_exit_code = 2
            if self.worker:
                self.send("bench_done", {"fatal": "worker initialization failed"})
            gevent.spawn(self.runner.quit)

    def on_claim(self, environment, msg, **kwargs):
        if not self.controller:
            return
        slot = msg.data["slot"]
        job = None
        if self.ledger and time.time() >= self.measurement_at + self.cfg["batch"]["delay_seconds"]:
            if self.batch_started is None:
                self.batch_started = time.time()
            job = self.ledger.claim(f"{msg.node_id}:{slot}")
        self.send("bench_job", {"slot": slot, "assignment": job}, msg.node_id)

    def on_job(self, environment, msg, **kwargs):
        if msg.data["slot"] in self.slots:
            self.slots[msg.data["slot"]].put(msg.data["assignment"])

    def on_heartbeat(self, environment, msg, **kwargs):
        if self.controller and self.ledger:
            d = msg.data
            self.ledger.heartbeat(f"{msg.node_id}:{d['slot']}", d["job"], d["attempt"])

    def on_done(self, environment, msg, **kwargs):
        if not self.controller:
            return
        d = msg.data
        if d.get("fatal"):
            LOG.error("Worker fatal error: %s", d["fatal"])
            self.env.process_exit_code = 2
            gevent.spawn(self.runner.quit)
        elif self.ledger:
            self.ledger.finish(f"{msg.node_id}:{d['slot']}", d["job"], d["attempt"],
                               d.get("counts"), d.get("error"))

    def batch_slot(self, slot):
        gevent.sleep(max(0, self.measurement_at + self.cfg["batch"]["delay_seconds"] - time.time()))
        while not self.stopping:
            self.send("bench_claim", {"slot": slot})
            job = self.slots[slot].get()
            if not job:
                gevent.sleep(0.5)
                continue
            message = {"slot": slot, "job": job["job"], "attempt": job["attempt"]}

            def heartbeat():
                while True:
                    gevent.sleep(self.cfg["batch"]["lease_seconds"] / 3)
                    self.send("bench_heartbeat", message)

            heart = gevent.spawn(heartbeat)
            try:
                counts = self.work.measured("adt.job_e2e", lambda: self.work.compute(job["lo"], job["hi"]), "BUSINESS")
                self.send("bench_done", {**message, "counts": counts})
            except Exception as exc:
                LOG.warning("ADT job %s failed: %s", job["job"], type(exc).__name__)
                self.send("bench_done", {**message, "error": type(exc).__name__})
            finally:
                heart.kill()

    def online(self):
        try:
            self.work.online()
            return True
        except Exception:
            # The instrumented operation has already emitted a failure event.
            return False

    def arrivals(self):
        # Fixed worker membership is intentional: changing it invalidates a rate-controlled run.
        interval = self.assignment["workers"] / self.cfg["load"]["rate"]
        due = time.monotonic() + interval * self.assignment["rank"] / self.assignment["workers"]

        def execute(scheduled, measured):
            lag_ms = (time.monotonic() - scheduled) * 1000
            if measured:
                self.counts["started"] += 1
                self.env.events.request.fire(request_type="SCHEDULER", name="arrival.start_lag",
                                             response_time=lag_ms, response_length=0, exception=None)
            successful = self.online()
            if measured:
                self.counts["completed" if successful else "failed"] += 1

        while not self.stopping:
            gevent.sleep(max(0, due - time.monotonic()))
            measured = time.time() >= self.measurement_at
            if measured:
                self.counts["scheduled"] += 1
            if self.arrival_pool.full():
                if measured:
                    self.counts["dropped"] += 1
            else:
                self.arrival_pool.spawn(execute, due, measured)
            due += interval

    def coordinate(self):
        gevent.sleep(max(0, self.measurement_at - time.time()))
        self.env.stats.reset_all()
        self.env.events.reset_stats.fire()
        last_snapshot = 0
        initial_clients = set(self.runner.clients) if self.master else {"local"}
        self.worker_count = len(initial_clients)
        while not self.stopping:
            now = time.time()
            if self.master and set(self.runner.clients) != initial_clients:
                LOG.error("Worker membership changed during measurement; run invalid")
                self.env.process_exit_code = 2
                gevent.spawn(self.runner.quit)
                return
            if self.ledger:
                self.ledger.expire()
                if self.ledger.done and self.batch_ended is None:
                    self.batch_ended = now
                    LOG.info("ADT finished: %s", self.ledger.summary())
                    if self.ledger.failed:
                        self.env.process_exit_code = 2
                    if self.cfg["scenario"] == "adt":
                        self.finished_successfully = not self.ledger.failed
                        gevent.spawn(self.runner.quit)
                        return
            if now - last_snapshot >= 5:
                self.snapshots.append({"epoch": now, "phase": "after_adt" if self.batch_ended else (
                    "during_adt" if self.batch_started else "online"), "operations": self.operation_stats()})
                last_snapshot = now
            gevent.sleep(0.25)

    def operation_stats(self):
        return [{"type": entry.method, "name": entry.name, "requests": entry.num_requests,
                 "failures": entry.num_failures, "p50_ms": entry.get_response_time_percentile(0.50),
                 "p95_ms": entry.get_response_time_percentile(0.95),
                 "p99_ms": entry.get_response_time_percentile(0.99),
                 "response_times": dict(entry.response_times)}
                for entry in self.env.stats.entries.values()]

    def report(self, client_id, data, **kwargs):
        data["benchmark_arrivals"] = dict(self.counts)

    def receive_report(self, client_id, data, **kwargs):
        self.worker_counts[client_id] = data.get("benchmark_arrivals", {})

    def stop(self):
        self.stopping = True
        gevent.killall(self.greenlets, block=True)
        self.arrival_pool.kill()
        if self.client:
            self.client.close()
        if not self.controller:
            return
        end = time.time()
        duration = max(0, end - self.measurement_at)
        arrivals = dict(self.counts)
        if self.master:
            arrivals = {k: sum(c.get(k, 0) for c in self.worker_counts.values()) for k in self.counts}
        has_work = bool(self.ledger) or any(entry.num_requests for entry in self.env.stats.entries.values()
                                          if entry.method != "SCHEDULER")
        valid = duration > 0 and has_work and not self.env.process_exit_code
        if self.ledger and (not self.ledger.done or self.ledger.failed):
            valid = False
        result = {"run_id": self.run_id, "label": self.cfg["label"], "config": self.cfg,
                  "config_hash": config_hash(self.cfg), "started_at": self.started_at,
                  "measurement_at": self.measurement_at, "ended_at": end,
                  "measurement_seconds": duration, "valid": valid,
                  "worker_count": getattr(self, "worker_count", None),
                  "user_count": getattr(self.runner, "target_user_count", None),
                  "versions": {name: version(name) for name in ("pymongo", "locust")},
                  "operations": self.operation_stats(), "arrivals": arrivals,
                  "arrival_counts_note": "Distributed counters are periodic reports; final in-flight work may be absent.",
                  "batch": self.ledger.summary() if self.ledger else None,
                  "batch_started_at": self.batch_started, "batch_ended_at": self.batch_ended,
                  "batch_seconds": (self.batch_ended - self.batch_started) if self.batch_ended and self.batch_started else None,
                  "snapshots": self.snapshots,
                  "recorded_at": datetime.now(timezone.utc).isoformat()}
        path = output_dir() / f"run-{self.run_id}.json"
        path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        if not valid:
            self.env.process_exit_code = self.env.process_exit_code or 2
        LOG.info("Benchmark results saved to %s (valid=%s)", path, valid)
