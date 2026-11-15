import copy
import unittest
from datetime import timedelta
from decimal import Decimal

from bson.decimal128 import Decimal128

from benchmark.adt import reference
from benchmark.config import DEFAULTS, as_of
from benchmark.data import daily_rows, profile
from benchmark.jobs import JobLedger
from benchmark.report import summarize


class CoreTests(unittest.TestCase):
    def test_partition_independent_documents(self):
        cfg = copy.deepcopy(DEFAULTS)
        direct = {i: (profile(cfg, i), list(daily_rows(cfg, i))) for i in range(12)}
        partitioned = {}
        for partition in range(3):
            for i in range(partition, 12, 3):
                partitioned[i] = (profile(cfg, i), list(daily_rows(cfg, i)))
        self.assertEqual(direct, partitioned)
        for i, (_, rows) in direct.items():
            self.assertEqual(len({r["gaming_date"] for r in rows}), len(rows))

    def test_adt_boundaries_and_active_denominator(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg["windows"] = [7]
        rows = [{"gaming_date": as_of(cfg) - timedelta(days=age),
                 "gaming": {"total": {"theo_win": Decimal128(value)}}}
                for age, value in [(0, "1.00"), (6, "2.01"), (7, "1000.00")]]
        result = reference(cfg, rows)
        self.assertEqual(result["d7"], Decimal("1.50"))  # ties-to-even
        self.assertEqual(result["active_days"]["d7"], 2)
        cfg["denominator"] = "window"
        self.assertEqual(reference(cfg, rows)["d7"], Decimal("0.43"))

    def test_expired_assignment_cannot_complete_new_attempt(self):
        ledger = JobLedger(5, 100, 2, 10, 2)
        first = ledger.claim("lost-worker", now=0)
        ledger.expire(now=11)
        jobs = [ledger.claim("worker", now=12) for _ in range(3)]
        retried = next(j for j in jobs if j["job"] == first["job"])
        self.assertFalse(ledger.finish("lost-worker", first["job"], 1, {"patrons": 99}))
        self.assertTrue(ledger.finish("worker", retried["job"], 2, {"patrons": 2}))
        self.assertFalse(ledger.finish("worker", retried["job"], 2, {"patrons": 2}))
        self.assertEqual(ledger.totals["patrons"], 2)
        for job in jobs:
            ledger.finish("worker", job["job"], job["attempt"])
        self.assertTrue(ledger.done)
        self.assertEqual(len(ledger.completed), 3)

    def test_terminal_failure_and_partial_final_slice(self):
        ledger = JobLedger(5, 100, 2, 10, 1)
        self.assertEqual(ledger.jobs, [(100, 102), (102, 104), (104, 105)])
        while not ledger.done:
            job = ledger.claim("worker")
            ledger.finish("worker", job["job"], job["attempt"], error="timeout")
        self.assertEqual(ledger.summary()["failed_jobs"], 3)

    def test_reporting_does_not_double_count_incremental_components(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg["scenario"] = "incremental"
        run = {"run_id": "test", "label": "s1", "valid": True, "config": cfg,
               "measurement_seconds": 10,
               "arrivals": {"scheduled": 0, "started": 0, "completed": 0, "failed": 0, "dropped": 0},
               "operations": [{"type": kind, "name": name, "requests": 100, "failures": 0, "p99_ms": 20}
                              for kind, name in [("BUSINESS", "adt.refresh_patron_e2e"),
                                                 ("MONGO", "adt.compute_batch"),
                                                 ("MONGO", "adt.writeback_batch")]]}
        self.assertEqual(summarize(run)["successful_ops_s"], 10)
        self.assertTrue(summarize(run)["slo_pass"])

    def test_reporting_rejects_arrival_shedding(self):
        cfg = copy.deepcopy(DEFAULTS)
        cfg["load"]["mode"] = "arrival"
        run = {"run_id": "test", "label": "s1", "valid": True, "config": cfg,
               "measurement_seconds": 10,
               "arrivals": {"scheduled": 101, "started": 100, "completed": 100, "failed": 0, "dropped": 1},
               "operations": [{"type": "MONGO", "name": "profile.read_full", "requests": 100,
                               "failures": 0, "p99_ms": 20}]}
        self.assertFalse(summarize(run)["slo_pass"])


if __name__ == "__main__":
    unittest.main()
