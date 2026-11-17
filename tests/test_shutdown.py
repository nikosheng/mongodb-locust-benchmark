import copy
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from greenlet import GreenletExit

from benchmark import artifacts
from benchmark.config import DEFAULTS
from benchmark.workloads import Workloads

# These unit tests do not use network I/O; distributed coverage patches before imports.
with patch.dict(os.environ, {"LOCUST_SKIP_MONKEY_PATCH": "1"}):
    from locust.env import Environment
    from benchmark.runtime import Runtime


class ShutdownTests(unittest.TestCase):
    def setUp(self):
        self.work = Workloads.__new__(Workloads)
        self.work.events = SimpleNamespace(request=SimpleNamespace(fire=Mock()))
        self.work.interrupted = 0

    def test_completed_operation_is_recorded(self):
        self.assertEqual(self.work.measured("test", lambda: 42), 42)
        self.assertIsNone(self.work.events.request.fire.call_args.kwargs["exception"])

    def test_real_operation_failure_is_recorded(self):
        error = ValueError("database failure")
        with self.assertRaises(ValueError):
            self.work.measured("test", Mock(side_effect=error))
        self.assertIs(self.work.events.request.fire.call_args.kwargs["exception"], error)

    def test_greenlet_cancellation_is_not_reported_as_failure_or_success(self):
        with self.assertRaises(GreenletExit):
            self.work.measured("test", Mock(side_effect=GreenletExit()))
        self.work.events.request.fire.assert_not_called()
        self.assertEqual(self.work.interrupted, 1)

    def test_missing_worker_report_invalidates_artifact(self):
        cfg = copy.deepcopy(DEFAULTS)
        env = Environment()
        env.create_local_runner()
        runtime = Runtime(env, cfg)
        runtime.master = True
        runtime.controller = True
        runtime.runner = SimpleNamespace(clients={}, target_user_count=1)
        runtime.run_id = "test"
        runtime.measurement_clients = {"lost-worker"}
        runtime.measurement_at = runtime.started_at = time.time() - 1
        runtime.worker_count = 1
        env.stats.log_request("MONGO", "profile.read_full", 1, 0)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, BENCH_RESULTS=directory):
            with self.assertLogs("benchmark.runtime", level="ERROR"):
                runtime.stop()
            result = json.loads((Path(directory) / "run-test.json").read_text())
        self.assertFalse(result["valid"])
        self.assertFalse(result["final_worker_reports_complete"])
        self.assertEqual(result["missing_final_workers"], ["lost-worker"])
        self.assertEqual(env.process_exit_code, 2)
        env.runner.quit()

    def test_connected_worker_timeout_is_not_treated_as_complete(self):
        runtime = Runtime.__new__(Runtime)
        runtime.measurement_clients = {"worker"}
        runtime.final_workers = set()
        runtime.runner = SimpleNamespace(clients={"worker": object()})
        runtime.env = SimpleNamespace(stop_timeout=0, process_exit_code=None)
        runtime.run_id = "test"
        runtime.send = Mock()
        with patch("benchmark.runtime.time.monotonic", side_effect=[0, 11]), \
                self.assertLogs("benchmark.runtime", level="ERROR"):
            self.assertEqual(runtime.finalize_workers(), ["worker"])
        self.assertFalse(runtime.final_reports_complete)
        self.assertEqual(runtime.env.process_exit_code, 2)
        runtime.send.assert_called_once_with("bench_finalize", {"run_id": "test"}, "worker")

    def test_interrupted_operation_invalidates_result(self):
        cfg = copy.deepcopy(DEFAULTS)
        env = Environment()
        env.create_local_runner()
        runtime = Runtime(env, cfg)
        runtime.run_id = "test"
        runtime.measurement_at = runtime.started_at = time.time() - 1
        runtime.work = SimpleNamespace(interrupted=1)
        env.stats.log_request("MONGO", "profile.read_full", 1, 0)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, BENCH_RESULTS=directory):
            with self.assertLogs("benchmark.runtime", level="ERROR"):
                runtime.stop()
            result = json.loads((Path(directory) / "run-test.json").read_text())
        self.assertFalse(result["valid"])
        self.assertEqual(result["interrupted_operations"], 1)
        self.assertEqual(env.process_exit_code, 2)
        env.runner.quit()

    def test_artifacts_upload_even_if_locust_fails(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(os.environ, {"BENCH_RESULTS": directory, "BENCH_GCS_PREFIX": "gs://bucket/run"}), \
                patch("sys.argv", ["artifacts", "--", "locust"]), \
                patch("benchmark.artifacts.subprocess.run", return_value=SimpleNamespace(returncode=1)), \
                patch("benchmark.artifacts.upload") as upload:
            with self.assertRaises(SystemExit) as exit_error:
                artifacts.main()
            self.assertEqual(exit_error.exception.code, 1)
            upload.assert_called_once_with(directory, "gs://bucket/run")

    def test_upload_preserves_paths_under_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "locust.html"
            path.parent.mkdir()
            path.write_text("report")
            with patch("google.cloud.storage.Client") as client:
                artifacts.upload(directory, "gs://bucket/prefix/run")
                bucket = client.return_value.bucket.return_value
                client.return_value.bucket.assert_called_once_with("bucket")
                bucket.blob.assert_called_once_with("prefix/run/nested/locust.html")
                bucket.blob.return_value.upload_from_filename.assert_called_once_with(str(path))

    def test_upload_failure_does_not_report_success(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(os.environ, {"BENCH_RESULTS": directory, "BENCH_GCS_PREFIX": "gs://bucket/run"}), \
                patch("sys.argv", ["artifacts", "--", "locust"]), \
                patch("benchmark.artifacts.subprocess.run", return_value=SimpleNamespace(returncode=0)), \
                patch("benchmark.artifacts.upload", side_effect=RuntimeError("upload failed")):
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                artifacts.main()


if __name__ == "__main__":
    unittest.main()
