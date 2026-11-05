"""Local HTTP behavior, measurement semantics, and independent saved-data checks.

Run: python3 -B -m unittest discover -s tests -v
Kubernetes is not mocked here; actual controller experiments are in scripts/experiment.py.
"""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


experiment = module("original_experiment", "scripts/experiment.py")
load = module("original_load", "scripts/load.py")
analysis = module("key_analysis", "scripts/analyze_results.py")


@contextlib.contextmanager
def application(version, startup):
    # Use the actual application unchanged. Override only the bind address so tests
    # never occupy its documented port or expose a test server on the network.
    launcher = """
import http.server,runpy,sys
real=http.server.ThreadingHTTPServer
def bind(address,handler):
    server=real(('127.0.0.1',0),handler)
    print('TEST_PORT='+str(server.server_port),flush=True)
    return server
http.server.ThreadingHTTPServer=bind
runpy.run_path(sys.argv[1],run_name='__main__')
"""
    env = dict(os.environ, APP_VERSION=version, POD_NAME="test-pod", STARTUP_SECONDS=str(startup))
    process = subprocess.Popen([sys.executable, "-B", "-c", launcher, str(ROOT / "app/server.py")],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env)
    output = queue.Queue()
    reader = threading.Thread(target=lambda: [output.put(line) for line in process.stdout], daemon=True)
    reader.start()
    try:
        line = output.get(timeout=10)
        if not line.startswith("TEST_PORT="):
            raise RuntimeError(line)
        yield f"http://127.0.0.1:{int(line.split('=')[1])}"
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        reader.join(timeout=2)
        process.stdout.close()


def get(url):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        response = opener.open(url, timeout=2)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return response.status, json.loads(response.read())


class ApplicationTests(unittest.TestCase):
    def test_startup_readiness_and_response_identity(self):
        with application("v1", 1.5) as url:
            status, body = get(url + "/ready")
            self.assertEqual(status, 503)
            self.assertIs(body["ready"], False)
            self.assertEqual(get(url + "/")[0], 503)
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                status, body = get(url + "/ready")
                if status == 200:
                    break
                time.sleep(.05)
            self.assertEqual(status, 200)
            self.assertEqual(body, {"version": "v1", "pod": "test-pod", "ready": True})
            self.assertEqual(get(url + "/")[0], 200)
            self.assertEqual(get(url + "/missing")[0], 404)

    def test_version_label_changes_without_different_api(self):
        with application("v2", 0) as url:
            status, body = get(url + "/")
            self.assertEqual((status, body), (200, {"version": "v2", "pod": "test-pod", "ready": True}))


class TestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        status = 503 if self.path == "/unready" else 200
        payload = b"not-json" if self.path == "/invalid" else json.dumps(
            {"version": "v1", "pod": "fixture", "ready": status == 200}).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class GeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), TestHandler)
        cls.worker = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.worker.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.worker.join()

    def records(self, path):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            load.run(f"http://127.0.0.1:{self.server.server_port}{path}", .12, 25, 1)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertCountEqual([r["seq"] for r in rows], range(3))
        self.assertFalse(any(r["generator_error"] for r in rows))
        return rows

    def test_success_records_include_latency_version_and_pod(self):
        rows = self.records("/")
        self.assertTrue(all(r["ok"] and r["version"] == "v1" and r["pod"] == "fixture" for r in rows))
        self.assertTrue(all(r["latency_ms"] >= 0 and r["schedule_delay_ms"] >= 0 for r in rows))

    def test_http_errors_are_request_failures(self):
        rows = self.records("/unready")
        self.assertTrue(all(not r["ok"] and r["status"] == 503 for r in rows))

    def test_invalid_json_is_not_a_successful_request(self):
        rows = self.records("/invalid")
        self.assertTrue(all(not r["ok"] and r["error"] for r in rows))


class MeasurementTests(unittest.TestCase):
    def test_latency_excludes_failures_and_generator_errors_are_separate(self):
        rows = [{"ok": True, "latency_ms": i, "schedule_delay_ms": 0,
                 "version": "v1", "pod": "p"} for i in range(1, 21)]
        rows += [{"ok": False, "latency_ms": 1000, "schedule_delay_ms": 0},
                 {"ok": False, "generator_error": True}]
        result = experiment.summarize(rows)
        self.assertEqual((result["scheduled_requests"], result["sent_requests"],
                          result["failed_requests"], result["generator_errors"]), (22, 21, 1, 1))
        self.assertEqual(result["successful_latency_p95_ms"], 19)
        self.assertAlmostEqual(result["failure_percent"], 100 / 21)
        self.assertIsNone(experiment.percentile([], .95))

    def test_terminating_pod_is_not_counted_ready(self):
        pod = {"metadata": {}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        self.assertTrue(experiment.ready(pod))
        pod["metadata"]["deletionTimestamp"] = "2026-10-27T00:00:00Z"
        self.assertFalse(experiment.ready(pod))
        pod["metadata"].clear()
        pod["status"]["conditions"][0]["status"] = "False"
        self.assertFalse(experiment.ready(pod))


class RecordedEvidenceTests(unittest.TestCase):
    def test_autoscaling_raw_records_reproduce_observed_scale_up_and_down(self):
        autoscaling = module('autoscaling_analysis', 'experiments/autoscaling/analyze.py')
        manifest = json.loads((ROOT / 'experiments/autoscaling/run-manifest.json').read_text())
        data = autoscaling.analyze(ROOT / manifest['session'])
        self.assertEqual(data['validation_issues'], [])
        self.assertEqual(data, json.loads((ROOT / 'artifacts/autoscaling-results.json').read_text()))
        self.assertEqual(len(data['runs']), 3)
        self.assertEqual((data['hpa_spec']['minReplicas'], data['hpa_spec']['maxReplicas']), (2, 6))
        for run in data['runs']:
            self.assertGreater(run['max_ready'], 2)
            self.assertGreater(run['scale_up_ready_s'], 0)
            self.assertGreater(run['scale_down_to_two_s'], 0)

    def test_raw_records_reproduce_key_results_and_saved_checksums(self):
        data, hashes = analysis.analyze()
        self.assertEqual(data["issues"], [])
        self.assertEqual(data, analysis.read_json(ROOT / "artifacts/key-results.json"))
        self.assertEqual(hashes, analysis.read_json(ROOT / "artifacts/data-checksums.json"))
        self.assertEqual(sum(r["sent"] for r in data["cohorts"]["original"]), 9600)
        self.assertEqual(sum(r["failed"] for r in data["cohorts"]["original"]), 0)
        self.assertEqual(data["pressure_rollout"]["rollout-a"]["failed"], 370)
        self.assertEqual(data["pressure_rollout"]["rollout-b"]["failed"], 2595)
        self.assertEqual(sum(r["sent"] for r in data["cohorts"]["pressure_capacity"] +
                             data["cohorts"]["pressure_rollout"]), 120000)


if __name__ == "__main__":
    unittest.main()
