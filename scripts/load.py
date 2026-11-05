"""Fixed arrival-rate HTTP requests from a Pod through the Kubernetes Service.

One JSON line per scheduled request. A request uses a new TCP connection.
Scheduling delay is recorded separately from HTTP latency. No third-party packages.
"""
import argparse
import concurrent.futures
import json
import math
import threading
import time
import urllib.error
import urllib.request


def run(url, duration, rate, timeout, workers=16):
    lock = threading.Lock()
    slots = threading.BoundedSemaphore(workers)
    start = time.monotonic()

    def emit(row):
        with lock:
            print(json.dumps(row), flush=True)

    def request(seq, scheduled):
        began = time.monotonic()
        row = {"seq": seq, "scheduled_s": scheduled,
               "start_s": began - start, "unix_s": time.time(),
               "schedule_delay_ms": max(0, (began - start - scheduled) * 1000),
               "status": None, "version": None, "pod": None,
               "ok": False, "error": None, "generator_error": False}
        try:
            # Reach the in-cluster Service directly, bypassing environment proxies.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            req = urllib.request.Request(url, headers={"Connection": "close"})
            with opener.open(req, timeout=timeout) as response:
                row["status"] = response.status
                body = json.loads(response.read())
                row["version"], row["pod"] = body.get("version"), body.get("pod")
                row["ok"] = (response.status == 200 and body.get("ready") is True
                             and isinstance(row["version"], str)
                             and isinstance(row["pod"], str))
                if not row["ok"]:
                    row["error"] = "unexpected_response"
        except urllib.error.HTTPError as exc:
            row["status"], row["error"] = exc.code, str(exc)
        except Exception as exc:
            row["error"] = type(exc).__name__ + ": " + str(exc)
        finally:
            row["latency_ms"] = (time.monotonic() - began) * 1000
            row["finish_s"] = time.monotonic() - start
            emit(row)
            slots.release()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for seq in range(math.ceil(duration * rate)):
            scheduled = seq / rate
            time.sleep(max(0, start + scheduled - time.monotonic()))
            if slots.acquire(blocking=False):
                pool.submit(request, seq, scheduled)
            else:
                emit({"seq": seq, "scheduled_s": scheduled,
                      "start_s": time.monotonic() - start,
                      "unix_s": time.time(), "ok": False,
                      "generator_error": True, "error": "generator_saturated",
                      "status": None, "version": None, "pod": None,
                      "latency_ms": None, "schedule_delay_ms": None})
        # Keep the experiment window at the requested duration, even at low rate.
        time.sleep(max(0, start + duration - time.monotonic()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://demo-web:8080/")
    parser.add_argument("--duration", type=float, default=80)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--timeout", type=float, default=1)
    args = parser.parse_args()
    if min(args.duration, args.rate, args.timeout) <= 0:
        parser.error("duration, rate and timeout must be positive")
    run(args.url, args.duration, args.rate, args.timeout)
