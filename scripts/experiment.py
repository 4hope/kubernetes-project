"""Run on WSL; uses explicit kubectl context and namespace on every API call.

Experiments: baseline, delete, rollout-a (unavailable=0/surge=1),
rollout-b (unavailable=1/surge=0). Each starts from two stable v1 Pods.
Samples are polling observations, not a lossless watch. Recovery/completion
times end at first successful observation, so include polling/API delay.
"""
import argparse
import collections
import datetime
import json
import math
from pathlib import Path
import subprocess
import time


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def ready(pod):
    return (not pod["metadata"].get("deletionTimestamp") and
            any(c["type"] == "Ready" and c["status"] == "True"
                for c in pod.get("status", {}).get("conditions", [])))


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def summarize(rows):
    sent = [r for r in rows if not r.get("generator_error")]
    good = [r for r in sent if r["ok"]]
    return {
        "scheduled_requests": len(rows), "sent_requests": len(sent),
        "generator_errors": len(rows) - len(sent),
        "failed_requests": len(sent) - len(good),
        "failure_percent": 100 * (len(sent) - len(good)) / len(sent) if sent else None,
        "successful_latency_p50_ms": percentile([r["latency_ms"] for r in good], .5),
        "successful_latency_p95_ms": percentile([r["latency_ms"] for r in good], .95),
        "max_schedule_delay_ms": max((r["schedule_delay_ms"] for r in sent), default=None),
        "schedule_delay_p95_ms": percentile([r["schedule_delay_ms"] for r in sent], .95),
        "versions": dict(collections.Counter(r["version"] for r in good)),
        "serving_pods": dict(collections.Counter(r["pod"] for r in good)),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["baseline", "delete", "rollout-a", "rollout-b"])
    parser.add_argument("--duration", type=float, default=80)
    parser.add_argument("--action-at", type=float, default=15)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--sample-period", type=float, default=1)
    parser.add_argument("--context", default="kind-selfheal")
    parser.add_argument("--namespace", default="selfheal")
    args = parser.parse_args()
    if not (0 < args.action_at < args.duration - 5 and args.rate > 0 and args.sample_period > 0):
        parser.error("require 0 < action-at < duration-5, and positive rate/sample-period")
    base = ["kubectl", "--context", args.context, "-n", args.namespace]

    def k(*arguments):
        result = subprocess.run(base + list(arguments), text=True, capture_output=True, timeout=240)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return result.stdout

    def obj(*arguments):
        return json.loads(k(*arguments, "-o", "json"))

    def pods():
        return obj("get", "pods", "-l", "app=demo-web")["items"]

    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2), encoding="utf-8")

    # Avoid overwriting previous data, including interrupted/failed experiments.
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    out = Path("results") / (stamp + "_" + args.mode)
    out.mkdir(parents=True, exist_ok=False)
    print("Results:", out, flush=True)
    save("parameters.json", {**vars(args), "created_utc": utc(), "request_timeout_s": 1,
                              "workers": 16, "runner_source": Path(__file__).read_text(encoding="utf-8")})
    workload = None
    try:
        obj("get", "pod", "loadgen")
        k("wait", "--for=condition=Ready", "pod/loadgen", "--timeout=60s")
        print("Preparing two stable v1 Pods...", flush=True)
        unavailable, surge = (1, 0) if args.mode == "rollout-b" else (0, 1)
        patch = {"spec": {"replicas": 2,
                 "strategy": {"type": "RollingUpdate", "rollingUpdate": {
                     "maxUnavailable": unavailable, "maxSurge": surge}},
                 "template": {"spec": {"containers": [{"name": "web", "image": "selfheal-web:v1"}]}}}}
        k("patch", "deployment", "demo-web", "--type=strategic", "-p", json.dumps(patch))
        k("rollout", "status", "deployment/demo-web", "--timeout=180s")
        deadline = time.monotonic() + 180
        while True:
            initial = pods()
            if (len(initial) == 2 and all(ready(p) for p in initial) and
                all(p["spec"]["containers"][0]["image"] == "selfheal-web:v1" for p in initial)):
                break
            if time.monotonic() > deadline:
                raise RuntimeError("Two stable v1 Pods did not appear")
            time.sleep(1)
        # Check the same in-cluster Service path used for the actual workload.
        k("exec", "loadgen", "--", "python", "-c",
          "import json,urllib.request; "
          "d=json.load(urllib.request.urlopen('http://demo-web:8080/',timeout=3)); "
          "assert d['version']=='v1' and d['ready'] is True, d")
        save("initial-pods.json", initial)
        save("initial-deployment.json", obj("get", "deployment", "demo-web"))
        save("service.json", obj("get", "service", "demo-web"))
        save("loadgen-pod.json", obj("get", "pod", "loadgen"))
        save("load-script-configmap.json", obj("get", "configmap", "measurement-scripts"))
        save("nodes.json", obj("get", "nodes"))
        save("versions.json", json.loads(k("version", "-o", "json")))
        initial_uids = {p["metadata"]["uid"] for p in initial}
        victim = initial[0]["metadata"]["name"]
        markers = []
        completion = None
        action_start = None
        action_utc = None
        peak_gap = 0
        previous_sample = None
        with (out / "requests.jsonl").open("w", encoding="utf-8") as requests, \
             (out / "client-stderr.txt").open("w", encoding="utf-8") as stderr, \
             (out / "states.jsonl").open("w", encoding="utf-8") as states:
            workload = subprocess.Popen(base + ["exec", "loadgen", "--", "python", "/scripts/load.py",
                                      "--duration", str(args.duration), "--rate", str(args.rate)],
                                      stdout=requests, stderr=stderr, text=True)
            started = time.monotonic()
            print(f"Load: {args.rate:g} requests/s, {args.duration:g} s; mode={args.mode}", flush=True)
            while True:
                loop_start = time.monotonic()
                elapsed = loop_start - started
                if previous_sample is not None:
                    peak_gap = max(peak_gap, loop_start - previous_sample)
                previous_sample = loop_start
                if action_start is None and elapsed >= args.action_at:
                    action_start, action_utc = time.monotonic(), utc()
                    markers.append({"event": "action_started", "mode": args.mode,
                                    "utc": action_utc, "elapsed_s": action_start - started,
                                    "victim": victim if args.mode == "delete" else None})
                    save("markers.json", markers)
                    print("Action:", args.mode, flush=True)
                    if args.mode == "delete":
                        k("delete", "pod", victim, "--wait=false")
                    elif args.mode.startswith("rollout"):
                        k("set", "image", "deployment/demo-web", "web=selfheal-web:v2")
                    markers.append({"event": "action_acknowledged", "utc": utc(),
                                    "elapsed_s": time.monotonic() - started})
                    save("markers.json", markers)
                current = pods()
                deployment = obj("get", "deployment", "demo-web")
                endpoints = obj("get", "endpointslices", "-l", "kubernetes.io/service-name=demo-web")
                states.write(json.dumps({"utc": utc(), "elapsed_s": time.monotonic() - started,
                                         "pods": current, "deployment": deployment,
                                         "endpointslices": endpoints}) + "\n")
                states.flush()
                if action_start is not None and completion is None:
                    completed = False
                    if args.mode == "delete":
                        completed = any(ready(p) and p["metadata"]["uid"] not in initial_uids for p in current)
                    elif args.mode.startswith("rollout"):
                        active = [p for p in current if not p["metadata"].get("deletionTimestamp")]
                        status = deployment.get("status", {})
                        completed = (len(active) == 2 and all(ready(p) for p in active)
                            and all(p["spec"]["containers"][0]["image"] == "selfheal-web:v2" for p in active)
                            and status.get("observedGeneration", 0) >= deployment["metadata"]["generation"]
                            and status.get("updatedReplicas", 0) == 2 and status.get("availableReplicas", 0) == 2)
                    if completed:
                        completion = time.monotonic() - action_start
                        markers.append({"event": "completion_observed", "utc": utc(),
                                        "elapsed_s": time.monotonic() - started,
                                        "since_action_s": completion})
                        save("markers.json", markers)
                        print(f"Completion observed after {completion:.2f} s", flush=True)
                if workload.poll() is not None:
                    break
                if time.monotonic() - started > args.duration + 30:
                    raise RuntimeError("Workload exceeded experiment deadline")
                time.sleep(max(0, args.sample_period - (time.monotonic() - loop_start)))
            if workload.returncode:
                raise RuntimeError("Load generator failed; inspect client-stderr.txt")
        rows = [json.loads(line) for line in (out / "requests.jsonl").read_text().splitlines() if line.strip()]
        summary = summarize(rows)
        # Fixed wall-clock window around the action, separate from full-run metrics.
        action_epoch = datetime.datetime.fromisoformat(action_utc).timestamp()
        window = [r for r in rows if action_epoch <= r["unix_s"] < action_epoch + 45]
        summary.update({"mode": args.mode, "completion_observed_s": completion,
                        "completion_definition": ("replacement Ready" if args.mode == "delete" else
                            "none (baseline)" if args.mode == "baseline" else "v2 updated and available"),
                        "max_sample_gap_s": peak_gap, "action_window_45s": summarize(window),
                        "action_window_fully_scheduled": max((r["unix_s"] for r in rows), default=0) >= action_epoch + 45})
        problems = []
        if len(rows) != math.ceil(args.duration * args.rate):
            problems.append("missing request records")
        if summary["generator_errors"]:
            problems.append("load generator saturation")
        if (summary["schedule_delay_p95_ms"] is not None and
                summary["schedule_delay_p95_ms"] > 1000 / args.rate):
            problems.append("p95 request scheduling delay exceeds one arrival interval")
        if args.mode != "baseline" and completion is None:
            problems.append("completion not observed inside the measurement window")
        if args.mode == "baseline" and summary["failed_requests"]:
            problems.append("baseline requests failed")
        if args.mode.startswith("rollout") and summary["versions"].get("v2", 0) == 0:
            problems.append("no successful v2 response")
        summary["validation_issues"] = problems
        save("summary.json", summary)
        print(json.dumps(summary, indent=2), flush=True)
        if problems:
            raise RuntimeError("Validation issues: " + "; ".join(problems))
    except Exception as exc:
        save("run-error.json", {"utc": utc(), "error": str(exc)})
        raise
    finally:
        if workload is not None and workload.poll() is None:
            workload.terminate()
            workload.wait(timeout=10)
        for filename, arguments in [
            ("events.json", ("get", "events")),
            ("final-pods.json", ("get", "pods", "-l", "app=demo-web")),
            ("final-deployment.json", ("get", "deployment", "demo-web")),
        ]:
            try:
                save(filename, obj(*arguments))
            except Exception as exc:
                save(filename + ".error", {"error": str(exc)})


if __name__ == "__main__":
    main()
