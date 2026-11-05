"""Audit recorded experiments and regenerate key results without a live cluster.

Only cohorts named in experiments/run-manifest.json enter the reported comparison.
No raw records are changed. Third-party packages are needed only for --figures.
"""
import argparse
import csv
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def read_lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def ready(pod):
    return not pod["metadata"].get("deletionTimestamp") and any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in pod.get("status", {}).get("conditions", []))


def nearest_rank(values, fraction=.95):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def audit_run(directory):
    params = read_json(directory / "parameters.json")
    stored = read_json(directory / "summary.json")
    rows = read_lines(directory / "requests.jsonl")
    states = read_lines(directory / "states.jsonl")
    expected = math.ceil(params["duration"] * params["rate"])
    issues = []
    if len(rows) != expected or sorted(r["seq"] for r in rows) != list(range(expected)):
        issues.append("request IDs missing or duplicated")
    sent = [r for r in rows if not r.get("generator_error")]
    good = [r for r in sent if r["ok"]]
    failed = len(sent) - len(good)
    p95 = nearest_rank([r["latency_ms"] for r in good])
    if (len(rows) != stored["scheduled_requests"] or len(sent) != stored["sent_requests"]
            or failed != stored["failed_requests"]):
        issues.append("raw counts disagree with stored summary")
    if p95 != stored["successful_latency_p95_ms"]:
        issues.append("raw latency disagrees with stored summary")
    known_pods = {p["metadata"]["name"] for state in states for p in state["pods"]}
    for r in good:
        if r["status"] != 200 or r["version"] not in ("v1", "v2") or r["pod"] not in known_pods:
            issues.append("successful response has an unexpected status, version or Pod")
            break
    mode = params["mode"]
    initial = read_json(directory / "initial-pods.json")
    final = read_json(directory / "final-pods.json")
    if isinstance(final, dict):
        final = final["items"]
    markers = read_json(directory / "markers.json")
    if mode == "delete":
        old_uids = {p["metadata"]["uid"] for p in initial}
        if not any(ready(p) and p["metadata"]["uid"] not in old_uids for p in final):
            issues.append("no Ready replacement with a new UID")
    if mode.startswith("rollout"):
        active = [p for p in final if not p["metadata"].get("deletionTimestamp")]
        dep_file = directory / "final-deployment.json"
        status = (read_json(dep_file) if dep_file.exists() else states[-1]["deployment"])["status"]
        if (len(active) != 2 or not all(ready(p) for p in active)
                or any(p["spec"]["containers"][0]["image"] != "selfheal-web:v2" for p in active)
                or status.get("availableReplicas") != 2 or status.get("updatedReplicas") != 2):
            issues.append("final update state is incomplete")
        late = [r for r in rows if r["seq"] >= expected - 100 and r.get("ok")]
        if not late or any(r["version"] != "v2" for r in late):
            issues.append("final checked responses do not all use v2")
    completion = stored.get("completion_observed_s")
    if mode == "delete" or mode.startswith("rollout"):
        event = next((m for m in markers if m["event"] == "completion_observed"), None)
        if event is None or completion is None or not math.isclose(event["since_action_s"], completion):
            issues.append("completion marker missing or inconsistent")
    counts = [sum(ready(p) for p in state["pods"]) for state in states]
    if not counts:
        issues.append("state observations missing")
    if len(rows) != len(sent):
        issues.append("generator saturation")
    schedule_p95 = nearest_rank([r["schedule_delay_ms"] for r in sent])
    if schedule_p95 is not None and schedule_p95 > 1000 / params["rate"]:
        issues.append("generator p95 delay exceeds one request interval")
    offsets = [r["unix_s"] - r["start_s"] for r in sent if "unix_s" in r]
    pre = [r for r in sent if r["scheduled_s"] < 14]
    return {
        "run": directory.relative_to(ROOT).as_posix(), "mode": mode,
        "rate": params["rate"], "duration_s": params["duration"],
        "scheduled": len(rows), "sent": len(sent), "failed": failed,
        "generator_errors": len(rows) - len(sent), "p95_ms": p95,
        "completion_s": completion, "min_ready": min(counts) if counts else None,
        "max_ready": max(counts) if counts else None,
        "pre_action_scheduled_before_14s": len(pre),
        "pre_action_failures_before_14s": sum(not r["ok"] for r in pre),
        "wall_clock_offset_span_s": max(offsets) - min(offsets) if offsets else None,
        "issues": issues,
    }


def group(rows):
    groups = {}
    for mode in dict.fromkeys(r["mode"] for r in rows):
        part = [r for r in rows if r["mode"] == mode]
        durations = [r["completion_s"] for r in part if r["completion_s"] is not None]
        sent = sum(r["sent"] for r in part)
        failed = sum(r["failed"] for r in part)
        groups[mode] = {
            "repeats": len(part), "sent": sent, "failed": failed,
            "failed_percent": 100 * failed / sent if sent else None,
            "completion_mean_s": statistics.mean(durations) if durations else None,
            "completion_range_s": [min(durations), max(durations)] if durations else None,
            "mean_run_p95_ms": statistics.mean(r["p95_ms"] for r in part),
            "ready_range": [min(r["min_ready"] for r in part), max(r["max_ready"] for r in part)],
        }
    return groups


def analyze():
    manifest = read_json(ROOT / "experiments/run-manifest.json")
    original_dirs = [ROOT / "results" / n for n in manifest["original"]]
    capacity_dirs = sorted((ROOT / "experiments/pressure/results" / manifest["pressure_capacity_session"]).glob("*/summary.json"))
    rollout_dirs = sorted((ROOT / "experiments/pressure/results" / manifest["pressure_rollout_session"]).glob("*/summary.json"))
    if len(original_dirs) != 12 or len(capacity_dirs) != 6 or len(rollout_dirs) != 6:
        raise ValueError("Expected 12 original, 6 capacity and 6 pressure rollout runs")
    cohorts = {
        "original": [audit_run(p) for p in original_dirs],
        "pressure_capacity": [audit_run(p.parent) for p in capacity_dirs],
        "pressure_rollout": [audit_run(p.parent) for p in rollout_dirs],
        "followup_delete": [audit_run(ROOT / "results" / p) for p in manifest["followup_delete"]],
    }
    # Retain the original pressure client's independent CPU and scheduling diagnostics.
    spec = importlib.util.spec_from_file_location("pressure_analysis", ROOT / "experiments/pressure/analyze.py")
    pressure = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pressure)
    for cohort in ["pressure_capacity", "pressure_rollout"]:
        for run in cohorts[cohort]:
            full = pressure.read_run(ROOT / run["run"])
            run["issues"] += full["issues"]
            run["replicas"] = full["replicas"]
    original_counts = {mode: sum(r["mode"] == mode for r in cohorts["original"])
                       for mode in ["baseline", "delete", "rollout-a", "rollout-b"]}
    rollout_counts = {mode: sum(r["mode"] == mode for r in cohorts["pressure_rollout"])
                      for mode in ["rollout-a", "rollout-b"]}
    capacity_counts = {n: sum(r["replicas"] == n for r in cohorts["pressure_capacity"]) for n in [1, 2]}
    if (set(original_counts.values()) != {3} or set(rollout_counts.values()) != {3}
            or set(capacity_counts.values()) != {3}):
        raise ValueError("Comparison requires 3 repeats of each declared condition")
    original_sources, pressure_sources, resources = set(), set(), set()
    for cohort, part in cohorts.items():
        for run in part:
            directory = ROOT / run["run"]
            params = read_json(directory / "parameters.json")
            dep = read_json(directory / "initial-deployment.json")
            container = dep["spec"]["template"]["spec"]["containers"][0]
            resources.add(json.dumps(container["resources"], sort_keys=True))
            if dep["spec"]["minReadySeconds"] != 2 or container["readinessProbe"]["periodSeconds"] != 1:
                raise ValueError("Readiness/availability settings changed between runs")
            if cohort == "original":
                if (run["rate"], run["duration_s"]) != (10, 80):
                    raise ValueError("Original workload changed")
                original_sources.add(params["runner_source"])
            if cohort.startswith("pressure"):
                duration = 20 if cohort == "pressure_capacity" else 60
                if (run["rate"], run["duration_s"]) != (250, duration):
                    raise ValueError("Pressure workload changed")
                pressure_sources.add(params["client_source"])
    expected_resources = {"requests": {"cpu": "100m", "memory": "64Mi"},
                          "limits": {"cpu": "250m", "memory": "128Mi"}}
    if resources != {json.dumps(expected_resources, sort_keys=True)}:
        raise ValueError("Per-application resource limits changed")
    if len(original_sources) != 1 or len(pressure_sources) != 1:
        raise ValueError("Runner/client source changed within the selected comparison")
    checksums = {}
    for directory in [ROOT / "results", ROOT / "experiments/pressure/results"]:
        for f in sorted(directory.rglob("*")):
            if f.is_file():
                checksums[f.relative_to(ROOT).as_posix()] = hashlib.sha256(f.read_bytes()).hexdigest()
    digest = hashlib.sha256(json.dumps(checksums, sort_keys=True).encode()).hexdigest()
    issues = [{"run": r["run"], "issues": r["issues"]}
              for rows in cohorts.values() for r in rows if r["issues"]]
    return {
        "schema": 1, "cohorts": cohorts, "original": group(cohorts["original"]),
        "pressure_rollout": group(cohorts["pressure_rollout"]),
        "raw_file_count": len(checksums), "raw_sha256": digest, "issues": issues,
        "protocol_checks": {"three_repeats_per_condition": True,
                            "same_workload_within_comparison": True,
                            "same_application_resources": expected_resources,
                            "one_original_runner_source": True, "one_pressure_client_source": True},
        "notes": ["p95 values describe successful requests only; grouped p95 is the mean of run-level p95 values.",
                  "Original wall-clock action_window_45s statistics are excluded because of clock shifts.",
                  "Pressure failure totals cover entire runs, including pre-update errors. None were subtracted.",
                  "Setup failure, pilots and follow-up demos remain separate from the original comparison."],
    }, checksums


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify saved evidence and raw hashes without writing files")
    parser.add_argument("--out", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--figures", action="store_true", help="also regenerate vector figures (requires reportlab)")
    args = parser.parse_args()
    data, hashes = analyze()
    if data["issues"]:
        raise SystemExit(json.dumps(data["issues"], indent=2))
    if args.check:
        published = read_json(args.out / "key-results.json")
        if data != published or hashes != read_json(args.out / "data-checksums.json"):
            raise SystemExit("Saved key results or raw records changed. Re-analyze and review the differences.")
        print(f"PASS: 27 completed runs audited; {data['raw_file_count']} raw files match saved checksums.")
        return
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "key-results.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    (args.out / "data-checksums.json").write_text(json.dumps(hashes, indent=2) + "\n", encoding="utf-8")
    lines = ["# Key results", "", "Regenerated from the fixed cohorts in `experiments/run-manifest.json`.", "",
             "## Original comparison", "", "10 requests/s, 80 s, 800 attempts/run, 3 repeats/condition.", "",
             "| Condition | Failed / sent | Completion mean (range), s | Mean run p95, ms | Observed Ready range |",
             "|---|---:|---:|---:|---:|"]
    for mode, r in data["original"].items():
        completion = (f"{r['completion_mean_s']:.2f} ({r['completion_range_s'][0]:.2f}-{r['completion_range_s'][1]:.2f})"
                      if r["completion_mean_s"] is not None else "n/a")
        lines.append(f"| {mode} | {r['failed']} / {r['sent']} | {completion} | {r['mean_run_p95_ms']:.2f} | {r['ready_range'][0]}-{r['ready_range'][1]} |")
    lines += ["", "## Pressure comparison", "", "250 requests/s, 60 s, 15,000 attempts/run, 3 repeats/strategy.", "",
              "| Strategy | Failed / sent | Failure rate | Mean run p95, ms | Observed Ready range |",
              "|---|---:|---:|---:|---:|"]
    for mode, r in data["pressure_rollout"].items():
        lines.append(f"| {mode} | {r['failed']} / {r['sent']} | {r['failed_percent']:.2f}% | {r['mean_run_p95_ms']:.2f} | {r['ready_range'][0]}-{r['ready_range'][1]} |")
    lines += ["", "### Failures before the update", "", "Client scheduled time below 14 s. Update begins near 15 s.", "",
              "| Run | Failed / attempted before 14 s |", "|---|---:|"]
    for r in data["cohorts"]["pressure_rollout"]:
        lines.append(f"| {Path(r['run']).name} | {r['pre_action_failures_before_14s']} / {r['pre_action_scheduled_before_14s']} |")
    lines += ["", "## Interpretation", ""] + ["- " + note for note in data["notes"]]
    lines += ["- A had fewer recorded pressure failures, but the pre-update errors prevent attributing the entire difference to strategy alone.",
              "- Original deletion mean is time until a replacement is Ready, not client downtime.",
              "- B's original timing advantage is smaller than the roughly 1 s observation interval and does not establish a reliable speed advantage.",
              "- Capacity checks use 20 s / 5,000 attempts per run. One-copy failure rates were 2.72%, 2.48%, 1.58%; two-copy checks had no failures.",
              "", "## Separate records", "", "Three later deletion demos and one loadgen preparation timeout are retained separately. Pilot runs remain under experiments/pressure/results, with their diagnostics in experiments/pressure/analysis. They are not discarded or pooled into the original comparison.", ""]
    (args.out / "key-results.md").write_text("\n".join(lines), encoding="utf-8")
    rows = [dict(cohort=cohort, **r) for cohort, part in data["cohorts"].items() for r in part]
    with (args.out / "runs.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) + ["replicas"])
        writer.writeheader()
        writer.writerows(rows)
    if args.figures:
        from report_figures import write_figures
        write_figures(data, args.out)
    print(f"PASS: wrote audited key results to {args.out}")


if __name__ == "__main__":
    main()
