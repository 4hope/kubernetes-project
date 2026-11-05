"""Read-only live check: two ready replicas and valid responses through the Service."""
import argparse
import collections
import json
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default="kind-selfheal")
    parser.add_argument("--namespace", default="selfheal")
    args = parser.parse_args()
    base = ["kubectl", "--context", args.context, "-n", args.namespace]

    def kubectl(*parts):
        result = subprocess.run(base + list(parts), text=True, capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return result.stdout

    dep = json.loads(kubectl("get", "deployment", "demo-web", "-o", "json"))
    status = dep.get("status", {})
    if dep["spec"]["replicas"] != 2 or status.get("availableReplicas", 0) < 2:
        raise RuntimeError("Expected 2 desired and available replicas; wait for rollout completion")
    pods = json.loads(kubectl("get", "pods", "-l", "app=demo-web", "-o", "json"))["items"]
    ready_names = {p["metadata"]["name"] for p in pods
                   if not p["metadata"].get("deletionTimestamp") and any(
                       c["type"] == "Ready" and c["status"] == "True"
                       for c in p.get("status", {}).get("conditions", []))}
    probe = (
        "import json,urllib.request; "
        "op=urllib.request.build_opener(urllib.request.ProxyHandler({})); "
        "rows=[json.load(op.open('http://demo-web:8080/',timeout=3)) for _ in range(20)]; "
        "print(json.dumps(rows))"
    )
    rows = json.loads(kubectl("exec", "loadgen", "--", "python", "-c", probe))
    if len(ready_names) != 2 or any(r.get("ready") is not True or r.get("version") not in ("v1", "v2")
                                    or r.get("pod") not in ready_names for r in rows):
        raise RuntimeError("Service responses do not match the Ready application Pods")
    print(json.dumps({"status": "PASS", "ready_pods": sorted(ready_names), "requests": len(rows),
                      "versions": dict(collections.Counter(r["version"] for r in rows)),
                      "serving_pods": dict(collections.Counter(r["pod"] for r in rows))}, indent=2))


if __name__ == "__main__":
    main()
