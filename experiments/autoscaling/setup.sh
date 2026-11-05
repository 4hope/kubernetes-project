#!/usr/bin/env bash
# Optional local-kind extension. Run the root setup with --pressure first.
set -euo pipefail
cd "$(dirname "$0")/../.."
kubectl --context kind-selfheal get node selfheal-control-plane >/dev/null
echo 'Installing pinned Metrics Server v0.9.1 into the local kind cluster...'
metrics_manifest=$(mktemp -t selfheal-metrics-server.XXXXXX.yaml)
trap 'rm -f -- "$metrics_manifest"' EXIT
curl -fsSL --retry 2 --max-time 60 \
  https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.1/components.yaml \
  -o "$metrics_manifest"
printf '%s  %s\n' '3c7f57cdbee0f57fb0259a150e8f6d6cb4f29d72d1aa5dce8803151dd6d4c2a1' "$metrics_manifest" | sha256sum -c -
kubectl --context kind-selfheal apply -f "$metrics_manifest"
# kind's local kubelet certificate is not trusted by Metrics Server.
# This disables kubelet certificate verification only for this local experiment.
kubectl --context kind-selfheal -n kube-system patch deployment metrics-server \
  --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl --context kind-selfheal -n kube-system rollout status deployment/metrics-server --timeout=180s
kubectl --context kind-selfheal wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=90s
kubectl --context kind-selfheal -n selfheal get pod loadgen-pressure >/dev/null
kubectl --context kind-selfheal apply -f experiments/autoscaling/app.yaml
kubectl --context kind-selfheal -n selfheal rollout status deployment/demo-autoscale --timeout=180s
kubectl --context kind-selfheal -n selfheal get hpa demo-autoscale
echo 'Wait for metric samples to appear; run experiments/autoscaling/run.py next.'
