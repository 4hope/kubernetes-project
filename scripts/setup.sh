#!/usr/bin/env bash
# Run from Ubuntu/WSL. Changes only the selfheal kind cluster and namespace.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" == "--help" ]]; then
  echo 'Usage: bash scripts/setup.sh [--pressure]'
  echo 'Prepare local images, two application Pods and a fresh request-generator Pod.'
  exit 0
fi
if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--pressure" ) ]]; then
  echo 'Expected no arguments or --pressure' >&2
  exit 2
fi
for command in docker kind kubectl python3; do
  command -v "$command" >/dev/null || { echo "Missing dependency: $command" >&2; exit 1; }
done
docker info >/dev/null
if ! kind get clusters | grep -qx selfheal; then
  kind create cluster --name selfheal --image kindest/node:v1.37.0
fi
kind export kubeconfig --name selfheal
kubectl --context kind-selfheal wait --for=condition=Ready nodes --all --timeout=180s
docker build --build-arg APP_VERSION=v1 -t selfheal-web:v1 ./app
docker build --build-arg APP_VERSION=v2 -t selfheal-web:v2 ./app
kind load docker-image selfheal-web:v1 selfheal-web:v2 --name selfheal
kubectl --context kind-selfheal create namespace selfheal --dry-run=client -o yaml |
  kubectl --context kind-selfheal apply -f -
kubectl --context kind-selfheal -n selfheal create configmap measurement-scripts \
  --from-file=load.py=scripts/load.py --dry-run=client -o yaml |
  kubectl --context kind-selfheal -n selfheal apply -f -
kubectl --context kind-selfheal -n selfheal apply -f k8s/app.yaml
# restartPolicy: Never and sleep(86400) make old loadgen Pods expire after one day.
kubectl --context kind-selfheal -n selfheal delete pod loadgen --ignore-not-found=true --wait=true --timeout=60s
kubectl --context kind-selfheal -n selfheal apply -f k8s/loadgen.yaml
kubectl --context kind-selfheal -n selfheal rollout status deployment/demo-web --timeout=180s
kubectl --context kind-selfheal -n selfheal wait --for=condition=Ready pod/loadgen --timeout=90s
if [[ "${1:-}" == "--pressure" ]]; then
  kubectl --context kind-selfheal -n selfheal create configmap pressure-scripts \
    --from-file=load.py=experiments/pressure/load.py --dry-run=client -o yaml |
    kubectl --context kind-selfheal -n selfheal apply -f -
  kubectl --context kind-selfheal -n selfheal delete pod loadgen-pressure --ignore-not-found=true --wait=true --timeout=60s
  kubectl --context kind-selfheal -n selfheal apply -f experiments/pressure/loadgen.yaml
  kubectl --context kind-selfheal -n selfheal wait --for=condition=Ready pod/loadgen-pressure --timeout=90s
fi
python3 -B scripts/smoke.py
