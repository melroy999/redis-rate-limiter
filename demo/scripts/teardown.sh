#!/usr/bin/env bash
# Delete the cluster created by setup.sh.
#
# Usage: ./demo/scripts/teardown.sh

set -euo pipefail

CLUSTER_NAME="celery-rate-limiter-demo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="$SCRIPT_DIR/.demo-state"

# ------------------------------------------------------------------
# Determine which tool was used
# ------------------------------------------------------------------

if [[ -f "$STATE_FILE" ]]; then
    source "$STATE_FILE"
elif [[ -n "${K8S_TOOL:-}" ]]; then
    : # use the env var provided by the caller
elif command -v kind &>/dev/null && kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    K8S_TOOL="kind"
elif command -v minikube &>/dev/null && minikube profile list -o json 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    K8S_TOOL="minikube"
else
    echo "Error: could not determine which tool was used. Set K8S_TOOL=kind or K8S_TOOL=minikube." >&2
    exit 1
fi

# ------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------

# Stop port-forward if it is still running.
if [[ -n "${PORT_FORWARD_PID:-}" ]] && kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
    echo "Stopping port-forward (PID $PORT_FORWARD_PID)..."
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
fi

echo "Deleting $K8S_TOOL cluster '$CLUSTER_NAME'..."
if [[ "$K8S_TOOL" == "kind" ]]; then
    kind delete cluster --name "$CLUSTER_NAME"
else
    minikube delete -p "$CLUSTER_NAME"
fi

rm -f "$STATE_FILE" "$SCRIPT_DIR/.port-forward.log"
echo "Done."
