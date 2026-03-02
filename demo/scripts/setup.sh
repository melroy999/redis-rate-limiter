#!/usr/bin/env bash
# Create a local Kubernetes cluster and deploy the rate limiter demo.
#
# Supported tools: kind, minikube (auto-detected or set via K8S_TOOL).
#
# Prerequisites: docker, kubectl, and either kind or minikube
# Usage: ./demo/scripts/setup.sh

set -euo pipefail

GRAFANA_PORT="${GRAFANA_PORT:-3001}"
CLUSTER_NAME="celery-rate-limiter-demo"
IMAGE_NAME="celery-rate-limiter-demo:latest"
NAMESPACE="celery-rate-limiter-demo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$SCRIPT_DIR/../k8s"
STATE_FILE="$SCRIPT_DIR/.demo-state"

# ------------------------------------------------------------------
# Tool detection
# ------------------------------------------------------------------

if [[ -n "${K8S_TOOL:-}" ]]; then
    case "$K8S_TOOL" in
        kind|minikube) ;;
        *) echo "Error: K8S_TOOL must be 'kind' or 'minikube', got '$K8S_TOOL'." >&2; exit 1 ;;
    esac
elif command -v kind &>/dev/null; then
    K8S_TOOL="kind"
elif command -v minikube &>/dev/null; then
    K8S_TOOL="minikube"
else
    echo "Error: neither 'kind' nor 'minikube' is installed." >&2
    exit 1
fi

echo "Using $K8S_TOOL as Kubernetes provider."

# ------------------------------------------------------------------
# Prerequisite checks
# ------------------------------------------------------------------

for cmd in docker kubectl; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '$cmd' is required but not installed." >&2
        exit 1
    fi
done

# ------------------------------------------------------------------
# Cluster creation
# ------------------------------------------------------------------

if [[ "$K8S_TOOL" == "kind" ]]; then
    echo "Creating kind cluster '$CLUSTER_NAME'..."
    cat <<EOF | kind create cluster --name "$CLUSTER_NAME" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30000
        hostPort: $GRAFANA_PORT
        protocol: TCP
EOF
else
    echo "Creating minikube cluster '$CLUSTER_NAME'..."
    minikube start -p "$CLUSTER_NAME"
fi

# ------------------------------------------------------------------
# Image build and load
# ------------------------------------------------------------------

echo "Building demo image..."
docker build -t "$IMAGE_NAME" -f "$PROJECT_ROOT/demo/Dockerfile" "$PROJECT_ROOT"

echo "Loading image into cluster..."
if [[ "$K8S_TOOL" == "kind" ]]; then
    kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"
else
    minikube -p "$CLUSTER_NAME" image load "$IMAGE_NAME"
fi

# ------------------------------------------------------------------
# Deploy manifests
# ------------------------------------------------------------------

echo "Applying Kubernetes manifests..."
kubectl apply -f "$K8S_DIR/namespace.yaml"
kubectl apply -f "$K8S_DIR/prometheus-rbac.yaml"
kubectl apply -f "$K8S_DIR/redis.yaml"
kubectl apply -f "$K8S_DIR/prometheus-config.yaml"
kubectl apply -f "$K8S_DIR/prometheus.yaml"
kubectl apply -f "$K8S_DIR/grafana-datasource.yaml"
kubectl apply -f "$K8S_DIR/grafana-dashboard-provider.yaml"
kubectl apply -f "$K8S_DIR/grafana-dashboard.yaml"
kubectl apply -f "$K8S_DIR/grafana.yaml"
kubectl apply -f "$K8S_DIR/generator.yaml"
kubectl apply -f "$K8S_DIR/workers.yaml"

# ------------------------------------------------------------------
# Wait for readiness
# ------------------------------------------------------------------

echo "Waiting for pods to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=ready pod --all --timeout=300s

# ------------------------------------------------------------------
# Port forwarding (minikube only; kind uses extraPortMappings)
# ------------------------------------------------------------------

PORT_FORWARD_PID=""
if [[ "$K8S_TOOL" == "minikube" ]]; then
    echo "Starting port-forward for Grafana..."
    nohup kubectl -n "$NAMESPACE" port-forward svc/grafana "$GRAFANA_PORT:3000" \
        >"$SCRIPT_DIR/.port-forward.log" 2>&1 &
    PORT_FORWARD_PID=$!
fi

# Save state for teardown.
cat > "$STATE_FILE" <<EOF
K8S_TOOL=$K8S_TOOL
CLUSTER_NAME=$CLUSTER_NAME
PORT_FORWARD_PID=${PORT_FORWARD_PID:-}
EOF

echo ""
echo "============================================"
echo "  Demo is running! (via $K8S_TOOL)"
echo "  Grafana: http://localhost:$GRAFANA_PORT"
echo "============================================"
echo ""
echo "To tear down: ./demo/scripts/teardown.sh"
