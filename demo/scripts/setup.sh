#!/usr/bin/env bash
# Create a kind cluster and deploy the rate limiter demo.
#
# Prerequisites: docker, kind, kubectl
# Usage: ./demo/scripts/setup.sh

set -euo pipefail

CLUSTER_NAME="celery-rate-limiter-demo"
IMAGE_NAME="celery-rate-limiter-demo:latest"
NAMESPACE="celery-rate-limiter-demo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$SCRIPT_DIR/../k8s"

# ------------------------------------------------------------------
# Prerequisite checks
# ------------------------------------------------------------------

for cmd in docker kind kubectl; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '$cmd' is required but not installed." >&2
        exit 1
    fi
done

# ------------------------------------------------------------------
# Cluster creation
# ------------------------------------------------------------------

echo "Creating kind cluster '$CLUSTER_NAME'..."
cat <<EOF | kind create cluster --name "$CLUSTER_NAME" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30000
        hostPort: 3000
        protocol: TCP
EOF

# ------------------------------------------------------------------
# Image build and load
# ------------------------------------------------------------------

echo "Building demo image..."
docker build -t "$IMAGE_NAME" -f "$PROJECT_ROOT/demo/Dockerfile" "$PROJECT_ROOT"

echo "Loading image into kind cluster..."
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

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
kubectl -n "$NAMESPACE" wait --for=condition=ready pod --all --timeout=120s

echo ""
echo "============================================"
echo "  Demo is running!"
echo "  Grafana: http://localhost:3000"
echo "============================================"
echo ""
echo "To tear down: ./demo/scripts/teardown.sh"
