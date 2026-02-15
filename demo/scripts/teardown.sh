#!/usr/bin/env bash
# Delete the kind cluster created by setup.sh.
#
# Usage: ./demo/scripts/teardown.sh

set -euo pipefail

CLUSTER_NAME="celery-rate-limiter-demo"

echo "Deleting kind cluster '$CLUSTER_NAME'..."
kind delete cluster --name "$CLUSTER_NAME"
echo "Done."
