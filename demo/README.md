# Distributed Rate Limiter Demo

This directory contains a self-contained demonstration of the distributed rate limiter running across multiple worker pods, with live Prometheus metrics and a pre-built Grafana dashboard.

## Architecture

The demo deploys the following components:

- **Redis**: shared backend for all rate limiter state (sliding window counters, task buffer, concurrency tracking).
- **Traffic generator** (1 pod): schedules tasks at a sine-wave rate that periodically exceeds the configured limit, creating visible backpressure.
- **Workers** (3 pods): each runs an independent `ThreadPoolRateLimiter` with its own drain loop, consuming tasks from the shared Redis buffer. The Lua-based consume script enforces the global rate limit atomically.
- **Prometheus**: scrapes `/metrics` from all pods every second.
- **Grafana**: pre-provisioned with a dashboard that visualises offered vs consumed rate, buffer depth, per-worker load distribution, and consume outcomes.

## Quick start: Docker Compose

Requires Docker and Docker Compose.

```bash
cd demo
docker compose up --build
```

Open [http://localhost:3001](http://localhost:3001) (or the port set via `GRAFANA_PORT`) for the Grafana dashboard. Allow approximately 60 seconds for the sine wave to become visible.

Press `Ctrl+C` to stop.

## Quick start: Kubernetes (kind or minikube)

Requires Docker, kubectl, and either [kind](https://kind.sigs.k8s.io/) or [minikube](https://minikube.sigs.k8s.io/). The setup script auto-detects which tool is available (preferring kind). Set `K8S_TOOL` to override:

```bash
# Auto-detect.
./demo/scripts/setup.sh

# Force minikube.
K8S_TOOL=minikube ./demo/scripts/setup.sh
```

The script creates a cluster, builds the demo image, loads it into the cluster, deploys all manifests, and waits for pods to become ready. Once complete, the Grafana dashboard is available at [http://localhost:3001](http://localhost:3001) (or the port set via `GRAFANA_PORT`).

To tear down:

```bash
./demo/scripts/teardown.sh
```

## Dashboard panels

| Panel | Description |
|-------|-------------|
| **Offered vs Consumed Rate** | Sine wave (offered, dashed blue) vs actual throughput (consumed, solid green), with a horizontal red threshold at the configured rate limit. When offered exceeds the limit, consumed flatlines at the cap. |
| **Buffer Depth** | Number of tasks queued per pod. Grows when offered rate exceeds the limit; drains when it drops below. |
| **Per-Worker Consumed Rate** | Consumed tasks per second broken down by worker pod, showing how the shared rate limit is distributed across the cluster. |
| **Remaining Rate Limit Tokens** | Tokens available in the current sliding window. Fills back up during low-traffic troughs; depletes when throughput approaches the limit. |
| **Consume Outcomes** | Stacked bar chart of success (dispatched), rejected (rate limited), and expired (exceeded max_age) events. |

## Configuration

All parameters are configurable via environment variables. The defaults produce a visually clear demonstration with a 25 tasks/sec effective rate limit (250 tokens per 10-second window) and a 120-second sine-wave period whose average rate is below the limit, allowing the buffer to fully drain between peaks.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `redis` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `LIMITER_ID` | `k8s-demo` | Shared limiter identifier |
| `LIMIT` | `250` | Maximum tokens per window |
| `WINDOW` | `10.0` | Sliding window duration in seconds |
| `MAX_CONCURRENCY` | `9` | Maximum concurrent task executions (global across all workers) |
| `TRAFFIC_GENERATOR` | `false` | Enable sine-wave traffic generation |
| `SINE_PERIOD` | `120` | Full sine-wave cycle in seconds |
| `SINE_CENTER` | `0.75` | Center of the sine wave as a fraction of the effective rate (0.75 means the average offered rate is 75% of the limit) |
| `SINE_AMPLITUDE` | `0.65` | Amplitude as a fraction of the effective rate (with center=0.75, rate varies from 0.1x to 1.4x the limit) |
| `K8S_TOOL` | *(auto-detected)* | Kubernetes tool: `kind` or `minikube` |
| `GRAFANA_PORT` | `3001` | Host port for the Grafana dashboard |
| `METRICS_PORT` | `8000` | Prometheus HTTP metrics port |
| `TASK_DURATION` | `0.3` | Simulated task execution time in seconds |
