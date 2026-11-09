# StreamGuard — Kafka Consumer Lag & Infrastructure Telemetry Exporter

Lightweight **Python** observability exporter & SLA alerting engine designed to run as a Kubernetes sidecar alongside Apache Kafka consumers, Redis caches, and PostgreSQL connection pools.

## Capabilities

- **Zero-Bloat Prometheus Exposition (`stream_guard/metrics.py`):** Renders compliant Prometheus `v0.0.4` gauge metrics without heavy client footprints.
- **Partition-Aggregated Lag Evaluation (`stream_guard/evaluator.py`):** Aggregates offset lag across all partitions per consumer group + topic with `warning` / `critical` thresholds and cooldown deduplication to prevent alert fatigue during Kafka rebalances.
- **Kubernetes Native (`k8s/servicemonitor.yaml`):** Ready for Prometheus Operator scraping via `ServiceMonitor`.

## Running Tests

```bash
python3 -m unittest discover -s tests -v
```
