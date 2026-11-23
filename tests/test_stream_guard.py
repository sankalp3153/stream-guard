import json
import threading
import unittest
import urllib.request

from stream_guard.evaluator import LagAlertEvaluator, PartitionOffset
from stream_guard.metrics import MetricSample, render_prometheus
from stream_guard.notifier import build_webhook_payload
from stream_guard.probes import PostgresPoolSnapshot, RedisSnapshot, collect_infra_metrics
from stream_guard.server import MetricsRegistry, create_server


class TestStreamGuard(unittest.TestCase):
    def test_prometheus_exposition_output(self):
        output = render_prometheus(
            [
                MetricSample(
                    name="kafka_consumer_group_lag",
                    value=420,
                    labels={"group": "rcs-workers", "topic": "messages.outbound"},
                    help_text="Total consumer lag per group and topic",
                )
            ]
        )
        self.assertIn("# HELP kafka_consumer_group_lag", output)
        self.assertIn('kafka_consumer_group_lag{group="rcs-workers",topic="messages.outbound"} 420', output)

    def test_lag_evaluator_thresholds_and_cooldown(self):
        ev = LagAlertEvaluator(warn_threshold=100, critical_threshold=1000, cooldown_seconds=300)
        offsets = [
            PartitionOffset("messages.outbound", 0, "dispatchers", 1000, 1800),
            PartitionOffset("messages.outbound", 1, "dispatchers", 2000, 2500),
        ]
        first = ev.evaluate(offsets, now=1000.0)
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].severity, "critical")
        self.assertEqual(first[0].lag, 1300)

        payload = build_webhook_payload(first[0])
        self.assertEqual(payload["severity"], "critical")
        self.assertIn("lag=1300", payload["text"])

        # Deduplicated within cooldown window
        self.assertEqual(len(ev.evaluate(offsets, now=1100.0)), 0)
        # Fires again once cooldown expires
        self.assertEqual(len(ev.evaluate(offsets, now=1350.0)), 1)

    def test_infra_probes_and_http_metrics_server(self):
        registry = MetricsRegistry()
        registry.register(
            lambda: collect_infra_metrics(
                redis_nodes=[RedisSnapshot("redis-primary", 400_000_000, 1_000_000_000, 42)],
                pg_pools=[PostgresPoolSnapshot("ledger_core", 18, 12, 50)],
            )
        )
        server = create_server("127.0.0.1", 0, registry)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz") as resp:
                self.assertEqual(resp.status, 200)
                self.assertEqual(json.loads(resp.read().decode())["status"], "ok")

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics") as resp:
                self.assertEqual(resp.status, 200)
                body = resp.read().decode()
                self.assertIn('streamguard_redis_memory_utilization_ratio{instance="redis-primary"} 0.4', body)
                self.assertIn('streamguard_pg_pool_saturation_ratio{database="ledger_core"} 0.36', body)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
