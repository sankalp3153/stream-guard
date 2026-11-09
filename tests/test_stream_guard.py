import unittest

from stream_guard.evaluator import LagAlertEvaluator, PartitionOffset
from stream_guard.metrics import MetricSample, render_prometheus


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
        # Total lag = 800 + 500 = 1300 -> critical
        first = ev.evaluate(offsets, now=1000.0)
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].severity, "critical")
        self.assertEqual(first[0].lag, 1300)

        # Within cooldown -> deduplicated
        second = ev.evaluate(offsets, now=1100.0)
        self.assertEqual(len(second), 0)

        # After cooldown -> fires again
        third = ev.evaluate(offsets, now=1350.0)
        self.assertEqual(len(third), 1)


if __name__ == "__main__":
    unittest.main()
