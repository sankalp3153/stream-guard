"""
SLA Threshold & Lag Anomaly Evaluator with cooldown deduplication to prevent
alert storms during transient broker rebalances.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class PartitionOffset:
    topic: str
    partition: int
    group_id: str
    current_offset: int
    log_end_offset: int

    @property
    def lag(self) -> int:
        return max(0, self.log_end_offset - self.current_offset)


@dataclass(frozen=True)
class Alert:
    severity: str  # "warning" | "critical"
    key: str
    message: str
    lag: int


class LagAlertEvaluator:
    def __init__(
        self,
        warn_threshold: int = 1000,
        critical_threshold: int = 10000,
        cooldown_seconds: float = 300.0,
    ) -> None:
        self.warn_threshold = warn_threshold
        self.critical_threshold = critical_threshold
        self.cooldown_seconds = cooldown_seconds
        self._last_fired: Dict[str, float] = {}

    def evaluate(self, offsets: List[PartitionOffset], now: float) -> List[Alert]:
        grouped: Dict[Tuple[str, str], int] = {}
        for item in offsets:
            k = (item.group_id, item.topic)
            grouped[k] = grouped.get(k, 0) + item.lag

        alerts: List[Alert] = []
        for (group_id, topic), total_lag in grouped.items():
            if total_lag >= self.critical_threshold:
                severity = "critical"
            elif total_lag >= self.warn_threshold:
                severity = "warning"
            else:
                continue

            alert_key = f"{group_id}:{topic}:{severity}"
            last = self._last_fired.get(alert_key)
            if last is not None and (now - last) < self.cooldown_seconds:
                continue

            self._last_fired[alert_key] = now
            alerts.append(
                Alert(
                    severity=severity,
                    key=alert_key,
                    message=f"Consumer group '{group_id}' on topic '{topic}' lag={total_lag}",
                    lag=total_lag,
                )
            )

        return alerts
