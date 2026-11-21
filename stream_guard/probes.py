from dataclasses import dataclass
from typing import List

from stream_guard.metrics import MetricSample


@dataclass(frozen=True)
class RedisSnapshot:
    instance: str
    used_memory_bytes: int
    max_memory_bytes: int
    connected_clients: int

    @property
    def utilization_ratio(self) -> float:
        if self.max_memory_bytes <= 0:
            return 0.0
        return round(self.used_memory_bytes / self.max_memory_bytes, 4)


@dataclass(frozen=True)
class PostgresPoolSnapshot:
    database: str
    active_connections: int
    idle_connections: int
    max_connections: int

    @property
    def saturation_ratio(self) -> float:
        if self.max_connections <= 0:
            return 0.0
        return round(self.active_connections / self.max_connections, 4)


def collect_infra_metrics(
    redis_nodes: List[RedisSnapshot],
    pg_pools: List[PostgresPoolSnapshot],
) -> List[MetricSample]:
    samples: List[MetricSample] = []

    for r in redis_nodes:
        samples.append(
            MetricSample(
                name="streamguard_redis_memory_utilization_ratio",
                value=r.utilization_ratio,
                labels={"instance": r.instance},
                help_text="Redis memory usage as fraction of maxmemory",
            )
        )
        samples.append(
            MetricSample(
                name="streamguard_redis_connected_clients",
                value=float(r.connected_clients),
                labels={"instance": r.instance},
                help_text="Active client connections to Redis instance",
            )
        )

    for pg in pg_pools:
        samples.append(
            MetricSample(
                name="streamguard_pg_pool_saturation_ratio",
                value=pg.saturation_ratio,
                labels={"database": pg.database},
                help_text="Active PostgreSQL connections relative to max_connections",
            )
        )
        samples.append(
            MetricSample(
                name="streamguard_pg_pool_idle_connections",
                value=float(pg.idle_connections),
                labels={"database": pg.database},
                help_text="Idle connections in PostgreSQL pool",
            )
        )

    return samples
