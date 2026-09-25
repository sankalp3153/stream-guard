"""
Prometheus text exposition renderer (v0.0.4) with zero external dependency overhead
for lightweight Kubernetes sidecar deployments.
"""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class MetricSample:
    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    help_text: str = ""
    metric_type: str = "gauge"


def render_prometheus(samples: List[MetricSample]) -> str:
    lines: List[str] = []
    seen_headers: set[str] = set()

    for s in samples:
        if s.name not in seen_headers:
            seen_headers.add(s.name)
            if s.help_text:
                lines.append(f"# HELP {s.name} {s.help_text}")
            lines.append(f"# TYPE {s.name} {s.metric_type}")

        if s.labels:
            formatted_labels = ",".join(
                f'{k}="{str(v).replace(chr(34), chr(92) + chr(34))}"'
                for k, v in sorted(s.labels.items())
            )
            lines.append(f"{s.name}{{{formatted_labels}}} {s.value}")
        else:
            lines.append(f"{s.name} {s.value}")

    return "\n".join(lines) + "\n"
