import json
import urllib.request
from typing import Any, Dict

from stream_guard.evaluator import Alert


def build_webhook_payload(alert: Alert, service: str = "stream-guard") -> Dict[str, Any]:
    return {
        "service": service,
        "severity": alert.severity,
        "alert_key": alert.key,
        "lag": alert.lag,
        "text": f"[{alert.severity.upper()}] {alert.message}",
    }


def dispatch_webhook(url: str, alert: Alert, timeout_s: float = 5.0) -> int:
    body = json.dumps(build_webhook_payload(alert)).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.status
