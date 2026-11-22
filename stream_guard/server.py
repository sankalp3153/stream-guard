import json
import os
import signal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, List

from stream_guard.metrics import MetricSample, render_prometheus


class MetricsRegistry:
    def __init__(self) -> None:
        self._collectors: List[Callable[[], List[MetricSample]]] = []

    def register(self, collector: Callable[[], List[MetricSample]]) -> None:
        self._collectors.append(collector)

    def render(self) -> str:
        samples: List[MetricSample] = []
        for fn in self._collectors:
            samples.extend(fn())
        return render_prometheus(samples)


def create_server(host: str, port: int, registry: MetricsRegistry) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                body = json.dumps({"status": "ok", "service": "stream-guard"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/metrics":
                body = registry.render().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            return

    return ThreadingHTTPServer((host, port), Handler)


def main() -> None:
    host = os.getenv("EXPORTER_HOST", "0.0.0.0")
    port = int(os.getenv("EXPORTER_PORT", "9108"))
    registry = MetricsRegistry()
    registry.register(
        lambda: [MetricSample(name="streamguard_up", value=1.0, help_text="Exporter liveness")]
    )
    server = create_server(host, port, registry)

    def _handle_sig(_signum, _frame):
        server.shutdown()

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)
    server.serve_forever()


if __name__ == "__main__":
    main()
