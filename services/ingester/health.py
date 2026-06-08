"""Minimal HTTP server for liveness/readiness probes and Prometheus metrics.

Three endpoints, all on METRICS_PORT (default 9100):

  GET /health   — liveness: always 200 if the process is alive
  GET /ready    — readiness: 200 if all pollers had a successful poll in the
                  last 5 minutes, 503 otherwise
  GET /metrics  — Prometheus text format scraped by Prometheus/Grafana
"""

from __future__ import annotations

import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

if TYPE_CHECKING:
    from services.ingester.feed import FeedPoller

logger = logging.getLogger(__name__)


def make_server(
    port: int, pollers: list[FeedPoller]
) -> ThreadingHTTPServer:
    """Create (but do not start) the health/metrics HTTP server.

    Args:
        port: TCP port to bind.
        pollers: All active FeedPoller instances; used for readiness checks.

    Returns:
        A configured ThreadingHTTPServer.  Call ``serve_forever()`` to start.
    """

    class _Handler(BaseHTTPRequestHandler):
        """Request handler with a closure over *pollers*."""

        def do_GET(self) -> None:  # noqa: N802 — required by BaseHTTPRequestHandler
            if self.path == "/metrics":
                data = generate_latest()
                self._respond(200, CONTENT_TYPE_LATEST, data)
            elif self.path == "/health":
                self._respond(200, "text/plain", b"ok")
            elif self.path in ("/ready", "/readiness"):
                if all(p.is_ready() for p in pollers):
                    self._respond(200, "text/plain", b"ready")
                else:
                    self._respond(503, "text/plain", b"not ready")
            else:
                self._respond(404, "text/plain", b"not found")

        def _respond(
            self, status: int, content_type: str, body: bytes
        ) -> None:
            """Send a simple HTTP response."""
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            """Suppress default access-log noise; use Python logging instead."""
            logger.debug("health %s - %s", self.address_string(), fmt % args)

    server = ThreadingHTTPServer(("", port), _Handler)
    return server
