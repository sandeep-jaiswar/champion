"""Health check endpoint for container orchestration.

Provides a simple HTTP endpoint for Docker/Kubernetes health checks.
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Optional

from champion.utils.logger import get_logger

logger = get_logger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check requests."""
    
    def do_GET(self) -> None:
        """Handle GET requests for health check."""
        if self.path == "/health":
            # Return healthy status
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {
                "status": "healthy",
                "service": "champion"
            }
            self.wfile.write(json.dumps(response).encode())
        elif self.path == "/ready":
            # Readiness check (can be extended with actual checks)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {
                "status": "ready",
                "service": "champion"
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format: str, *args) -> None:
        """Override to use structured logging."""
        logger.debug(f"Health check request: {format % args}")


class HealthCheckServer:
    """Health check HTTP server running in background thread."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8080):
        """Initialize health check server.
        
        Args:
            host: Host to bind to (default: 0.0.0.0)
            port: Port to listen on (default: 8080)
        """
        self.host = host
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.thread: Optional[Thread] = None
    
    def start(self) -> None:
        """Start the health check server in background thread."""
        self.server = HTTPServer((self.host, self.port), HealthCheckHandler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        logger.info(f"Health check server started on {self.host}:{self.port}")
    
    def stop(self) -> None:
        """Stop the health check server."""
        if self.server:
            self.server.shutdown()
            logger.info("Health check server stopped")


# Global health check server instance
_health_server: Optional[HealthCheckServer] = None


def start_health_server(host: str = "0.0.0.0", port: int = 8080) -> None:
    """Start the global health check server.
    
    Args:
        host: Host to bind to
        port: Port to listen on
    """
    global _health_server
    if _health_server is None:
        _health_server = HealthCheckServer(host, port)
        _health_server.start()


def stop_health_server() -> None:
    """Stop the global health check server."""
    global _health_server
    if _health_server:
        _health_server.stop()
        _health_server = None
