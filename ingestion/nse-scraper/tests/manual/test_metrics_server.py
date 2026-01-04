"""Quick script to test metrics server.

Usage:
    python -m tests.manual.test_metrics_server
"""

import time

from src.utils.logger import configure_logging, get_logger
from src.utils.metrics import start_metrics_server

configure_logging()
logger = get_logger(__name__)


def main():
    """Start metrics server and keep it running."""
    # Start metrics server
    logger.info("Starting metrics server on port 9090")
    start_metrics_server(9090)

    logger.info("Metrics server started. Check http://localhost:9090/metrics")
    logger.info("Press Ctrl+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")


if __name__ == "__main__":
    main()
