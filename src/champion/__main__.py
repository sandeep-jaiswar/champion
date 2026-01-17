"""Main entry point for Champion application.

Handles:
- Signal handling for graceful shutdown
- Health check endpoint
- Production logging setup
"""

import signal
import sys
from typing import NoReturn

from champion.cli import app
from champion.utils.logger import configure_logging, get_logger

# Configure logging for production
configure_logging()
logger = get_logger(__name__)

# Global flag for graceful shutdown
_shutdown_requested = False


def signal_handler(signum: int, frame) -> None:
    """Handle shutdown signals gracefully.

    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global _shutdown_requested

    signal_name = signal.Signals(signum).name
    logger.info(f"Received {signal_name} signal, initiating graceful shutdown...")

    _shutdown_requested = True

    # Exit cleanly
    logger.info("Shutdown complete")
    sys.exit(0)


def main() -> NoReturn:
    """Main entry point with signal handling."""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Champion application starting...")
    logger.info("Signal handlers registered (SIGTERM, SIGINT)")

    # Start health check server for container orchestration
    from champion.core.health import start_health_server

    try:
        start_health_server(host="0.0.0.0", port=8080)
    except Exception as e:
        logger.warning(f"Failed to start health check server: {e}")

    try:
        # Run the Typer CLI app
        app()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

    # Should never reach here, but for type checking
    sys.exit(0)


if __name__ == "__main__":
    main()
