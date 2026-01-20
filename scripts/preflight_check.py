#!/usr/bin/env python3
"""Preflight checks for local development environment.

Checks:
- Python version >= 3.10
- ClickHouse HTTP endpoint responds to SELECT 1
- MLflow health endpoint responds
- CLI `show-config` invocation succeeds
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request


def check_python_version() -> None:
    if sys.version_info < (3, 10):
        raise SystemExit("Python 3.10+ is required for this project")


def http_get(url: str, timeout: int = 5) -> int:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status


def check_clickhouse() -> None:
    host = os.environ.get("CHAMPION_CLICKHOUSE_HOST", "localhost")
    port = os.environ.get("CHAMPION_CLICKHOUSE_PORT", "8123")
    user = os.environ.get("CHAMPION_CLICKHOUSE_USER", "default")
    password = os.environ.get("CHAMPION_CLICKHOUSE_PASSWORD", "")

    base = f"http://{host}:{port}"

    # Try root endpoint first
    attempts = 60
    for _ in range(attempts):
        try:
            status = http_get(base + "/", timeout=2)
            if status == 200:
                print("ClickHouse OK (root)")
                return
        except Exception:
            pass
        time.sleep(1)

    # Try query endpoint, optionally with basic auth
    query = "/?query=SELECT+1"
    for _ in range(attempts):
        try:
            if user and password:
                parsed = f"http://{user}:{password}@{host}:{port}{query}"
                status = http_get(parsed, timeout=2)
            else:
                status = http_get(base + query, timeout=2)
            if status == 200:
                print("ClickHouse OK (query)")
                return
        except Exception:
            pass
        time.sleep(1)

    raise SystemExit(f"ClickHouse HTTP endpoint not reachable at {base}")


def check_mlflow() -> None:
    url = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    # Support a full URL or host:port
    if not url.startswith("http"):
        url = f"http://{url}"
    health = urllib.parse.urljoin(url, "/health")
    try:
        status = http_get(health, timeout=2)
        if status == 200:
            print("MLflow OK")
            return
    except Exception:
        pass
    raise SystemExit(f"MLflow health endpoint not reachable at {health}")


def check_cli() -> None:
    # Try running the show-config command via module invocation
    try:
        subprocess.run([sys.executable, "-m", "champion.cli", "show-config"], check=True)
        print("Champion CLI OK")
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Champion CLI failed: {e}") from e


def main() -> int:
    check_python_version()
    print(f"Python {sys.version.split()[0]} OK")
    # ClickHouse and MLflow may be optional; perform checks but report helpful errors
    check_clickhouse()
    check_mlflow()
    check_cli()
    print("All preflight checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
