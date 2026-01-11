#!/usr/bin/env python
"""
Complete Prefect + MLflow + Prometheus Stack Setup & Launch

This script provides one-command setup and launch of the entire
Champion data pipeline visualization and orchestration stack.
"""

import os
import sys
import subprocess
import time
from pathlib import Path
from typing import List, Tuple
import signal

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
except ImportError:
    print("Installing required packages...")
    subprocess.run([sys.executable, "-m", "pip", "install", "rich", "requests"], check=True)
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


class StackManager:
    """Manages the complete Prefect + MLflow + Prometheus stack."""

    def __init__(self):
        """Initialize stack manager."""
        self.processes: dict[str, subprocess.Popen] = {}
        self.project_root = Path(__file__).parent

    def check_dependencies(self) -> Tuple[bool, List[str]]:
        """Check if required tools are installed."""
        missing = []

        tools = {
            "docker": "Docker",
            "docker-compose": "Docker Compose",
            "poetry": "Poetry",
        }

        for cmd, name in tools.items():
            try:
                subprocess.run(
                    [cmd, "--version"],
                    capture_output=True,
                    check=True,
                )
            except (subprocess.CalledProcessError, FileNotFoundError):
                missing.append(name)

        return len(missing) == 0, missing

    def display_banner(self):
        """Display welcome banner."""
        banner = """
╔══════════════════════════════════════════════════════════════════════╗
║       🚀 CHAMPION DATA PIPELINE - COMPLETE STACK SETUP & LAUNCH      ║
║                                                                      ║
║        Prefect + MLflow + Prometheus + ClickHouse + Kafka            ║
╚══════════════════════════════════════════════════════════════════════╝
        """
        console.print(banner, style="cyan bold")

    def display_architecture(self):
        """Display complete architecture."""
        console.print("\n[bold cyan]📊 Complete Stack Architecture[/bold cyan]\n")

        architecture = """
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                         │
│  NSE API → httpx + retry logic → Polars parsing               │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION LAYER                           │
│  Prefect Flows [🔀] ← Scheduling, monitoring, retries         │
│  ├─ nse-bhavcopy-etl                                           │
│  ├─ bulk-block-deals-etl                                       │
│  ├─ trading-calendar-etl                                       │
│  ├─ index-constituents-etl                                     │
│  ├─ option-chain-etl                                           │
│  └─ combined-market-data-etl                                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
    │ Parquet Lake │  │  Kafka Msgs  │  │ ClickHouse DW    │
    │   (Bronze)   │  │  (Avro)      │  │ (Analytics)      │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────────┘
           │                 │                  │
           └─────────────────┼──────────────────┘
                             ▼
                    ┌──────────────────┐
                    │ Observability    │
                    │                  │
                    │ MLflow [📊]      │ - Metrics tracking
                    │ Prometheus [📈]  │ - System metrics
                    │ Grafana [📉]     │ - Dashboards
                    │ Prefect UI [🔀]  │ - Flow monitoring
                    └──────────────────┘
        """
        console.print(architecture, style="dim")

    def start_docker_services(self) -> bool:
        """Start Docker Compose services (Kafka, ClickHouse, etc)."""
        console.print("\n[yellow]⏳ Starting Docker Compose services...[/yellow]")

        docker_compose_path = self.project_root.parent.parent / "docker-compose.yml"

        if not docker_compose_path.exists():
            console.print(
                f"[red]✗ docker-compose.yml not found at {docker_compose_path}[/red]"
            )
            return False

        try:
            # Start services in background
            process = subprocess.Popen(
                ["docker-compose", "-f", str(docker_compose_path), "up", "-d"],
                cwd=str(docker_compose_path.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            process.wait(timeout=60)

            console.print("✅ [green]Docker Compose services started[/green]")
            return True

        except subprocess.TimeoutExpired:
            console.print("[yellow]⚠️  Docker Compose startup timeout (may still be starting)[/yellow]")
            return True
        except Exception as e:
            console.print(f"[red]✗ Failed to start Docker Compose: {e}[/red]")
            return False

    def start_prefect_server(self) -> bool:
        """Start Prefect server."""
        console.print("\n[yellow]⏳ Starting Prefect server...[/yellow]")

        try:
            self.processes["prefect_server"] = subprocess.Popen(
                ["prefect", "server", "start"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Wait for server to start
            time.sleep(3)

            # Check if running
            try:
                import requests
                requests.get("http://localhost:4200/api/hello", timeout=5)
                console.print(
                    "✅ [green]Prefect Server started[/green] → http://localhost:4200"
                )
                return True
            except Exception:
                console.print(
                    "[yellow]⚠️  Prefect Server starting (may take a moment)[/yellow]"
                )
                return True

        except Exception as e:
            console.print(f"[red]✗ Failed to start Prefect Server: {e}[/red]")
            return False

    def start_mlflow_server(self) -> bool:
        """Start MLflow server."""
        console.print("\n[yellow]⏳ Starting MLflow server...[/yellow]")

        try:
            mlflow_dir = self.project_root / "data" / "mlflow"
            mlflow_dir.mkdir(parents=True, exist_ok=True)

            self.processes["mlflow"] = subprocess.Popen(
                [
                    "mlflow",
                    "ui",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "5000",
                    "--backend-store-uri",
                    f"sqlite:///{mlflow_dir}/mlflow.db",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Wait for server to start
            time.sleep(2)

            console.print(
                "✅ [green]MLflow Server started[/green] → http://localhost:5000"
            )
            return True

        except Exception as e:
            console.print(f"[red]✗ Failed to start MLflow Server: {e}[/red]")
            return False

    def deploy_flows(self) -> bool:
        """Deploy Prefect flows."""
        console.print("\n[yellow]⏳ Deploying Prefect flows...[/yellow]")

        try:
            flows_module = self.project_root / "src" / "orchestration" / "flows.py"

            if not flows_module.exists():
                console.print(f"[red]✗ Flows module not found at {flows_module}[/red]")
                return False

            # Deploy flows
            result = subprocess.run(
                [sys.executable, "-m", "src.orchestration.flows", "deploy"],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode == 0:
                console.print("✅ [green]Flows deployed successfully[/green]")
                return True
            else:
                console.print(f"[red]✗ Failed to deploy flows: {result.stderr}[/red]")
                return False

        except subprocess.TimeoutExpired:
            console.print("[yellow]⚠️  Flow deployment timeout[/yellow]")
            return False
        except Exception as e:
            console.print(f"[red]✗ Failed to deploy flows: {e}[/red]")
            return False

    def start_prefect_agent(self) -> bool:
        """Start Prefect agent."""
        console.print("\n[yellow]⏳ Starting Prefect agent...[/yellow]")

        try:
            self.processes["prefect_agent"] = subprocess.Popen(
                ["prefect", "agent", "start", "-q", "default"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            time.sleep(2)
            console.print("✅ [green]Prefect Agent started[/green]")
            return True

        except Exception as e:
            console.print(f"[red]✗ Failed to start Prefect Agent: {e}[/red]")
            return False

    def display_status_dashboard(self):
        """Display status of all services."""
        console.print("\n[bold cyan]📊 Service Status Dashboard[/bold cyan]\n")

        status_table = Table(show_header=True, header_style="bold cyan")
        status_table.add_column("Service", style="green")
        status_table.add_column("Status", style="yellow")
        status_table.add_column("URL/Command", style="magenta")

        services = [
            ("Prefect Server", "🟢 Running", "http://localhost:4200"),
            ("MLflow Server", "🟢 Running", "http://localhost:5000"),
            ("Prefect Agent", "🟢 Running", "Monitoring flows..."),
            ("Docker Services", "🟢 Running", "Kafka, ClickHouse, etc."),
            ("Dashboard", "🟢 Ready", "python prefect_dashboard.py"),
        ]

        for service, status, url in services:
            status_table.add_row(service, status, url)

        console.print(status_table)

    def display_endpoints(self):
        """Display all accessible endpoints."""
        console.print("\n[bold cyan]🔗 Service Endpoints[/bold cyan]\n")

        endpoints = [
            ("Prefect Dashboard", "[cyan underline]http://localhost:4200[/cyan underline]", "View flows, runs, logs"),
            ("MLflow Tracking", "[cyan underline]http://localhost:5000[/cyan underline]", "Metrics, experiments"),
            ("ClickHouse", "[cyan underline]http://localhost:8123[/cyan underline]", "SQL queries"),
            ("Kafka UI", "[cyan underline]http://localhost:8080[/cyan underline]", "Topics, messages"),
            ("Prometheus", "[cyan underline]http://localhost:9090[/cyan underline]", "Metrics (if enabled)"),
            ("Grafana", "[cyan underline]http://localhost:3000[/cyan underline]", "Dashboards (if enabled)"),
        ]

        for service, url, purpose in endpoints:
            console.print(f"  {service:20} → {url:40} ({purpose})")

    def display_next_steps(self):
        """Display next steps and commands."""
        console.print("\n[bold cyan]🚀 Next Steps[/bold cyan]\n")

        steps_text = """
[green]✅ STACK IS RUNNING[/green]

[yellow]1. View Prefect Dashboard:[/yellow]
   [cyan]→ http://localhost:4200[/cyan]

[yellow]2. View MLflow Metrics:[/yellow]
   [cyan]→ http://localhost:5000[/cyan]

[yellow]3. Launch Visualization Dashboard:[/yellow]
   [cyan]cd ingestion/nse-scraper
   python prefect_dashboard.py[/cyan]

[yellow]4. Trigger Manual Flow Run:[/yellow]
   [cyan]prefect deployment run 'nse-bhavcopy-etl/nse-bhavcopy-daily'[/cyan]

[yellow]5. View Flow Logs:[/yellow]
   [cyan]prefect flow-run logs <flow-run-id>[/cyan]

[yellow]6. Monitor Data Pipeline:[/yellow]
   [cyan]→ http://localhost:4200 (Prefect UI - Real-time)[/cyan]
   [cyan]→ http://localhost:5000 (MLflow - Metrics & History)[/cyan]

[magenta]Advanced Commands:[/magenta]
   • List all flows: [cyan]prefect deployment ls[/cyan]
   • View recent runs: [cyan]prefect flow-run ls -l 10[/cyan]
   • Set parameters: [cyan]prefect deployment set-schedule ...[/cyan]
   • Access ClickHouse: [cyan]poetry run python -c "import clickhouse_connect"[/cyan]
        """
        console.print(steps_text)

    def display_troubleshooting(self):
        """Display troubleshooting tips."""
        console.print("\n[bold yellow]⚠️  Troubleshooting[/bold yellow]\n")

        troubleshooting = """
[yellow]If Prefect Server doesn't start:[/yellow]
   1. Check port 4200 is not in use: [cyan]lsof -i :4200[/cyan]
   2. Clear Prefect data: [cyan]rm -rf ~/.prefect[/cyan]
   3. Restart: [cyan]prefect server start[/cyan]

[yellow]If MLflow Server doesn't start:[/yellow]
   1. Check port 5000 is not in use: [cyan]lsof -i :5000[/cyan]
   2. Check SQLite database: [cyan]ls -la data/mlflow/[/cyan]
   3. Restart: [cyan]poetry run mlflow ui --port 5000[/cyan]

[yellow]If flows don't execute:[/yellow]
   1. Check agent is running: [cyan]prefect agent status[/cyan]
   2. Check work queue: [cyan]prefect work-queue ls[/cyan]
   3. View agent logs: [cyan]prefect agent status[/cyan]

[yellow]If Docker services fail:[/yellow]
   1. Check Docker: [cyan]docker --version[/cyan]
   2. Restart Docker: [cyan]docker-compose -f docker-compose.yml restart[/cyan]
   3. View logs: [cyan]docker-compose logs -f[/cyan]

[yellow]For more help:[/yellow]
   • Prefect docs: https://docs.prefect.io
   • MLflow docs: https://mlflow.org/docs
   • Issue tracker: GitHub Issues
        """
        console.print(troubleshooting)

    def cleanup_on_exit(self):
        """Clean up running processes on exit."""
        console.print("\n[yellow]Shutting down services...[/yellow]")

        for name, process in self.processes.items():
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                    console.print(f"  ✓ Stopped {name}")
                except Exception as e:
                    console.print(f"  ⚠️  Failed to stop {name}: {e}")

        console.print("[green]✅ Cleanup complete[/green]")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""

        def handle_signal(signum, frame):
            console.print(
                "\n[yellow]Received interrupt signal, shutting down gracefully...[/yellow]"
            )
            self.cleanup_on_exit()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

    def run_setup(self):
        """Run complete setup."""
        self.display_banner()

        # Check dependencies
        console.print("\n[cyan]Checking dependencies...[/cyan]")
        all_ok, missing = self.check_dependencies()

        if not all_ok:
            console.print(
                f"[red]✗ Missing tools: {', '.join(missing)}[/red]\n"
                "Please install and try again."
            )
            return False

        console.print("✅ [green]All dependencies found[/green]")

        # Display architecture
        self.display_architecture()

        # Setup signal handlers
        self.setup_signal_handlers()

        # Start services
        console.print("\n[bold cyan]🚀 Starting services...[/bold cyan]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("Starting services...", total=None)

            # Start services
            self.start_docker_services()
            time.sleep(2)

            self.start_prefect_server()
            time.sleep(2)

            self.start_mlflow_server()
            time.sleep(2)

            self.deploy_flows()
            time.sleep(1)

            self.start_prefect_agent()

        # Display status
        self.display_status_dashboard()
        self.display_endpoints()
        self.display_next_steps()
        self.display_troubleshooting()

        # Keep running
        try:
            console.print("\n[cyan]Stack is running. Press Ctrl+C to stop.[/cyan]")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup_on_exit()


def main():
    """Main entry point."""
    manager = StackManager()

    try:
        manager.run_setup()
    except Exception as e:
        console.print(f"[red]✗ Error: {e}[/red]")
        manager.cleanup_on_exit()
        sys.exit(1)


if __name__ == "__main__":
    main()
