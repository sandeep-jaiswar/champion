"""Script to initialize demo user in ClickHouse."""

import os
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(src_path))

from champion.api.repositories import UserRepository
from champion.warehouse.adapters import ClickHouseSink
from passlib.context import CryptContext

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def init_demo_user():
    """Initialize demo user in ClickHouse."""
    # Get environment variables
    clickhouse_host = os.getenv("CHAMPION_CLICKHOUSE_HOST", "localhost")
    clickhouse_port = int(os.getenv("CHAMPION_CLICKHOUSE_PORT", "8123"))
    clickhouse_user = os.getenv("CHAMPION_CLICKHOUSE_USER", "default")
    clickhouse_password = os.getenv("CHAMPION_CLICKHOUSE_PASSWORD", "")
    clickhouse_database = os.getenv("CHAMPION_CLICKHOUSE_DATABASE", "champion")

    # Create ClickHouse sink
    sink = ClickHouseSink(
        host=clickhouse_host,
        port=clickhouse_port,
        user=clickhouse_user,
        password=clickhouse_password,
        database=clickhouse_database,
    )
    sink.connect()

    # Create repository
    repo = UserRepository(sink)

    # Initialize table
    print("Initializing users table...")
    repo.init_table()

    # Check if demo user already exists
    demo_user = repo.get_by_username("demo")
    if demo_user:
        print("Demo user already exists. Skipping initialization.")
        return

    # Create demo user
    print("Creating demo user...")

    # Get password from environment variable or generate secure one
    demo_password = os.getenv("DEMO_PASSWORD")
    if not demo_password:
        import secrets
        import string

        # Generate secure random password with safe characters
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        demo_password = "".join(secrets.choice(alphabet) for _ in range(16))
        # Print to stderr for CI/dev environments only
        if os.getenv("CI") or os.getenv("ENV") == "dev":
            print(f"Generated password: {demo_password}", file=sys.stderr)

    hashed_password = pwd_context.hash(demo_password)
    success = repo.create_user(
        username="demo", email="demo@champion.com", hashed_password=hashed_password, disabled=False
    )

    if success:
        print("✅ Demo user created successfully!")
        print("  Username: demo")
        print("  Email: demo@champion.com")
        if not os.getenv("DEMO_PASSWORD"):
            print("  Note: Set DEMO_PASSWORD environment variable to use a specific password")
    else:
        print("❌ Failed to create demo user")
        sys.exit(1)


if __name__ == "__main__":
    init_demo_user()
