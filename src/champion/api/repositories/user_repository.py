"""User repository for data persistence in ClickHouse."""

from typing import Any

from champion.warehouse.adapters import ClickHouseSink


class UserRepository:
    """Repository for user operations in ClickHouse."""

    # Table schema SQL
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS users (
        username String NOT NULL,
        email String NOT NULL,
        hashed_password String NOT NULL,
        disabled UInt8 DEFAULT 0,
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY username
    """

    def __init__(self, sink: ClickHouseSink):
        """Initialize repository with ClickHouse sink.

        Args:
            sink: ClickHouse database connection
        """
        self.sink = sink

    def init_table(self) -> None:
        """Initialize the users table if it doesn't exist."""
        try:
            if not self.sink.client:
                return
            self.sink.client.query(self.CREATE_TABLE_SQL)
        except Exception:
            # Table might already exist, ignore error
            pass

    def get_by_username(self, username: str) -> dict[str, Any] | None:
        """Get user by username.

        Args:
            username: The username to look up

        Returns:
            User data dictionary or None if not found
        """
        try:
            if not self.sink.client:
                return None

            query = "SELECT username, email, hashed_password, disabled FROM users WHERE username = %(username)s LIMIT 1"
            result = self.sink.client.query(query, {"username": username})

            if result and result.result_rows:
                data = result.result_rows[0]
                return {
                    "username": data[0],
                    "email": data[1],
                    "hashed_password": data[2],
                    "disabled": bool(data[3]),
                }
            return None
        except Exception:
            return None

    def create_user(
        self,
        username: str,
        email: str,
        hashed_password: str,
        disabled: bool = False
    ) -> bool:
        """Create a new user.

        Args:
            username: Username
            email: Email address
            hashed_password: Hashed password
            disabled: Whether account is disabled

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.sink.client:
                return False

            # Use insert method with list of dicts
            self.sink.client.insert(
                "users",
                [{
                    "username": username,
                    "email": email,
                    "hashed_password": hashed_password,
                    "disabled": int(disabled),
                }]
            )
            return True
        except Exception:
            return False

    def user_exists(self, username: str) -> bool:
        """Check if user exists.

        Args:
            username: Username to check

        Returns:
            True if user exists, False otherwise
        """
        try:
            if not self.sink.client:
                return False

            query = "SELECT 1 FROM users WHERE username = %(username)s LIMIT 1"
            result = self.sink.client.query(query, {"username": username})
            return bool(result and result.result_rows)
        except Exception:
            return False
