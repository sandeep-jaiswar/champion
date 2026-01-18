"""User repository for data persistence in ClickHouse."""

import logging
from typing import Any

from champion.warehouse.adapters import ClickHouseSink

logger = logging.getLogger(__name__)


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
        """Initialize the users table if it doesn't exist.
        
        Raises:
            Exception: Re-raises database errors after logging
        """
        try:
            if not self.sink.client:
                error_msg = "ClickHouse client not connected"
                logger.error(f"init_table_failed: {error_msg}")
                raise RuntimeError(error_msg)
            
            self.sink.client.query(self.CREATE_TABLE_SQL)
            logger.info("users_table_initialized")
        except Exception as e:
            logger.error(
                f"init_table_error: Failed to initialize users table",
                extra={
                    "sql": self.CREATE_TABLE_SQL,
                    "client": str(self.sink.client) if self.sink.client else None,
                    "error": str(e)
                }
            )
            raise RuntimeError(f"Failed to initialize users table: {e}") from e

    def get_by_username(self, username: str) -> dict[str, Any] | None:
        """Get user by username.

        Args:
            username: The username to look up

        Returns:
            User data dictionary or None if not found
            
        Raises:
            Exception: Re-raises database errors after logging
        """
        try:
            if not self.sink.client:
                logger.error("get_by_username_failed: ClickHouse client not connected")
                raise RuntimeError("ClickHouse client not connected")

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
        except Exception as e:
            logger.error(
                f"get_by_username_error: {str(e)}",
                extra={"username": username, "client": str(self.sink.client) if self.sink.client else None}
            )
            raise

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
            
        Raises:
            Exception: Re-raises database errors after logging
        """
        try:
            if not self.sink.client:
                logger.error("create_user_failed: ClickHouse client not connected")
                raise RuntimeError("ClickHouse client not connected")

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
            logger.info(f"user_created: {username}")
            return True
        except Exception as e:
            logger.error(
                f"create_user_error: {str(e)}",
                extra={
                    "username": username,
                    "email": email,
                    "client": str(self.sink.client) if self.sink.client else None
                }
            )
            raise

    def user_exists(self, username: str) -> bool:
        """Check if user exists.

        Args:
            username: Username to check

        Returns:
            True if user exists, False otherwise
            
        Raises:
            Exception: Re-raises database errors after logging
        """
        try:
            if not self.sink.client:
                logger.error("user_exists_failed: ClickHouse client not connected")
                raise RuntimeError("ClickHouse client not connected")

            query = "SELECT 1 FROM users WHERE username = %(username)s LIMIT 1"
            result = self.sink.client.query(query, {"username": username})
            return bool(result and result.result_rows)
        except Exception as e:
            logger.error(
                f"user_exists_error: {str(e)}",
                extra={"username": username, "client": str(self.sink.client) if self.sink.client else None}
            )
            raise
