"""Avro schema loading and management utilities."""

import json
from pathlib import Path
from typing import Any, cast

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SchemaLoader:
    """Loads and caches Avro schemas from the schemas repository."""

    def __init__(self, schema_base_path: Path | None = None):
        """Initialize schema loader.

        Args:
            schema_base_path: Base path to schemas directory.
                             Defaults to ../../schemas relative to this file.
        """
        if schema_base_path is None:
            # Default: go up from src/models to ingestion/nse-scraper, then to schemas
            current_file = Path(__file__).resolve()
            nse_scraper_root = current_file.parent.parent.parent
            repo_schema_path = nse_scraper_root.parent.parent / "schemas"

            # Prefer repo-mounted schemas path; fall back to /app/schemas when running in container
            container_schema_path = Path("/app/schemas")

            if repo_schema_path.exists():
                schema_base_path = repo_schema_path
            elif container_schema_path.exists():
                schema_base_path = container_schema_path
            else:
                schema_base_path = repo_schema_path

        self.schema_base_path = schema_base_path.resolve()
        self._schema_cache: dict[str, dict[str, Any]] = {}

        logger.info("SchemaLoader initialized", schema_path=str(self.schema_base_path))

    @staticmethod
    def schema_map() -> dict[str, str]:
        """Get mapping of schema types to file paths.

        Returns:
            Dictionary mapping schema types to relative paths
        """
        return {
            "raw_equity_ohlc": "market-data/raw_equity_ohlc.avsc",
            "normalized_equity_ohlc": "market-data/normalized_equity_ohlc.avsc",
            "option_chain_snapshot": "market-data/option_chain_snapshot.avsc",
            "symbol_master": "reference-data/symbol_master.avsc",
            "corporate_action": "reference-data/corporate_action.avsc",
            "trading_calendar": "reference-data/trading_calendar.avsc",
        }

    def load_schema(self, schema_path: str) -> dict[str, Any]:
        """Load an Avro schema from a .avsc file.

        Args:
            schema_path: Relative path from schema_base_path (e.g., 'market-data/raw_equity_ohlc.avsc')

        Returns:
            Avro schema as dictionary

        Raises:
            FileNotFoundError: If schema file doesn't exist
            json.JSONDecodeError: If schema file is invalid JSON
        """
        # Check cache first
        if schema_path in self._schema_cache:
            logger.debug("Schema loaded from cache", schema=schema_path)
            return self._schema_cache[schema_path]

        # Load from file
        full_path = self.schema_base_path / schema_path

        if not full_path.exists():
            raise FileNotFoundError(f"Schema file not found: {full_path}")

        logger.info("Loading schema from file", schema=schema_path, path=str(full_path))

        with open(full_path, encoding="utf-8") as f:
            schema = cast(dict[str, Any], json.load(f))

        # Cache the schema
        self._schema_cache[schema_path] = schema

        logger.info("Schema loaded successfully", schema=schema_path, name=schema.get("name"))
        return schema

    def load_value_schema(self, topic_type: str) -> dict[str, Any]:
        """Load value schema for a specific topic type.

        Args:
            topic_type: Topic type (e.g., 'raw_equity_ohlc', 'symbol_master')

        Returns:
            Avro schema dictionary

        Raises:
            ValueError: If topic type is unknown
        """
        schema_mapping = self.schema_map()

        if topic_type not in schema_mapping:
            raise ValueError(
                f"Unknown topic type: {topic_type}. Valid types: {list(schema_mapping.keys())}"
            )

        return self.load_schema(schema_mapping[topic_type])

    def get_schema_string(self, schema_path: str) -> str:
        """Get schema as JSON string.

        Args:
            schema_path: Relative path to schema file

        Returns:
            Schema as JSON string
        """
        schema = self.load_schema(schema_path)
        return json.dumps(schema)

    def clear_cache(self) -> None:
        """Clear the schema cache."""
        self._schema_cache.clear()
        logger.info("Schema cache cleared")


# Global schema loader instance
_schema_loader: SchemaLoader | None = None


def get_schema_loader() -> SchemaLoader:
    """Get or create the global schema loader instance.

    Returns:
        SchemaLoader instance
    """
    global _schema_loader
    if _schema_loader is None:
        _schema_loader = SchemaLoader()
    return _schema_loader


def load_schema(schema_path: str) -> dict[str, Any]:
    """Convenience function to load a schema using the global loader.

    Args:
        schema_path: Relative path from schema base directory

    Returns:
        Avro schema dictionary
    """
    return get_schema_loader().load_schema(schema_path)
