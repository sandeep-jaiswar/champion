"""
ClickHouse Batch Loader for Champion Market Data

This module provides utilities to load Parquet data from the data lake
into ClickHouse tables. Supports all three layers: raw, normalized, and features.

Usage:
    python -m warehouse.loader.batch_loader \\
        --table raw_equity_ohlc \\
        --source data/lake/raw/equity_ohlc/date=2024-01-15/ \\
        --host localhost \\
        --port 8123

Environment Variables:
    CLICKHOUSE_HOST: ClickHouse host (default: localhost)
    CLICKHOUSE_PORT: ClickHouse HTTP port (default: 8123)
    CLICKHOUSE_USER: ClickHouse user (default: champion_user)
    CLICKHOUSE_PASSWORD: ClickHouse password (default: champion_pass)
    CLICKHOUSE_DATABASE: ClickHouse database (default: champion_market)
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import clickhouse_connect
    import polars as pl
    from clickhouse_connect.driver import Client
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Please install dependencies: pip install polars clickhouse-connect")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """Batch loader for loading Parquet data into ClickHouse."""

    # Supported tables and their layer mappings
    SUPPORTED_TABLES = {
        "raw_equity_ohlc": "raw",
        "normalized_equity_ohlc": "normalized",
        "features_equity_indicators": "features",
        "symbol_master": "reference",
    }

    # Column name mapping: Parquet column -> ClickHouse column
    # This allows loading from Parquet files with different naming conventions
    COLUMN_MAPPINGS = {
        "normalized_equity_ohlc": {
            # Support both NSE names (TradDt) and normalized names (trade_date)
            "trade_date": "TradDt",
            "symbol": "TckrSymb",
            "open": "OpnPric",
            "high": "HghPric",
            "low": "LwPric",
            "close": "ClsPric",
            "last_price": "LastPric",
            "prev_close": "PrvsClsgPric",
            "settlement_price": "SttlmPric",
            "volume": "TtlTradgVol",
            "turnover": "TtlTrfVal",
            "trades": "TtlNbOfTxsExctd",
            "isin": "ISIN",
            "instrument_id": "FinInstrmId",
            "instrument_type": "FinInstrmTp",
            "instrument_name": "FinInstrmNm",
            "segment": "Sgmt",
            "series": "SctySrs",
            "exchange": "Src",
            "business_date": "BizDt",
            "expiry_date": "XpryDt",
            "strike_price": "StrkPric",
            "option_type": "OptnTp",
            "underlying_price": "UndrlygPric",
            "open_interest": "OpnIntrst",
            "change_in_oi": "ChngInOpnIntrst",
            "session_id": "SsnId",
            "board_lot_qty": "NewBrdLotQty",
            "remarks": "Rmks",
        },
        "features_equity_indicators": {
            # Features table uses normalized names, no mapping needed yet
        },
    }

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        user: str = "champion_user",
        password: str = "champion_pass",
        database: str = "champion_market",
    ):
        """
        Initialize ClickHouse loader.

        Args:
            host: ClickHouse server host
            port: ClickHouse HTTP port
            user: Database user
            password: Database password
            database: Database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client: Client | None = None

    def connect(self) -> None:
        """Establish connection to ClickHouse."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
            )
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")

            # Test connection
            _ = self.client.command("SELECT 1")
            logger.info("Connection test successful")

        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def disconnect(self) -> None:
        """Close connection to ClickHouse."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from ClickHouse")

    def load_parquet_files(
        self,
        table: str,
        source_path: str,
        batch_size: int = 100000,
        dry_run: bool = False,
    ) -> dict:
        """
        Load Parquet files from source path into ClickHouse table.

        Args:
            table: Target ClickHouse table name
            source_path: Path to Parquet file(s) or directory
            batch_size: Number of rows per batch insert
            dry_run: If True, only validate without loading

        Returns:
            Dictionary with load statistics
        """
        if table not in self.SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table: {table}. "
                f"Supported tables: {list(self.SUPPORTED_TABLES.keys())}"
            )

        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Source path not found: {source_path}")

        # Find all Parquet files
        parquet_files = self._find_parquet_files(source)
        if not parquet_files:
            raise ValueError(f"No Parquet files found in: {source_path}")

        logger.info(f"Found {len(parquet_files)} Parquet file(s) to load")

        total_rows = 0
        total_files = 0
        start_time = datetime.now()

        for file_path in parquet_files:
            try:
                logger.info(f"Loading file: {file_path.name}")

                # Read Parquet file
                df = pl.read_parquet(file_path)
                rows = len(df)

                logger.info(f"Read {rows:,} rows from {file_path.name}")

                if dry_run:
                    logger.info(f"[DRY RUN] Would insert {rows:,} rows into {table}")
                    logger.info(f"[DRY RUN] Sample schema: {df.schema}")
                else:
                    # Insert data in batches
                    rows_inserted = self._insert_dataframe(
                        df=df,
                        table=table,
                        batch_size=batch_size,
                    )
                    logger.info(f"Inserted {rows_inserted:,} rows into {table}")

                total_rows += rows
                total_files += 1

            except Exception as e:
                logger.error(f"Error loading file {file_path}: {e}")
                raise

        duration = (datetime.now() - start_time).total_seconds()

        stats = {
            "table": table,
            "files_loaded": total_files,
            "total_rows": total_rows,
            "duration_seconds": duration,
            "rows_per_second": int(total_rows / duration) if duration > 0 else 0,
            "dry_run": dry_run,
        }

        logger.info(f"Load complete: {stats}")
        return stats

    def _find_parquet_files(self, path: Path) -> list[Path]:
        """Find all Parquet files in path (recursively if directory)."""
        if path.is_file() and path.suffix == ".parquet":
            return [path]
        elif path.is_dir():
            return sorted(path.rglob("*.parquet"))
        else:
            return []

    def _insert_dataframe(
        self,
        df: pl.DataFrame,
        table: str,
        batch_size: int,
    ) -> int:
        """
        Insert Polars DataFrame into ClickHouse table.

        Args:
            df: DataFrame to insert
            table: Target table name
            batch_size: Rows per batch

        Returns:
            Number of rows inserted
        """
        if not self.client:
            raise RuntimeError("Not connected to ClickHouse. Call connect() first.")

        # Prepare column mapping (handle datetime conversions)
        df = self._prepare_dataframe_for_insert(df, table)

        # Convert to pandas for clickhouse-connect compatibility
        pdf = df.to_pandas()

        total_rows = len(pdf)
        rows_inserted = 0

        # Insert in batches
        for i in range(0, total_rows, batch_size):
            batch = pdf.iloc[i : i + batch_size]

            try:
                self.client.insert_df(
                    table=table, df=batch, settings={"async_insert": 0, "wait_for_async_insert": 0}
                )
                rows_inserted += len(batch)

                if rows_inserted % (batch_size * 10) == 0:
                    logger.info(f"Progress: {rows_inserted:,} / {total_rows:,} rows")

            except Exception as e:
                logger.error(f"Error inserting batch at offset {i}: {e}")
                raise

        return rows_inserted

    def _prepare_dataframe_for_insert(
        self,
        df: pl.DataFrame,
        table: str,
    ) -> pl.DataFrame:
        """
        Prepare DataFrame for insertion (handle type conversions and column mapping).

        Args:
            df: Input DataFrame
            table: Target table name

        Returns:
            Prepared DataFrame with columns mapped to ClickHouse schema
        """
        # Apply column name mapping if configured for this table
        if table in self.COLUMN_MAPPINGS and len(self.COLUMN_MAPPINGS[table]) > 0:
            mapping = self.COLUMN_MAPPINGS[table]

            # Only rename columns that exist in the DataFrame
            rename_map = {}
            for source_col, target_col in mapping.items():
                if source_col in df.columns:
                    rename_map[source_col] = target_col
                    logger.debug(f"Mapping column: {source_col} -> {target_col}")

            if rename_map:
                df = df.rename(rename_map)
                logger.info(f"Applied column name mappings: {len(rename_map)} columns renamed")

        # Validate required columns based on table
        self._validate_schema(df, table)

        # Convert Date columns if needed (Polars Date -> Python date)
        # ClickHouse expects date objects for Date columns

        # Handle timestamp columns (convert to datetime if they're integers)
        timestamp_cols = ["event_time", "ingest_time", "feature_timestamp"]
        for col in timestamp_cols:
            if col in df.columns:
                if df[col].dtype in [pl.Int64, pl.Int32]:
                    # Convert from milliseconds since epoch to datetime
                    df = df.with_columns(pl.from_epoch(pl.col(col), time_unit="ms").alias(col))

        # Handle date columns
        date_cols = [
            "TradDt",
            "BizDt",
            "XpryDt",
            "FininstrmActlXpryDt",
            "trade_date",
            "adjustment_date",
        ]
        for col in date_cols:
            if col in df.columns:
                if df[col].dtype == pl.Utf8:
                    # Parse string dates
                    try:
                        df = df.with_columns(
                            pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d").alias(col)
                        )
                    except (pl.ComputeError, ValueError) as e:
                        logger.warning(f"Failed to parse date column {col}: {e}")
                        pass  # Keep as is if parsing fails

        return df

    def _validate_schema(self, df: pl.DataFrame, table: str) -> None:
        """
        Validate that DataFrame has required columns for the target table.

        Args:
            df: DataFrame to validate
            table: Target table name

        Raises:
            ValueError: If required columns are missing
        """
        # Define required columns for each table (columns used in ORDER BY or critical fields)
        required_columns = {
            "raw_equity_ohlc": ["TckrSymb", "FinInstrmId", "TradDt", "event_time"],
            "normalized_equity_ohlc": ["TckrSymb", "FinInstrmId", "TradDt", "event_time"],
            "features_equity_indicators": ["symbol", "trade_date", "feature_timestamp"],
        }

        if table not in required_columns:
            logger.warning(f"No schema validation defined for table: {table}")
            return

        missing_cols = []
        for required_col in required_columns[table]:
            if required_col not in df.columns:
                missing_cols.append(required_col)

        if missing_cols:
            available_cols = sorted(df.columns)
            error_msg = (
                f"Schema validation failed for table '{table}'. "
                f"Missing required columns: {missing_cols}. "
                f"Available columns in Parquet: {available_cols}. "
                f"Hint: The Parquet file may use a different naming convention. "
                f"Check COLUMN_MAPPINGS in batch_loader.py for supported mappings."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

    def verify_load(self, table: str, expected_rows: int | None = None) -> dict:
        """
        Verify data was loaded correctly.

        Args:
            table: Table name to verify
            expected_rows: Expected row count (optional)

        Returns:
            Dictionary with verification results
        """
        if not self.client:
            raise RuntimeError("Not connected to ClickHouse")

        # Get row count
        result = self.client.command(f"SELECT count() FROM {self.database}.{table}")
        actual_rows = int(result)

        # Get sample data
        sample_query = f"SELECT * FROM {self.database}.{table} LIMIT 5"
        sample = self.client.query(sample_query)

        verification = {
            "table": table,
            "row_count": actual_rows,
            "sample_rows": len(sample.result_rows),
        }

        if expected_rows is not None:
            verification["expected_rows"] = expected_rows
            verification["match"] = actual_rows == expected_rows

        logger.info(f"Verification: {verification}")
        return verification


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Load Parquet data into ClickHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=["raw_equity_ohlc", "normalized_equity_ohlc", "features_equity_indicators"],
        help="Target ClickHouse table",
    )

    parser.add_argument("--source", required=True, help="Path to Parquet file(s) or directory")

    parser.add_argument(
        "--host",
        default=os.getenv("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse host (default: localhost)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        help="ClickHouse HTTP port (default: 8123)",
    )

    parser.add_argument(
        "--user",
        default=os.getenv("CLICKHOUSE_USER", "champion_user"),
        help="ClickHouse user (default: champion_user)",
    )

    parser.add_argument(
        "--password",
        default=os.getenv("CLICKHOUSE_PASSWORD", "champion_pass"),
        help="ClickHouse password",
    )

    parser.add_argument(
        "--database",
        default=os.getenv("CLICKHOUSE_DATABASE", "champion_market"),
        help="ClickHouse database (default: champion_market)",
    )

    parser.add_argument(
        "--batch-size", type=int, default=100000, help="Batch size for inserts (default: 100000)"
    )

    parser.add_argument("--dry-run", action="store_true", help="Validate without loading data")

    parser.add_argument("--verify", action="store_true", help="Verify data after loading")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create loader
    loader = ClickHouseLoader(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    try:
        # Connect
        loader.connect()

        # Load data
        stats = loader.load_parquet_files(
            table=args.table,
            source_path=args.source,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )

        print("\n=== Load Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")

        # Verify if requested
        if args.verify and not args.dry_run:
            print("\n=== Verification ===")
            verification = loader.verify_load(table=args.table, expected_rows=stats["total_rows"])
            for key, value in verification.items():
                print(f"{key}: {value}")

        return 0

    except Exception as e:
        logger.error(f"Load failed: {e}")
        return 1

    finally:
        loader.disconnect()


if __name__ == "__main__":
    sys.exit(main())
