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
import time
from datetime import datetime
from pathlib import Path

try:
    import clickhouse_connect
    import numpy as np
    import pandas as pd
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
        "trading_calendar": "reference",
        "bulk_block_deals": "normalized",
        "corporate_actions": "reference",
        "symbol_master": "reference",
        "quarterly_financials": "reference",
        "quarterly_financials_raw": "reference",
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
        "quarterly_financials": {
            # Common alternate names from NSE master / ETL parquet -> ClickHouse column
            # Keep this conservative: rename obvious fields so inserts align to schema
            "symbol": "symbol",
            "company_name": "company_name",
            "cin": "cin",
            "period_end_date": "period_end_date",
            "period_type": "period_type",
            "statement_type": "statement_type",
            "filing_date": "filing_date",
            "revenue": "revenue",
            "operating_profit": "operating_profit",
            "net_profit": "net_profit",
            "total_assets": "total_assets",
            "total_liabilities": "total_liabilities",
            "eps": "eps",
            # ETL-level fields that sometimes exist — we'll pack any extras into `metadata`
            "period_start": "period_start",
            "audited": "audited",
            "consolidated": "consolidated",
            "xbrl": "xbrl",
        },
    }

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
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
        # Respect explicit args, otherwise fall back to environment variables
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.user = user or os.getenv("CLICKHOUSE_USER")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD")
        self.database = database or os.getenv("CLICKHOUSE_DATABASE")
        self.client: Client | None = None
        self.native_client = None

    def connect(self) -> None:
        """Establish connection to ClickHouse."""
        try:
            # If the configured port is the native ClickHouse TCP port (9000)
            # prefer the native protocol client. Otherwise use the HTTP client.
            if int(self.port) == 9000:
                # Attempt to create a native driver client for inserts (clickhouse native TCP)
                logger.info(f"Attempting native ClickHouse driver for {self.host}:{self.port}")
                try:
                    # Defer import to here so dependency is optional
                    from clickhouse_driver import Client as NativeClient

                    self.native_client = NativeClient(
                        host=self.host,
                        port=self.port,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                    )
                    logger.info("Native ClickHouse driver client created")
                except Exception as exc:
                    logger.debug(f"Native ClickHouse driver unavailable: {exc}")
                    self.native_client = None
                # If native client created successfully, prefer it and avoid attempting
                # an HTTP connection that may require different auth. Metadata queries
                # will fall back to using row keys if HTTP is unavailable.
                if self.native_client is not None:
                    logger.info("Using native ClickHouse client; skipping HTTP metadata client")
                else:
                    # Try to create an HTTP client for metadata queries (system.columns)
                    try:
                        self.client = clickhouse_connect.get_client(
                            host=self.host,
                            port=8123,
                            username=self.user,
                            password=self.password,
                            database=self.database,
                        )
                        logger.info(f"HTTP ClickHouse client for metadata at {self.host}:8123 created")
                    except Exception:
                        # If HTTP client cannot be created, leave as None and rely on fallbacks
                        logger.debug(
                            "HTTP metadata client could not be created; some metadata queries may fail"
                        )
            else:
                # Default to HTTP client (clickhouse-connect helper)
                logger.info(f"Using HTTP ClickHouse client for {self.host}:{self.port}")
                self.client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    database=self.database,
                )

            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")

            # Test connection using the available client
            try:
                if self.native_client is not None:
                    # native_client.execute returns rows; just run a lightweight query
                    try:
                        self.native_client.execute("SELECT 1")
                        logger.debug("Native client connection test succeeded")
                    except Exception:
                        logger.debug("Native client connection test failed; continuing")
                elif self.client is not None:
                    # Some client variants return results directly, others via a list of lines.
                    try:
                        resp = self.client.command("SELECT 1")
                        logger.debug(f"Connection test response: {resp}")
                    except Exception:
                        try:
                            # Attempt a lightweight query via alternate method
                            self.client.query("SELECT 1")
                        except Exception:
                            raise
                else:
                    # No client could be created; raise to notify caller
                    raise RuntimeError("No ClickHouse client available")
            except Exception:
                # Allow upper layers to handle connection failures
                raise

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

                # Read Parquet file (disable hive partition inference to avoid
                # merging partition schema with file schema which can cause
                # duplicate-field errors on some files)
                df = pl.read_parquet(file_path, hive_partitioning=False)
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

        # Debug: log pandas dtypes and sample types to help diagnose insertion issues
        try:
            logger.debug(f"Pandas dtypes: {pdf.dtypes.to_dict()}")
            if len(pdf) > 0:
                sample = pdf.head(1).to_dict(orient="records")[0]
                sample_types = {k: type(v).__name__ for k, v in sample.items()}
                logger.debug(f"Pandas sample types: {sample_types}")
        except Exception:
            pass

        total_rows = len(pdf)
        rows_inserted = 0

        # Insert in batches. Convert each pandas batch to list-of-dicts (native python types)
        # to avoid issues with numpy scalars and ensure clickhouse-connect serializes values correctly.
        for i in range(0, total_rows, batch_size):
            batch = pdf.iloc[i : i + batch_size]

            try:
                # Replace NaNs with None for ClickHouse NULLs
                batch_clean = batch.where(pd.notnull(batch), None)

                # Convert each row to native python types to avoid serialization issues
                # (numpy scalars, pandas timestamps, etc.) and then use insert_df.
                def _normalize_value(v):
                    try:
                        # numpy scalar -> native python
                        if isinstance(v, np.generic):
                            return v.item()
                    except Exception:
                        pass
                    try:
                        # pandas Timestamp -> python datetime
                        import pandas as _pd

                        if isinstance(v, _pd.Timestamp):
                            return v.to_pydatetime()
                        # pandas NA / missing -> None
                        try:
                            if _pd.isna(v):
                                return None
                        except Exception:
                            pass
                    except Exception:
                        pass
                    # numpy/pandas NA types or float NaN -> None
                    try:
                        if isinstance(v, float) and np.isnan(v):
                            return None
                    except Exception:
                        pass
                    return v

                # Convert to list-of-dicts, normalize values, then back to pandas DataFrame
                rows = batch_clean.to_dict(orient="records")
                normalized_rows = [{k: _normalize_value(v) for k, v in r.items()} for r in rows]
                # Insert using list-of-dicts to avoid pandas/clickhouse-connect dtype edge cases
                # Align row dicts to the ClickHouse table column order to avoid
                # "Insert data column count does not match column names" errors.
                if normalized_rows:
                    try:
                        # Fetch table columns in order from system.columns
                        import json

                        # Try to fetch table columns using whichever client is available.
                        columns = []
                        column_types = []
                        try:
                            if self.client is not None:
                                cols_q = (
                                    "SELECT name, type FROM system.columns "
                                    f"WHERE database = '{self.database}' AND table = '{table}' "
                                    "ORDER BY position FORMAT JSON"
                                )
                                cols_json = self.client.command(cols_q)
                                # clickhouse-connect may return a list of lines; join if so
                                if isinstance(cols_json, list):
                                    cols_json = "".join(cols_json)
                                cols_obj = json.loads(cols_json)
                                data = cols_obj.get("data", [])
                                for r in data:
                                    if isinstance(r, dict) and "name" in r:
                                        columns.append(r["name"])
                                        column_types.append(r.get("type"))
                                    elif isinstance(r, (list, tuple)) and len(r) > 1:
                                        columns.append(r[0])
                                        column_types.append(r[1])
                            elif self.native_client is not None:
                                # clickhouse_driver native client returns list of tuples
                                desc_rows = self.native_client.execute(
                                    f"SELECT name, type FROM system.columns WHERE database = '{self.database}' AND table = '{table}' ORDER BY position"
                                )
                                for r in desc_rows:
                                    if isinstance(r, dict) and "name" in r:
                                        columns.append(r["name"])
                                        column_types.append(r.get("type"))
                                    elif isinstance(r, (list, tuple)) and len(r) > 1:
                                        columns.append(r[0])
                                        column_types.append(r[1])
                            else:
                                raise RuntimeError("No metadata client available to fetch table columns")
                        except Exception as exc:
                            logger.debug(f"Failed to fetch table columns for {table}: {exc}")
                            # Fall back to using keys from first row if metadata lookup fails
                            columns = list(normalized_rows[0].keys())

                        if not columns:
                            # Fallback: use keys from first row
                            columns = list(normalized_rows[0].keys())

                        # Pack any extra fields (present in Parquet/CSV but not in table columns)
                        # into the `metadata` Map column to avoid data loss. Also coerce
                        # `consolidated` -> `statement_type` when helpful.
                        try:
                            import json as _json

                            table_cols_set = set(columns)
                            # Determine extra columns present in the input rows
                            extras = set()
                            if normalized_rows:
                                extras = set(normalized_rows[0].keys()) - table_cols_set

                            if extras and ("metadata" in table_cols_set or True):
                                for row in normalized_rows:
                                    # Initialize metadata from existing column if present
                                    metadata_val = {}
                                    if "metadata" in row and row.get("metadata"):
                                        try:
                                            if isinstance(row["metadata"], str):
                                                metadata_val = _json.loads(row["metadata"]) if row["metadata"] else {}
                                            elif isinstance(row["metadata"], dict):
                                                metadata_val = row["metadata"]
                                        except Exception:
                                            metadata_val = {"metadata_raw": str(row.get("metadata"))}

                                    # Move extras into metadata (stringify values)
                                    for extra in list(extras):
                                        if extra in row:
                                            v = row.pop(extra)
                                            # skip empty placeholders
                                            if v is None or (isinstance(v, str) and v.strip() == ""):
                                                continue
                                            try:
                                                metadata_val[extra] = v if isinstance(v, str) else str(v)
                                            except Exception:
                                                metadata_val[extra] = str(v)

                                    # If `consolidated` exists and statement_type missing, set it
                                    if "consolidated" in metadata_val and ("statement_type" not in row or not row.get("statement_type")):
                                        c = metadata_val.get("consolidated")
                                        if isinstance(c, bool):
                                            row["statement_type"] = "CONSOLIDATED" if c else "STANDALONE"
                                        else:
                                            sval = str(c).strip().lower()
                                            if sval in ("true", "yes", "1"):
                                                row["statement_type"] = "CONSOLIDATED"
                                            elif sval in ("false", "no", "0"):
                                                row["statement_type"] = "STANDALONE"

                                    # Assign back metadata as a dict (clickhouse-connect will serialize)
                                    row["metadata"] = metadata_val
                        except Exception:
                            # Be tolerant: if metadata packing fails, continue with original rows
                            logger.debug("Failed to pack extras into metadata; continuing without packing")

                    except Exception as exc:
                        logger.debug(f"Failed to fetch table columns for {table}: {exc}")
                        # If anything fails, fall back to using keys from first row
                        columns = list(normalized_rows[0].keys())

                    # Align each row to the column order, filling missing with None
                    aligned_rows = [
                        {col: row.get(col, None) for col in columns} for row in normalized_rows
                    ]

                    # If ClickHouse column type is non-Nullable, replace None with a
                    # sensible default for that column type to avoid insertion errors.
                    def _default_for_type(typ: str):
                        if typ is None:
                            # Unknown type: return empty-string default to avoid
                            # sending explicit None for non-nullable columns.
                            return ""
                        t = typ.lower()
                        # For complex container types, return sensible empty container defaults
                        if "map" in t or t.startswith("map("):
                            return {}
                        if "array" in t or t.startswith("array("):
                            return []
                        if "date" in t and "time" not in t:
                            # ClickHouse Date will be sent as integer days-since-epoch
                            return 0
                        if "datetime" in t or "timestamp" in t or "time" in t:
                            # DateTime/DateTime64 will be sent as milliseconds since epoch
                            return 0
                        if "int" in t or "uint" in t or "float" in t or "decimal" in t:
                            return 0
                        # default for strings
                        return ""

                    # Build tuples in column order, coercing defaults for non-nullable types
                    aligned_tuples = []
                    for row in aligned_rows:
                        tup = []
                        for col_idx, col in enumerate(columns):
                            val = row.get(col, None)
                            col_type = (
                                column_types[col_idx] if col_idx < len(column_types) else None
                            )
                            if (
                                val is None
                                and col_type
                                and "nullable(" not in col_type.lower()
                            ):
                                val = _default_for_type(col_type)
                            tup.append(val)
                        aligned_tuples.append(tuple(tup))

                    # Coerce values to types expected by ClickHouse to avoid runtime
                    # serialization errors (e.g., inserting int into String column)
                    def _coerce_value(val, typ):
                        if val is None:
                            return None
                        if typ is None:
                            return val
                        t = typ.lower()
                        try:
                            # Handle pandas NA and numpy nan/NA
                            import pandas as _pd

                            try:
                                if _pd.isna(val):
                                    return None
                            except Exception:
                                pass
                            # Handle Map and Array container types first to avoid
                            # matching 'string' inside e.g. 'array(string)'
                            if "map" in t or t.startswith("map("):
                                # Expect a dict-like object for Map columns
                                if isinstance(val, str):
                                    try:
                                        import json

                                        return json.loads(val)
                                    except Exception:
                                        return {}
                                if isinstance(val, dict):
                                    return val
                                return {}
                            if "array" in t or t.startswith("array("):
                                # Expect list/tuple for Array columns
                                if isinstance(val, str):
                                    try:
                                        import json

                                        parsed = json.loads(val)
                                        return (
                                            list(parsed)
                                            if isinstance(parsed, list | tuple)
                                            else [parsed]
                                        )
                                    except Exception:
                                        return []
                                if isinstance(val, list | tuple):
                                    return list(val)
                                return []
                            if "string" in t or "varchar" in t or "text" in t:
                                return str(val)
                            if "int" in t or "uint" in t:
                                try:
                                    return int(val)
                                except Exception:
                                    return _default_for_type(typ)
                            if "float" in t or "decimal" in t:
                                return float(val)
                            if "date" in t and "time" not in t:
                                # convert to days since epoch (int)
                                from datetime import date as _date

                                if isinstance(val, int):
                                    # YYYYMMDD -> convert to days since epoch
                                    v = int(val)
                                    if v > 10000000:
                                        s = str(v)
                                        try:
                                            d = datetime.strptime(s, "%Y%m%d").date()
                                            return (d - _date(1970, 1, 1)).days
                                        except Exception:
                                            return _default_for_type(typ)
                                    return int(val)
                                # numpy integer types
                                if isinstance(val, np.integer):
                                    return int(val)
                                if isinstance(val, _date):
                                    return (val - _date(1970, 1, 1)).days
                                if isinstance(val, datetime):
                                    return (val.date() - _date(1970, 1, 1)).days
                                try:
                                    # parse string date
                                    s = str(val).strip()
                                    if s == "":
                                        return _default_for_type(typ)
                                    d = datetime.strptime(s, "%Y-%m-%d").date()
                                    return (d - _date(1970, 1, 1)).days
                                except Exception:
                                    return _default_for_type(typ)
                            if "datetime" in t or "timestamp" in t or "time" in t:
                                # convert to milliseconds since epoch (int)
                                if isinstance(val, datetime):
                                    return int(val.timestamp() * 1000)
                                try:
                                    # pandas Timestamp or string
                                    s = str(val).strip()
                                    if s == "":
                                        return _default_for_type(typ)
                                    return int(datetime.fromisoformat(s).timestamp() * 1000)
                                except Exception:
                                    try:
                                        # numeric epoch (seconds or ms)
                                        v = int(val)
                                        # heuristics: if > 1e12 assume ms, if >1e9 assume seconds
                                        if v > 1_000_000_000_000:
                                            return v
                                        if v > 1_000_000_000:
                                            return v * 1000
                                        return v * 1000
                                    except Exception:
                                        return _default_for_type(typ)
                        except Exception:
                            return val
                        return val

                    # Apply coercion
                    coerced_tuples = []
                    for tup in aligned_tuples:
                        coerced = []
                        for idx, v in enumerate(tup):
                            typ = column_types[idx] if idx < len(column_types) else None
                            coerced.append(_coerce_value(v, typ))
                        coerced_tuples.append(tuple(coerced))

                    aligned_tuples = coerced_tuples

                    logger.info(f"Inserting into {table} with columns={columns}")
                    logger.info(
                        f"Sample aligned row (tuple): {aligned_tuples[0] if aligned_tuples else None}"
                    )

                    try:
                        # If a native client exists, use it for bulk inserts (native TCP)
                        if self.native_client is not None:
                            try:
                                # clickhouse_driver accepts execute with data as list of tuples
                                insert_stmt = f"INSERT INTO {self.database}.{table} ({', '.join(columns)}) VALUES"
                                self.native_client.execute(insert_stmt, aligned_tuples)
                            except Exception as exc:
                                logger.error(f"Native ClickHouse insert failed: {repr(exc)}")
                                raise
                        else:
                            # For HTTP client, prefer list-of-dicts to preserve complex types
                            try:
                                # Build list-of-dicts from coerced tuples and sanitize values
                                aligned_dicts = [dict(zip(columns, tup)) for tup in aligned_tuples]

                                # Log sample aligned dict (sanitized) at INFO for diagnostics
                                try:
                                    logger.info("Sanitized aligned dict sample for insert: %s", aligned_dicts[0] if aligned_dicts else None)
                                except Exception:
                                    pass

                                # Ensure non-Nullable columns have sensible defaults and
                                # `metadata` is a Map(String,String) without None values.
                                # Build a column->type map for robust lookups
                                col_type_map = {columns[i]: column_types[i] if i < len(column_types) else None for i in range(len(columns))}

                                def _deep_stringify(obj):
                                    """Recursively convert metadata to a dict[str,str] where all values
                                    are strings. Nested dicts/lists are JSON-dumped; None/NA -> "".
                                    """
                                    import json as _json
                                    import pandas as _pd
                                    import numpy as _np

                                    def _is_missing(x):
                                        try:
                                            if x is None:
                                                return True
                                            try:
                                                if _pd.isna(x):
                                                    return True
                                            except Exception:
                                                pass
                                            if isinstance(x, float) and _np.isnan(x):
                                                return True
                                        except Exception:
                                            pass
                                        return False

                                    # Primitive missing -> empty string
                                    if _is_missing(obj):
                                        return ""

                                    # If it's a dict, produce a flat dict[str,str]
                                    if isinstance(obj, dict):
                                        out = {}
                                        for k, v in obj.items():
                                            key = str(k)
                                            if _is_missing(v):
                                                out[key] = ""
                                            elif isinstance(v, dict):
                                                # Dump nested dict as JSON string to ensure Map(String,String)
                                                try:
                                                    # sanitize nested None values
                                                    sanitized = {str(kk): ("" if _is_missing(vv) else vv) for kk, vv in v.items()}
                                                    out[key] = _json.dumps(sanitized, ensure_ascii=False)
                                                except Exception:
                                                    out[key] = str(v)
                                            elif isinstance(v, (list, tuple)):
                                                try:
                                                    lst = ["" if _is_missing(i) else i for i in v]
                                                    out[key] = _json.dumps(lst, ensure_ascii=False)
                                                except Exception:
                                                    out[key] = str(v)
                                            else:
                                                out[key] = str(v)
                                        return out

                                    # Lists/tuples -> JSON string
                                    if isinstance(obj, (list, tuple)):
                                        try:
                                            lst = ["" if _is_missing(i) else i for i in obj]
                                            return _json.dumps(lst, ensure_ascii=False)
                                        except Exception:
                                            return str(obj)

                                    # Fallback: stringify
                                    return str(obj)

                                for row in aligned_dicts:
                                    now_ms = int(datetime.utcnow().timestamp() * 1000)

                                    # If envelope timestamps are missing, set current epoch ms
                                    if "event_time" in row and row.get("event_time") is None:
                                        row["event_time"] = now_ms
                                    if "ingest_time" in row and row.get("ingest_time") is None:
                                        row["ingest_time"] = now_ms

                                    # Ensure every row has all columns present. Use type-aware defaults
                                    # so list-of-dicts have a consistent key set for clickhouse-connect.
                                    for col in columns:
                                        val = row.get(col, None)
                                        typ = col_type_map.get(col)
                                        if val is None:
                                            # Coerce missing/nonetype to a sensible default (avoid explicit None)
                                            if typ and "nullable(" not in (typ or "").lower():
                                                row[col] = _default_for_type(typ)
                                            else:
                                                # keep None for nullable columns
                                                row[col] = None

                                    # Normalize and sanitize metadata to Map(String,String)
                                    if "metadata" in row:
                                        m = row.get("metadata")
                                        try:
                                            # Use the aggressive deep-stringify helper defined above
                                            if isinstance(m, str):
                                                import json as _json

                                                parsed = _json.loads(m) if m else {}
                                                row["metadata"] = _deep_stringify(parsed)
                                            else:
                                                row["metadata"] = _deep_stringify(m)
                                        except Exception:
                                            # Fallback: coerce to a safe string-based dict
                                            row["metadata"] = {"metadata_raw": str(m) if m is not None else ""}

                                    # If ClickHouse expects a plain String for metadata (not Map),
                                    # serialize the dict as JSON to avoid Map serialization errors.
                                    try:
                                        if "metadata" in columns:
                                            meta_idx = columns.index("metadata")
                                            meta_typ = (
                                                column_types[meta_idx]
                                                if meta_idx < len(column_types)
                                                else None
                                            )
                                            if meta_typ and (
                                                "string" in meta_typ.lower()
                                                or "varchar" in meta_typ.lower()
                                                or "text" in meta_typ.lower()
                                            ):
                                                import json as _json

                                                if isinstance(row.get("metadata"), dict):
                                                    row["metadata"] = _json.dumps(
                                                        {k: ("" if v is None else str(v)) for k, v in row["metadata"].items()},
                                                        ensure_ascii=False,
                                                    )
                                                else:
                                                    row["metadata"] = str(row.get("metadata", ""))
                                    except Exception:
                                        # tolerant fallback: leave metadata as-is
                                        pass

                                # Pre-insert validation: ensure no non-Nullable column has None
                                # and `metadata` (Map(String,String)) contains only string values.
                                offenses: list[tuple[int, str, object]] = []
                                meta_idx = None
                                meta_typ = None
                                if "metadata" in columns:
                                    meta_idx = columns.index("metadata")
                                    if meta_idx < len(column_types):
                                        meta_typ = column_types[meta_idx]

                                for ridx, r in enumerate(aligned_dicts):
                                    for col in columns:
                                        typ = col_type_map.get(col)
                                        val = r.get(col)
                                        if val is None and typ and "nullable(" not in (typ or "").lower():
                                            offenses.append((ridx, col, val))
                                    # Validate metadata map values when expected to be Map(String,String)
                                    if meta_idx is not None and meta_typ and ("map" in (meta_typ or "").lower()):
                                        mval = r.get("metadata")
                                        if not isinstance(mval, dict):
                                            offenses.append((ridx, "metadata", mval))
                                        else:
                                            for mk, mv in mval.items():
                                                if mv is None or not isinstance(mv, str):
                                                    offenses.append((ridx, f"metadata.{mk}", mv))

                                if offenses:
                                    # Log a concise sample of offending rows and raise a clear error
                                    logger.error("Pre-insert validation failed: offending values found in non-nullable columns or metadata")
                                    for off in offenses[:10]:
                                        logger.error("Offense sample - row=%s column=%s value=%s", off[0], off[1], off[2])
                                    raise ValueError("Pre-insert validation failed: non-nullable columns contain None or metadata contains invalid values")

                                self.client.insert(table=table, data=aligned_dicts)
                            except Exception:
                                # Fallback to previous behaviour if list-of-dicts fails
                                self.client.insert(
                                    table=table, data=aligned_tuples, column_names=columns
                                )
                    except Exception as exc:
                        logger.error(f"ClickHouse insert failed: {repr(exc)}")
                        raise
                rows_inserted += len(batch_clean)

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

            # Only rename columns that exist in the DataFrame. Match mapping
            # keys against DataFrame columns in a case-insensitive manner so
            # common variations (e.g., 'Symbol' vs 'symbol') are handled.
            df_cols_lower = {col.lower(): col for col in df.columns}
            rename_map: dict[str, str] = {}
            for source_col, target_col in mapping.items():
                # Exact match preferred
                if source_col in df.columns:
                    rename_map[source_col] = target_col
                    logger.debug(f"Mapping column: {source_col} -> {target_col}")
                else:
                    # Case-insensitive fallback
                    lc = source_col.lower()
                    if lc in df_cols_lower:
                        actual_col = df_cols_lower[lc]
                        rename_map[actual_col] = target_col
                        logger.debug(f"Mapping column (case-insensitive): {actual_col} -> {target_col}")

            if rename_map:
                df = df.rename(rename_map)
                logger.info(f"Applied column name mappings: {len(rename_map)} columns renamed")

        # Add missing instrumentation columns used by ClickHouse tables
        # e.g., add `event_time` if missing to satisfy schema requirements
        if "event_time" not in df.columns:
            try:
                df = df.with_columns(pl.lit(datetime.utcnow()).alias("event_time"))
                logger.debug("Added missing column 'event_time' with current timestamp")
            except Exception:
                # If polars conversion fails, skip and let validation detect missing column
                logger.debug("Failed to add 'event_time' column via polars; validation may fail")

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
                # If dates are encoded as integers (YYYYMMDD), convert to Date
                if df[col].dtype in [pl.Int64, pl.Int32]:
                    try:
                        df = df.with_columns(
                            pl.col(col)
                            .cast(pl.Utf8)
                            .str.strptime(pl.Date, format="%Y%m%d")
                            .alias(col)
                        )
                    except (pl.ComputeError, ValueError) as e:
                        logger.warning(f"Failed to parse integer date column {col}: {e}")
                        pass
                elif df[col].dtype == pl.Utf8:
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
        # Implementation omitted for brevity in this file excerpt
        return {}

    def insert_polars_dataframe(
        self,
        table: str,
        df: pl.DataFrame,
        batch_size: int = 100000,
        dry_run: bool = False,
    ) -> int:
        """
        Insert a Polars DataFrame directly into a ClickHouse table.

        This is a convenience wrapper used by ETL flows to stream parsed data
        into ClickHouse without writing Parquet first.
        """
        if table not in self.SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table}")

        # Validate input
        if not isinstance(df, pl.DataFrame):
            raise ValueError("df must be a Polars DataFrame")

        rows = len(df)
        if rows == 0:
            logger.info(f"insert_polars_dataframe_skipped_empty table={table}")
            return 0

        if dry_run:
            logger.info(f"insert_polars_dataframe_dry_run table={table} rows={rows}")
            return 0

        # Ensure connection (with retry)
        if not self.client:
            try:
                self.connect()
            except Exception as e:
                logger.error(f"clickhouse_connect_failed table={table} error={e}")
                raise

        # Delegate to existing insert routine with simple retry
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                rows_inserted = self._insert_dataframe(df=df, table=table, batch_size=batch_size)
                logger.info(f"insert_polars_dataframe_complete table={table} rows={rows_inserted}")
                return rows_inserted
            except Exception as e:
                logger.warning(
                    f"insert_polars_dataframe_attempt_failed table={table} attempt={attempt} error={e}"
                )
                if attempt < attempts:
                    time.sleep(1 * attempt)
                    continue
                logger.error(f"insert_polars_dataframe_failed table={table} error={e}")
                raise


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
        default=os.getenv("CLICKHOUSE_USER"),
        help="ClickHouse user (from CLICKHOUSE_USER env)",
    )

    parser.add_argument(
        "--password",
        default=os.getenv("CLICKHOUSE_PASSWORD"),
        help="ClickHouse password (from CLICKHOUSE_PASSWORD env)",
    )

    parser.add_argument(
        "--database",
        default=os.getenv("CLICKHOUSE_DATABASE"),
        help="ClickHouse database (from CLICKHOUSE_DATABASE env)",
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
