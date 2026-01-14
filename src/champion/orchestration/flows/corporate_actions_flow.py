"""Prefect flow for NSE corporate actions ETL."""

import os
from datetime import date
from pathlib import Path

import mlflow
import structlog
from prefect import flow, task

from champion.config import config
from champion.scrapers.nse.corporate_actions import CorporateActionsScraper

logger = structlog.get_logger()

# Ensure file-based MLflow by default
os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")


@task(
    name="scrape-corporate-actions",
    retries=2,
    retry_delay_seconds=30,
)
def scrape_ca_task(effective_date: date | None = None) -> Path:
    """Scrape NSE corporate actions data.

    Args:
        effective_date: Date to scrape for (used for logging; scraper doesn't filter by date)

    Returns:
        Path to downloaded JSON file
    """
    scraper = CorporateActionsScraper()
    logger.info("scraping_corporate_actions", effective_date=effective_date)

    # If an effective_date is provided, request that single date range from NSE
    if effective_date:
        from_date = effective_date.strftime("%d-%m-%Y")
        to_date = effective_date.strftime("%d-%m-%Y")
        scraper.scrape(from_date=from_date, to_date=to_date, dry_run=False)
    else:
        # Default scraper will fetch recent window (90 days)
        scraper.scrape(dry_run=False)

    ca_dir = Path(config.storage.data_dir) / "lake" / "reference" / "corporate_actions"
    ca_dir.mkdir(parents=True, exist_ok=True)
    return ca_dir


@task(
    name="parse-corporate-actions",
    retries=1,
    retry_delay_seconds=10,
)
def parse_ca_task(ca_dir: Path, effective_date: date | None = None) -> dict:
    """Parse corporate actions data from directory.

    Args:
        ca_dir: Directory where corporate actions data would be
        effective_date: Effective date for events

    Returns:
        Dictionary with parsed events and metadata (empty for stub implementation)
    """
    import json
    from datetime import datetime

    eff_date = effective_date or date.today()
    logger.info("parsing_corporate_actions", effective_date=eff_date)

    events: list[dict] = []

    # Find all JSON files saved by the scraper
    json_files = sorted(ca_dir.glob("*.json"))
    for jf in json_files:
        try:
            with open(jf, "r") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("failed_to_load_json", path=str(jf), error=str(e))
            continue

        if not isinstance(data, list):
            # If API returns an object with a key containing list, try to extract
            if isinstance(data, dict):
                # Common key might be 'data' or 'corporates'
                for key in ("data", "corporates", "result", "items"):
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break

        for item in data:
            # Parse exDate/recDate formats like '16-Oct-2025'
            ex = item.get("exDate") or item.get("EX-DATE") or item.get("ex_date")
            if not ex:
                continue
            parsed_ex = None
            for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    parsed_ex = datetime.strptime(ex.strip(), fmt).date()
                    break
                except Exception:
                    continue
            if parsed_ex is None:
                continue

            # Only include events matching the effective_date (single-day backfill semantics)
            if parsed_ex != eff_date:
                continue

            # Map fields from API to expected event keys
            ev = {
                "SYMBOL": item.get("symbol"),
                "COMPANY": item.get("comp") or item.get("company"),
                "SERIES": item.get("series"),
                "FACE VALUE": item.get("faceVal"),
                "PURPOSE": item.get("subject") or item.get("purpose"),
                "EX-DATE": ex,
                "RECORD DATE": item.get("recDate"),
                "BC START DATE": item.get("bcStartDate"),
                "BC END DATE": item.get("bcEndDate"),
                "ND START DATE": item.get("ndStartDate"),
                "ND END DATE": item.get("ndEndDate"),
                "ACTUAL PAYMENT DATE": item.get("caBroadcastDate"),
                "ISIN": item.get("isin"),
            }
            events.append(ev)

    result = {
        "events": events,
        "count": len(events),
        "effective_date": eff_date.isoformat(),
        "status": "ok",
    }
    logger.info("parsed_corporate_actions", **{"count": result["count"], "effective_date": result["effective_date"]})
    return result


@task(
    name="write-corporate-actions-parquet",
    retries=1,
    retry_delay_seconds=10,
)
def write_ca_parquet_task(parse_result: dict, output_base_path: str | None = None) -> str:
    """Write parsed corporate actions to Parquet.

    Args:
        parse_result: Output from parse_ca_task
        output_base_path: Base path for data lake

    Returns:
        Path to written Parquet file
    """
    import polars as pl
    import time
    import uuid

    from champion.storage.parquet_io import write_df_safe

    events = parse_result["events"]
    if not events:
        logger.warning("no_corporate_actions_to_write")
        return ""

    output_base = Path(output_base_path or config.storage.data_dir) / "lake"
    output_dir = output_base / "reference" / "corporate_actions"
    output_dir.mkdir(parents=True, exist_ok=True)

    eff_date = parse_result["effective_date"]

    # Normalize event keys to expected schema field names
    normalized: list[dict] = []
    ingest_ms = int(time.time() * 1000)
    for ev in events:
        symbol = ev.get("SYMBOL") or ev.get("symbol")
        company = ev.get("COMPANY") or ev.get("company")
        series = ev.get("SERIES") or ev.get("series")
        face_value = ev.get("FACE VALUE") or ev.get("face_value") or ev.get("faceVal")
        purpose = ev.get("PURPOSE") or ev.get("purpose") or ev.get("subject")
        ex_date_raw = ev.get("EX-DATE") or ev.get("ex_date")
        record_date = ev.get("RECORD DATE") or ev.get("record_date") or ev.get("recDate")
        bc_start = ev.get("BC START DATE") or ev.get("bc_start_date")
        bc_end = ev.get("BC END DATE") or ev.get("bc_end_date")
        nd_start = ev.get("ND START DATE") or ev.get("nd_start_date")
        nd_end = ev.get("ND END DATE") or ev.get("nd_end_date")
        actual_payment = ev.get("ACTUAL PAYMENT DATE") or ev.get("actual_payment_date") or ev.get("caBroadcastDate")
        isin = ev.get("ISIN") or ev.get("isin")

        # Normalize ex_date to ISO string if it's a date object
        try:
            if hasattr(ex_date_raw, "isoformat"):
                ex_date = ex_date_raw.isoformat()
            else:
                ex_date = str(ex_date_raw) if ex_date_raw is not None else None
        except Exception:
            ex_date = str(ex_date_raw)

        row = {
            "event_id": ev.get("event_id") or str(uuid.uuid4()),
            "event_time": ev.get("event_time") or ingest_ms,
            "ingest_time": ev.get("ingest_time") or ingest_ms,
            "source": ev.get("source") or "nse_corporate_actions",
            "schema_version": ev.get("schema_version") or "v1",
            "entity_id": (symbol + ":NSE") if symbol else None,
            "symbol": symbol,
            "company": company,
            "series": series,
            "face_value": face_value,
            "purpose": purpose,
            "ex_date": ex_date,
            "record_date": record_date,
            "bc_start_date": bc_start,
            "bc_end_date": bc_end,
            "nd_start_date": nd_start,
            "nd_end_date": nd_end,
            "actual_payment_date": actual_payment,
            "isin": isin,
            "action_type": ev.get("action_type"),
            "adjustment_factor": ev.get("adjustment_factor"),
            "split_ratio": ev.get("split_ratio"),
            "bonus_ratio": ev.get("bonus_ratio"),
        }
        normalized.append(row)

    df = pl.DataFrame(normalized)

    # Normalize types and compute derived fields using the dedicated parser

    # Local helper functions (avoid instantiating abstract parser)
    import re
    from datetime import datetime as _dt

    action_type_patterns = {
        "SPLIT": [r"split", r"sub-division", r"subdivision"],
        "BONUS": [r"bonus", r"capitalisation"],
        "DIVIDEND": [r"dividend", r"div"],
        "RIGHTS": [r"rights"],
        "INTEREST_PAYMENT": [r"interest"],
        "EGMMEETING": [r"egm", r"agm", r"meeting"],
        "DEMERGER": [r"demerger", r"de-merger"],
        "MERGER": [r"merger", r"amalgamation"],
        "BUYBACK": [r"buy-back", r"buyback"],
    }

    def _parse_action_type(purpose: str | None) -> str | None:
        if not purpose:
            return None
        p = purpose.lower()
        for action, patterns in action_type_patterns.items():
            for pat in patterns:
                if re.search(pat, p):
                    return action
        return "OTHER"

    def _parse_date(s: str | None):
        if not s or s.strip() in ("", "-"):
            return None
        s = s.strip()
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return _dt.strptime(s, fmt).date()
            except Exception:
                continue
        return None

    def _parse_split_ratio(purpose: str | None):
        if not purpose:
            return None
        m = re.search(r"rs\.?\s*(\d+(?:\.\d+)?)\s*/?\-?\s*to\s*rs\.?\s*(\d+(?:\.\d+)?)", purpose.lower())
        if m:
            old_fv = float(m.group(1))
            new_fv = float(m.group(2))
            ratio = old_fv / new_fv if new_fv != 0 else None
            if ratio:
                return {"old_shares": 1, "new_shares": int(ratio)}
        return None

    def _parse_bonus_ratio(purpose: str | None):
        if not purpose:
            return None
        m = re.search(r"(\d+)\s*(?:[:]\s*|for\s+)(\d+)", purpose.lower())
        if m:
            return {"new_shares": int(m.group(1)), "existing_shares": int(m.group(2))}
        return None

    def _compute_adjustment(action_type: str | None, purpose: str | None) -> float | None:
        try:
            if action_type == "SPLIT":
                r = _parse_split_ratio(purpose or "")
                if r:
                    return r["new_shares"] / r["old_shares"]
            if action_type == "BONUS":
                r = _parse_bonus_ratio(purpose or "")
                if r:
                    return (r["existing_shares"] + r["new_shares"]) / r["existing_shares"]
        except Exception:
            return None
        return None

    # Parse and coerce fields
    df = df.with_columns(
        [
            pl.col("face_value").map_elements(lambda v: float(v) if v not in (None, "", "-") else None, return_dtype=pl.Float64).alias("face_value"),
            pl.col("ex_date").map_elements(_parse_date, return_dtype=pl.Date).alias("ex_date_parsed"),
            pl.col("record_date").map_elements(_parse_date, return_dtype=pl.Date).alias("record_date_parsed"),
        ]
    )

    df = df.with_columns(pl.col("purpose").map_elements(_parse_action_type, return_dtype=pl.Utf8).alias("action_type"))
    df = df.with_columns(pl.struct(["action_type", "purpose"]).map_elements(lambda r: _compute_adjustment(r.get("action_type"), r.get("purpose")), return_dtype=pl.Float64).alias("adjustment_factor"))

    df = df.with_columns([
        pl.col("ex_date_parsed").dt.strftime("%Y-%m-%d").alias("ex_date"),
        pl.col("ex_date_parsed").dt.year().alias("year"),
        pl.col("ex_date_parsed").dt.month().alias("month"),
        pl.col("ex_date_parsed").dt.day().alias("day"),
    ])

    # Drop intermediate parsed date columns
    if "ex_date_parsed" in df.columns:
        df = df.drop("ex_date_parsed")
    if "record_date_parsed" in df.columns:
        df = df.drop("record_date_parsed")

    # Use validated writer to ensure produced Parquet conforms to schema
    output_path = write_df_safe(
        df=df,
        dataset="reference/corporate_actions",
        base_path=output_base,
        schema_name="corporate_actions",
        schema_dir="schemas/parquet",
        compression="snappy",
        fail_on_validation_errors=True,
        quarantine_dir=output_base / "quarantine",
    )

    out_file = output_path / "data.parquet"
    logger.info("wrote_corporate_actions", path=str(out_file), count=len(df))
    return str(out_file)


@flow(name="corporate-actions-etl", log_prints=True)
def corporate_actions_etl_flow(
    effective_date: date | None = None,
    output_base_path: str | None = None,
    load_to_clickhouse: bool = False,
) -> dict:
    """ETL flow for NSE corporate actions data.

    Args:
        effective_date: Date to process (defaults to today)
        output_base_path: Base path for data lake
        load_to_clickhouse: Whether to load to ClickHouse (stub for now)

    Returns:
        Dictionary with pipeline statistics
    """
    eff_date = effective_date or date.today()

    logger.info(
        "starting_corporate_actions_etl_flow",
        effective_date=eff_date,
        load_to_clickhouse=load_to_clickhouse,
    )

    # Start MLflow run
    mlflow.set_experiment("corporate-actions-etl")
    with mlflow.start_run(run_name=f"ca-etl-{eff_date}"):
        mlflow.log_param("effective_date", str(eff_date))
        mlflow.log_param("load_to_clickhouse", load_to_clickhouse)

        try:
            # Step 1: Scrape
            ca_dir = scrape_ca_task(eff_date)
            mlflow.log_param("ca_directory", str(ca_dir))

            # Step 2: Parse
            parse_result = parse_ca_task(ca_dir, eff_date)
            mlflow.log_metric("events_parsed", parse_result["count"])

            # Step 3: Write to Parquet
            parquet_path = write_ca_parquet_task(parse_result, output_base_path)
            mlflow.log_param("parquet_path", parquet_path)

            # Step 4: (Optional) Load to ClickHouse
            if load_to_clickhouse and parquet_path:
                try:
                    from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

                    loader = ClickHouseLoader()
                    loader.connect()
                    stats = loader.load_parquet_files(table="corporate_actions", source_path=parquet_path)
                    mlflow.log_metric("clickhouse_rows", stats.get("total_rows", 0))
                    loader.disconnect()
                except Exception as e:
                    logger.warning("clickhouse_load_failed", error=str(e), path=parquet_path)
                    mlflow.log_param("clickhouse_error", str(e))

            result = {
                "status": "completed",
                "effective_date": str(eff_date),
                "events_processed": parse_result["count"],
                "parquet_path": parquet_path,
                "note": parse_result.get("status", ""),
            }

            logger.info("corporate_actions_etl_flow_complete", **result)
            return result

        except Exception as e:
            logger.error("corporate_actions_etl_flow_failed", error=str(e))
            mlflow.log_param("error", str(e))
            raise
