"""Bhavcopy CSV parser."""

import csv
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger
from src.utils.metrics import rows_parsed

logger = get_logger(__name__)


class BhavcopyParser:
    """Parser for NSE CM Bhavcopy CSV files."""

    def parse(self, file_path: Path, trade_date: date) -> list[dict[str, Any]]:
        """Parse bhavcopy CSV file into event structures.

        Args:
            file_path: Path to CSV file
            trade_date: Trading date

        Returns:
            List of event dictionaries ready for Kafka
        """
        logger.info("Parsing bhavcopy file", path=str(file_path))

        events = []

        try:
            with open(file_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for row in reader:
                    try:
                        event = self._row_to_event(row, trade_date)
                        if event:
                            events.append(event)
                            rows_parsed.labels(scraper="bhavcopy", status="success").inc()
                    except Exception as e:
                        logger.error("Failed to parse row", row=row, error=str(e))
                        rows_parsed.labels(scraper="bhavcopy", status="failed").inc()

        except Exception as e:
            logger.error("Failed to read CSV file", path=str(file_path), error=str(e))
            raise

        logger.info("Parsed bhavcopy file", path=str(file_path), events=len(events))
        return events

    def _row_to_event(self, row: dict[str, str], trade_date: date) -> dict[str, Any] | None:
        """Convert CSV row to event structure.

        Args:
            row: CSV row as dictionary
            trade_date: Trading date

        Returns:
            Event dictionary with envelope and payload, or None if row should be skipped
        """
        # Skip if symbol is empty
        symbol = row.get("TckrSymb", "").strip()
        if not symbol:
            return None

        # Generate deterministic event_id
        event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"nse_cm_bhavcopy:{trade_date}:{symbol}"))

        # Build event envelope
        event_time = int(datetime.combine(trade_date, datetime.min.time()).timestamp() * 1000)
        ingest_time = int(datetime.now().timestamp() * 1000)

        event = {
            "event_id": event_id,
            "event_time": event_time,
            "ingest_time": ingest_time,
            "source": "nse_cm_bhavcopy",
            "schema_version": "v1",
            "entity_id": f"{symbol}:NSE",
            "payload": self._build_payload(row),
        }

        return event

    def _build_payload(self, row: dict[str, str]) -> dict[str, Any]:
        """Build payload from CSV row.

        Args:
            row: CSV row as dictionary

        Returns:
            Payload dictionary matching Avro schema
        """

        def safe_str(value: str | None) -> str | None:
            """Get string value or None."""
            val = value.strip() if value else None
            return val if val and val != "-" else None

        def safe_float(value: str | None) -> float | None:
            """Get float value or None."""
            try:
                return (
                    float(value.strip())
                    if value and value.strip() and value.strip() != "-"
                    else None
                )
            except (ValueError, AttributeError):
                return None

        def safe_int(value: str | None) -> int | None:
            """Get int value or None."""
            try:
                return (
                    int(float(value.strip()))
                    if value and value.strip() and value.strip() != "-"
                    else None
                )
            except (ValueError, AttributeError):
                return None

        return {
            "TradDt": safe_str(row.get("TradDt")),
            "BizDt": safe_str(row.get("BizDt")),
            "Sgmt": safe_str(row.get("Sgmt")),
            "Src": safe_str(row.get("Src")),
            "FinInstrmTp": safe_str(row.get("FinInstrmTp")),
            "FinInstrmId": safe_int(row.get("FinInstrmId")),
            "ISIN": safe_str(row.get("ISIN")),
            "TckrSymb": safe_str(row.get("TckrSymb")),
            "SctySrs": safe_str(row.get("SctySrs")),
            "XpryDt": safe_str(row.get("XpryDt")),
            "FininstrmActlXpryDt": safe_str(row.get("FininstrmActlXpryDt")),
            "StrkPric": safe_float(row.get("StrkPric")),
            "OptnTp": safe_str(row.get("OptnTp")),
            "FinInstrmNm": safe_str(row.get("FinInstrmNm")),
            "OpnPric": safe_float(row.get("OpnPric")),
            "HghPric": safe_float(row.get("HghPric")),
            "LwPric": safe_float(row.get("LwPric")),
            "ClsPric": safe_float(row.get("ClsPric")),
            "LastPric": safe_float(row.get("LastPric")),
            "PrvsClsgPric": safe_float(row.get("PrvsClsgPric")),
            "UndrlygPric": safe_float(row.get("UndrlygPric")),
            "SttlmPric": safe_float(row.get("SttlmPric")),
            "OpnIntrst": safe_int(row.get("OpnIntrst")),
            "ChngInOpnIntrst": safe_int(row.get("ChngInOpnIntrst")),
            "TtlTradgVol": safe_int(row.get("TtlTradgVol")),
            "TtlTrfVal": safe_float(row.get("TtlTrfVal")),
            "TtlNbOfTxsExctd": safe_int(row.get("TtlNbOfTxsExctd")),
            "SsnId": safe_str(row.get("SsnId")),
            "NewBrdLotQty": safe_int(row.get("NewBrdLotQty")),
            "Rmks": safe_str(row.get("Rmks")),
            "Rsvd01": safe_str(row.get("Rsvd01")),
            "Rsvd02": safe_str(row.get("Rsvd02")),
            "Rsvd03": safe_str(row.get("Rsvd03")),
            "Rsvd04": safe_str(row.get("Rsvd04")),
        }
