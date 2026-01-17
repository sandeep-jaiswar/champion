from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import os
import time
import httpx
import pandas as pd
from urllib.parse import quote_plus

from champion.utils.logger import get_logger

logger = get_logger(__name__)


"""Fetch NSE quarterly results master CSV and download referenced XBRL/XML files.

Usage:
    scraper = QuarterlyResultsScraper()
    master = scraper.get_master('16-10-2025', '16-01-2026')
    saved = scraper.download_documents(master, out_dir)

Notes:
- By default the master CSV is written to `data/tmp` during normal runs.
- When running in CI (environment variable `CI` is set) the master CSV will be
  written to the repository `data/` folder so CI artifacts can pick it up.
"""


class QuarterlyResultsScraper:
    MASTER_URL = (
        "https://www.nseindia.com/api/corporates-financial-results"
        "?index=equities&from_date={from_date}&to_date={to_date}&period={period}&csv=true"
    )

    def __init__(self, user_agent: Optional[str] = None) -> None:
        self.user_agent = (
            user_agent
            or "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        )

    def get_master(
        self,
        from_date: str,
        to_date: str,
        period: str = "Quarterly",
        out_dir: Optional[Path] = None,
        symbol: Optional[str] = None,
        issuer: Optional[str] = None,
        filter_audited: bool = False,
        retries: int = 0,
        backoff: float = 1.0,
    ) -> pd.DataFrame:
        """Return a pandas DataFrame parsed from the NSE CSV master.

        Dates must be in DD-MM-YYYY format as required by NSE CSV endpoint.

        If `out_dir` is not provided the method writes the CSV to:
        - `data/` when running in CI (os.environ['CI'] set to truthy value),
        - otherwise `data/tmp/` for local runs.

        When `symbol` is provided the endpoint returns JSON and the method will
        attempt to parse JSON into a DataFrame. If `filter_audited` is True the
        DataFrame will be filtered to rows where the audited column equals
        'Audited' (case-insensitive) when such a column exists.
        """
        headers = {"User-Agent": self.user_agent}

        # If a symbol is specified, call the symbol-specific endpoint (likely JSON)
        params = None
        is_csv = True
        if symbol:
            params = {"index": "equities", "symbol": symbol, "period": period}
            if issuer:
                params["issuer"] = issuer
            url = "https://www.nseindia.com/api/corporates-financial-results"
            is_csv = False
        else:
            url = self.MASTER_URL.format(from_date=from_date, to_date=to_date, period=period)
            is_csv = True

        logger.info("fetching_master", url=url, params=params)

        def _get_with_retries(client: httpx.Client, url: str, params=None):
            attempt = 0
            while True:
                try:
                    resp = client.get(url, params=params)
                    resp.raise_for_status()
                    return resp
                except Exception as e:
                    attempt += 1
                    if attempt >= retries:
                        raise
                    logger.warning("request_failed_retry", url=url, attempt=attempt, error=str(e))
                    time.sleep(backoff * attempt)

        with httpx.Client(timeout=30.0, headers=headers) as client:
            r = _get_with_retries(client, url, params=params)
            text = r.content

        # choose output directory: prefer explicit out_dir, then CI -> data/, else data/tmp
        if out_dir:
            out_path = Path(out_dir)
        else:
            if os.environ.get("CI"):
                out_path = Path("data")
            else:
                out_path = Path("data/tmp")
        out_path.mkdir(parents=True, exist_ok=True)
        csv_path = out_path / f"master_{to_date.replace('-','')}.csv"

        # Parse response depending on content type
        content_type = r.headers.get("content-type", "")
        df: pd.DataFrame
        if not is_csv or "application/json" in content_type or content_type.startswith("application/"):
            # Try parsing JSON response
            try:
                j = r.json()
                if isinstance(j, dict):
                    # try common keys
                    for k in ("data", "results", "rows", "result"):
                        if k in j and isinstance(j[k], (list,)):
                            j = j[k]
                            break
                    else:
                        # convert dict to single-row DataFrame
                        df = pd.DataFrame([j])
                if isinstance(j, list):
                    df = pd.DataFrame(j)
            except Exception:
                # fallback: try reading as CSV
                csv_path.write_bytes(text)
                try:
                    df = pd.read_csv(csv_path, encoding="utf-8-sig")
                except Exception:
                    df = pd.read_csv(csv_path, encoding="latin1")
        else:
            csv_path.write_bytes(text)
            try:
                df = pd.read_csv(csv_path, encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(csv_path, encoding="latin1")

        # If symbol-based query, also write a symbol-specific CSV filename for CI
        if symbol:
            sym_name = quote_plus(symbol)
            sym_path = out_path / f"master_{sym_name}_{to_date.replace('-', '')}.csv"
            df.to_csv(sym_path, index=False, encoding="utf-8")

        logger.info("master_parsed", rows=int(len(df)), path=str(csv_path))

        # Optionally filter audited rows
        if filter_audited:
            aud_col = None
            for c in df.columns:
                if str(c).strip().lower() == "audited" or "audited" in str(c).strip().lower():
                    aud_col = c
                    break
            if aud_col is not None:
                before = len(df)
                df = df[df[aud_col].astype(str).str.strip().str.lower() == "audited"].copy()
                logger.info("filtered_audited", before=before, after=len(df), column=aud_col)
            else:
                logger.info("no_audited_column_found")

        return df

    def _candidate_urls_from_row(self, row: pd.Series) -> List[str]:
        # Be flexible with column names (case-insensitive, compact variants)
        if row is None:
            return []
        # Build a mapping of lowercase column name -> original column name
        cols = {str(c).strip().lower(): c for c in row.index}

        # Look for XBRL-like columns
        for key in ("** xbrl", "**xbrl", "xbrl"):
            if key in cols:
                val = row[cols[key]]
                if pd.isna(val):
                    return []
                val = str(val).strip()
                if val == "-":
                    return []
                return [val]

        # Try more permissive match: any column containing 'xbrl'
        for lc, orig in cols.items():
            if "xbrl" in lc:
                val = row[orig]
                if pd.isna(val):
                    return []
                val = str(val).strip()
                if val == "-":
                    return []
                return [val]

        # Fallback: look for relating/relatingto variants
        for key in ("relating to", "relatingto", "relating_to", "relating"):
            if key in cols:
                val = row[cols[key]]
                if pd.isna(val):
                    return []
                val = str(val).strip()
                if val.startswith("http"):
                    return [val]

        # Try any column containing 'relat'
        for lc, orig in cols.items():
            if "relat" in lc:
                val = row[orig]
                if pd.isna(val):
                    return []
                val = str(val).strip()
                if val.startswith("http"):
                    return [val]

        return []

    def download_documents(self, master: pd.DataFrame, out_dir: Optional[Path] = None) -> List[Path]:
        """Download referenced documents from the master DataFrame into `out_dir`.

        If `out_dir` is not provided defaults to `data/quarterly_documents/`.
        """
        if out_dir:
            out_path = Path(out_dir)
        else:
            out_path = Path("data/quarterly_documents")
        out_path.mkdir(parents=True, exist_ok=True)
        saved: List[Path] = []
        headers = {"User-Agent": self.user_agent}
        with httpx.Client(timeout=30.0, headers=headers) as client:
            for idx, row in master.iterrows():
                urls = self._candidate_urls_from_row(row)
                for url in urls:
                    try:
                        if not url or url.strip() == "-":
                            logger.debug("skip_empty_url", index=int(idx), url=url)
                            continue
                        # Skip obvious placeholder links
                        last_seg = str(url).rstrip("/\n \t").rsplit("/", 1)[-1]
                        if last_seg == "-" or not url.strip():
                            logger.debug("skip_placeholder_url", index=int(idx), url=url)
                            continue

                        try:
                            attempt = 0
                            max_attempts = 3
                            while True:
                                try:
                                    r = client.get(url)
                                    r.raise_for_status()
                                    break
                                except Exception as e:
                                    attempt += 1
                                    if attempt >= max_attempts:
                                        # give up on this URL and move on
                                        logger.warning("download_give_up", url=url, attempts=attempt, error=str(e))
                                        raise
                                    logger.warning("download_retry", url=url, attempt=attempt, error=str(e))
                                    time.sleep(0.5 * attempt)
                        except Exception as e:
                            logger.exception("download_exception", index=int(idx), url=url, error=str(e))
                            continue
                        # choose extension
                        ext = None
                        ctype = r.headers.get("content-type", "")
                        if "xml" in ctype or url.lower().endswith(".xml"):
                            ext = ".xml"
                        elif "html" in ctype or url.lower().endswith(".html"):
                            ext = ".html"
                        else:
                            ext = Path(url).suffix or ".xml"
                        # Try to prefix filename with symbol or company name when available
                        try:
                            cols = {str(c).strip().lower(): c for c in row.index}
                            sym_val = None
                            for key in ("symbol",):
                                if key in cols:
                                    sym_val = str(row[cols[key]]).strip()
                                    break
                            if not sym_val:
                                for key in ("companyname", "name of company", "company_name"):
                                    if key in cols:
                                        sym_val = str(row[cols[key]]).strip()
                                        break
                            if not sym_val:
                                sym_val = "unknown"
                        except Exception:
                            sym_val = "unknown"
                        # sanitize symbol/company for filesystem
                        safe_sym = "".join(c if (c.isalnum() or c in ("-", "_")) else "_" for c in sym_val)[:64]
                        fn = out_path / f"{safe_sym}_{idx}_{Path(url).name}"
                        # ensure extension
                        if not fn.suffix:
                            fn = fn.with_suffix(ext)
                        fn.write_bytes(r.content)
                        saved.append(fn)
                        logger.info("download_saved", path=str(fn), url=url)
                    except Exception as e:
                        logger.exception("download_exception", index=int(idx), url=url, error=str(e))
        return saved

    def normalize_master_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize common date/time fields and canonicalise column names.

        - Coerce period_start, period_end_date, filing_date to pandas datetime.date
        - Coerce exchdisstime, broadCastDate to ISO datetime strings
        - Ensure `year` and `quarter` are integers when derivable
        """
        if df is None or len(df) == 0:
            return df

        pdf = df.copy()

        # common rename hints
        rename_map = {
            "companyName": "company_name",
            "financialYear": "year",
            "filingDate": "filing_date",
            "fromDate": "period_start",
            "toDate": "period_end_date",
            "period": "period_type",
            "isin": "cin",
        }
        existing_rename = {k: v for k, v in rename_map.items() if k in pdf.columns}
        if existing_rename:
            pdf = pdf.rename(columns=existing_rename)

        # Normalize year
        if "year" in pdf.columns:
            try:
                pdf["year"] = pd.to_numeric(pdf["year"], errors="coerce").fillna(0).astype(int)
            except Exception:
                pass

        # Dates -> Date
        for dcol in ("period_start", "period_end_date", "filing_date"):
            if dcol in pdf.columns:
                try:
                    pdf[dcol] = pd.to_datetime(pdf[dcol], errors="coerce").dt.date
                except Exception:
                    pdf[dcol] = None

        # Datetimes -> ISO strings
        for dtcol in ("exchdisstime", "broadCastDate"):
            if dtcol in pdf.columns:
                try:
                    pdf[dtcol] = pd.to_datetime(pdf[dtcol], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pdf[dtcol] = None

        # Derive quarter if possible
        if "quarter" not in pdf.columns:
            try:
                if "period_type" in pdf.columns:
                    q = pdf["period_type"].astype(str).str.extract(r"Q([1-4])")[0]
                    if q.notna().any():
                        pdf["quarter"] = pd.to_numeric(q, errors="coerce").fillna(0).astype(int)
                if "quarter" not in pdf.columns or pdf["quarter"].isnull().all():
                    if "period_end_date" in pdf.columns:
                        pdf["period_end_date"] = pd.to_datetime(pdf["period_end_date"], errors="coerce")
                        pdf["quarter"] = pdf["period_end_date"].dt.quarter.fillna(0).astype(int)
            except Exception:
                pass

        return pdf


def get_master(
    start_date,
    end_date,
    period: str = "Quarterly",
    out_dir: Optional[Path] = None,
    symbol: Optional[str] = None,
    issuer: Optional[str] = None,
    filter_audited: bool = False,
    retries: int = 3,
    backoff: float = 1.0,
):
    """Compatibility wrapper used by the CLI.

    Accepts `start_date` and `end_date` as date objects or strings and writes
    the master CSV to `out_dir` (or `data/` when `CI` is set).
    """
    # Lazy import for typing compatibility without adding top-level deps
    from datetime import date as _date

    def _to_str(d):
        if isinstance(d, _date):
            return d.strftime("%d-%m-%Y")
        if isinstance(d, str):
            return d
        return str(d)

    scraper = QuarterlyResultsScraper()
    return scraper.get_master(
        from_date=_to_str(start_date),
        to_date=_to_str(end_date),
        period=period,
        out_dir=out_dir,
        symbol=symbol,
        issuer=issuer,
        filter_audited=filter_audited,
        retries=retries,
        backoff=backoff,
    )

