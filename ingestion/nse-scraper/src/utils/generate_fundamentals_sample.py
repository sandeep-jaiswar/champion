"""Sample data generator for fundamentals (quarterly financials and shareholding patterns).

This module generates realistic sample data for testing the fundamentals ingestion pipeline.
"""

import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

logger = get_logger(__name__)


def generate_quarterly_financials_sample(
    symbols: list[str],
    start_date: date,
    end_date: date,
    output_dir: Path,
) -> Path:
    """Generate sample quarterly financials data.

    Args:
        symbols: List of trading symbols
        start_date: Start date for data generation
        end_date: End date for data generation
        output_dir: Output directory for Parquet files

    Returns:
        Path to generated Parquet file
    """
    logger.info(
        "Generating quarterly financials sample",
        symbols=len(symbols),
        start_date=str(start_date),
        end_date=str(end_date),
    )

    records = []

    # Generate quarterly data for each symbol
    current_date = start_date
    while current_date <= end_date:
        # Find quarter end date
        quarter = (current_date.month - 1) // 3 + 1
        if quarter == 1:
            quarter_end = date(current_date.year, 3, 31)
        elif quarter == 2:
            quarter_end = date(current_date.year, 6, 30)
        elif quarter == 3:
            quarter_end = date(current_date.year, 9, 30)
        else:
            quarter_end = date(current_date.year, 12, 31)

        if quarter_end > end_date:
            break

        for symbol in symbols:
            # Generate realistic financial metrics
            base_revenue = 1000 + (hash(symbol) % 9000)  # 1000-10000 crores
            growth_factor = 1 + (hash(f"{symbol}{quarter_end}") % 20 - 10) / 100  # -10% to +10%

            revenue = base_revenue * growth_factor
            operating_profit = revenue * (0.15 + (hash(f"{symbol}op") % 15) / 100)  # 15-30%
            net_profit = operating_profit * (0.5 + (hash(f"{symbol}np") % 30) / 100)  # 50-80%
            
            total_assets = revenue * (1.5 + (hash(f"{symbol}assets") % 100) / 100)  # 1.5-2.5x revenue
            equity = total_assets * (0.4 + (hash(f"{symbol}equity") % 30) / 100)  # 40-70% of assets
            total_debt = total_assets - equity - (total_assets * 0.2)  # Some current liabilities
            
            current_assets = total_assets * (0.3 + (hash(f"{symbol}ca") % 20) / 100)
            current_liabilities = current_assets * (0.7 + (hash(f"{symbol}cl") % 40) / 100)

            now = datetime.utcnow()
            record = {
                "event_id": str(uuid.uuid4()),
                "event_time": now,
                "ingest_time": now,
                "source": "BSE_SAMPLE",
                "schema_version": "1.0.0",
                "entity_id": f"CIN{symbol}",
                "symbol": symbol,
                "company_name": f"{symbol} Limited",
                "cin": f"U12345MH2000PLC{hash(symbol) % 100000:06d}",
                "period_end_date": quarter_end,
                "period_type": "QUARTERLY",
                "statement_type": "STANDALONE",
                "filing_date": quarter_end + timedelta(days=30),
                "revenue": round(revenue, 2),
                "operating_profit": round(operating_profit, 2),
                "net_profit": round(net_profit, 2),
                "total_assets": round(total_assets, 2),
                "total_liabilities": round(total_assets - equity, 2),
                "equity": round(equity, 2),
                "total_debt": round(total_debt, 2),
                "current_assets": round(current_assets, 2),
                "current_liabilities": round(current_liabilities, 2),
                "cash_and_equivalents": round(current_assets * 0.3, 2),
                "inventories": round(current_assets * 0.25, 2),
                "depreciation": round(revenue * 0.05, 2),
                "interest_expense": round(total_debt * 0.08 / 4, 2),  # Quarterly
                "tax_expense": round(net_profit * 0.3, 2),
                "eps": round(net_profit / 100, 2),  # Assume 100 crore shares
                "book_value_per_share": round(equity / 100, 2),
                "roe": round((net_profit / equity) * 100, 2) if equity > 0 else None,
                "roa": round((net_profit / total_assets) * 100, 2) if total_assets > 0 else None,
                "debt_to_equity": round(total_debt / equity, 2) if equity > 0 else None,
                "current_ratio": round(current_assets / current_liabilities, 2) if current_liabilities > 0 else None,
                "operating_margin": round((operating_profit / revenue) * 100, 2) if revenue > 0 else None,
                "net_margin": round((net_profit / revenue) * 100, 2) if revenue > 0 else None,
                "year": quarter_end.year,
                "quarter": quarter,
                "metadata": {"source_type": "sample_generated"},
            }
            records.append(record)

        # Move to next quarter
        if quarter == 4:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, (quarter * 3) + 1, 1)

    # Create DataFrame
    df = pl.DataFrame(records)

    # Save to Parquet
    output_path = output_dir / "quarterly_financials_sample.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    logger.info("Generated quarterly financials sample", rows=len(df), path=str(output_path))
    return output_path


def generate_shareholding_pattern_sample(
    symbols: list[str],
    start_date: date,
    end_date: date,
    output_dir: Path,
) -> Path:
    """Generate sample shareholding pattern data.

    Args:
        symbols: List of trading symbols
        start_date: Start date for data generation
        end_date: End date for data generation
        output_dir: Output directory for Parquet files

    Returns:
        Path to generated Parquet file
    """
    logger.info(
        "Generating shareholding pattern sample",
        symbols=len(symbols),
        start_date=str(start_date),
        end_date=str(end_date),
    )

    records = []

    # Generate quarterly data for each symbol
    current_date = start_date
    while current_date <= end_date:
        # Find quarter end date
        quarter = (current_date.month - 1) // 3 + 1
        if quarter == 1:
            quarter_end = date(current_date.year, 3, 31)
        elif quarter == 2:
            quarter_end = date(current_date.year, 6, 30)
        elif quarter == 3:
            quarter_end = date(current_date.year, 9, 30)
        else:
            quarter_end = date(current_date.year, 12, 31)

        if quarter_end > end_date:
            break

        for symbol in symbols:
            # Generate realistic shareholding pattern
            promoter_pct = 40 + (hash(f"{symbol}promoter") % 35)  # 40-75%
            fii_pct = 10 + (hash(f"{symbol}fii") % 20)  # 10-30%
            dii_pct = 5 + (hash(f"{symbol}dii") % 15)  # 5-20%
            public_pct = 100 - promoter_pct - fii_pct - dii_pct

            total_shares = 1000000000 + (hash(symbol) % 9000000000)  # 100 crore to 1000 crore shares

            now = datetime.utcnow()
            record = {
                "event_id": str(uuid.uuid4()),
                "event_time": now,
                "ingest_time": now,
                "source": "BSE_SAMPLE",
                "schema_version": "1.0.0",
                "entity_id": f"{symbol}_{quarter_end}",
                "symbol": symbol,
                "company_name": f"{symbol} Limited",
                "scrip_code": f"{hash(symbol) % 900000 + 100000}",
                "isin": f"INE{hash(symbol) % 900000 + 100000:06d}01",
                "quarter_end_date": quarter_end,
                "filing_date": quarter_end + timedelta(days=21),
                "promoter_shareholding_percent": round(promoter_pct, 2),
                "promoter_shares": int(total_shares * promoter_pct / 100),
                "public_shareholding_percent": round(public_pct, 2),
                "public_shares": int(total_shares * public_pct / 100),
                "institutional_shareholding_percent": round(fii_pct + dii_pct, 2),
                "institutional_shares": int(total_shares * (fii_pct + dii_pct) / 100),
                "fii_shareholding_percent": round(fii_pct, 2),
                "fii_shares": int(total_shares * fii_pct / 100),
                "dii_shareholding_percent": round(dii_pct, 2),
                "dii_shares": int(total_shares * dii_pct / 100),
                "mutual_fund_shareholding_percent": round(dii_pct * 0.6, 2),
                "mutual_fund_shares": int(total_shares * dii_pct * 0.6 / 100),
                "insurance_companies_percent": round(dii_pct * 0.3, 2),
                "insurance_companies_shares": int(total_shares * dii_pct * 0.3 / 100),
                "banks_shareholding_percent": round(dii_pct * 0.1, 2),
                "banks_shares": int(total_shares * dii_pct * 0.1 / 100),
                "employee_shareholding_percent": round((hash(f"{symbol}emp") % 5) / 10, 2),
                "employee_shares": int(total_shares * (hash(f"{symbol}emp") % 5) / 1000),
                "total_shares_outstanding": total_shares,
                "pledged_promoter_shares_percent": round((hash(f"{symbol}pledge") % 20) / 10, 2),
                "pledged_promoter_shares": int(total_shares * promoter_pct * (hash(f"{symbol}pledge") % 20) / 10000),
                "year": quarter_end.year,
                "quarter": quarter,
                "metadata": {"source_type": "sample_generated"},
            }
            records.append(record)

        # Move to next quarter
        if quarter == 4:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, (quarter * 3) + 1, 1)

    # Create DataFrame
    df = pl.DataFrame(records)

    # Save to Parquet
    output_path = output_dir / "shareholding_pattern_sample.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    logger.info("Generated shareholding pattern sample", rows=len(df), path=str(output_path))
    return output_path
