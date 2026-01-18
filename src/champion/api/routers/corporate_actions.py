"""Corporate actions endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from champion.api.dependencies import get_clickhouse_client, get_pagination_params
from champion.warehouse.adapters import ClickHouseSink

router = APIRouter(prefix="/corporate-actions", tags=["Corporate Actions"])


@router.get("")
async def get_corporate_actions(
    symbol: Optional[str] = Query(None, description="Stock symbol (optional)"),
    from_date: Optional[date] = Query(None, alias="from", description="Start date"),
    to_date: Optional[date] = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get corporate actions data.

    Args:
        symbol: Optional stock symbol filter
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        Corporate actions data

    Example:
        GET /api/v1/corporate-actions?symbol=INFY&from=2024-01-01
    """
    try:
        # Build where clause
        where_clauses = []

        if symbol:
            where_clauses.append(f"symbol = '{symbol}'")

        if from_date:
            where_clauses.append(f"ex_date >= '{from_date}'")

        if to_date:
            where_clauses.append(f"ex_date <= '{to_date}'")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Query corporate actions table
        # Note: This assumes a corporate_actions table exists
        # If not, this will gracefully fail with a helpful message
        query = f"""
        SELECT
            symbol,
            ex_date,
            ca_type AS action_type,
            purpose AS description,
            ratio,
            dividend_amount AS amount
        FROM corporate_actions
        WHERE {where_clause}
        ORDER BY ex_date DESC, symbol
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format corporate actions
        actions = []
        for row in rows:
            actions.append({
                "symbol": row[0],
                "ex_date": str(row[1]),
                "action_type": row[2],
                "description": row[3],
                "ratio": row[4],
                "amount": float(row[5]) if row[5] is not None else None,
            })

        return JSONResponse(
            content={
                "data": actions,
                "count": len(actions),
                "page": pagination["page"],
                "page_size": pagination["page_size"],
            }
        )

    except Exception as e:
        # If table doesn't exist, return empty result with message
        if "doesn't exist" in str(e).lower() or "unknown table" in str(e).lower():
            return JSONResponse(
                content={
                    "data": [],
                    "count": 0,
                    "message": "Corporate actions table not yet populated",
                }
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch corporate actions: {str(e)}",
        )


@router.get("/{symbol}/splits")
async def get_stock_splits(
    symbol: str,
    from_date: Optional[date] = Query(None, alias="from", description="Start date"),
    to_date: Optional[date] = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get stock split data for a symbol.

    Args:
        symbol: Stock symbol
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        Stock split data

    Example:
        GET /api/v1/corporate-actions/INFY/splits
    """
    try:
        # Build where clause
        where_clauses = [f"symbol = '{symbol}'", "ca_type = 'split'"]

        if from_date:
            where_clauses.append(f"ex_date >= '{from_date}'")

        if to_date:
            where_clauses.append(f"ex_date <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        query = f"""
        SELECT
            symbol,
            ex_date,
            ratio,
            purpose AS description
        FROM corporate_actions
        WHERE {where_clause}
        ORDER BY ex_date DESC
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Parse split ratios (e.g., "1:2" means 1 old share becomes 2 new shares)
        splits = []
        for row in rows:
            ratio_str = row[2] if row[2] else "1:1"
            parts = ratio_str.split(":")
            old_ratio = int(parts[0]) if len(parts) > 0 else 1
            new_ratio = int(parts[1]) if len(parts) > 1 else 1

            splits.append({
                "symbol": row[0],
                "ex_date": str(row[1]),
                "old_ratio": old_ratio,
                "new_ratio": new_ratio,
                "description": row[3],
            })

        return JSONResponse(
            content={
                "symbol": symbol,
                "splits": splits,
                "count": len(splits),
            }
        )

    except Exception as e:
        if "doesn't exist" in str(e).lower() or "unknown table" in str(e).lower():
            return JSONResponse(
                content={
                    "symbol": symbol,
                    "splits": [],
                    "count": 0,
                    "message": "Corporate actions table not yet populated",
                }
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch stock splits: {str(e)}",
        )


@router.get("/{symbol}/dividends")
async def get_dividends(
    symbol: str,
    from_date: Optional[date] = Query(None, alias="from", description="Start date"),
    to_date: Optional[date] = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get dividend data for a symbol.

    Args:
        symbol: Stock symbol
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        Dividend data

    Example:
        GET /api/v1/corporate-actions/INFY/dividends
    """
    try:
        # Build where clause
        where_clauses = [
            f"symbol = '{symbol}'",
            "ca_type IN ('dividend', 'interim_dividend', 'final_dividend')"
        ]

        if from_date:
            where_clauses.append(f"ex_date >= '{from_date}'")

        if to_date:
            where_clauses.append(f"ex_date <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        query = f"""
        SELECT
            symbol,
            ex_date,
            record_date,
            dividend_amount,
            ca_type AS dividend_type,
            purpose AS description
        FROM corporate_actions
        WHERE {where_clause}
        ORDER BY ex_date DESC
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format dividends
        dividends = []
        for row in rows:
            dividends.append({
                "symbol": row[0],
                "ex_date": str(row[1]),
                "record_date": str(row[2]) if row[2] else None,
                "dividend_amount": float(row[3]) if row[3] is not None else 0.0,
                "dividend_type": row[4],
                "description": row[5],
            })

        return JSONResponse(
            content={
                "symbol": symbol,
                "dividends": dividends,
                "count": len(dividends),
            }
        )

    except Exception as e:
        if "doesn't exist" in str(e).lower() or "unknown table" in str(e).lower():
            return JSONResponse(
                content={
                    "symbol": symbol,
                    "dividends": [],
                    "count": 0,
                    "message": "Corporate actions table not yet populated",
                }
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch dividends: {str(e)}",
        )
