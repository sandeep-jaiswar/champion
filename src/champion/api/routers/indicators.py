"""Technical indicators endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from champion.api.dependencies import get_clickhouse_client, get_pagination_params
from champion.warehouse.adapters import ClickHouseSink

router = APIRouter(prefix="/indicators", tags=["Technical Indicators"])


@router.get("/{symbol}/sma")
async def get_sma(
    symbol: str,
    period: int = Query(default=20, ge=1, le=200, description="SMA period (default: 20)"),
    from_date: Optional[date] = Query(None, alias="from", description="Start date"),
    to_date: Optional[date] = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get Simple Moving Average (SMA) for a symbol.

    Calculates SMA dynamically from OHLC data if features table is not available.

    Args:
        symbol: Stock symbol
        period: SMA period (1-200 days)
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        SMA data

    Example:
        GET /api/v1/indicators/INFY/sma?period=20
    """
    try:
        # Build where clause
        where_clauses = [f"TckrSymb = '{symbol}'"]

        if from_date:
            where_clauses.append(f"TradDt >= '{from_date}'")

        if to_date:
            where_clauses.append(f"TradDt <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        # Calculate SMA dynamically using ClickHouse window functions
        query = f"""
        SELECT
            TradDt AS trade_date,
            ClsPric AS close,
            avg(ClsPric) OVER (
                ORDER BY TradDt
                ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW
            ) AS sma_value
        FROM normalized_equity_ohlc
        WHERE {where_clause}
        ORDER BY TradDt DESC
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format SMA data
        sma_data = []
        for row in rows:
            sma_data.append({
                "trade_date": str(row[0]),
                "close": float(row[1]) if row[1] is not None else None,
                "sma_value": float(row[2]) if row[2] is not None else None,
            })

        return JSONResponse(
            content={
                "symbol": symbol,
                "indicator": "sma",
                "period": period,
                "data": sma_data,
                "count": len(sma_data),
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate SMA: {str(e)}",
        )


@router.get("/{symbol}/rsi")
async def get_rsi(
    symbol: str,
    period: int = Query(default=14, ge=2, le=50, description="RSI period (default: 14)"),
    from_date: Optional[date] = Query(None, alias="from", description="Start date"),
    to_date: Optional[date] = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get Relative Strength Index (RSI) for a symbol.

    Calculates RSI dynamically from OHLC data if features table is not available.
    RSI = 100 - (100 / (1 + RS))
    where RS = Average Gain / Average Loss

    Args:
        symbol: Stock symbol
        period: RSI period (2-50 days, default: 14)
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        RSI data

    Example:
        GET /api/v1/indicators/INFY/rsi?period=14
    """
    try:
        # Build where clause
        where_clauses = [f"TckrSymb = '{symbol}'"]

        if from_date:
            where_clauses.append(f"TradDt >= '{from_date}'")

        if to_date:
            where_clauses.append(f"TradDt <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        # Calculate RSI using a multi-step approach
        # Step 1: Calculate price changes
        # Step 2: Calculate average gains and losses
        # Step 3: Calculate RSI
        query = f"""
        WITH price_changes AS (
            SELECT
                TradDt AS trade_date,
                ClsPric AS close,
                ClsPric - lagInFrame(ClsPric, 1) OVER (ORDER BY TradDt) AS price_change
            FROM normalized_equity_ohlc
            WHERE {where_clause}
        ),
        gains_losses AS (
            SELECT
                trade_date,
                close,
                if(price_change > 0, price_change, 0) AS gain,
                if(price_change < 0, abs(price_change), 0) AS loss
            FROM price_changes
        ),
        avg_gains_losses AS (
            SELECT
                trade_date,
                close,
                avg(gain) OVER (
                    ORDER BY trade_date
                    ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW
                ) AS avg_gain,
                avg(loss) OVER (
                    ORDER BY trade_date
                    ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW
                ) AS avg_loss
            FROM gains_losses
        )
        SELECT
            trade_date,
            close,
            if(avg_loss = 0, 100,
               100 - (100 / (1 + (avg_gain / avg_loss)))
            ) AS rsi_value
        FROM avg_gains_losses
        ORDER BY trade_date DESC
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format RSI data
        rsi_data = []
        for row in rows:
            rsi_data.append({
                "trade_date": str(row[0]),
                "close": float(row[1]) if row[1] is not None else None,
                "rsi_value": float(row[2]) if row[2] is not None else None,
            })

        return JSONResponse(
            content={
                "symbol": symbol,
                "indicator": "rsi",
                "period": period,
                "data": rsi_data,
                "count": len(rsi_data),
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate RSI: {str(e)}",
        )


@router.get("/{symbol}/ema")
async def get_ema(
    symbol: str,
    period: int = Query(default=12, ge=1, le=200, description="EMA period (default: 12)"),
    from_date: Optional[date] = Query(None, alias="from", description="Start date"),
    to_date: Optional[date] = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get Exponential Moving Average (EMA) for a symbol.

    Calculates EMA dynamically from OHLC data.

    Args:
        symbol: Stock symbol
        period: EMA period (1-200 days)
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        EMA data

    Example:
        GET /api/v1/indicators/INFY/ema?period=12
    """
    try:
        # Build where clause
        where_clauses = [f"TckrSymb = '{symbol}'"]

        if from_date:
            where_clauses.append(f"TradDt >= '{from_date}'")

        if to_date:
            where_clauses.append(f"TradDt <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        # Calculate EMA using exponential smoothing
        # alpha = 2 / (period + 1)
        alpha = 2.0 / (period + 1)

        query = f"""
        SELECT
            TradDt AS trade_date,
            ClsPric AS close,
            exponentialMovingAverage({alpha})(ClsPric) OVER (
                ORDER BY TradDt
            ) AS ema_value
        FROM normalized_equity_ohlc
        WHERE {where_clause}
        ORDER BY TradDt DESC
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format EMA data
        ema_data = []
        for row in rows:
            ema_data.append({
                "trade_date": str(row[0]),
                "close": float(row[1]) if row[1] is not None else None,
                "ema_value": float(row[2]) if row[2] is not None else None,
            })

        return JSONResponse(
            content={
                "symbol": symbol,
                "indicator": "ema",
                "period": period,
                "data": ema_data,
                "count": len(ema_data),
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate EMA: {str(e)}",
        )
