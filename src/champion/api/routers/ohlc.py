"""OHLC data endpoints."""

from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from champion.api.config import APISettings, get_api_settings
from champion.api.dependencies import get_clickhouse_client, get_pagination_params
from champion.api.schemas import OHLCData, OHLCResponse
from champion.warehouse.adapters import ClickHouseSink

router = APIRouter(prefix="/ohlc", tags=["OHLC Data"])


@router.get("", response_model=OHLCResponse)
async def get_ohlc_data(
    symbol: str = Query(..., description="Stock symbol (e.g., INFY)"),
    from_date: date | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: date | None = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
    settings: APISettings = Depends(get_api_settings),
) -> OHLCResponse:
    """Get OHLC data for a symbol with optional date range.

    Args:
        symbol: Stock symbol (e.g., INFY, TCS, RELIANCE)
        from_date: Start date (inclusive)
        to_date: End date (inclusive)
        pagination: Pagination parameters
        db: ClickHouse client
        settings: API settings

    Returns:
        OHLC data with metadata

    Example:
        GET /api/v1/ohlc?symbol=INFY&from=2024-01-01&to=2024-12-31&page=1&page_size=100
    """
    try:
        # Build query
        where_clauses = [f"TckrSymb = '{symbol}'"]

        if from_date:
            where_clauses.append(f"TradDt >= '{from_date}'")

        if to_date:
            where_clauses.append(f"TradDt <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        # Query for data
        query = f"""
        SELECT
            TckrSymb AS symbol,
            TradDt AS trade_date,
            OpnPric AS open,
            HghPric AS high,
            LwPric AS low,
            ClsPric AS close,
            TtlTradgVol AS volume,
            TtlTrfVal AS turnover
        FROM {settings.clickhouse_database}.normalized_equity_ohlc
        WHERE {where_clause}
        ORDER BY TradDt DESC
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Convert to OHLCData objects
        ohlc_data = []
        for row in rows:
            ohlc_data.append(
                OHLCData(
                    symbol=row[0],
                    trade_date=row[1],
                    open=row[2],
                    high=row[3],
                    low=row[4],
                    close=row[5],
                    volume=row[6],
                    turnover=row[7],
                )
            )

        # Prepare response metadata
        date_range = None
        if from_date or to_date:
            date_range = {
                "from": from_date or date(2000, 1, 1),  # Reasonable default
                "to": to_date or date.today(),
            }

        return OHLCResponse(
            data=ohlc_data,
            count=len(ohlc_data),
            symbol=symbol,
            date_range=date_range,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch OHLC data: {str(e)}",
        ) from e


@router.get("/{symbol}/latest", response_model=OHLCData)
async def get_latest_ohlc(
    symbol: str,
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> OHLCData:
    """Get the latest OHLC data for a symbol.

    Args:
        symbol: Stock symbol
        db: ClickHouse client

    Returns:
        Latest OHLC data

    Example:
        GET /api/v1/ohlc/INFY/latest
    """
    try:
        query = f"""
        SELECT
            TckrSymb AS symbol,
            TradDt AS trade_date,
            OpnPric AS open,
            HghPric AS high,
            LwPric AS low,
            ClsPric AS close,
            TtlTradgVol AS volume,
            TtlTrfVal AS turnover
        FROM normalized_equity_ohlc
        WHERE TckrSymb = '{symbol}'
        ORDER BY TradDt DESC
        LIMIT 1
        """

        result = db.client.query(query)
        rows = result.result_rows

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No OHLC data found for symbol {symbol}",
            )

        row = rows[0]
        return OHLCData(
            symbol=row[0],
            trade_date=row[1],
            open=row[2],
            high=row[3],
            low=row[4],
            close=row[5],
            volume=row[6],
            turnover=row[7],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch latest OHLC data: {str(e)}",
        ) from e


@router.get("/{symbol}/candles")
async def get_candles(
    symbol: str,
    interval: str = Query(default="1d", description="Candle interval (1d, 1w, 1M)"),
    from_date: date | None = Query(None, alias="from", description="Start date"),
    to_date: date | None = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get candle data for charting.

    Args:
        symbol: Stock symbol
        interval: Candle interval (1d=daily, 1w=weekly, 1M=monthly)
        from_date: Start date
        to_date: End date
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        Candle data in JSON format

    Example:
        GET /api/v1/ohlc/INFY/candles?interval=1d&from=2024-01-01&to=2024-12-31
    """
    try:
        # Validate interval
        valid_intervals = ["1d", "1w", "1M"]
        if interval not in valid_intervals:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid interval. Must be one of: {', '.join(valid_intervals)}",
            )

        # Build date filter
        where_clauses = [f"TckrSymb = '{symbol}'"]

        if from_date:
            where_clauses.append(f"TradDt >= '{from_date}'")

        if to_date:
            where_clauses.append(f"TradDt <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        # Build aggregation based on interval
        if interval == "1d":
            # Daily candles (no aggregation needed)
            query = f"""
            SELECT
                toDateTime(TradDt) AS timestamp,
                OpnPric AS open,
                HghPric AS high,
                LwPric AS low,
                ClsPric AS close,
                TtlTradgVol AS volume
            FROM normalized_equity_ohlc
            WHERE {where_clause}
            ORDER BY TradDt DESC
            LIMIT {pagination['limit']} OFFSET {pagination['offset']}
            """
        elif interval == "1w":
            # Weekly candles
            query = f"""
            SELECT
                toStartOfWeek(TradDt) AS timestamp,
                argMin(OpnPric, TradDt) AS open,
                max(HghPric) AS high,
                min(LwPric) AS low,
                argMax(ClsPric, TradDt) AS close,
                sum(TtlTradgVol) AS volume
            FROM normalized_equity_ohlc
            WHERE {where_clause}
            GROUP BY toStartOfWeek(TradDt)
            ORDER BY timestamp DESC
            LIMIT {pagination['limit']} OFFSET {pagination['offset']}
            """
        else:  # 1M
            # Monthly candles
            query = f"""
            SELECT
                toStartOfMonth(TradDt) AS timestamp,
                argMin(OpnPric, TradDt) AS open,
                max(HghPric) AS high,
                min(LwPric) AS low,
                argMax(ClsPric, TradDt) AS close,
                sum(TtlTradgVol) AS volume
            FROM normalized_equity_ohlc
            WHERE {where_clause}
            GROUP BY toStartOfMonth(TradDt)
            ORDER BY timestamp DESC
            LIMIT {pagination['limit']} OFFSET {pagination['offset']}
            """

        result = db.client.query(query)
        rows = result.result_rows

        # Format candles
        candles = []
        for row in rows:
            candles.append(
                {
                    "timestamp": row[0].isoformat()
                    if isinstance(row[0], datetime)
                    else str(row[0]),
                    "open": float(row[1]) if row[1] is not None else None,
                    "high": float(row[2]) if row[2] is not None else None,
                    "low": float(row[3]) if row[3] is not None else None,
                    "close": float(row[4]) if row[4] is not None else None,
                    "volume": int(row[5]) if row[5] is not None else None,
                }
            )

        return JSONResponse(
            content={
                "symbol": symbol,
                "interval": interval,
                "candles": candles,
                "count": len(candles),
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch candle data: {str(e)}",
        ) from e
