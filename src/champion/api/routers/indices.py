"""Index data endpoints."""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from champion.api.config import APISettings, get_api_settings
from champion.api.dependencies import get_clickhouse_client, get_pagination_params
from champion.warehouse.adapters import ClickHouseSink

router = APIRouter(prefix="/indices", tags=["Index Data"])
settings = get_api_settings()


@router.get("/{index}/constituents")
async def get_index_constituents(
    index: str,
    as_of_date: date | None = Query(None, description="As-of date (defaults to latest)"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get constituents of an index.

    Args:
        index: Index name (e.g., NIFTY50, SENSEX, NIFTY500)
        as_of_date: Optional date for historical constituents
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        Index constituent data

    Example:
        GET /api/v1/indices/NIFTY50/constituents
    """
    try:
        # Build where clause
        where_clauses = [f"index_name = '{index.upper()}'"]

        if as_of_date:
            where_clauses.append(f"effective_date <= '{as_of_date}'")

        where_clause = " AND ".join(where_clauses)

        # Query index constituents table
        # Note: This assumes an index_constituents table exists
        query = f"""
        SELECT
            index_name,
            symbol,
            company_name,
            weightage,
            effective_date
        FROM {settings.clickhouse_database}.index_constituents
        WHERE {where_clause}
        ORDER BY weightage DESC NULLS LAST, symbol
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format constituents
        constituents = []
        for row in rows:
            constituents.append(
                {
                    "index_name": row[0],
                    "symbol": row[1],
                    "company_name": row[2],
                    "weightage": float(row[3]) if row[3] is not None else None,
                    "effective_date": str(row[4]),
                }
            )

        return JSONResponse(
            content={
                "index": index.upper(),
                "as_of_date": str(as_of_date) if as_of_date else "latest",
                "constituents": constituents,
                "count": len(constituents),
            }
        )

    except Exception as e:
        # If table doesn't exist, return placeholder data
        if "doesn't exist" in str(e).lower() or "unknown table" in str(e).lower():
            return JSONResponse(
                content={
                    "index": index.upper(),
                    "constituents": [],
                    "count": 0,
                    "message": "Index constituents table not yet populated",
                }
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch index constituents: {str(e)}",
        ) from e


@router.get("/{index}/changes")
async def get_index_changes(
    index: str,
    from_date: date | None = Query(None, alias="from", description="Start date"),
    to_date: date | None = Query(None, alias="to", description="End date"),
    pagination: dict = Depends(get_pagination_params),
    db: ClickHouseSink = Depends(get_clickhouse_client),
) -> JSONResponse:
    """Get historical changes to index constituents.

    Tracks additions, removals, and rebalancing events.

    Args:
        index: Index name
        from_date: Start date filter
        to_date: End date filter
        pagination: Pagination parameters
        db: ClickHouse client

    Returns:
        Index change history

    Example:
        GET /api/v1/indices/NIFTY50/changes?from=2024-01-01
    """
    try:
        # Build where clause
        where_clauses = [f"index_name = '{index.upper()}'"]

        if from_date:
            where_clauses.append(f"change_date >= '{from_date}'")

        if to_date:
            where_clauses.append(f"change_date <= '{to_date}'")

        where_clause = " AND ".join(where_clauses)

        # Query index changes table
        query = f"""
        SELECT
            index_name,
            change_date,
            change_type,
            symbol,
            company_name,
            reason
        FROM index_changes
        WHERE {where_clause}
        ORDER BY change_date DESC, symbol
        LIMIT {pagination['limit']} OFFSET {pagination['offset']}
        """

        result = db.client.query(query)
        rows = result.result_rows

        # Format changes
        changes = []
        for row in rows:
            changes.append(
                {
                    "index_name": row[0],
                    "change_date": str(row[1]),
                    "change_type": row[2],
                    "symbol": row[3],
                    "company_name": row[4],
                    "reason": row[5] if len(row) > 5 else None,
                }
            )

        return JSONResponse(
            content={
                "index": index.upper(),
                "changes": changes,
                "count": len(changes),
            }
        )

    except Exception as e:
        # If table doesn't exist, return placeholder data
        if "doesn't exist" in str(e).lower() or "unknown table" in str(e).lower():
            return JSONResponse(
                content={
                    "index": index.upper(),
                    "changes": [],
                    "count": 0,
                    "message": "Index changes table not yet populated",
                }
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch index changes: {str(e)}",
        ) from e


@router.get("")
async def list_indices(
    db: ClickHouseSink = Depends(get_clickhouse_client),
    settings: APISettings = Depends(get_api_settings),
) -> JSONResponse:
    """List all available indices.

    Args:
        db: ClickHouse client
        settings: API settings

    Returns:
        List of available indices

    Example:
        GET /api/v1/indices
    """
    try:
        # Query distinct index names
        query = f"""
        SELECT DISTINCT index_name
        FROM {settings.clickhouse_database}.index_constituents
        ORDER BY index_name
        """

        result = db.client.query(query)
        rows = result.result_rows

        indices = [row[0] for row in rows]

        return JSONResponse(
            content={
                "indices": indices,
                "count": len(indices),
            }
        )

    except Exception as e:
        # If table doesn't exist, return common indices as placeholder
        if "doesn't exist" in str(e).lower() or "unknown table" in str(e).lower():
            return JSONResponse(
                content={
                    "indices": [
                        "NIFTY50",
                        "NIFTY100",
                        "NIFTY500",
                        "NIFTYMIDCAP",
                        "NIFTYSMALLCAP",
                        "SENSEX",
                    ],
                    "count": 6,
                    "message": "Placeholder data - index tables not yet populated",
                }
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list indices: {str(e)}",
        ) from e
