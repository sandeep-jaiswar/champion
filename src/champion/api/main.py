"""Main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from champion.api.config import get_api_settings
from champion.api.middleware import add_cache_middleware, add_cors_middleware
from champion.api.routers import auth, corporate_actions, indicators, indices, ohlc


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    print("Starting Champion API...")
    
    # Initialize database tables
    from champion.api.dependencies import get_clickhouse_client
    from champion.api.repositories import UserRepository
    
    try:
        clickhouse = get_clickhouse_client()
        clickhouse.connect()
        repo = UserRepository(clickhouse)
        repo.init_table()
        print("Database tables initialized successfully")
    except Exception as e:
        print(f"Warning: Failed to initialize database tables: {e}")
    
    yield
    
    # Shutdown
    print("Shutting down Champion API...")
    try:
        if clickhouse:
            clickhouse.disconnect()
    except Exception:
        pass


def create_app() -> FastAPI:
    """Create and configure FastAPI application.

    Returns:
        Configured FastAPI application
    """
    settings = get_api_settings()

    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description="""
        Champion Market Data API provides access to:

        - **OHLC Data**: Historical and latest price data
        - **Corporate Actions**: Splits, dividends, and other corporate events
        - **Technical Indicators**: SMA, RSI, EMA calculations
        - **Index Data**: Index constituents and changes

        ## Authentication

        Most endpoints require authentication. Use `/api/v1/auth/token` to get a JWT token.

        Demo credentials:
        - Username: `demo`
        - Password: `demo123`

        ## Rate Limiting

        API requests are rate-limited to 60 requests per minute per IP address.

        ## Caching

        GET requests are cached for 5 minutes to improve performance.
        """,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Add middleware
    add_cors_middleware(app)
    add_cache_middleware(app)

    # Add routers with API prefix
    app.include_router(auth.router, prefix=settings.api_prefix)
    app.include_router(ohlc.router, prefix=settings.api_prefix)
    app.include_router(corporate_actions.router, prefix=settings.api_prefix)
    app.include_router(indicators.router, prefix=settings.api_prefix)
    app.include_router(indices.router, prefix=settings.api_prefix)

    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint."""
        return {
            "name": settings.api_title,
            "version": settings.api_version,
            "docs": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json",
        }

    # Health check endpoint
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "service": "champion-api",
            "version": settings.api_version,
        }

    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """Handle uncaught exceptions."""
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "detail": str(exc),
            },
        )

    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_api_settings()

    uvicorn.run(
        "champion.api.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
