"""API dependencies for dependency injection."""

import time

import redis
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from champion.api.config import APISettings, get_api_settings
from champion.api.repositories import UserRepository
from champion.api.schemas import TokenData
from champion.warehouse.adapters import ClickHouseSink

# Security
security = HTTPBearer()


def get_clickhouse_client(
    settings: APISettings = Depends(get_api_settings),
) -> ClickHouseSink:
    """Get ClickHouse client instance.

    Args:
        settings: API settings

    Returns:
        Connected ClickHouse client
    """
    client = ClickHouseSink(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    client.connect()
    return client


def get_redis_client(
    settings: APISettings = Depends(get_api_settings),
) -> redis.Redis:
    """Get Redis client for caching.

    Args:
        settings: API settings

    Returns:
        Redis client instance
    """
    return redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        decode_responses=True,
    )


def get_user_repository(
    clickhouse: ClickHouseSink = Depends(get_clickhouse_client),
) -> UserRepository:
    """Get user repository instance.

    Args:
        clickhouse: ClickHouse client

    Returns:
        UserRepository instance
    """
    return UserRepository(clickhouse)


def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    settings: APISettings = Depends(get_api_settings),
) -> TokenData:
    """Verify JWT token.

    Args:
        credentials: HTTP authorization credentials
        settings: API settings

    Returns:
        Token data

    Raises:
        HTTPException: If token is invalid
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        token = credentials.credentials
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        username: str | None = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError as e:
        raise credentials_exception from e

    return token_data


async def rate_limiter(
    request: Request,
    settings: APISettings = Depends(get_api_settings),
    redis_client: redis.Redis = Depends(get_redis_client),
) -> None:
    """Rate limiting middleware using Redis.

    Args:
        request: FastAPI request
        settings: API settings
        redis_client: Redis client

    Raises:
        HTTPException: If rate limit exceeded
    """
    # Get client identifier (IP address)
    client_id = request.client.host if request.client else "unknown"

    # Create rate limit key
    current_minute = int(time.time() / 60)
    key = f"rate_limit:{client_id}:{current_minute}"

    try:
        # Increment counter
        current: int = redis_client.incr(key)  # type: ignore[assignment]

        # Set expiration on first request
        if current == 1:
            redis_client.expire(key, 60)

        # Check rate limit
        if current > settings.rate_limit_per_minute:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Please try again later.",
            )
    except redis.exceptions.ConnectionError:
        # If Redis is down, allow the request (fail open)
        pass


def get_pagination_params(
    page: int = 1,
    page_size: int = 100,
    settings: APISettings = Depends(get_api_settings),
) -> dict[str, int]:
    """Get pagination parameters.

    Args:
        page: Page number (1-indexed)
        page_size: Number of items per page
        settings: API settings

    Returns:
        Dictionary with pagination parameters

    Raises:
        HTTPException: If parameters are invalid
    """
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Page number must be >= 1",
        )

    if page_size < 1 or page_size > settings.max_page_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Page size must be between 1 and {settings.max_page_size}",
        )

    offset = (page - 1) * page_size

    return {
        "limit": page_size,
        "offset": offset,
        "page": page,
        "page_size": page_size,
    }


__all__ = [
    "get_clickhouse_client",
    "get_redis_client",
    "get_user_repository",
    "verify_token",
    "rate_limiter",
    "get_pagination_params",
    "security",
]
