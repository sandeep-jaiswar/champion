"""API middleware."""

import hashlib
import json
from collections.abc import Callable

import redis
from fastapi import Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from champion.api.config import get_api_settings


def add_cors_middleware(app) -> None:
    """Add CORS middleware to the application.

    Args:
        app: FastAPI application instance
    """
    settings = get_api_settings()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


class CacheMiddleware(BaseHTTPMiddleware):
    """Redis cache middleware for GET requests."""

    def __init__(self, app, redis_client: redis.Redis, cache_ttl: int = 300):
        super().__init__(app)
        self.redis_client = redis_client
        self.cache_ttl = cache_ttl

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Dispatch request with caching.

        Args:
            request: FastAPI request
            call_next: Next middleware/handler

        Returns:
            Response (cached or fresh)
        """
        # Only cache GET requests
        if request.method != "GET":
            return await call_next(request)

        # Skip caching for auth endpoints
        if "/auth/" in str(request.url):
            return await call_next(request)

        # Generate cache key from URL and query params
        cache_key = self._generate_cache_key(request)

        try:
            # Try to get from cache
            cached_response = self.redis_client.get(cache_key)

            if cached_response:
                # Return cached response
                cached_data = json.loads(cached_response)
                return Response(
                    content=cached_data["content"],
                    status_code=cached_data["status_code"],
                    headers=cached_data["headers"],
                    media_type=cached_data["media_type"],
                )
        except redis.exceptions.ConnectionError:
            # If Redis is down, continue without caching
            pass

        # Get fresh response
        response = await call_next(request)

        # Cache successful responses
        if response.status_code == 200:
            try:
                # Read response body
                response_body = b""
                async for chunk in response.body_iterator:
                    response_body += chunk

                # Cache the response
                cache_data = {
                    "content": response_body.decode(),
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "media_type": response.media_type,
                }

                self.redis_client.setex(
                    cache_key,
                    self.cache_ttl,
                    json.dumps(cache_data)
                )

                # Return response with body
                return Response(
                    content=response_body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )
            except redis.exceptions.ConnectionError:
                # If Redis is down, return response without caching
                pass

        return response

    def _generate_cache_key(self, request: Request) -> str:
        """Generate cache key from request.

        Args:
            request: FastAPI request

        Returns:
            Cache key
        """
        # Create key from path and query params
        key_data = f"{request.url.path}?{request.url.query}"

        # Hash the key to keep it short
        key_hash = hashlib.md5(key_data.encode()).hexdigest()

        return f"cache:{key_hash}"


def add_cache_middleware(app) -> None:
    """Add cache middleware to the application.

    Args:
        app: FastAPI application instance
    """
    settings = get_api_settings()

    try:
        redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True,
        )

        # Test connection
        redis_client.ping()

        app.add_middleware(
            CacheMiddleware,
            redis_client=redis_client,
            cache_ttl=settings.cache_ttl,
        )
    except redis.exceptions.ConnectionError:
        # If Redis is not available, skip caching
        print("Warning: Redis not available, caching disabled")
