"""Authentication endpoints."""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt
from passlib.context import CryptContext
from slowapi import Limiter
from slowapi.util import get_remote_address

from champion.api.config import APISettings, get_api_settings
from champion.api.dependencies import get_user_repository, verify_token
from champion.api.repositories import UserRepository
from champion.api.schemas import Token, TokenData, User

router = APIRouter(prefix="/auth", tags=["Authentication"])

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash.

    Args:
        plain_password: Plain text password
        hashed_password: Hashed password to verify against

    Returns:
        True if passwords match, False otherwise
    """
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
    data: dict, settings: APISettings, expires_delta: timedelta | None = None
) -> str:
    """Create JWT access token.

    Args:
        data: Data to encode in token (e.g., {"sub": username})
        settings: API settings with JWT configuration
        expires_delta: Optional custom expiration time delta

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.now(UTC) + expires_delta
    else:
        expire = datetime.now(UTC) + timedelta(minutes=settings.jwt_expiration_minutes)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return encoded_jwt


@router.post("/token", response_model=Token)
@limiter.limit("5/minute")
async def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    settings: APISettings = Depends(get_api_settings),
    user_repo: UserRepository = Depends(get_user_repository),
) -> Token:
    """Login endpoint to get JWT token.

    Rate limited to 5 attempts per minute to prevent brute force attacks.

    Args:
        form_data: OAuth2 password form data (username and password)
        settings: API settings
        user_repo: User repository for database access

    Returns:
        JWT access token

    Raises:
        HTTPException: If authentication fails

    Example:
        POST /api/v1/auth/token
        Content-Type: application/x-www-form-urlencoded

        username=demo&password=demo123
    """
    # Get user from repository
    user_data = user_repo.get_by_username(form_data.username)

    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verify password
    if not verify_password(form_data.password, user_data["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if user is disabled
    if user_data["disabled"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User account is disabled",
        )

    # Create access token
    access_token_expires = timedelta(minutes=settings.jwt_expiration_minutes)
    access_token = create_access_token(
        data={"sub": user_data["username"]}, settings=settings, expires_delta=access_token_expires
    )

    return Token(access_token=access_token, token_type="bearer")


@router.get("/me", response_model=User)
async def read_users_me(
    token_data: TokenData = Depends(verify_token),
    user_repo: UserRepository = Depends(get_user_repository),
) -> User:
    """Get current user information.

    This endpoint requires authentication via JWT token.

    Args:
        token_data: Decoded token data from JWT
        user_repo: User repository for database access

    Returns:
        Current user information

    Raises:
        HTTPException: If user not found

    Example:
        GET /api/v1/auth/me
        Authorization: Bearer <token>
    """
    # Check if username is present in token
    if not token_data.username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )

    # Get user from repository using username from token
    user_data = user_repo.get_by_username(token_data.username)

    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return User(
        username=user_data["username"],
        email=user_data["email"],
        disabled=user_data["disabled"],
    )
