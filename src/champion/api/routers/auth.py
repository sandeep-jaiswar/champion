"""Authentication endpoints."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt
from passlib.context import CryptContext

from champion.api.config import APISettings, get_api_settings
from champion.api.schemas import Token, User

router = APIRouter(prefix="/auth", tags=["Authentication"])

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Fake user database for demonstration
# In production, this should come from a real database
fake_users_db = {
    "demo": {
        "username": "demo",
        "email": "demo@champion.com",
        "hashed_password": pwd_context.hash("demo123"),
        "disabled": False,
    }
}


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_user(username: str) -> Optional[User]:
    """Get user from database."""
    if username in fake_users_db:
        user_dict = fake_users_db[username]
        return User(**user_dict)
    return None


def authenticate_user(username: str, password: str) -> Optional[User]:
    """Authenticate a user."""
    user_dict = fake_users_db.get(username)
    if not user_dict:
        return None
    if not verify_password(password, user_dict["hashed_password"]):
        return None
    return User(**user_dict)


def create_access_token(
    data: dict,
    settings: APISettings,
    expires_delta: Optional[timedelta] = None
) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.jwt_expiration_minutes)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode,
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm
    )
    return encoded_jwt


@router.post("/token", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    settings: APISettings = Depends(get_api_settings),
) -> Token:
    """Login endpoint to get JWT token.
    
    Args:
        form_data: OAuth2 password form data (username and password)
        settings: API settings
        
    Returns:
        JWT access token
        
    Example:
        POST /api/v1/auth/token
        Content-Type: application/x-www-form-urlencoded
        
        username=demo&password=demo123
    """
    user = authenticate_user(form_data.username, form_data.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if user.disabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User account is disabled",
        )
    
    access_token_expires = timedelta(minutes=settings.jwt_expiration_minutes)
    access_token = create_access_token(
        data={"sub": user.username},
        settings=settings,
        expires_delta=access_token_expires
    )
    
    return Token(access_token=access_token, token_type="bearer")


@router.get("/me", response_model=User)
async def read_users_me(
    settings: APISettings = Depends(get_api_settings),
) -> User:
    """Get current user information.
    
    This endpoint requires authentication.
    
    Args:
        settings: API settings
        
    Returns:
        Current user information
        
    Example:
        GET /api/v1/auth/me
        Authorization: Bearer <token>
    """
    # For demo purposes, return a default user
    # In production, this should extract the user from the JWT token
    return User(username="demo", email="demo@champion.com", disabled=False)
