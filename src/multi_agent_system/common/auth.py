"""JWT authentication for API endpoints."""

import time
import hmac
import hashlib
import base64
import json
import logging
from typing import Optional, Dict, Tuple

logger = logging.getLogger('auth')

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False
    logger.warning("PyJWT not available. Install with: pip install pyjwt")


class JWTAuth:
    """JWT-based authentication."""

    def __init__(self, secret_key: str = None, algorithm: str = 'HS256',
                 token_expiry: int = 3600):
        if not JWT_AVAILABLE:
            raise ImportError("PyJWT required: pip install pyjwt")

        self.secret_key = secret_key or 'default-secret-change-in-production'
        self.algorithm = algorithm
        self.token_expiry = token_expiry

    def generate_token(self, subject: str, roles: list = None,
                       additional_claims: dict = None) -> str:
        """Generate a JWT token."""
        now = int(time.time())
        payload = {
            'sub': subject,
            'iat': now,
            'exp': now + self.token_expiry,
            'roles': roles or []
        }
        if additional_claims:
            payload.update(additional_claims)

        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def verify_token(self, token: str) -> Tuple[bool, Optional[Dict], Optional[str]]:
        """
        Verify JWT token.
        Returns: (is_valid, payload, error_message)
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return True, payload, None
        except jwt.ExpiredSignatureError:
            return False, None, "Token expired"
        except jwt.InvalidTokenError as e:
            return False, None, f"Invalid token: {str(e)}"

    def get_subject(self, token: str) -> Optional[str]:
        """Extract subject from token."""
        valid, payload, _ = self.verify_token(token)
        return payload.get('sub') if valid else None

    def has_role(self, token: str, role: str) -> bool:
        """Check if token has specific role."""
        valid, payload, _ = self.verify_token(token)
        if not valid:
            return False
        return role in payload.get('roles', [])


class APIKeyAuth:
    """Simple API key authentication."""

    def __init__(self, valid_keys: Dict[str, str] = None):
        # Format: {api_key: (description, roles)}
        self.valid_keys = valid_keys or {
            'dev-key-001': ('Development key', ['read', 'write']),
            'prod-key-001': ('Production key', ['read']),
        }

    def verify_key(self, api_key: str) -> Tuple[bool, Optional[str], Optional[list]]:
        """
        Verify API key.
        Returns: (is_valid, description, roles)
        """
        if api_key in self.valid_keys:
            desc, roles = self.valid_keys[api_key]
            return True, desc, roles
        return False, None, None


class AuthManager:
    """Manages multiple authentication methods."""

    def __init__(self, jwt_secret: str = None, api_keys: Dict[str, str] = None):
        self.jwt_auth = JWTAuth(secret_key=jwt_secret) if jwt_secret else None
        self.api_key_auth = APIKeyAuth(api_keys)

    def authenticate_request(self, auth_header: Optional[str],
                             api_key: Optional[str] = None) -> Tuple[bool, str, list]:
        """
        Authenticate request using Authorization header or API key.
        Returns: (is_authenticated, auth_type, roles)
        """
        # Try JWT from Authorization header
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
            if self.jwt_auth:
                valid, payload, error = self.jwt_auth.verify_token(token)
                if valid:
                    return True, 'jwt', payload.get('roles', [])
                logger.warning(f"JWT auth failed: {error}")

        # Try API key
        if api_key:
            valid, desc, roles = self.api_key_auth.verify_key(api_key)
            if valid:
                return True, 'api_key', roles

        return False, 'none', []


# Global auth manager
_auth_manager = None


def init_auth(jwt_secret: str = None, api_keys: Dict[str, str] = None):
    global _auth_manager
    _auth_manager = AuthManager(jwt_secret=jwt_secret, api_keys=api_keys)


def get_auth_manager() -> AuthManager:
    return _auth_manager