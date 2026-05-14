"""Security utilities for authentication and authorization."""

import hashlib
import hmac
import logging
import secrets
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('security')


class Permission(Enum):
    """Permission types."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"


@dataclass
class User:
    """User account."""
    user_id: str
    username: str
    password_hash: str
    roles: List[str] = field(default_factory=list)
    permissions: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_login: float = None
    is_active: bool = True


@dataclass
class TokenInfo:
    """Token information."""
    token: str
    user_id: str
    created_at: float
    expires_at: float
    scopes: List[str] = field(default_factory=list)


class PasswordHasher:
    """Secure password hashing."""

    @staticmethod
    def hash(password: str, salt: str = None) -> tuple:
        """Hash a password. Returns (hash, salt)."""
        salt = salt or secrets.token_hex(16)
        key = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode(),
            salt.encode(),
            100000
        )
        return key.hex(), salt

    @staticmethod
    def verify(password: str, password_hash: str, salt: str) -> bool:
        """Verify a password against a hash."""
        key, _ = PasswordHasher.hash(password, salt)
        return hmac.compare_digest(key, password_hash)


class TokenManager:
    """Token generation and validation."""

    def __init__(self, secret_key: str = None, token_ttl: int = 3600):
        self.secret_key = secret_key or secrets.token_hex(32)
        self.token_ttl = token_ttl
        self._tokens: Dict[str, TokenInfo] = {}
        self._lock = threading.RLock()

    def generate_token(self, user_id: str, scopes: List[str] = None) -> str:
        """Generate a new token."""
        token = secrets.token_urlsafe(32)
        expires_at = time.time() + self.token_ttl

        token_info = TokenInfo(
            token=token,
            user_id=user_id,
            created_at=time.time(),
            expires_at=expires_at,
            scopes=scopes or []
        )

        with self._lock:
            self._tokens[token] = token_info

        return token

    def validate_token(self, token: str) -> Optional[TokenInfo]:
        """Validate a token."""
        with self._lock:
            token_info = self._tokens.get(token)

            if not token_info:
                return None

            if time.time() > token_info.expires_at:
                del self._tokens[token]
                return None

            return token_info

    def revoke_token(self, token: str) -> bool:
        """Revoke a token."""
        with self._lock:
            if token in self._tokens:
                del self._tokens[token]
                return True
        return False

    def cleanup_expired(self) -> int:
        """Clean up expired tokens."""
        now = time.time()
        removed = 0

        with self._lock:
            expired = [
                token for token, info in self._tokens.items()
                if now > info.expires_at
            ]

            for token in expired:
                del self._tokens[token]
                removed += 1

        return removed


class RBACEngine:
    """
    Role-Based Access Control engine.
    """

    def __init__(self):
        self._roles: Dict[str, List[str]] = {}  # role -> permissions
        self._user_roles: Dict[str, List[str]] = {}  # user_id -> roles
        self._lock = threading.RLock()

    def define_role(self, role: str, permissions: List[str]):
        """Define a role with permissions."""
        with self._lock:
            self._roles[role] = permissions
            logger.info(f"Defined role: {role} with {len(permissions)} permissions")

    def assign_role(self, user_id: str, role: str):
        """Assign a role to a user."""
        with self._lock:
            if user_id not in self._user_roles:
                self._user_roles[user_id] = []
            if role not in self._user_roles[user_id]:
                self._user_roles[user_id].append(role)

    def revoke_role(self, user_id: str, role: str):
        """Revoke a role from a user."""
        with self._lock:
            if user_id in self._user_roles:
                if role in self._user_roles[user_id]:
                    self._user_roles[user_id].remove(role)

    def has_permission(self, user_id: str, permission: str) -> bool:
        """Check if user has a permission."""
        with self._lock:
            roles = self._user_roles.get(user_id, [])

            for role in roles:
                if role in self._roles:
                    if permission in self._roles[role]:
                        return True
                    if 'admin' in self._roles[role]:  # Admin has all permissions
                        return True

        return False

    def get_user_permissions(self, user_id: str) -> List[str]:
        """Get all permissions for a user."""
        with self._lock:
            roles = self._user_roles.get(user_id, [])
            permissions = []

            for role in roles:
                if role in self._roles:
                    permissions.extend(self._roles[role])

            return list(set(permissions))


class SecurityContext:
    """Security context for the current request."""

    def __init__(self, user_id: str = None, token: str = None):
        self.user_id = user_id
        self.token = token
        self._previous_context = None

    def __enter__(self):
        global _current_context
        self._previous_context = _current_context
        _current_context = self
        return self

    def __exit__(self, *args):
        global _current_context
        _current_context = self._previous_context


# Global instances
_password_hasher = PasswordHasher()
_token_manager = TokenManager()
_rbac_engine = RBACEngine()
_current_context: Optional[SecurityContext] = None


def get_token_manager() -> TokenManager:
    return _token_manager


def get_rbac_engine() -> RBACEngine:
    return _rbac_engine


def hash_password(password: str) -> tuple:
    """Hash a password."""
    return _password_hasher.hash(password)


def verify_password(password: str, hash: str, salt: str) -> bool:
    """Verify a password."""
    return _password_hasher.verify(password, hash, salt)


def generate_token(user_id: str, scopes: List[str] = None) -> str:
    """Generate a token for a user."""
    return _token_manager.generate_token(user_id, scopes)


def validate_token(token: str) -> Optional[TokenInfo]:
    """Validate a token."""
    return _token_manager.validate_token(token)


def require_permission(permission: str):
    """Decorator to require a permission."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            context = _current_context
            if not context or not context.user_id:
                raise Exception("Authentication required")

            if not _rbac_engine.has_permission(context.user_id, permission):
                raise Exception(f"Permission denied: {permission}")

            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_role(role: str):
    """Decorator to require a role."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            context = _current_context
            if not context or not context.user_id:
                raise Exception("Authentication required")

            roles = _rbac_engine._user_roles.get(context.user_id, [])
            if role not in roles:
                raise Exception(f"Role required: {role}")

            return func(*args, **kwargs)
        return wrapper
    return decorator