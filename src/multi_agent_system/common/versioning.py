"""API Version management with backwards compatibility."""

import logging
import re
from typing import Callable, Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger('versioning')


class APIVersion(Enum):
    V1 = "v1"
    V2 = "v2"
    LATEST = "v2"


class VersionManager:
    """
    Manages API versioning with backwards compatibility.
    Routes requests to appropriate handlers based on version.
    """

    def __init__(self, default_version: APIVersion = APIVersion.V1):
        self.default_version = default_version
        self._routes: Dict[str, Dict[str, Callable]] = {}
        self._deprecation_warnings: Dict[str, str] = {}

    def register(self, endpoint: str, version: APIVersion, handler: Callable):
        """Register a handler for a specific version of an endpoint."""
        if endpoint not in self._routes:
            self._routes[endpoint] = {}
        self._routes[endpoint][version.value] = handler
        logger.info(f'Registered {version.value} handler for {endpoint}')

    def register_deprecated(self, endpoint: str, deprecated_in: APIVersion,
                          removed_in: str, message: str):
        """Register deprecation warning for an endpoint."""
        key = f"{deprecated_in.value}:{endpoint}"
        self._deprecation_warnings[key] = message
        logger.warning(f'Deprecated {endpoint} in {deprecated_in.value}, removed in {removed_in}')

    def get_handler(self, endpoint: str, version: APIVersion = None) -> Tuple[Optional[Callable], Optional[str]]:
        """Get handler for endpoint and version. Returns (handler, version_used)."""
        if endpoint not in self._routes:
            return None, None

        # Use specified version or default
        version = version or self.default_version

        # Try exact version match first
        if version.value in self._routes[endpoint]:
            return self._routes[endpoint][version.value], version.value

        # Fall back to latest available version
        latest = APIVersion.LATEST
        if latest.value in self._routes[endpoint]:
            logger.info(f'No handler for {version.value}, using {latest.value}')
            return self._routes[endpoint][latest.value], latest.value

        # Try default version
        if self.default_version.value in self._routes[endpoint]:
            return self._routes[endpoint][self.default_version.value], self.default_version.value

        return None, None

    def parse_version_from_path(self, path: str) -> Tuple[Optional[str], str]:
        """Parse version from URL path like /v1/tasks -> (v1, /tasks)."""
        # Match /v1/, /v2/, etc.
        match = re.match(r'^(/v\d+)(/.*)$', path)
        if match:
            return match.group(1).lstrip('/'), match.group(2)
        return None, path

    def add_version_header(self, response: Dict, version: str) -> Dict:
        """Add API version info to response headers."""
        response['_api_version'] = version
        response['_api_version_info'] = {
            'version': version,
            'deprecated': f"{version}:{response.get('_endpoint', '')}" in self._deprecation_warnings
        }
        return response

    def get_registered_versions(self, endpoint: str) -> List[str]:
        """Get list of available versions for an endpoint."""
        if endpoint in self._routes:
            return list(self._routes[endpoint].keys())
        return []


class VersionedRouter:
    """Routes requests to versioned handlers."""

    def __init__(self, version_manager: VersionManager = None):
        self._vm = version_manager or VersionManager()

    def route(self, path: str, method: str, handler_v1: Callable,
              handler_v2: Callable = None) -> Optional[Callable]:
        """Route request based on version in path."""
        version_str, clean_path = self._vm.parse_version_from_path(path)

        version = APIVersion.V1
        if version_str == 'v2':
            version = APIVersion.V2

        handler, used_version = self._vm.get_handler(clean_path, version)

        if handler:
            logger.debug(f'Routed {method} {path} to {used_version}')
            return handler

        # No handler found, try to use provided handlers based on version
        if version == APIVersion.V2 and handler_v2:
            return handler_v2

        return handler_v1


class CompatibilityLayer:
    """
    Provides backwards compatibility between API versions.
    Transforms request/response data for version compatibility.
    """

    def __init__(self):
        self._transforms: Dict[str, Callable] = {}

    def register_transform(self, from_version: str, to_version: str,
                          endpoint: str, transform: Callable):
        """Register a transformation function for request/response mapping."""
        key = f"{from_version}:{to_version}:{endpoint}"
        self._transforms[key] = transform

    def transform_request(self, version_from: str, version_to: str,
                         endpoint: str, data: Dict) -> Dict:
        """Transform request data from one version to another."""
        key = f"{version_from}:{version_to}:{endpoint}"
        if key in self._transforms:
            return self._transforms[key](data)
        return data

    def transform_response(self, version_from: str, version_to: str,
                          endpoint: str, data: Dict) -> Dict:
        """Transform response data from one version to another."""
        # For responses, transform in opposite direction
        key = f"{version_to}:{version_from}:{endpoint}"
        if key in self._transforms:
            return self._transforms[key](data)
        return data


class APIVersionMiddleware:
    """HTTP middleware for API versioning."""

    def __init__(self, version_manager: VersionManager = None):
        self._vm = version_manager or VersionManager()

    def process_request(self, path: str, headers: Dict, body: Dict) -> Tuple[Dict, Dict, str]:
        """
        Process incoming request, extract version, transform if needed.
        Returns: (transformed_body, updated_headers, clean_path)
        """
        version_str, clean_path = self._vm.parse_version_from_path(path)

        headers['X-API-Version'] = version_str or self._vm.default_version.value

        # Add deprecation warning header if needed
        if version_str:
            dep_key = f"{version_str}:{clean_path}"
            if dep_key in self._vm._deprecation_warnings:
                headers['X-API-Deprecated'] = self._vm._deprecation_warnings[dep_key]

        return body, headers, clean_path

    def process_response(self, path: str, response: Dict) -> Dict:
        """Process outgoing response, add version headers."""
        version_str, _ = self._vm.parse_version_from_path(path)
        version = version_str or self._vm.default_version.value

        response['X-API-Version'] = version
        return response


# Global instances
_version_manager = VersionManager()
_compatibility = CompatibilityLayer()
_middleware = APIVersionMiddleware(_version_manager)


def get_version_manager() -> VersionManager:
    return _version_manager


def get_compatibility() -> CompatibilityLayer:
    return _compatibility


def get_middleware() -> APIVersionMiddleware:
    return _middleware