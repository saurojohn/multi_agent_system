"""Data validation utilities for input sanitization and validation."""

import re
import logging
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('validation')


class ValidationError(Exception):
    """Validation error with field information."""
    def __init__(self, errors: List[Dict]):
        self.errors = errors
        super().__init__(str(errors))


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class Validator:
    """Base validator class."""

    def validate(self, value: Any) -> ValidationResult:
        """Validate a value."""
        raise NotImplementedError


class StringValidator(Validator):
    """String validation."""

    def __init__(self, min_length: int = 0, max_length: int = None,
                 pattern: str = None, allow_empty: bool = True):
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = re.compile(pattern) if pattern else None
        self.allow_empty = allow_empty

    def validate(self, value: Any) -> ValidationResult:
        errors = []
        warnings = []

        if not isinstance(value, str):
            errors.append(f"Expected string, got {type(value).__name__}")
            return ValidationResult(valid=False, errors=errors)

        if not value and not self.allow_empty:
            errors.append("Empty string not allowed")
            return ValidationResult(valid=False, errors=errors)

        if len(value) < self.min_length:
            errors.append(f"String too short: {len(value)} < {self.min_length}")

        if self.max_length and len(value) > self.max_length:
            errors.append(f"String too long: {len(value)} > {self.max_length}")

        if self.pattern and not self.pattern.match(value):
            errors.append(f"String does not match pattern: {self.pattern.pattern}")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class NumberValidator(Validator):
    """Number validation."""

    def __init__(self, min_value: float = None, max_value: float = None,
                 integer_only: bool = False):
        self.min_value = min_value
        self.max_value = max_value
        self.integer_only = integer_only

    def validate(self, value: Any) -> ValidationResult:
        errors = []

        try:
            if self.integer_only:
                if not isinstance(value, int):
                    if isinstance(value, float) and not value.is_integer():
                        errors.append("Expected integer")
                        return ValidationResult(valid=False, errors=errors)
                    value = int(value)
            else:
                value = float(value)
        except (TypeError, ValueError):
            errors.append(f"Cannot convert to number: {value}")
            return ValidationResult(valid=False, errors=errors)

        if self.min_value is not None and value < self.min_value:
            errors.append(f"Value too small: {value} < {self.min_value}")

        if self.max_value is not None and value > self.max_value:
            errors.append(f"Value too large: {value} > {self.max_value}")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class EmailValidator(Validator):
    """Email validation."""

    EMAIL_PATTERN = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )

    def validate(self, value: Any) -> ValidationResult:
        errors = []

        if not isinstance(value, str):
            errors.append("Expected string")
            return ValidationResult(valid=False, errors=errors)

        if not self.EMAIL_PATTERN.match(value):
            errors.append("Invalid email format")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class URLValidator(Validator):
    """URL validation."""

    def __init__(self, allowed_schemes: List[str] = None):
        self.allowed_schemes = allowed_schemes or ['http', 'https']

    def validate(self, value: Any) -> ValidationResult:
        errors = []

        if not isinstance(value, str):
            errors.append("Expected string")
            return ValidationResult(valid=False, errors=errors)

        # Basic URL pattern
        url_pattern = re.compile(
            r'^(https?|ftp)://[^\s/$.?#].[^\s]*$',
            re.IGNORECASE
        )

        if not url_pattern.match(value):
            errors.append("Invalid URL format")
            return ValidationResult(valid=False, errors=errors)

        # Check scheme
        from urllib.parse import urlparse
        try:
            parsed = urlparse(value)
            if parsed.scheme not in self.allowed_schemes:
                errors.append(f"Scheme must be one of: {self.allowed_schemes}")
        except:
            errors.append("Cannot parse URL")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class EnumValidator(Validator):
    """Enum validation."""

    def __init__(self, allowed_values: List[Any]):
        self.allowed_values = allowed_values

    def validate(self, value: Any) -> ValidationResult:
        errors = []

        if value not in self.allowed_values:
            errors.append(f"Value must be one of: {self.allowed_values}")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class ListValidator(Validator):
    """List validation."""

    def __init__(self, item_validator: Validator = None,
                 min_items: int = 0, max_items: int = None):
        self.item_validator = item_validator
        self.min_items = min_items
        self.max_items = max_items

    def validate(self, value: Any) -> ValidationResult:
        errors = []

        if not isinstance(value, list):
            errors.append("Expected list")
            return ValidationResult(valid=False, errors=errors)

        if len(value) < self.min_items:
            errors.append(f"List too short: {len(value)} < {self.min_items}")

        if self.max_items and len(value) > self.max_items:
            errors.append(f"List too long: {len(value)} > {self.max_items}")

        if self.item_validator:
            for i, item in enumerate(value):
                result = self.item_validator.validate(item)
                if not result.valid:
                    errors.append(f"Item {i}: {', '.join(result.errors)}")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class DictValidator(Validator):
    """Dictionary/schema validation."""

    def __init__(self, schema: Dict[str, Validator]):
        self.schema = schema

    def validate(self, value: Any) -> ValidationResult:
        errors = []
        warnings = []

        if not isinstance(value, dict):
            errors.append("Expected dict")
            return ValidationResult(valid=False, errors=errors)

        # Check required fields
        for field_name, validator in self.schema.items():
            if field_name not in value:
                errors.append(f"Missing required field: {field_name}")
                continue

            result = validator.validate(value[field_name])
            if not result.valid:
                errors.append(f"{field_name}: {', '.join(result.errors)}")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


class SchemaValidator:
    """
    Validates data against a schema.
    """

    def __init__(self):
        self._validators: Dict[str, Validator] = {}
        self._required_fields: List[str] = []

    def add_field(self, name: str, validator: Validator, required: bool = False):
        """Add a field to the schema."""
        self._validators[name] = validator
        if required:
            self._required_fields.append(name)

    def validate(self, data: Dict) -> ValidationResult:
        """Validate data against schema."""
        errors = []
        warnings = []

        # Check required fields
        for field_name in self._required_fields:
            if field_name not in data:
                errors.append(f"Missing required field: {field_name}")

        # Validate present fields
        for field_name, value in data.items():
            if field_name in self._validators:
                result = self._validators[field_name].validate(value)
                if not result.valid:
                    errors.append(f"{field_name}: {', '.join(result.errors)}")
                warnings.extend(result.warnings)

        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)


class Sanitizer:
    """
    Sanitizes input data.
    """

    @staticmethod
    def sanitize_string(value: str, strip: bool = True,
                       remove_control_chars: bool = True,
                       max_length: int = None) -> str:
        """Sanitize a string."""
        if not isinstance(value, str):
            return str(value)

        if strip:
            value = value.strip()

        if remove_control_chars:
            # Remove control characters except newlines and tabs
            value = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', value)

        if max_length:
            value = value[:max_length]

        return value

    @staticmethod
    def sanitize_html(value: str) -> str:
        """Remove HTML tags."""
        return re.sub(r'<[^>]+>', '', value)

    @staticmethod
    def sanitize_sql(value: str) -> str:
        """Basic SQL injection prevention."""
        # Remove or escape dangerous characters
        dangerous = ["'", '"', ';', '--', '/*', '*/', 'DROP', 'SELECT', 'UNION']
        for pattern in dangerous:
            value = value.replace(pattern, '')
        return value

    @staticmethod
    def sanitize_dict(data: Dict, fields: List[str] = None,
                     strip: bool = True) -> Dict:
        """Sanitize dictionary values."""
        result = {}
        for key, value in data.items():
            if fields and key not in fields:
                result[key] = value
                continue

            if isinstance(value, str):
                result[key] = Sanitizer.sanitize_string(value, strip=strip)
            elif isinstance(value, dict):
                result[key] = Sanitizer.sanitize_dict(value, strip=strip)
            elif isinstance(value, list):
                result[key] = [
                    Sanitizer.sanitize_string(v, strip=strip) if isinstance(v, str) else v
                    for v in value
                ]
            else:
                result[key] = value

        return result


# Input validation middleware
class InputValidationMiddleware:
    """
    Middleware for validating API inputs.
    """

    def __init__(self):
        self._schemas: Dict[str, SchemaValidator] = {}

    def register_schema(self, endpoint: str, schema: SchemaValidator):
        """Register a validation schema for an endpoint."""
        self._schemas[endpoint] = schema

    def validate_request(self, endpoint: str, data: Dict) -> ValidationResult:
        """Validate a request."""
        schema = self._schemas.get(endpoint)
        if not schema:
            return ValidationResult(valid=True)  # No schema = allow

        return schema.validate(data)


# Validation helper functions
def validate_email(email: str) -> bool:
    """Validate an email address."""
    return EmailValidator().validate(email).valid


def validate_url(url: str) -> bool:
    """Validate a URL."""
    return URLValidator().validate(url).valid


def validate_required_fields(data: Dict, required: List[str]) -> List[str]:
    """Check for required fields."""
    missing = [f for f in required if f not in data]
    return missing


def sanitize_user_input(value: str) -> str:
    """Sanitize user input."""
    return Sanitizer.sanitize_string(value, max_length=1000)