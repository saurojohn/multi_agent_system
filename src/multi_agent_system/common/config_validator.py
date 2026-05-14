"""Configuration validation and schema enforcement."""

import logging
import re
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('config_validator')


class ValidationLevel(Enum):
    """Validation strictness levels."""
    STRICT = "strict"     # Fail on any validation error
    WARN = "warn"        # Log warnings but continue
    PERMISSIVE = "permissive"  # Skip validation


class ConfigType(Enum):
    """Configuration value types."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    LIST = "list"
    DICT = "dict"
    ENUM = "enum"
    PATTERN = "pattern"


@dataclass
class FieldSchema:
    """Schema for a configuration field."""
    name: str
    field_type: ConfigType
    required: bool = True
    default: Any = None
    description: str = ""
    min_value: float = None
    max_value: float = None
    min_length: int = None
    max_length: int = None
    pattern: str = None
    allowed_values: List[Any] = None
    nested_schema: Dict[str, 'FieldSchema'] = None  # For dict type


@dataclass
class ValidationIssue:
    """A validation issue."""
    field_path: str
    severity: str  # "error", "warning", "info"
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of configuration validation."""
    valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    validated_config: Dict = field(default_factory=dict)


class ConfigSchema:
    """Schema for a configuration section."""

    def __init__(self, name: str):
        self.name = name
        self._fields: Dict[str, FieldSchema] = {}

    def add_field(self, field_schema: FieldSchema) -> 'ConfigSchema':
        """Add a field to the schema."""
        self._fields[field_schema.name] = field_schema
        return self

    def string_field(self, name: str, **kwargs) -> 'ConfigSchema':
        """Add a string field."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.STRING, **kwargs))

    def integer_field(self, name: str, **kwargs) -> 'ConfigSchema':
        """Add an integer field."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.INTEGER, **kwargs))

    def float_field(self, name: str, **kwargs) -> 'ConfigSchema':
        """Add a float field."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.FLOAT, **kwargs))

    def boolean_field(self, name: str, **kwargs) -> 'ConfigSchema':
        """Add a boolean field."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.BOOLEAN, **kwargs))

    def list_field(self, name: str, **kwargs) -> 'ConfigSchema':
        """Add a list field."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.LIST, **kwargs))

    def dict_field(self, name: str, nested_schema: Dict[str, FieldSchema] = None, **kwargs) -> 'ConfigSchema':
        """Add a dict field with nested schema."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.DICT, nested_schema=nested_schema, **kwargs))

    def enum_field(self, name: str, allowed_values: List[Any], **kwargs) -> 'ConfigSchema':
        """Add an enum field."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.ENUM, allowed_values=allowed_values, **kwargs))

    def pattern_field(self, name: str, pattern: str, **kwargs) -> 'ConfigSchema':
        """Add a pattern field (validated against regex)."""
        return self.add_field(FieldSchema(name=name, field_type=ConfigType.PATTERN, pattern=pattern, **kwargs))

    def get_field(self, name: str) -> Optional[FieldSchema]:
        """Get a field by name."""
        return self._fields.get(name)

    def get_all_fields(self) -> Dict[str, FieldSchema]:
        """Get all fields."""
        return dict(self._fields)


class ConfigValidator:
    """
    Validates configuration against schemas.
    """

    def __init__(self, level: ValidationLevel = ValidationLevel.STRICT):
        self.level = level
        self._schemas: Dict[str, ConfigSchema] = {}
        self._custom_validators: Dict[str, Callable] = {}

    def register_schema(self, schema: ConfigSchema):
        """Register a configuration schema."""
        self._schemas[schema.name] = schema

    def add_schema(self, name: str, fields: List[FieldSchema]) -> ConfigSchema:
        """Add a schema by name with fields."""
        schema = ConfigSchema(name)
        for field_schema in fields:
            schema.add_field(field_schema)
        self.register_schema(schema)
        return schema

    def register_custom_validator(self, field_path: str, validator: Callable):
        """Register a custom validator function."""
        self._custom_validators[field_path] = validator

    def validate(self, config: Dict, schema_name: str) -> ValidationResult:
        """Validate configuration against a schema."""
        schema = self._schemas.get(schema_name)

        if not schema:
            return ValidationResult(
                valid=False,
                issues=[ValidationIssue(
                    field_path="",
                    severity="error",
                    message=f"Schema '{schema_name}' not found"
                )]
            )

        return self._validate_against_schema(config, schema)

    def _validate_against_schema(self, config: Dict, schema: ConfigSchema) -> ValidationResult:
        """Validate config against a schema."""
        issues = []
        validated = {}

        for field_name, field_schema in schema._fields.items():
            value = config.get(field_name)

            # Check required
            if value is None:
                if field_schema.required:
                    issues.append(ValidationIssue(
                        field_path=f"{schema.name}.{field_name}",
                        severity="error",
                        message=f"Required field missing: {field_name}",
                        value=None
                    ))
                if field_schema.default is not None:
                    value = field_schema.default
                    validated[field_name] = value
                continue

            # Validate type
            type_issues = self._validate_type(value, field_schema, field_name)
            issues.extend(type_issues)

            if not any(i.severity == "error" for i in type_issues):
                validated[field_name] = value

        valid = not any(i.severity == "error" for i in issues)
        return ValidationResult(valid=valid, issues=issues, validated_config=validated)

    def _validate_type(self, value: Any, field_schema: FieldSchema, field_path: str) -> List[ValidationIssue]:
        """Validate a value against its type schema."""
        issues = []
        path = f"{field_schema.name}.{field_path}"

        if field_schema.field_type == ConfigType.STRING:
            if not isinstance(value, str):
                issues.append(ValidationIssue(path, "error", f"Expected string, got {type(value).__name__}", value))
            else:
                if field_schema.min_length and len(value) < field_schema.min_length:
                    issues.append(ValidationIssue(path, "error", f"String too short: {len(value)} < {field_schema.min_length}", value))
                if field_schema.max_length and len(value) > field_schema.max_length:
                    issues.append(ValidationIssue(path, "error", f"String too long: {len(value)} > {field_schema.max_length}", value))
                if field_schema.pattern and not re.match(field_schema.pattern, value):
                    issues.append(ValidationIssue(path, "error", f"String does not match pattern: {field_schema.pattern}", value))

        elif field_schema.field_type == ConfigType.INTEGER:
            if not isinstance(value, int) or isinstance(value, bool):
                issues.append(ValidationIssue(path, "error", f"Expected integer, got {type(value).__name__}", value))
            else:
                if field_schema.min_value is not None and value < field_schema.min_value:
                    issues.append(ValidationIssue(path, "error", f"Integer too small: {value} < {field_schema.min_value}", value))
                if field_schema.max_value is not None and value > field_schema.max_value:
                    issues.append(ValidationIssue(path, "error", f"Integer too large: {value} > {field_schema.max_value}", value))

        elif field_schema.field_type == ConfigType.FLOAT:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                issues.append(ValidationIssue(path, "error", f"Expected float, got {type(value).__name__}", value))
            else:
                if field_schema.min_value is not None and value < field_schema.min_value:
                    issues.append(ValidationIssue(path, "error", f"Float too small: {value} < {field_schema.min_value}", value))
                if field_schema.max_value is not None and value > field_schema.max_value:
                    issues.append(ValidationIssue(path, "error", f"Float too large: {value} > {field_schema.max_value}", value))

        elif field_schema.field_type == ConfigType.BOOLEAN:
            if not isinstance(value, bool):
                issues.append(ValidationIssue(path, "error", f"Expected boolean, got {type(value).__name__}", value))

        elif field_schema.field_type == ConfigType.LIST:
            if not isinstance(value, list):
                issues.append(ValidationIssue(path, "error", f"Expected list, got {type(value).__name__}", value))
            else:
                if field_schema.min_length and len(value) < field_schema.min_length:
                    issues.append(ValidationIssue(path, "error", f"List too short: {len(value)} < {field_schema.min_length}", value))
                if field_schema.max_length and len(value) > field_schema.max_length:
                    issues.append(ValidationIssue(path, "error", f"List too long: {len(value)} > {field_schema.max_length}", value))

        elif field_schema.field_type == ConfigType.ENUM:
            if field_schema.allowed_values and value not in field_schema.allowed_values:
                issues.append(ValidationIssue(path, "error", f"Value not in allowed values: {field_schema.allowed_values}", value))

        elif field_schema.field_type == ConfigType.PATTERN:
            if not isinstance(value, str):
                issues.append(ValidationIssue(path, "error", f"Expected string for pattern validation", value))
            elif field_schema.pattern and not re.match(field_schema.pattern, value):
                issues.append(ValidationIssue(path, "error", f"String does not match pattern: {field_schema.pattern}", value))

        return issues


class ConfigMerger:
    """
    Merges configuration from multiple sources.
    """

    def __init__(self):
        self._overrides: Dict[str, Any] = {}

    def set_override(self, key: str, value: Any):
        """Set an override value."""
        self._overrides[key] = value

    def merge(self, *configs: Dict) -> Dict:
        """Merge multiple configuration dicts."""
        result = {}

        for config in configs:
            result = self._deep_merge(result, config)

        # Apply overrides
        for key, value in self._overrides.items():
            keys = key.split('.')
            self._set_nested(result, keys, value)

        return result

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dicts."""
        result = dict(base)

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def _set_nested(self, d: Dict, keys: List[str], value: Any):
        """Set a nested value."""
        for key in keys[:-1]:
            if key not in d:
                d[key] = {}
            d = d[key]
        d[keys[-1]] = value


class EnvironmentConfigLoader:
    """
    Loads configuration from environment variables.
    """

    def __init__(self, prefix: str = "APP_", separator: str = "_"):
        self.prefix = prefix
        self.separator = separator

    def load(self) -> Dict:
        """Load configuration from environment."""
        import os
        config = {}

        for key, value in os.environ.items():
            if not key.startswith(self.prefix):
                continue

            # Remove prefix
            config_key = key[len(self.prefix):]

            # Convert to nested dict using separator
            parts = config_key.split(self.separator)
            self._set_nested(config, parts, self._parse_value(value))

        return config

    def _parse_value(self, value: str) -> Any:
        """Parse environment variable value."""
        # Try boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'

        # Try number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except:
            pass

        # Try JSON
        import json
        try:
            return json.loads(value)
        except:
            pass

        return value

    def _set_nested(self, d: Dict, keys: List[str], value: Any):
        """Set nested value."""
        for key in keys[:-1]:
            if key not in d:
                d[key] = {}
            d = d[key]
        d[keys[-1]] = value


# Global validator
_default_validator = ConfigValidator()


def get_validator() -> ConfigValidator:
    return _default_validator


def validate_config(config: Dict, schema_name: str) -> ValidationResult:
    """Validate configuration."""
    return _default_validator.validate(config, schema_name)


def create_schema(name: str) -> ConfigSchema:
    """Create a new configuration schema."""
    return ConfigSchema(name)