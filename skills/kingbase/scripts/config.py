#!/usr/bin/env python3
"""
Kingbase Database Configuration Manager

Manages database connection parameters from environment variables or user input.
Supports KingbaseES (人大金仓) database connections.
"""

import os
import sys
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class KingbaseConfig:
    """KingbaseES database connection configuration."""

    host: str = "localhost"
    port: int = 54321
    database: str = "test"
    user: str = "system"
    password: str = ""
    schema: str = "public"
    connect_timeout: int = 10
    application_name: str = "kingbase_skill"

    @classmethod
    def from_env(cls) -> "KingbaseConfig":
        """
        Load configuration from environment variables.

        Environment variables:
            KINGBASE_HOST - Database host (default: localhost)
            KINGBASE_PORT - Database port (default: 54321)
            KINGBASE_DATABASE - Database name (default: test)
            KINGBASE_USER - Database user (default: system)
            KINGBASE_PASSWORD - Database password (default: empty)
            KINGBASE_SCHEMA - Database schema (default: public)
            KINGBASE_CONNECT_TIMEOUT - Connection timeout in seconds (default: 10)
        """
        return cls(
            host=os.getenv("KINGBASE_HOST", "localhost"),
            port=int(os.getenv("KINGBASE_PORT", "54321")),
            database=os.getenv("KINGBASE_DATABASE", "test"),
            user=os.getenv("KINGBASE_USER", "system"),
            password=os.getenv("KINGBASE_PASSWORD", ""),
            schema=os.getenv("KINGBASE_SCHEMA", "public"),
            connect_timeout=int(os.getenv("KINGBASE_CONNECT_TIMEOUT", "10")),
        )

    @classmethod
    def from_dict(cls, config_dict: dict) -> "KingbaseConfig":
        """
        Load configuration from dictionary.

        Args:
            config_dict: Dictionary containing connection parameters

        Returns:
            KingbaseConfig instance
        """
        # Filter out None values and use defaults
        filtered = {k: v for k, v in config_dict.items() if v is not None}
        return cls(**filtered)

    def get_connection_params(self) -> dict:
        """
        Get connection parameters for psycopg2/kdb driver.

        Returns:
            Dictionary of connection parameters
        """
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
            "options": f"-c search_path={self.schema}",
        }

    def get_connection_string(self) -> str:
        """
        Get connection string for KingbaseES.

        Returns:
            Connection string in format: host=localhost port=54321 ...
        """
        params = self.get_connection_params()
        # Remove password from options if present
        params.pop("options", None)
        return " ".join(f"{k}={v}" for k, v in params.items())

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate configuration parameters.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        if not self.host:
            errors.append("Host cannot be empty")
        if not (1 <= self.port <= 65535):
            errors.append(f"Port must be between 1 and 65535, got: {self.port}")
        if not self.database:
            errors.append("Database name cannot be empty")
        if not self.user:
            errors.append("User cannot be empty")
        if self.connect_timeout <= 0:
            errors.append(f"Timeout must be positive, got: {self.connect_timeout}")

        return len(errors) == 0, errors

    def get_redacted_connection_string(self) -> str:
        """
        Get connection string with password redacted for logging.

        Returns:
            Connection string with password hidden
        """
        params = self.get_connection_params()
        params["password"] = "***"
        params.pop("options", None)
        return " ".join(f"{k}={v}" for k, v in params.items())


def interactive_config() -> KingbaseConfig:
    """
    Interactively collect configuration from user.

    Returns:
        KingbaseConfig instance with user-provided values
    """
    print("\n=== KingbaseES Database Configuration ===")
    print("Press Enter to use default values shown in brackets\n")

    config = KingbaseConfig()

    config.host = input(f"Host [{config.host}]: ").strip() or config.host
    port_input = input(f"Port [{config.port}]: ").strip()
    config.port = int(port_input) if port_input else config.port
    config.database = input(f"Database [{config.database}]: ").strip() or config.database
    config.user = input(f"User [{config.user}]: ").strip() or config.user
    config.password = input("Password: ").strip() or config.password
    schema_input = input(f"Schema [{config.schema}]: ").strip()
    config.schema = schema_input if schema_input else config.schema

    return config


def main():
    """Test configuration module."""
    print("=== Kingbase Configuration Test ===\n")

    # Test default config
    print("1. Default configuration:")
    config = KingbaseConfig()
    print(f"   Connection string: {config.get_redacted_connection_string()}")

    # Test from environment
    print("\n2. From environment:")
    os.environ["KINGBASE_HOST"] = "192.168.1.100"
    os.environ["KINGBASE_PORT"] = "54322"
    os.environ["KINGBASE_PASSWORD"] = "secret123"
    config = KingbaseConfig.from_env()
    print(f"   Connection string: {config.get_redacted_connection_string()}")

    # Test from dict
    print("\n3. From dictionary:")
    config = KingbaseConfig.from_dict({
        "host": "db.example.com",
        "database": "production",
        "user": "admin"
    })
    print(f"   Connection string: {config.get_redacted_connection_string()}")

    # Test validation
    print("\n4. Validation tests:")
    valid, errors = KingbaseConfig(host="", port=99999).validate()
    print(f"   Invalid config errors: {errors}")

    valid, errors = config.validate()
    print(f"   Valid config: {valid}")


if __name__ == "__main__":
    main()
