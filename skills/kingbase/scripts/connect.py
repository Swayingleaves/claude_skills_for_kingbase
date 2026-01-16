#!/usr/bin/env python3
"""
Kingbase Database Connection Manager

Manages connections to KingbaseES (人大金仓) database.
Supports connection pooling and context management.
"""

import sys
from contextlib import contextmanager
from typing import Optional, Any

# Try to import KingbaseES/PostgreSQL drivers
# KingbaseES is based on PostgreSQL and uses psycopg2
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import sqlalchemy
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import QueuePool
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from config import KingbaseConfig


class KingbaseConnectionError(Exception):
    """Exception raised for connection errors."""
    pass


class KingbaseConnection:
    """
    KingbaseES database connection wrapper.

    Provides a simple interface for connecting to and interacting with
    KingbaseES database using psycopg2 or SQLAlchemy.
    """

    def __init__(self, config: KingbaseConfig):
        """
        Initialize connection with configuration.

        Args:
            config: KingbaseConfig instance with connection parameters
        """
        self.config = config
        self._connection = None
        self._engine = None

    def connect(self) -> Any:
        """
        Establish database connection.

        Returns:
            Connection object (psycopg2 connection or SQLAlchemy engine)

        Raises:
            KingbaseConnectionError: If connection fails
        """
        # Validate configuration first
        is_valid, errors = self.config.validate()
        if not is_valid:
            raise KingbaseConnectionError(f"Invalid configuration: {errors}")

        # Try psycopg2 first (native PostgreSQL/KingbaseES driver)
        if PSYCOPG2_AVAILABLE:
            return self._connect_psycopg2()

        # Fall back to SQLAlchemy
        if SQLALCHEMY_AVAILABLE:
            return self._connect_sqlalchemy()

        raise KingbaseConnectionError(
            "No database driver available. "
            "Install psycopg2: pip install psycopg2-binary"
        )

    def _connect_psycopg2(self):
        """Connect using psycopg2 driver."""
        try:
            params = self.config.get_connection_params()
            # Remove options for psycopg2, set schema after connection
            params.pop("options", None)

            self._connection = psycopg2.connect(**params)
            self._connection.autocommit = False

            # Set search_path to schema
            with self._connection.cursor() as cursor:
                cursor.execute(f"SET search_path TO {self.config.schema}")

            return self._connection
        except psycopg2.Error as e:
            raise KingbaseConnectionError(f"Failed to connect: {e}")
        except Exception as e:
            raise KingbaseConnectionError(f"Unexpected error: {e}")

    def _connect_sqlalchemy(self):
        """Connect using SQLAlchemy engine."""
        try:
            params = self.config.get_connection_params()
            params.pop("options", None)

            # Build connection URL
            # Format: postgresql+psycopg2://user:password@host:port/database
            url = (
                f"postgresql://{params['user']}:{params['password']}"
                f"@{params['host']}:{params['port']}/{params['database']}"
            )

            self._engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                connect_args={
                    "connect_timeout": self.config.connect_timeout
                }
            )

            # Test connection
            with self._engine.connect() as conn:
                # Set schema
                conn.execute(text(f"SET search_path TO {self.config.schema}"))
                conn.commit()

            return self._engine
        except Exception as e:
            raise KingbaseConnectionError(f"Failed to connect with SQLAlchemy: {e}")

    def is_connected(self) -> bool:
        """
        Check if connection is active.

        Returns:
            True if connection is active, False otherwise
        """
        if self._connection:
            try:
                return not self._connection.closed
            except Exception:
                return False
        if self._engine:
            try:
                with self._engine.connect() as conn:
                    return True
            except Exception:
                return False
        return False

    def close(self):
        """Close database connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

        if self._engine:
            try:
                self._engine.dispose()
            except Exception:
                pass
            self._engine = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def cursor(self):
        """
        Get cursor for psycopg2 connection.

        Returns:
            Cursor object

        Raises:
            KingbaseConnectionError: If using SQLAlchemy engine
        """
        if not self._connection:
            raise KingbaseConnectionError("Not connected. Call connect() first.")
        return self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def commit(self):
        """Commit transaction."""
        if self._connection:
            self._connection.commit()

    def rollback(self):
        """Rollback transaction."""
        if self._connection:
            self._connection.rollback()


@contextmanager
def get_connection(config: Optional[KingbaseConfig] = None):
    """
    Context manager for database connection.

    Usage:
        with get_connection(config) as conn:
            # Use conn here
            pass

    Args:
        config: KingbaseConfig instance. If None, loads from environment.

    Yields:
        Connection object
    """
    if config is None:
        config = KingbaseConfig.from_env()

    conn = KingbaseConnection(config)
    try:
        yield conn.connect()
    finally:
        conn.close()


def test_connection(config: KingbaseConfig) -> dict:
    """
    Test database connection and return server info.

    Args:
        config: KingbaseConfig instance

    Returns:
        Dictionary with connection status and server information
    """
    result = {
        "success": False,
        "error": None,
        "server_version": None,
        "server_info": None
    }

    try:
        with get_connection(config) as conn:
            if hasattr(conn, 'cursor'):  # psycopg2
                with conn.cursor() as cursor:
                    # Get server version
                    cursor.execute("SELECT version()")
                    version_info = cursor.fetchone()["version"]

                    # Get current database
                    cursor.execute("SELECT current_database()")
                    current_db = cursor.fetchone()["current_database"]

                    # Get current user
                    cursor.execute("SELECT current_user")
                    current_user = cursor.fetchone()["current_user"]

                    result.update({
                        "success": True,
                        "server_version": version_info,
                        "server_info": {
                            "database": current_db,
                            "user": current_user,
                            "host": config.host,
                            "port": config.port
                        }
                    })
            else:  # SQLAlchemy
                with conn.connect() as db_conn:
                    # Get server version
                    version_result = db_conn.execute(text("SELECT version()"))
                    version_info = version_result.scalar()

                    # Get current database
                    db_result = db_conn.execute(text("SELECT current_database()"))
                    current_db = db_result.scalar()

                    # Get current user
                    user_result = db_conn.execute(text("SELECT current_user"))
                    current_user = user_result.scalar()

                    result.update({
                        "success": True,
                        "server_version": version_info,
                        "server_info": {
                            "database": current_db,
                            "user": current_user,
                            "host": config.host,
                            "port": config.port
                        }
                    })

    except KingbaseConnectionError as e:
        result["error"] = str(e)
    except Exception as e:
        result["error"] = f"Unexpected error: {e}"

    return result


def main():
    """Test connection module."""
    print("=== Kingbase Connection Test ===\n")

    # Check driver availability
    print("1. Driver availability:")
    print(f"   psycopg2: {'✓ Available' if PSYCOPG2_AVAILABLE else '✗ Not installed'}")
    print(f"   SQLAlchemy: {'✓ Available' if SQLALCHEMY_AVAILABLE else '✗ Not installed'}")

    if not PSYCOPG2_AVAILABLE and not SQLALCHEMY_AVAILABLE:
        print("\n   ⚠ No database driver found!")
        print("   Install with: pip install psycopg2-binary")
        sys.exit(1)

    # Test with default config (will likely fail without actual database)
    print("\n2. Connection test (using environment variables):")
    config = KingbaseConfig.from_env()
    print(f"   Config: {config.get_redacted_connection_string()}")

    result = test_connection(config)
    print(f"   Success: {result['success']}")
    if result['success']:
        print(f"   Server: {result['server_version'][:60]}...")
        print(f"   Database: {result['server_info']['database']}")
        print(f"   User: {result['server_info']['user']}")
    else:
        print(f"   Error: {result['error']}")


if __name__ == "__main__":
    main()
