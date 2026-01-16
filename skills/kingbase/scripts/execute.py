#!/usr/bin/env python3
"""
Kingbase Database Execute Module

Executes non-SELECT SQL statements (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER).
Supports transaction management and batch operations.
"""

import time
from typing import Any, Optional, List
from dataclasses import dataclass
from enum import Enum

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    from sqlalchemy import text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from config import KingbaseConfig
from connect import KingbaseConnection


class StatementType(Enum):
    """Types of SQL statements."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    TRUNCATE = "TRUNCATE"
    GRANT = "GRANT"
    REVOKE = "REVOKE"
    OTHER = "OTHER"


@dataclass
class ExecuteResult:
    """Result of a non-SELECT SQL execution."""
    success: bool
    rows_affected: int
    execution_time: float
    error: Optional[str] = None
    statement_type: StatementType = StatementType.OTHER


def detect_statement_type(sql: str) -> StatementType:
    """
    Detect the type of SQL statement.

    Args:
        sql: SQL statement

    Returns:
        StatementType enum value
    """
    sql_upper = sql.strip().upper()

    # Check for each statement type in order
    for stmt_type in [
        StatementType.SELECT,
        StatementType.INSERT,
        StatementType.UPDATE,
        StatementType.DELETE,
        StatementType.CREATE,
        StatementType.DROP,
        StatementType.ALTER,
        StatementType.TRUNCATE,
        StatementType.GRANT,
        StatementType.REVOKE
    ]:
        if sql_upper.startswith(stmt_type.value):
            return stmt_type

    return StatementType.OTHER


def execute_statement(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    params: Optional[tuple] = None,
    auto_commit: bool = True
) -> ExecuteResult:
    """
    Execute a non-SELECT SQL statement.

    Args:
        sql: SQL statement (INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
        config: Database configuration
        params: Optional parameters for parameterized query
        auto_commit: Whether to auto-commit after execution

    Returns:
        ExecuteResult with execution details
    """
    start_time = time.time()
    result = ExecuteResult(
        success=False,
        rows_affected=0,
        execution_time=0
    )

    result.statement_type = detect_statement_type(sql)

    if config is None:
        config = KingbaseConfig.from_env()

    # Warn if executing SELECT
    if result.statement_type == StatementType.SELECT:
        print("⚠ Warning: SELECT statement detected. Consider using query.py instead.")

    conn = KingbaseConnection(config)

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):  # psycopg2
            with connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                result.rows_affected = cursor.rowcount

                if auto_commit:
                    connection.commit()

                result.success = True

        else:  # SQLAlchemy
            with connection.begin() as transaction:
                db_result = connection.execute(text(sql), params or ())
                result.rows_affected = db_result.rowcount

                if not auto_commit:
                    transaction.commit()

                result.success = True

    except psycopg2.Error as e:
        result.error = f"Database error: {e}"
        if conn._connection:
            conn.rollback()
    except Exception as e:
        result.error = f"Error: {e}"
        if conn._connection:
            conn.rollback()
    finally:
        conn.close()

    result.execution_time = time.time() - start_time
    return result


def execute_batch(
    sql: str,
    params_list: List[tuple],
    config: Optional[KingbaseConfig] = None
) -> ExecuteResult:
    """
    Execute a statement multiple times with different parameters.

    Args:
        sql: SQL statement with parameter placeholders
        params_list: List of parameter tuples
        config: Database configuration

    Returns:
        ExecuteResult with total rows affected
    """
    start_time = time.time()
    result = ExecuteResult(
        success=False,
        rows_affected=0,
        execution_time=0
    )

    if not params_list:
        result.success = True
        return result

    result.statement_type = detect_statement_type(sql)

    if config is None:
        config = KingbaseConfig.from_env()

    conn = KingbaseConnection(config)

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):  # psycopg2
            with connection.cursor() as cursor:
                # Use executemany for batch operations
                cursor.executemany(sql, params_list)
                result.rows_affected = cursor.rowcount
                connection.commit()
                result.success = True

        else:  # SQLAlchemy
            with connection.begin() as transaction:
                for params in params_list:
                    db_result = connection.execute(text(sql), params)
                    result.rows_affected += db_result.rowcount
                result.success = True

    except psycopg2.Error as e:
        result.error = f"Database error: {e}"
        conn.rollback()
    except Exception as e:
        result.error = f"Error: {e}"
        conn.rollback()
    finally:
        conn.close()

    result.execution_time = time.time() - start_time
    return result


def insert_data(
    table_name: str,
    data: dict,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> ExecuteResult:
    """
    Insert a single row into a table.

    Args:
        table_name: Name of the table
        data: Dictionary of column_name: value pairs
        config: Database configuration
        schema: Schema name (optional, uses config default if None)

    Returns:
        ExecuteResult
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    columns = list(data.keys())
    values = list(data.values())
    placeholders = ", ".join(["%s"] * len(columns))

    full_table_name = f"{schema}.{table_name}" if schema else table_name
    sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    return execute_statement(sql, config, params=tuple(values))


def update_data(
    table_name: str,
    data: dict,
    where_clause: str,
    where_params: Optional[tuple] = None,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> ExecuteResult:
    """
    Update rows in a table.

    Args:
        table_name: Name of the table
        data: Dictionary of column_name: value pairs to update
        where_clause: WHERE clause condition (without the "WHERE" keyword)
        where_params: Optional parameters for WHERE clause
        config: Database configuration
        schema: Schema name (optional)

    Returns:
        ExecuteResult
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
    values = list(data.values())
    if where_params:
        values.extend(where_params)

    full_table_name = f"{schema}.{table_name}" if schema else table_name
    sql = f"UPDATE {full_table_name} SET {set_clause} WHERE {where_clause}"

    return execute_statement(sql, config, params=tuple(values))


def delete_data(
    table_name: str,
    where_clause: str,
    where_params: Optional[tuple] = None,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> ExecuteResult:
    """
    Delete rows from a table.

    Args:
        table_name: Name of the table
        where_clause: WHERE clause condition (without the "WHERE" keyword)
        where_params: Optional parameters for WHERE clause
        config: Database configuration
        schema: Schema name (optional)

    Returns:
        ExecuteResult
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    full_table_name = f"{schema}.{table_name}" if schema else table_name
    sql = f"DELETE FROM {full_table_name} WHERE {where_clause}"

    return execute_statement(sql, config, params=where_params)


def create_table(
    table_name: str,
    columns: dict,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None,
    primary_key: Optional[str] = None,
    if_not_exists: bool = True
) -> ExecuteResult:
    """
    Create a new table.

    Args:
        table_name: Name of the table
        columns: Dictionary of column_name: type_definition pairs
        config: Database configuration
        schema: Schema name (optional)
        primary_key: Optional primary key column name
        if_not_exists: Add IF NOT EXISTS clause

    Returns:
        ExecuteResult
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    column_defs = [f"{name} {definition}" for name, definition in columns.items()]

    if primary_key:
        column_defs.append(f"PRIMARY KEY ({primary_key})")

    full_table_name = f"{schema}.{table_name}" if schema else table_name
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    sql = f"CREATE TABLE {exists_clause}{full_table_name} (\n    " + ",\n    ".join(column_defs) + "\n)"

    return execute_statement(sql, config)


def drop_table(
    table_name: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None,
    if_exists: bool = True,
    cascade: bool = False
) -> ExecuteResult:
    """
    Drop a table.

    Args:
        table_name: Name of the table
        config: Database configuration
        schema: Schema name (optional)
        if_exists: Add IF EXISTS clause
        cascade: Add CASCADE clause

    Returns:
        ExecuteResult
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    full_table_name = f"{schema}.{table_name}" if schema else table_name
    exists_clause = "IF EXISTS " if if_exists else ""
    cascade_clause = " CASCADE" if cascade else ""
    sql = f"DROP TABLE {exists_clause}{full_table_name}{cascade_clause}"

    return execute_statement(sql, config)


def format_execute_result(result: ExecuteResult) -> str:
    """
    Format execution result as string.

    Args:
        result: ExecuteResult from execute_statement

    Returns:
        Formatted result string
    """
    if result.success:
        status = "✓ Success"
        details = f"{result.rows_affected} row(s) affected"
        if result.rows_affected == 0 and result.statement_type in [StatementType.INSERT, StatementType.UPDATE, StatementType.DELETE]:
            details += " (no rows matched)"
    else:
        status = "✗ Failed"
        details = result.error or "Unknown error"

    return f"{status} | {result.statement_type.value} | {details} | {result.execution_time:.3f}s"


def main():
    """Test execute module."""
    print("=== Kingbase Execute Test ===\n")

    config = KingbaseConfig.from_env()
    print(f"Config: {config.get_redacted_connection_string()}\n")

    # Test statement type detection
    print("1. Statement type detection:")
    test_statements = [
        "SELECT * FROM users",
        "INSERT INTO users VALUES (1, 'test')",
        "UPDATE users SET name = 'new'",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE test (id INT)",
        "DROP TABLE test",
        "ALTER TABLE test ADD COLUMN name VARCHAR(50)",
    ]

    for sql in test_statements:
        stmt_type = detect_statement_type(sql)
        print(f"   {sql[:40]:40} -> {stmt_type.value}")

    # Test execution (will fail without actual database)
    print("\n2. Execution test (requires actual database):")
    result = execute_statement("SELECT 1", config)
    print(f"   {format_execute_result(result)}")


if __name__ == "__main__":
    main()
