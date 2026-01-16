#!/usr/bin/env python3
"""
Kingbase Database Query Module

Executes SELECT queries against KingbaseES database.
Returns results in table format with automatic limit handling.
"""

import sys
from typing import Any, Optional, List
from dataclasses import dataclass

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    from sqlalchemy import text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from config import KingbaseConfig
from connect import KingbaseConnection, get_connection


@dataclass
class QueryResult:
    """Result of a SELECT query."""
    success: bool
    rows: List[dict]
    columns: List[str]
    row_count: int
    execution_time: float
    error: Optional[str] = None
    was_limited: bool = False
    limit_applied: int = 0


def execute_query(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    limit: int = 100,
    params: Optional[tuple] = None
) -> QueryResult:
    """
    Execute a SELECT query.

    Args:
        sql: SQL SELECT statement
        config: Database configuration. If None, loads from environment.
        limit: Maximum number of rows to return (default: 100)
        params: Optional parameters for parameterized query

    Returns:
        QueryResult with query results
    """
    import time

    start_time = time.time()
    result = QueryResult(
        success=False,
        rows=[],
        columns=[],
        row_count=0,
        execution_time=0
    )

    if config is None:
        config = KingbaseConfig.from_env()

    conn = KingbaseConnection(config)

    try:
        connection = conn.connect()

        # Check if query already has LIMIT
        sql_upper = sql.upper().strip()
        has_limit = " LIMIT " in sql_upper

        # Apply limit if not already present
        final_sql = sql
        if not has_limit and limit > 0:
            final_sql = f"{sql.rstrip(';')} LIMIT {limit}"
            result.was_limited = True
            result.limit_applied = limit

        if hasattr(connection, 'cursor'):  # psycopg2
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(final_sql, params or ())

                rows = cursor.fetchall()
                if rows:
                    result.columns = list(rows[0].keys())
                    result.rows = [dict(row) for row in rows]
                else:
                    # Get column names from cursor description
                    if cursor.description:
                        result.columns = [desc[0] for desc in cursor.description]

                result.row_count = len(result.rows)
                result.success = True

        else:  # SQLAlchemy
            with connection.connect() as db_conn:
                db_result = db_conn.execute(text(final_sql), params or ())

                # Get column names
                result.columns = list(db_result.keys())

                # Get rows
                rows = db_result.fetchall()
                result.rows = [dict(row._mapping) for row in rows]
                result.row_count = len(result.rows)
                result.success = True

    except psycopg2.Error as e:
        result.error = f"Database error: {e}"
    except Exception as e:
        result.error = f"Error: {e}"
    finally:
        conn.close()

    result.execution_time = time.time() - start_time
    return result


def format_result_table(result: QueryResult, max_width: int = 50) -> str:
    """
    Format query result as a table.

    Args:
        result: QueryResult from execute_query
        max_width: Maximum width for each column

    Returns:
        Formatted table string
    """
    if not result.success:
        return f"Query failed: {result.error}"

    if result.row_count == 0:
        return "No results found."

    # Calculate column widths
    col_widths = {}
    for col in result.columns:
        col_widths[col] = min(max_width, len(col))

    for row in result.rows:
        for col, value in row.items():
            str_value = str(value) if value is not None else "NULL"
            col_widths[col] = max(col_widths.get(col, 0), min(max_width, len(str_value)))

    # Build separator line
    separator = "+" + "+".join(f"-{w + 2}-" for w in col_widths.values()) + "+"

    # Build header
    header = "|" + "|".join(f" {col.ljust(col_widths[col] + 1)} " for col in result.columns) + "|"

    # Build rows
    lines = [separator, header, separator]

    for row in result.rows:
        row_str = "|" + "|".join(
            f" {str(row.get(col, '') if row.get(col, '') is not None else 'NULL').ljust(col_widths[col] + 1)} "
            for col in result.columns
        ) + "|"
        lines.append(row_str)
        lines.append(separator)

    # Add footer info
    footer_info = f"\n{result.row_count} row(s)"
    if result.was_limited:
        footer_info += f" (limited to {result.limit_applied})"
    footer_info += f" | {result.execution_time:.3f}s"

    return "\n".join(lines) + footer_info


def execute_and_format(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    limit: int = 100,
    params: Optional[tuple] = None
) -> str:
    """
    Execute query and return formatted table.

    Args:
        sql: SQL SELECT statement
        config: Database configuration
        limit: Maximum number of rows to return
        params: Optional query parameters

    Returns:
        Formatted table string
    """
    result = execute_query(sql, config, limit, params)
    return format_result_table(result)


def execute_to_json(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    limit: int = 100,
    params: Optional[tuple] = None
) -> dict:
    """
    Execute query and return JSON-serializable dict.

    Args:
        sql: SQL SELECT statement
        config: Database configuration
        limit: Maximum number of rows to return
        params: Optional query parameters

    Returns:
        Dictionary with query results
    """
    result = execute_query(sql, config, limit, params)

    return {
        "success": result.success,
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.row_count,
        "execution_time": result.execution_time,
        "error": result.error,
        "was_limited": result.was_limited,
        "limit_applied": result.limit_applied
    }


def execute_count(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    params: Optional[tuple] = None
) -> int:
    """
    Execute a COUNT query to get total row count.

    Wraps the original query in SELECT COUNT(*) FROM (...).

    Args:
        sql: SQL SELECT statement
        config: Database configuration
        params: Optional query parameters

    Returns:
        Total row count
    """
    count_sql = f"SELECT COUNT(*) AS total FROM ({sql.rstrip(';')}) AS subq"
    result = execute_query(count_sql, config, limit=0, params=params)

    if result.success and result.rows:
        return result.rows[0].get('total', 0)
    return 0


def main():
    """Test query module."""
    print("=== Kingbase Query Test ===\n")

    config = KingbaseConfig.from_env()
    print(f"Config: {config.get_redacted_connection_string()}")

    # Test query examples
    test_queries = [
        "SELECT current_database(), current_user, version()",
        "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = current_schema() LIMIT 5",
    ]

    for sql in test_queries:
        print(f"\nQuery: {sql}")
        print("-" * 60)

        result = execute_query(sql, config, limit=10)
        print(format_result_table(result))


if __name__ == "__main__":
    main()
