#!/usr/bin/env python3
"""
Kingbase Database Structure Query Module

Queries database and table structure information:
- List all databases
- List all tables in a database/schema
- Get detailed table structure (columns, types, constraints)
- Get indexes information
- Get foreign keys
- Get table size statistics
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict

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
from connect import KingbaseConnection


@dataclass
class ColumnInfo:
    """Information about a table column."""
    name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str]
    max_length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_refs: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a table."""
    schema: str
    name: str
    table_type: str  # 'BASE TABLE', 'VIEW', etc.
    row_count: Optional[int] = None
    total_size: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class IndexInfo:
    """Information about an index."""
    name: str
    table_name: str
    column_names: List[str]
    is_unique: bool
    is_primary: bool
    index_type: str


@dataclass
class ForeignKeyInfo:
    """Information about a foreign key."""
    name: str
    column_name: str
    referenced_table: str
    referenced_column: str
    on_delete: str
    on_update: str


def list_databases(config: Optional[KingbaseConfig] = None) -> List[str]:
    """
    List all databases in the KingbaseES instance.

    Args:
        config: Database configuration

    Returns:
        List of database names
    """
    if config is None:
        config = KingbaseConfig.from_env()

    conn = KingbaseConnection(config)
    databases = []

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor() as cursor:
                cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
                databases = [row['datname'] for row in cursor.fetchall()]
        else:
            with connection.connect() as db_conn:
                result = db_conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"))
                databases = [row[0] for row in result]

    finally:
        conn.close()

    return databases


def list_schemas(config: Optional[KingbaseConfig] = None) -> List[Dict[str, Any]]:
    """
    List all schemas in the current database.

    Args:
        config: Database configuration

    Returns:
        List of schema information dictionaries
    """
    if config is None:
        config = KingbaseConfig.from_env()

    conn = KingbaseConnection(config)
    schemas = []

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        schema_name,
                        schema_owner,
                        schema_acl
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'sys', 'pg_toast')
                    ORDER BY schema_name
                """)
                schemas = [dict(row) for row in cursor.fetchall()]
        else:
            with connection.connect() as db_conn:
                result = db_conn.execute(text("""
                    SELECT
                        schema_name,
                        schema_owner,
                        schema_acl
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'sys', 'pg_toast')
                    ORDER BY schema_name
                """))
                schemas = [dict(row._mapping) for row in result]

    finally:
        conn.close()

    return schemas


def list_tables(
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None,
    include_views: bool = True
) -> List[TableInfo]:
    """
    List all tables in a schema.

    Args:
        config: Database configuration
        schema: Schema name (default from config)
        include_views: Whether to include views

    Returns:
        List of TableInfo objects
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    conn = KingbaseConnection(config)
    tables = []

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                table_type_filter = ""
                if not include_views:
                    table_type_filter = "AND table_type = 'BASE TABLE'"

                cursor.execute(f"""
                    SELECT
                        table_schema as schema,
                        table_name as name,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                        {table_type_filter}
                    ORDER BY table_name
                """, (schema,))

                tables = [TableInfo(**row) for row in cursor.fetchall()]
        else:
            with connection.connect() as db_conn:
                table_type_filter = ""
                if not include_views:
                    table_type_filter = "AND table_type = 'BASE TABLE'"

                result = db_conn.execute(text(f"""
                    SELECT
                        table_schema as schema,
                        table_name as name,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                        {table_type_filter}
                    ORDER BY table_name
                """), {"schema": schema})

                tables = [TableInfo(**dict(row._mapping)) for row in result]

    finally:
        conn.close()

    return tables


def get_table_columns(
    table_name: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> List[ColumnInfo]:
    """
    Get column information for a table.

    Args:
        table_name: Name of the table
        config: Database configuration
        schema: Schema name (default from config)

    Returns:
        List of ColumnInfo objects
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    conn = KingbaseConnection(config)
    columns = []

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        column_name as name,
                        data_type,
                        is_nullable,
                        column_default as default_value,
                        character_maximum_length as max_length,
                        numeric_precision as precision,
                        numeric_scale as scale
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table_name))

                columns_data = [dict(row) for row in cursor.fetchall()]

                # Get primary key columns
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                """, (f"{schema}.{table_name}",))
                pk_columns = set([row['attname'] for row in cursor.fetchall()])

                # Get foreign key columns
                cursor.execute("""
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = %s
                        AND tc.table_name = %s
                """, (schema, table_name))

                fk_info = {row['column_name']: f"{row['foreign_table_name']}.{row['foreign_column_name']}"
                           for row in cursor.fetchall()}

                # Build ColumnInfo objects
                for col in columns_data:
                    col_name = col['name']
                    col['is_primary_key'] = col_name in pk_columns
                    col['is_foreign_key'] = col_name in fk_info
                    col['foreign_key_refs'] = fk_info.get(col_name)
                    columns.append(ColumnInfo(**col))

        else:
            with connection.connect() as db_conn:
                result = db_conn.execute(text("""
                    SELECT
                        column_name as name,
                        data_type,
                        is_nullable,
                        column_default as default_value,
                        character_maximum_length as max_length,
                        numeric_precision as precision,
                        numeric_scale as scale
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table_name
                    ORDER BY ordinal_position
                """), {"schema": schema, "table_name": table_name})

                columns_data = [dict(row._mapping) for row in result]

                # Get primary key columns
                pk_result = db_conn.execute(text("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = :table_regclass AND i.indisprimary
                """), {"table_regclass": f"{schema}.{table_name}"})
                pk_columns = set([row[0] for row in pk_result])

                # Build ColumnInfo objects (simplified, without FK for SQLAlchemy path)
                for col in columns_data:
                    col['is_primary_key'] = col['name'] in pk_columns
                    columns.append(ColumnInfo(**col))

    finally:
        conn.close()

    return columns


def get_table_indexes(
    table_name: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> List[IndexInfo]:
    """
    Get index information for a table.

    Args:
        table_name: Name of the table
        config: Database configuration
        schema: Schema name (default from config)

    Returns:
        List of IndexInfo objects
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    conn = KingbaseConnection(config)
    indexes = []

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        i.relname as index_name,
                        a.attname as column_name,
                        ix.indisunique as is_unique,
                        ix.indisprimary as is_primary,
                        am.amname as index_type
                    FROM pg_class t
                    JOIN pg_index ix ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_am am ON i.relam = am.oid
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = %s AND t.relname = %s
                    ORDER BY i.relname, a.attnum
                """, (schema, table_name))

                # Group columns by index
                index_data = {}
                for row in cursor.fetchall():
                    idx_name = row['index_name']
                    if idx_name not in index_data:
                        index_data[idx_name] = {
                            'name': idx_name,
                            'table_name': table_name,
                            'column_names': [],
                            'is_unique': row['is_unique'],
                            'is_primary': row['is_primary'],
                            'index_type': row['index_type']
                        }
                    index_data[idx_name]['column_names'].append(row['column_name'])

                indexes = [IndexInfo(**idx) for idx in index_data.values()]

        else:
            with connection.connect() as db_conn:
                result = db_conn.execute(text("""
                    SELECT
                        i.relname as index_name,
                        a.attname as column_name,
                        ix.indisunique as is_unique,
                        ix.indisprimary as is_primary,
                        am.amname as index_type
                    FROM pg_class t
                    JOIN pg_index ix ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_am am ON i.relam = am.oid
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = :schema AND t.relname = :table_name
                    ORDER BY i.relname, a.attnum
                """), {"schema": schema, "table_name": table_name})

                index_data = {}
                for row in result:
                    idx_name = row[0]
                    if idx_name not in index_data:
                        index_data[idx_name] = {
                            'name': idx_name,
                            'table_name': table_name,
                            'column_names': [],
                            'is_unique': row[3],
                            'is_primary': row[4],
                            'index_type': row[5]
                        }
                    index_data[idx_name]['column_names'].append(row[1])

                indexes = [IndexInfo(**idx) for idx in index_data.values()]

    finally:
        conn.close()

    return indexes


def get_table_size(
    table_name: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get size statistics for a table.

    Args:
        table_name: Name of the table
        config: Database configuration
        schema: Schema name (default from config)

    Returns:
        Dictionary with size information
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    conn = KingbaseConnection(config)
    size_info = {}

    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        pg_size_pretty(pg_total_relation_size(%s)) as total_size,
                        pg_size_pretty(pg_relation_size(%s)) as table_size,
                        pg_size_pretty(pg_indexes_size(%s)) as indexes_size
                """, (f"{schema}.{table_name}",) * 3)
                size_info = dict(cursor.fetchone())

                # Get row count
                cursor.execute(f"SELECT COUNT(*) as row_count FROM {schema}.{table_name}")
                size_info['row_count'] = cursor.fetchone()['row_count']

        else:
            with connection.connect() as db_conn:
                result = db_conn.execute(text("""
                    SELECT
                        pg_size_pretty(pg_total_relation_size(:table)) as total_size,
                        pg_size_pretty(pg_relation_size(:table)) as table_size,
                        pg_size_pretty(pg_indexes_size(:table)) as indexes_size
                """), {"table": f"{schema}.{table_name}"})

                row = result.fetchone()
                size_info = {
                    'total_size': row[0],
                    'table_size': row[1],
                    'indexes_size': row[2]
                }

                # Get row count
                count_result = db_conn.execute(text(f"SELECT COUNT(*) as row_count FROM {schema}.{table_name}"))
                size_info['row_count'] = count_result.scalar()

    finally:
        conn.close()

    return size_info


def format_table_structure(
    table_name: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> str:
    """
    Format complete table structure information as text.

    Args:
        table_name: Name of the table
        config: Database configuration
        schema: Schema name

    Returns:
        Formatted table structure
    """
    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    lines = []
    lines.append(f"Table: {schema}.{table_name}")
    lines.append("=" * 60)

    # Get column information
    columns = get_table_columns(table_name, config, schema)
    if columns:
        lines.append("\nColumns:")
        lines.append("-" * 60)

        # Header
        lines.append(f"{'Name':<25} {'Type':<20} {'Nullable':<8} {'Key':<10} {'Default'}")
        lines.append("-" * 60)

        for col in columns:
            key = ""
            if col.is_primary_key:
                key = "PK"
            elif col.is_foreign_key:
                key = f"FK → {col.foreign_key_refs}"

            type_str = col.data_type
            if col.max_length:
                type_str += f"({col.max_length})"
            elif col.precision and col.scale:
                type_str += f"({col.precision},{col.scale})"
            elif col.precision:
                type_str += f"({col.precision})"

            nullable = "YES" if col.is_nullable else "NO"

            lines.append(f"{col.name:<25} {type_str:<20} {nullable:<8} {key:<10} {col.default_value or ''}")

    # Get index information
    indexes = get_table_indexes(table_name, config, schema)
    if indexes:
        lines.append("\nIndexes:")
        lines.append("-" * 60)
        for idx in indexes:
            idx_type = "PRIMARY" if idx.is_primary else ("UNIQUE" if idx.is_unique else "INDEX")
            lines.append(f"{idx.name:30} {idx_type:10} ({', '.join(idx.column_names)})")

    # Get size information
    size = get_table_size(table_name, config, schema)
    lines.append(f"\nSize Information:")
    lines.append(f"  Total size: {size.get('total_size', 'N/A')}")
    lines.append(f"  Table size: {size.get('table_size', 'N/A')}")
    lines.append(f"  Indexes size: {size.get('indexes_size', 'N/A')}")
    lines.append(f"  Row count: {size.get('row_count', 'N/A')}")

    return "\n".join(lines)


def main():
    """Test structure module."""
    print("=== Kingbase Structure Query Test ===\n")

    config = KingbaseConfig.from_env()
    print(f"Config: {config.get_redacted_connection_string()}\n")

    # List databases
    print("1. Databases:")
    databases = list_databases(config)
    for db in databases:
        print(f"   - {db}")

    # List schemas
    print("\n2. Schemas:")
    schemas = list_schemas(config)
    for schema in schemas[:5]:  # Show first 5
        print(f"   - {schema['schema_name']} (owner: {schema['schema_owner']})")

    # List tables
    print("\n3. Tables:")
    tables = list_tables(config)
    for table in tables[:5]:
        print(f"   - {table.name} ({table.table_type})")

    # Show structure for first table (if any)
    if tables:
        print(f"\n4. Table Structure ({tables[0].name}):")
        print(format_table_structure(tables[0].name, config))


if __name__ == "__main__":
    main()
