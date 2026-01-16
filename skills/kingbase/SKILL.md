---
name: kingbase
description: "KingbaseES (人大金仓) database operations skill. Use when user needs to: (1) Connect to and query KingbaseES/Kingbase database, (2) Perform CRUD operations (SELECT, INSERT, UPDATE, DELETE), (3) Create/modify database structures (databases, tables, indexes), (4) Query database/table structure and metadata, (5) Validate SQL syntax and check for security/performance issues. Triggers when user mentions 'kingbase', '人大金仓', 'KingbaseES', or refers to Kingbase database operations."
---

# KingbaseES Database Operations

## Overview

This skill enables operations on KingbaseES (人大金仓) database, a PostgreSQL-compatible database system. It provides connection management, query execution, structure inspection, SQL validation, and DDL operations through Python scripts.

## Quick Start

### 1. Connection Setup

Configure connection via environment variables or direct parameters:

```bash
# Environment variables
export KINGBASE_HOST=localhost
export KINGBASE_PORT=54321
export KINGBASE_DATABASE=test
export KINGBASE_USER=system
export KINGBASE_PASSWORD=your_password
export KINGBASE_SCHEMA=public
```

Or pass configuration directly when calling scripts.

### 2. Testing Connection

```python
from scripts.connect import test_connection
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()
result = test_connection(config)
print(result)  # Shows server info and connection status
```

## Core Capabilities

### 1. Querying Data (SELECT)

**Script**: `scripts/query.py`

Execute SELECT queries and return results in table format:

```python
from scripts.query import execute_query, format_result_table
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()

# Simple query
result = execute_query("SELECT * FROM users LIMIT 10", config)
print(format_result_table(result))

# With parameters (safe from SQL injection)
result = execute_query(
    "SELECT * FROM users WHERE status = %s AND created_at > %s",
    config,
    params=('active', '2024-01-01')
)
```

**Default behavior**: Automatically applies LIMIT 100 if not present in query.

### 2. Modifying Data (INSERT/UPDATE/DELETE)

**Script**: `scripts/execute.py`

Execute data modification statements:

```python
from scripts.execute import (
    execute_statement,
    insert_data,
    update_data,
    delete_data
)
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()

# Direct execution
result = execute_statement(
    "UPDATE users SET status = %s WHERE id = %s",
    config,
    params=('inactive', 123)
)

# Helper functions
insert_data('users', {'name': 'John', 'email': 'john@example.com'}, config)
update_data('users', {'status': 'active'}, 'id = 123', config)
delete_data('users', 'created_at < 2020-01-01', config)
```

### 3. Database Structure Queries

**Script**: `scripts/structure.py`

Query database and table structure:

```python
from scripts.structure import (
    list_databases,
    list_tables,
    get_table_columns,
    format_table_structure
)
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()

# List all databases
databases = list_databases(config)

# List tables in schema
tables = list_tables(config, schema='public')

# Get table structure
columns = get_table_columns('users', config)
print(format_table_structure('users', config))
```

### 4. DDL Operations (CREATE/DROP/ALTER)

**Script**: `scripts/execute.py`

Create and modify database structures:

```python
from scripts.execute import create_table, drop_table
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()

# Create table
create_table(
    'users',
    columns={
        'id': 'SERIAL PRIMARY KEY',
        'username': 'VARCHAR(50) NOT NULL UNIQUE',
        'email': 'VARCHAR(100) NOT NULL',
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    },
    config=config
)

# Drop table
drop_table('users', config, if_exists=True, cascade=False)
```

### 5. SQL Validation

**Script**: `scripts/validate.py`

Validate SQL for syntax, security, performance, and naming:

```python
from scripts.validate import validate_sql, format_validation_result

# Validate without database connection
result = validate_sql("SELECT * FROM users WHERE id = 1")
print(format_validation_result(result))

# Validate with table/column existence checking
result = validate_sql(
    "SELECT * FROM users WHERE email = 'test@example.com'",
    check_existence=True  # Requires database connection
)
```

**Validation checks**:
- Syntax correctness (balanced parentheses, quotes)
- Security issues (SQL injection patterns)
- Performance problems (SELECT *, leading wildcards, functions in WHERE)
- Naming conventions (snake_case recommended)
- Table/column existence (when `check_existence=True`)

## Operation Patterns

### Pattern 1: Quick Data Query

User asks: "查询用户表的前10条记录"

```python
from scripts.query import execute_and_format
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()
result = execute_and_format("SELECT * FROM users LIMIT 10", config)
print(result)
```

### Pattern 2: Validate Before Execute

User asks: "帮我看下这个SQL有没有问题"

```python
from scripts.validate import validate_sql, format_validation_result

sql = "SELECT * FROM users WHERE email LIKE '%@example.com'"
result = validate_sql(sql, check_existence=True)
print(format_validation_result(result))

# If valid, execute
if result.is_valid:
    from scripts.query import execute_and_format
    print(execute_and_format(sql))
```

### Pattern 3: Check Table Structure

User asks: "查看orders表的结构"

```python
from scripts.structure import format_table_structure
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()
print(format_table_structure('orders', config))
```

### Pattern 4: Batch Insert

User asks: "批量插入100条用户数据"

```python
from scripts.execute import execute_batch
from scripts.config import KingbaseConfig

config = KingbaseConfig.from_env()
sql = "INSERT INTO users (username, email) VALUES (%s, %s)"
params = [(f'user{i}', f'user{i}@example.com') for i in range(100)]

result = execute_batch(sql, params, config)
print(f"Inserted {result.rows_affected} rows")
```

## Script Reference

| Script | Purpose | Key Functions |
|--------|---------|---------------|
| `config.py` | Configuration management | `KingbaseConfig.from_env()`, `KingbaseConfig.from_dict()` |
| `connect.py` | Database connection | `get_connection()`, `test_connection()` |
| `query.py` | SELECT queries | `execute_query()`, `format_result_table()` |
| `execute.py` | INSERT/UPDATE/DELETE/CREATE | `execute_statement()`, `insert_data()`, `update_data()`, `delete_data()` |
| `validate.py` | SQL validation | `validate_sql()`, `format_validation_result()` |
| `structure.py` | Structure queries | `list_tables()`, `get_table_columns()`, `format_table_structure()` |

## Reference Documentation

For detailed information, consult these references:

- **[syntax.md](references/syntax.md)** - Complete KingbaseES SQL syntax reference
- **[validation_rules.md](references/validation_rules.md)** - Detailed validation rules and patterns
- **[best_practices.md](references/best_practices.md)** - Performance optimization and best practices

## Dependencies

Required Python packages:

```bash
pip install psycopg2-binary           # PostgreSQL/KingbaseES driver
pip install sqlalchemy               # Optional, for SQLAlchemy support
pip install sqlparse                 # Optional, for SQL parsing
```

**Note**: KingbaseES is PostgreSQL-compatible, so `psycopg2` is used as the database driver.

## Common Tasks

### Check if table exists
```python
from scripts.structure import list_tables
tables = list_tables(config)
exists = any(t.name == 'target_table' for t in tables)
```

### Get table row count
```python
from scripts.structure import get_table_size
size = get_table_size('users', config)
print(f"Rows: {size['row_count']}")
```

### Execute transaction
```python
from scripts.execute import execute_statement
conn.begin()
try:
    execute_statement("INSERT INTO ...", config)
    execute_statement("UPDATE ...", config)
    conn.commit()
except:
    conn.rollback()
```

### Export query results to JSON
```python
from scripts.query import execute_to_json
result = execute_to_json("SELECT * FROM users LIMIT 10", config)
import json
print(json.dumps(result, indent=2))
```
