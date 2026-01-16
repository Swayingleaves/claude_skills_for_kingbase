#!/usr/bin/env python3
"""
Kingbase SQL Validation Module

Validates SQL statements for:
- Syntax correctness
- Security issues (SQL injection)
- Performance optimization opportunities
- Naming convention compliance
- Table/column existence
"""

import re
from typing import List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

try:
    import sqlparse
    SQLPARSE_AVAILABLE = True
except ImportError:
    SQLPARSE_AVAILABLE = False

from config import KingbaseConfig
from connect import KingbaseConnection


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """A validation issue found in SQL."""
    severity: ValidationSeverity
    category: str
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    suggestion: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    issues: List[ValidationIssue]

    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return any(issue.severity == ValidationSeverity.ERROR for issue in self.issues)

    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return any(issue.severity == ValidationSeverity.WARNING for issue in self.issues)

    def get_errors(self) -> List[ValidationIssue]:
        """Get all error issues."""
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]

    def get_warnings(self) -> List[ValidationIssue]:
        """Get all warning issues."""
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]

    def get_info(self) -> List[ValidationIssue]:
        """Get all info issues."""
        return [i for i in self.issues if i.severity == ValidationSeverity.INFO]


# SQL Injection Patterns
SQL_INJECTION_PATTERNS = [
    (r"'\s*;\s*DROP\s+TABLE", "Potential DROP TABLE injection"),
    (r"'\s*;\s*DELETE\s+FROM", "Potential DELETE injection"),
    (r"'\s*;\s*EXEC\s*\(", "Potential EXEC injection"),
    (r"'\s*OR\s+'?\d+'?\s*=\s*'\d+", "Potential OR-based injection"),
    (r"'\s*AND\s+'?\d+'?\s*=\s*'\d+", "Potential AND-based injection"),
    (r"'\s*;\s*--", "Comment-based injection"),
    (r"admin'--", "Comment-based authentication bypass"),
    (r"admin'#", "Comment-based authentication bypass"),
    (r"'\s+OR\s+1\s*=\s*1", "Classic tautology injection"),
    (r"UNION\s+SELECT", "Potential UNION-based injection"),
]

# Performance anti-patterns
PERFORMANCE_PATTERNS = [
    (r"SELECT\s+\*\s+FROM", "SELECT * can be inefficient, specify columns"),
    (r"SELECT\s+\*\s+FROM\s+\w+\s+WHERE\s+\w+\s+LIKE\s+'%[^%]*%'",
     "Leading wildcard in LIKE prevents index use"),
    (r"WHERE\s+SUBSTR\(", "Function on column in WHERE prevents index use"),
    (r"WHERE\s+SUBSTRING\(", "Function on column in WHERE prevents index use"),
    (r"WHERE\s+LOWER\(", "Function on column in WHERE prevents index use"),
    (r"WHERE\s+UPPER\(", "Function on column in WHERE prevents index use"),
    (r"ORDER\s+BY\s+\d+(?:\s*,\s*\d+)*", "ORDER BY ordinal position is fragile"),
]

# Naming convention patterns (snake_case recommended)
NAMING_PATTERNS = [
    (r'\b[A-Z][a-z0-9]*[A-Z][a-z0-9]*\b',
     "CamelCase detected - use snake_case for identifiers"),
]


def validate_syntax(sql: str) -> ValidationResult:
    """
    Validate SQL syntax.

    Args:
        sql: SQL statement to validate

    Returns:
        ValidationResult with syntax issues
    """
    issues = []
    sql_clean = sql.strip()

    if not sql_clean:
        issues.append(ValidationIssue(
            severity=ValidationSeverity.ERROR,
            category="syntax",
            message="Empty SQL statement"
        ))
        return ValidationResult(is_valid=False, issues=issues)

    # Check for balanced parentheses
    open_parens = sql_clean.count('(')
    close_parens = sql_clean.count(')')
    if open_parens != close_parens:
        issues.append(ValidationIssue(
            severity=ValidationSeverity.ERROR,
            category="syntax",
            message=f"Unbalanced parentheses: {open_parens} open, {close_parens} close",
            suggestion="Ensure all opening parentheses have matching closing parentheses"
        ))

    # Check for balanced quotes
    single_quotes = sql_clean.count("'") - sql_clean.count("\\'")  # Ignore escaped
    if single_quotes % 2 != 0:
        issues.append(ValidationIssue(
            severity=ValidationSeverity.ERROR,
            category="syntax",
            message="Unbalanced single quotes",
            suggestion="Ensure all single quotes are properly closed"
        ))

    # Check for basic statement structure
    if not re.search(r';\s*$', sql_clean):
        issues.append(ValidationIssue(
            severity=ValidationSeverity.INFO,
            category="style",
            message="Missing semicolon at end of statement",
            suggestion="Add semicolon for better SQL standards compliance"
        ))

    # Check for obvious syntax errors
    if re.search(r'SELECT\s+(?!.*\bFROM\b)', sql_clean, re.IGNORECASE):
        issues.append(ValidationIssue(
            severity=ValidationSeverity.ERROR,
            category="syntax",
            message="SELECT without FROM clause",
            suggestion="Check if SELECT statement is properly formed"
        ))

    return ValidationResult(is_valid=len(issues) == 0, issues=issues)


def validate_security(sql: str) -> ValidationResult:
    """
    Validate SQL for security issues.

    Args:
        sql: SQL statement to validate

    Returns:
        ValidationResult with security issues
    """
    issues = []

    # Check for SQL injection patterns
    for pattern, message in SQL_INJECTION_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                category="security",
                message=message,
                suggestion="Use parameterized queries instead of string concatenation"
            ))

    # Check for hardcoded credentials
    if re.search(r"(password|passwd|pwd)\s*=\s*'[^{']+\"", sql, re.IGNORECASE):
        issues.append(ValidationIssue(
            severity=ValidationSeverity.WARNING,
            category="security",
            message="Possible hardcoded password in query",
            suggestion="Use parameterized queries for sensitive data"
        ))

    return ValidationResult(is_valid=True, issues=issues)


def validate_performance(sql: str) -> ValidationResult:
    """
    Validate SQL for performance issues.

    Args:
        sql: SQL statement to validate

    Returns:
        ValidationResult with performance issues
    """
    issues = []

    # Check performance anti-patterns
    for pattern, message in PERFORMANCE_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE | re.MULTILINE):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                category="performance",
                message=message
            ))

    # Check for missing LIMIT on large result sets
    if re.search(r'SELECT.+FROM', sql, re.IGNORECASE):
        if not re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.INFO,
                category="performance",
                message="No LIMIT clause on SELECT statement",
                suggestion="Consider adding LIMIT to prevent large result sets"
            ))

    # Check for missing WHERE on DELETE/UPDATE
    if re.search(r'\b(DELETE|UPDATE)\b', sql, re.IGNORECASE):
        if not re.search(r'\bWHERE\b', sql, re.IGNORECASE):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                category="performance",
                message="DELETE/UPDATE without WHERE clause",
                suggestion="Ensure WHERE clause is present to avoid full table operations"
            ))

    return ValidationResult(is_valid=True, issues=issues)


def validate_naming(sql: str) -> ValidationResult:
    """
    Validate SQL naming conventions.

    Args:
        sql: SQL statement to validate

    Returns:
        ValidationResult with naming issues
    """
    issues = []

    # Extract identifiers (table names, column names, aliases)
    # Look for identifiers after FROM, JOIN, INTO, UPDATE, etc.
    identifier_patterns = [
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bCREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
    ]

    for pattern in identifier_patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        for match in matches:
            identifier = match.group(1)
            # Check for camelCase or PascalCase
            if re.search(r'[a-z][A-Z]', identifier):
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    category="naming",
                    message=f"Identifier '{identifier}' uses mixed case",
                    suggestion="Consider using snake_case for consistency"
                ))

    return ValidationResult(is_valid=True, issues=issues)


def validate_table_exists(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> ValidationResult:
    """
    Validate that referenced tables exist in the database.

    Args:
        sql: SQL statement to validate
        config: Database configuration
        schema: Schema to check (default from config)

    Returns:
        ValidationResult with table existence issues
    """
    issues = []

    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    # Extract table names from SQL
    table_pattern = r'\b(?:FROM|JOIN|INSERT\s+INTO|UPDATE|CREATE\s+TABLE|DROP\s+TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    tables = re.findall(table_pattern, sql, re.IGNORECASE)

    # Remove duplicates and subqueries
    tables = list(set([t.lower() for t in tables if not t.lower() in ('select', 'where', 'order', 'group')]))

    if not tables:
        return ValidationResult(is_valid=True, issues=issues)

    # Query database for existing tables
    conn = KingbaseConnection(config)
    try:
        connection = conn.connect()

        if hasattr(connection, 'cursor'):
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = %s
                """, (schema,))
                existing_tables = set([row['tablename'].lower() for row in cursor.fetchall()])
        else:
            from sqlalchemy import text
            with connection.connect() as db_conn:
                result = db_conn.execute(text("""
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = :schema
                """), {"schema": schema})
                existing_tables = set([row[0].lower() for row in result])

        # Check each referenced table
        for table in tables:
            if table not in existing_tables:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category="existence",
                    message=f"Table '{table}' does not exist in schema '{schema}'",
                    suggestion=f"Verify table name or create the table first"
                ))

    except Exception as e:
        issues.append(ValidationIssue(
            severity=ValidationSeverity.WARNING,
            category="existence",
            message=f"Could not verify table existence: {e}",
            suggestion="Ensure database connection is working"
        ))
    finally:
        conn.close()

    return ValidationResult(is_valid=len(issues) == 0, issues=issues)


def validate_column_exists(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    schema: Optional[str] = None
) -> ValidationResult:
    """
    Validate that referenced columns exist in their respective tables.

    Args:
        sql: SQL statement to validate
        config: Database configuration
        schema: Schema to check (default from config)

    Returns:
        ValidationResult with column existence issues
    """
    issues = []

    if config is None:
        config = KingbaseConfig.from_env()

    if schema is None:
        schema = config.schema

    # This is a simplified check - full implementation would parse the SQL
    # and verify each column against its table's schema

    return ValidationResult(is_valid=True, issues=issues)


def validate_sql(
    sql: str,
    config: Optional[KingbaseConfig] = None,
    check_existence: bool = False
) -> ValidationResult:
    """
    Perform comprehensive SQL validation.

    Args:
        sql: SQL statement to validate
        config: Database configuration (required for existence checks)
        check_existence: Whether to check table/column existence (requires DB connection)

    Returns:
        Comprehensive ValidationResult
    """
    all_issues = []

    # Always run these validations
    all_issues.extend(validate_syntax(sql).issues)
    all_issues.extend(validate_security(sql).issues)
    all_issues.extend(validate_performance(sql).issues)
    all_issues.extend(validate_naming(sql).issues)

    # Optionally run existence checks (require database connection)
    if check_existence:
        all_issues.extend(validate_table_exists(sql, config).issues)

    # Determine overall validity
    has_errors = any(i.severity == ValidationSeverity.ERROR for i in all_issues)

    return ValidationResult(is_valid=not has_errors, issues=all_issues)


def format_validation_result(result: ValidationResult) -> str:
    """
    Format validation result as readable text.

    Args:
        result: ValidationResult from validate_sql

    Returns:
        Formatted validation report
    """
    lines = []

    # Summary
    errors = len(result.get_errors())
    warnings = len(result.get_warnings())
    info = len(result.get_info())

    if result.is_valid and warnings == 0 and info == 0:
        lines.append("✓ SQL validation passed - No issues found")
    elif result.is_valid:
        lines.append(f"✓ SQL validation passed - {warnings} warning(s), {info} info")
    else:
        lines.append(f"✗ SQL validation failed - {errors} error(s), {warnings} warning(s), {info} info")

    # Group by category
    categories = {}
    for issue in result.issues:
        if issue.category not in categories:
            categories[issue.category] = []
        categories[issue.category].append(issue)

    # Print issues by category and severity
    for category in sorted(categories.keys()):
        lines.append(f"\n{category.upper()}:")
        for issue in categories[category]:
            icon = {
                ValidationSeverity.ERROR: "✗",
                ValidationSeverity.WARNING: "⚠",
                ValidationSeverity.INFO: "ℹ",
            }[issue.severity]
            lines.append(f"  {icon} {issue.message}")
            if issue.suggestion:
                lines.append(f"     → {issue.suggestion}")

    return "\n".join(lines)


def main():
    """Test validation module."""
    print("=== Kingbase SQL Validation Test ===\n")

    # Test SQL statements
    test_cases = [
        ("SELECT * FROM users;", "Valid simple SELECT"),
        ("SELECT name FROM users WHERE id = 1", "Valid SELECT with WHERE"),
        ("SELECT * FROM users WHERE name = 'admin' OR '1'='1'", "SQL injection"),
        ("SELECT * FROM users WHERE name LIKE '%test%'", "Leading wildcard"),
        ("INSERT INTO users VALUES (1, 'test');", "Valid INSERT"),
        ("SELECT * FROM users;", "Multiple issues test"),
    ]

    for sql, description in test_cases:
        print(f"\nTest: {description}")
        print(f"SQL: {sql}")
        print("-" * 50)

        result = validate_sql(sql)
        print(format_validation_result(result))


if __name__ == "__main__":
    main()
