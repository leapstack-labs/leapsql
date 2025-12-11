---
title: Writing Macros
description: How to create custom Starlark macros
---

This guide covers everything you need to know to write effective macros in LeapSQL.

## Macro File Structure

Macros are defined in `.star` files in your `macros/` directory:

```
macros/
├── utils.star       # General utilities
├── metrics.star     # Metric calculations
├── dates.star       # Date handling
└── testing.star     # Test helpers
```

### Basic Structure

```python title="macros/utils.star"
# Optional: Module-level docstring
"""
General utility macros for SQL generation.
"""

# Constants (available within the file)
DEFAULT_PRECISION = 2

# Helper functions (private, not exported)
def _format_number(value, precision):
    return "ROUND({}, {})".format(value, precision)

# Public functions (exported as macros)
def format_currency(column, precision=DEFAULT_PRECISION):
    """Format a column as currency with specified precision."""
    return _format_number(column, precision)
```

## Function Basics

### Simple Functions

```python
def hello():
    return "Hello, World!"

def greet(name):
    return "Hello, {}!".format(name)
```

### With Parameters

```python
def select_columns(columns):
    """
    Generate a SELECT clause from a list of columns.
    
    Args:
        columns: List of column names
    
    Returns:
        SQL string for SELECT clause
    """
    return "SELECT {}".format(", ".join(columns))
```

### Default Parameters

```python
def limit_rows(limit=100):
    return "LIMIT {}".format(limit)

def date_filter(column, days=30, operator=">="):
    return "{} {} CURRENT_DATE - INTERVAL '{} days'".format(
        column, operator, days
    )
```

### Keyword Arguments

```python
def build_where(column, value, operator="=", quote=True):
    if quote:
        return "WHERE {} {} '{}'".format(column, operator, value)
    else:
        return "WHERE {} {} {}".format(column, operator, value)

# Usage:
# {{ utils.build_where("status", "active") }}
# {{ utils.build_where("count", 10, operator=">", quote=False) }}
```

## String Handling

### String Formatting

```python
# Basic formatting
def simple_select(table):
    return "SELECT * FROM {}".format(table)

# Multiple placeholders
def join_tables(left, right, key):
    return "{} JOIN {} ON {}.{} = {}.{}".format(
        left, right, left, key, right, key
    )

# Named placeholders
def templated_query(table, columns, condition):
    return "SELECT {columns} FROM {table} WHERE {condition}".format(
        table=table,
        columns=columns,
        condition=condition
    )
```

### String Methods

```python
def normalize_column(column):
    # Starlark string methods
    return column.lower().strip().replace(" ", "_")

def to_snake_case(name):
    # Simple snake_case conversion
    return name.lower().replace(" ", "_").replace("-", "_")
```

### Multi-line SQL

```python
def complex_case(column, mappings):
    """
    Generate a CASE statement from a mapping dictionary.
    
    Args:
        column: Column to check
        mappings: Dict of value -> result
    """
    lines = ["CASE"]
    for value, result in mappings.items():
        lines.append("    WHEN {} = '{}' THEN '{}'".format(
            column, value, result
        ))
    lines.append("    ELSE 'unknown'")
    lines.append("END")
    return "\n".join(lines)
```

## Working with Collections

### Lists

```python
def column_list(columns):
    """Join columns with commas."""
    return ", ".join(columns)

def quoted_list(values):
    """Create quoted list for IN clause."""
    quoted = ["'{}'".format(v) for v in values]
    return ", ".join(quoted)

def in_clause(column, values):
    """Generate IN clause."""
    if not values:
        return "FALSE"
    return "{} IN ({})".format(column, quoted_list(values))
```

### List Comprehensions

```python
def uppercase_columns(columns):
    return [col.upper() for col in columns]

def filter_columns(columns, prefix):
    return [col for col in columns if col.startswith(prefix)]

def alias_columns(columns, prefix):
    return ["{} as {}_{}".format(col, prefix, col) for col in columns]
```

### Dictionaries

```python
def build_case_when(column, mappings, default="NULL"):
    """
    Build CASE WHEN from dictionary.
    
    Args:
        column: Column to evaluate
        mappings: Dict of {value: result}
        default: Default else value
    """
    cases = []
    for value, result in mappings.items():
        cases.append("WHEN {} = '{}' THEN '{}'".format(
            column, value, result
        ))
    return "CASE {} ELSE {} END".format(" ".join(cases), default)
```

## Control Flow

### Conditionals

```python
def optional_column(column, include=True):
    if include:
        return column
    else:
        return ""

def environment_table(table, is_prod):
    if is_prod:
        return "production.{}".format(table)
    return "development.{}".format(table)
```

### Loops

```python
def generate_unions(tables):
    """Generate UNION ALL for multiple tables."""
    queries = []
    for table in tables:
        queries.append("SELECT * FROM {}".format(table))
    return " UNION ALL ".join(queries)

def pivot_columns(column, values):
    """Generate pivot columns."""
    cols = []
    for value in values:
        cols.append(
            "SUM(CASE WHEN {} = '{}' THEN 1 ELSE 0 END) as {}_{}"
            .format(column, value, column, value)
        )
    return ", ".join(cols)
```

## Advanced Patterns

### Composable Macros

```python
# Build macros from other macros
def _quote(value):
    return "'{}'".format(value)

def _null_safe(column):
    return "COALESCE({}, '')".format(column)

def compare_strings(col1, col2):
    """Null-safe string comparison."""
    return "{} = {}".format(_null_safe(col1), _null_safe(col2))
```

### Factory Functions

```python
def make_aggregator(agg_func):
    """Create an aggregation function."""
    def aggregate(column, alias=None):
        result = "{}({})".format(agg_func, column)
        if alias:
            result += " as {}".format(alias)
        return result
    return aggregate

# Create specific aggregators
sum_col = make_aggregator("SUM")
avg_col = make_aggregator("AVG")
count_col = make_aggregator("COUNT")
```

### Validation

```python
def safe_table_name(name):
    """Validate and return safe table name."""
    # Only allow alphanumeric and underscore
    for char in name:
        if not (char.isalnum() or char == "_"):
            fail("Invalid character in table name: {}".format(char))
    return name

def require_columns(columns, required):
    """Ensure required columns are present."""
    missing = [r for r in required if r not in columns]
    if missing:
        fail("Missing required columns: {}".format(", ".join(missing)))
    return True
```

## Real-World Examples

### Date Utilities

```python title="macros/dates.star"
def date_trunc(unit, column):
    """Truncate date to specified unit."""
    valid_units = ["day", "week", "month", "quarter", "year"]
    if unit not in valid_units:
        fail("Invalid unit: {}. Must be one of: {}".format(
            unit, ", ".join(valid_units)
        ))
    return "DATE_TRUNC('{}', {})".format(unit, column)

def days_between(start, end):
    """Calculate days between two dates."""
    return "DATEDIFF('day', {}, {})".format(start, end)

def is_weekend(date_column):
    """Check if date is weekend."""
    return "EXTRACT(DOW FROM {}) IN (0, 6)".format(date_column)

def fiscal_quarter(date_column, fiscal_start_month=1):
    """Calculate fiscal quarter (1-4)."""
    return """
        CASE 
            WHEN EXTRACT(MONTH FROM {}) >= {} 
            THEN CEIL((EXTRACT(MONTH FROM {}) - {} + 1) / 3.0)
            ELSE CEIL((EXTRACT(MONTH FROM {}) + 12 - {} + 1) / 3.0)
        END
    """.format(
        date_column, fiscal_start_month,
        date_column, fiscal_start_month,
        date_column, fiscal_start_month
    )
```

### Metrics Library

```python title="macros/metrics.star"
def safe_divide(numerator, denominator, default=0):
    """Divide with zero handling."""
    return "COALESCE({} / NULLIF({}, 0), {})".format(
        numerator, denominator, default
    )

def percentage(part, whole, decimals=2):
    """Calculate percentage."""
    return "ROUND({} * 100.0, {})".format(
        safe_divide(part, whole), decimals
    )

def growth_rate(current, previous):
    """Calculate growth rate as percentage."""
    return percentage(
        "{} - {}".format(current, previous),
        previous
    )

def running_total(column, partition_by=None, order_by="date"):
    """Calculate running total."""
    partition = ""
    if partition_by:
        partition = "PARTITION BY {} ".format(partition_by)
    return "SUM({}) OVER ({}ORDER BY {})".format(
        column, partition, order_by
    )
```

### Testing Helpers

```python title="macros/testing.star"
def assert_not_null(column):
    """Generate test for NOT NULL."""
    return "SELECT COUNT(*) as failures FROM model WHERE {} IS NULL".format(
        column
    )

def assert_unique(column):
    """Generate test for uniqueness."""
    return """
        SELECT {}, COUNT(*) as occurrences 
        FROM model 
        GROUP BY {} 
        HAVING COUNT(*) > 1
    """.format(column, column)

def assert_accepted_values(column, values):
    """Generate test for accepted values."""
    return """
        SELECT {} as invalid_value, COUNT(*) as occurrences
        FROM model
        WHERE {} NOT IN ({})
        GROUP BY {}
    """.format(
        column, column, 
        ", ".join(["'{}'".format(v) for v in values]),
        column
    )
```

## Debugging Macros

### Print Debugging

```python
def debug_macro(column):
    # Use print() for debugging during development
    print("debug_macro called with:", column)
    return "SELECT {}".format(column)
```

### Error Messages

```python
def validated_macro(value):
    if not value:
        fail("validated_macro requires a non-empty value")
    if not isinstance(value, str):
        fail("validated_macro expects a string, got: {}".format(type(value)))
    return value
```

## Next Steps

- [Using Macros](/macros/using-macros) - How to call macros in models
- [Built-in Functions](/macros/builtins) - Starlark standard library
- [Templating Overview](/templating/overview) - Template syntax
