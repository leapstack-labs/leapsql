---
title: Using Macros
description: How to call macros in your SQL models
---

This guide covers how to use macros effectively in your LeapSQL models.

## Basic Usage

### Calling a Macro

Macros are called using the expression syntax `{{ }}`:

```sql
SELECT
    customer_id,
    {{ utils.cents_to_dollars('amount_cents') }} as amount
FROM orders
```

### Namespace

Macros are namespaced by their filename (without `.star`):

| File | Namespace |
|------|-----------|
| `macros/utils.star` | `utils` |
| `macros/metrics.star` | `metrics` |
| `macros/my_helpers.star` | `my_helpers` |

```sql
-- Call function from utils.star
{{ utils.my_function() }}

-- Call function from metrics.star
{{ metrics.calculate_growth() }}
```

## Passing Arguments

### Positional Arguments

```sql
-- Macro: def greet(name)
{{ utils.greet("World") }}

-- Macro: def add(a, b)
{{ utils.add(1, 2) }}
```

### String Arguments

Quote strings with single or double quotes:

```sql
-- Both work
{{ utils.format_column("customer_name") }}
{{ utils.format_column('customer_name') }}
```

### Column References

Pass column names as strings:

```sql
-- Correct: Pass column name as string
{{ utils.safe_divide("revenue", "orders") }}

-- Wrong: This won't work
{{ utils.safe_divide(revenue, orders) }}
```

### Numeric Arguments

```sql
{{ utils.round_to("price", 2) }}
{{ utils.limit_rows(1000) }}
```

### Boolean Arguments

```sql
{{ utils.format_column("email", lowercase=True) }}
{{ utils.optional_column("phone", include=False) }}
```

### List Arguments

```sql
-- Pass a list
{{ utils.select_columns(["id", "name", "email"]) }}

-- In clause
{{ utils.in_list("status", ["active", "pending"]) }}
```

### Dictionary Arguments

```sql
{{ utils.case_when("status", {"A": "Active", "I": "Inactive"}) }}
```

## Keyword Arguments

Use keyword arguments for clarity:

```sql
-- Positional (less clear)
{{ utils.date_filter("created_at", 30, ">=") }}

-- Keyword (more clear)
{{ utils.date_filter("created_at", days=30, operator=">=") }}

-- Mix positional and keyword
{{ utils.date_filter("created_at", days=30) }}
```

### Default Values

If a macro has defaults, you can omit those arguments:

```python title="macros/utils.star"
def format_money(column, precision=2, currency="$"):
    return "'{}'  || ROUND({}, {})".format(currency, column, precision)
```

```sql
-- Use all defaults
{{ utils.format_money("price") }}
-- Result: '$' || ROUND(price, 2)

-- Override precision
{{ utils.format_money("price", precision=0) }}
-- Result: '$' || ROUND(price, 0)

-- Override both
{{ utils.format_money("price", precision=0, currency="€") }}
-- Result: '€' || ROUND(price, 0)
```

## Context in Macros

Macros execute in a context with access to globals:

```python title="macros/utils.star"
def env_schema():
    # Access env from the template context
    return env.get("TARGET_SCHEMA", "public")

def is_production():
    return target.name == "prod"
```

```sql
SELECT * FROM {{ utils.env_schema() }}.customers
{* if utils.is_production() *}
-- Production-only SQL
{* endif *}
```

## Combining Macros

### Nesting Macro Calls

```sql
-- Call macro within macro result
{{ utils.round_column(metrics.growth_rate("revenue", "prev_revenue")) }}
```

### Multiple Macros in One Line

```sql
SELECT
    {{ utils.safe_divide("a", "b") }} as ratio,
    {{ utils.percentage("part", "whole") }} as pct
FROM data
```

### Macros with Control Flow

```sql
SELECT
    {* for col in ["revenue", "orders", "customers"] *}
    {{ metrics.growth_rate(col, "prev_" + col) }} as {{ col }}_growth,
    {* endfor *}
FROM metrics
```

## Common Patterns

### Building SELECT Clauses

```python title="macros/utils.star"
def select_with_audit(columns):
    audit = ["CURRENT_TIMESTAMP as _loaded_at", "CURRENT_USER as _loaded_by"]
    all_cols = list(columns) + audit
    return ", ".join(all_cols)
```

```sql
SELECT
    {{ utils.select_with_audit(["id", "name", "email"]) }}
FROM users
```

### Dynamic WHERE Clauses

```python title="macros/utils.star"
def where_filters(filters):
    """Generate WHERE clause from dict of column: value pairs."""
    if not filters:
        return "WHERE 1=1"
    conditions = []
    for col, val in filters.items():
        conditions.append("{} = '{}'".format(col, val))
    return "WHERE " + " AND ".join(conditions)
```

```sql
{{ utils.where_filters({"status": "active", "region": "US"}) }}
-- Result: WHERE status = 'active' AND region = 'US'
```

### Aggregation Helpers

```python title="macros/metrics.star"
def sum_by(column, group_by_cols):
    return """
        SELECT {groups}, SUM({col}) as total_{col}
        FROM source
        GROUP BY {groups}
    """.format(col=column, groups=", ".join(group_by_cols))
```

```sql
{{ metrics.sum_by("revenue", ["region", "date"]) }}
```

### Conditional SQL Generation

```python title="macros/utils.star"
def pii_column(column, mask="***"):
    """Return column or mask based on environment."""
    if target.name == "prod":
        return column
    else:
        return "'{}'".format(mask)
```

```sql
SELECT
    id,
    {{ utils.pii_column("email") }} as email,
    {{ utils.pii_column("phone") }} as phone
FROM users
```

## Error Handling

### Missing Macros

If you reference a non-existent macro:

```sql
{{ utils.nonexistent_function() }}
-- Error: 'utils' object has no attribute 'nonexistent_function'
```

### Wrong Namespace

```sql
{{ wrong_namespace.my_function() }}
-- Error: name 'wrong_namespace' is not defined
```

### Argument Errors

```sql
-- Missing required argument
{{ utils.greet() }}
-- Error: greet() missing 1 required positional argument: 'name'

-- Too many arguments
{{ utils.greet("Alice", "Bob") }}
-- Error: greet() takes 1 positional argument but 2 were given
```

## Debugging

### Check Rendered SQL

The rendered SQL is logged during execution. Check the output to verify macro expansion.

### Test Macros Independently

Create a simple test model:

```sql
/*---
name: _macro_test
---*/

-- Test your macros
SELECT
    '{{ utils.my_macro("test") }}' as test1,
    {{ utils.another_macro(123) }} as test2
```

### Print in Macros

```python
def debug_macro(value):
    print("debug_macro received:", value)
    return "processed_{}".format(value)
```

## Best Practices

### 1. Use Descriptive Calls

```sql
-- Good: Clear what's happening
{{ utils.safe_divide("revenue", "orders", default=0) }}

-- Less clear: What does 0 mean?
{{ utils.safe_divide("revenue", "orders", 0) }}
```

### 2. Keep Macro Calls Simple

```sql
-- Good: Simple, readable
{{ metrics.growth_rate("revenue", "prev_revenue") }}

-- Complex: Hard to read
{{ utils.round(metrics.safe_divide(metrics.sum("a"), metrics.sum("b")), 2) }}

-- Better: Break into multiple lines or use CTE
```

### 3. Document Complex Usage

```sql
/*---
name: customer_metrics
meta:
  notes: |
    Uses metrics library for calculations:
    - growth_rate: YoY growth calculation
    - safe_divide: Division with null handling
---*/

SELECT
    customer_id,
    {{ metrics.growth_rate("revenue", "prev_year_revenue") }} as yoy_growth
FROM customer_summary
```

### 4. Validate Inputs

In your macros, validate inputs to provide clear error messages:

```python
def in_list(column, values):
    if not values:
        fail("in_list requires at least one value")
    if not column:
        fail("in_list requires a column name")
    # ... rest of implementation
```

## Next Steps

- [Writing Macros](/macros/writing-macros) - Create custom macros
- [Built-in Functions](/macros/builtins) - Standard library functions
- [Templating](/templating/overview) - Template syntax reference
