---
title: Templating Overview
description: Dynamic SQL generation with Starlark templating
---

LeapSQL uses Starlark templating to generate dynamic SQL. This lets you create reusable patterns, inject variables, and build complex queries programmatically.

## What is Starlark?

Starlark is a Python-like language designed for configuration. It's:

- **Safe**: No file system access, no network calls
- **Deterministic**: Same inputs always produce same outputs
- **Familiar**: Python-like syntax most developers already know

## Template Syntax

LeapSQL supports two types of template constructs:

### Expressions: `{{ }}`

Expressions evaluate to a value and insert it into the SQL:

```sql
SELECT *
FROM customers
WHERE created_at >= '{{ env.START_DATE }}'
```

### Control Flow: `{* *}`

Control flow statements handle conditionals and loops:

```sql
SELECT
    id,
    {* for col in ['name', 'email', 'phone'] *}
    {{ col }},
    {* endfor *}
    created_at
FROM users
```

## Quick Examples

### Variable Substitution

```sql
-- Using environment variables
SELECT * FROM events
WHERE date >= '{{ env.START_DATE }}'
  AND date <= '{{ env.END_DATE }}'
```

### Conditional SQL

```sql
SELECT
    id,
    name,
    {* if target.name == 'prod' *}
    email,  -- Only include PII in prod
    {* endif *}
    created_at
FROM users
```

### Loop Generation

```sql
SELECT
    date,
    {* for metric in ['clicks', 'views', 'conversions'] *}
    SUM({{ metric }}) as total_{{ metric }},
    {* endfor *}
FROM daily_events
GROUP BY date
```

### Macro Calls

```sql
-- Call a macro defined in macros/utils.star
SELECT
    customer_id,
    {{ utils.cents_to_dollars('amount_cents') }} as amount_dollars,
    {{ utils.safe_divide('revenue', 'orders') }} as avg_order_value
FROM customer_metrics
```

## How Templating Works

1. **Parse**: LeapSQL reads your `.sql` file
2. **Render**: Template expressions and control flow are evaluated
3. **Execute**: The resulting SQL is run against the database

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Template SQL   │───▶│  Rendered SQL   │───▶│    Execute      │
│                 │    │                 │    │                 │
│ {{ env.DATE }}  │    │ '2024-01-15'    │    │  Query runs     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Global Variables

LeapSQL provides several built-in global variables:

| Variable | Description |
|----------|-------------|
| `env` | Environment variables |
| `config` | Project configuration |
| `target` | Current target environment |
| `this` | Current model's metadata |
| `var()` | Function to get variables with defaults |

See [Global Variables](/templating/globals) for complete documentation.

## When to Use Templating

### Good Use Cases

- **Environment-specific logic**: Different behavior for dev/prod
- **Dynamic column lists**: Generate columns from configuration
- **Reusable patterns**: DRY up common SQL snippets
- **Date handling**: Dynamic date ranges

### When to Avoid

- **Simple queries**: Don't over-engineer
- **Complex logic**: Consider moving to macros
- **Readability suffers**: Keep templates understandable

## Template vs Raw SQL

Compare these approaches:

**Without templating:**
```sql
-- Need to update this date manually
SELECT * FROM events WHERE date >= '2024-01-01'
```

**With templating:**
```sql
-- Date comes from environment
SELECT * FROM events WHERE date >= '{{ env.START_DATE }}'
```

**With macro:**
```sql
-- Reusable date logic
SELECT * FROM events WHERE {{ utils.date_filter('date', 30) }}
```

## Error Handling

Template errors are caught during parsing:

```sql
-- Undefined variable
SELECT * FROM {{ undefined_var }}
-- Error: name 'undefined_var' is not defined

-- Syntax error
SELECT * FROM {* if *}
-- Error: expected expression after 'if'
```

Errors include:
- File path
- Line number  
- Description of the problem

## Next Steps

- [Expressions](/templating/expressions) - Expression syntax and operators
- [Control Flow](/templating/control-flow) - If statements and loops
- [Global Variables](/templating/globals) - Built-in variables
- [Macros](/macros/overview) - Reusable functions
