---
title: Global Variables
description: Built-in variables available in LeapSQL templates
---

# Global Variables

LeapSQL provides several global variables that are available in all templates. These let you access environment configuration, model metadata, and more.

## Available Globals

| Variable | Type | Description |
|----------|------|-------------|
| `env` | dict | Environment variables |
| `config` | dict | Project configuration |
| `target` | object | Current target environment |
| `this` | object | Current model metadata |
| `var()` | function | Get variables with defaults |

## env

Access to environment variables from the system.

### Usage

```sql
-- Direct access
SELECT * FROM events WHERE date >= '{{ env.START_DATE }}'

-- Safe access with get()
SELECT * FROM {{ env.get('SCHEMA', 'public') }}.users
```

### Properties

All system environment variables are available:

```sql
{{ env.HOME }}           -- /Users/username
{{ env.USER }}           -- username
{{ env.DATABASE_URL }}   -- postgres://...
{{ env.START_DATE }}     -- 2024-01-01
```

### Safe Access

Use `.get()` to provide defaults for optional variables:

```sql
-- Returns 'default_value' if MY_VAR is not set
{{ env.get('MY_VAR', 'default_value') }}

-- Returns None if not set (be careful!)
{{ env.get('OPTIONAL_VAR') }}
```

### Common Patterns

```sql
-- Date range from environment
WHERE created_at BETWEEN '{{ env.START_DATE }}' AND '{{ env.END_DATE }}'

-- Environment-specific schema
FROM {{ env.get('TARGET_SCHEMA', 'analytics') }}.customers

-- Feature flags
{* if env.get('ENABLE_FEATURE_X', 'false') == 'true' *}
-- Feature X SQL
{* endif *}
```

## config

Access to project configuration.

### Usage

```sql
SELECT * FROM {{ config.default_schema }}.users
```

### Available Properties

| Property | Type | Description |
|----------|------|-------------|
| `config.project_name` | string | Project name |
| `config.version` | string | Project version |
| `config.default_schema` | string | Default schema for models |
| `config.models_dir` | string | Models directory path |
| `config.seeds_dir` | string | Seeds directory path |
| `config.macros_dir` | string | Macros directory path |

### Example

```sql
/*---
name: versioned_table
---*/

SELECT
    *,
    '{{ config.version }}' as pipeline_version,
    '{{ config.project_name }}' as project
FROM source_table
```

## target

Information about the current execution target (environment).

### Usage

```sql
-- Different behavior per environment
SELECT *
FROM {* if target.name == 'prod' *}production{* else *}development{* endif *}.users
```

### Available Properties

| Property | Type | Description |
|----------|------|-------------|
| `target.name` | string | Target name (e.g., 'dev', 'prod') |
| `target.schema` | string | Target schema |
| `target.database` | string | Target database |

### Common Patterns

```sql
-- Environment-specific tables
FROM {{ target.schema }}.customers

-- Conditional logic
{* if target.name == 'prod' *}
-- Production-only code
{* endif *}

-- Sample data in dev
SELECT *
FROM customers
{* if target.name != 'prod' *}
LIMIT 1000
{* endif *}
```

### Environment Comparison

```sql
-- Full table in prod, sample in dev
SELECT
    *
FROM orders
{* if target.name == 'prod' *}
-- Full dataset
{* else *}
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
LIMIT 10000
{* endif *}
```

## this

Metadata about the current model being rendered.

### Usage

```sql
-- Access model properties
SELECT '{{ this.name }}' as model_name, *
FROM source_table
```

### Available Properties

| Property | Type | Description |
|----------|------|-------------|
| `this.name` | string | Model name |
| `this.materialized` | string | Materialization type |
| `this.schema` | string | Target schema |
| `this.owner` | string | Model owner |
| `this.tags` | list | Model tags |
| `this.meta` | dict | Model meta dictionary |

### Accessing Frontmatter

The `this` variable reflects your model's frontmatter:

```sql
/*---
name: customer_metrics
materialized: table
owner: analytics-team
schema: marts
tags:
  - customers
  - core
meta:
  refresh: daily
  priority: high
---*/

-- Access these values in the template
SELECT
    '{{ this.name }}' as model_name,
    '{{ this.owner }}' as owner,
    '{{ this.meta.refresh }}' as refresh_schedule,
    '{{ this.meta.priority }}' as priority,
    *
FROM source_data
```

### Using Meta for Configuration

```sql
/*---
name: configurable_model
meta:
  lookback_days: 30
  include_nulls: false
---*/

SELECT *
FROM events
WHERE event_date >= CURRENT_DATE - INTERVAL '{{ this.meta.lookback_days }} days'
{* if not this.meta.include_nulls *}
  AND event_value IS NOT NULL
{* endif *}
```

### Checking Tags

```sql
/*---
tags:
  - pii
  - sensitive
---*/

{* if 'pii' in this.tags *}
-- This model contains PII, add audit logging
SELECT *, CURRENT_TIMESTAMP as _accessed_at
{* else *}
SELECT *
{* endif *}
FROM source_table
```

## var()

Function to get variables with default values.

### Usage

```sql
-- Get variable with default
SELECT * FROM events
WHERE date >= '{{ var("start_date", "2024-01-01") }}'
```

### Behavior

1. Checks for the variable in the current context
2. Returns the default if not found
3. Raises error if no default provided and variable missing

### Examples

```sql
-- Required variable (errors if not set)
{{ var("required_param") }}

-- Optional with default
{{ var("optional_param", "default_value") }}

-- Numeric default
LIMIT {{ var("row_limit", 1000) }}

-- Boolean default
{* if var("include_deleted", False) *}
-- Include deleted records
{* endif *}
```

## Combining Globals

### Environment-Aware Models

```sql
/*---
name: user_report
meta:
  pii_columns: ['email', 'phone', 'address']
---*/

SELECT
    user_id,
    name,
    {* if target.name == 'prod' *}
        {* for col in this.meta.pii_columns *}
    {{ col }},
        {* endfor *}
    {* else *}
        {* for col in this.meta.pii_columns *}
    'REDACTED' as {{ col }},
        {* endfor *}
    {* endif *}
    created_at
FROM users
```

### Dynamic Configuration

```sql
/*---
name: metrics_summary
---*/

SELECT
    date,
    {{ var("metric_column", "revenue") }} as metric_value
FROM {{ env.get("METRICS_SCHEMA", config.default_schema) }}.daily_metrics
WHERE date >= '{{ var("start_date", env.get("DEFAULT_START", "2024-01-01")) }}'
{* if target.name != 'prod' *}
LIMIT {{ var("dev_limit", 10000) }}
{* endif *}
```

## Best Practices

### 1. Use Defaults for Optional Values

```sql
-- Good: Always has a value
{{ env.get('OPTIONAL_VAR', 'sensible_default') }}

-- Risky: May be None
{{ env.OPTIONAL_VAR }}
```

### 2. Document Expected Variables

```sql
/*---
name: configurable_report
meta:
  required_env_vars:
    - START_DATE: Beginning of date range
    - END_DATE: End of date range
  optional_env_vars:
    - SCHEMA: Target schema (default: analytics)
---*/
```

### 3. Validate in Complex Templates

```sql
{* if not env.get('REQUIRED_VAR') *}
{{ error('REQUIRED_VAR must be set') }}
{* endif *}
```

### 4. Keep Production Defaults Safe

```sql
-- Default to restrictive behavior
{* if env.get('INCLUDE_ALL', 'false') == 'true' *}
-- Include everything (must be explicitly enabled)
{* else *}
-- Default: Limited scope
{* endif *}
```

## Next Steps

- [Expressions](/templating/expressions) - Expression syntax
- [Control Flow](/templating/control-flow) - Conditionals and loops
- [Macros](/macros/overview) - Create reusable functions
