---
title: Expressions
description: Template expression syntax and evaluation
---

Expressions are the core of LeapSQL templating. They evaluate Starlark code and insert the result into your SQL.

## Syntax

Expressions are wrapped in double curly braces:

```sql
{{ expression }}
```

The expression is evaluated and the result is converted to a string and inserted into the SQL.

## Basic Examples

### Variable Access

```sql
-- Access environment variable
SELECT * FROM events WHERE date >= '{{ env.START_DATE }}'

-- Access nested property
SELECT * FROM users WHERE region = '{{ config.default_region }}'
```

### String Literals

```sql
-- String concatenation
SELECT * FROM {{ 'staging_' + 'users' }}

-- Result: SELECT * FROM staging_users
```

### Numbers

```sql
-- Arithmetic
SELECT * FROM events LIMIT {{ 100 * 10 }}

-- Result: SELECT * FROM events LIMIT 1000
```

### Function Calls

```sql
-- Built-in functions
SELECT * FROM users WHERE name LIKE '%{{ 'SMITH'.lower() }}%'

-- Result: SELECT * FROM users WHERE name LIKE '%smith%'
```

## Starlark Data Types

### Strings

```sql
-- String methods
{{ 'hello'.upper() }}          -- HELLO
{{ 'HELLO'.lower() }}          -- hello
{{ '  hello  '.strip() }}      -- hello
{{ 'hello'.replace('l', 'L') }} -- heLLo

-- String formatting
{{ 'Hello, {}!'.format('World') }}  -- Hello, World!
{{ '{} + {} = {}'.format(1, 2, 3) }} -- 1 + 2 = 3
```

### Numbers

```sql
-- Integer operations
{{ 10 + 5 }}   -- 15
{{ 10 - 5 }}   -- 5
{{ 10 * 5 }}   -- 50
{{ 10 // 3 }}  -- 3 (integer division)
{{ 10 % 3 }}   -- 1 (modulo)

-- Float operations
{{ 10 / 3 }}   -- 3.3333...
{{ 10.5 + 2.3 }} -- 12.8
```

### Booleans

```sql
-- Boolean values
{{ True }}   -- True
{{ False }}  -- False

-- Comparisons
{{ 5 > 3 }}    -- True
{{ 5 == 5 }}   -- True
{{ 5 != 3 }}   -- True
{{ 5 >= 5 }}   -- True
{{ 5 <= 10 }}  -- True

-- Logical operators
{{ True and False }}  -- False
{{ True or False }}   -- True
{{ not True }}        -- False
```

### Lists

```sql
-- List creation
{{ [1, 2, 3] }}

-- List access
{{ ['a', 'b', 'c'][0] }}  -- a
{{ ['a', 'b', 'c'][-1] }} -- c (last element)

-- List methods
{{ [1, 2, 3] + [4, 5] }}  -- [1, 2, 3, 4, 5]
{{ len([1, 2, 3]) }}      -- 3
{{ 2 in [1, 2, 3] }}      -- True
```

### Dictionaries

```sql
-- Dict creation
{{ {'name': 'Alice', 'age': 30} }}

-- Dict access
{{ {'name': 'Alice'}['name'] }}   -- Alice
{{ {'name': 'Alice'}.get('name') }} -- Alice
{{ {'name': 'Alice'}.get('email', 'N/A') }} -- N/A (with default)

-- Dict methods
{{ {'a': 1, 'b': 2}.keys() }}   -- ['a', 'b']
{{ {'a': 1, 'b': 2}.values() }} -- [1, 2]
{{ {'a': 1, 'b': 2}.items() }}  -- [('a', 1), ('b', 2)]
```

## Conditional Expressions

Use ternary-style conditionals for inline logic:

```sql
-- Ternary conditional
{{ 'prod_table' if target.name == 'prod' else 'dev_table' }}

-- In context
SELECT * FROM {{ 'customers' if target.name == 'prod' else 'sample_customers' }}
```

## String Formatting

### format() Method

```sql
-- Positional arguments
{{ 'SELECT * FROM {} WHERE {} = {}'.format('users', 'id', 1) }}

-- Named arguments  
{{ 'SELECT * FROM {table} WHERE {col} = {val}'.format(table='users', col='id', val=1) }}
```

### Building SQL Dynamically

```sql
-- Build column list
{{ ', '.join(['id', 'name', 'email']) }}
-- Result: id, name, email

-- Build IN clause
{{ ', '.join(["'{}'".format(x) for x in ['a', 'b', 'c']]) }}
-- Result: 'a', 'b', 'c'
```

## Working with Lists

### List Comprehensions

```sql
-- Transform list elements
{{ [x.upper() for x in ['a', 'b', 'c']] }}
-- Result: ['A', 'B', 'C']

-- Filter list elements
{{ [x for x in [1, 2, 3, 4, 5] if x > 2] }}
-- Result: [3, 4, 5]

-- Combined transform and filter
{{ [x * 2 for x in [1, 2, 3, 4] if x % 2 == 0] }}
-- Result: [4, 8]
```

### Join for SQL Lists

```sql
-- Column list
SELECT {{ ', '.join(columns) }}
FROM my_table

-- IN clause
WHERE status IN ({{ ', '.join(["'" + s + "'" for s in statuses]) }})
```

## Accessing Globals

### Environment Variables

```sql
-- Direct access
{{ env.DATABASE_NAME }}
{{ env.START_DATE }}

-- With default using get()
{{ env.get('SCHEMA', 'public') }}
```

### Configuration

```sql
-- Project config
{{ config.project_name }}
{{ config.version }}
```

### Target Environment

```sql
-- Current target
{{ target.name }}      -- 'dev', 'prod', etc.
{{ target.schema }}    -- Target schema
```

### Model Metadata

```sql
-- Current model info
{{ this.name }}        -- Model name
{{ this.schema }}      -- Model schema
{{ this.tags }}        -- Model tags
{{ this.meta }}        -- Model meta dict
```

## Calling Macros

Macros defined in `.star` files are available as global functions:

```sql
-- Call macro from utils.star
{{ utils.cents_to_dollars('amount') }}

-- Call with multiple arguments
{{ utils.date_diff('start_date', 'end_date', 'days') }}
```

## Expression Best Practices

### 1. Keep Expressions Simple

```sql
-- Good: Simple and readable
{{ env.START_DATE }}

-- Bad: Too complex inline
{{ (datetime.strptime(env.START_DATE, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d') }}
-- Move this to a macro instead
```

### 2. Use Macros for Complex Logic

```sql
-- Instead of complex expressions, use a macro:
{{ utils.date_30_days_ago() }}
```

### 3. Quote String Values

```sql
-- Remember to add SQL quotes for string values
WHERE name = '{{ user_name }}'
WHERE date >= '{{ env.START_DATE }}'
```

### 4. Handle None/Missing Values

```sql
-- Use get() with defaults
{{ env.get('OPTIONAL_VAR', 'default_value') }}

-- Or use conditional
{{ value if value else 'fallback' }}
```

## Common Patterns

### Dynamic Table Names

```sql
SELECT * FROM {{ env.get('SCHEMA', 'public') }}.{{ table_name }}
```

### Date Ranges

```sql
WHERE created_at BETWEEN '{{ env.START_DATE }}' AND '{{ env.END_DATE }}'
```

### Environment-Specific Values

```sql
LIMIT {{ 100 if target.name == 'dev' else 1000000 }}
```

### Building CASE Statements

```sql
CASE
    {* for code, label in status_codes.items() *}
    WHEN status = '{{ code }}' THEN '{{ label }}'
    {* endfor *}
    ELSE 'Unknown'
END as status_label
```

## Next Steps

- [Control Flow](/templating/control-flow) - If statements and loops
- [Global Variables](/templating/globals) - All available globals
- [Writing Macros](/macros/writing-macros) - Create reusable functions
