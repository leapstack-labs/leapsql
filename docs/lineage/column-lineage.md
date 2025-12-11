---
title: Column-Level Lineage
description: Track data flow at the column level with LeapSQL's fine-grained lineage analysis
---

# Column-Level Lineage

Column-level lineage provides granular tracking of how individual columns flow through your SQL transformations. LeapSQL's lineage engine analyzes SELECT statements to determine exactly which source columns contribute to each output column.

## Understanding Column Lineage

Column-level lineage answers the question: "Where does this output column's data come from?"

For each output column, LeapSQL tracks:
- **Source columns** - The input columns that contribute to the output
- **Source tables** - Which tables those columns belong to
- **Transform type** - Whether the data is passed through directly or transformed
- **Function** - For aggregates/windows, which function was applied

## Transform Types

LeapSQL classifies column transformations into two categories:

### Direct Transform

A column passes through unchanged from source to output:

```sql
SELECT id, name, email FROM users
--     ^    ^     ^
--     All three are direct transforms
```

Direct transforms preserve the exact values from the source column. Functions that pass through a single column value (like `UPPER()`, `LOWER()`, `TRIM()`) are also considered direct transforms.

### Expression Transform

A column is derived from computation, aggregation, or multiple sources:

```sql
SELECT 
    price * quantity AS total,    -- Expression (2 source columns)
    COUNT(*) AS order_count,      -- Aggregate function
    SUM(amount) AS revenue,       -- Aggregate function  
    'literal' AS label            -- No source columns
FROM orders
```

## Extracting Column Lineage

Use the `ExtractLineage` function to analyze SQL:

```go
import "github.com/yacobolo/leapsql/pkg/lineage"

sql := `
SELECT 
    u.id,
    u.name,
    COUNT(o.id) AS order_count,
    SUM(o.amount) AS total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name
`

result, err := lineage.ExtractLineage(sql, nil)
if err != nil {
    log.Fatal(err)
}

// Examine each output column
for _, col := range result.Columns {
    fmt.Printf("Column: %s\n", col.Name)
    fmt.Printf("  Transform: %s\n", col.Transform)
    fmt.Printf("  Function: %s\n", col.Function)
    for _, src := range col.Sources {
        fmt.Printf("  Source: %s.%s\n", src.Table, src.Column)
    }
}
```

Output:
```
Column: id
  Transform: DIRECT
  Function: 
  Source: users.id

Column: name
  Transform: DIRECT
  Function: 
  Source: users.name

Column: order_count
  Transform: EXPR
  Function: count
  Source: orders.id

Column: total_spent
  Transform: EXPR
  Function: sum
  Source: orders.amount
```

## Schema for Star Expansion

When queries use `SELECT *`, provide schema information for complete column tracking:

```go
schema := lineage.Schema{
    "users":  {"id", "name", "email", "created_at"},
    "orders": {"id", "user_id", "amount", "status"},
}

sql := `SELECT * FROM users`

result, err := lineage.ExtractLineage(sql, schema)
// Returns lineage for all 4 columns: id, name, email, created_at
```

Without schema, star expressions are tracked as a single `*` column.

## Complex Expression Analysis

### Binary Expressions

Columns combined with operators track all source columns:

```sql
SELECT 
    first_name || ' ' || last_name AS full_name,
    price * quantity AS line_total,
    amount / 100 AS amount_dollars
FROM order_items
```

- `full_name` - Sources: `first_name`, `last_name` (Transform: EXPR)
- `line_total` - Sources: `price`, `quantity` (Transform: EXPR)
- `amount_dollars` - Sources: `amount` (Transform: EXPR)

### CASE Expressions

CASE tracks all columns referenced in conditions and results:

```sql
SELECT 
    id,
    CASE 
        WHEN status = 'active' THEN 'Active'
        WHEN status = 'pending' THEN 'Pending'
        ELSE 'Unknown'
    END AS status_label
FROM users
```

- `status_label` - Sources: `status` (Transform: EXPR)

### COALESCE and NULL Handling

```sql
SELECT 
    COALESCE(nickname, first_name, 'Anonymous') AS display_name
FROM users
```

- `display_name` - Sources: `nickname`, `first_name` (Transform: EXPR)

## Aggregate Function Tracking

Aggregates are tracked with their function name:

```sql
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total,
    AVG(amount) AS average,
    MIN(amount) AS minimum,
    MAX(amount) AS maximum
FROM orders
GROUP BY customer_id
```

| Column | Sources | Transform | Function |
|--------|---------|-----------|----------|
| customer_id | orders.customer_id | DIRECT | - |
| order_count | - | EXPR | count |
| total | orders.amount | EXPR | sum |
| average | orders.amount | EXPR | avg |
| minimum | orders.amount | EXPR | min |
| maximum | orders.amount | EXPR | max |

## Window Function Tracking

Window functions are tracked similarly to aggregates:

```sql
SELECT 
    id,
    amount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running_total,
    ROW_NUMBER() OVER (ORDER BY created_at) AS row_num,
    LAG(amount) OVER (ORDER BY created_at) AS prev_amount
FROM orders
```

| Column | Transform | Function |
|--------|-----------|----------|
| id | DIRECT | - |
| amount | DIRECT | - |
| running_total | EXPR | sum |
| row_num | EXPR | row_number |
| prev_amount | EXPR | lag |

## CTE and Subquery Lineage

LeapSQL traces lineage through CTEs and subqueries to underlying physical tables:

```sql
WITH monthly_totals AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', created_at) AS month,
        SUM(amount) AS total
    FROM orders
    GROUP BY customer_id, DATE_TRUNC('month', created_at)
)
SELECT customer_id, month, total
FROM monthly_totals
```

The output columns trace back to `orders`, not `monthly_totals`:
- Sources: `["orders"]`
- Columns reference the underlying `orders` table

## Join Lineage

Columns from joined tables maintain their source attribution:

```sql
SELECT 
    u.name AS customer_name,
    o.amount,
    p.name AS product_name
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN products p ON o.product_id = p.id
```

| Column | Source Table |
|--------|--------------|
| customer_name | users |
| amount | orders |
| product_name | products |

## Set Operation Lineage

UNION, INTERSECT, and EXCEPT combine sources from both sides:

```sql
SELECT id, name FROM customers
UNION ALL
SELECT id, name FROM suppliers
```

- `id` - Sources: `customers.id`, `suppliers.id` (Transform: EXPR)
- `name` - Sources: `customers.name`, `suppliers.name` (Transform: EXPR)

Set operations always result in EXPR transforms since values come from multiple sources.

## Generator Functions

Functions that generate values without input columns have no sources:

```sql
SELECT 
    id,
    NOW() AS current_time,
    RANDOM() AS rand_value
FROM users
```

| Column | Sources | Transform |
|--------|---------|-----------|
| id | users.id | DIRECT |
| current_time | (none) | EXPR |
| rand_value | (none) | EXPR |

## Use Cases

### Data Quality Impact Analysis

Before modifying a source column, understand what downstream columns depend on it:

```go
// Find all columns derived from users.email
for _, col := range result.Columns {
    for _, src := range col.Sources {
        if src.Table == "users" && src.Column == "email" {
            fmt.Printf("Column %s depends on users.email\n", col.Name)
        }
    }
}
```

### Compliance and Auditing

Track PII flow through transformations:

```go
piiColumns := map[string]bool{
    "email": true, "phone": true, "ssn": true,
}

for _, col := range result.Columns {
    for _, src := range col.Sources {
        if piiColumns[src.Column] {
            fmt.Printf("Output %s contains PII from %s.%s\n", 
                col.Name, src.Table, src.Column)
        }
    }
}
```

### Documentation Generation

Auto-generate column documentation from lineage:

```go
for _, col := range result.Columns {
    doc := fmt.Sprintf("**%s**", col.Name)
    if col.Transform == lineage.TransformDirect {
        doc += fmt.Sprintf(": Direct from %s", col.Sources[0].Column)
    } else if col.Function != "" {
        doc += fmt.Sprintf(": %s aggregation", col.Function)
    }
    fmt.Println(doc)
}
```

## Limitations

Current column lineage analysis has some limitations:

- **Scalar subqueries in SELECT**: Not traced (intentionally excluded)
- **Dynamic SQL**: Cannot be analyzed statically
- **UDFs**: User-defined functions are treated as passthrough
- **Complex type operations**: Array/JSON access has limited support

For queries LeapSQL cannot fully analyze, it returns partial lineage where possible rather than failing entirely.
