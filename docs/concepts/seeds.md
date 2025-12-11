---
title: Seeds
description: Loading static data from CSV files
---

Seeds are CSV files that LeapSQL loads directly into your database as tables. They're useful for static reference data, lookup tables, and sample data for development.

## What are Seeds?

Seeds are CSV files in your `seeds/` directory that become database tables. Unlike models (which transform data), seeds provide source data.

```
my-project/
├── seeds/
│   ├── country_codes.csv
│   ├── status_mappings.csv
│   └── sample_customers.csv
└── models/
    └── ...
```

## Creating Seeds

### Basic CSV Format

Create a CSV file with headers in the first row:

```csv title="seeds/country_codes.csv"
code,name,region
US,United States,North America
CA,Canada,North America
UK,United Kingdom,Europe
DE,Germany,Europe
JP,Japan,Asia
```

### Requirements

- **First row must be headers**: Column names come from the header row
- **Valid CSV format**: Proper quoting, escaping, delimiters
- **Consistent columns**: All rows must have the same number of columns
- **File extension**: Must be `.csv`

## Loading Seeds

Use the `seed` command to load all seeds:

```bash
leapsql seed
```

This:
1. Scans the `seeds/` directory for CSV files
2. Creates or replaces tables for each file
3. Reports the number of rows loaded

### Output

```
Loading seeds...
  country_codes: 5 rows
  status_mappings: 4 rows
  sample_customers: 100 rows
Done! Loaded 3 seeds.
```

### Table Names

The table name matches the filename (without `.csv`):

| File | Table Name |
|------|------------|
| `country_codes.csv` | `country_codes` |
| `raw_customers.csv` | `raw_customers` |
| `2024_q1_data.csv` | `2024_q1_data` |

## Using Seeds in Models

Reference seed tables like any other table:

```sql
/*---
name: customer_regions
---*/

SELECT
    c.customer_id,
    c.name,
    c.country_code,
    cc.name as country_name,
    cc.region
FROM stg_customers c
LEFT JOIN country_codes cc ON c.country_code = cc.code
```

LeapSQL automatically detects the dependency on `country_codes` and ensures seeds are loaded before models run.

## Data Types

LeapSQL infers data types from CSV content:

| CSV Content | Inferred Type |
|-------------|---------------|
| `123`, `456` | INTEGER |
| `123.45`, `67.89` | DOUBLE |
| `true`, `false` | BOOLEAN |
| `2024-01-15` | DATE |
| `2024-01-15 10:30:00` | TIMESTAMP |
| Everything else | VARCHAR |

### Type Inference Examples

```csv
id,amount,is_active,created_at,name
1,99.99,true,2024-01-15,Alice
2,149.50,false,2024-01-16,Bob
```

Results in:
- `id`: INTEGER
- `amount`: DOUBLE  
- `is_active`: BOOLEAN
- `created_at`: DATE
- `name`: VARCHAR

## Common Use Cases

### Reference Data

Country codes, currency codes, status mappings:

```csv title="seeds/order_statuses.csv"
status_code,status_name,is_final
pending,Pending,false
processing,Processing,false
shipped,Shipped,false
delivered,Delivered,true
cancelled,Cancelled,true
```

### Lookup Tables

Mappings and translations:

```csv title="seeds/product_categories.csv"
category_id,category_name,parent_category
1,Electronics,
2,Computers,Electronics
3,Laptops,Computers
4,Clothing,
5,Men's Clothing,Clothing
```

### Development Data

Sample data for testing:

```csv title="seeds/sample_customers.csv"
id,name,email,created_at
1,Test User 1,test1@example.com,2024-01-01
2,Test User 2,test2@example.com,2024-01-02
3,Test User 3,test3@example.com,2024-01-03
```

### Configuration Data

Feature flags, thresholds, settings:

```csv title="seeds/config.csv"
key,value,description
min_order_amount,10.00,Minimum order value
max_discount_percent,50,Maximum discount allowed
free_shipping_threshold,100.00,Order value for free shipping
```

## Seed vs External Data

When to use seeds vs external data loading:

| Use Seeds When | Use External Loading When |
|----------------|---------------------------|
| Data is small (< 10K rows) | Data is large |
| Data changes infrequently | Data updates frequently |
| Data is version-controlled | Data comes from external systems |
| Same data across environments | Data varies by environment |

## Best Practices

### 1. Keep Seeds Small

Seeds are meant for reference data, not large datasets:

```
Good: country_codes.csv (200 rows)
Bad: all_transactions.csv (10M rows)
```

### 2. Version Control Seeds

Seeds should be committed to git:

```bash
git add seeds/country_codes.csv
git commit -m "Add country codes reference data"
```

### 3. Use Descriptive Names

Name seeds clearly:

```
Good: order_status_codes.csv
Bad: data.csv
```

### 4. Document in the CSV

Add a comment row or README for complex seeds:

```csv title="seeds/README.md"
# Seeds

## country_codes.csv
ISO 3166-1 country codes with regions.
Source: https://www.iso.org/iso-3166-country-codes.html

## order_statuses.csv
Valid order status values and their properties.
```

### 5. Handle Special Characters

Properly escape commas and quotes:

```csv
id,name,description
1,Widget,"A simple, basic widget"
2,Gadget,"The ""best"" gadget"
```

## Troubleshooting

### Type Mismatch Errors

If a seed column has mixed types:

```csv
id,value
1,100
2,abc    # String in numeric column!
```

Solution: Ensure consistent data types in each column, or cast in your model:

```sql
SELECT
    id,
    TRY_CAST(value AS INTEGER) as value
FROM my_seed
```

### Encoding Issues

If you see garbled characters:

1. Ensure CSV is UTF-8 encoded
2. Check for BOM (Byte Order Mark) characters
3. Use a text editor to verify encoding

### Missing Headers

If the first row is data, not headers:

```csv
1,Alice,alice@example.com  # Missing header row!
2,Bob,bob@example.com
```

Solution: Add a header row:

```csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

### Whitespace Issues

Extra whitespace can cause problems:

```csv
id, name ,email           # Spaces around headers
1 , Alice , alice@example.com  # Spaces in data
```

Solution: Trim whitespace in your model:

```sql
SELECT
    TRIM(id) as id,
    TRIM(name) as name,
    TRIM(email) as email
FROM my_seed
```

## Next Steps

- [Project Structure](/project-structure) - Directory organization
- [Models](/concepts/models) - Using seeds in models
- [CLI Seed Command](/cli/seed) - Seed command options
