# LeapSQL Example Project

This project demonstrates LeapSQL's SQL transformation capabilities with a
simple e-commerce data model.

## Project Structure

```
├── seeds/              # Raw CSV data loaded into DuckDB
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   └── raw_products.csv
├── models/
│   ├── staging/        # Clean and standardize raw data
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── stg_products.sql
│   └── marts/          # Business logic and aggregations
│       ├── dim_customers.sql
│       └── fct_orders.sql
├── macros/             # Reusable Starlark functions
│   └── utils.star
└── leapsql.yaml        # Configuration
```

## Quick Start

```bash
# Load seed data into DuckDB
leapsql seed

# Run all models in dependency order
leapsql run

# View models and their dependencies
leapsql list

# Visualize the DAG
leapsql dag

# Render a model's compiled SQL
leapsql render marts.fct_orders
```

## Data Flow

```
raw_customers.csv ──► stg_customers ──► dim_customers
                                    │
raw_orders.csv ─────► stg_orders ───┼──► fct_orders
                                    │
raw_products.csv ───► stg_products ─┘
```

## Model Descriptions

### Staging Layer
- **stg_customers**: Cleaned customer data with standardized column names
- **stg_orders**: Order data with normalized status values
- **stg_products**: Product catalog with categorization

### Marts Layer
- **dim_customers**: Customer dimension with aggregated order statistics
- **fct_orders**: Fact table joining orders with customer and product details

## Exploring Results

After running the models, you can query the results directly:

```bash
# Open DuckDB CLI
duckdb warehouse.duckdb

# Query the fact table
SELECT * FROM marts.fct_orders LIMIT 10;

# Check customer statistics
SELECT * FROM marts.dim_customers ORDER BY total_orders DESC;
```
