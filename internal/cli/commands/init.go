package commands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/spf13/cobra"
)

// NewInitCommand creates the init command.
func NewInitCommand() *cobra.Command {
	var force bool
	var example bool

	cmd := &cobra.Command{
		Use:   "init [directory]",
		Short: "Initialize a new LeapSQL project",
		Long: `Initialize a new LeapSQL project with default directory structure and configuration.

This creates:
  - models/ directory for SQL models
  - seeds/ directory for seed data CSV files
  - macros/ directory for Starlark macros
  - leapsql.yaml configuration file

Use --example to create a full working demo project with sample data, 
models (staging + marts), and macros demonstrating best practices.`,
		Example: `  # Initialize in current directory
  leapsql init

  # Initialize with a full working example
  leapsql init --example

  # Initialize in a new directory
  leapsql init my-project --example

  # Force overwrite existing config
  leapsql init --force`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) > 0 {
				dir = args[0]
			}

			// Create renderer
			cfg := getConfig()
			mode := output.Mode(cfg.OutputFormat)
			r := output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), mode)

			if example {
				return runInitExample(r, dir, force)
			}
			return runInit(r, dir, force)
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing configuration")
	cmd.Flags().BoolVar(&example, "example", false, "Create a full example project with seeds, models, and macros")

	return cmd
}

func runInit(r *output.Renderer, dir string, force bool) error {
	// Create directory if specified and doesn't exist
	if dir != "." {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	configPath := filepath.Join(dir, "leapsql.yaml")

	// Check if config already exists
	if _, err := os.Stat(configPath); err == nil && !force {
		return fmt.Errorf("leapsql.yaml already exists. Use --force to overwrite")
	}

	// Create directories
	dirs := []string{
		filepath.Join(dir, "models"),
		filepath.Join(dir, "models", "staging"),
		filepath.Join(dir, "models", "marts"),
		filepath.Join(dir, "seeds"),
		filepath.Join(dir, "macros"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0750); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", d, err)
		}
		r.StatusLine(d+"/", "success", "")
	}

	// Create config file
	if err := os.WriteFile(configPath, []byte(minimalConfig), 0600); err != nil {
		return fmt.Errorf("failed to create leapsql.yaml: %w", err)
	}
	r.StatusLine("leapsql.yaml", "success", "")

	// Create example model
	modelPath := filepath.Join(dir, "models", "staging", "stg_example.sql")
	if _, err := os.Stat(modelPath); os.IsNotExist(err) || force {
		if err := os.WriteFile(modelPath, []byte(minimalModel), 0600); err != nil {
			return fmt.Errorf("failed to create example model: %w", err)
		}
		r.StatusLine("models/staging/stg_example.sql", "success", "")
	}

	// Create .gitignore
	gitignorePath := filepath.Join(dir, ".gitignore")
	if _, err := os.Stat(gitignorePath); os.IsNotExist(err) || force {
		if err := os.WriteFile(gitignorePath, []byte(gitignoreContent), 0600); err != nil {
			r.Warning(fmt.Sprintf("failed to create .gitignore: %v", err))
		} else {
			r.StatusLine(".gitignore", "success", "")
		}
	}

	r.Println("")
	r.Success("LeapSQL project initialized!")
	r.Println("")
	r.Println("Next steps:")
	r.Println("  1. Add your seed data to seeds/")
	r.Println("  2. Create SQL models in models/")
	r.Println("  3. Run 'leapsql run' to execute models")
	r.Println("  4. Run 'leapsql list' to see all models")

	return nil
}

func runInitExample(r *output.Renderer, dir string, force bool) error {
	// Create directory if specified and doesn't exist
	if dir != "." {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	configPath := filepath.Join(dir, "leapsql.yaml")

	// Check if config already exists
	if _, err := os.Stat(configPath); err == nil && !force {
		return fmt.Errorf("leapsql.yaml already exists. Use --force to overwrite")
	}

	// Create directories
	dirs := []string{
		filepath.Join(dir, "models"),
		filepath.Join(dir, "models", "staging"),
		filepath.Join(dir, "models", "marts"),
		filepath.Join(dir, "seeds"),
		filepath.Join(dir, "macros"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0750); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", d, err)
		}
	}

	// Track created files for output
	type fileCreate struct {
		path    string
		content string
		info    string
	}

	// Config files
	configFiles := []fileCreate{
		{filepath.Join(dir, "leapsql.yaml"), exampleConfig, ""},
		{filepath.Join(dir, ".gitignore"), gitignoreContent, ""},
		{filepath.Join(dir, "README.md"), readmeContent, ""},
	}

	r.Header(2, "Configuration")
	for _, f := range configFiles {
		if err := writeFileIfNotExists(f.path, f.content, force); err != nil {
			return err
		}
		relPath, _ := filepath.Rel(dir, f.path)
		if dir == "." {
			relPath = f.path
		}
		r.StatusLine(relPath, "success", f.info)
	}

	// Seed files
	seedFiles := []fileCreate{
		{filepath.Join(dir, "seeds", "raw_customers.csv"), seedCustomers, "5 rows"},
		{filepath.Join(dir, "seeds", "raw_orders.csv"), seedOrders, "10 rows"},
		{filepath.Join(dir, "seeds", "raw_products.csv"), seedProducts, "5 rows"},
	}

	r.Println("")
	r.Header(2, "Seeds")
	for _, f := range seedFiles {
		if err := writeFileIfNotExists(f.path, f.content, force); err != nil {
			return err
		}
		relPath, _ := filepath.Rel(dir, f.path)
		if dir == "." {
			relPath = f.path
		}
		r.StatusLine(relPath, "success", f.info)
	}

	// Model files
	modelFiles := []fileCreate{
		{filepath.Join(dir, "models", "staging", "stg_customers.sql"), modelStgCustomers, ""},
		{filepath.Join(dir, "models", "staging", "stg_orders.sql"), modelStgOrders, ""},
		{filepath.Join(dir, "models", "staging", "stg_products.sql"), modelStgProducts, ""},
		{filepath.Join(dir, "models", "marts", "dim_customers.sql"), modelDimCustomers, ""},
		{filepath.Join(dir, "models", "marts", "fct_orders.sql"), modelFctOrders, ""},
	}

	r.Println("")
	r.Header(2, "Models")
	for _, f := range modelFiles {
		if err := writeFileIfNotExists(f.path, f.content, force); err != nil {
			return err
		}
		relPath, _ := filepath.Rel(dir, f.path)
		if dir == "." {
			relPath = f.path
		}
		r.StatusLine(relPath, "success", f.info)
	}

	// Macro files
	macroFiles := []fileCreate{
		{filepath.Join(dir, "macros", "utils.star"), macroUtils, ""},
	}

	r.Println("")
	r.Header(2, "Macros")
	for _, f := range macroFiles {
		if err := writeFileIfNotExists(f.path, f.content, force); err != nil {
			return err
		}
		relPath, _ := filepath.Rel(dir, f.path)
		if dir == "." {
			relPath = f.path
		}
		r.StatusLine(relPath, "success", f.info)
	}

	r.Println("")
	r.Success("LeapSQL project initialized with example data!")
	r.Println("")
	r.Println("Next steps:")
	r.Println("  leapsql seed     Load CSV data into DuckDB")
	r.Println("  leapsql run      Execute all models in dependency order")
	r.Println("  leapsql list     View models and dependencies")
	r.Println("  leapsql dag      Visualize the dependency graph")

	return nil
}

func writeFileIfNotExists(path, content string, force bool) error {
	if _, err := os.Stat(path); os.IsNotExist(err) || force {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			return fmt.Errorf("failed to create %s: %w", path, err)
		}
	}
	return nil
}

// ============================================================================
// Template Content
// ============================================================================

const minimalConfig = `# LeapSQL Configuration
models_dir: models
seeds_dir: seeds
macros_dir: macros
state_path: .leapsql/state.db
environment: dev

# Database target
target:
  type: duckdb
  database: ""  # Empty for in-memory DuckDB
  schema: main
`

const minimalModel = `/*---
materialized: table
---*/
-- Example staging model
SELECT 
    1 as id,
    'example' as name,
    CURRENT_TIMESTAMP as created_at
`

const gitignoreContent = `# LeapSQL
.leapsql/
*.duckdb
*.duckdb.wal

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
`

const exampleConfig = `# LeapSQL Configuration
# Generated by: leapsql init --example

# Directory paths
models_dir: models
seeds_dir: seeds
macros_dir: macros

# State tracking for incremental runs
state_path: .leapsql/state.db

# Environment
environment: dev

# Default target (DuckDB for local development)
target:
  type: duckdb
  database: warehouse.duckdb
  schema: main

# Environment-specific targets
environments:
  dev:
    target:
      type: duckdb
      database: dev.duckdb
  prod:
    target:
      type: duckdb
      database: prod.duckdb
`

const readmeContent = `# LeapSQL Example Project

This project demonstrates LeapSQL's SQL transformation capabilities with a
simple e-commerce data model.

## Project Structure

` + "```" + `
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
` + "```" + `

## Quick Start

` + "```bash" + `
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
` + "```" + `

## Data Flow

` + "```" + `
raw_customers.csv ──► stg_customers ──► dim_customers
                                    │
raw_orders.csv ─────► stg_orders ───┼──► fct_orders
                                    │
raw_products.csv ───► stg_products ─┘
` + "```" + `

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

` + "```bash" + `
# Open DuckDB CLI
duckdb warehouse.duckdb

# Query the fact table
SELECT * FROM marts.fct_orders LIMIT 10;

# Check customer statistics
SELECT * FROM marts.dim_customers ORDER BY total_orders DESC;
` + "```" + `
`

// ============================================================================
// Seed Data
// ============================================================================

const seedCustomers = `id,name,email,created_at
1,Alice Johnson,alice@example.com,2024-01-15
2,Bob Smith,bob@example.com,2024-02-20
3,Carol Williams,carol@example.com,2024-03-10
4,David Brown,david@example.com,2024-04-05
5,Eve Davis,eve@example.com,2024-05-12
`

const seedOrders = `id,customer_id,product_id,quantity,order_date,status
1,1,1,2,2024-06-01,completed
2,1,3,1,2024-06-05,completed
3,2,2,3,2024-06-10,completed
4,3,1,1,2024-06-15,pending
5,2,4,2,2024-06-20,completed
6,4,5,1,2024-06-25,shipped
7,1,2,1,2024-07-01,completed
8,5,3,2,2024-07-05,pending
9,3,4,1,2024-07-10,completed
10,2,1,2,2024-07-15,completed
`

const seedProducts = `id,name,price,category
1,Laptop,999.99,Electronics
2,Headphones,149.99,Electronics
3,Coffee Maker,79.99,Home
4,Notebook,12.99,Office
5,Desk Lamp,45.99,Home
`

// ============================================================================
// Staging Models
// ============================================================================

const modelStgCustomers = `/*---
materialized: table
description: Cleaned customer data from raw source
owner: data-team
tags: [staging, customers]
---*/
SELECT
    id AS customer_id,
    name AS customer_name,
    LOWER(email) AS email,
    CAST(created_at AS DATE) AS signup_date
FROM raw_customers
`

const modelStgOrders = `/*---
materialized: table
description: Cleaned order data with standardized columns
owner: data-team
tags: [staging, orders]
---*/
SELECT
    id AS order_id,
    customer_id,
    product_id,
    quantity,
    CAST(order_date AS DATE) AS order_date,
    UPPER(status) AS order_status
FROM raw_orders
`

const modelStgProducts = `/*---
materialized: table
description: Product catalog with cleaned data
owner: data-team
tags: [staging, products]
---*/
SELECT
    id AS product_id,
    name AS product_name,
    price AS unit_price,
    UPPER(category) AS category
FROM raw_products
`

// ============================================================================
// Mart Models
// ============================================================================

const modelDimCustomers = `/*---
materialized: table
description: Customer dimension with order statistics
owner: analytics-team
tags: [marts, customers, dimension]
---*/
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.signup_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.quantity), 0) AS total_items_ordered,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM staging.stg_customers c
LEFT JOIN staging.stg_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.email, c.signup_date
`

const modelFctOrders = `/*---
materialized: table
description: Order fact table with customer and product details
owner: analytics-team
tags: [marts, orders, fact]
---*/
SELECT
    o.order_id,
    o.order_date,
    o.order_status,
    o.quantity,
    c.customer_id,
    c.customer_name,
    p.product_id,
    p.product_name,
    p.unit_price,
    p.category,
    (o.quantity * p.unit_price) AS line_total,
    {{ utils.generate_surrogate_key('o.order_id', 'o.customer_id', 'o.product_id') }} AS order_key
FROM staging.stg_orders o
JOIN staging.stg_customers c ON o.customer_id = c.customer_id
JOIN staging.stg_products p ON o.product_id = p.product_id
`

// ============================================================================
// Macros
// ============================================================================

const macroUtils = `# LeapSQL Utility Macros
# 
# This file contains reusable Starlark functions that can be called
# from SQL models using the {{ namespace.function() }} syntax.

def generate_surrogate_key(*columns):
    """
    Generate a surrogate key by hashing multiple columns.
    
    Usage in SQL:
        {{ utils.generate_surrogate_key('col1', 'col2', 'col3') }}
    
    Produces:
        MD5(CAST(col1 AS VARCHAR) || '-' || CAST(col2 AS VARCHAR) || '-' || CAST(col3 AS VARCHAR))
    """
    parts = []
    for col in columns:
        parts.append("CAST({} AS VARCHAR)".format(col))
    return "MD5({})".format(" || '-' || ".join(parts))


def safe_divide(numerator, denominator, default="0"):
    """
    Safely divide two values, returning a default if denominator is zero.
    
    Usage in SQL:
        {{ utils.safe_divide('revenue', 'quantity', '0') }}
    
    Produces:
        CASE WHEN quantity = 0 THEN 0 ELSE revenue / quantity END
    """
    return "CASE WHEN {} = 0 THEN {} ELSE {} / {} END".format(
        denominator, default, numerator, denominator
    )


def current_timestamp():
    """
    Return the current timestamp function for DuckDB.
    
    Usage in SQL:
        {{ utils.current_timestamp() }}
    
    Produces:
        CURRENT_TIMESTAMP
    """
    return "CURRENT_TIMESTAMP"
`
