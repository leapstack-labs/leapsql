---
title: PostgreSQL Adapter
description: Connect LeapSQL to PostgreSQL databases
---

# PostgreSQL Adapter

Connect LeapSQL to PostgreSQL databases for transformations on production-grade relational data.

## Configuration

```yaml
target:
  type: postgres
  host: localhost
  port: 5432
  database: mydb
  user: postgres
  password: secret
  schema: public
  options:
    sslmode: disable  # disable, prefer, require, verify-ca, verify-full
```

## Connection Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | localhost | PostgreSQL server hostname |
| `port` | 5432 | PostgreSQL server port |
| `database` | (required) | Database name |
| `user` | (optional) | Username for authentication |
| `password` | (optional) | Password for authentication |
| `schema` | public | Default schema for models |
| `options.sslmode` | disable | SSL connection mode |

## SSL Modes

PostgreSQL supports several SSL modes for secure connections:

- `disable` - No SSL (default, good for local development)
- `prefer` - Try SSL, fall back to non-SSL
- `require` - Require SSL, don't verify certificate
- `verify-ca` - Require SSL, verify server certificate
- `verify-full` - Require SSL, verify server certificate and hostname

For production environments, use `require` or higher:

```yaml
target:
  type: postgres
  host: prod-db.example.com
  port: 5432
  database: analytics
  user: analyst
  password: ${POSTGRES_PASSWORD}
  options:
    sslmode: require
```

## Loading Seeds (CSV)

The PostgreSQL adapter streams CSV data from your local machine to the database using `COPY ... FROM STDIN`. This works with both local and remote PostgreSQL servers.

**Note:** All CSV columns are created as `TEXT` type. Use SQL casts in your models to convert to appropriate types:

```sql
SELECT
    id::INTEGER,
    amount::NUMERIC(10,2),
    created_at::TIMESTAMP
FROM {{ ref('raw_orders') }}
```

## Example Configuration

```yaml
# leapsql.yaml
models_dir: models
seeds_dir: seeds

target:
  type: postgres
  host: localhost
  port: 5432
  database: analytics
  user: analyst
  password: ${POSTGRES_PASSWORD}
  schema: public

environments:
  prod:
    target:
      host: prod-db.example.com
      options:
        sslmode: require
```

## Environment Variables

For security, use environment variables for sensitive values:

```yaml
target:
  type: postgres
  host: ${POSTGRES_HOST}
  port: ${POSTGRES_PORT}
  database: ${POSTGRES_DB}
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
```

## Local Development with Docker

For local development, you can use Docker to run PostgreSQL:

```bash
# Start PostgreSQL
docker run -d \
  --name leapsql-postgres \
  -e POSTGRES_USER=leapsql \
  -e POSTGRES_PASSWORD=leapsql \
  -e POSTGRES_DB=analytics \
  -p 5432:5432 \
  postgres:16

# Configure leapsql.yaml
target:
  type: postgres
  host: localhost
  port: 5432
  database: analytics
  user: leapsql
  password: leapsql
```

## Differences from DuckDB

| Feature | PostgreSQL | DuckDB |
|---------|------------|--------|
| CSV Loading | `COPY FROM STDIN` (streams data) | `read_csv_auto` (reads file directly) |
| Type Inference | All columns as TEXT | Automatic type inference |
| Schema Default | `public` | `main` |
| Parameter Syntax | `$1, $2, ...` | `?, ?, ...` |

## Reserved Words

The PostgreSQL adapter automatically quotes column names that are reserved words:

- `user`, `order`, `group`, `table`, `select`, `from`, `where`, `index`

For example, a CSV with a column named `order` will create a table with column `"order"`.

## Troubleshooting

### Connection Refused

```
failed to ping postgres: dial tcp 127.0.0.1:5432: connect: connection refused
```

Ensure PostgreSQL is running and accepting connections on the specified host and port.

### Authentication Failed

```
failed to ping postgres: password authentication failed for user "..."
```

Verify your username and password are correct. Check PostgreSQL's `pg_hba.conf` for authentication settings.

### SSL Required

```
failed to ping postgres: server does not support SSL, but SSL was required
```

Either configure your PostgreSQL server for SSL, or set `sslmode: disable` for local development.

### Table Not Found

```
table users not found
```

Ensure the table exists in the specified schema. Use `schema.table` notation if the table is not in the default schema:

```yaml
target:
  schema: analytics  # Default schema
```

Or reference tables with explicit schema:

```sql
SELECT * FROM analytics.users
```
