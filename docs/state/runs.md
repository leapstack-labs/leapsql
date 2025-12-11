---
title: Run History
description: Track and query pipeline execution history in LeapSQL
---

# Run History

LeapSQL maintains a complete history of pipeline runs, including timing, status, and per-model execution details. This enables debugging, auditing, and performance analysis.

## Run Structure

Each pipeline execution creates a **Run** record:

```go
type Run struct {
    ID          string     // Unique run identifier (UUID)
    Environment string     // Environment name (e.g., "dev", "prod")
    Status      RunStatus  // running, completed, failed, cancelled
    StartedAt   time.Time  // When the run started
    CompletedAt *time.Time // When the run finished (nil if still running)
    Error       string     // Error message if failed
}
```

Within each run, individual model executions are tracked as **ModelRun** records:

```go
type ModelRun struct {
    ID           string         // Unique model run identifier
    RunID        string         // Parent run ID
    ModelID      string         // Model being executed
    Status       ModelRunStatus // pending, running, success, failed, skipped
    RowsAffected int64          // Number of rows affected
    StartedAt    time.Time      // When execution started
    CompletedAt  *time.Time     // When execution finished
    ExecutionMS  int64          // Execution time in milliseconds
    Error        string         // Error message if failed
}
```

## Querying Run History

### Latest Run

Get the most recent run for an environment:

```sql
SELECT id, environment, status, started_at, completed_at, error
FROM runs
WHERE environment = 'dev'
ORDER BY started_at DESC
LIMIT 1;
```

### Run Summary

Get a summary of recent runs:

```sql
SELECT 
    id,
    environment,
    status,
    started_at,
    completed_at,
    ROUND((JULIANDAY(completed_at) - JULIANDAY(started_at)) * 86400, 2) as duration_seconds
FROM runs
ORDER BY started_at DESC
LIMIT 10;
```

### Failed Runs

Find failed runs with error details:

```sql
SELECT id, environment, started_at, error
FROM runs
WHERE status = 'failed'
ORDER BY started_at DESC;
```

## Model Run Analysis

### Execution Times

Find the slowest models in a run:

```sql
SELECT 
    m.path,
    mr.execution_ms,
    mr.rows_affected,
    mr.status
FROM model_runs mr
JOIN models m ON mr.model_id = m.id
WHERE mr.run_id = 'your-run-id'
ORDER BY mr.execution_ms DESC
LIMIT 10;
```

### Model Success Rate

Calculate success rate per model:

```sql
SELECT 
    m.path,
    COUNT(*) as total_runs,
    SUM(CASE WHEN mr.status = 'success' THEN 1 ELSE 0 END) as successful,
    ROUND(100.0 * SUM(CASE WHEN mr.status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate
FROM model_runs mr
JOIN models m ON mr.model_id = m.id
GROUP BY m.path
ORDER BY success_rate ASC;
```

### Average Execution Time

Track performance trends:

```sql
SELECT 
    m.path,
    AVG(mr.execution_ms) as avg_ms,
    MIN(mr.execution_ms) as min_ms,
    MAX(mr.execution_ms) as max_ms
FROM model_runs mr
JOIN models m ON mr.model_id = m.id
WHERE mr.status = 'success'
GROUP BY m.path
ORDER BY avg_ms DESC;
```

## Run Lifecycle

### 1. Run Created

When `leapsql run` starts, a new run is created:

```sql
INSERT INTO runs (id, environment, status, started_at)
VALUES ('uuid', 'dev', 'running', CURRENT_TIMESTAMP);
```

### 2. Model Execution Begins

For each model, a model_run record is created:

```sql
INSERT INTO model_runs (id, run_id, model_id, status, started_at)
VALUES ('uuid', 'run-uuid', 'model-uuid', 'running', CURRENT_TIMESTAMP);
```

### 3. Model Execution Completes

After execution, the model_run is updated:

```sql
UPDATE model_runs 
SET status = 'success',
    rows_affected = 1000,
    completed_at = CURRENT_TIMESTAMP,
    execution_ms = 150
WHERE id = 'model-run-uuid';
```

### 4. Run Completes

When all models finish, the run is completed:

```sql
UPDATE runs 
SET status = 'completed',
    completed_at = CURRENT_TIMESTAMP
WHERE id = 'run-uuid';
```

## Inspecting Runs

### Using SQLite CLI

```bash
# Open the state database
sqlite3 .leapsql/state.db

# View recent runs
.headers on
.mode column
SELECT * FROM runs ORDER BY started_at DESC LIMIT 5;

# View model runs for the latest run
SELECT m.path, mr.status, mr.execution_ms 
FROM model_runs mr 
JOIN models m ON mr.model_id = m.id 
WHERE mr.run_id = (SELECT id FROM runs ORDER BY started_at DESC LIMIT 1);
```

### Run Details Report

Generate a detailed run report:

```sql
WITH run_stats AS (
    SELECT 
        r.id,
        r.environment,
        r.status,
        r.started_at,
        r.completed_at,
        COUNT(mr.id) as total_models,
        SUM(CASE WHEN mr.status = 'success' THEN 1 ELSE 0 END) as successful,
        SUM(CASE WHEN mr.status = 'failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN mr.status = 'skipped' THEN 1 ELSE 0 END) as skipped,
        SUM(mr.execution_ms) as total_execution_ms,
        SUM(mr.rows_affected) as total_rows
    FROM runs r
    LEFT JOIN model_runs mr ON r.id = mr.run_id
    GROUP BY r.id
)
SELECT * FROM run_stats ORDER BY started_at DESC LIMIT 10;
```

## Retention

State data grows over time. Consider periodic cleanup of old runs:

```sql
-- Delete runs older than 30 days
DELETE FROM runs 
WHERE started_at < DATE('now', '-30 days');

-- Model runs are automatically deleted via CASCADE
```

## Use Cases

### Debugging Failed Runs

1. Find the failed run:
   ```sql
   SELECT id, error FROM runs WHERE status = 'failed' ORDER BY started_at DESC LIMIT 1;
   ```

2. Find which models failed:
   ```sql
   SELECT m.path, mr.error 
   FROM model_runs mr 
   JOIN models m ON mr.model_id = m.id 
   WHERE mr.run_id = 'run-id' AND mr.status = 'failed';
   ```

### Performance Monitoring

Track execution time trends over time:

```sql
SELECT 
    DATE(r.started_at) as run_date,
    AVG(mr.execution_ms) as avg_model_ms,
    SUM(mr.execution_ms) as total_ms
FROM runs r
JOIN model_runs mr ON r.id = mr.run_id
WHERE r.status = 'completed'
GROUP BY DATE(r.started_at)
ORDER BY run_date DESC;
```

### Audit Trail

Track who/when models were last built:

```sql
SELECT 
    m.path,
    r.environment,
    mr.status,
    mr.started_at,
    mr.execution_ms
FROM models m
JOIN model_runs mr ON m.id = mr.model_id
JOIN runs r ON mr.run_id = r.id
WHERE mr.id IN (
    SELECT id FROM model_runs mr2 
    WHERE mr2.model_id = m.id 
    ORDER BY started_at DESC LIMIT 1
);
```
