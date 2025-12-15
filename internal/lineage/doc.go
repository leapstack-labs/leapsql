// Package lineage provides domain-specific SQL lineage extraction.
//
// This package uses the sql parsing toolkit from pkg/sql to analyze SQL
// statements and extract data lineage information, including table-level
// and column-level dependencies.
//
// This is an internal package designed for use within the LeapSQL project.
// External consumers should use pkg/sql directly for SQL parsing needs.
//
// # Features
//
//   - Table Lineage: Identifies source and target tables in SQL statements
//   - Column Lineage: Tracks column-level data flow and transformations
//   - Schema-aware: Resolves column references against provided schemas
//
// # Basic Usage
//
//	schemas := map[string]sql.Schema{
//	    "users": {
//	        Columns: []string{"id", "name", "email"},
//	    },
//	}
//
//	result, err := lineage.ExtractLineage("SELECT id, name FROM users", schemas)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for _, col := range result.Columns {
//	    fmt.Printf("Column: %s, Sources: %v\n", col.Name, col.Sources)
//	}
package lineage
