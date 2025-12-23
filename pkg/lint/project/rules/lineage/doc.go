// Package lineage provides project-level lineage rules.
//
// These rules analyze column-level lineage to detect architectural issues
// that SQL-level linting cannot catch:
//
//   - PL01: Passthrough Bloat - Too many columns without transformation
//   - PL02: Orphaned Columns - Columns never used by downstream models
//   - PL04: Implicit Cross-Join - JOINs with no visible join keys
//
// These are LeapSQL-exclusive rules that leverage the column lineage parser.
package lineage
