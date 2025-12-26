// Package projectrules registers all project health lint rules.
// Import this package to register all project-level rules with the global registry.
//
// Rule Categories:
//
// PL (Lineage): Rules about data lineage and dependencies
//   - PL01: Passthrough Bloat - Too many columns without transformation
//   - PL02: Orphaned Columns - Columns never used by downstream models
//   - PL04: Implicit Cross-Join - JOINs with no visible join keys
//   - PL05: Schema Drift - SELECT * from source with changed schema
//
// PM (Modeling): Rules about model structure and organization
//   - PM01: Root Models - Models with no sources (broken DAG lineage)
//   - PM02: Source Fanout - Source referenced by multiple non-staging models
//   - PM03: Staging Depends Staging - Staging model references another staging
//   - PM04: Model Fanout - Model has too many direct downstream consumers
//   - PM05: Too Many Joins - Model references too many upstream models
//   - PM06: Downstream on Source - Marts/intermediate depends directly on source
//   - PM07: Rejoining Upstream - Unnecessary intermediate model pattern
//
// PS (Structure): Rules about project structure and naming
//   - PS01: Model Naming - Model naming convention mismatch
//   - PS02: Model Directory - Model directory mismatch
package projectrules
