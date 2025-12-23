// Package modeling provides project-level modeling rules.
//
// These rules analyze the DAG structure and model relationships:
//
//   - PM01: Root Models - Models with no sources (broken DAG lineage)
//   - PM02: Source Fanout - Source referenced by >1 non-staging model
//   - PM03: Staging Depends on Staging - Staging model references another staging model
//   - PM04: Model Fanout - Model with >3 direct leaf children
//   - PM05: Too Many Joins - Model references >7 upstream models
//   - PM06: Downstream on Source - Marts/intermediate depends directly on source
package modeling
