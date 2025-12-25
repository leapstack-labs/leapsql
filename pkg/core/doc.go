// Package core defines the shared language of the LeapSQL system.
//
// This package contains:
//   - Domain entities (Model, DialectConfig, Run, etc.)
//   - Service interfaces (Adapter, Store)
//   - Configuration types (ProjectConfig, TargetConfig)
//   - Base AST interface (Node)
//
// The Golden Rule: pkg/core imports ONLY pkg/token and stdlib.
// All other packages depend on core, not the reverse.
package core
