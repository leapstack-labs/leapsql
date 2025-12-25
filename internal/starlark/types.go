// Package starlark provides Starlark execution context and builtins for template rendering.
package starlark

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// TargetInfo contains database adapter/target information.
// Exposed as the "target" global in Starlark execution.
type TargetInfo struct {
	Type     string // "duckdb", "postgres", "snowflake"
	Schema   string // Default schema
	Database string // Database name
}

// ThisInfo contains current model information.
// Exposed as the "this" global in Starlark execution.
type ThisInfo struct {
	Name   string // Current model name
	Schema string // Current model schema
}

// ToStarlark converts TargetInfo to a Starlark struct value.
func (t *TargetInfo) ToStarlark() starlark.Value {
	return starlarkstruct.FromStringDict(starlark.String("target"), starlark.StringDict{
		"type":     starlark.String(t.Type),
		"schema":   starlark.String(t.Schema),
		"database": starlark.String(t.Database),
	})
}

// ToStarlark converts ThisInfo to a Starlark struct value.
func (t *ThisInfo) ToStarlark() starlark.Value {
	return starlarkstruct.FromStringDict(starlark.String("this"), starlark.StringDict{
		"name":   starlark.String(t.Name),
		"schema": starlark.String(t.Schema),
	})
}

// TargetInfoFromConfig converts a core.TargetConfig to a TargetInfo for template rendering.
// This extracts only the fields that should be exposed to templates (not credentials).
func TargetInfoFromConfig(t *core.TargetConfig) *TargetInfo {
	if t == nil {
		return nil
	}
	return &TargetInfo{
		Type:     t.Type,
		Schema:   t.Schema,
		Database: t.Database,
	}
}

// GoToStarlark converts a Go value to a Starlark value.
// Supported types: string, int, int64, float64, bool, []string, []any, map[string]any
func GoToStarlark(v any) (starlark.Value, error) {
	if v == nil {
		return starlark.None, nil
	}

	switch val := v.(type) {
	case string:
		return starlark.String(val), nil

	case int:
		return starlark.MakeInt(val), nil

	case int64:
		return starlark.MakeInt64(val), nil

	case float64:
		return starlark.Float(val), nil

	case bool:
		return starlark.Bool(val), nil

	case []string:
		list := make([]starlark.Value, len(val))
		for i, s := range val {
			list[i] = starlark.String(s)
		}
		return starlark.NewList(list), nil

	case []any:
		list := make([]starlark.Value, len(val))
		for i, item := range val {
			sv, err := GoToStarlark(item)
			if err != nil {
				return nil, fmt.Errorf("list index %d: %w", i, err)
			}
			list[i] = sv
		}
		return starlark.NewList(list), nil

	case map[string]any:
		dict := starlark.NewDict(len(val))
		for k, v := range val {
			sv, err := GoToStarlark(v)
			if err != nil {
				return nil, fmt.Errorf("dict key %q: %w", k, err)
			}
			if err := dict.SetKey(starlark.String(k), sv); err != nil {
				return nil, fmt.Errorf("dict setkey %q: %w", k, err)
			}
		}
		return dict, nil

	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// ToGo converts a Starlark value back to a Go value.
// Returns: string, int64, float64, bool, []any, map[string]any, or nil
func ToGo(v starlark.Value) (any, error) {
	switch val := v.(type) {
	case starlark.NoneType:
		return nil, nil

	case starlark.String:
		return string(val), nil

	case starlark.Int:
		i64, ok := val.Int64()
		if !ok {
			// Fallback for very large integers - convert to string
			return val.String(), nil
		}
		return i64, nil

	case starlark.Float:
		return float64(val), nil

	case starlark.Bool:
		return bool(val), nil

	case *starlark.List:
		result := make([]any, val.Len())
		for i := 0; i < val.Len(); i++ {
			gv, err := ToGo(val.Index(i))
			if err != nil {
				return nil, fmt.Errorf("list index %d: %w", i, err)
			}
			result[i] = gv
		}
		return result, nil

	case *starlark.Dict:
		result := make(map[string]any)
		for _, item := range val.Items() {
			key, ok := item[0].(starlark.String)
			if !ok {
				return nil, fmt.Errorf("dict key must be string, got %T", item[0])
			}
			gv, err := ToGo(item[1])
			if err != nil {
				return nil, fmt.Errorf("dict key %q: %w", key, err)
			}
			result[string(key)] = gv
		}
		return result, nil

	case *starlark.Tuple:
		result := make([]any, val.Len())
		for i := 0; i < val.Len(); i++ {
			gv, err := ToGo(val.Index(i))
			if err != nil {
				return nil, fmt.Errorf("tuple index %d: %w", i, err)
			}
			result[i] = gv
		}
		return result, nil

	default:
		// Try to get a string representation
		return val.String(), nil
	}
}
