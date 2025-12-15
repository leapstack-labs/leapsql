package starlark

import (
	"go.starlark.net/starlark"
)

// ConfigToStarlark converts frontmatter config map to a Starlark dict.
// The config dict is accessible as "config" global in templates.
func ConfigToStarlark(config map[string]any) (starlark.Value, error) {
	if config == nil {
		return starlark.NewDict(0), nil
	}
	return GoToStarlark(config)
}

// BuildConfigDict creates a config dict from FrontmatterConfig-like values.
// This is a convenience function that builds the map from individual fields.
func BuildConfigDict(name, materialized, uniqueKey, owner, schema string, tags []string, meta map[string]any) starlark.Value {
	dict := starlark.NewDict(8)

	if name != "" {
		_ = dict.SetKey(starlark.String("name"), starlark.String(name))
	}
	if materialized != "" {
		_ = dict.SetKey(starlark.String("materialized"), starlark.String(materialized))
	}
	if uniqueKey != "" {
		_ = dict.SetKey(starlark.String("unique_key"), starlark.String(uniqueKey))
	}
	if owner != "" {
		_ = dict.SetKey(starlark.String("owner"), starlark.String(owner))
	}
	if schema != "" {
		_ = dict.SetKey(starlark.String("schema"), starlark.String(schema))
	}

	if len(tags) > 0 {
		tagList := make([]starlark.Value, len(tags))
		for i, t := range tags {
			tagList[i] = starlark.String(t)
		}
		_ = dict.SetKey(starlark.String("tags"), starlark.NewList(tagList))
	}

	if len(meta) > 0 {
		metaVal, err := GoToStarlark(meta)
		if err == nil {
			_ = dict.SetKey(starlark.String("meta"), metaVal)
		}
	}

	return dict
}

// EnvToStarlark converts environment string to Starlark value.
// The env string is accessible as "env" global in templates.
func EnvToStarlark(env string) starlark.Value {
	return starlark.String(env)
}

// Predeclared returns all predeclared/builtin globals for template execution.
// This includes: config, env, target, this
// Note: Macros are added separately via the macro loader.
func Predeclared(config starlark.Value, env string, target *TargetInfo, this *ThisInfo) starlark.StringDict {
	globals := starlark.StringDict{
		"config": config,
		"env":    EnvToStarlark(env),
	}

	if target != nil {
		globals["target"] = target.ToStarlark()
	}

	if this != nil {
		globals["this"] = this.ToStarlark()
	}

	return globals
}
