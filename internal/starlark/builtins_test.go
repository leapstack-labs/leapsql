package starlark

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func TestBuildConfigDict(t *testing.T) {
	dict := BuildConfigDict(
		"my_model",
		"incremental",
		"id",
		"data-team",
		"analytics",
		[]string{"finance", "metrics"},
		map[string]any{"priority": "high"},
	)

	require.NotNil(t, dict, "BuildConfigDict returned nil")

	d, ok := dict.(*starlark.Dict)
	require.True(t, ok, "expected *starlark.Dict, got %T", dict)

	// Check name
	nameVal, found, _ := d.Get(starlark.String("name"))
	assert.True(t, found, "name not found in dict")
	assert.Equal(t, `"my_model"`, nameVal.String(), "name value")

	// Check materialized
	matlVal, found, _ := d.Get(starlark.String("materialized"))
	assert.True(t, found, "materialized not found in dict")
	assert.Equal(t, `"incremental"`, matlVal.String(), "materialized value")

	// Check tags
	tagsVal, found, _ := d.Get(starlark.String("tags"))
	assert.True(t, found, "tags not found in dict")
	tagsList, ok := tagsVal.(*starlark.List)
	require.True(t, ok, "tags is not a list: %T", tagsVal)
	assert.Equal(t, 2, tagsList.Len(), "tags length")

	// Check meta
	metaVal, found, _ := d.Get(starlark.String("meta"))
	assert.True(t, found, "meta not found in dict")
	metaDict, ok := metaVal.(*starlark.Dict)
	require.True(t, ok, "meta is not a dict: %T", metaVal)
	priorityVal, found, _ := metaDict.Get(starlark.String("priority"))
	assert.True(t, found, "meta.priority not found")
	assert.Equal(t, `"high"`, priorityVal.String(), "meta.priority value")
}

func TestBuildConfigDict_Empty(t *testing.T) {
	dict := BuildConfigDict("", "", "", "", "", nil, nil)

	d, ok := dict.(*starlark.Dict)
	require.True(t, ok, "expected *starlark.Dict, got %T", dict)

	// Empty config should have no keys
	assert.Equal(t, 0, d.Len(), "expected empty dict")
}

func TestPredeclared(t *testing.T) {
	config := starlark.NewDict(1)
	config.SetKey(starlark.String("name"), starlark.String("test"))

	target := &TargetInfo{
		Type:     "duckdb",
		Schema:   "main",
		Database: "test.db",
	}

	this := &ThisInfo{
		Name:   "my_model",
		Schema: "analytics",
	}

	globals := Predeclared(config, "dev", target, this)

	// Check config
	_, ok := globals["config"]
	assert.True(t, ok, "config not found in globals")

	// Check env
	envVal, ok := globals["env"]
	assert.True(t, ok, "env not found in globals")
	assert.Equal(t, `"dev"`, envVal.String(), "env value")

	// Check target
	_, ok = globals["target"]
	assert.True(t, ok, "target not found in globals")

	// Check this
	_, ok = globals["this"]
	assert.True(t, ok, "this not found in globals")
}

func TestPredeclared_NilTarget(t *testing.T) {
	config := starlark.NewDict(0)
	globals := Predeclared(config, "prod", nil, nil)

	// Should have config and env
	_, ok := globals["config"]
	assert.True(t, ok, "config not found in globals")
	_, ok = globals["env"]
	assert.True(t, ok, "env not found in globals")

	// Should not have target or this
	_, ok = globals["target"]
	assert.False(t, ok, "target should not be in globals when nil")
	_, ok = globals["this"]
	assert.False(t, ok, "this should not be in globals when nil")
}
