package macro

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func TestRegistry_Register(t *testing.T) {
	registry := NewRegistry()

	module := &LoadedModule{
		Namespace: "datetime",
		Path:      "/path/to/datetime.star",
		Exports: starlark.StringDict{
			"now": starlark.String("func"),
		},
	}

	err := registry.Register(module)
	require.NoError(t, err, "unexpected error")

	assert.True(t, registry.Has("datetime"), "expected registry to have 'datetime'")
	assert.Equal(t, 1, registry.Len(), "expected len 1")
}

func TestRegistry_ReservedNamespace(t *testing.T) {
	for _, reserved := range ReservedNamespaces {
		t.Run(reserved, func(t *testing.T) {
			registry := NewRegistry()
			module := &LoadedModule{
				Namespace: reserved,
				Path:      "/path/to/" + reserved + ".star",
				Exports:   starlark.StringDict{},
			}

			err := registry.Register(module)
			require.Error(t, err, "expected error for reserved namespace %q", reserved)

			regErr, ok := err.(*RegistryError)
			require.True(t, ok, "expected *RegistryError, got %T", err)
			assert.Equal(t, reserved, regErr.Namespace, "expected namespace %q", reserved)
		})
	}
}

func TestRegistry_DuplicateNamespace(t *testing.T) {
	registry := NewRegistry()

	module1 := &LoadedModule{
		Namespace: "utils",
		Path:      "/path/to/utils.star",
		Exports:   starlark.StringDict{},
	}
	module2 := &LoadedModule{
		Namespace: "utils",
		Path:      "/other/path/utils.star",
		Exports:   starlark.StringDict{},
	}

	err := registry.Register(module1)
	require.NoError(t, err, "unexpected error")

	err = registry.Register(module2)
	require.Error(t, err, "expected error for duplicate namespace")

	regErr, ok := err.(*RegistryError)
	require.True(t, ok, "expected *RegistryError, got %T", err)
	assert.Equal(t, "utils", regErr.Namespace, "expected namespace 'utils'")
}

func TestRegistry_RegisterAll(t *testing.T) {
	registry := NewRegistry()

	modules := []*LoadedModule{
		{Namespace: "datetime", Path: "/datetime.star", Exports: starlark.StringDict{}},
		{Namespace: "math", Path: "/math.star", Exports: starlark.StringDict{}},
		{Namespace: "utils", Path: "/utils.star", Exports: starlark.StringDict{}},
	}

	err := registry.RegisterAll(modules)
	require.NoError(t, err, "unexpected error")

	assert.Equal(t, 3, registry.Len(), "expected 3 modules")

	for _, m := range modules {
		assert.True(t, registry.Has(m.Namespace), "expected registry to have %q", m.Namespace)
	}
}

func TestRegistry_RegisterAll_StopsOnError(t *testing.T) {
	registry := NewRegistry()

	modules := []*LoadedModule{
		{Namespace: "datetime", Path: "/datetime.star", Exports: starlark.StringDict{}},
		{Namespace: "config", Path: "/config.star", Exports: starlark.StringDict{}}, // reserved
		{Namespace: "utils", Path: "/utils.star", Exports: starlark.StringDict{}},
	}

	err := registry.RegisterAll(modules)
	require.Error(t, err, "expected error")

	// Only the first one should be registered
	assert.Equal(t, 1, registry.Len(), "expected 1 module (before error)")
}

func TestRegistry_Get(t *testing.T) {
	registry := NewRegistry()

	module := &LoadedModule{
		Namespace: "datetime",
		Path:      "/path/to/datetime.star",
		Exports: starlark.StringDict{
			"now": starlark.String("func"),
		},
	}
	registry.Register(module)

	got := registry.Get("datetime")
	assert.Equal(t, module, got, "Get returned wrong module")

	got = registry.Get("nonexistent")
	assert.Nil(t, got, "expected nil for nonexistent namespace")
}

func TestRegistry_Namespaces(t *testing.T) {
	registry := NewRegistry()

	modules := []*LoadedModule{
		{Namespace: "zeta", Path: "/zeta.star", Exports: starlark.StringDict{}},
		{Namespace: "alpha", Path: "/alpha.star", Exports: starlark.StringDict{}},
		{Namespace: "beta", Path: "/beta.star", Exports: starlark.StringDict{}},
	}
	registry.RegisterAll(modules)

	namespaces := registry.Namespaces()
	require.Len(t, namespaces, 3, "expected 3 namespaces")

	// Should be sorted
	expected := []string{"alpha", "beta", "zeta"}
	for i, ns := range expected {
		assert.Equal(t, ns, namespaces[i], "expected %q at index %d", ns, i)
	}
}

func TestRegistry_ToStarlarkDict(t *testing.T) {
	registry := NewRegistry()

	module := &LoadedModule{
		Namespace: "utils",
		Path:      "/utils.star",
		Exports: starlark.StringDict{
			"greet": starlark.String("hello_func"),
			"add":   starlark.String("add_func"),
		},
	}
	registry.Register(module)

	dict := registry.ToStarlarkDict()
	require.Len(t, dict, 1, "expected 1 entry")

	utilsVal, ok := dict["utils"]
	require.True(t, ok, "expected 'utils' in dict")

	// Check it's a module with HasAttrs
	mod, ok := utilsVal.(starlark.HasAttrs)
	require.True(t, ok, "expected HasAttrs, got %T", utilsVal)

	// Check attribute access
	greetVal, err := mod.Attr("greet")
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, `"hello_func"`, greetVal.String(), "expected 'hello_func'")

	// Check AttrNames
	attrNames := mod.AttrNames()
	assert.Len(t, attrNames, 2, "expected 2 attr names")
}

func TestStarlarkModule_NoSuchAttr(t *testing.T) {
	mod := &starlarkModule{
		name:    "test",
		exports: starlark.StringDict{},
	}

	_, err := mod.Attr("nonexistent")
	assert.Error(t, err, "expected error for nonexistent attr")
}

func TestStarlarkModule_Interface(t *testing.T) {
	mod := &starlarkModule{
		name: "test",
		exports: starlark.StringDict{
			"foo": starlark.String("bar"),
		},
	}

	// Test String()
	assert.Equal(t, "<module test>", mod.String(), "unexpected String()")

	// Test Type()
	assert.Equal(t, "module", mod.Type(), "unexpected Type()")

	// Test Truth()
	assert.Equal(t, starlark.True, mod.Truth(), "expected Truth() to return True")

	// Test Hash()
	_, err := mod.Hash()
	assert.Error(t, err, "expected error from Hash()")
}

func TestLoadAndRegister(t *testing.T) {
	// Test with nonexistent directory - should return empty registry
	registry, err := LoadAndRegister("/nonexistent/path")
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, 0, registry.Len(), "expected empty registry")
}
