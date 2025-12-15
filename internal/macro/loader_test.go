package macro

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func TestLoader_Load(t *testing.T) {
	tests := []struct {
		name           string
		setupDir       func(t *testing.T) string
		wantModules    int
		wantNil        bool // expect nil modules (not empty slice)
		wantErr        bool
		wantNamespaces []string
		checkExports   map[string][]string // namespace -> expected exports
	}{
		{
			name: "empty directory",
			setupDir: func(_ *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				require.NoError(t, os.Mkdir(macrosDir, 0750))
				return macrosDir
			},
			wantModules: 0,
		},
		{
			name: "non-existent directory",
			setupDir: func(_ *testing.T) string {
				return "/nonexistent/path/to/macros"
			},
			wantNil: true,
		},
		{
			name: "not a directory",
			setupDir: func(_ *testing.T) string {
				dir := t.TempDir()
				filePath := filepath.Join(dir, "macros")
				require.NoError(t, os.WriteFile(filePath, []byte("not a dir"), 0600))
				return filePath
			},
			wantErr: true,
		},
		{
			name: "single macro with multiple functions",
			setupDir: func(_ *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				require.NoError(t, os.Mkdir(macrosDir, 0750))
				macroContent := `
def greet(name):
    return "Hello, " + name + "!"

def add(a, b):
    return a + b

_private = "should not be exported"
`
				macroPath := filepath.Join(macrosDir, "utils.star")
				require.NoError(t, os.WriteFile(macroPath, []byte(macroContent), 0600))
				return macrosDir
			},
			wantModules:    1,
			wantNamespaces: []string{"utils"},
			checkExports: map[string][]string{
				"utils": {"greet", "add"},
			},
		},
		{
			name: "multiple macro files",
			setupDir: func(_ *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				require.NoError(t, os.Mkdir(macrosDir, 0750))
				files := map[string]string{
					"datetime.star": `
def now():
    return "2024-01-01"
`,
					"math.star": `
def square(x):
    return x * x
`,
				}
				for name, content := range files {
					path := filepath.Join(macrosDir, name)
					require.NoError(t, os.WriteFile(path, []byte(content), 0600))
				}
				return macrosDir
			},
			wantModules:    2,
			wantNamespaces: []string{"datetime", "math"},
		},
		{
			name: "syntax error in macro",
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				require.NoError(t, os.Mkdir(macrosDir, 0750))
				badContent := `
def broken(:
    return 1
`
				macroPath := filepath.Join(macrosDir, "broken.star")
				require.NoError(t, os.WriteFile(macroPath, []byte(badContent), 0600))
				return macrosDir
			},
			wantErr: true,
		},
		{
			name: "invalid namespace (starts with number)",
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				require.NoError(t, os.Mkdir(macrosDir, 0750))
				macroPath := filepath.Join(macrosDir, "123invalid.star")
				require.NoError(t, os.WriteFile(macroPath, []byte("x = 1"), 0600))
				return macrosDir
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			macrosDir := tt.setupDir(t)
			loader := NewLoader(macrosDir)
			modules, err := loader.Load()

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, modules)
				return
			}

			require.Len(t, modules, tt.wantModules)

			// Check namespaces
			if len(tt.wantNamespaces) > 0 {
				namespaces := make(map[string]bool)
				for _, m := range modules {
					namespaces[m.Namespace] = true
				}
				for _, ns := range tt.wantNamespaces {
					assert.True(t, namespaces[ns], "expected namespace %q not found", ns)
				}
			}

			// Check exports
			if tt.checkExports != nil {
				moduleMap := make(map[string]*LoadedModule)
				for _, m := range modules {
					moduleMap[m.Namespace] = m
				}
				for ns, expectedExports := range tt.checkExports {
					module, ok := moduleMap[ns]
					require.True(t, ok, "namespace %q not found", ns)
					for _, export := range expectedExports {
						_, ok := module.Exports[export]
						assert.True(t, ok, "expected export %q in namespace %q", export, ns)
					}
					// Check that private symbols are not exported
					_, ok = module.Exports["_private"]
					assert.False(t, ok, "'_private' should not be exported")
				}
			}
		})
	}
}

func TestLoader_Load_SyntaxError_Details(t *testing.T) {
	// This test verifies the LoadError structure specifically
	dir := t.TempDir()
	macrosDir := filepath.Join(dir, "macros")
	require.NoError(t, os.Mkdir(macrosDir, 0750))

	badContent := `
def broken(:
    return 1
`
	macroPath := filepath.Join(macrosDir, "broken.star")
	require.NoError(t, os.WriteFile(macroPath, []byte(badContent), 0600))

	loader := NewLoader(macrosDir)
	_, err := loader.Load()
	require.Error(t, err)

	var loadErr *LoadError
	require.ErrorAs(t, err, &loadErr)
	assert.Equal(t, macroPath, loadErr.File)
}

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid lowercase", "datetime", false},
		{"valid with underscore", "date_time", false},
		{"valid start with underscore", "_private", false},
		{"valid with numbers", "utils2", false},
		{"empty", "", true},
		{"starts with number", "123abc", true},
		{"contains hyphen", "date-time", true},
		{"contains space", "date time", true},
		{"contains dot", "date.time", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNamespace(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoader_ExecuteFunction(t *testing.T) {
	dir := t.TempDir()
	macrosDir := filepath.Join(dir, "macros")
	require.NoError(t, os.Mkdir(macrosDir, 0750))

	// Create a macro with a function we can call
	macroContent := `
def double(x):
    return x * 2
`
	macroPath := filepath.Join(macrosDir, "math.star")
	require.NoError(t, os.WriteFile(macroPath, []byte(macroContent), 0600))

	loader := NewLoader(macrosDir)
	modules, err := loader.Load()
	require.NoError(t, err)

	module := modules[0]
	doubleFn := module.Exports["double"]
	require.NotNil(t, doubleFn, "expected 'double' function")

	// Call the function
	thread := &starlark.Thread{Name: "test"}
	result, err := starlark.Call(thread, doubleFn, starlark.Tuple{starlark.MakeInt(5)}, nil)
	require.NoError(t, err)

	intResult, ok := result.(starlark.Int)
	require.True(t, ok, "expected Int result, got %T", result)

	val, _ := intResult.Int64()
	assert.Equal(t, int64(10), val)
}
