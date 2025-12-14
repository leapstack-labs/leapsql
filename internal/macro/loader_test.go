package macro

import (
	"os"
	"path/filepath"
	"testing"

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
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				if err := os.Mkdir(macrosDir, 0755); err != nil {
					t.Fatal(err)
				}
				return macrosDir
			},
			wantModules: 0,
		},
		{
			name: "non-existent directory",
			setupDir: func(t *testing.T) string {
				return "/nonexistent/path/to/macros"
			},
			wantNil: true,
		},
		{
			name: "not a directory",
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				filePath := filepath.Join(dir, "macros")
				if err := os.WriteFile(filePath, []byte("not a dir"), 0644); err != nil {
					t.Fatal(err)
				}
				return filePath
			},
			wantErr: true,
		},
		{
			name: "single macro with multiple functions",
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				if err := os.Mkdir(macrosDir, 0755); err != nil {
					t.Fatal(err)
				}
				macroContent := `
def greet(name):
    return "Hello, " + name + "!"

def add(a, b):
    return a + b

_private = "should not be exported"
`
				macroPath := filepath.Join(macrosDir, "utils.star")
				if err := os.WriteFile(macroPath, []byte(macroContent), 0644); err != nil {
					t.Fatal(err)
				}
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
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				if err := os.Mkdir(macrosDir, 0755); err != nil {
					t.Fatal(err)
				}
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
					if err := os.WriteFile(path, []byte(content), 0644); err != nil {
						t.Fatal(err)
					}
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
				if err := os.Mkdir(macrosDir, 0755); err != nil {
					t.Fatal(err)
				}
				badContent := `
def broken(:
    return 1
`
				macroPath := filepath.Join(macrosDir, "broken.star")
				if err := os.WriteFile(macroPath, []byte(badContent), 0644); err != nil {
					t.Fatal(err)
				}
				return macrosDir
			},
			wantErr: true,
		},
		{
			name: "invalid namespace (starts with number)",
			setupDir: func(t *testing.T) string {
				dir := t.TempDir()
				macrosDir := filepath.Join(dir, "macros")
				if err := os.Mkdir(macrosDir, 0755); err != nil {
					t.Fatal(err)
				}
				macroPath := filepath.Join(macrosDir, "123invalid.star")
				if err := os.WriteFile(macroPath, []byte("x = 1"), 0644); err != nil {
					t.Fatal(err)
				}
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
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantNil {
				if modules != nil {
					t.Errorf("expected nil modules, got %v", modules)
				}
				return
			}

			if len(modules) != tt.wantModules {
				t.Fatalf("expected %d modules, got %d", tt.wantModules, len(modules))
			}

			// Check namespaces
			if len(tt.wantNamespaces) > 0 {
				namespaces := make(map[string]bool)
				for _, m := range modules {
					namespaces[m.Namespace] = true
				}
				for _, ns := range tt.wantNamespaces {
					if !namespaces[ns] {
						t.Errorf("expected namespace %q not found", ns)
					}
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
					if !ok {
						t.Errorf("namespace %q not found", ns)
						continue
					}
					for _, export := range expectedExports {
						if _, ok := module.Exports[export]; !ok {
							t.Errorf("expected export %q in namespace %q", export, ns)
						}
					}
					// Check that private symbols are not exported
					if _, ok := module.Exports["_private"]; ok {
						t.Error("'_private' should not be exported")
					}
				}
			}
		})
	}
}

func TestLoader_Load_SyntaxError_Details(t *testing.T) {
	// This test verifies the LoadError structure specifically
	dir := t.TempDir()
	macrosDir := filepath.Join(dir, "macros")
	if err := os.Mkdir(macrosDir, 0755); err != nil {
		t.Fatal(err)
	}

	badContent := `
def broken(:
    return 1
`
	macroPath := filepath.Join(macrosDir, "broken.star")
	if err := os.WriteFile(macroPath, []byte(badContent), 0644); err != nil {
		t.Fatal(err)
	}

	loader := NewLoader(macrosDir)
	_, err := loader.Load()
	if err == nil {
		t.Fatal("expected error for syntax error in macro")
	}

	loadErr, ok := err.(*LoadError)
	if !ok {
		t.Fatalf("expected *LoadError, got %T", err)
	}
	if loadErr.File != macroPath {
		t.Errorf("expected file %q, got %q", macroPath, loadErr.File)
	}
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
			if (err != nil) != tt.wantErr {
				t.Errorf("validateNamespace(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestLoader_ExecuteFunction(t *testing.T) {
	dir := t.TempDir()
	macrosDir := filepath.Join(dir, "macros")
	if err := os.Mkdir(macrosDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a macro with a function we can call
	macroContent := `
def double(x):
    return x * 2
`
	macroPath := filepath.Join(macrosDir, "math.star")
	if err := os.WriteFile(macroPath, []byte(macroContent), 0644); err != nil {
		t.Fatal(err)
	}

	loader := NewLoader(macrosDir)
	modules, err := loader.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	module := modules[0]
	doubleFn := module.Exports["double"]
	if doubleFn == nil {
		t.Fatal("expected 'double' function")
	}

	// Call the function
	thread := &starlark.Thread{Name: "test"}
	result, err := starlark.Call(thread, doubleFn, starlark.Tuple{starlark.MakeInt(5)}, nil)
	if err != nil {
		t.Fatalf("failed to call function: %v", err)
	}

	intResult, ok := result.(starlark.Int)
	if !ok {
		t.Fatalf("expected Int result, got %T", result)
	}

	val, _ := intResult.Int64()
	if val != 10 {
		t.Errorf("expected 10, got %d", val)
	}
}
