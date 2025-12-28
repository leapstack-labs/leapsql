// Package docs provides frontend build utilities using esbuild.
package docs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/evanw/esbuild/pkg/api"
)

// BuildResult contains the compiled JS and CSS from the frontend build.
type BuildResult struct {
	JS  string
	CSS string
}

// BuildFrontend compiles TypeScript/JSX source files into bundled JS and CSS.
// It uses esbuild with Preact JSX transformation and aliases React to Preact.
// The theme parameter specifies which theme CSS to use (vercel, claude, corporate).
func BuildFrontend(docsDir string, minify bool, theme string) (*BuildResult, error) {
	srcDir := filepath.Join(docsDir, "src")
	entryPoint := filepath.Join(srcDir, "main.tsx")
	nodeModules := filepath.Join(docsDir, "node_modules")

	// Build options
	buildOpts := api.BuildOptions{
		EntryPoints: []string{entryPoint},
		Bundle:      true,
		Write:       false, // Keep in memory for injection

		// Virtual output directory (required for CSS bundling even with Write: false)
		Outdir: "out",

		// Preact JSX transformation
		JSX:             api.JSXAutomatic,
		JSXImportSource: "preact",

		// React → Preact aliasing (for React Flow compatibility)
		Alias: map[string]string{
			"react":             "preact/compat",
			"react-dom":         "preact/compat",
			"react/jsx-runtime": "preact/jsx-runtime",
		},

		// External packages (not bundled, resolved at runtime)
		// sql.js-httpvfs is only used in production WASM mode, not dev mode
		External: []string{"sql.js-httpvfs"},

		// Resolve from node_modules
		NodePaths: []string{nodeModules},

		Loader: map[string]api.Loader{
			".tsx": api.LoaderTSX,
			".ts":  api.LoaderTS,
			".css": api.LoaderCSS,
		},

		Platform: api.PlatformBrowser,
		Format:   api.FormatIIFE, // Single file, no imports
		Target:   api.ES2020,

		// Tree shaking
		TreeShaking: api.TreeShakingTrue,

		// Source maps for development (optional)
		Sourcemap: api.SourceMapNone,

		// Define process.env.NODE_ENV
		Define: map[string]string{
			"process.env.NODE_ENV": `"production"`,
		},

		// Log level
		LogLevel: api.LogLevelWarning,
	}

	// Add minification for production
	if minify {
		buildOpts.MinifyWhitespace = true
		buildOpts.MinifyIdentifiers = true
		buildOpts.MinifySyntax = true
	}

	// Run the build
	result := api.Build(buildOpts)

	if len(result.Errors) > 0 {
		var errMsg string
		for _, err := range result.Errors {
			errMsg += fmt.Sprintf("%s:%d:%d: %s\n",
				err.Location.File,
				err.Location.Line,
				err.Location.Column,
				err.Text)
		}
		return nil, fmt.Errorf("esbuild errors:\n%s", errMsg)
	}

	// Extract JS and CSS from output files
	buildResult := &BuildResult{}
	for _, file := range result.OutputFiles {
		if filepath.Ext(file.Path) == ".js" {
			buildResult.JS = string(file.Contents)
		} else if filepath.Ext(file.Path) == ".css" {
			buildResult.CSS = string(file.Contents)
		}
	}

	if buildResult.JS == "" {
		return nil, fmt.Errorf("no JavaScript output generated")
	}

	// Inject theme CSS at the beginning
	themeCSS, err := loadThemeCSS(docsDir, theme)
	if err != nil {
		// Log warning but don't fail - use built CSS only
		fmt.Printf("Warning: failed to load theme CSS: %v\n", err)
	} else {
		buildResult.CSS = themeCSS + "\n" + buildResult.CSS
	}

	return buildResult, nil
}

// loadThemeCSS loads the theme CSS file and base mappings.
func loadThemeCSS(docsDir, theme string) (string, error) {
	if theme == "" {
		theme = "vercel"
	}

	themesDir := filepath.Join(docsDir, "src", "css", "themes")

	// Read theme CSS
	themePath := filepath.Join(themesDir, theme+".css")
	themeCSS, err := os.ReadFile(themePath) //nolint:gosec // G304: themePath is constructed from trusted input
	if err != nil {
		// Fall back to vercel theme
		themePath = filepath.Join(themesDir, "vercel.css")
		themeCSS, err = os.ReadFile(themePath) //nolint:gosec // G304: themePath is constructed from trusted input
		if err != nil {
			return "", fmt.Errorf("failed to read theme file: %w", err)
		}
	}

	// Read base mappings
	basePath := filepath.Join(themesDir, "_base.css")
	baseCSS, err := os.ReadFile(basePath) //nolint:gosec // G304: basePath is constructed from trusted input
	if err != nil {
		// Base is optional - just use theme without base
		return string(themeCSS), nil //nolint:nilerr // err is expected when base file doesn't exist
	}

	return string(themeCSS) + "\n" + string(baseCSS), nil
}

// GetDocsDir returns the absolute path to the docs package directory.
// This is useful for locating the src/ and node_modules/ directories.
func GetDocsDir() (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get current file path")
	}
	return filepath.Dir(currentFile), nil
}
