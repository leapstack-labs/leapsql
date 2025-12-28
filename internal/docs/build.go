// Package docs provides frontend build utilities using esbuild.
package docs

import (
	"fmt"
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
func BuildFrontend(docsDir string, minify bool) (*BuildResult, error) {
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

	return buildResult, nil
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
