// Package main provides a generator that extracts CLI, schema, globals, and lint metadata
// from LeapSQL source code and generates markdown documentation.
//
// Usage:
//
//	go run ./scripts/gendocs -gen=cli -outdir=docs/cli
//	go run ./scripts/gendocs -gen=schema -outdir=docs/concepts
//	go run ./scripts/gendocs -gen=globals -outdir=docs/templating
//	go run ./scripts/gendocs -gen=lint -outdir=docs/linting
//	go run ./scripts/gendocs -gen=all
package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
)

var (
	genFlag    = flag.String("gen", "all", "what to generate: cli, schema, globals, lint, all")
	outDirFlag = flag.String("outdir", "", "output directory (defaults based on gen type)")
)

func main() {
	flag.Parse()

	// Validate gen flag
	validGenFlags := map[string]bool{"cli": true, "schema": true, "globals": true, "lint": true, "all": true}
	if !validGenFlags[*genFlag] {
		log.Fatalf("unknown -gen value: %s (use: cli, schema, globals, lint, all)", *genFlag)
	}

	// Find project root (where go.mod is)
	projectRoot, err := findProjectRoot()
	if err != nil {
		log.Fatalf("failed to find project root: %v", err)
	}

	log.Printf("Project root: %s", projectRoot)

	switch *genFlag {
	case "cli":
		outDir := *outDirFlag
		if outDir == "" {
			outDir = filepath.Join(projectRoot, "docs", "cli")
		}
		if err := generateCLIDocs(outDir); err != nil {
			log.Fatalf("failed to generate CLI docs: %v", err)
		}

	case "schema":
		outDir := *outDirFlag
		if outDir == "" {
			outDir = filepath.Join(projectRoot, "docs", "concepts")
		}
		if err := generateSchemaDocs(outDir); err != nil {
			log.Fatalf("failed to generate schema docs: %v", err)
		}

	case "globals":
		outDir := *outDirFlag
		if outDir == "" {
			outDir = filepath.Join(projectRoot, "docs", "templating")
		}
		if err := generateGlobalsDocs(outDir); err != nil {
			log.Fatalf("failed to generate globals docs: %v", err)
		}

	case "lint":
		outDir := *outDirFlag
		if outDir == "" {
			outDir = filepath.Join(projectRoot, "docs", "linting")
		}
		if err := generateLintDocs(outDir); err != nil {
			log.Fatalf("failed to generate lint docs: %v", err)
		}

	case "all":
		// Generate CLI docs
		cliOutDir := filepath.Join(projectRoot, "docs", "cli")
		if err := generateCLIDocs(cliOutDir); err != nil {
			log.Fatalf("failed to generate CLI docs: %v", err)
		}

		// Generate schema docs
		schemaOutDir := filepath.Join(projectRoot, "docs", "concepts")
		if err := generateSchemaDocs(schemaOutDir); err != nil {
			log.Fatalf("failed to generate schema docs: %v", err)
		}

		// Generate globals docs
		globalsOutDir := filepath.Join(projectRoot, "docs", "templating")
		if err := generateGlobalsDocs(globalsOutDir); err != nil {
			log.Fatalf("failed to generate globals docs: %v", err)
		}

		// Generate lint docs
		lintOutDir := filepath.Join(projectRoot, "docs", "linting")
		if err := generateLintDocs(lintOutDir); err != nil {
			log.Fatalf("failed to generate lint docs: %v", err)
		}
	}

	log.Println("Done!")
}

// findProjectRoot walks up from current directory to find go.mod.
func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
