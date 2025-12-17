package commands

import (
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

//go:embed all:templates
var templateFS embed.FS

// copyTemplate copies an embedded template directory to the target path.
// It handles special file renames (e.g., "gitignore" -> ".gitignore").
func copyTemplate(templateName, targetDir string, force bool) error {
	root := filepath.Join("templates", templateName)

	return fs.WalkDir(templateFS, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path from template root
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		// Skip root directory
		if relPath == "." {
			return nil
		}

		// Handle special file renames
		targetPath := filepath.Join(targetDir, renameSpecialFiles(relPath))

		if d.IsDir() {
			return os.MkdirAll(targetPath, 0750)
		}

		// Check if file exists
		if !force {
			if _, err := os.Stat(targetPath); err == nil {
				return nil // Skip existing files
			}
		}

		// Read and write file
		content, err := templateFS.ReadFile(path)
		if err != nil {
			return err
		}

		return os.WriteFile(targetPath, content, 0600)
	})
}

// renameSpecialFiles handles files that need renaming (e.g., dotfiles).
func renameSpecialFiles(path string) string {
	// Rename "gitignore" to ".gitignore"
	base := filepath.Base(path)
	dir := filepath.Dir(path)

	switch base {
	case "gitignore":
		return filepath.Join(dir, ".gitignore")
	default:
		return path
	}
}

// listTemplateFiles returns all files in a template for display purposes.
func listTemplateFiles(templateName string) ([]string, error) {
	var files []string
	root := filepath.Join("templates", templateName)

	err := fs.WalkDir(templateFS, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			relPath, _ := filepath.Rel(root, path)
			files = append(files, renameSpecialFiles(relPath))
		}
		return nil
	})

	return files, err
}

// groupTemplateFiles groups files by category for display.
func groupTemplateFiles(files []string) map[string][]string {
	groups := map[string][]string{
		"config": {},
		"seeds":  {},
		"models": {},
		"macros": {},
	}

	for _, f := range files {
		switch {
		case strings.HasPrefix(f, "seeds/") || strings.HasPrefix(f, "seeds\\"):
			groups["seeds"] = append(groups["seeds"], f)
		case strings.HasPrefix(f, "models/") || strings.HasPrefix(f, "models\\"):
			groups["models"] = append(groups["models"], f)
		case strings.HasPrefix(f, "macros/") || strings.HasPrefix(f, "macros\\"):
			groups["macros"] = append(groups["macros"], f)
		default:
			groups["config"] = append(groups["config"], f)
		}
	}

	return groups
}
