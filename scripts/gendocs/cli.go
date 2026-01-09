package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// generateCLIDocs generates CLI documentation from Cobra commands.
func generateCLIDocs(outDir string) error {
	log.Printf("Generating CLI docs to %s", outDir)

	// Create output directory
	if err := os.MkdirAll(outDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get root command
	rootCmd := cli.NewRootCmd()

	// Generate index page
	if err := generateCLIIndex(rootCmd, outDir); err != nil {
		return fmt.Errorf("failed to generate index: %w", err)
	}
	log.Printf("  Generated index.md")

	// Generate page for each command
	for _, cmd := range rootCmd.Commands() {
		if cmd.Hidden || cmd.Name() == "help" || cmd.Name() == "__complete" {
			continue
		}
		if err := generateCommandPage(cmd, outDir); err != nil {
			return fmt.Errorf("failed to generate page for %s: %w", cmd.Name(), err)
		}
		log.Printf("  Generated %s.md", cmd.Name())
	}

	return nil
}

// generateCLIIndex generates the CLI overview page.
func generateCLIIndex(rootCmd *cobra.Command, outDir string) error {
	w := NewMarkdownWriter()

	// Frontmatter
	w.Frontmatter("CLI Reference", "Command-line interface reference for LeapSQL")
	w.GeneratedMarker()

	// Title and intro
	w.Header(1, "CLI Reference")
	w.Paragraph("LeapSQL provides a command-line interface for running models, managing seeds, and inspecting your data transformation pipeline.")

	// Installation
	w.Header(2, "Installation")
	w.CodeBlock("bash", "go install github.com/leapstack-labs/leapsql/cmd/leapsql@latest")

	// Basic usage
	w.Header(2, "Basic Usage")
	w.CodeBlock("bash", "leapsql <command> [options]")

	// Commands table
	w.Header(2, "Commands")

	headers := []string{"Command", "Description"}
	var rows [][]string

	for _, cmd := range rootCmd.Commands() {
		if cmd.Hidden || cmd.Name() == "help" || cmd.Name() == "__complete" {
			continue
		}
		link := fmt.Sprintf("[%s](/cli/%s)", InlineCode(cmd.Name()), cmd.Name())
		rows = append(rows, []string{link, cleanDescription(cmd.Short)})
	}

	w.Table(headers, rows)

	// Global flags
	w.Header(2, "Global Options")
	w.Paragraph("These flags are available for all commands:")
	writeFlagsTable(w, rootCmd.PersistentFlags())

	// Environment variables
	w.Header(2, "Environment Variables")
	w.Paragraph("LeapSQL respects these environment variables:")

	envHeaders := []string{"Variable", "Description"}
	envRows := [][]string{
		{InlineCode("LEAPSQL_MODELS_DIR"), "Default models directory"},
		{InlineCode("LEAPSQL_SEEDS_DIR"), "Default seeds directory"},
		{InlineCode("LEAPSQL_MACROS_DIR"), "Default macros directory"},
		{InlineCode("LEAPSQL_DATABASE"), "Default database path"},
		{InlineCode("LEAPSQL_STATE_PATH"), "Default state database path"},
		{InlineCode("LEAPSQL_ENVIRONMENT"), "Default environment name"},
	}
	w.Table(envHeaders, envRows)

	w.Paragraph("Command-line flags take precedence over environment variables.")

	// Exit codes
	w.Header(2, "Exit Codes")
	exitHeaders := []string{"Code", "Meaning"}
	exitRows := [][]string{
		{InlineCode("0"), "Success"},
		{InlineCode("1"), "Error (check stderr for details)"},
	}
	w.Table(exitHeaders, exitRows)

	// Getting help
	w.Header(2, "Getting Help")
	w.CodeBlock("bash", `# General help
leapsql help
leapsql --help

# Command-specific help
leapsql run --help`)

	// Write file
	filename := filepath.Join(outDir, "index.md")
	return os.WriteFile(filename, w.Bytes(), 0600)
}

// generateCommandPage generates documentation for a single command.
func generateCommandPage(cmd *cobra.Command, outDir string) error {
	w := NewMarkdownWriter()

	// Frontmatter
	w.Frontmatter(cmd.Name(), cmd.Short)
	w.GeneratedMarker()

	// Title and long description
	w.Header(1, cmd.Name())
	if cmd.Long != "" {
		w.Paragraph(cmd.Long)
	} else {
		w.Paragraph(cmd.Short)
	}

	// Usage
	w.Header(2, "Usage")
	useLine := cmd.UseLine()
	if cmd.HasSubCommands() {
		useLine = fmt.Sprintf("leapsql %s <subcommand> [options]", cmd.Name())
	} else if !strings.HasPrefix(useLine, "leapsql") {
		useLine = "leapsql " + useLine
	}
	w.CodeBlock("bash", useLine)

	// Aliases
	if len(cmd.Aliases) > 0 {
		w.Header(2, "Aliases")
		var aliases []string
		for _, alias := range cmd.Aliases {
			aliases = append(aliases, InlineCode(alias))
		}
		w.BulletList(aliases)
	}

	// Subcommands
	if cmd.HasSubCommands() {
		w.Header(2, "Subcommands")
		headers := []string{"Subcommand", "Description"}
		var rows [][]string
		for _, sub := range cmd.Commands() {
			if sub.Hidden {
				continue
			}
			rows = append(rows, []string{InlineCode(sub.Name()), cleanDescription(sub.Short)})
		}
		w.Table(headers, rows)
	}

	// Local flags
	if cmd.HasLocalFlags() {
		w.Header(2, "Options")
		writeFlagsTable(w, cmd.LocalFlags())
	}

	// Inherited flags from parent
	if cmd.HasInheritedFlags() {
		w.Header(2, "Global Options")
		writeFlagsTable(w, cmd.InheritedFlags())
	}

	// Examples
	if cmd.Example != "" {
		w.Header(2, "Examples")
		// Clean up example - remove common leading whitespace
		example := cleanExample(cmd.Example)
		w.CodeBlock("bash", example)
	}

	// Write file
	filename := filepath.Join(outDir, cmd.Name()+".md")
	return os.WriteFile(filename, w.Bytes(), 0600)
}

// writeFlagsTable writes a table of flags.
func writeFlagsTable(w *MarkdownWriter, flags *pflag.FlagSet) {
	headers := []string{"Option", "Short", "Default", "Description"}
	var rows [][]string

	flags.VisitAll(func(f *pflag.Flag) {
		// Skip hidden flags
		if f.Hidden {
			return
		}

		option := "--" + f.Name
		short := ""
		if f.Shorthand != "" {
			short = "-" + f.Shorthand
		}

		defVal := f.DefValue
		switch {
		case defVal == "":
			// Keep empty
		case defVal == "false" || defVal == "true":
			// Keep as-is for booleans
		case f.Value.Type() == "string" && defVal != "":
			defVal = InlineCode(defVal)
		}

		desc := cleanDescription(f.Usage)

		rows = append(rows, []string{
			InlineCode(option),
			short,
			defVal,
			desc,
		})
	})

	w.Table(headers, rows)
}

// cleanExample removes common leading whitespace from example text.
func cleanExample(example string) string {
	lines := strings.Split(example, "\n")
	if len(lines) == 0 {
		return example
	}

	// Find minimum indentation (ignoring empty lines)
	minIndent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if minIndent == -1 || indent < minIndent {
			minIndent = indent
		}
	}

	if minIndent <= 0 {
		return strings.TrimSpace(example)
	}

	// Remove common indentation
	var result []string
	for _, line := range lines {
		if len(line) >= minIndent {
			result = append(result, line[minIndent:])
		} else {
			result = append(result, strings.TrimLeft(line, " \t"))
		}
	}

	return strings.TrimSpace(strings.Join(result, "\n"))
}
